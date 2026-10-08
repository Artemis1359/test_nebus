import json

from starlette.responses import JSONResponse
from starlette.types import ASGIApp, Message, Receive, Scope, Send

from app.core.validation import MAX_BODY_BYTES, MAX_JSON_DEPTH, validate_json


class RequestLimitsMiddleware:
    """Ограничиваем размер запроса и проверяем JSON до разбора фреймворком."""

    def __init__(self, app: ASGIApp) -> None:
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return
        chunks: list[bytes] = []
        size = 0
        while True:
            message = await receive()
            if message["type"] == "http.disconnect":
                return
            chunk = message.get("body", b"")
            size += len(chunk)
            if size > MAX_BODY_BYTES:
                await JSONResponse({"detail": "Request body must not exceed 64 KiB"}, 413)(
                    scope, receive, send
                )
                return
            chunks.append(chunk)
            if not message.get("more_body", False):
                break
        body = b"".join(chunks)
        content_type = (
            dict(scope["headers"]).get(b"content-type", b"").split(b";", 1)[0].strip().lower()
        )
        if body and (
            not content_type
            or content_type == b"application/json"
            or content_type.endswith(b"+json")
        ):
            try:
                validate_json(json.loads(body), max_depth=MAX_JSON_DEPTH + 1)
            except (ValueError, UnicodeDecodeError, RecursionError) as exc:
                detail = "JSON nesting is too deep" if isinstance(exc, RecursionError) else str(exc)
                await JSONResponse({"detail": detail}, 422)(scope, receive, send)
                return

        delivered = False

        async def bounded_receive() -> Message:
            nonlocal delivered
            if not delivered:
                delivered = True
                return {"type": "http.request", "body": body, "more_body": False}
            return await receive()

        await self.app(scope, bounded_receive, send)
