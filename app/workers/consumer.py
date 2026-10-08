import asyncio
import json
import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager, suppress
from typing import Annotated
from uuid import UUID

import httpx
from faststream import Context, FastStream
from faststream.exceptions import NackMessage, RejectMessage
from sqlalchemy.exc import SQLAlchemyError

from app.core.config import get_settings
from app.core.db import engine
from app.core.logging import configure_logging
from app.core.messaging import broker, declare_topology, exchange, queue
from app.exceptions.payment import InvalidEvent
from app.services.payments import process_event
from app.workers.outbox import run_outbox

logger = logging.getLogger(__name__)
MAX_MESSAGE_BYTES = 64 * 1024


@asynccontextmanager
async def lifespan() -> AsyncIterator[None]:
    configure_logging(get_settings().log_level)
    try:
        async with httpx.AsyncClient(
            timeout=get_settings().webhook_timeout,
            follow_redirects=False,
        ) as client:
            app.context.set_global("webhook_client", client)
            try:
                yield
            finally:
                app.context.reset_global("webhook_client")
    finally:
        await engine.dispose()


app = FastStream(broker, logger=logger, lifespan=lifespan)
WebhookClient = Annotated[httpx.AsyncClient, Context("webhook_client")]


def _parse_event_id(body: bytes) -> UUID:
    try:
        if len(body) > MAX_MESSAGE_BYTES:
            raise ValueError("Message exceeds 64 KiB")
        payload = json.loads(body)
        event_id = payload.get("event_id") if isinstance(payload, dict) else None
        if not isinstance(event_id, str):
            raise ValueError("Expected an object with a string event_id")
        return UUID(event_id)
    except (ValueError, TypeError, RecursionError) as exc:
        raise InvalidEvent("Invalid payment event") from exc


# ACK после коммита; ошибки БД возвращают сообщение в очередь, некорректные события — в DLQ.
@broker.subscriber(
    queue,
    exchange,
    no_reply=True,
    decoder=lambda msg: msg.body,
)
async def consume(body: bytes, client: WebhookClient) -> None:
    try:
        event_id = _parse_event_id(body)
        await process_event(event_id, client)
    except InvalidEvent as exc:
        logger.warning("Invalid message sent to DLQ: %s", exc)
        raise RejectMessage(requeue=False) from exc
    except (SQLAlchemyError, TimeoutError) as exc:
        # При недоступной БД нельзя сохранить повтор; возвращаем исходное сообщение в очередь.
        logger.exception("Infrastructure failure; requeueing message")
        await asyncio.sleep(get_settings().retry_base_delay)
        raise NackMessage(requeue=True) from exc
    except Exception as exc:
        logger.exception("Unexpected processing error; rejecting message to DLQ")
        raise RejectMessage(requeue=False) from exc


@app.on_startup
async def startup() -> None:
    await broker.connect()
    await declare_topology()


@app.after_startup
async def start_publisher() -> None:
    app.context.set_global("outbox_task", asyncio.create_task(run_outbox()))


@app.on_shutdown
async def stop_outbox() -> None:
    task = app.context.get("outbox_task")
    if task is not None:
        task.cancel()
        try:
            with suppress(asyncio.CancelledError):
                await task
        except Exception:
            # Сбой публикатора не должен мешать остановке брокера и возврату сообщений в очередь.
            logger.exception("Outbox publisher failed before shutdown")
        finally:
            app.context.reset_global("outbox_task")
