import asyncio
from types import SimpleNamespace

import httpx
import pytest

from app.integrations.webhook import send_webhook
from app.models import Payment
from app.schemas.payments import PaymentDetail


@pytest.fixture
def payment():
    from datetime import UTC, datetime
    from decimal import Decimal
    from uuid import uuid4

    from app.enums.payment import Currency, PaymentStatus

    return Payment(
        id=uuid4(),
        amount=Decimal("1.00"),
        currency=Currency.RUB,
        description="Webhook test",
        metadata_={},
        webhook_url="http://merchant.example/webhook",
        status=PaymentStatus.SUCCEEDED,
        created_at=datetime.now(UTC),
        processed_at=datetime.now(UTC),
    )


async def test_success_does_not_read_response_body(payment):
    class UnreadBody(httpx.AsyncByteStream):
        closed = False

        async def __aiter__(self):
            raise AssertionError("Webhook response body must not be read")
            yield b""  # pragma: no cover

        async def aclose(self):
            self.closed = True

    stream = UnreadBody()
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(lambda request: httpx.Response(204, stream=stream))
    ) as client:
        await send_webhook(client, payment)
    assert stream.closed


async def test_deadline_cancels_slow_response_headers(payment, monkeypatch):
    monkeypatch.setattr(
        "app.integrations.webhook.get_settings", lambda: SimpleNamespace(webhook_timeout=0.05)
    )
    cancelled = asyncio.Event()

    async def slow_headers(request):
        try:
            await asyncio.Event().wait()
        finally:
            cancelled.set()

    async with httpx.AsyncClient(transport=httpx.MockTransport(slow_headers)) as client:
        with pytest.raises(TimeoutError):
            await asyncio.wait_for(send_webhook(client, payment), timeout=1)
    assert cancelled.is_set()


async def test_webhook_payload_and_headers(payment):
    import json

    def receive(request):
        assert json.loads(request.content) == PaymentDetail.from_payment(payment).model_dump(
            mode="json"
        )
        assert request.headers["Idempotency-Key"] == str(payment.id)
        assert request.headers["X-Payment-ID"] == str(payment.id)
        assert "X-API-Key" not in request.headers
        return httpx.Response(200)

    async with httpx.AsyncClient(transport=httpx.MockTransport(receive)) as client:
        await send_webhook(client, payment)
