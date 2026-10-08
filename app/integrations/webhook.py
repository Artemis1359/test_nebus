import asyncio

import httpx

from app.core.config import get_settings
from app.models import Payment
from app.schemas.payments import PaymentDetail


async def send_webhook(client: httpx.AsyncClient, payment: Payment) -> None:
    async with asyncio.timeout(get_settings().webhook_timeout):
        async with client.stream(
            "POST",
            payment.webhook_url,
            json=PaymentDetail.from_payment(payment).model_dump(mode="json"),
            headers={"Idempotency-Key": str(payment.id), "X-Payment-ID": str(payment.id)},
        ) as response:
            response.raise_for_status()
