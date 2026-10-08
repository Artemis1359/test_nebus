import asyncio
import logging
from datetime import UTC, datetime

from sqlalchemy import func, select

from app.core.config import get_settings
from app.core.db import sessions
from app.core.messaging import broker, dead_exchange, exchange
from app.models import Outbox

logger = logging.getLogger(__name__)


async def publish_one() -> bool:
    # Сбой после подтверждения брокера может дать дубль; consumer проверяет ID события.
    async with sessions() as session, session.begin():
        event = await session.scalar(
            select(Outbox)
            .where(Outbox.published_at.is_(None), Outbox.available_at <= func.now())
            .order_by(Outbox.available_at, Outbox.id)
            .with_for_update(skip_locked=True)
            .limit(1)
        )
        if event is None:
            return False
        await broker.publish(
            {"event_id": str(event.id), "payment_id": str(event.payment_id), **event.payload},
            exchange=dead_exchange if event.routing_key == "payments.dlq" else exchange,
            routing_key=event.routing_key,
            persist=True,
            mandatory=True,
            message_id=str(event.id),
            timeout=10,
            headers={
                "attempt": event.payload["attempt"],
                "payment_id": str(event.payment_id),
                **({"error": event.payload["error"]} if "error" in event.payload else {}),
            },
        )
        event.published_at = datetime.now(UTC)
    return True


async def run_outbox() -> None:
    while True:
        try:
            if await publish_one():
                continue
        except Exception:
            logger.exception("Outbox publication failed; event remains pending")
        await asyncio.sleep(get_settings().outbox_poll_interval)
