import hashlib
import json
import logging
from datetime import UTC, datetime, timedelta
from uuid import UUID, uuid4

import httpx
from sqlalchemy import select, update
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import get_settings
from app.core.db import sessions
from app.enums.payment import PaymentStatus
from app.exceptions.payment import GatewayError, IdempotencyConflict, InvalidEvent, PaymentNotFound
from app.integrations.gateway import process_payment
from app.integrations.webhook import send_webhook
from app.models import Outbox, Payment
from app.schemas.payments import PaymentCreate

logger = logging.getLogger(__name__)
MAX_ATTEMPTS = 3


async def create_payment(
    session: AsyncSession,
    body: PaymentCreate,
    idempotency_key: str,
) -> Payment:
    canonical = json.dumps(body.model_dump(mode="json"), sort_keys=True, separators=(",", ":"))
    request_hash = hashlib.sha256(canonical.encode()).hexdigest()
    async with session.begin():
        statement = (
            insert(Payment)
            .values(
                id=uuid4(),
                amount=body.amount,
                currency=body.currency,
                description=body.description,
                metadata_=body.metadata,
                webhook_url=str(body.webhook_url),
                idempotency_key=idempotency_key,
                request_hash=request_hash,
            )
            .on_conflict_do_nothing(index_elements=[Payment.idempotency_key])
            .returning(Payment)
        )
        payment = (await session.scalars(statement)).one_or_none()
        if payment is not None:
            # Запись outbox выполняется при выходе из транзакции; ошибка откатывает и платёж.
            session.add(Outbox(payment_id=payment.id, payload={"attempt": 1}))
        else:
            payment = (
                await session.scalars(
                    select(Payment).where(Payment.idempotency_key == idempotency_key)
                )
            ).one()
            if payment.request_hash != request_hash:
                raise IdempotencyConflict("Idempotency-Key used with another payload")
    return payment


async def get_payment(session: AsyncSession, payment_id: UUID) -> Payment:
    payment = await session.get(Payment, payment_id)
    if payment is None:
        raise PaymentNotFound("Payment not found")
    return payment


async def settle_payment(payment_id: UUID, *, final_attempt: bool = False) -> Payment:
    # Сохраняем результат шлюза до webhook, чтобы повторять только отправку уведомления.
    async with sessions() as session, session.begin():
        payment = await session.scalar(
            select(Payment).where(Payment.id == payment_id).with_for_update()
        )
        if payment is None:
            raise InvalidEvent("Payment does not exist")
        if payment.status == PaymentStatus.PENDING:
            try:
                payment.status = await process_payment()
            except GatewayError:
                if not final_attempt:
                    raise
                # Эмуляция не списывает деньги: после исчерпания попыток можно зафиксировать отказ.
                payment.status = PaymentStatus.FAILED
                logger.warning("Gateway attempts exhausted payment_id=%s", payment_id)
            payment.processed_at = datetime.now(UTC)
        return payment


async def process_event(event_id: UUID, client: httpx.AsyncClient) -> None:
    async with sessions() as session, session.begin():
        event = await session.scalar(select(Outbox).where(Outbox.id == event_id).with_for_update())
        if event is None or event.routing_key != "payments.new":
            raise InvalidEvent("Unknown payment event")
        if event.consumed_at is not None:
            return
        attempt = event.payload.get("attempt")
        if type(attempt) is not int or not 1 <= attempt <= MAX_ATTEMPTS:
            raise InvalidEvent("Invalid attempt")
        try:
            payment = await settle_payment(event.payment_id, final_attempt=attempt == MAX_ATTEMPTS)
        except GatewayError as exc:
            _schedule_retry(session, event, attempt, exc, category="gateway")
            event.consumed_at = datetime.now(UTC)
            return
        if payment.webhook_delivered_at is None:
            try:
                await send_webhook(client, payment)
            except (httpx.RequestError, httpx.HTTPStatusError, TimeoutError) as exc:
                _schedule_retry(session, event, attempt, exc, category="webhook")
            else:
                await session.execute(
                    update(Payment)
                    .where(Payment.id == payment.id)
                    .values(webhook_delivered_at=datetime.now(UTC))
                )
        event.consumed_at = datetime.now(UTC)


def _schedule_retry(
    session: AsyncSession, event: Outbox, attempt: int, exc: Exception, *, category: str
) -> None:
    # Сохраняем повтор или DLQ и завершение текущего события одной транзакцией до ACK.
    final = attempt == MAX_ATTEMPTS
    delay = 0 if final else get_settings().retry_base_delay * 2 ** (attempt - 1)
    http_status = exc.response.status_code if isinstance(exc, httpx.HTTPStatusError) else None
    session.add(
        Outbox(
            payment_id=event.payment_id,
            routing_key="payments.dlq" if final else "payments.new",
            payload={
                "attempt": attempt if final else attempt + 1,
                "error": type(exc).__name__,
                "category": category,
                "http_status": http_status,
                "source_event_id": str(event.id),
            },
            available_at=datetime.now(UTC) + timedelta(seconds=delay),
        )
    )
    logger.warning(
        "Payment attempt failed payment_id=%s attempt=%s dlq=%s "
        "category=%s error=%s http_status=%s",
        event.payment_id,
        attempt,
        final,
        category,
        type(exc).__name__,
        http_status,
    )
