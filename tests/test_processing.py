from datetime import UTC, datetime
from unittest.mock import AsyncMock
from uuid import UUID, uuid4

import httpx
import pytest
from sqlalchemy import select

from app.enums.payment import PaymentStatus
from app.exceptions.payment import GatewayError, InvalidEvent
from app.models import Outbox, Payment
from app.services.payments import process_event


async def create(api, database, payload):
    response = await api.post(
        "/api/v1/payments", json=payload, headers={"Idempotency-Key": "process"}
    )
    payment_id = UUID(response.json()["payment_id"])
    async with database() as session:
        event = await session.scalar(select(Outbox).where(Outbox.payment_id == payment_id))
    return payment_id, event.id


@pytest.mark.parametrize("result", ["succeeded", "failed"])
async def test_settles_once_and_deduplicates(api, database, payload, monkeypatch, result):
    gateway = AsyncMock(return_value=result)
    monkeypatch.setattr("app.services.payments.process_payment", gateway)
    payment_id, event_id = await create(api, database, payload)
    requests = []

    def webhook(request):
        requests.append(request)
        assert request.headers["Idempotency-Key"] == str(payment_id)
        return httpx.Response(204)

    async with httpx.AsyncClient(transport=httpx.MockTransport(webhook)) as client:
        await process_event(event_id, client)
        await process_event(event_id, client)
    assert len(requests) == 1
    gateway.assert_awaited_once()
    async with database() as session:
        payment = await session.get(Payment, payment_id)
        assert payment.status is PaymentStatus(result)
        assert payment.processed_at is not None
        assert payment.webhook_delivered_at is not None
        assert (await session.get(Outbox, event_id)).consumed_at is not None


async def test_retry_three_attempts_exponential_delay_and_dlq(api, database, payload, monkeypatch):
    monkeypatch.setattr(
        "app.services.payments.process_payment", gateway := AsyncMock(return_value="succeeded")
    )
    payment_id, event_id = await create(api, database, payload)
    calls = []

    def failing(request):
        calls.append(request)
        return httpx.Response(500)

    async with httpx.AsyncClient(transport=httpx.MockTransport(failing)) as client:
        for attempt in range(1, 4):
            before = datetime.now(UTC)
            await process_event(event_id, client)
            await process_event(
                event_id, client
            )  # Повторная доставка не должна создавать лишние попытки.
            async with database() as session:
                events = list(
                    (await session.scalars(select(Outbox).order_by(Outbox.created_at))).all()
                )
                assert len(events) == attempt + 1
                next_event = next(e for e in events if e.consumed_at is None)
                assert next_event.payload["attempt"] == min(attempt + 1, 3)
                assert next_event.routing_key == (
                    "payments.dlq" if attempt == 3 else "payments.new"
                )
                if attempt < 3:
                    assert (next_event.available_at - before).total_seconds() >= 2**attempt
                event_id = next_event.id
        with pytest.raises(InvalidEvent):
            await process_event(event_id, client)
    assert len(calls) == 3
    gateway.assert_awaited_once()
    async with database() as session:
        payment = await session.get(Payment, payment_id)
        assert payment.status == "succeeded"  # Ошибка webhook не меняет результат платежа.
        assert payment.webhook_delivered_at is None


async def test_retry_recovers_without_reprocessing(api, database, payload, monkeypatch):
    monkeypatch.setattr(
        "app.services.payments.process_payment", gateway := AsyncMock(return_value="failed")
    )
    payment_id, event_id = await create(api, database, payload)
    responses = iter([500, 200])
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(lambda request: httpx.Response(next(responses)))
    ) as client:
        await process_event(event_id, client)
        async with database() as session:
            retry = await session.scalar(select(Outbox).where(Outbox.consumed_at.is_(None)))
        await process_event(retry.id, client)
    gateway.assert_awaited_once()
    async with database() as session:
        assert (await session.get(Payment, payment_id)).webhook_delivered_at is not None
        assert len((await session.scalars(select(Outbox))).all()) == 2


async def test_unknown_event(database):
    async with httpx.AsyncClient() as client:
        with pytest.raises(InvalidEvent):
            await process_event(uuid4(), client)


async def test_concurrent_delivery_locks_event(api, database, payload, monkeypatch):
    import asyncio

    monkeypatch.setattr(
        "app.services.payments.process_payment", gateway := AsyncMock(return_value="succeeded")
    )
    _, event_id = await create(api, database, payload)
    calls = []

    def webhook(request):
        calls.append(request)
        return httpx.Response(200)

    async with httpx.AsyncClient(transport=httpx.MockTransport(webhook)) as client:
        await asyncio.gather(process_event(event_id, client), process_event(event_id, client))
    assert len(calls) == 1
    gateway.assert_awaited_once()


async def test_result_committed_before_webhook(api, database, payload, monkeypatch):
    monkeypatch.setattr(
        "app.services.payments.process_payment", AsyncMock(return_value="succeeded")
    )
    payment_id, event_id = await create(api, database, payload)

    async def webhook(request):
        async with database() as session:
            payment = await session.get(Payment, payment_id)
            assert payment.status == "succeeded"
            assert payment.processed_at is not None
        return httpx.Response(200)

    async with httpx.AsyncClient(transport=httpx.MockTransport(webhook)) as client:
        await process_event(event_id, client)


async def test_gateway_exception_retries_pending_payment(api, database, payload, monkeypatch):
    monkeypatch.setattr(
        "app.services.payments.process_payment",
        gateway := AsyncMock(side_effect=[GatewayError("Temporary gateway error"), "succeeded"]),
    )
    payment_id, event_id = await create(api, database, payload)
    calls = []

    def webhook(request):
        calls.append(request)
        return httpx.Response(200)

    async with httpx.AsyncClient(transport=httpx.MockTransport(webhook)) as client:
        await process_event(event_id, client)
        async with database() as session:
            payment = await session.get(Payment, payment_id)
            assert payment.status == "pending"
            assert payment.processed_at is None
            retry = await session.scalar(select(Outbox).where(Outbox.consumed_at.is_(None)))
        await process_event(retry.id, client)
    assert len(calls) == 1
    assert gateway.await_count == 2


@pytest.mark.parametrize("db_timeout", [False, True])
async def test_database_failure_in_settlement_does_not_spend_attempt(
    api, database, payload, monkeypatch, db_timeout
):
    from sqlalchemy.exc import OperationalError

    payment_id, event_id = await create(api, database, payload)
    failure = OperationalError("SELECT payments", {}, RuntimeError("DB unavailable"))
    if db_timeout:
        failure = TimeoutError("Database operation timed out")
    monkeypatch.setattr("app.services.payments.settle_payment", AsyncMock(side_effect=failure))
    async with httpx.AsyncClient() as client:
        with pytest.raises(type(failure)):
            await process_event(event_id, client)
    async with database() as session:
        events = list((await session.scalars(select(Outbox))).all())
        assert len(events) == 1
        assert events[0].consumed_at is None
        assert events[0].payload["attempt"] == 1
        assert (await session.get(Payment, payment_id)).status is PaymentStatus.PENDING


async def test_gateway_exhaustion_settles_and_notifies(api, database, payload, monkeypatch):
    gateway = AsyncMock(side_effect=GatewayError("Emulator unavailable"))
    monkeypatch.setattr("app.services.payments.process_payment", gateway)
    payment_id, event_id = await create(api, database, payload)
    notifications = []

    def webhook(request):
        import json

        notifications.append(json.loads(request.content))
        return httpx.Response(204)

    async with httpx.AsyncClient(transport=httpx.MockTransport(webhook)) as client:
        for attempt in range(1, 4):
            await process_event(event_id, client)
            if attempt < 3:
                async with database() as session:
                    retry = await session.scalar(select(Outbox).where(Outbox.consumed_at.is_(None)))
                    event_id = retry.id
        await process_event(event_id, client)
    assert gateway.await_count == 3
    assert len(notifications) == 1
    assert notifications[0]["status"] == "failed"
    async with database() as session:
        payment = await session.get(Payment, payment_id)
        assert payment.status is PaymentStatus.FAILED
        assert payment.processed_at is not None
        assert payment.webhook_delivered_at is not None


async def test_webhook_retry_records_status_without_sensitive_url(
    api, database, payload, monkeypatch
):
    monkeypatch.setattr(
        "app.services.payments.process_payment", AsyncMock(return_value=PaymentStatus.SUCCEEDED)
    )
    _, event_id = await create(api, database, payload)
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(lambda request: httpx.Response(429))
    ) as client:
        await process_event(event_id, client)
    async with database() as session:
        retry = await session.scalar(select(Outbox).where(Outbox.consumed_at.is_(None)))
        assert retry.payload["category"] == "webhook"
        assert retry.payload["http_status"] == 429
        assert "merchant.example" not in str(retry.payload)


async def test_database_failure_after_webhook_preserves_original_event(
    api, database, payload, monkeypatch
):
    from sqlalchemy import text
    from sqlalchemy.exc import IntegrityError

    gateway = AsyncMock(return_value=PaymentStatus.SUCCEEDED)
    monkeypatch.setattr("app.services.payments.process_payment", gateway)
    payment_id, event_id = await create(api, database, payload)
    async with database() as session, session.begin():
        await session.execute(
            text(
                "ALTER TABLE payments ADD CONSTRAINT test_reject_delivery "
                "CHECK (webhook_delivered_at IS NULL)"
            )
        )
    notifications = []

    def webhook(request):
        notifications.append(request.headers["Idempotency-Key"])
        return httpx.Response(204)

    async with httpx.AsyncClient(transport=httpx.MockTransport(webhook)) as client:
        try:
            with pytest.raises(IntegrityError):
                await process_event(event_id, client)
            async with database() as session:
                payment = await session.get(Payment, payment_id)
                assert payment.status is PaymentStatus.SUCCEEDED
                assert payment.webhook_delivered_at is None
                events = list((await session.scalars(select(Outbox))).all())
                assert len(events) == 1
                assert events[0].consumed_at is None
                assert events[0].payload["attempt"] == 1
        finally:
            async with database() as session, session.begin():
                await session.execute(
                    text("ALTER TABLE payments DROP CONSTRAINT test_reject_delivery")
                )
        await process_event(event_id, client)
    gateway.assert_awaited_once()
    assert notifications == [str(payment_id), str(payment_id)]
