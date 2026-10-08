import json
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest
from faststream.exceptions import NackMessage
from sqlalchemy.exc import OperationalError

from app.workers import consumer


@pytest.mark.parametrize("db_timeout", [False, True])
async def test_database_failure_preserves_original_delivery(monkeypatch, db_timeout):
    database_error = OperationalError("SELECT outbox", {}, RuntimeError("database unavailable"))
    if db_timeout:
        database_error = TimeoutError("DB command timed out")
    process = AsyncMock(side_effect=database_error)
    delay = AsyncMock()
    client = object()
    monkeypatch.setattr(consumer, "process_event", process)
    monkeypatch.setattr(consumer.asyncio, "sleep", delay)
    event_id = uuid4()

    with pytest.raises(NackMessage) as result:
        await consumer.consume(json.dumps({"event_id": str(event_id)}).encode(), client)

    assert result.value.extra_options == {"requeue": True}
    assert result.value.__cause__ is database_error
    process.assert_awaited_once_with(event_id, client)
    delay.assert_awaited_once_with(consumer.get_settings().retry_base_delay)


@pytest.mark.parametrize("outcome", ["success", "invalid_event", "database_failure"])
async def test_faststream_acknowledges_according_to_handler_outcome(monkeypatch, outcome):
    from faststream.rabbit import TestRabbitBroker
    from faststream.rabbit.message import RabbitMessage

    from app.exceptions.payment import InvalidEvent

    client = object()
    process = AsyncMock()
    if outcome == "invalid_event":
        process.side_effect = InvalidEvent("Unknown event")
    elif outcome == "database_failure":
        process.side_effect = OperationalError("SELECT outbox", {}, RuntimeError("DB unavailable"))
    monkeypatch.setattr(consumer, "process_event", process)
    monkeypatch.setattr(consumer.asyncio, "sleep", AsyncMock())
    ack, nack, reject = AsyncMock(), AsyncMock(), AsyncMock()
    monkeypatch.setattr(RabbitMessage, "ack", ack)
    monkeypatch.setattr(RabbitMessage, "nack", nack)
    monkeypatch.setattr(RabbitMessage, "reject", reject)

    with consumer.app.context.scope("webhook_client", client):
        async with TestRabbitBroker(consumer.broker):
            await consumer.broker.publish(
                {"event_id": str(uuid4())},
                queue=consumer.queue,
                exchange=consumer.exchange,
            )

    process.assert_awaited_once()
    assert process.call_args.args[1] is client
    if outcome == "success":
        ack.assert_awaited_once()
        nack.assert_not_awaited()
        reject.assert_not_awaited()
    elif outcome == "invalid_event":
        reject.assert_awaited_once_with(requeue=False)
        ack.assert_not_awaited()
        nack.assert_not_awaited()
    else:
        nack.assert_awaited_once_with(requeue=True)
        ack.assert_not_awaited()
        reject.assert_not_awaited()


async def test_framework_does_not_ack_before_processing_completes(monkeypatch):
    import asyncio

    from faststream.rabbit import TestRabbitBroker
    from faststream.rabbit.message import RabbitMessage

    entered, committed = asyncio.Event(), asyncio.Event()

    async def process(event_id, client):
        entered.set()
        await committed.wait()

    monkeypatch.setattr(consumer, "process_event", process)
    ack = AsyncMock()
    monkeypatch.setattr(RabbitMessage, "ack", ack)
    with consumer.app.context.scope("webhook_client", object()):
        async with TestRabbitBroker(consumer.broker):
            delivery = asyncio.create_task(
                consumer.broker.publish(
                    {"event_id": str(uuid4())},
                    queue=consumer.queue,
                    exchange=consumer.exchange,
                )
            )
            try:
                await asyncio.wait_for(entered.wait(), timeout=2)
                ack.assert_not_awaited()
            finally:
                committed.set()
                await asyncio.wait_for(delivery, timeout=2)
    ack.assert_awaited_once()


async def test_lifespan_closes_resources_when_startup_fails(monkeypatch):
    from unittest.mock import MagicMock

    client = object()
    context = AsyncMock()
    context.__aenter__.return_value = client
    factory = MagicMock(return_value=context)
    dispose = AsyncMock()
    monkeypatch.setattr(consumer.httpx, "AsyncClient", factory)
    monkeypatch.setattr(consumer, "engine", MagicMock(dispose=dispose))
    monkeypatch.setattr(consumer, "configure_logging", MagicMock())
    with pytest.raises(RuntimeError, match="startup failed"):
        async with consumer.lifespan():
            assert consumer.app.context.get("webhook_client") is client
            raise RuntimeError("startup failed")
    context.__aexit__.assert_awaited_once()
    dispose.assert_awaited_once()
    assert consumer.app.context.get("webhook_client") is None


@pytest.mark.parametrize("failed", [False, True])
async def test_outbox_shutdown_clears_task_even_if_publisher_failed(monkeypatch, failed):
    import asyncio

    async def publisher():
        if failed:
            raise RuntimeError("publisher failed")
        await asyncio.Event().wait()

    monkeypatch.setattr(consumer, "run_outbox", publisher)
    await consumer.start_publisher()
    task = consumer.app.context.get("outbox_task")
    await asyncio.sleep(0)
    await consumer.stop_outbox()
    assert task.done()
    assert consumer.app.context.get("outbox_task") is None
    await consumer.stop_outbox()


@pytest.mark.parametrize("body", [b"[" * 10000 + b"]" * 10000, b"x" * (64 * 1024 + 1)])
async def test_oversized_and_recursive_messages_are_rejected(monkeypatch, body):
    from faststream.exceptions import RejectMessage

    process = AsyncMock()
    monkeypatch.setattr(consumer, "process_event", process)
    with pytest.raises(RejectMessage) as result:
        await consumer.consume(body, object())
    assert result.value.extra_options == {"requeue": False}
    process.assert_not_awaited()


async def test_programming_error_does_not_loop_forever(monkeypatch):
    from faststream.exceptions import RejectMessage

    monkeypatch.setattr(consumer, "process_event", AsyncMock(side_effect=TypeError("bug")))
    with pytest.raises(RejectMessage):
        await consumer.consume(json.dumps({"event_id": str(uuid4())}).encode(), object())
