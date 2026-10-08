from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock

import pytest
from sqlalchemy import select

from app.models import Outbox
from app.workers.outbox import publish_one


async def test_confirm_before_marking_and_retry_after_publish_failure(
    api, database, payload, monkeypatch
):
    await api.post("/api/v1/payments", json=payload, headers={"Idempotency-Key": "publish"})
    publish = AsyncMock(side_effect=RuntimeError("broker unavailable"))
    monkeypatch.setattr("app.workers.outbox.broker.publish", publish)
    with pytest.raises(RuntimeError):
        await publish_one()
    async with database() as session:
        event = await session.scalar(select(Outbox))
        assert event.published_at is None
    publish.side_effect = None
    assert await publish_one() is True
    assert await publish_one() is False
    async with database() as session:
        assert (await session.get(Outbox, event.id)).published_at is not None
    assert publish.call_args.kwargs["persist"] is True
    assert publish.call_args.kwargs["mandatory"] is True
    assert publish.call_args.kwargs["message_id"] == str(event.id)


async def test_scheduled_retry_not_published_early(api, database, payload, monkeypatch):
    await api.post("/api/v1/payments", json=payload, headers={"Idempotency-Key": "later"})
    async with database() as session, session.begin():
        event = await session.scalar(select(Outbox))
        event.available_at = datetime.now(UTC) + timedelta(minutes=1)
    publish = AsyncMock()
    monkeypatch.setattr("app.workers.outbox.broker.publish", publish)
    assert await publish_one() is False
    publish.assert_not_awaited()


async def test_parallel_publishers_skip_locked(api, database, payload, monkeypatch):
    import asyncio

    await api.post("/api/v1/payments", json=payload, headers={"Idempotency-Key": "parallel"})
    entered = asyncio.Event()
    release = asyncio.Event()

    async def blocked_publish(*args, **kwargs):
        entered.set()
        await release.wait()

    publish = AsyncMock(side_effect=blocked_publish)
    monkeypatch.setattr("app.workers.outbox.broker.publish", publish)
    first = asyncio.create_task(publish_one())
    try:
        await asyncio.wait_for(entered.wait(), timeout=2)
        assert await publish_one() is False
    finally:
        release.set()
        await first
    publish.assert_awaited_once()
