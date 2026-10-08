from unittest.mock import AsyncMock

import pytest

from app.enums.payment import PaymentStatus
from app.integrations.gateway import process_payment


@pytest.mark.parametrize(
    "draw,expected",
    [(0.0, "succeeded"), (0.89999, "succeeded"), (0.9, "failed"), (0.999, "failed")],
)
async def test_gateway_probability_and_delay(monkeypatch, draw, expected):
    delay = AsyncMock()
    monkeypatch.setattr("app.integrations.gateway.asyncio.sleep", delay)
    monkeypatch.setattr(
        "app.integrations.gateway.random.uniform", lambda low, high: (low + high) / 2
    )
    monkeypatch.setattr("app.integrations.gateway.random.random", lambda: draw)
    assert await process_payment() is PaymentStatus(expected)
    delay.assert_awaited_once_with(3.5)
