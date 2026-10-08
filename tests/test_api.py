import asyncio
from uuid import uuid4

import pytest
from sqlalchemy import func, select, text
from sqlalchemy.exc import IntegrityError

from app.enums.payment import Currency
from app.models import Outbox, Payment


async def counts(database):
    async with database() as session:
        return tuple(
            [
                await session.scalar(select(func.count()).select_from(model))
                for model in (Payment, Outbox)
            ]
        )


async def test_create_and_get(api, database, payload):
    result = await api.post(
        "/api/v1/payments", json=payload, headers={"Idempotency-Key": "order42"}
    )
    assert result.status_code == 202
    assert result.json()["status"] == "pending"
    payment = await api.get(result.headers["Location"])
    assert payment.status_code == 200
    assert payment.json() | {"payment_id": "ignored", "created_at": "ignored"} == {
        **payload,
        "amount": "100.50",
        "status": "pending",
        "processed_at": None,
        "payment_id": "ignored",
        "created_at": "ignored",
    }
    assert await counts(database) == (1, 1)
    async with database() as session:
        saved = await session.scalar(select(Payment))
        assert saved.currency is Currency.RUB


async def test_concurrent_idempotency(api, database, payload):
    responses = await asyncio.gather(
        *[
            api.post("/api/v1/payments", json=payload, headers={"Idempotency-Key": "same"})
            for _ in range(12)
        ]
    )
    assert all(r.status_code == 202 for r in responses)
    assert len({r.json()["payment_id"] for r in responses}) == 1
    assert await counts(database) == (1, 1)
    # Формат суммы и порядок ключей JSON не должны менять хеш запроса.
    repeat = await api.post(
        "/api/v1/payments", json={**payload, "amount": 100.5}, headers={"Idempotency-Key": "same"}
    )
    assert repeat.status_code == 202
    conflict = await api.post(
        "/api/v1/payments",
        json={**payload, "amount": "101.50"},
        headers={"Idempotency-Key": "same"},
    )
    assert conflict.status_code == 409
    assert await counts(database) == (1, 1)


@pytest.mark.parametrize(
    "path", ["/health", "/api/v1/payments", "/api/v1/payments/" + str(uuid4())]
)
async def test_auth(api, path):
    api.headers.clear()
    method = api.post if path == "/api/v1/payments" else api.get
    assert (await method(path)).status_code == 401
    assert (await method(path, headers={"X-API-Key": "wrong"})).status_code == 401


@pytest.mark.parametrize(
    "changes",
    [
        {"amount": "0"},
        {"amount": "-1"},
        {"amount": "1.001"},
        {"amount": "NaN"},
        {"amount": "10000000000000000"},
        {"currency": "GBP"},
        {"webhook_url": "ftp://a.com"},
        {"webhook_url": "https://user:pass@example.com"},
        {"metadata": []},
        {"unexpected": 1},
    ],
)
async def test_invalid_payload(api, database, payload, changes):
    response = await api.post(
        "/api/v1/payments", json={**payload, **changes}, headers={"Idempotency-Key": "invalid"}
    )
    assert response.status_code == 422
    assert await counts(database) == (0, 0)


@pytest.mark.parametrize(
    "changes",
    [
        {"description": "bad\x00text"},
        {"metadata": {"nested": ["\x00"]}},
        {"metadata": {"\x00": "value"}},
        {"metadata": {"value": "\ud800"}},
        {"metadata": {"value": "x" * (16 * 1024)}},
    ],
)
async def test_postgresql_incompatible_or_large_metadata(api, database, payload, changes):
    import json

    response = await api.post(
        "/api/v1/payments",
        content=json.dumps({**payload, **changes}),
        headers={"Content-Type": "application/json", "Idempotency-Key": "invalid-text"},
    )
    assert response.status_code == 422
    assert await counts(database) == (0, 0)


async def test_body_limit_applies_without_content_length(api, database):
    async def body():
        for _ in range(65):
            yield b"x" * 1024

    response = await api.post(
        "/api/v1/payments", content=body(), headers={"Idempotency-Key": "large"}
    )
    assert response.status_code == 413
    assert await counts(database) == (0, 0)


@pytest.mark.parametrize("depth", [40, 10000])
@pytest.mark.parametrize("content_type", [None, "application/json", "Application/JSON"])
async def test_deep_json_rejected_before_framework_decoding(api, database, depth, content_type):
    headers = {"Idempotency-Key": "deep"}
    if content_type:
        headers["Content-Type"] = content_type
    response = await api.post(
        "/api/v1/payments",
        content="[" * depth + "]" * depth,
        headers=headers,
    )
    assert response.status_code == 422
    assert await counts(database) == (0, 0)


@pytest.mark.parametrize("key", [None, "", "   ", "a" * 256])
async def test_invalid_idempotency_key(api, payload, key):
    headers = {} if key is None else {"Idempotency-Key": key}
    assert (await api.post("/api/v1/payments", json=payload, headers=headers)).status_code == 422


async def test_not_found_and_invalid_uuid(api):
    assert (await api.get(f"/api/v1/payments/{uuid4()}")).status_code == 404
    assert (await api.get("/api/v1/payments/invalid")).status_code == 422


async def test_payment_rolled_back_if_outbox_flush_fails(api, database, payload):
    # Вызываем ошибку записи outbox при завершающем flush транзакции.
    async with database() as session, session.begin():
        await session.execute(
            text("ALTER TABLE outbox ADD CONSTRAINT test_reject_outbox CHECK (false)")
        )
    try:
        with pytest.raises(IntegrityError):
            await api.post("/api/v1/payments", json=payload, headers={"Idempotency-Key": "atomic"})
        assert await counts(database) == (0, 0)
    finally:
        async with database() as session, session.begin():
            await session.execute(text("ALTER TABLE outbox DROP CONSTRAINT test_reject_outbox"))
    # Откат транзакции должен освобождать ключ идемпотентности клиента.
    response = await api.post(
        "/api/v1/payments", json=payload, headers={"Idempotency-Key": "atomic"}
    )
    assert response.status_code == 202
    assert await counts(database) == (1, 1)


@pytest.mark.parametrize("value", ["NaN", "Infinity", "-Infinity"])
async def test_nonfinite_metadata_is_rejected(api, database, value):
    body = (
        '{"amount":"1.00","currency":"RUB","description":"JSON validation",'
        '"webhook_url":"http://merchant.example/webhook","metadata":{"nested":[' + value + "]}}"
    )
    result = await api.post(
        "/api/v1/payments",
        content=body,
        headers={"Content-Type": "application/json", "Idempotency-Key": "nonfinite"},
    )
    assert result.status_code == 422
    assert await counts(database) == (0, 0)
