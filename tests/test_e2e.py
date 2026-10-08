"""Сквозные тесты с временными контейнерами и настоящими HTTP-уведомлениями."""

import asyncio
import json
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from uuid import uuid4

import httpx
import pytest

pytestmark = pytest.mark.e2e


def _wait_for_consumer(client: httpx.Client, *, timeout: float = 30) -> None:
    deadline = time.monotonic() + timeout
    last_state = "RabbitMQ management is unavailable"
    while True:
        try:
            main = client.get("/api/queues/payments_test/payments.new")
            dead = client.get("/api/queues/payments_test/payments.dlq")
            if main.status_code == dead.status_code == 200:
                consumers = main.json().get("consumers", 0)
                if consumers > 0:
                    return
                last_state = "Queues exist, but payments.new has no active consumer"
            else:
                last_state = (
                    f"Queue status: payments.new={main.status_code}, "
                    f"payments.dlq={dead.status_code}"
                )
                if main.status_code in (401, 403) or dead.status_code in (401, 403):
                    pytest.fail(f"RabbitMQ management authentication failed. {last_state}")
        except httpx.TransportError as exc:
            last_state = f"RabbitMQ management: {type(exc).__name__}"
        if time.monotonic() >= deadline:
            pytest.fail(
                f"E2E consumer did not become ready within {timeout:g}s. {last_state}. "
                "Check the Testcontainers startup logs in pytest output"
            )
        time.sleep(0.2)


@pytest.fixture(scope="module", autouse=True)
def ready_consumer(e2e_stack):
    with httpx.Client(
        base_url=e2e_stack.management_url, auth=("payments", "payments"), timeout=2
    ) as client:
        _wait_for_consumer(client)


@pytest.fixture
def receiver():
    calls = []
    statuses = []
    received = threading.Event()
    release = threading.Event()
    release.set()

    class Handler(BaseHTTPRequestHandler):
        def do_POST(self):
            body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
            calls.append((time.monotonic(), body, self.headers["Idempotency-Key"]))
            received.set()
            if not release.wait(timeout=15):
                self.send_error(504)
                return
            self.send_response(statuses.pop(0) if len(statuses) > 1 else statuses[0])
            self.end_headers()

        def log_message(self, *args):
            pass

    server = ThreadingHTTPServer(("0.0.0.0", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    yield (
        f"http://host.docker.internal:{server.server_port}/webhook",
        calls,
        statuses,
        release,
        received,
    )
    release.set()
    server.shutdown()
    server.server_close()
    thread.join()


@pytest.mark.parametrize("statuses,expected_attempts", [([204], 1), ([500, 200], 2), ([500], 3)])
async def test_real_payment_pipeline(e2e_stack, receiver, statuses, expected_attempts):
    url, calls, server_statuses, _, _ = receiver
    server_statuses.extend(statuses)
    async with httpx.AsyncClient(
        base_url=e2e_stack.api_url,
        headers={"X-API-Key": e2e_stack.api_key},
    ) as api:
        response = await api.post(
            "/api/v1/payments",
            headers={"Idempotency-Key": str(uuid4())},
            json={"amount": "12.34", "currency": "EUR", "description": "E2E", "webhook_url": url},
        )
        assert response.status_code == 202, response.text
        payment_id = response.json()["payment_id"]
        deadline = time.monotonic() + 25
        while len(calls) < expected_attempts and time.monotonic() < deadline:  # noqa: ASYNC110
            await asyncio.sleep(0.2)
        assert len(calls) == expected_attempts
        result = (await api.get(f"/api/v1/payments/{payment_id}")).json()
        assert result["status"] in {"succeeded", "failed"}
        assert result["processed_at"] is not None
        assert all(body == result and key == payment_id for _, body, key in calls)
        if expected_attempts >= 2:
            assert calls[1][0] - calls[0][0] >= 2
        if expected_attempts == 3:
            assert calls[2][0] - calls[1][0] >= 4
            async with httpx.AsyncClient(
                base_url=e2e_stack.management_url, auth=("payments", "payments")
            ) as management:
                while time.monotonic() < deadline:
                    messages = (
                        await management.post(
                            "/api/queues/payments_test/payments.dlq/get",
                            json={"count": 100, "ackmode": "ack_requeue_false", "encoding": "auto"},
                        )
                    ).json()
                    if any(
                        m["properties"].get("headers", {}).get("payment_id") == payment_id
                        for m in messages
                    ):
                        break
                    await asyncio.sleep(0.2)
                else:
                    pytest.fail("Final failure not delivered to the real DLQ")
        await asyncio.sleep(1)
        assert len(calls) == expected_attempts


@pytest.mark.parametrize(
    "body",
    [
        "not-json",
        "[]",
        '{"event_id":"invalid"}',
        '{"event_id":123}',
        '{"event_id":null}',
        None,
        "[" * 10000 + "]" * 10000,
        "x" * (64 * 1024 + 1),
    ],
    ids=[
        "invalid-json",
        "array",
        "invalid-uuid",
        "integer-id",
        "null-id",
        "unknown-event",
        "deep-json",
        "oversized",
    ],
)
async def test_poison_message_dead_lettered(e2e_stack, body):
    marker = str(uuid4())
    if body is None:
        body = json.dumps({"event_id": str(uuid4())})
    async with httpx.AsyncClient(
        base_url=e2e_stack.management_url, auth=("payments", "payments")
    ) as management:
        result = await management.post(
            "/api/exchanges/payments_test/payments/publish",
            json={
                "properties": {
                    "delivery_mode": 2,
                    "message_id": marker,
                    "content_type": "application/json",
                },
                "routing_key": "payments.new",
                "payload": body,
                "payload_encoding": "string",
            },
        )
        assert result.json()["routed"] is True
        for _ in range(30):
            messages = (
                await management.post(
                    "/api/queues/payments_test/payments.dlq/get",
                    json={"count": 100, "ackmode": "ack_requeue_false", "encoding": "auto"},
                )
            ).json()
            if any(m["properties"].get("message_id") == marker for m in messages):
                break
            await asyncio.sleep(0.2)
        else:
            pytest.fail("Poison message was not dead-lettered")


async def test_sigterm_waits_for_webhook_and_commits_before_closing(e2e_stack, receiver):
    url, calls, statuses, release, received = receiver
    statuses.append(204)
    release.clear()
    stop = None
    async with httpx.AsyncClient(
        base_url=e2e_stack.api_url,
        headers={"X-API-Key": e2e_stack.api_key},
    ) as api:
        try:
            response = await api.post(
                "/api/v1/payments",
                headers={"Idempotency-Key": str(uuid4())},
                json={
                    "amount": "1.00",
                    "currency": "RUB",
                    "description": "SIGTERM",
                    "webhook_url": url,
                },
            )
            assert response.status_code == 202
            payment_id = response.json()["payment_id"]
            assert await asyncio.to_thread(received.wait, 10), "Webhook did not start"
            container = e2e_stack.consumer.get_wrapped_container()
            stop = asyncio.create_task(asyncio.to_thread(container.stop, timeout=30))
            await asyncio.sleep(0.5)
            assert not stop.done(), "Consumer exited while the webhook was still running"
            release.set()
            await stop
            delivered = await asyncio.to_thread(
                e2e_stack.postgres.exec,
                [
                    "psql",
                    "-U",
                    "payments",
                    "-d",
                    "payments_e2e_test",
                    "-Atc",
                    "SELECT webhook_delivered_at IS NOT NULL FROM payments "
                    f"WHERE id = '{payment_id}'",
                ],
            )
            assert delivered.exit_code == 0, delivered.output.decode()
            assert delivered.output.decode().strip() == "t"
            await asyncio.to_thread(container.reload)
            assert container.attrs["State"]["ExitCode"] == 0

        finally:
            release.set()
            if stop is not None:
                await stop
            await asyncio.to_thread(e2e_stack.consumer.get_wrapped_container().start)
            async with httpx.AsyncClient(
                base_url=e2e_stack.management_url, auth=("payments", "payments")
            ) as management:
                async with asyncio.timeout(15):
                    while True:  # noqa: ASYNC110
                        queue = await management.get("/api/queues/payments_test/payments.new")
                        if queue.status_code == 200 and queue.json().get("consumers", 0) > 0:
                            break
                        await asyncio.sleep(0.2)
        await asyncio.sleep(1)
        assert len(calls) == 1, "Completed delivery must not be repeated after restart"
