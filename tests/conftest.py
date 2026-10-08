import os
import subprocess
import sys
import time
from contextlib import ExitStack
from pathlib import Path
from types import SimpleNamespace

import httpx
import pytest
import pytest_asyncio
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine
from testcontainers.community.postgres import PostgresContainer
from testcontainers.core.container import DockerContainer
from testcontainers.core.docker_client import DockerClient
from testcontainers.core.image import DockerImage
from testcontainers.core.network import Network
from testcontainers.core.wait_strategies import HttpWaitStrategy, LogMessageWaitStrategy

TEST_API_KEY = "test-api-key-1234567890"
ROOT = Path(__file__).resolve().parents[1]

# При сборе тестов исключаем подключение к рабочей БД. Фикстура database привязывает
# общую фабрику сессий к новому engine в цикле событий текущего теста.
os.environ.update(
    API_KEY=TEST_API_KEY,
    DATABASE_URL="postgresql+asyncpg://payments:payments@localhost:1/payments_test",
    RETRY_BASE_DELAY="2",
    WEBHOOK_TIMEOUT="10",
    OUTBOX_POLL_INTERVAL="0.5",
)

from app.core.db import sessions  # noqa: E402
from app.main import app  # noqa: E402


@pytest.fixture(scope="session")
def docker_environment():
    with pytest.MonkeyPatch.context() as patch:
        # docker-py не читает активный контекст Docker CLI самостоятельно.
        if not os.getenv("DOCKER_HOST"):
            endpoint = subprocess.check_output(
                ["docker", "context", "inspect", "--format", "{{.Endpoints.docker.Host}}"],
                text=True,
            ).strip()
            patch.setenv("DOCKER_HOST", endpoint)
        if sys.platform == "darwin" and not os.getenv("TESTCONTAINERS_DOCKER_SOCKET_OVERRIDE"):
            patch.setenv("TESTCONTAINERS_DOCKER_SOCKET_OVERRIDE", "/var/run/docker.sock")

        # Docker Desktop может сообщить о запуске до публикации случайных портов.
        # Testcontainers не повторяет поиск порта, в том числе при запуске Ryuk.
        original_port = DockerClient.port

        def published_port(client, container_id, port):
            deadline = time.monotonic() + 10
            while True:
                try:
                    return original_port(client, container_id, port)
                except ConnectionError:
                    if time.monotonic() >= deadline:
                        raise
                    time.sleep(0.05)

        patch.setattr(DockerClient, "port", published_port)
        yield


@pytest.fixture(scope="session")
def test_network(docker_environment):
    with Network() as network:
        yield network


@pytest.fixture(scope="session")
def postgres(test_network):
    with (
        PostgresContainer(
            "postgres:16-alpine",
            username="payments",
            password="payments",
            dbname="payments_test",
            driver="asyncpg",
        )
        .with_network(test_network)
        .with_network_aliases("postgres")
    ) as container:
        yield container


@pytest.fixture(scope="session")
def migrated_database(postgres):
    subprocess.run(
        [sys.executable, "-m", "alembic", "upgrade", "head"],
        cwd=ROOT,
        env=os.environ | {"DATABASE_URL": postgres.get_connection_url()},
        check=True,
    )


@pytest_asyncio.fixture
async def database(postgres, migrated_database):
    engine = create_async_engine(postgres.get_connection_url(), hide_parameters=True)
    original_bind = sessions.kw["bind"]
    sessions.configure(bind=engine)
    try:
        async with engine.begin() as connection:
            await connection.execute(text("TRUNCATE outbox, payments CASCADE"))
        yield sessions
    finally:
        sessions.configure(bind=original_bind)
        await engine.dispose()


@pytest_asyncio.fixture
async def api(database):
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url="http://test",
        headers={"X-API-Key": TEST_API_KEY},
    ) as client:
        yield client


@pytest.fixture(scope="session")
def e2e_stack(postgres, test_network):
    # Миграции и TRUNCATE в тестах не должны затрагивать БД работающего consumer.
    result = postgres.exec(
        [
            "psql",
            "-U",
            "payments",
            "-d",
            "postgres",
            "-v",
            "ON_ERROR_STOP=1",
            "-c",
            "CREATE DATABASE payments_e2e_test",
        ]
    )
    assert result.exit_code == 0, result.output.decode()
    with ExitStack() as stack:
        # Сохраняем образ и кеш сборки; контейнеры, сеть и данные — временные.
        image = stack.enter_context(
            DockerImage(
                path=ROOT,
                tag="payment-processor-tests:local",
                clean_up=False,
            )
        )
        rabbit = stack.enter_context(
            DockerContainer("rabbitmq:3.13-management-alpine")
            .with_network(test_network)
            .with_network_aliases("rabbitmq")
            .with_envs(
                RABBITMQ_DEFAULT_USER="payments",
                RABBITMQ_DEFAULT_PASS="payments",
                RABBITMQ_DEFAULT_VHOST="payments_test",
                RABBITMQ_SERVER_ADDITIONAL_ERL_ARGS="+S 2:2",
                RABBITMQ_CTL_ERL_ARGS="+S 1:1",
            )
            .with_exposed_ports(15672)
            .waiting_for(HttpWaitStrategy(15672).for_status_code(200))
        )
        environment = {
            "API_KEY": TEST_API_KEY,
            "DATABASE_URL": "postgresql+asyncpg://payments:payments@postgres:5432/payments_e2e_test",
            "RABBITMQ_URL": "amqp://payments:payments@rabbitmq:5672/payments_test",
            "RETRY_BASE_DELAY": "2",
            "WEBHOOK_TIMEOUT": "10",
            "OUTBOX_POLL_INTERVAL": "0.5",
        }
        api_container = stack.enter_context(
            DockerContainer(str(image))
            .with_network(test_network)
            .with_envs(**environment)
            .with_exposed_ports(8000)
            .with_kwargs(init=True)
            .with_command(
                [
                    "sh",
                    "-c",
                    "alembic upgrade head && exec uvicorn app.main:app --host 0.0.0.0 --port 8000",
                ]
            )
            .waiting_for(
                HttpWaitStrategy(8000, path="/health")
                .with_header("X-API-Key", TEST_API_KEY)
                .for_status_code(200)
            )
        )
        consumer = stack.enter_context(
            DockerContainer(str(image))
            .with_network(test_network)
            .with_envs(**environment)
            .with_kwargs(init=True, extra_hosts={"host.docker.internal": "host-gateway"})
            .with_command(["faststream", "run", "app.workers.consumer:app"])
            .waiting_for(LogMessageWaitStrategy("FastStream app started successfully"))
        )
        yield SimpleNamespace(
            api_url=f"http://{api_container.get_container_host_ip()}:{api_container.get_exposed_port(8000)}",
            management_url=f"http://{rabbit.get_container_host_ip()}:{rabbit.get_exposed_port(15672)}",
            api_key=TEST_API_KEY,
            consumer=consumer,
            postgres=postgres,
        )


@pytest.fixture
def payload():
    return {
        "amount": "100.50",
        "currency": "RUB",
        "description": "Order 42",
        "metadata": {"order_id": 42},
        "webhook_url": "https://merchant.example/webhook",
    }
