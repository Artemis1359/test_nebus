import asyncio
import os
import sys


async def test_migration_roundtrip_and_model_parity(database, postgres):
    # Мигрируем только временную БД интеграционных тестов; E2E использует другую базу.
    for command in (("downgrade", "base"), ("upgrade", "head"), ("check",)):
        process = await asyncio.create_subprocess_exec(
            sys.executable,
            "-m",
            "alembic",
            *command,
            env=os.environ | {"DATABASE_URL": postgres.get_connection_url()},
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.STDOUT,
        )
        output, _ = await process.communicate()
        assert process.returncode == 0, output.decode()
