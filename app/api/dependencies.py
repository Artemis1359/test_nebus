import secrets
from typing import Annotated

from fastapi import Depends, HTTPException
from fastapi.security import APIKeyHeader
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import get_settings
from app.core.db import get_session

Session = Annotated[AsyncSession, Depends(get_session)]


async def authenticate(
    key: Annotated[
        str | None,
        Depends(
            APIKeyHeader(
                name="X-API-Key",
                scheme_name="ApiKeyAuth",
                description="Статический API-ключ из настройки API_KEY.",
                auto_error=False,
            )
        ),
    ],
) -> None:
    if key is None or not secrets.compare_digest(
        key.encode(), get_settings().api_key.get_secret_value().encode()
    ):
        raise HTTPException(status_code=401, detail="Invalid API key")
