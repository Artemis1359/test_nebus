from fastapi import APIRouter
from sqlalchemy import select

from app.api.dependencies import Session

router = APIRouter()


@router.get("/health", include_in_schema=False)
async def health(session: Session) -> dict[str, str]:
    await session.execute(select(1))
    return {"status": "ok"}
