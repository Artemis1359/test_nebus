from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import Depends, FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse

from app.api.dependencies import authenticate
from app.api.payments import router as payments_router
from app.api.system import router as system_router
from app.core.config import get_settings
from app.core.db import engine
from app.core.logging import configure_logging
from app.core.middleware import RequestLimitsMiddleware


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    configure_logging(get_settings().log_level)
    try:
        yield
    finally:
        await engine.dispose()


app = FastAPI(
    title="Payment processor",
    version="1.0.0",
    lifespan=lifespan,
    description="Асинхронная обработка платежей с уведомлением результата через webhook.",
    redoc_url=None,
    swagger_ui_oauth2_redirect_url=None,
)
app.include_router(payments_router, dependencies=[Depends(authenticate)])
app.include_router(system_router, dependencies=[Depends(authenticate)])
app.add_middleware(RequestLimitsMiddleware)


@app.exception_handler(RequestValidationError)
async def validation_error(request: Request, exc: RequestValidationError) -> JSONResponse:
    # Входные данные могут содержать NaN или секреты; возвращаем только описание ошибок.
    return JSONResponse(
        status_code=422,
        content={
            "detail": [
                {key: error[key] for key in ("loc", "msg", "type")} for error in exc.errors()
            ]
        },
    )
