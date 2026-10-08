from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Header, HTTPException, Response

from app.api.dependencies import Session
from app.exceptions.payment import IdempotencyConflict, PaymentNotFound
from app.schemas.payments import PaymentAccepted, PaymentCreate, PaymentDetail
from app.services import payments

router = APIRouter(prefix="/api/v1/payments", tags=["payments"])


@router.post(
    "",
    response_model=PaymentAccepted,
    status_code=202,
    summary="Создать платёж",
    description=(
        "Сохраняет платёж и событие обработки в одной транзакции. "
        "Обработка выполняется асинхронно; результат отправляется на webhook_url. "
        "Повтор с тем же Idempotency-Key и нормализованным телом возвращает "
        "тот же платёж и его текущий статус. Другое тело с тем же ключом даёт 409. "
        "Максимальный размер тела запроса — 64 KiB."
    ),
    responses={
        202: {
            "description": "Платёж принят или найден по ключу идемпотентности",
            "headers": {
                "Location": {"schema": {"type": "string"}, "description": "URL получения платежа"}
            },
        },
        401: {"description": "API-ключ отсутствует или неверен"},
        409: {"description": "Ключ идемпотентности уже использован с другим телом"},
        413: {"description": "Тело запроса превышает 64 KiB"},
        422: {"description": "Некорректное тело запроса или Idempotency-Key"},
    },
)
async def create_payment(
    body: PaymentCreate,
    response: Response,
    session: Session,
    idempotency_key: Annotated[
        str,
        Header(
            min_length=1,
            max_length=255,
            description="Ключ, выбранный клиентом. Сохраняйте его при повторе запроса.",
            examples=["order-42"],
        ),
    ],
) -> PaymentAccepted:
    if not idempotency_key.strip():
        raise HTTPException(status_code=422, detail="Idempotency-Key must not be blank")
    try:
        payment = await payments.create_payment(session, body, idempotency_key)
    except IdempotencyConflict as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    response.headers["Location"] = f"/api/v1/payments/{payment.id}"
    return PaymentAccepted(
        payment_id=payment.id, status=payment.status, created_at=payment.created_at
    )


@router.get(
    "/{payment_id}",
    response_model=PaymentDetail,
    summary="Получить платёж",
    description="Возвращает текущий статус и данные платежа. До обработки processed_at равен null.",
    responses={
        401: {"description": "API-ключ отсутствует или неверен"},
        404: {"description": "Платёж не найден"},
        422: {"description": "Некорректный UUID платежа"},
    },
)
async def get_payment(payment_id: UUID, session: Session) -> PaymentDetail:
    try:
        payment = await payments.get_payment(session, payment_id)
    except PaymentNotFound as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    return PaymentDetail.from_payment(payment)
