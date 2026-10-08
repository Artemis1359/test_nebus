from datetime import datetime
from decimal import Decimal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, HttpUrl, JsonValue, field_validator

from app.core.validation import validate_metadata, validate_text
from app.enums.payment import Currency, PaymentStatus
from app.models import Payment


class PaymentCreate(BaseModel):
    model_config = ConfigDict(
        extra="forbid",
        allow_inf_nan=False,
        json_schema_extra={
            "examples": [
                {
                    "amount": "100.50",
                    "currency": "RUB",
                    "description": "Оплата заказа №42",
                    "metadata": {"order_id": "42"},
                    "webhook_url": "https://merchant.example/webhooks/payments",
                }
            ]
        },
    )

    amount: Decimal = Field(
        gt=0,
        max_digits=18,
        decimal_places=2,
        description="Положительная сумма, до 2 знаков после запятой",
    )
    currency: Currency
    description: str = Field(max_length=1000)
    metadata: dict[str, JsonValue] = Field(
        default_factory=dict,
        description="Дополнительные данные: до 16 KiB и 32 уровней вложенности",
    )
    webhook_url: HttpUrl = Field(
        max_length=2048, description="HTTP(S) URL уведомления, без credentials и fragment"
    )

    @field_validator("description")
    @classmethod
    def valid_description(cls, value: str) -> str:
        return validate_text(value)

    @field_validator("metadata")
    @classmethod
    def valid_metadata(cls, value: dict[str, JsonValue]) -> dict[str, JsonValue]:
        validate_metadata(value)
        return value

    @field_validator("amount")
    @classmethod
    def normalize_amount(cls, value: Decimal) -> Decimal:
        return value.quantize(Decimal("0.01"))

    @field_validator("webhook_url")
    @classmethod
    def no_credentials(cls, value: HttpUrl) -> HttpUrl:
        if value.username or value.password or value.fragment:
            raise ValueError("Webhook URL must not contain credentials or a fragment")
        return value


class PaymentAccepted(BaseModel):
    payment_id: UUID
    status: PaymentStatus
    created_at: datetime


class PaymentDetail(PaymentAccepted):
    amount: Decimal = Field(
        description="Сумма платежа в виде строки с двумя знаками после запятой",
        json_schema_extra={"example": "100.50"},
    )
    currency: Currency
    description: str
    metadata: dict[str, JsonValue]
    webhook_url: str
    processed_at: datetime | None

    @classmethod
    def from_payment(cls, payment: Payment) -> "PaymentDetail":
        return cls(
            payment_id=payment.id,
            status=payment.status,
            created_at=payment.created_at,
            amount=payment.amount,
            currency=payment.currency,
            description=payment.description,
            metadata=payment.metadata_,
            webhook_url=payment.webhook_url,
            processed_at=payment.processed_at,
        )
