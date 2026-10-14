import uuid
from datetime import datetime
from decimal import Decimal
from typing import Annotated, Any

from pydantic import (
    BaseModel,
    BeforeValidator,
    ConfigDict,
    Field,
    HttpUrl,
)

from payments.models import Currency, Payment, PaymentStatus
from payments.money import MINOR_UNITS, from_minor


def _require_string(value: object) -> object:
    # JSON numbers are binary floats in most clients; a string keeps the amount exact end to end.
    if not isinstance(value, str):
        raise ValueError('amount must be a decimal string, e.g. "100.50"')  # noqa: TRY004
    return value


Amount = Annotated[
    Decimal,
    BeforeValidator(_require_string),
    # 17 digits keep the amount in minor units within BIGINT.
    Field(gt=0, max_digits=17, decimal_places=MINOR_UNITS, examples=["100.50"]),
]


class PaymentCreate(BaseModel):
    model_config = ConfigDict(extra="forbid")

    amount: Amount
    currency: Currency
    description: str = Field(min_length=1, max_length=1000)
    metadata: dict[str, Any] = Field(default_factory=dict)
    webhook_url: HttpUrl


class PaymentAccepted(BaseModel):
    payment_id: uuid.UUID
    status: PaymentStatus
    created_at: datetime

    @classmethod
    def from_model(cls, payment: Payment) -> PaymentAccepted:
        return cls(payment_id=payment.id, status=payment.status, created_at=payment.created_at)


class PaymentRead(BaseModel):
    payment_id: uuid.UUID
    amount: Decimal = Field(examples=["100.50"])
    currency: Currency
    description: str
    metadata: dict[str, Any]
    status: PaymentStatus
    webhook_url: str
    created_at: datetime
    processed_at: datetime | None
    webhook_delivered_at: datetime | None

    @classmethod
    def from_model(cls, payment: Payment) -> PaymentRead:
        return cls(
            payment_id=payment.id,
            amount=from_minor(payment.amount_minor),
            currency=payment.currency,
            description=payment.description,
            metadata=payment.metadata_,
            status=payment.status,
            webhook_url=payment.webhook_url,
            created_at=payment.created_at,
            processed_at=payment.processed_at,
            webhook_delivered_at=payment.webhook_delivered_at,
        )


class PaymentCreatedEvent(BaseModel):
    """Body of the `payments.new` message: consumers re-read the payment from the database."""

    payment_id: uuid.UUID
