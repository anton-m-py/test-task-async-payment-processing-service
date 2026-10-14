import uuid
from typing import Annotated

from fastapi import APIRouter, Depends, Header, HTTPException, status
from sqlalchemy import text

from payments.api.dependencies import PaymentServiceDep, SessionDep, require_api_key
from payments.schemas import PaymentAccepted, PaymentCreate, PaymentRead
from payments.services.payments import IdempotencyKeyReusedError

payments_router = APIRouter(
    prefix="/api/v1/payments",
    tags=["payments"],
    dependencies=[Depends(require_api_key)],
    responses={status.HTTP_401_UNAUTHORIZED: {"description": "Invalid or missing API key"}},
)


@payments_router.post(
    "",
    status_code=status.HTTP_202_ACCEPTED,
    responses={
        status.HTTP_409_CONFLICT: {
            "description": "The Idempotency-Key was already used for a different request",
        },
    },
)
async def create_payment(
    body: PaymentCreate,
    service: PaymentServiceDep,
    idempotency_key: Annotated[
        str,
        Header(
            alias="Idempotency-Key",
            min_length=1,
            max_length=255,
            description="Repeating a request with the same key returns the original payment.",
        ),
    ],
) -> PaymentAccepted:
    try:
        payment = await service.create(body, idempotency_key)
    except IdempotencyKeyReusedError as exc:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="Idempotency-Key was already used with a different request body",
        ) from exc
    return PaymentAccepted.from_model(payment)


@payments_router.get(
    "/{payment_id}",
    responses={status.HTTP_404_NOT_FOUND: {"description": "Payment not found"}},
)
async def get_payment(payment_id: uuid.UUID, service: PaymentServiceDep) -> PaymentRead:
    payment = await service.get(payment_id)
    if payment is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Payment not found")
    return PaymentRead.from_model(payment)


health_router = APIRouter(tags=["health"])


@health_router.get("/health")
async def health(session: SessionDep) -> dict[str, str]:
    await session.execute(text("SELECT 1"))
    return {"status": "ok"}
