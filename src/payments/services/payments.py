import hashlib
import json
import uuid
from typing import Any

from sqlalchemy.ext.asyncio import AsyncSession

from payments.messaging.topology import PAYMENT_CREATED_EVENT, PAYMENTS_NEW
from payments.models import Payment
from payments.money import to_minor
from payments.repositories import OutboxRepository, PaymentRepository
from payments.schemas import PaymentCreate, PaymentCreatedEvent


class IdempotencyKeyReusedError(Exception):
    """The idempotency key already belongs to a payment created from a different request."""


def request_hash(fields: dict[str, Any]) -> str:
    canonical = json.dumps(fields, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return hashlib.sha256(canonical.encode()).hexdigest()


class PaymentService:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session
        self._payments = PaymentRepository(session)
        self._outbox = OutboxRepository(session)

    async def create(self, data: PaymentCreate, idempotency_key: str) -> Payment:
        """Create a payment and its outbox event atomically, or return the one made by a retry.

        Raises IdempotencyKeyReusedError if the key was used for a different request.
        """
        fields: dict[str, Any] = {
            "amount_minor": to_minor(data.amount),
            "currency": data.currency.value,
            "description": data.description,
            "metadata": data.metadata,
            "webhook_url": str(data.webhook_url),
        }
        fingerprint = request_hash(fields)

        async with self._session.begin():
            payment = await self._payments.insert_if_absent(
                payment_id=uuid.uuid7(),
                amount_minor=fields["amount_minor"],
                currency=data.currency,
                description=data.description,
                metadata=data.metadata,
                idempotency_key=idempotency_key,
                request_hash=fingerprint,
                webhook_url=fields["webhook_url"],
            )
            if payment is None:
                return await self._existing(idempotency_key, fingerprint)

            event = PaymentCreatedEvent(payment_id=payment.id)
            self._outbox.add(
                event_type=PAYMENT_CREATED_EVENT,
                routing_key=PAYMENTS_NEW,
                payload=event.model_dump(mode="json"),
            )
        return payment

    async def get(self, payment_id: uuid.UUID) -> Payment | None:
        return await self._payments.get(payment_id)

    async def _existing(self, idempotency_key: str, fingerprint: str) -> Payment:
        payment = await self._payments.get_by_idempotency_key(idempotency_key)
        if payment is None:
            # ON CONFLICT waits for the competing transaction, so its row is committed by now.
            raise RuntimeError("payment with a conflicting idempotency key is not visible")
        if payment.request_hash != fingerprint:
            raise IdempotencyKeyReusedError
        return payment
