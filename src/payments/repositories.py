import uuid
from datetime import datetime
from typing import Any, cast

from sqlalchemy import CursorResult, delete, func, select, update
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from payments.models import Currency, OutboxEvent, Payment, PaymentStatus


class PaymentRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def get(self, payment_id: uuid.UUID) -> Payment | None:
        return await self._session.get(Payment, payment_id)

    async def get_by_idempotency_key(self, idempotency_key: str) -> Payment | None:
        return await self._session.scalar(
            select(Payment).where(Payment.idempotency_key == idempotency_key)
        )

    async def insert_if_absent(
        self,
        *,
        payment_id: uuid.UUID,
        amount_minor: int,
        currency: Currency,
        description: str,
        metadata: dict[str, Any],
        idempotency_key: str,
        request_hash: str,
        webhook_url: str,
    ) -> Payment | None:
        """Insert a payment unless its idempotency key is taken; None means it was taken.

        A concurrent insert with the same key blocks until the other transaction ends, so the
        caller can read the winner right after getting None.
        """
        stmt = (
            insert(Payment)
            .values(
                id=payment_id,
                amount_minor=amount_minor,
                currency=currency,
                description=description,
                metadata_=metadata,
                idempotency_key=idempotency_key,
                request_hash=request_hash,
                webhook_url=webhook_url,
            )
            .on_conflict_do_nothing(index_elements=[Payment.idempotency_key])
            .returning(Payment)
        )
        return await self._session.scalar(stmt)

    async def finalize(self, payment_id: uuid.UUID, status: PaymentStatus) -> Payment | None:
        """Move a pending payment to a final status; None if it was already final."""
        stmt = (
            update(Payment)
            .where(Payment.id == payment_id, Payment.status == PaymentStatus.PENDING)
            .values(status=status, processed_at=func.now())
            .returning(Payment)
        )
        return await self._session.scalar(stmt)

    async def mark_webhook_delivered(self, payment_id: uuid.UUID) -> None:
        await self._session.execute(
            update(Payment)
            .where(Payment.id == payment_id, Payment.webhook_delivered_at.is_(None))
            .values(webhook_delivered_at=func.now())
        )


class OutboxRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    def add(self, *, event_type: str, routing_key: str, payload: dict[str, Any]) -> None:
        self._session.add(
            OutboxEvent(
                id=uuid.uuid7(),
                event_type=event_type,
                routing_key=routing_key,
                payload=payload,
            )
        )

    async def lock_unpublished(self, limit: int) -> list[OutboxEvent]:
        """Lock a batch of unpublished events, skipping rows held by other relay instances."""
        result = await self._session.scalars(
            select(OutboxEvent)
            .where(OutboxEvent.published_at.is_(None))
            .order_by(OutboxEvent.created_at)
            .limit(limit)
            .with_for_update(skip_locked=True)
        )
        return list(result)

    async def delete_published_before(self, cutoff: datetime) -> int:
        stmt = delete(OutboxEvent).where(OutboxEvent.published_at < cutoff)
        # DML statements return a CursorResult; the session API types it as a plain Result.
        result = cast("CursorResult[Any]", await self._session.execute(stmt))
        return result.rowcount
