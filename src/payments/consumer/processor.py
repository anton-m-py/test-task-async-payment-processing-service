import logging
import uuid

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from payments.consumer.gateway import PaymentGateway
from payments.consumer.webhooks import WebhookSender
from payments.models import Payment, PaymentStatus
from payments.repositories import PaymentRepository

logger = logging.getLogger(__name__)


class PermanentProcessingError(Exception):
    """Retrying cannot help; the message goes straight to the dead-letter queue."""


class PaymentProcessor:
    """Charges a payment and notifies its webhook. Safe to run again for the same payment.

    Every step is skipped once done, so a redelivered or retried message only finishes the work
    that is left: a final status is never overwritten and a delivered webhook is not resent.
    """

    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        gateway: PaymentGateway,
        webhooks: WebhookSender,
    ) -> None:
        self._session_factory = session_factory
        self._gateway = gateway
        self._webhooks = webhooks

    async def process(self, payment_id: uuid.UUID) -> None:
        payment = await self._load(payment_id)
        if payment.status is PaymentStatus.PENDING:
            payment = await self._charge(payment)
        if payment.webhook_delivered_at is None:
            await self._webhooks.send(payment)
            async with self._session_factory() as session, session.begin():
                await PaymentRepository(session).mark_webhook_delivered(payment.id)
            logger.info("Webhook for payment %s delivered", payment.id)

    async def _load(self, payment_id: uuid.UUID) -> Payment:
        async with self._session_factory() as session:
            payment = await PaymentRepository(session).get(payment_id)
        if payment is None:
            raise PermanentProcessingError(f"payment {payment_id} does not exist")
        return payment

    async def _charge(self, payment: Payment) -> Payment:
        # No transaction is held open during the slow gateway call.
        approved = await self._gateway.charge(payment.id, payment.amount_minor, payment.currency)
        status = PaymentStatus.SUCCEEDED if approved else PaymentStatus.FAILED
        async with self._session_factory() as session, session.begin():
            updated = await PaymentRepository(session).finalize(payment.id, status)
        if updated is None:
            # A concurrent delivery of the same message finalized it first; its result stands.
            return await self._load(payment.id)
        logger.info("Payment %s %s", payment.id, status.value)
        return updated
