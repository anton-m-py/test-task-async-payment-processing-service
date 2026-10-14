import asyncio
import json
import uuid
from datetime import UTC, datetime, timedelta

from aio_pika.abc import AbstractIncomingMessage
from faststream.rabbit import RabbitBroker
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from payments.config import RelaySettings
from payments.messaging.topology import PAYMENTS_NEW_QUEUE
from payments.models import OutboxEvent, Payment
from payments.outbox.relay import BatchResult, OutboxRelay
from payments.schemas import PaymentCreate
from payments.services.payments import PaymentService

PAYMENT = PaymentCreate.model_validate(
    {
        "amount": "10.00",
        "currency": "EUR",
        "description": "Relay test",
        "webhook_url": "http://webhook-mock:8080/webhooks",
    }
)


async def _create_payment(session_factory: async_sessionmaker[AsyncSession]) -> Payment:
    async with session_factory() as session:
        return await PaymentService(session).create(PAYMENT, str(uuid.uuid4()))


async def _drain(broker: RabbitBroker) -> list[AbstractIncomingMessage]:
    queue = await broker.declare_queue(PAYMENTS_NEW_QUEUE)
    messages: list[AbstractIncomingMessage] = []
    while (message := await queue.get(no_ack=True, fail=False)) is not None:
        messages.append(message)
    return messages


async def _events(session_factory: async_sessionmaker[AsyncSession]) -> list[OutboxEvent]:
    async with session_factory() as session:
        return list(await session.scalars(select(OutboxEvent)))


async def test_publishes_pending_events(
    session_factory: async_sessionmaker[AsyncSession], broker: RabbitBroker
) -> None:
    payment = await _create_payment(session_factory)
    relay = OutboxRelay(session_factory, broker, RelaySettings())

    result = await relay.publish_batch()

    assert result == BatchResult(published=1, failed=False, full=False)
    [event] = await _events(session_factory)
    assert event.published_at is not None
    [message] = await _drain(broker)
    assert json.loads(message.body) == {"payment_id": str(payment.id)}
    assert message.message_id == str(event.id)
    assert message.type == "payment.created"
    # A second pass has nothing left to send.
    assert (await relay.publish_batch()).published == 0


async def test_unroutable_event_stays_pending(
    session_factory: async_sessionmaker[AsyncSession], broker: RabbitBroker
) -> None:
    async with session_factory() as session, session.begin():
        session.add(
            OutboxEvent(
                id=uuid.uuid7(),
                event_type="payment.created",
                routing_key="no.such.queue",
                payload={},
            )
        )
    relay = OutboxRelay(session_factory, broker, RelaySettings())

    result = await relay.publish_batch()

    assert result.failed
    [event] = await _events(session_factory)
    assert event.published_at is None
    assert event.attempts == 1
    assert event.last_error is not None


async def test_concurrent_relays_publish_each_event_once(
    session_factory: async_sessionmaker[AsyncSession], broker: RabbitBroker
) -> None:
    for _ in range(10):
        await _create_payment(session_factory)
    settings = RelaySettings(outbox_batch_size=3)
    relays = [OutboxRelay(session_factory, broker, settings) for _ in range(3)]

    async def drain_with(relay: OutboxRelay) -> None:
        while (await relay.publish_batch()).published:
            pass

    await asyncio.gather(*(drain_with(relay) for relay in relays))

    messages = await _drain(broker)
    assert len(messages) == 10
    assert len({m.message_id for m in messages}) == 10
    assert all(e.published_at is not None for e in await _events(session_factory))


async def test_cleanup_deletes_only_old_published_events(
    session_factory: async_sessionmaker[AsyncSession], broker: RabbitBroker
) -> None:
    now = datetime.now(UTC)
    async with session_factory() as session, session.begin():
        for published_at in (now - timedelta(days=8), now - timedelta(days=1), None):
            session.add(
                OutboxEvent(
                    id=uuid.uuid7(),
                    event_type="payment.created",
                    routing_key="payments.new",
                    payload={},
                    published_at=published_at,
                )
            )
    relay = OutboxRelay(session_factory, broker, RelaySettings(outbox_retention_days=7))

    await relay.cleanup()

    remaining = await _events(session_factory)
    assert len(remaining) == 2
    assert all(
        e.published_at is None or e.published_at > now - timedelta(days=7) for e in remaining
    )
