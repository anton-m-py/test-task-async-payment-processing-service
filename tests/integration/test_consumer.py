import asyncio
import json
import time
import uuid
from collections.abc import AsyncIterator, Awaitable, Callable
from dataclasses import dataclass, field

import httpx
import pytest
from aio_pika.abc import AbstractIncomingMessage
from faststream.rabbit import Channel, RabbitBroker
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from payments.config import RabbitSettings
from payments.consumer.handler import ATTEMPT_HEADER, ERROR_HEADER, register_payment_handler
from payments.consumer.processor import PaymentProcessor
from payments.consumer.webhooks import (
    ID_HEADER,
    SIGNATURE_HEADER,
    TIMESTAMP_HEADER,
    WebhookSender,
    verify,
)
from payments.messaging.topology import PAYMENTS_DLQ_QUEUE, PAYMENTS_EXCHANGE, PAYMENTS_NEW
from payments.models import Currency, Payment, PaymentStatus
from payments.repositories import PaymentRepository
from payments.schemas import PaymentCreate
from payments.services.payments import PaymentService

SECRET = b"webhook-secret"
PAYMENT = PaymentCreate.model_validate(
    {
        "amount": "42.00",
        "currency": "USD",
        "description": "Consumer test",
        "webhook_url": "http://receiver.test/webhooks",
    }
)


@dataclass
class FakeGateway:
    approve: bool = True
    charges: list[uuid.UUID] = field(default_factory=list)

    async def charge(self, payment_id: uuid.UUID, amount_minor: int, currency: Currency) -> bool:
        self.charges.append(payment_id)
        return self.approve


@dataclass
class Receiver:
    """Webhook endpoint double: answers with the queued status codes, then 200."""

    statuses: list[int] = field(default_factory=list)
    requests: list[httpx.Request] = field(default_factory=list)

    def __call__(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        return httpx.Response(self.statuses.pop(0) if self.statuses else 200)


@dataclass
class Consumer:
    gateway: FakeGateway
    receiver: Receiver


@pytest.fixture
async def consumer(
    broker: RabbitBroker,
    rabbit_settings: RabbitSettings,
    session_factory: async_sessionmaker[AsyncSession],
) -> AsyncIterator[Consumer]:
    """A running consumer with a fake gateway and an in-process webhook receiver."""
    gateway, receiver = FakeGateway(), Receiver()
    consumer_broker = RabbitBroker(
        rabbit_settings.rabbitmq_url,
        default_channel=Channel(publisher_confirms=True, on_return_raises=True),
        logger=None,
    )
    async with httpx.AsyncClient(transport=httpx.MockTransport(receiver)) as http:
        processor = PaymentProcessor(session_factory, gateway, WebhookSender(http, SECRET))
        register_payment_handler(consumer_broker, processor, rabbit_settings, concurrency=5)
        async with consumer_broker:
            await consumer_broker.start()
            yield Consumer(gateway, receiver)


async def _create_payment(session_factory: async_sessionmaker[AsyncSession]) -> Payment:
    async with session_factory() as session:
        return await PaymentService(session).create(PAYMENT, str(uuid.uuid4()))


async def _publish(broker: RabbitBroker, body: bytes | dict[str, str]) -> None:
    await broker.publish(body, exchange=PAYMENTS_EXCHANGE, routing_key=PAYMENTS_NEW, persist=True)


async def _get_payment(
    session_factory: async_sessionmaker[AsyncSession], payment_id: uuid.UUID
) -> Payment:
    async with session_factory() as session:
        payment = await PaymentRepository(session).get(payment_id)
    assert payment is not None
    return payment


async def _eventually[T](probe: Callable[[], Awaitable[T | None]], within_s: float = 10.0) -> T:
    deadline = time.monotonic() + within_s
    while time.monotonic() < deadline:
        if (result := await probe()) is not None:
            return result
        await asyncio.sleep(0.05)
    raise AssertionError("condition not met in time")


async def _dead_letter(broker: RabbitBroker) -> AbstractIncomingMessage:
    queue = await broker.declare_queue(PAYMENTS_DLQ_QUEUE)
    return await _eventually(lambda: queue.get(no_ack=True, fail=False))


async def _delivered(
    session_factory: async_sessionmaker[AsyncSession], payment_id: uuid.UUID
) -> Payment:
    async def probe() -> Payment | None:
        payment = await _get_payment(session_factory, payment_id)
        return payment if payment.webhook_delivered_at else None

    return await _eventually(probe)


async def test_processes_payment_and_sends_signed_webhook(
    consumer: Consumer,
    broker: RabbitBroker,
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    payment = await _create_payment(session_factory)

    await _publish(broker, {"payment_id": str(payment.id)})
    processed = await _delivered(session_factory, payment.id)

    assert processed.status is PaymentStatus.SUCCEEDED
    assert processed.processed_at is not None
    [request] = consumer.receiver.requests
    assert str(request.url) == "http://receiver.test/webhooks"
    assert request.headers[ID_HEADER] == str(payment.id)
    assert verify(
        SECRET,
        request.headers[TIMESTAMP_HEADER],
        request.content,
        request.headers[SIGNATURE_HEADER],
    )
    body = json.loads(request.content)
    assert body["event"] == "payment.succeeded"
    assert body["payment"]["payment_id"] == str(payment.id)
    assert body["payment"]["amount"] == "42.00"


async def test_declined_payment_is_failed_and_notified(
    consumer: Consumer,
    broker: RabbitBroker,
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    consumer.gateway.approve = False
    payment = await _create_payment(session_factory)

    await _publish(broker, {"payment_id": str(payment.id)})
    processed = await _delivered(session_factory, payment.id)

    assert processed.status is PaymentStatus.FAILED
    assert json.loads(consumer.receiver.requests[0].content)["event"] == "payment.failed"


async def test_webhook_failure_is_retried_without_charging_again(
    consumer: Consumer,
    broker: RabbitBroker,
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    consumer.receiver.statuses = [503, 500]
    payment = await _create_payment(session_factory)

    await _publish(broker, {"payment_id": str(payment.id)})
    await _delivered(session_factory, payment.id)

    assert len(consumer.receiver.requests) == 3
    assert consumer.gateway.charges == [payment.id]


async def test_message_is_dead_lettered_after_max_attempts(
    consumer: Consumer,
    broker: RabbitBroker,
    rabbit_settings: RabbitSettings,
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    consumer.receiver.statuses = [500] * 10
    payment = await _create_payment(session_factory)

    await _publish(broker, {"payment_id": str(payment.id)})
    dead = await _dead_letter(broker)

    assert json.loads(dead.body) == {"payment_id": str(payment.id)}
    assert dead.headers[ATTEMPT_HEADER] == rabbit_settings.max_attempts
    assert "HTTP 500" in str(dead.headers[ERROR_HEADER])
    assert len(consumer.receiver.requests) == rabbit_settings.max_attempts
    stored = await _get_payment(session_factory, payment.id)
    assert stored.status is PaymentStatus.SUCCEEDED
    assert stored.webhook_delivered_at is None


async def test_unknown_payment_is_dead_lettered_immediately(
    consumer: Consumer, broker: RabbitBroker
) -> None:
    await _publish(broker, {"payment_id": str(uuid.uuid7())})

    dead = await _dead_letter(broker)

    assert dead.headers[ATTEMPT_HEADER] == 1
    assert "does not exist" in str(dead.headers[ERROR_HEADER])
    assert consumer.gateway.charges == []


async def test_malformed_message_is_dead_lettered(consumer: Consumer, broker: RabbitBroker) -> None:
    await _publish(broker, b"not json")

    dead = await _dead_letter(broker)

    assert dead.body == b"not json"
    assert consumer.gateway.charges == []


async def test_redelivered_message_does_not_repeat_work(
    consumer: Consumer,
    broker: RabbitBroker,
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    payment = await _create_payment(session_factory)
    await _publish(broker, {"payment_id": str(payment.id)})
    await _delivered(session_factory, payment.id)

    # The outbox delivers at least once: the same event may arrive again.
    await _publish(broker, {"payment_id": str(payment.id)})
    await asyncio.sleep(0.5)

    assert consumer.gateway.charges == [payment.id]
    assert len(consumer.receiver.requests) == 1
