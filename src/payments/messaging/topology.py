"""RabbitMQ topology shared by the outbox relay (producer) and the consumer.

    payments (direct) --payments.new--> payments.new --(consumer)
         ^                                   |  reject / delivery limit
         |                                   v
         |                         payments.dlx --> payments.new.dlq
         |                                   ^
         |                                   | attempts exhausted
    payments.retry (direct) --> payments.new.retry.<delay>ms (TTL) --+
                                         (expired messages go back to `payments`)

Both processes declare everything on startup. Declarations are idempotent, and the relay must not
publish before the queue exists: an unroutable message would be confirmed and silently dropped.
"""

from aio_pika import RobustExchange
from faststream.rabbit import ExchangeType, QueueType, RabbitBroker, RabbitExchange, RabbitQueue

from payments.config import RabbitSettings

PAYMENT_CREATED_EVENT = "payment.created"

PAYMENTS_NEW = "payments.new"
PAYMENTS_DLQ = "payments.new.dlq"

PAYMENTS_EXCHANGE = RabbitExchange("payments", type=ExchangeType.DIRECT, durable=True)
RETRY_EXCHANGE = RabbitExchange("payments.retry", type=ExchangeType.DIRECT, durable=True)
DEAD_LETTER_EXCHANGE = RabbitExchange("payments.dlx", type=ExchangeType.DIRECT, durable=True)

# Quorum queues are the durable, replicated default in RabbitMQ 4. Their built-in delivery limit
# also dead-letters a message that keeps crashing the consumer instead of looping forever.
PAYMENTS_NEW_QUEUE = RabbitQueue(
    PAYMENTS_NEW,
    queue_type=QueueType.QUORUM,
    routing_key=PAYMENTS_NEW,
    arguments={
        "x-dead-letter-exchange": DEAD_LETTER_EXCHANGE.name,
        "x-dead-letter-routing-key": PAYMENTS_DLQ,
    },
)

PAYMENTS_DLQ_QUEUE = RabbitQueue(
    PAYMENTS_DLQ, queue_type=QueueType.QUORUM, routing_key=PAYMENTS_DLQ
)


def retry_delay_ms(settings: RabbitSettings, retry: int) -> int:
    """Exponential delay before retry number `retry` (1-based)."""
    return settings.retry_base_delay_ms << (retry - 1)


def retry_queue(delay_ms: int) -> RabbitQueue:
    # The delay is part of the name: a queue's TTL cannot change after declaration, so a new
    # delay setting gets a new queue instead of failing with PRECONDITION_FAILED.
    name = f"{PAYMENTS_NEW}.retry.{delay_ms}ms"
    return RabbitQueue(
        name,
        queue_type=QueueType.QUORUM,
        routing_key=name,
        arguments={
            "x-message-ttl": delay_ms,
            "x-dead-letter-exchange": PAYMENTS_EXCHANGE.name,
            "x-dead-letter-routing-key": PAYMENTS_NEW,
        },
    )


def retry_queues(settings: RabbitSettings) -> list[RabbitQueue]:
    """One delay queue per retry; with max_attempts=3 there are two retries."""
    return [
        retry_queue(retry_delay_ms(settings, retry)) for retry in range(1, settings.max_attempts)
    ]


async def declare_topology(broker: RabbitBroker, settings: RabbitSettings) -> None:
    payments = await broker.declare_exchange(PAYMENTS_EXCHANGE)
    retry = await broker.declare_exchange(RETRY_EXCHANGE)
    dead_letter = await broker.declare_exchange(DEAD_LETTER_EXCHANGE)

    await _declare_bound(broker, PAYMENTS_NEW_QUEUE, payments)
    await _declare_bound(broker, PAYMENTS_DLQ_QUEUE, dead_letter)
    for queue in retry_queues(settings):
        await _declare_bound(broker, queue, retry)


async def _declare_bound(
    broker: RabbitBroker, queue: RabbitQueue, exchange: RobustExchange
) -> None:
    declared = await broker.declare_queue(queue)
    await declared.bind(exchange, routing_key=queue.routing())
