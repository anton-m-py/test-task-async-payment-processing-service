"""The `payments.new` subscriber: one handler that processes, retries and dead-letters.

A failed attempt is republished to a delay queue (`payments.retry` exchange) with an incremented
attempt header and the original is acked; the delay queue's TTL dead-letters it back to
`payments.new`. After `max_attempts` the message is published to the DLQ with the last error.
Retries go through the broker rather than sleeping in the handler, so a waiting retry holds no
prefetch slot and survives a consumer restart.
"""

import logging

from faststream import AckPolicy
from faststream.rabbit import Channel, RabbitBroker
from faststream.rabbit.annotations import RabbitMessage
from pydantic import ValidationError

from payments.config import RabbitSettings
from payments.consumer.processor import PaymentProcessor, PermanentProcessingError
from payments.messaging.topology import (
    DEAD_LETTER_EXCHANGE,
    PAYMENTS_DLQ,
    PAYMENTS_EXCHANGE,
    PAYMENTS_NEW_QUEUE,
    RETRY_EXCHANGE,
    retry_delay_ms,
    retry_queue,
)
from payments.schemas import PaymentCreatedEvent

logger = logging.getLogger(__name__)

ATTEMPT_HEADER = "payments-attempt"
ERROR_HEADER = "payments-last-error"
_MAX_ERROR_LENGTH = 1000


def register_payment_handler(
    broker: RabbitBroker,
    processor: PaymentProcessor,
    settings: RabbitSettings,
    *,
    concurrency: int,
) -> None:
    @broker.subscriber(
        PAYMENTS_NEW_QUEUE,
        PAYMENTS_EXCHANGE,
        # Handlers run concurrently up to the prefetch limit.
        channel=Channel(prefetch_count=concurrency),
        # Explicit acks below; anything escaping them is requeued, and the quorum queue's
        # delivery limit eventually dead-letters a message that keeps failing that way.
        ack_policy=AckPolicy.NACK_ON_ERROR,
    )
    async def handle_payment_created(message: RabbitMessage) -> None:
        attempt = _attempt(message)
        try:
            event = PaymentCreatedEvent.model_validate_json(message.body)
        except ValidationError:
            logger.exception("Malformed message %s, dead-lettering", message.message_id)
            # The queue's dead-letter exchange routes rejected messages to the DLQ.
            await message.reject()
            return

        try:
            await processor.process(event.payment_id)
        except PermanentProcessingError as exc:
            logger.error("Payment %s cannot be processed: %s", event.payment_id, exc)  # noqa: TRY400
            await _dead_letter(broker, message, attempt, exc)
        except Exception as exc:
            if attempt < settings.max_attempts:
                delay_ms = retry_delay_ms(settings, attempt)
                logger.warning(
                    "Attempt %d/%d for payment %s failed (%r), retrying in %d ms",
                    attempt,
                    settings.max_attempts,
                    event.payment_id,
                    exc,
                    delay_ms,
                )
                await _republish(
                    broker,
                    message,
                    exchange=RETRY_EXCHANGE.name,
                    routing_key=retry_queue(delay_ms).name,
                    headers={ATTEMPT_HEADER: attempt + 1, ERROR_HEADER: _describe(exc)},
                )
            else:
                logger.exception(
                    "Payment %s failed after %d attempts, dead-lettering",
                    event.payment_id,
                    attempt,
                )
                await _dead_letter(broker, message, attempt, exc)
        await message.ack()


def _attempt(message: RabbitMessage) -> int:
    try:
        return max(1, int(message.headers.get(ATTEMPT_HEADER, 1)))
    except TypeError, ValueError:
        return 1


def _describe(exc: BaseException) -> str:
    return f"{type(exc).__name__}: {exc}"[:_MAX_ERROR_LENGTH]


async def _dead_letter(
    broker: RabbitBroker, message: RabbitMessage, attempt: int, exc: BaseException
) -> None:
    await _republish(
        broker,
        message,
        exchange=DEAD_LETTER_EXCHANGE.name,
        routing_key=PAYMENTS_DLQ,
        headers={ATTEMPT_HEADER: attempt, ERROR_HEADER: _describe(exc)},
    )


async def _republish(
    broker: RabbitBroker,
    message: RabbitMessage,
    *,
    exchange: str,
    routing_key: str,
    headers: dict[str, str | int],
) -> None:
    """Publish a copy of the message (confirmed, mandatory) before the original is acked.

    A crash between the two leaves both copies; the processor is idempotent, so that is safe.
    """
    await broker.publish(
        message.body,
        exchange=exchange,
        routing_key=routing_key,
        # Broker-set `x-*` headers such as x-death describe the previous hop, not this message.
        headers={
            **{k: v for k, v in message.headers.items() if not k.startswith("x-")},
            **headers,
        },
        message_id=message.message_id,
        message_type=message.raw_message.type,
        content_type=message.content_type,
        persist=True,
        mandatory=True,
    )
