"""Transactional outbox relay: moves committed events from PostgreSQL to RabbitMQ.

Delivery is at least once. An event is marked published only after the broker confirms it, so a
crash between the confirm and the commit republishes the event; consumers must be idempotent.
"""

import asyncio
import contextlib
import logging
import time
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

from faststream.rabbit import RabbitBroker
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from payments.config import RelaySettings
from payments.messaging.topology import PAYMENTS_EXCHANGE
from payments.models import OutboxEvent
from payments.repositories import OutboxRepository

logger = logging.getLogger(__name__)

_MAX_ERROR_LENGTH = 2000
_PUBLISH_TIMEOUT_S = 10.0


@dataclass(frozen=True, slots=True)
class BatchResult:
    published: int
    failed: bool
    # A full batch means more events are probably waiting: poll again without sleeping.
    full: bool


class OutboxRelay:
    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        broker: RabbitBroker,
        settings: RelaySettings,
    ) -> None:
        self._session_factory = session_factory
        self._broker = broker
        self._settings = settings
        self._next_cleanup = 0.0

    async def run(self, stop: asyncio.Event) -> None:
        consecutive_errors = 0
        while not stop.is_set():
            try:
                result = await self.publish_batch()
                await self._cleanup_if_due()
            except Exception:
                logger.exception("Outbox relay iteration failed")
                result = BatchResult(published=0, failed=True, full=False)

            consecutive_errors = consecutive_errors + 1 if result.failed else 0
            await _wait(stop, self._delay(result, consecutive_errors))

    async def publish_batch(self) -> BatchResult:
        """Publish one batch of pending events in order, stopping at the first failure.

        Rows stay locked (FOR UPDATE SKIP LOCKED) until the batch commits, so several relay
        instances can run side by side without publishing the same event concurrently.
        """
        async with self._session_factory() as session, session.begin():
            events = await OutboxRepository(session).lock_unpublished(
                self._settings.outbox_batch_size
            )
            published = 0
            for event in events:
                try:
                    await self._publish(event)
                except Exception as exc:  # noqa: BLE001 - any failure leaves the event pending
                    logger.warning("Failed to publish outbox event %s: %r", event.id, exc)
                    event.attempts += 1
                    event.last_error = repr(exc)[:_MAX_ERROR_LENGTH]
                    return BatchResult(published=published, failed=True, full=False)
                event.published_at = datetime.now(UTC)
                published += 1

        if published:
            logger.info("Published %d outbox event(s)", published)
        return BatchResult(
            published=published,
            failed=False,
            full=len(events) == self._settings.outbox_batch_size,
        )

    async def _publish(self, event: OutboxEvent) -> None:
        await self._broker.publish(
            event.payload,
            exchange=PAYMENTS_EXCHANGE,
            routing_key=event.routing_key,
            message_id=str(event.id),
            message_type=event.event_type,
            persist=True,
            # With the channel's on_return_raises, an unroutable message fails instead of
            # being confirmed and dropped.
            mandatory=True,
            timeout=_PUBLISH_TIMEOUT_S,
        )

    async def _cleanup_if_due(self) -> None:
        now = time.monotonic()
        if now >= self._next_cleanup:
            self._next_cleanup = now + self._settings.outbox_cleanup_interval_s
            await self.cleanup()

    async def cleanup(self) -> None:
        """Delete published events past the retention period."""
        cutoff = datetime.now(UTC) - timedelta(days=self._settings.outbox_retention_days)
        async with self._session_factory() as session, session.begin():
            deleted = await OutboxRepository(session).delete_published_before(cutoff)
        if deleted:
            logger.info("Deleted %d published outbox event(s) older than %s", deleted, cutoff)

    def _delay(self, result: BatchResult, consecutive_errors: int) -> float:
        if consecutive_errors:
            backoff = self._settings.outbox_poll_interval_s * 2**consecutive_errors
            return float(min(backoff, self._settings.outbox_error_backoff_max_s))
        return 0.0 if result.full else self._settings.outbox_poll_interval_s


async def _wait(stop: asyncio.Event, delay: float) -> None:
    with contextlib.suppress(TimeoutError):
        async with asyncio.timeout(delay):
            await stop.wait()
