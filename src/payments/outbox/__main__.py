"""Entry point: `python -m payments.outbox`."""

import asyncio
import logging
import signal

from faststream.rabbit import Channel, RabbitBroker

from payments.config import DatabaseSettings, RabbitSettings, RelaySettings
from payments.db import create_engine, create_session_factory
from payments.messaging.topology import declare_topology
from payments.outbox.relay import OutboxRelay

logger = logging.getLogger("payments.outbox")


async def main() -> None:
    rabbit_settings = RabbitSettings()
    engine = create_engine(DatabaseSettings())
    broker = RabbitBroker(
        rabbit_settings.rabbitmq_url,
        default_channel=Channel(publisher_confirms=True, on_return_raises=True),
    )

    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop.set)

    try:
        async with broker:
            await declare_topology(broker, rabbit_settings)
            relay = OutboxRelay(create_session_factory(engine), broker, RelaySettings())
            logger.info("Outbox relay started")
            await relay.run(stop)
    finally:
        await engine.dispose()
    logger.info("Outbox relay stopped")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    asyncio.run(main())
