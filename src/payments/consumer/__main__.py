"""Entry point: `python -m payments.consumer`."""

import asyncio
import logging

import httpx
from faststream import FastStream
from faststream.rabbit import Channel, RabbitBroker

from payments.config import ConsumerSettings, DatabaseSettings, RabbitSettings
from payments.consumer.gateway import EmulatedGateway
from payments.consumer.handler import register_payment_handler
from payments.consumer.processor import PaymentProcessor
from payments.consumer.webhooks import WebhookSender
from payments.db import create_engine, create_session_factory
from payments.messaging.topology import declare_topology


def create_app() -> FastStream:
    rabbit_settings = RabbitSettings()
    settings = ConsumerSettings()

    broker = RabbitBroker(
        rabbit_settings.rabbitmq_url,
        # Retry and DLQ publishes must fail loudly if unroutable, so the message is requeued.
        default_channel=Channel(publisher_confirms=True, on_return_raises=True),
        graceful_timeout=30.0,
    )
    engine = create_engine(DatabaseSettings())
    http = httpx.AsyncClient(timeout=settings.webhook_timeout_s)
    processor = PaymentProcessor(
        create_session_factory(engine),
        EmulatedGateway(
            min_delay_s=settings.gateway_min_delay_s,
            max_delay_s=settings.gateway_max_delay_s,
            success_rate=settings.gateway_success_rate,
        ),
        WebhookSender(http, settings.webhook_secret.get_secret_value().encode()),
    )
    register_payment_handler(
        broker, processor, rabbit_settings, concurrency=settings.consumer_concurrency
    )

    app = FastStream(broker)

    @app.on_startup
    async def declare() -> None:
        await broker.connect()
        await declare_topology(broker, rabbit_settings)

    @app.after_shutdown
    async def close() -> None:
        await http.aclose()
        await engine.dispose()

    return app


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    asyncio.run(create_app().run())
