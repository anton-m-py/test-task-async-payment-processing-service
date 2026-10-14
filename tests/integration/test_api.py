import asyncio
import uuid
from typing import Any

import httpx
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from payments.messaging.topology import PAYMENT_CREATED_EVENT, PAYMENTS_NEW
from payments.models import OutboxEvent

PAYMENT: dict[str, Any] = {
    "amount": "100.50",
    "currency": "RUB",
    "description": "Order #42",
    "metadata": {"order_id": 42},
    "webhook_url": "http://webhook-mock:8080/webhooks",
}


def _key() -> dict[str, str]:
    return {"Idempotency-Key": str(uuid.uuid4())}


async def test_create_and_get_payment(client: httpx.AsyncClient) -> None:
    created = await client.post("/api/v1/payments", json=PAYMENT, headers=_key())

    assert created.status_code == 202
    body = created.json()
    assert body["status"] == "pending"
    assert set(body) == {"payment_id", "status", "created_at"}
    assert uuid.UUID(body["payment_id"]).version == 7

    fetched = await client.get(f"/api/v1/payments/{body['payment_id']}")

    assert fetched.status_code == 200
    payment = fetched.json()
    assert payment["amount"] == "100.50"
    assert payment["currency"] == "RUB"
    assert payment["metadata"] == {"order_id": 42}
    assert payment["processed_at"] is None


async def test_create_payment_writes_outbox_event(
    client: httpx.AsyncClient, session_factory: async_sessionmaker[AsyncSession]
) -> None:
    created = await client.post("/api/v1/payments", json=PAYMENT, headers=_key())

    async with session_factory() as session:
        event = await session.scalar(select(OutboxEvent))
    assert event is not None
    assert event.event_type == PAYMENT_CREATED_EVENT
    assert event.routing_key == PAYMENTS_NEW
    assert event.payload == {"payment_id": created.json()["payment_id"]}
    assert event.published_at is None


async def test_same_key_and_body_returns_original_payment(
    client: httpx.AsyncClient, session_factory: async_sessionmaker[AsyncSession]
) -> None:
    headers = _key()
    first = await client.post("/api/v1/payments", json=PAYMENT, headers=headers)
    # Same amount written differently is the same request.
    second = await client.post(
        "/api/v1/payments", json={**PAYMENT, "amount": "100.5"}, headers=headers
    )

    assert second.status_code == 202
    assert second.json() == first.json()
    async with session_factory() as session:
        assert await session.scalar(select(func.count()).select_from(OutboxEvent)) == 1


async def test_concurrent_requests_with_same_key_create_one_payment(
    client: httpx.AsyncClient, session_factory: async_sessionmaker[AsyncSession]
) -> None:
    headers = _key()
    responses = await asyncio.gather(
        *(client.post("/api/v1/payments", json=PAYMENT, headers=headers) for _ in range(5))
    )

    assert {r.status_code for r in responses} == {202}
    assert len({r.json()["payment_id"] for r in responses}) == 1
    async with session_factory() as session:
        assert await session.scalar(select(func.count()).select_from(OutboxEvent)) == 1


async def test_same_key_with_different_body_conflicts(client: httpx.AsyncClient) -> None:
    headers = _key()
    await client.post("/api/v1/payments", json=PAYMENT, headers=headers)

    response = await client.post(
        "/api/v1/payments", json={**PAYMENT, "amount": "1.00"}, headers=headers
    )

    assert response.status_code == 409


async def test_idempotency_key_is_required(client: httpx.AsyncClient) -> None:
    response = await client.post("/api/v1/payments", json=PAYMENT)

    assert response.status_code == 422


async def test_unknown_payment_is_not_found(client: httpx.AsyncClient) -> None:
    response = await client.get(f"/api/v1/payments/{uuid.uuid7()}")

    assert response.status_code == 404


async def test_api_key_is_required(client: httpx.AsyncClient) -> None:
    payment_url = f"/api/v1/payments/{uuid.uuid7()}"

    missing = await client.get(payment_url, headers={"X-API-Key": ""})
    wrong = await client.post(
        "/api/v1/payments", json=PAYMENT, headers={**_key(), "X-API-Key": "wrong"}
    )

    assert missing.status_code == 401
    assert wrong.status_code == 401


async def test_health_is_public(client: httpx.AsyncClient) -> None:
    response = await client.get("/health", headers={"X-API-Key": ""})

    assert response.status_code == 200
