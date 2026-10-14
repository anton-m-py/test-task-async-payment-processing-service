"""Demo webhook receiver for docker compose; not part of the service.

POST /webhooks         verifies the signature and records the delivery
POST /webhooks/flaky   answers 500 to the first two deliveries of each webhook, then 200
POST /webhooks/fail    always answers 500, so the message ends up in the DLQ
GET  /webhooks         recent deliveries, newest first
"""

import logging
from collections import Counter, deque
from typing import Annotated, Any

from fastapi import FastAPI, Header, HTTPException, Request, status
from pydantic import BaseModel, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict

from payments.consumer.webhooks import ID_HEADER, SIGNATURE_HEADER, TIMESTAMP_HEADER, verify

logger = logging.getLogger("uvicorn.error")

_FLAKY_FAILURES = 2


class MockSettings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    webhook_secret: SecretStr


class Delivery(BaseModel):
    endpoint: str
    webhook_id: str | None
    signature_valid: bool
    body: dict[str, Any]


secret = MockSettings().webhook_secret.get_secret_value().encode()
deliveries: deque[Delivery] = deque(maxlen=100)
attempts: Counter[str] = Counter()

app = FastAPI(title="Webhook mock")


async def _record(
    request: Request,
    endpoint: str,
    webhook_id: str | None,
    timestamp: str | None,
    signature: str | None,
) -> Delivery:
    body = await request.body()
    valid = bool(timestamp and signature and verify(secret, timestamp, body, signature))
    delivery = Delivery(
        endpoint=endpoint,
        webhook_id=webhook_id,
        signature_valid=valid,
        body=await request.json(),
    )
    deliveries.appendleft(delivery)
    logger.info(
        "Webhook %s on /%s: signature %s", webhook_id, endpoint, "valid" if valid else "INVALID"
    )
    if not valid:
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "invalid signature")
    return delivery


WebhookId = Annotated[str | None, Header(alias=ID_HEADER)]
Timestamp = Annotated[str | None, Header(alias=TIMESTAMP_HEADER)]
Signature = Annotated[str | None, Header(alias=SIGNATURE_HEADER)]


@app.post("/webhooks")
async def receive(
    request: Request, webhook_id: WebhookId, timestamp: Timestamp, signature: Signature
) -> dict[str, str]:
    await _record(request, "webhooks", webhook_id, timestamp, signature)
    return {"status": "received"}


@app.post("/webhooks/flaky")
async def receive_flaky(
    request: Request, webhook_id: WebhookId, timestamp: Timestamp, signature: Signature
) -> dict[str, str]:
    await _record(request, "webhooks/flaky", webhook_id, timestamp, signature)
    attempts[webhook_id or ""] += 1
    if attempts[webhook_id or ""] <= _FLAKY_FAILURES:
        raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR, "simulated failure")
    return {"status": "received"}


@app.post("/webhooks/fail")
async def receive_fail(
    request: Request, webhook_id: WebhookId, timestamp: Timestamp, signature: Signature
) -> None:
    await _record(request, "webhooks/fail", webhook_id, timestamp, signature)
    raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR, "simulated failure")


@app.get("/webhooks")
async def list_deliveries() -> list[Delivery]:
    return list(deliveries)


@app.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok"}
