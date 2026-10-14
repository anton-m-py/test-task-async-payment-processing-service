"""Signed webhook delivery.

Each request carries `X-Webhook-Timestamp` and `X-Webhook-Signature: sha256=<hex>`, where the
signature is HMAC-SHA256 over `<timestamp>.<raw body>` with the shared webhook secret. Receivers
recompute it, compare in constant time and reject stale timestamps to stop replays. Delivery is at
least once: `X-Webhook-Id` is stable across retries, so receivers can drop duplicates.
"""

import hashlib
import hmac
import time

import httpx

from payments.models import Payment
from payments.schemas import PaymentRead, WebhookPayload

SIGNATURE_HEADER = "X-Webhook-Signature"
TIMESTAMP_HEADER = "X-Webhook-Timestamp"
ID_HEADER = "X-Webhook-Id"


class WebhookDeliveryError(Exception):
    pass


def sign(secret: bytes, timestamp: str, body: bytes) -> str:
    digest = hmac.new(secret, timestamp.encode() + b"." + body, hashlib.sha256).hexdigest()
    return f"sha256={digest}"


def verify(
    secret: bytes,
    timestamp: str,
    body: bytes,
    signature: str,
    *,
    tolerance_s: int = 300,
    now: float | None = None,
) -> bool:
    """Receiver-side check, used by the webhook mock and shown in the README."""
    try:
        age = (time.time() if now is None else now) - int(timestamp)
    except ValueError:
        return False
    return abs(age) <= tolerance_s and hmac.compare_digest(sign(secret, timestamp, body), signature)


class WebhookSender:
    def __init__(self, client: httpx.AsyncClient, secret: bytes) -> None:
        self._client = client
        self._secret = secret

    async def send(self, payment: Payment) -> None:
        """POST the payment's final state to its webhook URL; any non-2xx answer is a failure."""
        body = (
            WebhookPayload(
                event=f"payment.{payment.status.value}",
                payment=PaymentRead.from_model(payment),
            )
            .model_dump_json()
            .encode()
        )
        timestamp = str(int(time.time()))
        headers = {
            "Content-Type": "application/json",
            ID_HEADER: str(payment.id),
            TIMESTAMP_HEADER: timestamp,
            SIGNATURE_HEADER: sign(self._secret, timestamp, body),
        }
        try:
            response = await self._client.post(payment.webhook_url, content=body, headers=headers)
        except httpx.HTTPError as exc:
            raise WebhookDeliveryError(f"{type(exc).__name__}: {exc}") from exc
        if not response.is_success:
            raise WebhookDeliveryError(f"webhook answered HTTP {response.status_code}")
