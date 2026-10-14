import random
import uuid

from payments.consumer.gateway import EmulatedGateway
from payments.consumer.webhooks import sign, verify
from payments.models import Currency

SECRET = b"secret"
BODY = b'{"event":"payment.succeeded"}'


def test_signature_roundtrip() -> None:
    signature = sign(SECRET, "1700000000", BODY)

    assert signature.startswith("sha256=")
    assert verify(SECRET, "1700000000", BODY, signature, now=1700000010)


def test_signature_rejects_tampering_and_replays() -> None:
    signature = sign(SECRET, "1700000000", BODY)

    assert not verify(SECRET, "1700000000", BODY + b" ", signature, now=1700000000)
    assert not verify(b"other", "1700000000", BODY, signature, now=1700000000)
    assert not verify(SECRET, "1700000001", BODY, signature, now=1700000000)
    assert not verify(SECRET, "1700000000", BODY, signature, now=1700000000 + 301)
    assert not verify(SECRET, "not-a-number", BODY, signature)


async def test_emulated_gateway_approval_rate() -> None:
    gateway = EmulatedGateway(min_delay_s=0, max_delay_s=0, success_rate=0.9, rng=random.Random(42))

    results = [await gateway.charge(uuid.uuid7(), 100, Currency.RUB) for _ in range(1000)]

    assert 850 < sum(results) < 950
