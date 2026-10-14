import asyncio
import random
import uuid
from typing import Protocol

from payments.models import Currency


class PaymentGateway(Protocol):
    async def charge(self, payment_id: uuid.UUID, amount_minor: int, currency: Currency) -> bool:
        """Charge the payment; True if approved, False if declined.

        The payment id doubles as the gateway's idempotency key: a real gateway must not charge
        twice when a crash between the charge and the status update causes a redelivery.
        """
        ...


class EmulatedGateway:
    """Stand-in for an external gateway: random latency and approval rate."""

    def __init__(
        self,
        *,
        min_delay_s: float,
        max_delay_s: float,
        success_rate: float,
        rng: random.Random | None = None,
    ) -> None:
        self._min_delay_s = min_delay_s
        self._max_delay_s = max_delay_s
        self._success_rate = success_rate
        self._rng = rng or random.Random()  # noqa: S311 - emulation, not cryptography

    async def charge(self, payment_id: uuid.UUID, amount_minor: int, currency: Currency) -> bool:
        await asyncio.sleep(self._rng.uniform(self._min_delay_s, self._max_delay_s))
        return self._rng.random() < self._success_rate
