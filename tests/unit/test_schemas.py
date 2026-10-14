from decimal import Decimal
from typing import Any

import pytest
from pydantic import ValidationError

from payments.schemas import PaymentCreate

VALID: dict[str, Any] = {
    "amount": "100.50",
    "currency": "USD",
    "description": "Order #42",
    "webhook_url": "https://example.com/hooks",
}


def test_valid_payment() -> None:
    payment = PaymentCreate.model_validate(VALID)

    assert payment.amount == Decimal("100.50")
    assert payment.metadata == {}


@pytest.mark.parametrize(
    "amount",
    [100.5, 100, "0", "-1", "1.001", "NaN", "Infinity", "1000000000000000.00", "abc"],
)
def test_invalid_amount(amount: object) -> None:
    with pytest.raises(ValidationError):
        PaymentCreate.model_validate({**VALID, "amount": amount})


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("currency", "GBP"),
        ("description", ""),
        ("webhook_url", "ftp://example.com"),
        ("webhook_url", "not a url"),
        ("unexpected", "field"),
    ],
)
def test_invalid_fields(field: str, value: object) -> None:
    with pytest.raises(ValidationError):
        PaymentCreate.model_validate({**VALID, field: value})
