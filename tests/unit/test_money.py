from decimal import Decimal

import pytest

from payments.money import from_minor, to_minor


@pytest.mark.parametrize(
    ("amount", "minor"),
    [
        (Decimal(100), 10000),
        (Decimal("100.5"), 10050),
        (Decimal("100.50"), 10050),
        (Decimal("0.01"), 1),
        (Decimal("999999999999999.99"), 99999999999999999),
    ],
)
def test_to_minor(amount: Decimal, minor: int) -> None:
    assert to_minor(amount) == minor


def test_to_minor_rejects_extra_precision() -> None:
    with pytest.raises(ValueError, match="decimal places"):
        to_minor(Decimal("1.001"))


def test_to_minor_accepts_trailing_zeros() -> None:
    assert to_minor(Decimal("1.000")) == 100


def test_from_minor_keeps_two_places() -> None:
    assert str(from_minor(10050)) == "100.50"
    assert str(from_minor(1)) == "0.01"
