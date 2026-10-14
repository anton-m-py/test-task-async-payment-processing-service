"""Conversion between decimal amounts at the API boundary and integer minor units in storage."""

from decimal import Decimal

# RUB, USD and EUR all have two minor-unit digits.
MINOR_UNITS = 2
_QUANTUM = Decimal(1).scaleb(-MINOR_UNITS)


def to_minor(amount: Decimal) -> int:
    """Convert an amount with at most MINOR_UNITS fractional digits; never rounds."""
    minor = amount.scaleb(MINOR_UNITS)
    if minor != minor.to_integral_value():
        raise ValueError(f"amount must have at most {MINOR_UNITS} decimal places")
    return int(minor)


def from_minor(minor: int) -> Decimal:
    return Decimal(minor).scaleb(-MINOR_UNITS).quantize(_QUANTUM)
