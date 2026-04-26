"""Pricing conversion helpers."""

from __future__ import annotations

from decimal import ROUND_HALF_UP, Decimal
from math import isfinite

from databento_stream_downloader.errors import FatalAPIError


def dollars_to_decimal(value: float | Decimal | str) -> Decimal:
    """Parse a Databento dollar estimate into a validated Decimal."""
    if isinstance(value, Decimal):
        decimal_value = value
    elif isinstance(value, str):
        try:
            decimal_value = Decimal(value)
        except Exception as exc:
            msg = f"invalid Databento cost estimate: {value!r}"
            raise FatalAPIError(msg) from exc
    else:
        if not isinstance(value, float) or not isfinite(value):
            msg = f"invalid Databento cost estimate: {value!r}"
            raise FatalAPIError(msg)
        decimal_value = Decimal(str(value))
    if decimal_value.is_nan() or decimal_value.is_infinite() or decimal_value < 0:
        msg = f"invalid Databento cost estimate: {value!r}"
        raise FatalAPIError(msg)
    return decimal_value


def decimal_dollars_to_cents(value: Decimal) -> int:
    """Convert a non-negative finite dollar value to cents using half-up rounding."""
    if value.is_nan() or value.is_infinite() or value < 0:
        msg = f"invalid Databento cost estimate: {value!s}"
        raise FatalAPIError(msg)
    return int((value * Decimal(100)).quantize(Decimal("1"), rounding=ROUND_HALF_UP))
