"""Pricing conversion helpers."""

from __future__ import annotations

from decimal import ROUND_HALF_UP, Decimal

from databento_stream_downloader.errors import FatalAPIError


def decimal_dollars_to_cents(value: Decimal) -> int:
    """Convert a non-negative finite dollar value to cents using half-up rounding."""
    if value.is_nan() or value.is_infinite() or value < 0:
        msg = f"invalid Databento cost estimate: {value!s}"
        raise FatalAPIError(msg)
    return int((value * Decimal(100)).quantize(Decimal("1"), rounding=ROUND_HALF_UP))
