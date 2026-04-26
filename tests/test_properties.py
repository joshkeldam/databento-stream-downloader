"""Property tests for non-trivial pure helpers."""
# pyright: reportPrivateUsage=false

from __future__ import annotations

from datetime import date, timedelta
from decimal import ROUND_HALF_UP, Decimal
from pathlib import Path
from tempfile import TemporaryDirectory

import pytest
from hypothesis import given
from hypothesis import strategies as st

from databento_stream_downloader.dbn import validate_dbn_metadata
from databento_stream_downloader.errors import ValidationError
from databento_stream_downloader.models import StreamQuery
from databento_stream_downloader.pricing import (
    decimal_dollars_to_cents,
    dollars_to_decimal,
)
from databento_stream_downloader.runner import WorkItem, _cost_ranges


@given(st.lists(st.integers(min_value=0, max_value=365), min_size=1, max_size=40))
def test_cost_ranges_cover_exact_input_days(offsets: list[int]) -> None:
    base = date(2026, 1, 1)
    days = {base + timedelta(days=offset) for offset in offsets}
    work = [WorkItem(symbol="ES.FUT", schema="mbo", day=day) for day in days]

    covered: set[date] = set()
    for _symbol, _schema, start, end in _cost_ranges(work):
        day = start
        while day < end:
            covered.add(day)
            day += timedelta(days=1)

    assert covered == days


@given(st.decimals(min_value=0, max_value=10_000, places=4, allow_nan=False))
def test_cost_estimate_conversion_matches_decimal_half_up(value: Decimal) -> None:
    as_float = float(value)
    expected = int(
        (Decimal(str(as_float)) * Decimal(100)).quantize(
            Decimal("1"),
            rounding=ROUND_HALF_UP,
        )
    )

    assert decimal_dollars_to_cents(dollars_to_decimal(as_float)) == expected


@given(st.binary(min_size=0, max_size=256))
def test_dbn_metadata_parser_rejects_arbitrary_bytes(payload: bytes) -> None:
    with TemporaryDirectory() as directory:
        path = Path(directory) / "fuzz.dbn.zst"
        path.write_bytes(payload)
        query = StreamQuery(
            dataset="GLBX.MDP3",
            symbol="ES.FUT",
            schema="mbo",
            start=date(2026, 4, 1),
            end=date(2026, 4, 2),
        )

        with pytest.raises(ValidationError):
            validate_dbn_metadata(query, path, deep=True)
