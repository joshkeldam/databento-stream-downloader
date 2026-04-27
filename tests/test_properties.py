"""Property tests for non-trivial pure helpers."""
# pyright: reportPrivateUsage=false

from __future__ import annotations

from datetime import date, timedelta
from decimal import ROUND_HALF_UP, Decimal
from itertools import pairwise
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from databento_stream_downloader.dbn import validate_dbn_metadata
from databento_stream_downloader.errors import ValidationError
from databento_stream_downloader.models import StreamQuery
from databento_stream_downloader.pricing import (
    decimal_dollars_to_cents,
    dollars_to_decimal,
)
from databento_stream_downloader.runner import (
    WorkItem,
    _allocate_estimated_values,
    _cost_ranges,
    _read_sha256_sidecar,
    _sha256_file,
    _universe_semantic_sha256,
    _validate_sha256_sidecar,
    _write_sha256_sidecar,
)

_BASE_DAY = date(2026, 1, 1)


def _work_items_for_offsets(offsets: list[int]) -> list[WorkItem]:
    return _work_items_for_offsets_and_schema(offsets, "mbo")


def _work_items_for_offsets_and_schema(
    offsets: list[int],
    schema: str,
) -> list[WorkItem]:
    return [
        WorkItem(symbol="ES.FUT", schema=schema, day=_BASE_DAY + timedelta(days=offset))
        for offset in offsets
    ]


@given(st.lists(st.integers(min_value=0, max_value=365), min_size=1, max_size=40))
def test_cost_ranges_cover_input_span(offsets: list[int]) -> None:
    days = {_BASE_DAY + timedelta(days=offset) for offset in offsets}
    work = [WorkItem(symbol="ES.FUT", schema="mbo", day=day) for day in days]

    covered: set[date] = set()
    for _symbol, _schema, start, end in _cost_ranges(work):
        day = start
        while day < end:
            covered.add(day)
            day += timedelta(days=1)

    assert covered == days


@given(
    count=st.integers(min_value=1, max_value=80),
    total=st.integers(min_value=0, max_value=10_000),
)
def test_allocate_estimated_values_conserves_total_and_balances_items(
    count: int,
    total: int,
) -> None:
    work = _work_items_for_offsets(list(range(count)))

    allocation = _allocate_estimated_values(work, {("ES.FUT", "mbo"): total})
    values = list(allocation.values())

    assert set(allocation) == set(work)
    assert sum(values) == total
    assert max(values) - min(values) <= 1


@given(st.integers(min_value=1, max_value=80))
def test_cost_ranges_collapse_contiguous_mbo_days(length: int) -> None:
    work = _work_items_for_offsets(list(range(length)))

    assert _cost_ranges(work) == [
        ("ES.FUT", "mbo", _BASE_DAY, _BASE_DAY + timedelta(days=length))
    ]


@given(st.integers(min_value=1, max_value=80))
def test_cost_ranges_collapse_contiguous_non_mbo_days(length: int) -> None:
    work = _work_items_for_offsets_and_schema(list(range(length)), "definition")

    assert _cost_ranges(work) == [
        ("ES.FUT", "definition", _BASE_DAY, _BASE_DAY + timedelta(days=length))
    ]


@given(
    st.lists(
        st.integers(min_value=0, max_value=365),
        min_size=1,
        max_size=80,
        unique=True,
    )
)
def test_cost_ranges_split_at_missing_day_gaps(offsets: list[int]) -> None:
    sorted_offsets = sorted(offsets)
    work = _work_items_for_offsets_and_schema(sorted_offsets, "definition")
    ranges = _cost_ranges(work)

    covered_offsets: list[int] = []
    for _symbol, _schema, start, end in ranges:
        day = start
        while day < end:
            covered_offsets.append((day - _BASE_DAY).days)
            day += timedelta(days=1)

    assert covered_offsets == sorted_offsets
    assert all(
        right[2] >= left[3] + timedelta(days=1)
        for left, right in pairwise(ranges)
    )


@given(
    st.lists(
        st.sampled_from(["ES.FUT", "NQ.FUT", "CL.FUT", "GC.FUT", "ZN.FUT"]),
        min_size=1,
        max_size=5,
        unique=True,
    ),
    st.permutations(["BTC.FUT", "ETH.FUT", "SOL.FUT"]),
)
def test_universe_semantic_hash_is_stable_under_symbol_order(
    symbols: list[str],
    ordered_first_data_symbols: tuple[str, ...],
) -> None:
    first_data = {
        symbol: _BASE_DAY + timedelta(days=index)
        for index, symbol in enumerate(ordered_first_data_symbols)
    }
    with (
        patch(
            "databento_stream_downloader._runner.ledger.load_first_data_utc_dates",
            return_value=first_data,
        ),
        patch(
            "databento_stream_downloader._runner.ledger.load_default_symbols",
            return_value=tuple(symbols),
        ),
    ):
        first_hash = _universe_semantic_sha256()
    with (
        patch(
            "databento_stream_downloader._runner.ledger.load_first_data_utc_dates",
            return_value=first_data,
        ),
        patch(
            "databento_stream_downloader._runner.ledger.load_default_symbols",
            return_value=tuple(reversed(symbols)),
        ),
    ):
        assert _universe_semantic_sha256() == first_hash


@settings(max_examples=30)
@given(st.binary(min_size=0, max_size=4096))
def test_sha256_sidecar_round_trip_validates_any_file_payload(payload: bytes) -> None:
    with TemporaryDirectory() as directory:
        path = Path(directory) / "payload.dbn.zst"
        path.write_bytes(payload)

        _write_sha256_sidecar(path)

        assert _read_sha256_sidecar(path) == _sha256_file(path)
        _validate_sha256_sidecar(path)


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
