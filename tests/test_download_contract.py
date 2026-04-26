"""Executable tests for core downloader behavior."""
# pyright: reportPrivateUsage=false

from __future__ import annotations

import json
import os
import shutil
import time
from collections.abc import Sequence
from dataclasses import dataclass, field
from datetime import date, timedelta
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from typing import NoReturn, cast

import databento_dbn
import pytest
import zstandard
from databento_dbn.metadata import SymbolMapping
from pydantic import ValidationError as PydanticValidationError
from rich.console import Console

import databento_stream_downloader.dbn as dbn
import databento_stream_downloader.runner as runner
from databento_stream_downloader.config import DownloadConfig, RunMode
from databento_stream_downloader.dbn import validate_dbn_metadata, write_empty_dbn_file
from databento_stream_downloader.errors import (
    DegradedError,
    FatalError,
    InterruptedDownloadError,
    RetryableError,
    ValidationError,
)
from databento_stream_downloader.models import (
    CostEstimate,
    CostQuery,
    DownloadResult,
    StreamQuery,
)
from databento_stream_downloader.paths import canonical_path
from databento_stream_downloader.runner import (
    DATASET,
    DownloaderClient,
    WorkItem,
    _allocate_estimated_values,
    _bytes,
    _check_bucket_cost_caps,
    _check_cost_cap,
    _check_disk_space,
    _DirectoryFsyncTracker,
    _estimate_costs,
    _exclusive_run_lock,
    _existing_items,
    _fsync_directory,
    _money,
    _mount_for_path,
    _print_costs,
    _print_retry_summary,
    _raise_on_suspicious_all_no_data,
    _reject_known_network_filesystem,
    _repair_missing_sidecars,
    _rotate_ledger_if_needed,
    _run_download,
    _sha256_sidecar_path,
    _stream_missing,
    _stream_one,
    _sweep_stale_tmp_files,
    _total_estimated_cents,
    _universe_semantic_sha256,
    _validate,
    _validate_one,
    _validate_runtime_config,
    _validate_sha256_sidecar,
)


@dataclass
class _MappingInterval:
    start_date: date
    end_date: date
    symbol: str


@dataclass
class _SymbolMapping:
    raw_symbol: str
    intervals: Sequence[_MappingInterval]


@dataclass
class FakeClient:
    """Small Databento client fake for runner contract tests."""

    no_data: bool = False
    fatal: bool = False
    cost_queries: list[CostQuery] = field(default_factory=list)
    size_queries: list[CostQuery] = field(default_factory=list)
    streamed: list[StreamQuery] = field(default_factory=list)

    def estimate_cost(self, query: CostQuery) -> Decimal:
        self.cost_queries.append(query)
        return Decimal("0.07")

    def estimate_size(self, query: CostQuery) -> int:
        self.size_queries.append(query)
        return 11

    def stream_to_file(self, query: StreamQuery, output_path: Path) -> None:
        self.streamed.append(query)
        if self.fatal:
            raise FatalError("fatal")
        if self.no_data:
            raise DegradedError("no data")
        write_empty_dbn_file(query, output_path)

    def write_empty_file(self, query: StreamQuery, output_path: Path) -> None:
        write_empty_dbn_file(query, output_path)


class RetryClient(FakeClient):
    def stream_to_file(self, query: StreamQuery, output_path: Path) -> None:
        _ = (query, output_path)
        raise RetryableError("retry exhausted")


class InvalidDbnClient(FakeClient):
    def stream_to_file(self, query: StreamQuery, output_path: Path) -> None:
        _ = query
        output_path.write_bytes(b"not dbn")


class FatalAfterTmpClient(FakeClient):
    def stream_to_file(self, query: StreamQuery, output_path: Path) -> None:
        _ = query
        output_path.write_bytes(b"partial")
        raise FatalError("fatal")


class BuggyClient(FakeClient):
    def stream_to_file(self, query: StreamQuery, output_path: Path) -> None:
        _ = (query, output_path)
        raise TypeError("programmer bug")


class RetrySummaryClient(FakeClient):
    retry_count = 7


class FreeEstimateClient(FakeClient):
    def estimate_cost(self, query: CostQuery) -> Decimal:
        self.cost_queries.append(query)
        return Decimal("0.01")


def _config(tmp_path: Path, *, yes: bool = True) -> DownloadConfig:
    return DownloadConfig(
        data_dir=tmp_path,
        symbols=("ES.FUT",),
        schemas=("mbo",),
        start=date(2026, 4, 1),
        end=date(2026, 4, 1),
        mode=RunMode.EXECUTE,
        max_cost_cents=10_000,
        yes=yes,
    )


def test_empty_dbn_file_round_trips_metadata(tmp_path: Path) -> None:
    query = StreamQuery(
        dataset=DATASET,
        symbol="ES.FUT",
        schema="mbo",
        start=date(2026, 4, 1),
        end=date(2026, 4, 2),
    )
    path = tmp_path / "empty.dbn.zst"

    write_empty_dbn_file(query, path)

    validate_dbn_metadata(query, path)


def test_empty_dbn_file_round_trips_strict_validation(tmp_path: Path) -> None:
    query = StreamQuery(
        dataset=DATASET,
        symbol="SOL.FUT",
        schema="definition",
        start=date(2025, 3, 17),
        end=date(2025, 3, 18),
    )
    path = tmp_path / "empty.dbn.zst"

    write_empty_dbn_file(query, path)

    validate_dbn_metadata(query, path, strict=True)


def test_validator_passes_unknown_dbn_header_version_to_sdk_decode(
    tmp_path: Path,
) -> None:
    query = StreamQuery(
        dataset=DATASET,
        symbol="ES.FUT",
        schema="mbo",
        start=date(2026, 4, 1),
        end=date(2026, 4, 2),
    )
    path = tmp_path / "v1.dbn.zst"
    write_empty_dbn_file(query, path)
    payload = bytearray(zstandard.ZstdDecompressor().decompress(path.read_bytes()))
    payload[3] = 4
    path.write_bytes(zstandard.ZstdCompressor().compress(bytes(payload)))

    with pytest.raises(ValidationError) as exc_info:
        validate_dbn_metadata(query, path)

    assert "unsupported DBN version" not in str(exc_info.value)
    assert "can't decode newer version" in str(exc_info.value)


def test_deep_validation_rejects_decompression_cap(tmp_path: Path) -> None:
    query = StreamQuery(
        dataset=DATASET,
        symbol="ES.FUT",
        schema="mbo",
        start=date(2026, 4, 1),
        end=date(2026, 4, 2),
    )
    path = tmp_path / "empty.dbn.zst"
    write_empty_dbn_file(query, path)

    with pytest.raises(ValidationError, match="decompressed byte cap"):
        validate_dbn_metadata(query, path, deep=True, max_decompressed_bytes=1)


def test_validator_rejects_truncated_zstd_frame(tmp_path: Path) -> None:
    query = StreamQuery(
        dataset=DATASET,
        symbol="ES.FUT",
        schema="mbo",
        start=date(2026, 4, 1),
        end=date(2026, 4, 2),
    )
    path = tmp_path / "truncated.dbn.zst"
    write_empty_dbn_file(query, path)
    path.write_bytes(path.read_bytes()[:-4])

    with pytest.raises(ValidationError):
        validate_dbn_metadata(query, path, deep=True)


class _FakeField(list[int]):
    def min(self) -> int:
        return min(self)

    def max(self) -> int:
        return max(self)


class _FakeRecords:
    def __init__(
        self,
        instrument_ids: list[int],
        ts_recv: list[int] | None = None,
        ts_event: list[int] | None = None,
    ) -> None:
        self._instrument_ids = _FakeField(instrument_ids)
        self._ts_recv = _FakeField(ts_recv or [])
        self._ts_event = _FakeField(ts_event or [])
        names = ["instrument_id"]
        if ts_recv is not None:
            names.append("ts_recv")
        if ts_event is not None:
            names.append("ts_event")
        self.dtype = SimpleNamespace(names=tuple(names))

    def __len__(self) -> int:
        return max(len(self._ts_recv), len(self._ts_event), len(self._instrument_ids))

    def __getitem__(self, key: str) -> _FakeField:
        if key == "instrument_id":
            return self._instrument_ids
        if key == "ts_recv":
            return self._ts_recv
        if key == "ts_event":
            return self._ts_event
        raise KeyError(key)


class _FakeStore:
    def __init__(
        self,
        records: list[_FakeRecords],
        mappings: dict[str, list[dict[str, str]]],
    ) -> None:
        self.metadata = SimpleNamespace(mappings=mappings, stype_out="instrument_id")
        self._records = records

    def to_ndarray(self, *, count: int) -> list[_FakeRecords]:
        _ = count
        return self._records


def _patch_dbn_store(
    monkeypatch: pytest.MonkeyPatch,
    store: _FakeStore,
) -> None:
    def from_file(_path: object) -> _FakeStore:
        return store

    monkeypatch.setattr(dbn.databento.DBNStore, "from_file", from_file)


def test_strict_validation_checks_records(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    query = StreamQuery(
        dataset=DATASET,
        symbol="ES.FUT",
        schema="mbo",
        start=date(2026, 4, 1),
        end=date(2026, 4, 2),
    )
    path = tmp_path / "strict.dbn.zst"
    write_empty_dbn_file(query, path)
    records = _FakeRecords(
        [123, 123],
        [1775001600000000000, 1775001600000000001],
    )
    store = _FakeStore([records], {"ESM6": [{"symbol": "123"}]})
    _patch_dbn_store(monkeypatch, store)

    validate_dbn_metadata(query, path, strict=True)


def test_strict_validation_checks_real_dbn_store(tmp_path: Path) -> None:
    query = StreamQuery(
        dataset=DATASET,
        symbol="ES.FUT",
        schema="mbo",
        start=date(2026, 4, 1),
        end=date(2026, 4, 2),
    )
    path = tmp_path / "real-sample.dbn.zst"
    start_ns = 1775001600000000000
    metadata = databento_dbn.Metadata(
        dataset=DATASET,
        start=start_ns,
        end=1775088000000000000,
        stype_in=databento_dbn.SType.from_str("parent"),
        stype_out=databento_dbn.SType.from_str("instrument_id"),
        schema=databento_dbn.Schema.from_str("mbo"),
        symbols=["ES.FUT"],
        mappings=cast(
            "Sequence[SymbolMapping]",
            [
                _SymbolMapping(
                    raw_symbol="ESM6",
                    intervals=[
                        _MappingInterval(
                            start_date=date(2026, 4, 1),
                            end_date=date(2026, 4, 2),
                            symbol="123",
                        )
                    ],
                )
            ],
        ),
    )
    records = [
        databento_dbn.MBOMsg(
            1,
            123,
            start_ns,
            111,
            1000000000,
            1,
            databento_dbn.Action.from_str("A"),
            databento_dbn.Side.from_str("B"),
            start_ns + 1,
        ),
        databento_dbn.MBOMsg(
            1,
            123,
            start_ns + 2,
            112,
            1000000001,
            1,
            databento_dbn.Action.from_str("A"),
            databento_dbn.Side.from_str("B"),
            start_ns + 3,
        ),
    ]
    payload = metadata.encode() + b"".join(bytes(record) for record in records)
    path.write_bytes(zstandard.ZstdCompressor().compress(payload))

    validate_dbn_metadata(query, path, strict=True)


def test_strict_validation_uses_ts_event_when_ts_recv_absent(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    query = StreamQuery(
        dataset=DATASET,
        symbol="ES.FUT",
        schema="mbo",
        start=date(2026, 4, 1),
        end=date(2026, 4, 2),
    )
    path = tmp_path / "strict.dbn.zst"
    write_empty_dbn_file(query, path)
    records = _FakeRecords([123], ts_event=[1775088000000000000])
    store = _FakeStore([records], {"ESM6": [{"symbol": "123"}]})
    _patch_dbn_store(monkeypatch, store)

    with pytest.raises(ValidationError, match="ts_event outside requested UTC day"):
        validate_dbn_metadata(query, path, strict=True)


def test_strict_validation_rejects_backward_ts_recv(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    query = StreamQuery(
        dataset=DATASET,
        symbol="ES.FUT",
        schema="mbo",
        start=date(2026, 4, 1),
        end=date(2026, 4, 2),
    )
    path = tmp_path / "strict.dbn.zst"
    write_empty_dbn_file(query, path)
    records = _FakeRecords(
        [123, 123],
        [1775001600000000001, 1775001600000000000],
    )
    store = _FakeStore([records], {"ESM6": [{"symbol": "123"}]})
    _patch_dbn_store(monkeypatch, store)

    with pytest.raises(ValidationError, match="moved backwards"):
        validate_dbn_metadata(query, path, strict=True)


def test_strict_validation_rejects_unmapped_instrument_id(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    query = StreamQuery(
        dataset=DATASET,
        symbol="ES.FUT",
        schema="mbo",
        start=date(2026, 4, 1),
        end=date(2026, 4, 2),
    )
    path = tmp_path / "strict.dbn.zst"
    write_empty_dbn_file(query, path)
    records = _FakeRecords([123], [1775001600000000000])
    store = _FakeStore([records], {"ESM6": [{"symbol": "456"}]})
    _patch_dbn_store(monkeypatch, store)

    with pytest.raises(ValidationError, match="missing from symbology"):
        validate_dbn_metadata(query, path, strict=True)


def test_strict_validation_rejects_missing_mappings(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    query = StreamQuery(
        dataset=DATASET,
        symbol="ES.FUT",
        schema="mbo",
        start=date(2026, 4, 1),
        end=date(2026, 4, 2),
    )
    path = tmp_path / "strict.dbn.zst"
    write_empty_dbn_file(query, path)
    records = _FakeRecords([123], [1775001600000000000])
    _patch_dbn_store(monkeypatch, _FakeStore([records], {}))

    with pytest.raises(ValidationError, match="no symbology mappings"):
        validate_dbn_metadata(query, path, strict=True)


def test_strict_validation_rejects_out_of_bounds_ts_recv(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    query = StreamQuery(
        dataset=DATASET,
        symbol="ES.FUT",
        schema="mbo",
        start=date(2026, 4, 1),
        end=date(2026, 4, 2),
    )
    path = tmp_path / "strict.dbn.zst"
    write_empty_dbn_file(query, path)
    records = _FakeRecords([123], [1775088000000000000])
    store = _FakeStore([records], {"ESM6": [{"symbol": "123"}]})
    _patch_dbn_store(monkeypatch, store)

    with pytest.raises(ValidationError, match="outside requested UTC day"):
        validate_dbn_metadata(query, path, strict=True)


def test_strict_validation_ignores_zero_timestamp_sentinel_for_bounds(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    query = StreamQuery(
        dataset=DATASET,
        symbol="ES.FUT",
        schema="mbo",
        start=date(2026, 4, 1),
        end=date(2026, 4, 2),
    )
    path = tmp_path / "strict.dbn.zst"
    write_empty_dbn_file(query, path)
    records = _FakeRecords(
        [0, 123],
        [0, 1775001600000000000],
    )
    store = _FakeStore([records], {"ESM6": [{"symbol": "123"}]})
    _patch_dbn_store(monkeypatch, store)

    validate_dbn_metadata(query, path, strict=True)


def test_strict_validation_ignores_system_instrument_id_for_symbology(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    query = StreamQuery(
        dataset=DATASET,
        symbol="ES.FUT",
        schema="mbo",
        start=date(2026, 4, 1),
        end=date(2026, 4, 2),
    )
    path = tmp_path / "strict.dbn.zst"
    write_empty_dbn_file(query, path)
    records = _FakeRecords(
        [0, 123],
        [1775001600000000000, 1775001600000000001],
    )
    store = _FakeStore([records], {"ESM6": [{"symbol": "123"}]})
    _patch_dbn_store(monkeypatch, store)

    validate_dbn_metadata(query, path, strict=True)


def test_strict_validation_allows_zero_record_no_data(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    query = StreamQuery(
        dataset=DATASET,
        symbol="ES.FUT",
        schema="mbo",
        start=date(2026, 4, 1),
        end=date(2026, 4, 2),
    )
    path = tmp_path / "strict.dbn.zst"
    write_empty_dbn_file(query, path)
    _patch_dbn_store(monkeypatch, _FakeStore([], {}))

    validate_dbn_metadata(query, path, strict=True)


def test_mapped_instrument_ids_accepts_object_intervals() -> None:
    mappings: object = {"ESM6": [SimpleNamespace(symbol="123")]}

    assert dbn._mapped_instrument_ids(mappings) == {123}


def test_mapped_instrument_ids_rejects_malformed_shapes() -> None:
    mappings: object = [
        "not intervals",
        [{"not_symbol": "123"}],
        [{"symbol": "not numeric"}],
        [SimpleNamespace(symbol=None)],
    ]

    with pytest.raises(ValidationError, match="symbology"):
        dbn._mapped_instrument_ids(mappings)


def test_stream_one_materializes_no_data_file(tmp_path: Path) -> None:
    config = _config(tmp_path)
    client = FakeClient(no_data=True)
    item = WorkItem(symbol="ES.FUT", schema="mbo", day=date(2026, 4, 1))

    outcome, _label = _stream_one(config, client, item)

    path = canonical_path(tmp_path, "ES.FUT", "mbo", date(2026, 4, 1))
    query = StreamQuery(
        dataset=DATASET,
        symbol="ES.FUT",
        schema="mbo",
        start=date(2026, 4, 1),
        end=date(2026, 4, 2),
    )
    assert outcome == "no_data"
    validate_dbn_metadata(query, path)


def test_stream_one_reports_retryable_failure(tmp_path: Path) -> None:
    config = _config(tmp_path)
    item = WorkItem(symbol="ES.FUT", schema="mbo", day=date(2026, 4, 1))

    outcome, label = _stream_one(config, RetryClient(), item)

    assert outcome == "failed"
    assert "retryable error" in label


def test_stream_one_reports_validation_failure(tmp_path: Path) -> None:
    config = _config(tmp_path)
    item = WorkItem(symbol="ES.FUT", schema="mbo", day=date(2026, 4, 1))

    outcome, label = _stream_one(config, InvalidDbnClient(), item)

    assert outcome == "failed"
    assert "validation error" in label


def test_stream_one_removes_tmp_on_fatal_error(tmp_path: Path) -> None:
    config = _config(tmp_path)
    item = WorkItem(symbol="ES.FUT", schema="mbo", day=date(2026, 4, 1))

    with pytest.raises(FatalError):
        _stream_one(config, FatalAfterTmpClient(), item)

    dest = canonical_path(tmp_path, "ES.FUT", "mbo", date(2026, 4, 1))
    assert not dest.with_name(f".{dest.stem}.tmp").exists()


def test_stream_one_reraises_unexpected_exceptions(tmp_path: Path) -> None:
    config = _config(tmp_path)
    item = WorkItem(symbol="ES.FUT", schema="mbo", day=date(2026, 4, 1))

    with pytest.raises(TypeError, match="programmer bug"):
        _stream_one(config, BuggyClient(), item)


def test_cost_estimation_uses_only_missing_days() -> None:
    client = FakeClient()
    work = [
        WorkItem(symbol="ES.FUT", schema="mbo", day=date(2026, 4, 1)),
        WorkItem(symbol="ES.FUT", schema="mbo", day=date(2026, 4, 3)),
    ]

    estimates = _estimate_costs(client, work, max_workers=1)

    assert [(query.start, query.end) for query in client.cost_queries] == [
        (date(2026, 4, 1), date(2026, 4, 2)),
        (date(2026, 4, 3), date(2026, 4, 4)),
    ]
    assert estimates[0].cost_cents == 14
    assert estimates[0].size_bytes == 22


def test_cost_estimation_rounds_once_after_range_aggregation() -> None:
    class HalfCentClient(FakeClient):
        def estimate_cost(self, query: CostQuery) -> Decimal:
            self.cost_queries.append(query)
            return Decimal("0.005")

    client = HalfCentClient()
    work = [
        WorkItem(symbol="ES.FUT", schema="mbo", day=date(2026, 4, 1)),
        WorkItem(symbol="ES.FUT", schema="mbo", day=date(2026, 4, 3)),
    ]

    estimates = _estimate_costs(client, work, max_workers=1)

    assert estimates[0].cost_cents == 1


def test_cost_estimation_merges_contiguous_missing_days() -> None:
    client = FakeClient()
    work = [
        WorkItem(symbol="ES.FUT", schema="mbo", day=date(2026, 4, 1)),
        WorkItem(symbol="ES.FUT", schema="mbo", day=date(2026, 4, 2)),
        WorkItem(symbol="ES.FUT", schema="mbo", day=date(2026, 4, 3)),
    ]

    estimates = _estimate_costs(client, work, max_workers=1)

    assert [(query.start, query.end) for query in client.cost_queries] == [
        (date(2026, 4, 1), date(2026, 4, 4)),
    ]
    assert estimates[0].cost_cents == 7
    assert estimates[0].size_bytes == 11


def test_cost_estimation_cancels_pending_ranges_on_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[str] = []

    def fake_estimate_range(
        _client: DownloaderClient,
        symbol: str,
        schema: str,
        start: date,
        end: date,
    ) -> tuple[str, str, Decimal, int]:
        _ = (schema, start, end)
        calls.append(symbol)
        if symbol == "ES.FUT":
            raise RetryableError("metadata failed")
        raise AssertionError("pending range should have been cancelled")

    monkeypatch.setattr(runner, "_estimate_range", fake_estimate_range)
    work = [
        WorkItem(symbol="ES.FUT", schema="mbo", day=date(2026, 4, 1)),
        WorkItem(symbol="NQ.FUT", schema="mbo", day=date(2026, 4, 1)),
    ]

    with pytest.raises(RetryableError):
        _estimate_costs(FakeClient(), work, max_workers=1)

    assert calls == ["ES.FUT"]


def test_run_download_requires_cost_cap_for_paid_download(tmp_path: Path) -> None:
    config = _config(tmp_path).model_copy(update={"max_cost_cents": None})
    client = FakeClient()

    with pytest.raises(SystemExit) as exc_info:
        _run_download(config, client, Console(record=True))

    assert exc_info.value.code == 2
    assert client.streamed == []


def test_cost_cap_refusal_is_visible_under_quiet(tmp_path: Path) -> None:
    config = _config(tmp_path).model_copy(update={"max_cost_cents": None})
    client = FakeClient()
    quiet_console = Console(record=True, quiet=True)
    error_console = Console(record=True)

    with pytest.raises(SystemExit) as exc_info:
        _run_download(config, client, quiet_console, error_console)

    assert exc_info.value.code == 2
    assert "planning cap" in error_console.export_text()
    assert quiet_console.export_text() == ""


def test_cost_cap_rejects_estimate_above_limit(tmp_path: Path) -> None:
    config = _config(tmp_path).model_copy(update={"max_cost_cents": 1})

    with pytest.raises(SystemExit) as exc_info:
        _check_cost_cap(config, 2, Console(record=True))

    assert exc_info.value.code == 2


def test_zero_cost_cap_requires_explicit_free_only_mode(tmp_path: Path) -> None:
    config = _config(tmp_path).model_copy(
        update={"max_cost_cents": 0, "allow_free_only": False}
    )
    console = Console(record=True)

    with pytest.raises(SystemExit) as exc_info:
        _check_cost_cap(config, 0, console)

    assert exc_info.value.code == 2
    assert "allow-free-only" in console.export_text()


def test_bucket_cost_caps_share_zero_cap_refusal(tmp_path: Path) -> None:
    config = _config(tmp_path).model_copy(
        update={"max_cost_cents": 0, "allow_free_only": False}
    )
    console = Console(record=True)

    with pytest.raises(SystemExit) as exc_info:
        _check_bucket_cost_caps(config, [], console)

    assert exc_info.value.code == 2
    assert "allow-free-only" in console.export_text()


def test_zero_cost_cap_allows_explicit_free_only_estimate(tmp_path: Path) -> None:
    config = _config(tmp_path).model_copy(
        update={"max_cost_cents": 0, "allow_free_only": True}
    )

    _check_cost_cap(config, 0, Console(record=True))


def test_cost_cap_required_even_for_zero_estimate(tmp_path: Path) -> None:
    config = _config(tmp_path).model_copy(update={"max_cost_cents": None})

    with pytest.raises(SystemExit) as exc_info:
        _check_cost_cap(config, 0, Console(record=True))

    assert exc_info.value.code == 2


def test_bucket_cost_cap_rejects_single_expensive_bucket(tmp_path: Path) -> None:
    config = _config(tmp_path).model_copy(update={"max_cost_cents_per_bucket": 100})
    estimates = [
        CostEstimate(
            symbol="ES.FUT",
            schema="mbo",
            cost_cents=101,
            size_bytes=1,
            cost_dollars=Decimal("1.01"),
        )
    ]

    with pytest.raises(SystemExit) as exc_info:
        _check_bucket_cost_caps(config, estimates, Console(record=True))

    assert exc_info.value.code == 2


def test_bucket_cost_cap_rejects_nonzero_bucket_under_free_only(
    tmp_path: Path,
) -> None:
    config = _config(tmp_path).model_copy(
        update={"max_cost_cents": 0, "allow_free_only": True}
    )
    estimates = [
        CostEstimate(
            symbol="ES.FUT",
            schema="mbo",
            cost_cents=1,
            size_bytes=1,
            cost_dollars=Decimal("0.01"),
        )
    ]

    with pytest.raises(SystemExit) as exc_info:
        _check_bucket_cost_caps(config, estimates, Console(record=True))

    assert exc_info.value.code == 2


def test_bucket_cost_cap_warns_on_large_global_cap_fraction(tmp_path: Path) -> None:
    config = _config(tmp_path).model_copy(update={"max_cost_cents": 1000})
    console = Console(record=True)
    estimates = [
        CostEstimate(
            symbol="ES.FUT",
            schema="mbo",
            cost_cents=251,
            size_bytes=1,
            cost_dollars=Decimal("2.51"),
        )
    ]

    _check_bucket_cost_caps(config, estimates, console)

    assert "exceeds 25%" in console.export_text()


def test_disk_space_check_rejects_insufficient_free_space(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    usage = shutil.disk_usage(tmp_path)

    def tiny_usage(_path: Path) -> shutil._ntuple_diskusage:
        return usage._replace(free=1)

    monkeypatch.setattr(
        runner.shutil,
        "disk_usage",
        tiny_usage,
    )

    with pytest.raises(SystemExit) as exc_info:
        _check_disk_space(tmp_path, 2, Console(record=True))

    assert exc_info.value.code == 2


def test_runtime_config_rejects_file_data_dir(tmp_path: Path) -> None:
    data_dir = tmp_path / "data-file"
    data_dir.write_text("not a directory", encoding="utf-8")
    config = _config(tmp_path).model_copy(update={"data_dir": data_dir})

    with pytest.raises(FatalError, match="data_dir must be a directory"):
        _validate_runtime_config(config)


def test_runtime_config_preserves_write_probe_error(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    config = _config(tmp_path)

    def fail_fsync(_fd: int) -> NoReturn:
        raise OSError("fsync failed")

    monkeypatch.setattr(runner.os, "fsync", fail_fsync)

    with pytest.raises(FatalError, match="data_dir is not writable"):
        _validate_runtime_config(config)


def test_exclusive_run_lock_rejects_concurrent_run(tmp_path: Path) -> None:
    with (
        _exclusive_run_lock(tmp_path, "first-run", Console(record=True)),
        pytest.raises(SystemExit) as exc_info,
        _exclusive_run_lock(tmp_path, "second-run", Console(record=True)),
    ):
        pass

    assert exc_info.value.code == 2


def test_exclusive_run_lock_ignores_unlocked_stale_file(tmp_path: Path) -> None:
    lock = tmp_path / ".run.lock"
    lock.write_text(
        '{"host": "old-host", "pid": 999999999}',
        encoding="utf-8",
    )

    with _exclusive_run_lock(tmp_path, "run-id", Console(record=True)):
        assert lock.exists()
        payload = lock.read_text(encoding="utf-8")
        assert "run-id" in payload
        assert "host" not in payload
        assert "user" not in payload
        assert "pid" not in payload


def test_run_started_log_includes_pid_for_lock_correlation(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    events: list[tuple[str, dict[str, object]]] = []

    class Logger:
        def debug(self, event: str, **kwargs: object) -> None:
            events.append((event, kwargs))

        def info(self, event: str, **kwargs: object) -> None:
            events.append((event, kwargs))

        def warning(self, event: str, **kwargs: object) -> None:
            events.append((event, kwargs))

    config = _config(tmp_path).model_copy(update={"mode": RunMode.DRY_RUN})
    monkeypatch.setattr(runner, "LOGGER", Logger())

    _run_download(config, FakeClient(), Console(record=True))

    run_started = next(kwargs for event, kwargs in events if event == "run_started")
    assert run_started["pid"] == os.getpid()
    assert isinstance(run_started["run_id"], str)


def test_fsync_directory_ignores_unsupported_directory_fsync(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    def fail_open(_path: Path, _flags: int) -> NoReturn:
        raise OSError("directory fsync unsupported")

    monkeypatch.setattr(runner.os, "open", fail_open)

    tracker = _DirectoryFsyncTracker()
    other_tracker = _DirectoryFsyncTracker()

    _fsync_directory(tmp_path, tracker)

    assert tracker.count() == 1
    assert other_tracker.count() == 0


def test_windows_lock_uses_same_byte_for_acquire_and_release(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    offsets: list[tuple[str, int]] = []

    class FakeMsvcrt:
        LK_NBLCK = 1
        LK_UNLCK = 2

        @staticmethod
        def locking(fd: int, mode: int, size: int) -> None:
            _ = (fd, size)
            label = "lock" if mode == FakeMsvcrt.LK_NBLCK else "unlock"
            offsets.append((label, lock_file.tell()))

    import sys

    monkeypatch.setattr(runner.os, "name", "nt")
    monkeypatch.setitem(sys.modules, "msvcrt", FakeMsvcrt)
    lock_file = (tmp_path / ".run.lock").open("a+", encoding="utf-8")
    try:
        lock_file.write("existing payload")
        lock_file.flush()

        assert runner._try_lock_file(lock_file)
        runner._unlock_file(lock_file)
    finally:
        lock_file.close()

    assert offsets == [
        ("lock", runner._WINDOWS_LOCK_OFFSET),
        ("unlock", runner._WINDOWS_LOCK_OFFSET),
    ]


def test_run_download_validates_cached_files_without_prompt(tmp_path: Path) -> None:
    config = _config(tmp_path)
    client = FakeClient()
    path = canonical_path(tmp_path, "ES.FUT", "mbo", date(2026, 4, 1))
    path.parent.mkdir(parents=True)
    write_empty_dbn_file(
        StreamQuery(
            dataset=DATASET,
            symbol="ES.FUT",
            schema="mbo",
            start=date(2026, 4, 1),
            end=date(2026, 4, 2),
        ),
        path,
    )

    _run_download(config, client, Console(record=True))

    assert client.cost_queries == []
    assert client.streamed == []


def test_run_download_skips_cached_validation_by_default(tmp_path: Path) -> None:
    config = _config(tmp_path)
    path = canonical_path(tmp_path, "ES.FUT", "mbo", date(2026, 4, 1))
    path.parent.mkdir(parents=True)
    path.write_bytes(b"not dbn")

    with pytest.raises(SystemExit) as exc_info:
        _run_download(config, FakeClient(), Console(record=True))

    assert exc_info.value.code == 5


def test_run_download_can_validate_cached_files(tmp_path: Path) -> None:
    config = _config(tmp_path).model_copy(update={"validate_cached": True})
    path = canonical_path(tmp_path, "ES.FUT", "mbo", date(2026, 4, 1))
    path.parent.mkdir(parents=True)
    path.write_bytes(b"not dbn")

    with pytest.raises(SystemExit) as exc_info:
        _run_download(config, FakeClient(), Console(record=True))

    assert exc_info.value.code == 5


def test_stream_missing_handles_keyboard_interrupt_cleanly(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    def interrupting_as_completed(
        _futures: list[object],
    ) -> NoReturn:
        raise KeyboardInterrupt

    monkeypatch.setattr(runner, "as_completed", interrupting_as_completed)
    config = _config(tmp_path)
    work = [WorkItem(symbol="ES.FUT", schema="mbo", day=date(2026, 4, 1))]

    with pytest.raises(InterruptedDownloadError):
        _stream_missing(config, FakeClient(), Console(record=True), work)


def test_stream_missing_quiet_suppresses_no_data_rows(tmp_path: Path) -> None:
    config = _config(tmp_path)
    console = Console(record=True, quiet=True)
    work = [WorkItem(symbol="ES.FUT", schema="mbo", day=date(2026, 4, 1))]

    result = _stream_missing(config, FakeClient(no_data=True), console, work)

    assert result.no_data == 1
    assert console.export_text() == ""


def test_stream_missing_quiet_routes_failed_rows_to_error_console(
    tmp_path: Path,
) -> None:
    class RetryClient(FakeClient):
        def stream_to_file(self, query: StreamQuery, output_path: Path) -> None:
            _ = (query, output_path)
            raise RetryableError("temporary")

    config = _config(tmp_path)
    console = Console(record=True, quiet=True)
    error_console = Console(record=True)
    work = [WorkItem(symbol="ES.FUT", schema="mbo", day=date(2026, 4, 1))]

    result = _stream_missing(
        config,
        RetryClient(),
        console,
        work,
        error_console=error_console,
    )

    assert result.failed == 1
    assert console.export_text() == ""
    assert "retryable error: temporary" in error_console.export_text()


def test_download_result_enforces_accounting_invariant() -> None:
    with pytest.raises(ValueError, match="invariant"):
        DownloadResult(total=1, placed=1, cached=0, no_data=1, failed=0)
    with pytest.raises(ValueError, match="non-negative"):
        DownloadResult(total=1, placed=0, cached=-1, no_data=0, failed=0)


def test_money_rejects_negative_cents() -> None:
    with pytest.raises(ValueError, match="non-negative"):
        _money(-1)


def test_bytes_formats_tib() -> None:
    assert _bytes(2 * 1_099_511_627_776) == "2.00 TiB"


def test_cost_table_shows_expensive_rows_first() -> None:
    console = Console(record=True, width=120)
    estimates = [
        CostEstimate(
            symbol="ES.FUT",
            schema="definition",
            cost_cents=0,
            size_bytes=1,
            cost_dollars=Decimal("0"),
        ),
        CostEstimate(
            symbol="NQ.FUT",
            schema="mbo",
            cost_cents=200,
            size_bytes=1,
            cost_dollars=Decimal("2"),
        ),
        CostEstimate(
            symbol="CL.FUT",
            schema="mbo",
            cost_cents=100,
            size_bytes=1,
            cost_dollars=Decimal("1"),
        ),
    ]

    _print_costs(console, estimates, total_cents=300, max_cost_cents=None)

    output = console.export_text()
    assert output.index("NQ.FUT") < output.index("CL.FUT") < output.index("ES.FUT")


def test_config_rejects_invalid_schema_symbol_and_date_order(tmp_path: Path) -> None:
    with pytest.raises(PydanticValidationError):
        DownloadConfig(
            data_dir=tmp_path,
            symbols=("ES",),
            schemas=("mbo",),
            start=date(2026, 4, 1),
            end=date(2026, 4, 1),
            mode=RunMode.DRY_RUN,
        )
    with pytest.raises(PydanticValidationError):
        DownloadConfig(
            data_dir=tmp_path,
            symbols=("ES.FUT",),
            schemas=("trades",),
            start=date(2026, 4, 1),
            end=date(2026, 4, 1),
            mode=RunMode.DRY_RUN,
        )
    with pytest.raises(PydanticValidationError):
        DownloadConfig(
            data_dir=tmp_path,
            symbols=("ES.FUT",),
            schemas=("mbo",),
            start=date(2026, 4, 2),
            end=date(2026, 4, 1),
            mode=RunMode.DRY_RUN,
        )


def test_single_day_config_generates_one_partition(tmp_path: Path) -> None:
    config = _config(tmp_path)
    client = FakeClient()
    console = Console(record=True)

    _run_download(config, client, console)

    assert len(client.streamed) == 1
    assert client.streamed[0].end == client.streamed[0].start + timedelta(days=1)


def test_run_download_writes_ledger_and_sha256_sidecar(tmp_path: Path) -> None:
    config = _config(tmp_path)
    client = FakeClient()

    _run_download(config, client, Console(record=True))

    path = canonical_path(tmp_path, "ES.FUT", "mbo", date(2026, 4, 1))
    assert path.with_suffix(path.suffix + ".sha256").exists()
    ledger = tmp_path / "download-ledger.jsonl"
    assert ledger.exists()
    record = json.loads(ledger.read_text(encoding="utf-8"))
    assert record["ledger_schema_version"] == 3
    assert record["placed"] == 1
    assert record["exit_code"] == 0
    assert record["interrupted"] is False
    assert record["package_version"]
    assert record["retry_count_total"] == 0
    assert record["retry_count_by_operation"] == {}
    if os.name == "nt":
        assert record["directory_fsync_skipped_count"] > 0
    else:
        assert record["directory_fsync_skipped_count"] == 0


def test_run_download_persists_directory_fsync_skipped_count(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    config = _config(tmp_path)

    def fail_fsync_directory(
        path: Path,
        fsync_tracker: runner._DirectoryFsyncTracker | None = None,
    ) -> None:
        runner._record_directory_fsync_skipped(
            path,
            OSError("directory fsync unsupported"),
            fsync_tracker,
        )

    monkeypatch.setattr(runner, "_fsync_directory", fail_fsync_directory)

    _run_download(config, FakeClient(), Console(record=True))

    ledger = tmp_path / "download-ledger.jsonl"
    record = json.loads(ledger.read_text(encoding="utf-8"))
    assert record["ledger_schema_version"] == 3
    assert record["exit_code"] == 0
    assert record["interrupted"] is False
    assert record["directory_fsync_skipped_count"] > 0


def test_run_download_records_partial_failure_exit_code(tmp_path: Path) -> None:
    config = _config(tmp_path)

    with pytest.raises(SystemExit) as exc_info:
        _run_download(config, RetryClient(), Console(record=True))

    assert exc_info.value.code == 3
    ledger = tmp_path / "download-ledger.jsonl"
    record = json.loads(ledger.read_text(encoding="utf-8"))
    assert record["ledger_schema_version"] == 3
    assert record["failed"] == 1
    assert record["exit_code"] == 3
    assert record["interrupted"] is False


def test_run_download_records_validation_failure_exit_code(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    config = _config(tmp_path).model_copy(update={"validate_cached": True})

    def fake_validate(
        config: DownloadConfig,
        console: Console,
        work: list[WorkItem],
    ) -> int:
        _ = (config, console, work)
        return 1

    monkeypatch.setattr(runner, "_validate", fake_validate)

    with pytest.raises(SystemExit) as exc_info:
        _run_download(config, FakeClient(), Console(record=True))

    assert exc_info.value.code == 5
    ledger = tmp_path / "download-ledger.jsonl"
    record = json.loads(ledger.read_text(encoding="utf-8"))
    assert record["ledger_schema_version"] == 3
    assert record["validation_issues"] == 1
    assert record["exit_code"] == 5
    assert record["interrupted"] is False


def test_validate_only_scrubs_cached_files_without_streaming(tmp_path: Path) -> None:
    config = _config(tmp_path).model_copy(update={"validate_only": True})
    client = FakeClient()
    path = canonical_path(tmp_path, "ES.FUT", "mbo", date(2026, 4, 1))
    path.parent.mkdir(parents=True)
    query = StreamQuery(
        dataset=DATASET,
        symbol="ES.FUT",
        schema="mbo",
        start=date(2026, 4, 1),
        end=date(2026, 4, 2),
    )
    write_empty_dbn_file(query, path)
    digest = runner._sha256_file(path)
    runner._write_sha256_sidecar(path, digest)

    _run_download(config, client, Console(record=True))

    assert client.streamed == []


def test_repair_missing_sidecars_for_cached_files(tmp_path: Path) -> None:
    config = _config(tmp_path)
    path = canonical_path(tmp_path, "ES.FUT", "mbo", date(2026, 4, 1))
    path.parent.mkdir(parents=True)
    write_empty_dbn_file(
        StreamQuery(
            dataset=DATASET,
            symbol="ES.FUT",
            schema="mbo",
            start=date(2026, 4, 1),
            end=date(2026, 4, 2),
        ),
        path,
    )

    issues = _repair_missing_sidecars(
        config,
        [WorkItem(symbol="ES.FUT", schema="mbo", day=date(2026, 4, 1))],
        Console(record=True),
    )

    assert issues == 0
    assert _sha256_sidecar_path(path).exists()


def test_repair_rewrites_malformed_sidecar_after_metadata_validation(
    tmp_path: Path,
) -> None:
    config = _config(tmp_path)
    path = canonical_path(tmp_path, "ES.FUT", "mbo", date(2026, 4, 1))
    path.parent.mkdir(parents=True)
    write_empty_dbn_file(
        StreamQuery(
            dataset=DATASET,
            symbol="ES.FUT",
            schema="mbo",
            start=date(2026, 4, 1),
            end=date(2026, 4, 2),
        ),
        path,
    )
    _sha256_sidecar_path(path).write_text("", encoding="ascii")

    issues = _repair_missing_sidecars(
        config,
        [WorkItem(symbol="ES.FUT", schema="mbo", day=date(2026, 4, 1))],
        Console(record=True),
    )

    assert issues == 0
    _validate_sha256_sidecar(path)


def test_repair_verifies_valid_sidecar_without_rewriting(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    config = _config(tmp_path)
    path = canonical_path(tmp_path, "ES.FUT", "mbo", date(2026, 4, 1))
    path.parent.mkdir(parents=True)
    write_empty_dbn_file(
        StreamQuery(
            dataset=DATASET,
            symbol="ES.FUT",
            schema="mbo",
            start=date(2026, 4, 1),
            end=date(2026, 4, 2),
        ),
        path,
    )
    digest = runner._sha256_file(path)
    runner._write_sha256_sidecar(path, digest)

    def fail_write(_path: Path, _digest: str | None = None) -> NoReturn:
        raise AssertionError("repair should not rewrite a matching sidecar")

    monkeypatch.setattr(runner, "_write_sha256_sidecar", fail_write)

    issues = _repair_missing_sidecars(
        config,
        [WorkItem(symbol="ES.FUT", schema="mbo", day=date(2026, 4, 1))],
        Console(record=True),
    )

    assert issues == 0


def test_repair_rejects_valid_but_mismatched_sidecar(tmp_path: Path) -> None:
    config = _config(tmp_path)
    path = canonical_path(tmp_path, "ES.FUT", "mbo", date(2026, 4, 1))
    path.parent.mkdir(parents=True)
    write_empty_dbn_file(
        StreamQuery(
            dataset=DATASET,
            symbol="ES.FUT",
            schema="mbo",
            start=date(2026, 4, 1),
            end=date(2026, 4, 2),
        ),
        path,
    )
    stale_digest = "0" * 64
    _sha256_sidecar_path(path).write_text(
        f"{stale_digest}  2026-04-01.dbn.zst\n",
        encoding="ascii",
    )

    issues = _repair_missing_sidecars(
        config,
        [WorkItem(symbol="ES.FUT", schema="mbo", day=date(2026, 4, 1))],
        Console(record=True),
    )

    assert issues == 1
    assert (
        _sha256_sidecar_path(path).read_text(encoding="ascii").startswith(stale_digest)
    )


def test_repair_uses_metadata_only_validation(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    config = _config(tmp_path).model_copy(
        update={"deep_validate": True, "strict_validate": True}
    )
    path = canonical_path(tmp_path, "ES.FUT", "mbo", date(2026, 4, 1))
    path.parent.mkdir(parents=True)
    write_empty_dbn_file(
        StreamQuery(
            dataset=DATASET,
            symbol="ES.FUT",
            schema="mbo",
            start=date(2026, 4, 1),
            end=date(2026, 4, 2),
        ),
        path,
    )

    calls: list[tuple[bool, bool]] = []

    def validate(
        query: StreamQuery,
        dbn_path: Path,
        *,
        deep: bool = False,
        strict: bool = False,
    ) -> None:
        calls.append((deep, strict))
        validate_dbn_metadata(query, dbn_path, deep=deep, strict=strict)

    monkeypatch.setattr(runner, "validate_dbn_metadata", validate)

    issues = _repair_missing_sidecars(
        config,
        [WorkItem(symbol="ES.FUT", schema="mbo", day=date(2026, 4, 1))],
        Console(record=True),
    )

    assert issues == 0
    assert calls == [(False, False)]


def test_repair_refuses_to_sign_invalid_cached_file(tmp_path: Path) -> None:
    config = _config(tmp_path)
    path = canonical_path(tmp_path, "ES.FUT", "mbo", date(2026, 4, 1))
    path.parent.mkdir(parents=True)
    path.write_bytes(b"not dbn")

    issues = _repair_missing_sidecars(
        config,
        [WorkItem(symbol="ES.FUT", schema="mbo", day=date(2026, 4, 1))],
        Console(record=True),
    )

    assert issues == 1
    assert not _sha256_sidecar_path(path).exists()


def test_stream_missing_cached_race_preserves_accounting(tmp_path: Path) -> None:
    config = _config(tmp_path)
    item = WorkItem(symbol="ES.FUT", schema="mbo", day=date(2026, 4, 1))
    path = canonical_path(tmp_path, "ES.FUT", "mbo", date(2026, 4, 1))
    path.parent.mkdir(parents=True)
    write_empty_dbn_file(
        StreamQuery(
            dataset=DATASET,
            symbol="ES.FUT",
            schema="mbo",
            start=date(2026, 4, 1),
            end=date(2026, 4, 2),
        ),
        path,
    )

    result = _stream_missing(config, FakeClient(), Console(record=True), [item])

    assert result.cached == 1
    assert result.placed == 0


def test_stream_missing_enforces_in_flight_planning_guard(tmp_path: Path) -> None:
    config = _config(tmp_path).model_copy(update={"max_cost_cents": 1})
    item = WorkItem(symbol="ES.FUT", schema="mbo", day=date(2026, 4, 1))

    with pytest.raises(FatalError, match="in-flight planned cost exceeded"):
        _stream_missing(
            config,
            FakeClient(),
            Console(record=True),
            [item],
            estimated_bytes_by_item={item: 1},
            estimated_cost_cents_by_item={item: 2},
        )


def test_run_download_enforces_in_flight_planning_guard_across_partitions(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    config = _config(tmp_path).model_copy(
        update={"end": date(2026, 4, 2), "max_cost_cents": 1}
    )
    client = FreeEstimateClient()

    def inflated_allocation(
        work: list[WorkItem],
        _estimates: list[CostEstimate],
    ) -> dict[WorkItem, int]:
        return dict.fromkeys(work, 1)

    monkeypatch.setattr(runner, "_allocate_estimated_cost_cents", inflated_allocation)

    with pytest.raises(FatalError, match="in-flight planned cost exceeded"):
        _run_download(config, client, Console(record=True))

    assert len(client.streamed) == 2


def test_existing_items_ignores_unparseable_and_tmp_names(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    events: list[tuple[str, dict[str, object]]] = []

    class Logger:
        def warning(self, event: str, **kwargs: object) -> None:
            events.append((event, kwargs))

    monkeypatch.setattr(runner, "LOGGER", Logger())
    config = _config(tmp_path).model_copy(update={"end": date(2026, 4, 3)})
    directory = tmp_path / "raw" / "glbx-mdp3" / "ES.FUT" / "mbo"
    directory.mkdir(parents=True)
    (directory / "2026-04-01.dbn.zst").write_bytes(b"valid name")
    (directory / "not-a-date.dbn.zst").write_bytes(b"invalid date")
    (directory / ".2026-04-02.dbn.zst.tmp").write_bytes(b"tmp")
    (directory / "2026-04-03.dbn.zst.tmp").write_bytes(b"tmp")
    (directory / "2026-04-03.dbn.zst.bak").write_bytes(b"backup")

    existing = _existing_items(config)

    assert existing == {WorkItem(symbol="ES.FUT", schema="mbo", day=date(2026, 4, 1))}
    assert events == [
        (
            "suspicious_archive_file_ignored",
            {"path": str(directory / "2026-04-03.dbn.zst.bak")},
        )
    ]


def test_total_estimated_cents_rounds_aggregate_decimal_once() -> None:
    estimates = [
        CostEstimate(
            symbol="ES.FUT",
            schema="definition",
            cost_cents=0,
            size_bytes=1,
            cost_dollars=Decimal("0.004"),
        ),
        CostEstimate(
            symbol="NQ.FUT",
            schema="definition",
            cost_cents=0,
            size_bytes=1,
            cost_dollars=Decimal("0.004"),
        ),
    ]

    assert _total_estimated_cents(estimates) == 1


def test_validate_one_rejects_missing_sidecar(tmp_path: Path) -> None:
    config = _config(tmp_path)
    path = canonical_path(tmp_path, "ES.FUT", "mbo", date(2026, 4, 1))
    path.parent.mkdir(parents=True)
    write_empty_dbn_file(
        StreamQuery(
            dataset=DATASET,
            symbol="ES.FUT",
            schema="mbo",
            start=date(2026, 4, 1),
            end=date(2026, 4, 2),
        ),
        path,
    )

    _item, error = _validate_one(
        config,
        WorkItem(symbol="ES.FUT", schema="mbo", day=date(2026, 4, 1)),
    )

    assert error == "SHA256 sidecar missing"


def test_validate_succeeds_when_sidecar_is_present(tmp_path: Path) -> None:
    config = _config(tmp_path)
    item = WorkItem(symbol="ES.FUT", schema="mbo", day=date(2026, 4, 1))
    path = canonical_path(tmp_path, "ES.FUT", "mbo", date(2026, 4, 1))
    path.parent.mkdir(parents=True)
    write_empty_dbn_file(
        StreamQuery(
            dataset=DATASET,
            symbol="ES.FUT",
            schema="mbo",
            start=date(2026, 4, 1),
            end=date(2026, 4, 2),
        ),
        path,
    )
    _repair_missing_sidecars(config, [item], Console(record=True))

    assert _validate(config, Console(record=True), [item]) == 0


def test_validate_sha256_sidecar_rejects_malformed_file(tmp_path: Path) -> None:
    path = tmp_path / "file.dbn.zst"
    path.write_bytes(b"payload")
    _sha256_sidecar_path(path).write_text("", encoding="ascii")

    with pytest.raises(ValidationError, match="malformed"):
        _validate_sha256_sidecar(path)


def test_validate_sha256_sidecar_rejects_wrong_filename(tmp_path: Path) -> None:
    path = tmp_path / "file.dbn.zst"
    path.write_bytes(b"payload")
    digest = runner._sha256_file(path)
    _sha256_sidecar_path(path).write_text(
        f"{digest}  other.dbn.zst\n",
        encoding="ascii",
    )

    with pytest.raises(ValidationError, match="filename mismatch"):
        _validate_sha256_sidecar(path)


def test_cost_allocation_sums_exactly_to_estimate() -> None:
    work = [
        WorkItem(symbol="ES.FUT", schema="mbo", day=date(2026, 4, 1)),
        WorkItem(symbol="ES.FUT", schema="mbo", day=date(2026, 4, 2)),
        WorkItem(symbol="ES.FUT", schema="mbo", day=date(2026, 4, 3)),
    ]
    estimates = [
        CostEstimate(
            symbol="ES.FUT",
            schema="mbo",
            cost_cents=10,
            size_bytes=0,
            cost_dollars=Decimal("0.10"),
        )
    ]

    allocation = _allocate_estimated_values(
        work,
        {
            (estimate.symbol, estimate.schema): estimate.cost_cents
            for estimate in estimates
        },
    )

    assert sum(allocation.values()) == 10
    assert sorted(allocation.values()) == [3, 3, 4]


def test_cost_allocation_rejects_missing_estimate() -> None:
    work = [WorkItem(symbol="ES.FUT", schema="mbo", day=date(2026, 4, 1))]

    with pytest.raises(RuntimeError, match="missing estimate"):
        _allocate_estimated_values(work, {})


def test_runtime_config_accepts_symlink_data_dir(tmp_path: Path) -> None:
    real_dir = tmp_path / "real"
    real_dir.mkdir()
    symlink = tmp_path / "link"
    symlink.symlink_to(real_dir, target_is_directory=True)
    config = _config(symlink)

    _validate_runtime_config(config)


def test_reject_known_network_filesystem(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setattr(
        runner,
        "_linux_mount_entries",
        lambda: [(tmp_path.resolve(strict=False), "nfs")],
    )

    with pytest.raises(FatalError, match="network filesystem"):
        _reject_known_network_filesystem(tmp_path)


def test_mount_for_path_selects_nearest_mount(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    root = tmp_path.resolve(strict=False)
    child = root / "archive"
    child.mkdir()
    monkeypatch.setattr(
        runner,
        "_linux_mount_entries",
        lambda: [(root, "apfs"), (child, "nfs")],
    )

    assert _mount_for_path(child / "data") == (child, "nfs")


def test_suspicious_all_no_data_month_fails_loudly() -> None:
    days = {date(2025, 3, 17) + timedelta(days=offset) for offset in range(7)}
    work_days = {("SOL.FUT", "mbo"): days}
    no_data_days = {("SOL.FUT", "mbo"): days}

    with pytest.raises(FatalError, match="first UTC data day"):
        _raise_on_suspicious_all_no_data(
            work_days,
            no_data_days,
            threshold_weekdays=5,
        )


def test_suspicious_all_no_data_allows_short_holiday_window() -> None:
    days = {date(2026, 4, 4), date(2026, 4, 5)}

    _raise_on_suspicious_all_no_data(
        {("ES.FUT", "mbo"): days},
        {("ES.FUT", "mbo"): days},
        threshold_weekdays=5,
    )


def test_stale_tmp_sweep_is_scoped_to_requested_symbols(tmp_path: Path) -> None:
    config = _config(tmp_path)
    scoped = tmp_path / "raw" / "glbx-mdp3" / "ES.FUT" / "mbo" / ".old.tmp"
    other = tmp_path / "raw" / "glbx-mdp3" / "NQ.FUT" / "mbo" / ".old.tmp"
    scoped.parent.mkdir(parents=True)
    other.parent.mkdir(parents=True)
    scoped.write_bytes(b"tmp")
    other.write_bytes(b"tmp")
    old_time = time.time() - 10 * 60
    os.utime(scoped, (old_time, old_time))
    os.utime(other, (old_time, old_time))

    _sweep_stale_tmp_files(config)

    assert not scoped.exists()
    assert other.exists()


def test_universe_hash_ignores_comments(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(runner, "load_default_symbols", lambda: ("ES.FUT",))
    monkeypatch.setattr(
        runner,
        "load_first_data_utc_dates",
        lambda: {"ES.FUT": date(2010, 1, 1)},
    )

    first = _universe_semantic_sha256()
    second = _universe_semantic_sha256()

    assert first == second


def test_rotate_ledger_moves_large_file(tmp_path: Path) -> None:
    ledger = tmp_path / "download-ledger.jsonl"
    ledger.write_bytes(b"x" * (1024 * 1024 + 1))

    _rotate_ledger_if_needed(ledger, rotate_mb=1)

    assert not ledger.exists()
    assert list(tmp_path.glob("download-ledger.*.jsonl"))


def test_print_retry_summary_handles_real_and_injected_clients() -> None:
    console = Console(record=True)
    client = FakeClient()
    real_like_client = RetrySummaryClient()

    _print_retry_summary(real_like_client, console)
    _print_retry_summary(client, console)

    output = console.export_text()
    assert "Databento retries: 7" in output
    assert "unavailable" in output


def test_cost_table_skips_top_ten_section_for_small_breakdowns() -> None:
    console = Console(record=True, width=140)
    estimates = [
        CostEstimate(
            symbol=f"ES{index}.FUT",
            schema="mbo",
            cost_cents=index,
            size_bytes=index + 1,
            cost_dollars=Decimal(index) / Decimal(100),
        )
        for index in range(12)
    ]

    _print_costs(console, estimates, total_cents=66, max_cost_cents=100)

    output = console.export_text()
    assert "Top 10" not in output
    assert "Full breakdown" not in output


def test_cost_table_adds_top_ten_section_for_large_runs() -> None:
    console = Console(record=True, width=140)
    estimates = [
        CostEstimate(
            symbol=f"ES{index}.FUT",
            schema="mbo",
            cost_cents=index,
            size_bytes=index + 1,
            cost_dollars=Decimal(index) / Decimal(100),
        )
        for index in range(16)
    ]

    _print_costs(console, estimates, total_cents=120, max_cost_cents=100)

    output = console.export_text()
    assert "Top 10" in output
    assert "Full breakdown" in output


def test_first_data_utc_dates_clip_default_universe_work_items(tmp_path: Path) -> None:
    config = DownloadConfig(
        data_dir=tmp_path,
        symbols=("SOL.FUT",),
        schemas=("definition",),
        start=date(2020, 1, 1),
        end=date(2025, 3, 18),
        mode=RunMode.EXECUTE,
        max_cost_cents=0,
        allow_free_only=True,
        yes=True,
    )

    items = runner._all_items(config)

    assert [item.day for item in items] == [
        date(2025, 3, 17),
        date(2025, 3, 18),
    ]
    assert runner._total_partitions(config) == 2


def test_first_data_utc_dates_do_not_materialize_pre_data_utc_day(
    tmp_path: Path,
) -> None:
    config = DownloadConfig(
        data_dir=tmp_path,
        symbols=("BTC.FUT",),
        schemas=("definition",),
        start=date(2017, 12, 17),
        end=date(2017, 12, 18),
        mode=RunMode.EXECUTE,
        max_cost_cents=0,
        allow_free_only=True,
        yes=True,
    )

    items = runner._all_items(config)

    assert [item.day for item in items] == [date(2017, 12, 18)]


def test_first_data_utc_dates_skip_symbols_after_requested_end(tmp_path: Path) -> None:
    config = DownloadConfig(
        data_dir=tmp_path,
        symbols=("XRP.FUT",),
        schemas=("definition", "status"),
        start=date(2020, 1, 1),
        end=date(2025, 5, 18),
        mode=RunMode.EXECUTE,
        max_cost_cents=0,
        allow_free_only=True,
        yes=True,
    )

    assert runner._all_items(config) == []
    assert runner._total_partitions(config) == 0


def test_empty_dbn_file_bytes_are_stable(tmp_path: Path) -> None:
    query = StreamQuery(
        dataset=DATASET,
        symbol="ES.FUT",
        schema="mbo",
        start=date(2026, 4, 1),
        end=date(2026, 4, 2),
    )
    first = tmp_path / "first.dbn.zst"
    second = tmp_path / "second.dbn.zst"

    write_empty_dbn_file(query, first)
    write_empty_dbn_file(query, second)

    assert first.read_bytes() == second.read_bytes()


def test_validator_rejects_oversized_metadata_length(tmp_path: Path) -> None:
    query = StreamQuery(
        dataset=DATASET,
        symbol="ES.FUT",
        schema="mbo",
        start=date(2026, 4, 1),
        end=date(2026, 4, 2),
    )
    header = b"DBN" + bytes([3]) + (99_999_999).to_bytes(4, "little")
    path = tmp_path / "oversized.dbn.zst"
    path.write_bytes(zstandard.ZstdCompressor().compress(header))

    with pytest.raises(ValidationError, match="metadata too large"):
        validate_dbn_metadata(query, path)


def test_empty_dbn_rejects_pre_epoch_date(tmp_path: Path) -> None:
    query = StreamQuery(
        dataset=DATASET,
        symbol="ES.FUT",
        schema="mbo",
        start=date(1969, 12, 31),
        end=date(1970, 1, 1),
    )

    with pytest.raises(ValueError, match="1970-01-01"):
        write_empty_dbn_file(query, tmp_path / "pre-epoch.dbn.zst")
