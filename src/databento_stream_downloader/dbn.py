"""DBN metadata helpers."""

from __future__ import annotations

import warnings
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from threading import Lock
from typing import cast

import databento
import databento_dbn
import numpy as np
import structlog
import zstandard

from databento_stream_downloader.errors import ValidationError
from databento_stream_downloader.models import StreamQuery

_STYPE_PARENT = databento_dbn.SType.from_str("parent")
_STYPE_INSTRUMENT_ID = databento_dbn.SType.from_str("instrument_id")
_DBN_MAGIC = b"DBN"
_KNOWN_DBN_VERSIONS = frozenset({1, 2, 3})
_HEADER_SIZE = 8
_CHUNK_SIZE = 1024 * 1024
_MAX_METADATA_BYTES = 16 * 1024 * 1024
_ZSTD_MAX_WINDOW_SIZE = 2**27
_DEFAULT_MAX_DECOMPRESSED_BYTES = 32 * 1024 * 1024 * 1024
_UNIX_EPOCH = date(1970, 1, 1)
_NANOS_PER_DAY = 86_400 * 1_000_000_000
_INT64_MAX_NANOS = 2**63 - 1
# Strict validation treats instrument_id=0 as a system/sentinel record, not a
# tradable instrument. Databento's public DBN docs describe instrument_id as the
# numeric instrument mapping key, but do not currently document a permanent
# reservation for 0. If Databento assigns 0 to a legitimate instrument in a
# future dataset/schema, this assumption must be revisited.
_SYSTEM_INSTRUMENT_ID = 0
LOGGER = structlog.get_logger(__name__)
_WARNED_DBN_VERSIONS: set[int] = set()
_WARNED_DBN_VERSIONS_LOCK = Lock()
_ENCODE_WARNING_LOCK = Lock()


@dataclass(frozen=True, slots=True)
class MetadataShape:
    """Comparable DBN metadata subset."""

    dataset: str
    schema: str
    start: int
    end: int
    stype_in: str
    stype_out: str
    symbols: tuple[str, ...]


def write_empty_dbn_file(query: StreamQuery, output_path: Path) -> None:
    """Write a metadata-valid zero-record DBN file.

    Callers that write canonical archive files must fsync and atomically place the
    resulting temporary file after this helper returns.
    """
    metadata = databento_dbn.Metadata(
        dataset=query.dataset,
        start=_date_to_nanos(query.start),
        end=_date_to_nanos(query.end),
        stype_in=_STYPE_PARENT,
        stype_out=_STYPE_INSTRUMENT_ID,
        schema=databento_dbn.Schema.from_str(query.schema),
        symbols=[query.symbol],
    )
    with _ENCODE_WARNING_LOCK, warnings.catch_warnings(record=True) as caught:
        payload = metadata.encode()
    for warning in caught:
        LOGGER.warning(
            "empty_dbn_encode_warning",
            category=warning.category.__name__,
            message=str(warning.message),
        )
    output_path.write_bytes(zstandard.ZstdCompressor().compress(payload))


def validate_dbn_metadata(
    query: StreamQuery,
    path: Path,
    *,
    deep: bool = False,
    strict: bool = False,
    max_decompressed_bytes: int | None = None,
) -> None:
    """Validate one canonical DBN file's metadata and optional record structure."""
    try:
        stat_result = path.stat()
    except OSError as exc:
        raise ValidationError("file missing") from exc
    if stat_result.st_size == 0:
        raise ValidationError("zero bytes")
    actual = _actual_metadata(
        _read_metadata(
            path,
            deep=deep,
            max_decompressed_bytes=max_decompressed_bytes,
        )
    )
    expected = MetadataShape(
        dataset=query.dataset,
        schema=query.schema,
        start=_date_to_nanos(query.start),
        end=_date_to_nanos(query.end),
        stype_in="parent",
        stype_out="instrument_id",
        symbols=(query.symbol,),
    )
    if actual != expected:
        msg = f"DBN metadata mismatch: expected {expected}, got {actual}"
        raise ValidationError(msg)
    if strict:
        _validate_records(
            query,
            path,
            max_decompressed_bytes=max_decompressed_bytes,
        )


def _read_metadata(
    path: Path,
    *,
    deep: bool,
    max_decompressed_bytes: int | None = None,
) -> databento_dbn.Metadata:
    try:
        with (
            path.open("rb") as raw_file,
            zstandard.ZstdDecompressor(
                max_window_size=_ZSTD_MAX_WINDOW_SIZE
            ).stream_reader(raw_file) as reader,
        ):
            header = reader.read(_HEADER_SIZE)
            if len(header) != _HEADER_SIZE:
                raise ValueError("DBN header truncated")
            if header[:3] != _DBN_MAGIC:
                raise ValueError("DBN magic mismatch")
            _warn_unknown_dbn_version(header[3], path)
            metadata_len = int.from_bytes(header[4:8], byteorder="little")
            if metadata_len > _MAX_METADATA_BYTES:
                raise ValueError(f"DBN metadata too large: {metadata_len} bytes")
            payload = header + reader.read(metadata_len)
            if len(payload) != _HEADER_SIZE + metadata_len:
                raise ValueError("DBN metadata truncated")
            if deep:
                limit = max_decompressed_bytes or _DEFAULT_MAX_DECOMPRESSED_BYTES
                consumed = len(payload)
                if consumed > limit:
                    raise ValueError(
                        "DBN deep validation exceeded decompressed byte cap: "
                        f"limit={limit}"
                    )
                while chunk := reader.read(_CHUNK_SIZE):
                    consumed += len(chunk)
                    if consumed > limit:
                        raise ValueError(
                            "DBN deep validation exceeded decompressed byte cap: "
                            f"limit={limit}"
                        )
        return databento_dbn.Metadata.decode(payload)
    except Exception as exc:
        msg = f"DBN metadata decode failed: {exc}"
        raise ValidationError(msg) from exc


def _warn_unknown_dbn_version(version: int, path: Path) -> None:
    if version in _KNOWN_DBN_VERSIONS:
        return
    with _WARNED_DBN_VERSIONS_LOCK:
        if version in _WARNED_DBN_VERSIONS:
            return
        _WARNED_DBN_VERSIONS.add(version)
    LOGGER.warning(
        "unknown_dbn_version",
        version=version,
        path=str(path),
        message="attempting SDK metadata decode for forward compatibility",
    )


def _actual_metadata(metadata: databento_dbn.Metadata) -> MetadataShape:
    return MetadataShape(
        dataset=str(metadata.dataset),
        schema=str(metadata.schema),
        start=int(metadata.start),
        end=int(metadata.end),
        stype_in=str(metadata.stype_in),
        stype_out=str(metadata.stype_out),
        symbols=tuple(str(symbol) for symbol in metadata.symbols),
    )


def _validate_records(
    query: StreamQuery,
    path: Path,
    *,
    max_decompressed_bytes: int | None = None,
) -> None:
    try:
        _check_decompressed_byte_budget(
            path,
            max_decompressed_bytes=max_decompressed_bytes,
        )
        store = databento.DBNStore.from_file(path)
        if str(store.metadata.stype_out) != "instrument_id":
            raise ValueError(
                "strict validation requires DBN metadata stype_out=instrument_id"
            )
        mapped_ids = _mapped_instrument_ids(store.metadata.mappings)
        seen_ids: set[int] = set()
        last_ts_recv_by_instrument: dict[int, int] = {}
        record_count = 0
        start_ns = _date_to_nanos(query.start)
        end_ns = _date_to_nanos(query.end)

        for records in store.to_ndarray(count=65_536):
            record_count += len(records)
            names = records.dtype.names or ()
            if "instrument_id" in names:
                instrument_ids = np.asarray(records["instrument_id"])
                nonzero_instrument_ids = instrument_ids[
                    instrument_ids != _SYSTEM_INSTRUMENT_ID
                ]
                seen_ids.update(
                    int(value) for value in np.unique(nonzero_instrument_ids)
                )
            else:
                instrument_ids = None
            timestamp_field = _record_timestamp_field(names)
            if timestamp_field is None:
                continue
            timestamps = np.asarray(records[timestamp_field])
            if len(timestamps) == 0:
                continue
            nonzero_timestamps = timestamps[timestamps != 0]
            if len(nonzero_timestamps) == 0:
                continue
            first = int(nonzero_timestamps.min())
            last = int(nonzero_timestamps.max())
            # Databento Historical API ranges are documented as [start, end):
            # inclusive start, exclusive end, using ts_recv when present and
            # ts_event otherwise. A record exactly at query.end belongs to the
            # next UTC partition.
            if first < start_ns or last >= end_ns:
                raise ValueError(
                    f"record {timestamp_field} outside requested UTC day bounds "
                    "[start, end): "
                    f"min={first}, max={last}, start={start_ns}, end={end_ns}"
                )
            if instrument_ids is None or timestamp_field != "ts_recv":
                continue
            active = instrument_ids != _SYSTEM_INSTRUMENT_ID
            active_ids = instrument_ids[active]
            active_timestamps = timestamps[active]
            if len(active_ids) == 0:
                continue
            order = np.argsort(active_ids, kind="stable")
            grouped_ids = active_ids[order]
            grouped_timestamps = active_timestamps[order]
            boundaries = np.flatnonzero(grouped_ids[1:] != grouped_ids[:-1]) + 1
            starts = np.concatenate(([0], boundaries))
            ends = np.concatenate((boundaries, [len(grouped_ids)]))
            for start_index, end_index in zip(starts, ends, strict=True):
                instrument_id = int(grouped_ids[start_index])
                instrument_timestamps = grouped_timestamps[start_index:end_index]
                previous = last_ts_recv_by_instrument.get(instrument_id)
                first_ts = int(instrument_timestamps[0])
                if previous is not None and first_ts < previous:
                    raise ValueError(
                        "record ts_recv moved backwards for instrument_id "
                        f"{instrument_id}: previous={previous}, current={first_ts}"
                    )
                if np.any(instrument_timestamps[1:] < instrument_timestamps[:-1]):
                    raise ValueError(
                        "record ts_recv moved backwards for instrument_id "
                        f"{instrument_id}"
                    )
                last_ts_recv_by_instrument[instrument_id] = int(
                    instrument_timestamps[-1]
                )

        if record_count == 0:
            return
        if not mapped_ids:
            raise ValueError("non-empty DBN file has no symbology mappings")
        missing = seen_ids - mapped_ids
        if missing:
            preview = ", ".join(str(value) for value in sorted(missing)[:10])
            raise ValueError(
                f"instrument IDs missing from symbology mappings: {preview}"
            )
    except ValidationError:
        raise
    except Exception as exc:
        msg = f"DBN strict record validation failed: {exc}"
        raise ValidationError(msg) from exc


def _check_decompressed_byte_budget(
    path: Path,
    *,
    max_decompressed_bytes: int | None,
) -> None:
    limit = max_decompressed_bytes or _DEFAULT_MAX_DECOMPRESSED_BYTES
    consumed = 0
    with (
        path.open("rb") as raw_file,
        zstandard.ZstdDecompressor(max_window_size=_ZSTD_MAX_WINDOW_SIZE).stream_reader(
            raw_file
        ) as reader,
    ):
        while chunk := reader.read(_CHUNK_SIZE):
            consumed += len(chunk)
            if consumed > limit:
                raise ValueError(
                    "DBN strict validation exceeded decompressed byte cap: "
                    f"limit={limit}"
                )


def _record_timestamp_field(names: tuple[str, ...]) -> str | None:
    if "ts_recv" in names:
        return "ts_recv"
    if "ts_event" in names:
        return "ts_event"
    return None


def _mapped_instrument_ids(mappings: object) -> set[int]:
    ids: set[int] = set()
    if not mappings:
        return ids
    if isinstance(mappings, Mapping):
        mapping_values = cast("Mapping[object, object]", mappings)
        values: Iterable[object] = mapping_values.values()
    elif isinstance(mappings, Iterable) and not isinstance(mappings, str | bytes):
        values = (
            getattr(mapping, "intervals", None)
            for mapping in cast("Iterable[object]", mappings)
        )
    else:
        raise ValidationError("unexpected symbology mappings shape")
    for intervals in values:
        if not isinstance(intervals, Iterable) or isinstance(intervals, str | bytes):
            raise ValidationError("unexpected symbology mapping intervals shape")
        typed_intervals = cast("Iterable[object]", intervals)
        for interval in typed_intervals:
            if isinstance(interval, Mapping):
                typed_interval = cast("Mapping[object, object]", interval)
                raw_id = typed_interval.get("symbol")
            elif hasattr(interval, "symbol"):
                raw_id = getattr(interval, "symbol", None)
            else:
                raise ValidationError("unexpected symbology mapping interval shape")
            if raw_id is None:
                raise ValidationError("symbology mapping interval missing symbol")
            try:
                if isinstance(raw_id, int):
                    ids.add(raw_id)
                elif isinstance(raw_id, str):
                    ids.add(int(raw_id))
                else:
                    raise TypeError
            except TypeError, ValueError:
                raise ValidationError(
                    f"non-numeric symbology instrument id: {raw_id!r}"
                ) from None
    return ids


def _date_to_nanos(value: date) -> int:
    """Convert a UTC date boundary to nanoseconds since the Unix epoch."""
    if value < _UNIX_EPOCH:
        raise ValueError(f"date must be >= 1970-01-01, got {value}")
    nanos = (value - _UNIX_EPOCH).days * _NANOS_PER_DAY
    if nanos > _INT64_MAX_NANOS:
        raise ValueError(f"date exceeds int64 nanosecond range: {value}")
    return nanos
