"""Coverage manifest generation for local archive and S3 state."""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import TYPE_CHECKING, Any, cast

import structlog

from databento_stream_downloader._runner.work import _iter_all_items
from databento_stream_downloader.constants import DATASET, RAW_PREFIX

if TYPE_CHECKING:
    from collections.abc import Iterable, Mapping

    from databento_stream_downloader._sync.inventory import _RemoteEntry
    from databento_stream_downloader.config import DownloadConfig

LOGGER = structlog.get_logger(__name__)
MANIFEST_FILENAME = "databento-coverage-manifest.json"
MANIFEST_SCHEMA_VERSION = 1
_CANONICAL_SUFFIX = ".dbn.zst"


@dataclass(frozen=True, slots=True)
class _ManifestFile:
    relkey: str
    symbol: str
    schema: str
    day: date
    size_bytes: int
    sha256: str | None = None


@dataclass(frozen=True, slots=True)
class _ExpectedScope:
    dates_by_group: dict[tuple[str, str], set[date]]


def write_download_coverage_manifest(
    config: DownloadConfig,
    *,
    run_id: str | None = None,
) -> Path:
    """Write local coverage after a downloader run."""
    return write_coverage_manifest(
        data_dir=config.data_dir,
        download_config=config,
        run_id=run_id,
    )


def write_sync_coverage_manifest(
    *,
    data_dir: Path,
    remote: Mapping[str, _RemoteEntry] | None,
    bucket: str,
    prefix: str,
    direction: str,
    run_id: str | None = None,
) -> Path:
    """Write local + S3 coverage after a sync run."""
    return write_coverage_manifest(
        data_dir=data_dir,
        remote=remote,
        bucket=bucket,
        prefix=prefix,
        sync_direction=direction,
        run_id=run_id,
    )


def write_coverage_manifest(
    *,
    data_dir: Path,
    download_config: DownloadConfig | None = None,
    remote: Mapping[str, _RemoteEntry] | None = None,
    bucket: str | None = None,
    prefix: str = "",
    sync_direction: str | None = None,
    run_id: str | None = None,
) -> Path:
    """Build and atomically write the coverage manifest."""
    data_dir.mkdir(parents=True, exist_ok=True)
    manifest_path = data_dir / MANIFEST_FILENAME
    previous = _read_previous_manifest_scope(manifest_path)
    local_files = _collect_local_files(data_dir)
    remote_files = _collect_remote_files(remote or {})
    expected = _expected_scope(download_config, previous, local_files, remote_files)
    payload = _build_manifest_payload(
        data_dir=data_dir,
        local_files=local_files,
        remote_files=remote_files,
        expected=expected,
        download_config=download_config,
        bucket=bucket,
        prefix=prefix,
        sync_direction=sync_direction,
        run_id=run_id,
    )
    _atomic_write_json(manifest_path, payload)
    LOGGER.info(
        "coverage_manifest_written",
        path=str(manifest_path),
        local_files=len(local_files),
        s3_files=len(remote_files),
    )
    return manifest_path


def _build_manifest_payload(
    *,
    data_dir: Path,
    local_files: dict[str, _ManifestFile],
    remote_files: dict[str, _ManifestFile],
    expected: _ExpectedScope,
    download_config: DownloadConfig | None,
    bucket: str | None,
    prefix: str,
    sync_direction: str | None,
    run_id: str | None,
) -> dict[str, Any]:
    groups = _build_groups(local_files, remote_files, expected)
    local_keys = set(local_files)
    remote_keys = set(remote_files)
    local_missing = sum(len(group["missing_local_dates"]) for group in groups)
    s3_missing = sum(len(group["missing_s3_dates"]) for group in groups)
    local_not_in_s3 = sorted(local_keys - remote_keys) if remote_files else []
    s3_not_local = sorted(remote_keys - local_keys) if remote_files else []
    payload: dict[str, Any] = {
        "manifest_schema_version": MANIFEST_SCHEMA_VERSION,
        "generated_at": datetime.now(UTC).isoformat(),
        "generator": "databento-stream-downloader",
        "dataset": DATASET,
        "data_dir": str(data_dir),
        "manifest_path": str(data_dir / MANIFEST_FILENAME),
        "run_id": run_id,
        "sync_direction": sync_direction,
        "s3": {
            "bucket": bucket,
            "prefix": prefix,
            "inventory_available": bool(remote_files),
        },
        "request": _request_payload(download_config),
        "totals": {
            "expected_partitions": sum(
                len(days) for days in expected.dates_by_group.values()
            ),
            "local_files": len(local_files),
            "local_bytes": sum(item.size_bytes for item in local_files.values()),
            "local_missing_partitions": local_missing,
            "s3_files": len(remote_files),
            "s3_bytes": sum(item.size_bytes for item in remote_files.values()),
            "s3_missing_partitions": s3_missing if remote_files else None,
            "local_files_not_in_s3": len(local_not_in_s3) if remote_files else None,
            "s3_files_not_local": len(s3_not_local) if remote_files else None,
        },
        "groups": groups,
        "local_files": [_file_payload(item) for item in local_files.values()],
        "s3_files": [_file_payload(item) for item in remote_files.values()],
        "local_files_not_in_s3": local_not_in_s3,
        "s3_files_not_local": s3_not_local,
    }
    return payload


def _build_groups(
    local_files: dict[str, _ManifestFile],
    remote_files: dict[str, _ManifestFile],
    expected: _ExpectedScope,
) -> list[dict[str, Any]]:
    local_dates_by_group = _dates_by_group(local_files.values())
    remote_dates_by_group = _dates_by_group(remote_files.values())
    group_keys = set(expected.dates_by_group)
    group_keys.update(local_dates_by_group)
    group_keys.update(remote_dates_by_group)
    groups: list[dict[str, Any]] = []
    for symbol, schema in sorted(group_keys):
        expected_dates = expected.dates_by_group.get((symbol, schema), set())
        local_dates = local_dates_by_group.get((symbol, schema), set())
        remote_dates = remote_dates_by_group.get((symbol, schema), set())
        if not expected_dates:
            expected_dates = _infer_contiguous_dates(local_dates | remote_dates)
        all_dates = expected_dates | local_dates | remote_dates
        groups.append(
            {
                "symbol": symbol,
                "schema": schema,
                "date_start": min(all_dates).isoformat() if all_dates else None,
                "date_end": max(all_dates).isoformat() if all_dates else None,
                "expected_partitions": len(expected_dates),
                "local_present": len(local_dates & expected_dates)
                if expected_dates
                else len(local_dates),
                "local_extra_dates": _iso_sorted(local_dates - expected_dates),
                "missing_local_dates": _iso_sorted(expected_dates - local_dates),
                "s3_present": len(remote_dates & expected_dates)
                if expected_dates
                else len(remote_dates),
                "s3_extra_dates": _iso_sorted(remote_dates - expected_dates),
                "missing_s3_dates": _iso_sorted(expected_dates - remote_dates),
                "local_dates_not_in_s3": _iso_sorted(local_dates - remote_dates),
                "s3_dates_not_local": _iso_sorted(remote_dates - local_dates),
            },
        )
    return groups


def _dates_by_group(
    files: Iterable[_ManifestFile],
) -> dict[tuple[str, str], set[date]]:
    dates_by_group: dict[tuple[str, str], set[date]] = {}
    for item in files:
        dates_by_group.setdefault((item.symbol, item.schema), set()).add(item.day)
    return dates_by_group


def _collect_local_files(data_dir: Path) -> dict[str, _ManifestFile]:
    root = data_dir.resolve(strict=False)
    files: dict[str, _ManifestFile] = {}
    raw_root = data_dir / RAW_PREFIX
    if not raw_root.exists():
        return files
    for path in sorted(raw_root.rglob(f"*{_CANONICAL_SUFFIX}")):
        if not path.is_file() or path.is_symlink():
            continue
        try:
            resolved = path.resolve(strict=False)
        except OSError:
            LOGGER.warning("coverage_manifest_local_resolve_failed", path=str(path))
            continue
        if not resolved.is_relative_to(root):
            LOGGER.warning("coverage_manifest_local_escape_ignored", path=str(path))
            continue
        relkey = path.relative_to(data_dir).as_posix()
        parsed = _parse_canonical_relkey(relkey)
        if parsed is None:
            continue
        symbol, schema, day = parsed
        try:
            size = path.stat().st_size
        except OSError:
            LOGGER.warning("coverage_manifest_local_stat_failed", path=str(path))
            continue
        files[relkey] = _ManifestFile(
            relkey=relkey,
            symbol=symbol,
            schema=schema,
            day=day,
            size_bytes=size,
            sha256=_read_sha256_sidecar(path),
        )
    return files


def _collect_remote_files(
    remote: Mapping[str, _RemoteEntry],
) -> dict[str, _ManifestFile]:
    files: dict[str, _ManifestFile] = {}
    for relkey, entry in sorted(remote.items()):
        parsed = _parse_canonical_relkey(relkey)
        if parsed is None:
            continue
        symbol, schema, day = parsed
        files[relkey] = _ManifestFile(
            relkey=relkey,
            symbol=symbol,
            schema=schema,
            day=day,
            size_bytes=entry.size,
            sha256=entry.sha256,
        )
    return files


def _parse_canonical_relkey(relkey: str) -> tuple[str, str, date] | None:
    prefix = RAW_PREFIX.as_posix()
    parts = relkey.split("/")
    if len(parts) != 5 or "/".join(parts[:2]) != prefix:
        return None
    symbol, schema, name = parts[2], parts[3], parts[4]
    if not name.endswith(_CANONICAL_SUFFIX):
        return None
    try:
        day = date.fromisoformat(name.removesuffix(_CANONICAL_SUFFIX))
    except ValueError:
        return None
    return symbol, schema, day


def _expected_scope(
    download_config: DownloadConfig | None,
    previous: _ExpectedScope | None,
    local_files: dict[str, _ManifestFile],
    remote_files: dict[str, _ManifestFile],
) -> _ExpectedScope:
    dates_by_group: dict[tuple[str, str], set[date]] = {}
    if previous is not None:
        _merge_dates_by_group(dates_by_group, previous.dates_by_group)
    if download_config is not None:
        configured_dates: dict[tuple[str, str], set[date]] = {}
        for item in _iter_all_items(download_config):
            configured_dates.setdefault((item.symbol, item.schema), set()).add(
                item.day,
            )
        _merge_dates_by_group(dates_by_group, configured_dates)
        return _ExpectedScope(dates_by_group=dates_by_group)

    observed: dict[tuple[str, str], set[date]] = {}
    for item in [*local_files.values(), *remote_files.values()]:
        observed.setdefault((item.symbol, item.schema), set()).add(item.day)
    for key, dates in observed.items():
        _merge_dates_by_group(dates_by_group, {key: _infer_contiguous_dates(dates)})
    return _ExpectedScope(dates_by_group=dates_by_group)


def _merge_dates_by_group(
    target: dict[tuple[str, str], set[date]],
    source: dict[tuple[str, str], set[date]],
) -> None:
    for key, dates in source.items():
        target.setdefault(key, set()).update(dates)


def _read_previous_manifest_scope(path: Path) -> _ExpectedScope | None:
    try:
        raw = cast("dict[str, Any]", json.loads(path.read_text(encoding="utf-8")))
    except OSError, json.JSONDecodeError:
        return None
    groups = raw.get("groups")
    if not isinstance(groups, list):
        return None
    dates_by_group: dict[tuple[str, str], set[date]] = {}
    for group_raw in cast("list[object]", groups):
        if not isinstance(group_raw, dict):
            continue
        group = cast("dict[str, Any]", group_raw)
        symbol_raw = group.get("symbol")
        schema_raw = group.get("schema")
        start_raw = group.get("date_start")
        end_raw = group.get("date_end")
        if not (
            isinstance(symbol_raw, str)
            and isinstance(schema_raw, str)
            and isinstance(start_raw, str)
            and isinstance(end_raw, str)
        ):
            continue
        try:
            start = date.fromisoformat(start_raw)
            end = date.fromisoformat(end_raw)
        except ValueError:
            continue
        if start > end:
            continue
        dates_by_group[(symbol_raw, schema_raw)] = set(_iter_dates(start, end))
    return _ExpectedScope(dates_by_group=dates_by_group) if dates_by_group else None


def _request_payload(config: DownloadConfig | None) -> dict[str, Any] | None:
    if config is None:
        return None
    return {
        "symbols": list(config.symbols),
        "schemas": list(config.schemas),
        "start": config.start.isoformat(),
        "end": config.end.isoformat(),
    }


def _file_payload(item: _ManifestFile) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "relkey": item.relkey,
        "symbol": item.symbol,
        "schema": item.schema,
        "day": item.day.isoformat(),
        "size_bytes": item.size_bytes,
    }
    if item.sha256 is not None:
        payload["sha256"] = item.sha256
    return payload


def _read_sha256_sidecar(path: Path) -> str | None:
    try:
        return _parse_sidecar_digest(
            path.with_suffix(path.suffix + ".sha256").read_text(encoding="ascii"),
        )
    except OSError, UnicodeDecodeError:
        return None


def _parse_sidecar_digest(text: str) -> str | None:
    try:
        digest, _ = text.split(maxsplit=1)
    except ValueError:
        return None
    digest = digest.strip().lower()
    if len(digest) != 64 or any(ch not in "0123456789abcdef" for ch in digest):
        return None
    return digest


def _infer_contiguous_dates(dates: set[date]) -> set[date]:
    if not dates:
        return set()
    return set(_iter_dates(min(dates), max(dates)))


def _iter_dates(start: date, end: date) -> Iterable[date]:
    day = start
    while day <= end:
        yield day
        day += timedelta(days=1)


def _iso_sorted(dates: set[date]) -> list[str]:
    return [day.isoformat() for day in sorted(dates)]


def _atomic_write_json(path: Path, payload: dict[str, Any]) -> None:
    tmp = path.with_name(f".{path.name}.tmp")
    with tmp.open("w", encoding="utf-8") as file:
        json.dump(payload, file, indent=2, sort_keys=True)
        file.write("\n")
        file.flush()
        os.fsync(file.fileno())
    os.replace(tmp, path)


__all__ = [
    "MANIFEST_FILENAME",
    "write_coverage_manifest",
    "write_download_coverage_manifest",
    "write_sync_coverage_manifest",
]
