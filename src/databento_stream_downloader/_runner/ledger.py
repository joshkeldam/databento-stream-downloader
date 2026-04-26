"""Durable run-ledger writing and rotation."""

from __future__ import annotations

import getpass
import hashlib
import importlib.metadata
import json
import os
import platform
import uuid
from datetime import UTC, datetime
from pathlib import Path
from typing import cast

import structlog
from rich.console import Console

from databento_stream_downloader._runner.fsio import _fsync_directory
from databento_stream_downloader._runner.types import (
    DownloaderClient,
    _DirectoryFsyncTracker,
)
from databento_stream_downloader.config import DownloadConfig
from databento_stream_downloader.constants import DATASET
from databento_stream_downloader.models import DownloadResult
from databento_stream_downloader.symbols import (
    load_default_symbols,
    load_first_data_utc_dates,
)

_LEDGER_FILE = "download-ledger.jsonl"
LOGGER = structlog.get_logger(__name__)


def _print_retry_summary(client: DownloaderClient, console: Console) -> None:
    retry_count = getattr(client, "retry_count", None)
    if isinstance(retry_count, int):
        console.print(f"Databento retries: {retry_count}")
    else:
        console.print("Databento retries: unavailable for injected client")


def _retry_count_total(client: DownloaderClient) -> int:
    retry_count = getattr(client, "retry_count", None)
    return retry_count if isinstance(retry_count, int) else 0


def _retry_counts_by_operation(client: DownloaderClient) -> dict[str, int]:
    retry_counts = getattr(client, "retry_counts_by_operation", None)
    if isinstance(retry_counts, dict):
        typed = cast("dict[object, object]", retry_counts)
        if all(
            isinstance(key, str) and isinstance(value, int)
            for key, value in typed.items()
        ):
            return cast("dict[str, int]", retry_counts)
    return {}


def _stream_retry_count(client: DownloaderClient) -> int:
    return _retry_counts_by_operation(client).get("stream_to_file", 0)


def _stream_attempt_count_estimated(
    result: DownloadResult,
    stream_retry_count: int,
) -> int:
    return result.placed + result.no_data + result.failed + stream_retry_count


def _attempts_by_outcome(
    result: DownloadResult,
    stream_retry_count: int,
) -> dict[str, int]:
    return {
        "placed": result.placed,
        "no_data": result.no_data,
        "failed": result.failed,
        "cached": result.cached,
        "stream_retries_from_byte_zero": stream_retry_count,
    }


def _write_run_ledger(
    *,
    config: DownloadConfig,
    client: DownloaderClient,
    fsync_tracker: _DirectoryFsyncTracker,
    run_id: str,
    result: DownloadResult,
    validation_issues: int,
    exit_code: int,
    interrupted: bool,
    elapsed_seconds: float,
    estimated_cost_cents: int,
    estimated_billable_bytes: int,
    started_at: str,
    ended_at: str,
) -> None:
    ledger_path = config.data_dir / _LEDGER_FILE
    _rotate_ledger_if_needed(ledger_path, config.ledger_rotate_mb, fsync_tracker)
    package_version = importlib.metadata.version("databento-stream-downloader")
    host = platform.node()
    user = getpass.getuser()
    universe_sha256 = _universe_semantic_sha256()
    symbols_sha256 = hashlib.sha256(
        "\n".join(config.symbols).encode("utf-8"),
    ).hexdigest()
    retry_counts_by_operation = _retry_counts_by_operation(client)
    stream_retry_count = _stream_retry_count(client)
    payload = {
        "ledger_schema_version": 4,
        "run_id": run_id,
        "dataset": DATASET,
        "package_version": package_version,
        "host": host,
        "user": user,
        "mode": config.mode.value,
        "deep_validate": config.deep_validate,
        "strict_validate": config.strict_validate,
        "validate_cached": config.validate_cached,
        "started_at": started_at,
        "ended_at": ended_at,
        "symbols": list(config.symbols),
        "schemas": list(config.schemas),
        "start": config.start.isoformat(),
        "end": config.end.isoformat(),
        "placed": result.placed,
        "cached": result.cached,
        "no_data": result.no_data,
        "failed": result.failed,
        "validation_issues": validation_issues,
        "exit_code": exit_code,
        "interrupted": interrupted,
        "estimated_cost_cents": estimated_cost_cents,
        "estimated_cost_cents_landed": result.estimated_cost_cents_landed,
        "estimated_billable_bytes": estimated_billable_bytes,
        "estimated_billable_bytes_landed": result.estimated_billable_bytes_landed,
        "retry_count_total": _retry_count_total(client),
        "retry_count_by_operation": retry_counts_by_operation,
        "stream_retry_count": stream_retry_count,
        "stream_attempt_count_estimated": _stream_attempt_count_estimated(
            result,
            stream_retry_count,
        ),
        "attempts_by_outcome": _attempts_by_outcome(result, stream_retry_count),
        "directory_fsync_skipped_count": fsync_tracker.count(),
        "elapsed_seconds": round(elapsed_seconds, 3),
        "universe_sha256": universe_sha256,
        "symbols_sha256": symbols_sha256,
    }
    ledger_path.parent.mkdir(parents=True, exist_ok=True)
    with ledger_path.open("a", encoding="utf-8") as file:
        file.write(json.dumps(payload, sort_keys=True))
        file.write("\n")
        file.flush()
        os.fsync(file.fileno())
    _fsync_directory(ledger_path.parent, fsync_tracker)


def _rotate_ledger_if_needed(
    ledger_path: Path,
    rotate_mb: int,
    fsync_tracker: _DirectoryFsyncTracker | None = None,
) -> None:
    if not ledger_path.exists():
        return
    max_bytes = rotate_mb * 1024 * 1024
    if ledger_path.stat().st_size <= max_bytes:
        return
    rotated = ledger_path.with_name(
        f"{ledger_path.stem}.{datetime.now(UTC).strftime('%Y%m%dT%H%M%SZ')}"
        f".{uuid.uuid4().hex[:8]}"
        f"{ledger_path.suffix}"
    )
    os.replace(ledger_path, rotated)
    _fsync_directory(ledger_path.parent, fsync_tracker)
    LOGGER.info(
        "ledger_rotated",
        old_path=str(ledger_path),
        new_path=str(rotated),
        rotate_mb=rotate_mb,
    )


def _universe_semantic_sha256() -> str:
    first_data_utc = {
        symbol: value.isoformat()
        for symbol, value in sorted(load_first_data_utc_dates().items())
    }
    payload = {
        "symbols": sorted(load_default_symbols()),
        "first_data_utc": first_data_utc,
    }
    return hashlib.sha256(
        json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()


__all__ = [
    "_LEDGER_FILE",
    "_print_retry_summary",
    "_retry_count_total",
    "_retry_counts_by_operation",
    "_rotate_ledger_if_needed",
    "_stream_attempt_count_estimated",
    "_stream_retry_count",
    "_universe_semantic_sha256",
    "_write_run_ledger",
]
