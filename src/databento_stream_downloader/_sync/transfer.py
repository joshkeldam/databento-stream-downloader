"""Per-item transfer operations for sync (upload/download/delete)."""

from __future__ import annotations

import os
from pathlib import Path
from typing import TYPE_CHECKING, Any

import structlog

from databento_stream_downloader._runner.fsio import _fsync_directory, _fsync_file
from databento_stream_downloader._sync.inventory import compute_local_sha256
from databento_stream_downloader._sync.types import (
    SyncItem,
    SyncOutcome,
)
from databento_stream_downloader.errors import (
    FatalError,
    RetryableError,
    ValidationError,
)

if TYPE_CHECKING:
    from databento_stream_downloader._runner.concurrency import _InFlightRegistry
    from databento_stream_downloader.s3_client import S3Client

LOGGER = structlog.get_logger(__name__)

_CONTENT_TYPES = {
    ".dbn.zst": "application/zstd",
    ".sha256": "text/plain",
    ".jsonl": "application/x-jsonlines",
}


def _content_type_for(path: Path) -> str:
    name = path.name
    for suffix, ctype in _CONTENT_TYPES.items():
        if name.endswith(suffix):
            return ctype
    return "application/octet-stream"


def upload_one(
    client: S3Client,
    item: SyncItem,
    in_flight: _InFlightRegistry[SyncItem] | None,
) -> tuple[SyncOutcome, str, int]:
    """Upload a single item. Returns (outcome, label, bytes_moved)."""
    label = item.s3_key
    token = in_flight.add(item) if in_flight is not None else None
    try:
        extra: dict[str, Any] = {"ContentType": _content_type_for(item.local_path)}
        if item.sha256 is not None:
            extra["Metadata"] = {"sha256": item.sha256}
        try:
            client.upload_file(item.local_path, item.s3_key, extra_args=extra)
        except FatalError:
            raise
        except RetryableError as exc:
            return ("failed", f"{label}: retryable error: {exc}", 0)
        except OSError as exc:
            return ("failed", f"{label}: local I/O error: {exc}", 0)
    finally:
        if in_flight is not None and token is not None:
            in_flight.remove(token)
    return ("transferred", label, item.size_bytes)


def download_one(
    client: S3Client,
    item: SyncItem,
    in_flight: _InFlightRegistry[SyncItem] | None,
    *,
    verify_sha256: bool,
    fsync_writes: bool,
) -> tuple[SyncOutcome, str, int]:
    """Download one item then atomic-replace. Returns (outcome, label, bytes)."""
    label = item.s3_key
    token = in_flight.add(item) if in_flight is not None else None
    dest = item.local_path
    tmp = dest.with_name(f".{dest.name}.tmp")
    try:
        dest.parent.mkdir(parents=True, exist_ok=True)
        try:
            client.download_file(item.s3_key, tmp)
        except FatalError:
            tmp.unlink(missing_ok=True)
            raise
        except RetryableError as exc:
            tmp.unlink(missing_ok=True)
            return ("failed", f"{label}: retryable error: {exc}", 0)
        except OSError as exc:
            tmp.unlink(missing_ok=True)
            return ("failed", f"{label}: local I/O error: {exc}", 0)

        if verify_sha256 and item.sha256 is not None:
            try:
                actual = compute_local_sha256(tmp)
            except OSError as exc:
                tmp.unlink(missing_ok=True)
                return ("failed", f"{label}: hash read error: {exc}", 0)
            if actual != item.sha256:
                tmp.unlink(missing_ok=True)
                err = ValidationError(
                    f"sha256 mismatch: expected {item.sha256}, got {actual}",
                )
                return ("failed", f"{label}: validation error: {err}", 0)

        if fsync_writes:
            _fsync_file(tmp)
        os.replace(tmp, dest)
        if fsync_writes:
            _fsync_directory(dest.parent)
    finally:
        if in_flight is not None and token is not None:
            in_flight.remove(token)
    return ("transferred", label, item.size_bytes)


def delete_one(
    client: S3Client,
    item: SyncItem,
    in_flight: _InFlightRegistry[SyncItem] | None,
    *,
    delete_remote: bool,
) -> tuple[SyncOutcome, str, int]:
    """Delete one item from the destination side. Returns (outcome, label, 0)."""
    label = item.s3_key
    token = in_flight.add(item) if in_flight is not None else None
    try:
        if delete_remote:
            try:
                client.delete_object(item.s3_key)
            except FatalError:
                raise
            except RetryableError as exc:
                return ("failed", f"{label}: retryable error: {exc}", 0)
        else:
            try:
                item.local_path.unlink(missing_ok=True)
            except OSError as exc:
                return ("failed", f"{label}: local unlink error: {exc}", 0)
    finally:
        if in_flight is not None and token is not None:
            in_flight.remove(token)
    return ("deleted", label, 0)


__all__ = ["delete_one", "download_one", "upload_one"]
