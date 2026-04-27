"""Append-only archive audit manifest."""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path
from threading import Lock
from typing import Literal

import structlog

from databento_stream_downloader._runner.types import WorkItem
from databento_stream_downloader.constants import RAW_PREFIX

LOGGER = structlog.get_logger(__name__)

ARCHIVE_MANIFEST_FILE = "archive-manifest.jsonl"
ARCHIVE_MANIFEST_SCHEMA_VERSION = 1
_MANIFEST_WRITE_LOCK = Lock()
ManifestEvent = Literal[
    "databento_downloaded",
    "databento_no_data",
    "s3_synced",
    "s3_pulled",
]


@dataclass(frozen=True, slots=True)
class ManifestPartition:
    """A manifest entry that identifies one canonical DBN partition."""

    item: WorkItem
    relkey: str


def archive_manifest_path(data_dir: Path) -> Path:
    """Return the manifest path for a data directory."""
    return data_dir / ARCHIVE_MANIFEST_FILE


def relkey_for_work_item(item: WorkItem) -> str:
    """Return the archive-relative key for a canonical DBN partition."""
    return (
        RAW_PREFIX / item.symbol / item.schema / f"{item.day.isoformat()}.dbn.zst"
    ).as_posix()


def parse_partition_relkey(relkey: str) -> ManifestPartition | None:
    """Parse an archive-relative canonical DBN key into a WorkItem."""
    parts = Path(relkey).parts
    raw_parts = RAW_PREFIX.parts
    if len(parts) != len(raw_parts) + 3:
        return None
    if parts[: len(raw_parts)] != raw_parts:
        return None
    filename = parts[-1]
    if not filename.endswith(".dbn.zst"):
        return None
    try:
        day = date.fromisoformat(filename.removesuffix(".dbn.zst"))
    except ValueError:
        return None
    item = WorkItem(symbol=parts[-3], schema=parts[-2], day=day)
    return ManifestPartition(item=item, relkey="/".join(parts))


def record_manifest_event(
    data_dir: Path,
    event: ManifestEvent,
    item: WorkItem,
    *,
    size_bytes: int | None = None,
    sha256: str | None = None,
    s3_bucket: str | None = None,
    s3_key: str | None = None,
) -> None:
    """Append and fsync one manifest event."""
    relkey = relkey_for_work_item(item)
    record: dict[str, object] = {
        "manifest_schema_version": ARCHIVE_MANIFEST_SCHEMA_VERSION,
        "event": event,
        "recorded_at": datetime.now(UTC).isoformat(),
        "relkey": relkey,
        "symbol": item.symbol,
        "schema": item.schema,
        "day": item.day.isoformat(),
    }
    if size_bytes is not None:
        record["size_bytes"] = size_bytes
    if sha256 is not None:
        record["sha256"] = sha256
    if s3_bucket is not None:
        record["s3_bucket"] = s3_bucket
    if s3_key is not None:
        record["s3_key"] = s3_key

    path = archive_manifest_path(data_dir)
    path.parent.mkdir(parents=True, exist_ok=True)
    with _MANIFEST_WRITE_LOCK:
        with path.open("a", encoding="utf-8") as file:
            file.write(json.dumps(record, sort_keys=True, separators=(",", ":")) + "\n")
            file.flush()
            os.fsync(file.fileno())
        _fsync_manifest_directory(path.parent)
    LOGGER.info("archive_manifest_recorded", manifest_event=event, relkey=relkey)


def record_manifest_path_event(
    data_dir: Path,
    event: ManifestEvent,
    path: Path,
    *,
    sha256: str | None = None,
    s3_bucket: str | None = None,
    s3_key: str | None = None,
) -> bool:
    """Append a manifest event for a canonical local DBN path.

    Returns False when the path is not a canonical archive DBN partition.
    """
    try:
        relkey = path.relative_to(data_dir).as_posix()
    except ValueError:
        return False
    partition = parse_partition_relkey(relkey)
    if partition is None:
        return False
    try:
        size_bytes = path.stat().st_size
    except OSError:
        size_bytes = None
    record_manifest_event(
        data_dir,
        event,
        partition.item,
        size_bytes=size_bytes,
        sha256=sha256,
        s3_bucket=s3_bucket,
        s3_key=s3_key,
    )
    return True


def _fsync_manifest_directory(path: Path) -> None:
    try:
        fd = os.open(path, os.O_RDONLY)
    except OSError as exc:
        LOGGER.warning("archive_manifest_directory_fsync_skipped", error=str(exc))
        return
    try:
        os.fsync(fd)
    except OSError as exc:
        LOGGER.warning("archive_manifest_directory_fsync_skipped", error=str(exc))
    finally:
        os.close(fd)


__all__ = [
    "ARCHIVE_MANIFEST_FILE",
    "ARCHIVE_MANIFEST_SCHEMA_VERSION",
    "ManifestEvent",
    "ManifestPartition",
    "archive_manifest_path",
    "parse_partition_relkey",
    "record_manifest_event",
    "record_manifest_path_event",
    "relkey_for_work_item",
]
