"""Shared runner types."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from decimal import Decimal
from pathlib import Path
from threading import Lock
from typing import Literal, Protocol

from databento_stream_downloader.models import CostQuery, StreamQuery

DownloadOutcome = Literal["placed", "cached", "no_data", "failed"]


@dataclass(slots=True)
class _DirectoryFsyncTracker:
    _count: int = 0
    _lock: Lock = field(default_factory=Lock)

    def record_skip(self) -> None:
        with self._lock:
            self._count += 1

    def count(self) -> int:
        with self._lock:
            return self._count


class DownloaderClient(Protocol):
    """Client operations needed by the runner.

    Implementations should raise the downloader error taxonomy:
    FatalError/FatalConfigError for unrecoverable failures, RetryableError after
    exhausted transient retries, and DegradedError only for semantic no-data.
    """

    def estimate_cost(self, query: CostQuery) -> Decimal: ...

    def estimate_size(self, query: CostQuery) -> int: ...

    def stream_to_file(self, query: StreamQuery, output_path: Path) -> None: ...

    def write_empty_file(self, query: StreamQuery, output_path: Path) -> None: ...


@dataclass(frozen=True, slots=True)
class WorkItem:
    """One missing partition to download."""

    symbol: str
    schema: str
    day: date


__all__ = [
    "DownloadOutcome",
    "DownloaderClient",
    "WorkItem",
    "_DirectoryFsyncTracker",
]
