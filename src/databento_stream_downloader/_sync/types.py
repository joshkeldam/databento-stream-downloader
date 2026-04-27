"""Shared types for the S3 sync tool."""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import StrEnum
from pathlib import Path
from threading import Lock
from typing import ClassVar, Literal, Self

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from databento_stream_downloader.config import RunMode

SyncOutcome = Literal["transferred", "skipped", "deleted", "failed"]
SyncOp = Literal["transfer", "delete"]


class SyncDirection(StrEnum):
    """Sync direction relative to the local archive."""

    PUSH = "push"  # local -> s3
    PULL = "pull"  # s3 -> local


class PlanningMode(StrEnum):
    """How sync planning decides whether same-size files are equal."""

    SIZE = "size"
    SIDECAR = "sidecar"
    HEAD_METADATA = "head-metadata"


@dataclass(frozen=True, slots=True)
class SyncItem:
    """One item to transfer (or delete) during a sync run."""

    local_path: Path | None
    s3_key: str
    size_bytes: int
    op: SyncOp
    sha256: str | None = None  # local sidecar value when known


@dataclass(frozen=True, slots=True)
class SyncPlan:
    """Result of comparing local + remote inventories."""

    transfers: tuple[SyncItem, ...]
    deletes: tuple[SyncItem, ...]
    skipped: int
    extraneous: tuple[str, ...]
    total_transfer_bytes: int


@dataclass(slots=True)
class _SyncOutcomeCounts:
    """Thread-safe counter of per-item outcomes during a sync run."""

    transferred: int = 0
    skipped: int = 0
    deleted: int = 0
    failed: int = 0
    _lock: Lock = field(default_factory=Lock)

    def record(self, outcome: SyncOutcome) -> None:
        with self._lock:
            if outcome == "transferred":
                self.transferred += 1
            elif outcome == "skipped":
                self.skipped += 1
            elif outcome == "deleted":
                self.deleted += 1
            elif outcome == "failed":
                self.failed += 1

    def snapshot(self) -> tuple[int, int, int, int]:
        with self._lock:
            return (self.transferred, self.skipped, self.deleted, self.failed)


class SyncConfig(BaseModel):
    """Configuration for a sync run (push or pull)."""

    model_config: ClassVar[ConfigDict] = ConfigDict(frozen=True, strict=True)

    direction: SyncDirection
    data_dir: Path
    bucket: str
    prefix: str = ""
    region: str | None = None
    mode: RunMode
    max_workers: int = Field(default=4, ge=1, le=100)
    delete: bool = False
    verify_sha256: bool = False
    planning_mode: PlanningMode = PlanningMode.SIZE
    fsync_writes: bool = False
    yes: bool = False

    @field_validator("bucket", mode="before")
    @classmethod
    def _validate_bucket(cls, value: object) -> object:
        if not isinstance(value, str) or not value.strip():
            raise ValueError("bucket must be a non-empty string")
        return value.strip()

    @field_validator("prefix", mode="before")
    @classmethod
    def _normalize_prefix(cls, value: object) -> str:
        if value is None:
            return ""
        if not isinstance(value, str):
            raise ValueError("prefix must be a string")
        cleaned = value.strip().strip("/")
        if ".." in cleaned.split("/"):
            raise ValueError("prefix must not contain '..' segments")
        return cleaned

    @model_validator(mode="after")
    def _validate_fsync_scope(self) -> Self:
        if self.direction is SyncDirection.PUSH and self.fsync_writes:
            raise ValueError("fsync-writes only applies to pull")
        return self


__all__ = [
    "PlanningMode",
    "SyncConfig",
    "SyncDirection",
    "SyncItem",
    "SyncOp",
    "SyncOutcome",
    "SyncPlan",
    "_SyncOutcomeCounts",
]
