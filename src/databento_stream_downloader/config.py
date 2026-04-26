"""Downloader configuration models."""

from __future__ import annotations

from datetime import date
from enum import StrEnum
from pathlib import Path
from typing import ClassVar, Self, cast

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from databento_stream_downloader.validators import PARENT_SYMBOL_RE

DEFAULT_SCHEMAS: tuple[str, ...] = ("definition", "statistics", "status")
SUPPORTED_SCHEMAS: tuple[str, ...] = (*DEFAULT_SCHEMAS, "mbo")
_SUPPORTED_SCHEMA_SET = frozenset(SUPPORTED_SCHEMAS)
_MAX_RANGE_DAYS = 365 * 30


class RunMode(StrEnum):
    """Run mode for a downloader execution."""

    DRY_RUN = "dry_run"
    EXECUTE = "execute"


class DownloadConfig(BaseModel):
    """Configuration for a Databento download run."""

    model_config: ClassVar[ConfigDict] = ConfigDict(frozen=True, strict=True)

    data_dir: Path
    symbols: tuple[str, ...]
    schemas: tuple[str, ...]
    start: date
    end: date
    mode: RunMode
    max_workers: int = Field(default=4, ge=1, le=100)
    request_timeout_seconds: float = Field(default=100.0, gt=0)
    max_cost_cents: int | None = Field(default=None, ge=0)
    max_cost_cents_per_bucket: int | None = Field(default=None, ge=0)
    allow_free_only: bool = False
    ledger_rotate_mb: int = Field(default=50, ge=1)
    suspicious_no_data_weekdays: int = Field(default=5, ge=1)
    deep_validate: bool = False
    strict_validate: bool = False
    validate_cached: bool = False
    validate_only: bool = False
    write_sidecars: bool = False
    validate_on_write: bool = False
    fsync_writes: bool = False
    show_retries: bool = False
    yes: bool = False

    @property
    def requires_api_key(self) -> bool:
        """Whether this run mode needs Databento API credentials."""
        return not self.validate_only

    @field_validator("symbols")
    @classmethod
    def _validate_symbols(cls, symbols: tuple[str, ...]) -> tuple[str, ...]:
        symbols = tuple(dict.fromkeys(symbol.upper() for symbol in symbols))
        invalid = [
            symbol for symbol in symbols if not PARENT_SYMBOL_RE.fullmatch(symbol)
        ]
        if invalid:
            msg = f"invalid parent futures symbols: {', '.join(invalid)}"
            raise ValueError(msg)
        return symbols

    @field_validator("schemas")
    @classmethod
    def _validate_schemas(cls, schemas: tuple[str, ...]) -> tuple[str, ...]:
        schemas = tuple(dict.fromkeys(schemas))
        invalid = [schema for schema in schemas if schema not in _SUPPORTED_SCHEMA_SET]
        if invalid:
            msg = f"unsupported schemas: {', '.join(invalid)}"
            raise ValueError(msg)
        return schemas

    @model_validator(mode="before")
    @classmethod
    def _force_validate_on_write_for_strict_modes(cls, data: object) -> object:
        if not isinstance(data, dict):
            return data
        payload = cast("dict[str, object]", data)
        if payload.get("deep_validate") or payload.get("strict_validate"):
            return {**payload, "validate_on_write": True}
        return payload

    @model_validator(mode="after")
    def _validate_config(self) -> Self:
        if self.start > self.end:
            msg = f"start {self.start} must be <= end {self.end}"
            raise ValueError(msg)
        if (self.end - self.start).days + 1 > _MAX_RANGE_DAYS:
            msg = f"date range must be <= {_MAX_RANGE_DAYS} days"
            raise ValueError(msg)
        if not self.symbols:
            raise ValueError("symbols must not be empty")
        if not self.schemas:
            raise ValueError("schemas must not be empty")
        if self.max_cost_cents == 0 and not self.allow_free_only:
            msg = "max_cost_cents=0 requires allow_free_only=True"
            raise ValueError(msg)
        return self
