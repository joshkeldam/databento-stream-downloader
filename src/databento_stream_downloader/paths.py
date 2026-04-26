"""Canonical path helpers."""

from __future__ import annotations

from datetime import date
from pathlib import Path

from databento_stream_downloader.config import SUPPORTED_SCHEMAS
from databento_stream_downloader.constants import RAW_PREFIX
from databento_stream_downloader.validators import PARENT_SYMBOL_RE

_SUPPORTED_SCHEMA_SET = frozenset(SUPPORTED_SCHEMAS)


def canonical_path(data_dir: Path, symbol: str, schema: str, day: date) -> Path:
    """Return the canonical DBN path for one partition."""
    _validate_path_parts(symbol, schema)
    root = (data_dir / RAW_PREFIX).resolve(strict=False)
    path = (root / symbol / schema / f"{day.isoformat()}.dbn.zst").resolve(strict=False)
    if not path.is_relative_to(root):
        msg = f"canonical path escapes data directory: {path}"
        raise ValueError(msg)
    return path


def _validate_path_parts(symbol: str, schema: str) -> None:
    if not PARENT_SYMBOL_RE.fullmatch(symbol):
        msg = f"invalid parent futures symbol for canonical path: {symbol}"
        raise ValueError(msg)
    if schema not in _SUPPORTED_SCHEMA_SET:
        msg = f"unsupported schema for canonical path: {schema}"
        raise ValueError(msg)
