"""Default Databento parent futures universe."""

from __future__ import annotations

import tomllib
from datetime import date
from functools import cache
from importlib.resources import files
from typing import cast

from databento_stream_downloader.validators import PARENT_SYMBOL_RE


@cache
def _universe_data() -> dict[str, object]:
    return tomllib.loads(load_universe_text())


def load_default_symbols() -> tuple[str, ...]:
    """Load the default symbol universe from package data."""
    data = _universe_data()
    raw_symbols = data["symbols"]
    if not isinstance(raw_symbols, list):
        raise RuntimeError("universe.toml must define symbols as a string list")
    symbols: list[str] = []
    for value in cast("list[object]", raw_symbols):
        if not isinstance(value, str):
            raise RuntimeError("universe.toml symbols must be strings")
        if not PARENT_SYMBOL_RE.fullmatch(value):
            raise RuntimeError(f"invalid universe.toml symbol: {value}")
        symbols.append(value)
    return tuple(symbols)


def load_first_data_utc_dates() -> dict[str, date]:
    """Load optional first UTC data days by parent symbol."""
    data = _universe_data()
    raw_dates = data.get("first_data_utc", {})
    if not isinstance(raw_dates, dict):
        raise RuntimeError("universe.toml first_data_utc must be a table")
    first_data_utc: dict[str, date] = {}
    for symbol, value in cast("dict[object, object]", raw_dates).items():
        if not isinstance(symbol, str) or not isinstance(value, str):
            raise RuntimeError("universe.toml first_data_utc entries must be strings")
        if not PARENT_SYMBOL_RE.fullmatch(symbol):
            raise RuntimeError(f"invalid first_data_utc symbol: {symbol}")
        try:
            first_data_utc[symbol] = date.fromisoformat(value)
        except ValueError as exc:
            msg = f"invalid first_data_utc date for {symbol}: {value!r}"
            raise RuntimeError(msg) from exc
    return first_data_utc


def load_universe_text() -> str:
    """Read the packaged universe TOML text."""
    path = files("databento_stream_downloader").joinpath("universe.toml")
    return path.read_text(encoding="utf-8")
