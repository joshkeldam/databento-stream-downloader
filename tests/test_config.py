"""Tests for standalone downloader configuration."""
# pyright: reportPrivateUsage=false

from __future__ import annotations

import argparse
from datetime import date
from decimal import Decimal
from pathlib import Path
from typing import Any, cast

import pytest
from pydantic import ValidationError as PydanticValidationError

import databento_stream_downloader as dsd
from databento_stream_downloader import symbols as symbols_module
from databento_stream_downloader.cli import build_config
from databento_stream_downloader.config import DownloadConfig, RunMode
from databento_stream_downloader.errors import FatalAPIError
from databento_stream_downloader.paths import canonical_path
from databento_stream_downloader.pricing import (
    decimal_dollars_to_cents,
    dollars_to_decimal,
)
from databento_stream_downloader.settings import EnvSettings
from databento_stream_downloader.symbols import (
    load_default_symbols,
    load_first_data_utc_dates,
)


def test_default_symbols_include_requested_universe() -> None:
    symbols = load_default_symbols()
    assert "ES.FUT" in symbols
    assert "6E.FUT" in symbols
    assert "XRP.FUT" in symbols
    assert "MWN.FUT" in symbols


def test_first_data_utc_dates_load_quoted_parent_symbols() -> None:
    first_data_utc = load_first_data_utc_dates()

    assert first_data_utc["BTC.FUT"] == date(2017, 12, 18)
    assert first_data_utc["SOL.FUT"] == date(2025, 3, 17)
    assert first_data_utc["XRP.FUT"] == date(2025, 5, 19)
    assert "SOL" not in first_data_utc


def test_default_symbol_loader_rejects_malformed_universe(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def string_symbols() -> dict[str, object]:
        return {"symbols": "ES.FUT"}

    def non_string_symbols() -> dict[str, object]:
        return {"symbols": [123]}

    def invalid_symbols() -> dict[str, object]:
        return {"symbols": ["ES"]}

    monkeypatch.setattr(symbols_module, "_universe_data", string_symbols)

    with pytest.raises(RuntimeError, match="symbols as a string list"):
        load_default_symbols()

    monkeypatch.setattr(symbols_module, "_universe_data", non_string_symbols)

    with pytest.raises(RuntimeError, match="symbols must be strings"):
        load_default_symbols()

    monkeypatch.setattr(symbols_module, "_universe_data", invalid_symbols)

    with pytest.raises(RuntimeError, match=r"invalid universe\.toml symbol"):
        load_default_symbols()


def test_first_data_loader_rejects_malformed_universe(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def list_first_data() -> dict[str, object]:
        return {"symbols": [], "first_data_utc": []}

    def non_string_first_data() -> dict[str, object]:
        return {"symbols": [], "first_data_utc": {1: "2026-04-01"}}

    def invalid_symbol_first_data() -> dict[str, object]:
        return {"symbols": [], "first_data_utc": {"ES": "2026-04-01"}}

    def invalid_date_first_data() -> dict[str, object]:
        return {"symbols": [], "first_data_utc": {"ES.FUT": "bad-date"}}

    monkeypatch.setattr(symbols_module, "_universe_data", list_first_data)

    with pytest.raises(RuntimeError, match="first_data_utc must be a table"):
        load_first_data_utc_dates()

    monkeypatch.setattr(symbols_module, "_universe_data", non_string_first_data)

    with pytest.raises(RuntimeError, match="entries must be strings"):
        load_first_data_utc_dates()

    monkeypatch.setattr(symbols_module, "_universe_data", invalid_symbol_first_data)

    with pytest.raises(RuntimeError, match="invalid first_data_utc symbol"):
        load_first_data_utc_dates()

    monkeypatch.setattr(symbols_module, "_universe_data", invalid_date_first_data)

    with pytest.raises(RuntimeError, match="invalid first_data_utc date"):
        load_first_data_utc_dates()


def test_pricing_rejects_malformed_estimates() -> None:
    for value in ("not-a-decimal", 1, Decimal("NaN")):
        with pytest.raises(FatalAPIError, match="invalid Databento cost estimate"):
            dollars_to_decimal(cast("Any", value))

    with pytest.raises(FatalAPIError, match="invalid Databento cost estimate"):
        decimal_dollars_to_cents(Decimal("-0.01"))


def test_env_settings_rejects_non_string_api_key() -> None:
    with pytest.raises(PydanticValidationError, match="api_key must be a string"):
        EnvSettings(api_key=cast("Any", 123))


def test_canonical_path_matches_public_layout(tmp_path: Path) -> None:
    path = canonical_path(tmp_path, "ES.FUT", "mbo", date(2026, 4, 24))

    expected = (
        tmp_path / "raw" / "glbx-mdp3" / "ES.FUT" / "mbo" / "2026-04-24.dbn.zst"
    ).resolve(strict=False)
    assert path == expected


def test_public_api_exports_validator_and_canonical_path() -> None:
    assert dsd.canonical_path is canonical_path
    assert callable(dsd.validate_dbn_metadata)


def test_parent_symbol_validation_allows_longer_safe_codes(tmp_path: Path) -> None:
    path = canonical_path(tmp_path, "BRRNY.FUT", "mbo", date(2026, 4, 24))
    config = DownloadConfig(
        data_dir=tmp_path,
        symbols=("brrny.fut",),
        schemas=("mbo",),
        start=date(2026, 4, 24),
        end=date(2026, 4, 24),
        mode=RunMode.DRY_RUN,
    )

    assert path.name == "2026-04-24.dbn.zst"
    assert "BRRNY.FUT" in path.parts
    assert config.symbols == ("BRRNY.FUT",)


def test_parent_symbol_validation_rejects_overlong_codes(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="invalid parent futures symbol"):
        canonical_path(tmp_path, "ABCDEF.FUT", "mbo", date(2026, 4, 24))

    with pytest.raises(PydanticValidationError, match="invalid parent futures symbols"):
        DownloadConfig(
            data_dir=tmp_path,
            symbols=("ABCDEF.FUT",),
            schemas=("mbo",),
            start=date(2026, 4, 24),
            end=date(2026, 4, 24),
            mode=RunMode.DRY_RUN,
        )


def test_canonical_path_resolves_data_dir_segments(tmp_path: Path) -> None:
    nested = tmp_path / "nested"
    data_dir = nested / ".." / "archive"

    path = canonical_path(data_dir, "ES.FUT", "mbo", date(2026, 4, 24))

    assert path.is_relative_to((tmp_path / "archive").resolve(strict=False))


def test_canonical_path_rejects_path_escape(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="invalid parent futures symbol"):
        canonical_path(tmp_path, "..", "mbo", date(2026, 4, 24))


def test_canonical_path_rejects_unknown_schema(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="unsupported schema"):
        canonical_path(tmp_path, "ES.FUT", "bad", date(2026, 4, 24))


def test_download_config_validates() -> None:
    config = DownloadConfig(
        data_dir=Path("data"),
        symbols=("ES.FUT",),
        schemas=("mbo",),
        start=date(2026, 4, 1),
        end=date(2026, 4, 24),
        mode=RunMode.DRY_RUN,
    )

    assert config.symbols == ("ES.FUT",)
    assert config.requires_api_key is True


def test_download_config_requires_api_key_except_validate_only(tmp_path: Path) -> None:
    execute_config = DownloadConfig(
        data_dir=tmp_path,
        symbols=("ES.FUT",),
        schemas=("mbo",),
        start=date(2026, 4, 1),
        end=date(2026, 4, 1),
        mode=RunMode.EXECUTE,
    )
    validate_only_config = execute_config.model_copy(update={"validate_only": True})

    assert execute_config.requires_api_key is True
    assert validate_only_config.requires_api_key is False


def test_download_config_normalizes_lowercase_symbols() -> None:
    config = DownloadConfig(
        data_dir=Path("data"),
        symbols=("es.fut",),
        schemas=("mbo",),
        start=date(2026, 4, 1),
        end=date(2026, 4, 1),
        mode=RunMode.DRY_RUN,
    )

    assert config.symbols == ("ES.FUT",)


def test_download_config_rejects_empty_symbol_and_schema_sets() -> None:
    with pytest.raises(PydanticValidationError, match="symbols must not be empty"):
        DownloadConfig(
            data_dir=Path("data"),
            symbols=(),
            schemas=("mbo",),
            start=date(2026, 4, 1),
            end=date(2026, 4, 1),
            mode=RunMode.DRY_RUN,
        )

    with pytest.raises(PydanticValidationError, match="schemas must not be empty"):
        DownloadConfig(
            data_dir=Path("data"),
            symbols=("ES.FUT",),
            schemas=(),
            start=date(2026, 4, 1),
            end=date(2026, 4, 1),
            mode=RunMode.DRY_RUN,
        )


def test_cli_config_validation_rejects_too_many_workers() -> None:
    args = argparse.Namespace(
        data_dir=Path("data"),
        symbols=["ES.FUT"],
        schemas=["mbo"],
        start=date(2026, 4, 1),
        end=date(2026, 4, 1),
        dry_run=False,
        workers=200,
        max_cost_cents=None,
        deep_validate=False,
        strict_validate=False,
        validate_cached=False,
        yes=False,
        log_format="pretty",
        log_file=None,
        verbose=False,
    )

    with pytest.raises(PydanticValidationError, match="less than or equal to 100"):
        build_config(args)


def test_cli_default_schemas_exclude_mbo(tmp_path: Path) -> None:
    args = argparse.Namespace(
        data_dir=tmp_path,
        symbols=["ES.FUT"],
        schemas=["definition", "statistics", "status"],
        start=date(2026, 4, 1),
        end=date(2026, 4, 1),
        dry_run=False,
        workers=10,
        max_cost_cents=1,
        deep_validate=False,
        strict_validate=False,
        validate_cached=False,
        yes=False,
        log_format="pretty",
        log_file=None,
        verbose=False,
    )

    config = build_config(args)

    assert config.schemas == ("definition", "statistics", "status")
    assert "mbo" not in config.schemas


def test_fast_mode_defaults_skip_integrity_overhead() -> None:
    config = DownloadConfig(
        data_dir=Path("data"),
        symbols=("ES.FUT",),
        schemas=("definition",),
        start=date(2026, 4, 1),
        end=date(2026, 4, 1),
        mode=RunMode.EXECUTE,
        max_cost_cents=1,
    )

    assert config.write_sidecars is False
    assert config.validate_on_write is False
    assert config.fsync_writes is False


def test_strict_validate_implies_validate_on_write() -> None:
    config = DownloadConfig(
        data_dir=Path("data"),
        symbols=("ES.FUT",),
        schemas=("definition",),
        start=date(2026, 4, 1),
        end=date(2026, 4, 1),
        mode=RunMode.EXECUTE,
        max_cost_cents=1,
        strict_validate=True,
    )

    assert config.validate_on_write is True


def test_deep_validate_implies_validate_on_write() -> None:
    config = DownloadConfig(
        data_dir=Path("data"),
        symbols=("ES.FUT",),
        schemas=("definition",),
        start=date(2026, 4, 1),
        end=date(2026, 4, 1),
        mode=RunMode.EXECUTE,
        max_cost_cents=1,
        deep_validate=True,
    )

    assert config.validate_on_write is True


def test_zero_cost_cap_requires_allow_free_only(tmp_path: Path) -> None:
    with pytest.raises(PydanticValidationError, match="allow_free_only"):
        DownloadConfig(
            data_dir=tmp_path,
            symbols=("ES.FUT",),
            schemas=("definition",),
            start=date(2026, 4, 1),
            end=date(2026, 4, 1),
            mode=RunMode.EXECUTE,
            max_cost_cents=0,
        )

    config = DownloadConfig(
        data_dir=tmp_path,
        symbols=("ES.FUT",),
        schemas=("definition",),
        start=date(2026, 4, 1),
        end=date(2026, 4, 1),
        mode=RunMode.EXECUTE,
        max_cost_cents=0,
        allow_free_only=True,
    )

    assert config.allow_free_only is True
