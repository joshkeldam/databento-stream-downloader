"""CLI parsing tests."""
# pyright: reportPrivateUsage=false

from __future__ import annotations

import argparse
import importlib
import signal
import sys
from datetime import date
from pathlib import Path
from typing import cast

import pytest
import structlog
from pydantic import ValidationError as PydanticValidationError

from databento_stream_downloader import cli
from databento_stream_downloader.config import DownloadConfig
from databento_stream_downloader.errors import (
    FatalAPIError,
    FatalConfigError,
    FatalError,
    InterruptedDownloadError,
    ShutdownRequestedError,
)
from databento_stream_downloader.settings import EnvSettings


def test_main_module_imports_without_invoking_cli() -> None:
    module = importlib.import_module("databento_stream_downloader.__main__")

    assert module.main is cli.main


def _skip_dotenv(**_kwargs: object) -> bool:
    return False


def test_parse_date_rejects_bad_format() -> None:
    assert cli._parse_date("2026-04-24") == date(2026, 4, 24)

    with pytest.raises(argparse.ArgumentTypeError, match="YYYY-MM-DD"):
        cli._parse_date("24/04/2026")
    with pytest.raises(argparse.ArgumentTypeError, match="not a timestamp"):
        cli._parse_date("2026-04-24T00:00:00")


def test_parse_nonnegative_int_and_workers() -> None:
    assert cli._parse_nonnegative_int("50") == 50
    assert cli._parse_workers("50") == 50

    with pytest.raises(argparse.ArgumentTypeError, match="non-negative"):
        cli._parse_nonnegative_int("-1")
    with pytest.raises(argparse.ArgumentTypeError, match="Invalid integer"):
        cli._parse_nonnegative_int("abc")
    with pytest.raises(argparse.ArgumentTypeError, match="between 1 and 50"):
        cli._parse_workers("100")


def test_parse_positive_float_rejects_invalid_values() -> None:
    assert cli._parse_positive_float("12.5") == 12.5

    with pytest.raises(argparse.ArgumentTypeError, match="Invalid float"):
        cli._parse_positive_float("not-float")
    with pytest.raises(argparse.ArgumentTypeError, match="positive float"):
        cli._parse_positive_float("0")


def test_build_config_uses_env_settings_for_cost_cap(tmp_path: Path) -> None:
    args = argparse.Namespace(
        data_dir=tmp_path,
        symbols=["ES.FUT", "ES.FUT"],
        schemas=["mbo", "mbo"],
        start=date(2026, 4, 1),
        end=date(2026, 4, 1),
        dry_run=False,
        workers=10,
        max_cost_cents=None,
        deep_validate=False,
        strict_validate=False,
        validate_cached=False,
        yes=True,
        log_format="json",
        log_file=None,
        verbose=False,
    )
    settings = EnvSettings(api_key="key", max_cost_cents=123)

    config = cli.build_config(args, settings)

    assert config.symbols == ("ES.FUT",)
    assert config.schemas == ("mbo",)
    assert config.max_cost_cents == 123


def test_format_validation_error_is_compact(tmp_path: Path) -> None:
    args = argparse.Namespace(
        data_dir=tmp_path,
        symbols=["ES"],
        schemas=["mbo"],
        start=date(2026, 4, 1),
        end=date(2026, 4, 1),
        dry_run=False,
        workers=10,
        max_cost_cents=None,
        deep_validate=False,
        strict_validate=False,
        validate_cached=False,
        yes=True,
        log_format="json",
        log_file=None,
        verbose=False,
    )

    with pytest.raises(PydanticValidationError) as exc_info:
        cli.build_config(args, EnvSettings())

    formatted = cli._format_validation_error(exc_info.value)
    assert "symbols" in formatted
    assert ";" not in formatted


def test_symbol_aliases_parse_multiple_values(tmp_path: Path) -> None:
    parser = cli._build_parser()
    args = parser.parse_args(
        [
            "--symbol",
            "ES.FUT",
            "NQ.FUT",
            "--schemas",
            "definition",
            "--start",
            "2026-04-01",
            "--end",
            "2026-04-01",
            "--data-dir",
            str(tmp_path),
            "--max-cost-cents",
            "1",
        ]
    )

    config = cli.build_config(args, EnvSettings(api_key="key"))

    assert config.symbols == ("ES.FUT", "NQ.FUT")


def test_repeated_symbol_alias_uses_last_occurrence(tmp_path: Path) -> None:
    parser = cli._build_parser()
    args = parser.parse_args(
        [
            "--symbols",
            "ES.FUT",
            "--symbol",
            "NQ.FUT",
            "--schemas",
            "definition",
            "--start",
            "2026-04-01",
            "--end",
            "2026-04-01",
            "--data-dir",
            str(tmp_path),
            "--max-cost-cents",
            "1",
        ]
    )

    config = cli.build_config(args, EnvSettings(api_key="key"))

    assert config.symbols == ("NQ.FUT",)


def test_env_settings_normalizes_blank_api_key() -> None:
    assert EnvSettings(api_key=" ").api_key is None


def test_default_symbols_failure_maps_to_config_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fail_default_symbols() -> tuple[str, ...]:
        raise RuntimeError("bad universe")

    monkeypatch.setattr(cli, "load_default_symbols", fail_default_symbols)

    with pytest.raises(FatalConfigError, match="default symbol universe is invalid"):
        cli._default_symbols()


def test_main_exits_when_api_key_missing(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.delenv("DATABENTO_API_KEY", raising=False)
    monkeypatch.setattr(cli, "load_dotenv", _skip_dotenv)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "databento-stream-downloader",
            "--symbol",
            "ES.FUT",
            "--schemas",
            "mbo",
            "--start",
            "2026-04-01",
            "--end",
            "2026-04-01",
            "--data-dir",
            str(tmp_path),
        ],
    )

    with pytest.raises(SystemExit) as exc_info:
        cli.main()

    assert exc_info.value.code == 2


def test_main_missing_api_key_prints_compact_error(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
) -> None:
    monkeypatch.delenv("DATABENTO_API_KEY", raising=False)
    monkeypatch.setattr(cli, "load_dotenv", _skip_dotenv)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "databento-stream-downloader",
            "--symbol",
            "ES.FUT",
            "--schemas",
            "definition",
            "--start",
            "2026-04-01",
            "--end",
            "2026-04-01",
            "--data-dir",
            str(tmp_path),
        ],
    )

    with pytest.raises(SystemExit):
        cli.main()

    captured = capsys.readouterr()
    assert "DATABENTO_API_KEY is not configured" in captured.err
    assert "usage:" not in captured.err


def test_main_maps_pydantic_validation_to_parser_error(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
) -> None:
    monkeypatch.setenv("DATABENTO_API_KEY", "key")
    monkeypatch.setattr(cli, "load_dotenv", _skip_dotenv)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "databento-stream-downloader",
            "--symbol",
            "ES",
            "--schemas",
            "definition",
            "--start",
            "2026-04-01",
            "--end",
            "2026-04-01",
            "--data-dir",
            str(tmp_path),
        ],
    )

    with pytest.raises(SystemExit) as exc_info:
        cli.main()

    captured = capsys.readouterr()
    assert exc_info.value.code == 2
    assert "symbols" in captured.err


def test_main_maps_default_symbol_failure_to_exit_2(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
) -> None:
    def fail_default_symbols() -> tuple[str, ...]:
        raise RuntimeError("bad universe")

    monkeypatch.delenv("DATABENTO_API_KEY", raising=False)
    monkeypatch.setattr(cli, "load_dotenv", _skip_dotenv)
    monkeypatch.setattr(cli, "load_default_symbols", fail_default_symbols)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "databento-stream-downloader",
            "--schemas",
            "definition",
            "--start",
            "2026-04-01",
            "--end",
            "2026-04-01",
            "--data-dir",
            str(tmp_path),
        ],
    )

    with pytest.raises(SystemExit) as exc_info:
        cli.main()

    captured = capsys.readouterr()
    assert exc_info.value.code == 2
    assert "default symbol universe is invalid" in captured.err


def test_main_allows_validate_only_without_api_key(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.delenv("DATABENTO_API_KEY", raising=False)
    monkeypatch.setattr(cli, "load_dotenv", _skip_dotenv)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "databento-stream-downloader",
            "--symbol",
            "ES.FUT",
            "--schemas",
            "definition",
            "--start",
            "2026-04-01",
            "--end",
            "2026-04-01",
            "--data-dir",
            str(tmp_path),
            "--validate-only",
        ],
    )
    seen: dict[str, object] = {}

    def fake_run_download(
        config: DownloadConfig,
        api_key: str,
        *_args: object,
        **_kwargs: object,
    ) -> None:
        seen["config"] = config
        seen["api_key"] = api_key

    monkeypatch.setattr(cli, "run_download", fake_run_download)

    cli.main()

    config = cast("DownloadConfig", seen["config"])
    assert config.requires_api_key is False
    assert seen["api_key"] == ""


def test_json_quiet_routes_machine_logs_to_stderr_only(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
) -> None:
    monkeypatch.setenv("DATABENTO_API_KEY", "key")
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "databento-stream-downloader",
            "--symbol",
            "ES.FUT",
            "--schemas",
            "definition",
            "--start",
            "2026-04-01",
            "--end",
            "2026-04-01",
            "--data-dir",
            str(tmp_path),
            "--max-cost-cents",
            "0",
            "--allow-free-only",
            "--log-format",
            "json",
            "--verbose",
            "--quiet",
            "--yes",
        ],
    )

    def fake_run_download(*_args: object, **_kwargs: object) -> None:
        structlog.get_logger("test").info("json_test_event")

    monkeypatch.setattr(cli, "run_download", fake_run_download)

    cli.main()

    captured = capsys.readouterr()
    assert captured.out == ""
    assert "json_test_event" in captured.err


def test_show_retries_flows_through_cli_main(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setenv("DATABENTO_API_KEY", "key")
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "databento-stream-downloader",
            "--symbol",
            "ES.FUT",
            "--schemas",
            "definition",
            "--start",
            "2026-04-01",
            "--end",
            "2026-04-01",
            "--data-dir",
            str(tmp_path),
            "--max-cost-cents",
            "1",
            "--show-retries",
            "--yes",
        ],
    )
    configs: list[object] = []

    def fake_run_download(config: object, *_args: object, **_kwargs: object) -> None:
        configs.append(config)

    monkeypatch.setattr(cli, "run_download", fake_run_download)

    cli.main()

    assert len(configs) == 1
    assert cast("DownloadConfig", configs[0]).show_retries is True


def test_high_worker_count_warning_is_visible(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
) -> None:
    monkeypatch.setenv("DATABENTO_API_KEY", "key")
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "databento-stream-downloader",
            "--symbol",
            "ES.FUT",
            "--schemas",
            "definition",
            "--start",
            "2026-04-01",
            "--end",
            "2026-04-01",
            "--data-dir",
            str(tmp_path),
            "--max-cost-cents",
            "1",
            "--workers",
            "9",
            "--yes",
        ],
    )

    def fake_run_download(*_args: object, **_kwargs: object) -> None:
        return None

    monkeypatch.setattr(cli, "run_download", fake_run_download)

    cli.main()

    captured = capsys.readouterr()
    assert "high worker count configured" in captured.err


def test_signal_handler_context_restores_previous_handler() -> None:
    if not hasattr(signal, "SIGTERM"):
        pytest.skip("SIGTERM is not available on this platform")
    previous = signal.getsignal(signal.SIGTERM)

    with cli._installed_signal_handlers():
        assert signal.getsignal(signal.SIGTERM) is not previous

    assert signal.getsignal(signal.SIGTERM) is previous


def test_main_maps_config_failure_to_exit_2(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setenv("DATABENTO_API_KEY", "key")
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "databento-stream-downloader",
            "--symbol",
            "ES.FUT",
            "--schemas",
            "definition",
            "--start",
            "2026-04-01",
            "--end",
            "2026-04-01",
            "--data-dir",
            str(tmp_path),
            "--max-cost-cents",
            "1",
            "--yes",
        ],
    )

    def fail_config(*_args: object, **_kwargs: object) -> None:
        raise FatalConfigError("bad config")

    monkeypatch.setattr(cli, "run_download", fail_config)

    with pytest.raises(SystemExit) as exc_info:
        cli.main()

    assert exc_info.value.code == 2


def test_main_interrupt_prints_clean_message_and_exits_normally(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
) -> None:
    monkeypatch.setenv("DATABENTO_API_KEY", "key")
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "databento-stream-downloader",
            "--symbol",
            "ES.FUT",
            "--schemas",
            "definition",
            "--start",
            "2026-04-01",
            "--end",
            "2026-04-01",
            "--data-dir",
            str(tmp_path),
            "--max-cost-cents",
            "1",
            "--yes",
        ],
    )

    def interrupt(*_args: object, **_kwargs: object) -> None:
        raise InterruptedDownloadError("stop")

    monkeypatch.setattr(cli, "run_download", interrupt)

    with pytest.raises(SystemExit) as exc_info:
        cli.main()

    captured = capsys.readouterr()
    assert exc_info.value.code == 130
    assert "Interrupted. Stopping downloader" in captured.err
    assert "Traceback" not in captured.err


def test_main_shutdown_request_exits_143(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
) -> None:
    monkeypatch.setenv("DATABENTO_API_KEY", "key")
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "databento-stream-downloader",
            "--symbol",
            "ES.FUT",
            "--schemas",
            "definition",
            "--start",
            "2026-04-01",
            "--end",
            "2026-04-01",
            "--data-dir",
            str(tmp_path),
            "--max-cost-cents",
            "1",
            "--yes",
        ],
    )

    def shutdown(*_args: object, **_kwargs: object) -> None:
        raise ShutdownRequestedError("term")

    monkeypatch.setattr(cli, "run_download", shutdown)

    with pytest.raises(SystemExit) as exc_info:
        cli.main()

    captured = capsys.readouterr()
    assert exc_info.value.code == 143
    assert "Shutdown requested" in captured.err


@pytest.mark.parametrize(
    ("error", "exit_code", "label"),
    [
        (FatalAPIError("api failed"), 1, "Fatal API"),
        (FatalError("fatal failed"), 1, "Fatal"),
        (RuntimeError("bug"), 4, "Unexpected failure"),
    ],
)
def test_main_maps_terminal_error_arms(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
    error: Exception,
    exit_code: int,
    label: str,
) -> None:
    monkeypatch.setenv("DATABENTO_API_KEY", "key")
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "databento-stream-downloader",
            "--symbol",
            "ES.FUT",
            "--schemas",
            "definition",
            "--start",
            "2026-04-01",
            "--end",
            "2026-04-01",
            "--data-dir",
            str(tmp_path),
            "--max-cost-cents",
            "1",
            "--yes",
        ],
    )

    def fail(*_args: object, **_kwargs: object) -> None:
        raise error

    monkeypatch.setattr(cli, "run_download", fail)

    with pytest.raises(SystemExit) as exc_info:
        cli.main()

    captured = capsys.readouterr()
    assert exc_info.value.code == exit_code
    assert label in captured.err
