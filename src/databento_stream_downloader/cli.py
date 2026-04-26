"""Command line entry point for the standalone downloader."""

from __future__ import annotations

import argparse
import importlib.metadata
import signal
from collections.abc import Generator
from contextlib import contextmanager
from datetime import date
from pathlib import Path
from typing import cast

import structlog
from dotenv import load_dotenv
from pydantic import ValidationError
from rich.console import Console

from databento_stream_downloader.config import (
    DEFAULT_SCHEMAS,
    SUPPORTED_SCHEMAS,
    DownloadConfig,
    RunMode,
)
from databento_stream_downloader.errors import (
    FatalAPIError,
    FatalConfigError,
    FatalError,
    InterruptedDownloadError,
    ShutdownRequestedError,
)
from databento_stream_downloader.observability import LogFormat, configure_logging
from databento_stream_downloader.runner import run_download
from databento_stream_downloader.settings import EnvSettings
from databento_stream_downloader.symbols import load_default_symbols

LOGGER = structlog.get_logger(__name__)


def _parse_date(value: str) -> date:
    if "T" in value or ":" in value:
        msg = f"Invalid date format: {value!r}. Expected YYYY-MM-DD, not a timestamp."
        raise argparse.ArgumentTypeError(msg)
    try:
        return date.fromisoformat(value)
    except ValueError:
        msg = f"Invalid date format: {value!r}. Expected YYYY-MM-DD."
        raise argparse.ArgumentTypeError(msg) from None


def _parse_nonnegative_int(value: str) -> int:
    try:
        parsed = int(value)
    except ValueError:
        msg = f"Invalid integer: {value!r}."
        raise argparse.ArgumentTypeError(msg) from None
    if parsed < 0:
        msg = f"Expected a non-negative integer, got {value!r}."
        raise argparse.ArgumentTypeError(msg)
    return parsed


def _parse_workers(value: str) -> int:
    workers = _parse_nonnegative_int(value)
    if workers < 1 or workers > 50:
        msg = f"workers must be between 1 and 50, got {workers}."
        raise argparse.ArgumentTypeError(msg)
    return workers


def _parse_positive_float(value: str) -> float:
    try:
        parsed = float(value)
    except ValueError:
        msg = f"Invalid float: {value!r}."
        raise argparse.ArgumentTypeError(msg) from None
    if parsed <= 0:
        msg = f"Expected a positive float, got {value!r}."
        raise argparse.ArgumentTypeError(msg)
    return parsed


def _format_validation_error(exc: ValidationError) -> str:
    details: list[str] = []
    for error in exc.errors(include_url=False, include_context=False):
        location = ".".join(str(part) for part in error["loc"])
        message = str(error["msg"])
        if location:
            details.append(f"{location}: {message}")
        else:
            details.append(message)
    return "\n".join(details)


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Download Databento historical DBN data.",
        epilog=(
            "Exit codes: 0 success, 1 retry/fatal API failure, "
            "2 usage/config/safety refusal, 3 partial download failure, "
            "4 unexpected internal error, 5 validation failure, "
            "130 interrupted, 143 graceful shutdown."
        ),
    )
    _ = parser.add_argument(
        "--symbols",
        "--symbol",
        nargs="+",
        metavar="SYMBOL",
        help="Parent futures symbols to download, e.g. ES.FUT NQ.FUT.",
    )
    _ = parser.add_argument(
        "--schemas",
        nargs="+",
        choices=SUPPORTED_SCHEMAS,
        default=list(DEFAULT_SCHEMAS),
        metavar="SCHEMA",
        help=(
            "Schemas to download. Defaults to free metadata schemas; mbo is "
            "billable and opt-in."
        ),
    )
    _ = parser.add_argument(
        "--start",
        type=_parse_date,
        required=True,
        help="Inclusive UTC start date, YYYY-MM-DD only; no timestamp suffix.",
    )
    _ = parser.add_argument(
        "--end",
        type=_parse_date,
        required=True,
        help="Inclusive UTC end date, YYYY-MM-DD only; no timestamp suffix.",
    )
    _ = parser.add_argument(
        "--data-dir",
        type=Path,
        default=Path("data"),
        help="Local archive root. Defaults to ./data.",
    )
    _ = parser.add_argument(
        "--workers",
        type=_parse_workers,
        default=4,
        help="Concurrent download workers. Default 4; max 50.",
    )
    _ = parser.add_argument(
        "--max-cost-cents",
        type=_parse_nonnegative_int,
        help=(
            "Maximum Databento estimated planning cost in cents. Required for "
            "execute runs. Use 0 only with --allow-free-only; actual billing "
            "can exceed this on failed/retried streams."
        ),
    )
    _ = parser.add_argument(
        "--allow-free-only",
        action="store_true",
        help=(
            "Allow --max-cost-cents 0 as an explicit free-only planning cap. "
            "Without this flag, zero is rejected to avoid ambiguous budgets."
        ),
    )
    _ = parser.add_argument(
        "--max-cost-cents-per-bucket",
        type=_parse_nonnegative_int,
        help=(
            "Optional maximum estimated planning cost in cents for any one "
            "symbol/schema bucket."
        ),
    )
    _ = parser.add_argument(
        "--request-timeout-seconds",
        type=_parse_positive_float,
        default=100.0,
        help="Best-effort Databento SDK request timeout in seconds.",
    )
    _ = parser.add_argument(
        "--deep-validate",
        action="store_true",
        help="Drain zstd frames to EOF during validation; extra local I/O only.",
    )
    _ = parser.add_argument(
        "--strict-validate",
        action="store_true",
        help=(
            "Decode records with DBNStore and check timestamp bounds via "
            "ts_recv/ts_event, symbology coverage, and ts_recv monotonicity "
            "when present."
        ),
    )
    _ = parser.add_argument(
        "--validate-cached",
        action="store_true",
        help="Revalidate already-cached files in scope; can be expensive.",
    )
    _ = parser.add_argument(
        "--validate-only",
        action="store_true",
        help=(
            "Only validate cached files in scope; make no Databento cost or "
            "download requests."
        ),
    )
    _ = parser.add_argument(
        "--ledger-rotate-mb",
        type=_parse_nonnegative_int,
        default=50,
        help="Rotate download-ledger.jsonl when it exceeds this many MiB.",
    )
    _ = parser.add_argument(
        "--suspicious-no-data-weekdays",
        type=_parse_nonnegative_int,
        default=5,
        help=(
            "Fail when every requested partition for a symbol/schema returns "
            "no data across at least this many expected weekdays."
        ),
    )
    _ = parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Estimate and print planned work without downloading billable data.",
    )
    _ = parser.add_argument(
        "--log-format",
        choices=("pretty", "json"),
        default="pretty",
        help="Structured log renderer.",
    )
    _ = parser.add_argument(
        "--log-file",
        type=Path,
        help="Append structured logs to this file instead of stderr.",
    )
    _ = parser.add_argument(
        "--verbose",
        action="store_true",
        help="Emit INFO-level operational logs to stderr.",
    )
    _ = parser.add_argument(
        "--quiet",
        action="store_true",
        help="Suppress human progress output where possible.",
    )
    _ = parser.add_argument(
        "--show-retries",
        action="store_true",
        help="Print a retry summary after the run when using the real client.",
    )
    _ = parser.add_argument(
        "--yes",
        "-y",
        action="store_true",
        help="Proceed without an interactive confirmation prompt.",
    )
    _ = parser.add_argument(
        "--version",
        action="version",
        version=importlib.metadata.version("databento-stream-downloader"),
    )
    return parser


def build_config(
    args: argparse.Namespace,
    settings: EnvSettings | None = None,
) -> DownloadConfig:
    """Build validated config from parsed CLI args."""
    settings = settings or EnvSettings()
    symbols_arg = cast("list[str] | None", args.symbols)
    mode = RunMode.DRY_RUN if cast("bool", args.dry_run) else RunMode.EXECUTE
    max_cost_cents = cast("int | None", args.max_cost_cents)
    if max_cost_cents is None:
        max_cost_cents = settings.max_cost_cents
    allow_free_only = cast("bool", getattr(args, "allow_free_only", False))
    if not allow_free_only:
        allow_free_only = settings.allow_free_only
    max_cost_cents_per_bucket = cast(
        "int | None",
        getattr(args, "max_cost_cents_per_bucket", None),
    )
    if max_cost_cents_per_bucket is None:
        max_cost_cents_per_bucket = settings.max_cost_cents_per_bucket
    symbols = tuple(dict.fromkeys(symbols_arg)) if symbols_arg else _default_symbols()
    return DownloadConfig(
        data_dir=cast("Path", args.data_dir).absolute(),
        symbols=symbols,
        schemas=tuple(cast("list[str]", args.schemas)),
        start=cast("date", args.start),
        end=cast("date", args.end),
        mode=mode,
        max_workers=cast("int", args.workers),
        request_timeout_seconds=cast(
            "float",
            getattr(args, "request_timeout_seconds", 100.0),
        ),
        max_cost_cents=max_cost_cents,
        max_cost_cents_per_bucket=max_cost_cents_per_bucket,
        allow_free_only=allow_free_only,
        ledger_rotate_mb=cast("int", getattr(args, "ledger_rotate_mb", 50)),
        suspicious_no_data_weekdays=cast(
            "int",
            getattr(args, "suspicious_no_data_weekdays", 5),
        ),
        deep_validate=cast("bool", args.deep_validate),
        strict_validate=cast("bool", args.strict_validate),
        validate_cached=cast("bool", args.validate_cached),
        validate_only=cast("bool", getattr(args, "validate_only", False)),
        show_retries=cast("bool", getattr(args, "show_retries", False)),
        yes=cast("bool", args.yes),
    )


def _default_symbols() -> tuple[str, ...]:
    try:
        return load_default_symbols()
    except RuntimeError as exc:
        msg = f"default symbol universe is invalid: {exc}"
        raise FatalConfigError(msg) from exc


def main() -> None:
    """Parse CLI args and run the local downloader."""
    load_dotenv(dotenv_path=Path.cwd() / ".env", override=False)
    parser = _build_parser()
    args = parser.parse_args()
    configure_logging(
        log_format=cast("LogFormat", args.log_format),
        log_file=cast("Path | None", args.log_file),
        verbose=cast("bool", args.verbose),
        force=True,
    )
    json_logs_to_stderr = args.log_format == "json" and args.log_file is None
    # Keep `2>events.jsonl` parseable when JSON logs already occupy stderr.
    console = Console(stderr=not json_logs_to_stderr, quiet=cast("bool", args.quiet))
    error_console = Console(stderr=True)
    try:
        settings = EnvSettings()
        config = build_config(args, settings)
    except ValidationError as exc:
        parser.error(_format_validation_error(exc))
    except FatalConfigError as exc:
        error_console.print(f"[bold red]Config:[/bold red] {exc}")
        raise SystemExit(2) from exc
    api_key = settings.api_key
    if config.requires_api_key and not api_key:
        error_console.print(
            "[bold red]Config:[/bold red] DATABENTO_API_KEY is not configured"
        )
        raise SystemExit(2)
    if config.max_workers > 8:
        error_console.print(
            "[yellow]Warning:[/yellow] high worker count configured "
            f"({config.max_workers}). Start with 4-8 unless your Databento "
            "account has been observed handling this concurrency."
        )
        LOGGER.warning("high_worker_count", workers=config.max_workers)
    try:
        with _installed_signal_handlers():
            run_download(config, api_key or "", console, error_console)
    except InterruptedDownloadError as exc:
        error_console.print(
            "[yellow]Interrupted. Stopping downloader; partial temp files will "
            "be cleaned on the next run.[/yellow]"
        )
        error_console.file.flush()
        raise SystemExit(130) from exc
    except (ShutdownRequestedError, KeyboardInterrupt) as exc:
        error_console.print("[yellow]Shutdown requested. Stopping downloader.[/yellow]")
        error_console.file.flush()
        raise SystemExit(143) from exc
    except FatalConfigError as exc:
        LOGGER.error(
            "fatal_config_error",
            category=exc.category.value,
            error=str(exc),
        )
        error_console.print(f"[bold red]Config:[/bold red] {exc}")
        raise SystemExit(2) from exc
    except FatalAPIError as exc:
        LOGGER.error("fatal_api_error", category=exc.category.value, error=str(exc))
        error_console.print(f"[bold red]Fatal API:[/bold red] {exc}")
        raise SystemExit(1) from exc
    except FatalError as exc:
        LOGGER.error("fatal_error", category=exc.category.value, error=str(exc))
        error_console.print(f"[bold red]Fatal:[/bold red] {exc}")
        raise SystemExit(1) from exc
    except Exception as exc:
        LOGGER.exception("unexpected_cli_failure", error=str(exc))
        error_console.print(f"[bold red]Unexpected failure:[/bold red] {exc}")
        raise SystemExit(4) from exc


@contextmanager
def _installed_signal_handlers() -> Generator[None]:
    previous_sigterm = None
    if hasattr(signal, "SIGTERM"):

        def handle_sigterm(_signum: int, _frame: object) -> None:
            raise ShutdownRequestedError("SIGTERM received")

        previous_sigterm = signal.getsignal(signal.SIGTERM)
        signal.signal(signal.SIGTERM, handle_sigterm)
    try:
        yield
    finally:
        if previous_sigterm is not None:
            signal.signal(signal.SIGTERM, previous_sigterm)
