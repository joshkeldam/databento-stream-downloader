"""CLI entry point for ``databento-stream-sync`` (push/pull subcommands)."""
# pyright: reportPrivateUsage=false

from __future__ import annotations

import argparse
import importlib.metadata
from pathlib import Path
from typing import cast

from dotenv import load_dotenv
from pydantic import ValidationError as PydanticValidationError
from rich.console import Console

from databento_stream_downloader._cli_shared import (
    format_validation_error as _format_validation_error,
)
from databento_stream_downloader._cli_shared import (
    installed_signal_handlers as _installed_signal_handlers,
)
from databento_stream_downloader._cli_shared import (
    parse_workers as _parse_workers,
)
from databento_stream_downloader._sync.types import (
    PlanningMode,
    SyncConfig,
    SyncDirection,
)
from databento_stream_downloader.config import RunMode
from databento_stream_downloader.errors import (
    FatalAPIError,
    FatalConfigError,
    FatalError,
    InterruptedDownloadError,
    ShutdownRequestedError,
)
from databento_stream_downloader.observability import LogFormat, configure_logging
from databento_stream_downloader.settings import EnvSettings


def run_sync(
    config: SyncConfig,
    *,
    console: Console | None = None,
    error_console: Console | None = None,
) -> int:
    from databento_stream_downloader._sync.lifecycle import run_sync as _run_sync

    return _run_sync(config, console=console, error_console=error_console)


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="databento-stream-sync",
        description=(
            "Sync the local Databento archive to S3 (push) or restore it "
            "from S3 (pull). Both subcommands take the same flags."
        ),
    )
    _ = parser.add_argument(
        "--version",
        action="version",
        version=importlib.metadata.version("databento-stream-downloader"),
    )
    sub = parser.add_subparsers(dest="direction", required=True, metavar="DIRECTION")
    push = sub.add_parser("push", help="Upload local archive to S3.")
    pull = sub.add_parser("pull", help="Download S3 archive to local.")
    for sp in (push, pull):
        _add_common_args(sp)
    return parser


def _add_common_args(sp: argparse.ArgumentParser) -> None:
    _ = sp.add_argument(
        "--data-dir",
        type=Path,
        default=Path("data"),
        help="Local archive root. Default ./data.",
    )
    _ = sp.add_argument(
        "--bucket",
        help="S3 bucket name. Defaults to $DATABENTO_S3_BUCKET.",
    )
    _ = sp.add_argument(
        "--prefix",
        default=None,
        help=(
            "S3 prefix prepended to archive-relative keys. "
            "Defaults to $DATABENTO_S3_PREFIX."
        ),
    )
    _ = sp.add_argument(
        "--region",
        help="AWS region. Defaults to $DATABENTO_S3_REGION / boto3 default.",
    )
    _ = sp.add_argument(
        "--workers",
        type=_parse_workers,
        default=4,
        help="Concurrent transfer workers. Default 4; max 100.",
    )
    _ = sp.add_argument(
        "--delete",
        action="store_true",
        help=(
            "Remove destination-only files (S3 for push, local for pull). "
            "Requires typed confirmation."
        ),
    )
    _ = sp.add_argument(
        "--verify-sha256",
        action="store_true",
        help="Cross-check the SHA256 sidecar against S3 object metadata.",
    )
    _ = sp.add_argument(
        "--planning-mode",
        choices=tuple(mode.value for mode in PlanningMode),
        default=PlanningMode.SIZE.value,
        help=(
            "Plan equality by size only, SHA256 sidecar files, or S3 "
            "Metadata.sha256. Default size."
        ),
    )
    _ = sp.add_argument(
        "--fsync-writes",
        action="store_true",
        help="Pull only: fsync each placed file and parent dir (default off).",
    )
    _ = sp.add_argument(
        "--dry-run",
        action="store_true",
        help="Render the plan and exit without transferring.",
    )
    _ = sp.add_argument(
        "--yes",
        "-y",
        action="store_true",
        help="Proceed without an interactive confirmation prompt.",
    )
    _ = sp.add_argument(
        "--quiet",
        action="store_true",
        help="Suppress the Live UI (logs and the failure list still print).",
    )
    _ = sp.add_argument(
        "--log-format",
        choices=("pretty", "json"),
        default="pretty",
    )
    _ = sp.add_argument("--log-file", type=Path, default=None)
    _ = sp.add_argument("--verbose", action="store_true")


def _build_sync_config(
    args: argparse.Namespace,
    settings: EnvSettings,
) -> SyncConfig:
    direction = SyncDirection(cast("str", args.direction))
    bucket = cast("str | None", args.bucket) or settings.s3_bucket
    if not bucket:
        msg = "S3 bucket is not configured: pass --bucket or set DATABENTO_S3_BUCKET."
        raise FatalConfigError(msg)
    prefix_arg = cast("str | None", args.prefix)
    prefix = prefix_arg if prefix_arg is not None else settings.s3_prefix
    region = cast("str | None", args.region) or settings.s3_region
    mode = RunMode.DRY_RUN if cast("bool", args.dry_run) else RunMode.EXECUTE
    return SyncConfig(
        direction=direction,
        data_dir=cast("Path", args.data_dir).resolve(strict=False),
        bucket=bucket,
        prefix=prefix,
        region=region,
        mode=mode,
        max_workers=cast("int", args.workers),
        delete=cast("bool", args.delete),
        verify_sha256=cast("bool", args.verify_sha256),
        planning_mode=PlanningMode(cast("str", args.planning_mode)),
        fsync_writes=cast("bool", args.fsync_writes),
        yes=cast("bool", args.yes),
    )


def main() -> None:
    """Parse CLI args and run the sync."""
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
    console = Console(stderr=not json_logs_to_stderr, quiet=cast("bool", args.quiet))
    error_console = Console(stderr=True)
    try:
        settings = EnvSettings()
        config = _build_sync_config(args, settings)
    except PydanticValidationError as exc:
        parser.error(_format_validation_error(exc))
    except FatalConfigError as exc:
        error_console.print(f"[bold red]Config:[/bold red] {exc}")
        raise SystemExit(2) from exc
    if config.max_workers > 8:
        error_console.print(
            "[yellow]Warning:[/yellow] high worker count configured "
            f"({config.max_workers}). Start with 4-8 unless your S3 account "
            "has been observed handling this concurrency.",
        )
    try:
        with _installed_signal_handlers():
            exit_code = run_sync(
                config,
                console=console,
                error_console=error_console,
            )
    except InterruptedDownloadError as exc:
        error_console.print("[yellow]Interrupted. Stopping sync.[/yellow]")
        error_console.file.flush()
        raise SystemExit(130) from exc
    except (ShutdownRequestedError, KeyboardInterrupt) as exc:
        error_console.print("[yellow]Shutdown requested. Stopping sync.[/yellow]")
        error_console.file.flush()
        raise SystemExit(143) from exc
    except FatalConfigError as exc:
        error_console.print(f"[bold red]Config:[/bold red] {exc}")
        raise SystemExit(2) from exc
    except FatalAPIError as exc:
        error_console.print(f"[bold red]Fatal API:[/bold red] {exc}")
        raise SystemExit(1) from exc
    except FatalError as exc:
        error_console.print(f"[bold red]Fatal:[/bold red] {exc}")
        raise SystemExit(1) from exc
    except Exception as exc:
        error_console.print(f"[bold red]Unexpected failure:[/bold red] {exc}")
        raise SystemExit(4) from exc
    if exit_code:
        raise SystemExit(exit_code)


__all__ = ["main"]
