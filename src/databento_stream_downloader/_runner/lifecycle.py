"""Top-level runner lifecycle orchestration."""

from __future__ import annotations

import os
import sys
import time
import uuid
from datetime import UTC, datetime

import structlog
import structlog.contextvars
from rich.console import Console

from databento_stream_downloader._runner.cost import (
    _allocate_estimated_billable_bytes,
    _allocate_estimated_cost_cents,
    _check_bucket_cost_caps,
    _check_cost_cap,
    _check_disk_space,
    _estimate_costs,
    _total_estimated_cents,
    _warn_in_flight_planning_exposure,
)
from databento_stream_downloader._runner.format import _bytes, _money, _print_costs
from databento_stream_downloader._runner.fsio import (
    _exclusive_run_lock,
    _sweep_stale_tmp_files,
    _validate_runtime_config,
)
from databento_stream_downloader._runner.ledger import (
    _print_retry_summary,
    _write_run_ledger,
)
from databento_stream_downloader._runner.stream import _stream_missing
from databento_stream_downloader._runner.types import (
    DownloaderClient,
    _DirectoryFsyncTracker,
)
from databento_stream_downloader._runner.validation import (
    _repair_missing_sidecars,
    _validate,
)
from databento_stream_downloader._runner.work import (
    _all_items,
    _cached_items,
    _existing_items,
    _sorted_items,
    _total_partitions,
    _work_items_from_all,
)
from databento_stream_downloader.config import DownloadConfig, RunMode
from databento_stream_downloader.databento_client import DatabentoClient
from databento_stream_downloader.errors import RetryableError
from databento_stream_downloader.models import DownloadResult

_NONINTERACTIVE_REFUSAL = (
    "[bold red]Refusing non-interactive paid download without --yes.[/bold red]"
)
_EOF_REFUSAL = "[bold red]Confirmation aborted by EOF.[/bold red]"
LOGGER = structlog.get_logger(__name__)


def run_download(
    config: DownloadConfig,
    api_key: str,
    console: Console,
    error_console: Console | None = None,
) -> None:
    """Run cost estimation, streaming, and validation."""
    client = DatabentoClient(
        api_key,
        request_timeout_seconds=config.request_timeout_seconds,
    )
    _run_download(config, client, console, error_console)


def run_download_with_client(
    config: DownloadConfig,
    client: DownloaderClient,
    console: Console,
    error_console: Console | None = None,
) -> None:
    """Run the downloader with an injected client implementation."""
    _run_download(config, client, console, error_console)


def _run_download(
    config: DownloadConfig,
    client: DownloaderClient,
    console: Console,
    error_console: Console | None = None,
) -> None:
    error_console = error_console or Console(stderr=True)
    run_id = str(uuid.uuid4())
    run_started_at = _utc_now()
    fsync_tracker = _DirectoryFsyncTracker()
    _validate_runtime_config(config, fsync_tracker)
    with (
        _exclusive_run_lock(config.data_dir, run_id, error_console, fsync_tracker),
        structlog.contextvars.bound_contextvars(run_id=run_id),
    ):
        _run_download_locked(
            config=config,
            client=client,
            console=console,
            error_console=error_console,
            run_id=run_id,
            run_started_at=run_started_at,
            fsync_tracker=fsync_tracker,
        )


def _run_download_locked(
    *,
    config: DownloadConfig,
    client: DownloaderClient,
    console: Console,
    error_console: Console,
    run_id: str,
    run_started_at: str,
    fsync_tracker: _DirectoryFsyncTracker,
) -> None:
    LOGGER.info(
        "run_started",
        run_id=run_id,
        pid=os.getpid(),
        data_dir=str(config.data_dir),
        symbols=config.symbols,
        schemas=config.schemas,
        start=config.start.isoformat(),
        end=config.end.isoformat(),
        workers=config.max_workers,
        strict_validate=config.strict_validate,
    )
    _sweep_stale_tmp_files(config)
    all_items = _all_items(config)
    existing_items = _existing_items(config)
    repair_issues = _repair_missing_sidecars(
        config,
        existing_items,
        console,
        fsync_tracker=fsync_tracker,
    )
    if repair_issues:
        raise SystemExit(5)
    if config.validate_only:
        validation_items = _sorted_items(existing_items)
        if not validation_items:
            console.print("No cached partitions found in scope.")
            return
        validation_issues = _validate(config, console, validation_items)
        if validation_issues:
            raise SystemExit(5)
        return
    work = _work_items_from_all(all_items, existing_items)
    if not work:
        console.print("All partitions already cached, nothing to do.")
        LOGGER.info("run_cached", total=_total_partitions(config))
        if config.validate_cached:
            validation_issues = _validate(config, console, all_items)
            if validation_issues:
                raise SystemExit(5)
        return

    try:
        estimates = _estimate_costs(client, work, max_workers=config.max_workers)
    except RetryableError as exc:
        error_console.print(f"[bold red]Cost estimation failed:[/bold red] {exc}")
        raise SystemExit(1) from exc

    total_cents = _total_estimated_cents(estimates)
    total_bytes = sum(item.size_bytes for item in estimates)
    _print_costs(console, estimates, total_cents, config.max_cost_cents)
    _check_cost_cap(config, total_cents, error_console)
    _check_bucket_cost_caps(config, estimates, error_console)
    _check_disk_space(config.data_dir, total_bytes, error_console)

    if config.mode is RunMode.DRY_RUN:
        return

    estimated_bytes_by_item = _allocate_estimated_billable_bytes(work, estimates)
    estimated_cost_cents_by_item = _allocate_estimated_cost_cents(work, estimates)
    _warn_in_flight_planning_exposure(
        config,
        work,
        estimated_cost_cents_by_item,
        error_console,
    )

    if not config.yes:
        if not sys.stdin.isatty():
            error_console.print(_NONINTERACTIVE_REFUSAL)
            raise SystemExit(2)
        try:
            console.print(f"\nArchive: {config.data_dir}")
            console.print(
                f"Estimated: {_bytes(total_bytes)}; {_money(total_cents)} planning cost"
            )
            answer = console.input("Proceed? [y/N] ")
        except EOFError:
            error_console.print(_EOF_REFUSAL)
            raise SystemExit(2) from None
        if answer.strip().lower() not in ("y", "yes"):
            return

    started = time.monotonic()
    result = _stream_missing(
        config,
        client,
        console,
        work,
        error_console=error_console,
        estimated_bytes_by_item=estimated_bytes_by_item,
        estimated_cost_cents_by_item=estimated_cost_cents_by_item,
        fsync_tracker=fsync_tracker,
    )
    validation_issues = 0
    if config.validate_cached:
        validation_issues = _validate(config, console, _cached_items(all_items, work))
    elapsed = time.monotonic() - started
    exit_code = _run_exit_code(result, validation_issues)
    _write_run_ledger(
        config=config,
        run_id=run_id,
        result=result,
        validation_issues=validation_issues,
        exit_code=exit_code,
        interrupted=False,
        elapsed_seconds=elapsed,
        estimated_cost_cents=total_cents,
        estimated_billable_bytes=total_bytes,
        started_at=run_started_at,
        ended_at=_utc_now(),
        client=client,
        fsync_tracker=fsync_tracker,
    )
    LOGGER.info(
        "run_completed",
        placed=result.placed,
        cached=result.cached,
        no_data=result.no_data,
        failed=result.failed,
        validation_issues=validation_issues,
        elapsed_seconds=round(elapsed, 3),
    )
    console.print(
        f"Completed: placed={result.placed}, cached={result.cached}, "
        f"no_data={result.no_data}, failed={result.failed}, "
        f"validation_issues={validation_issues}, elapsed={elapsed:.1f}s"
    )
    fsync_skipped = fsync_tracker.count()
    if fsync_skipped:
        console.print(
            "[yellow]Warning:[/yellow] directory fsync skipped "
            f"{fsync_skipped} time(s); crash durability was best-effort."
        )
    if config.show_retries:
        _print_retry_summary(client, console)
    if exit_code:
        raise SystemExit(exit_code)


def _run_exit_code(result: DownloadResult, validation_issues: int) -> int:
    if result.failed:
        return 3
    if validation_issues:
        return 5
    return 0


def _utc_now() -> str:
    return datetime.now(UTC).isoformat()


__all__ = [
    "_EOF_REFUSAL",
    "_NONINTERACTIVE_REFUSAL",
    "_run_download",
    "_run_download_locked",
    "_run_exit_code",
    "_utc_now",
    "run_download",
    "run_download_with_client",
]
