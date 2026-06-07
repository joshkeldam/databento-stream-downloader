"""Top-level runner lifecycle orchestration."""

from __future__ import annotations

import os
import sys
import time
import uuid
from collections.abc import Iterable
from datetime import UTC, datetime

import structlog
import structlog.contextvars
from rich.console import Console

from databento_stream_downloader._runner.cost import (
    _check_bucket_cost_caps,
    _check_cost_cap,
    _check_disk_space,
    _estimate_costs,
    _total_estimated_cents,
    _warn_in_flight_planning_exposure_for_costs,
)
from databento_stream_downloader._runner.format import _print_costs
from databento_stream_downloader._runner.fsio import (
    _exclusive_run_lock,
    _sweep_stale_tmp_files,
    _validate_runtime_config,
)
from databento_stream_downloader._runner.ledger import (
    _print_retry_summary,
    _write_run_ledger,
)
from databento_stream_downloader._runner.types import (
    DownloaderClient,
    WorkItem,
    _DirectoryFsyncTracker,
)
from databento_stream_downloader._runner.work import (
    WorkKey,
    _iter_existing_items,
    _iter_missing_items,
    _summarize_missing_items,
    _total_partitions,
)
from databento_stream_downloader.config import DownloadConfig, RunMode
from databento_stream_downloader.coverage_manifest import (
    write_download_coverage_manifest,
)
from databento_stream_downloader.errors import RetryableError
from databento_stream_downloader.models import DownloadResult

_NONINTERACTIVE_REFUSAL = (
    "[bold red]Refusing non-interactive paid download without --yes.[/bold red]"
)
_EOF_REFUSAL = "[bold red]Confirmation aborted by EOF.[/bold red]"
LOGGER = structlog.get_logger(__name__)


def _validate(
    config: DownloadConfig,
    console: Console,
    work: Iterable[WorkItem],
) -> int:
    from databento_stream_downloader._runner.validation import _validate as validate

    return validate(config, console, work)


def _validate_cached_metadata_preflight(
    config: DownloadConfig,
    work: Iterable[WorkItem],
    console: Console,
) -> int:
    from databento_stream_downloader._runner.validation import (
        _validate_cached_metadata_preflight as validate_cached_metadata_preflight,
    )

    return validate_cached_metadata_preflight(config, work, console)


def _repair_missing_sidecars(
    config: DownloadConfig,
    work: Iterable[WorkItem],
    console: Console,
    *,
    fsync_tracker: _DirectoryFsyncTracker,
) -> int:
    from databento_stream_downloader._runner.validation import (
        _repair_missing_sidecars as repair_missing_sidecars,
    )

    return repair_missing_sidecars(
        config,
        work,
        console,
        fsync_tracker=fsync_tracker,
    )


def run_download(
    config: DownloadConfig,
    api_key: str,
    console: Console,
    error_console: Console | None = None,
) -> None:
    """Run cost estimation, streaming, and validation."""
    _run_download(config, None, console, error_console, api_key=api_key)


def run_download_with_client(
    config: DownloadConfig,
    client: DownloaderClient,
    console: Console,
    error_console: Console | None = None,
) -> None:
    """Run the downloader with an injected client implementation.

    The caller owns console routing. Pass ``console`` for human plan/progress
    output and an optional ``error_console`` for refusals, failures, and
    interruption messages. When ``error_console`` is omitted, the runner creates
    a stderr console so safety-critical output does not share stdout with normal
    human output.
    """
    _run_download(config, client, console, error_console, api_key=None)


def _run_download(
    config: DownloadConfig,
    client: DownloaderClient | None,
    console: Console,
    error_console: Console | None = None,
    *,
    api_key: str | None = None,
) -> None:
    error_console = error_console or Console(stderr=True)
    run_id = str(uuid.uuid4())
    run_started_at = _utc_now()
    fsync_tracker = _DirectoryFsyncTracker()
    _validate_runtime_config(config, fsync_tracker, error_console)
    with (
        _exclusive_run_lock(
            config.data_dir,
            run_id,
            error_console,
            fsync_tracker,
            fsync_writes=config.fsync_writes,
        ),
        structlog.contextvars.bound_contextvars(run_id=run_id),
    ):
        _run_download_locked(
            config=config,
            client=client,
            api_key=api_key,
            console=console,
            error_console=error_console,
            run_id=run_id,
            run_started_at=run_started_at,
            fsync_tracker=fsync_tracker,
        )


def _run_download_locked(
    *,
    config: DownloadConfig,
    client: DownloaderClient | None,
    api_key: str | None,
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
    if config.validate_only:
        metadata_issues = _validate_cached_metadata_preflight(
            config,
            _iter_existing_items(config),
            error_console,
        )
        if metadata_issues:
            raise SystemExit(5)
        if config.write_sidecars:
            repair_issues = _repair_missing_sidecars(
                config,
                _iter_existing_items(config),
                console,
                fsync_tracker=fsync_tracker,
            )
            if repair_issues:
                raise SystemExit(5)
        validation_issues = _validate(config, console, _iter_existing_items(config))
        if validation_issues:
            raise SystemExit(5)
        _write_coverage_manifest(config, run_id)
        return

    if config.validate_cached:
        metadata_issues = _validate_cached_metadata_preflight(
            config,
            _iter_existing_items(config),
            error_console,
        )
        if metadata_issues:
            raise SystemExit(5)
    if config.write_sidecars:
        repair_issues = _repair_missing_sidecars(
            config,
            _iter_existing_items(config),
            console,
            fsync_tracker=fsync_tracker,
        )
        if repair_issues:
            raise SystemExit(5)

    missing_summary = _summarize_missing_items(config)
    if missing_summary.total == 0:
        console.print("All partitions already cached, nothing to do.")
        LOGGER.info("run_cached", total=_total_partitions(config))
        if config.validate_cached:
            validation_issues = _validate(
                config,
                console,
                _iter_existing_items(config),
            )
            if validation_issues:
                raise SystemExit(5)
            _write_coverage_manifest(config, run_id)
        return

    if client is None:
        client = _databento_client(config, api_key)

    try:
        estimates = _estimate_costs(
            client,
            _iter_missing_items(config),
            max_workers=config.max_workers,
            console=console,
        )
    except RetryableError as exc:
        error_console.print(f"[bold red]Cost estimation failed:[/bold red] {exc}")
        raise SystemExit(1) from exc

    total_cents = _total_estimated_cents(estimates)
    total_bytes = sum(item.size_bytes for item in estimates)
    _print_costs(
        console,
        estimates,
        total_cents,
        config.max_cost_cents,
        archive=config.data_dir,
    )
    _check_cost_cap(config, total_cents, error_console)
    _check_bucket_cost_caps(config, estimates, error_console)
    _check_disk_space(config.data_dir, total_bytes, error_console)

    if config.mode is RunMode.DRY_RUN:
        return

    estimated_bytes_by_key = {
        (estimate.symbol, estimate.schema): estimate.size_bytes
        for estimate in estimates
    }
    estimated_cost_cents_by_key = {
        (estimate.symbol, estimate.schema): estimate.cost_cents
        for estimate in estimates
    }
    _warn_in_flight_planning_exposure_for_costs(
        config,
        _iter_allocated_values(
            _iter_missing_items(config),
            missing_summary.counts_by_key,
            estimated_cost_cents_by_key,
        ),
        total_work=missing_summary.total,
        console=error_console,
    )

    if not config.yes:
        if not sys.stdin.isatty():
            error_console.print(_NONINTERACTIVE_REFUSAL)
            raise SystemExit(2)
        try:
            answer = console.input("\n[bold]Proceed?[/bold] [dim]\\[y/N][/dim] ")
        except EOFError:
            error_console.print(_EOF_REFUSAL)
            raise SystemExit(2) from None
        if answer.strip().lower() not in ("y", "yes"):
            return

    started = time.monotonic()
    from databento_stream_downloader._runner.stream import _stream_missing

    result = _stream_missing(
        config,
        client,
        console,
        _iter_missing_items(config),
        total_work=missing_summary.total,
        error_console=error_console,
        estimated_bytes_by_key=estimated_bytes_by_key,
        estimated_cost_cents_by_key=estimated_cost_cents_by_key,
        work_counts_by_key=missing_summary.counts_by_key,
        expected_weekdays_by_key=missing_summary.expected_weekdays_by_key,
        fsync_tracker=fsync_tracker,
    )
    validation_issues = 0
    if config.validate_cached:
        validation_issues = _validate(
            config,
            console,
            _iter_existing_items(config),
        )
    elapsed = time.monotonic() - started
    exit_code = _run_exit_code(result, validation_issues)
    _write_coverage_manifest(config, run_id)
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


def _iter_allocated_values(
    work: Iterable[WorkItem],
    counts_by_key: dict[WorkKey, int],
    values_by_key: dict[WorkKey, int],
) -> Iterable[int]:
    indexes_by_key: dict[WorkKey, int] = {}
    for item in work:
        key = (item.symbol, item.schema)
        count = counts_by_key[key]
        index = indexes_by_key.get(key, 0)
        indexes_by_key[key] = index + 1
        base, remainder = divmod(values_by_key.get(key, 0), count)
        yield base + (1 if index < remainder else 0)


def _run_exit_code(result: DownloadResult, validation_issues: int) -> int:
    if result.failed:
        return 3
    if validation_issues:
        return 5
    return 0


def _write_coverage_manifest(config: DownloadConfig, run_id: str) -> None:
    manifest_path = write_download_coverage_manifest(config, run_id=run_id)
    LOGGER.info("coverage_manifest_updated", path=str(manifest_path))


def _databento_client(config: DownloadConfig, api_key: str | None) -> DownloaderClient:
    if api_key is None:
        msg = "api_key is required when no downloader client is injected"
        raise RuntimeError(msg)
    from databento_stream_downloader.databento_client import DatabentoClient

    return DatabentoClient(
        api_key,
        request_timeout_seconds=config.request_timeout_seconds,
    )


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
