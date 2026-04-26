"""Local filesystem downloader runner."""

from __future__ import annotations

import getpass
import hashlib
import importlib.metadata
import json
import os
import platform
import shutil
import sys
import tempfile
import time
import uuid
from collections.abc import Collection, Generator, Iterable
from concurrent.futures import Future, ThreadPoolExecutor, as_completed
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from threading import Lock
from typing import Any, Literal, Protocol, TextIO, cast

import structlog
import structlog.contextvars
from rich.console import Console
from rich.progress import (
    BarColumn,
    Progress,
    SpinnerColumn,
    TaskProgressColumn,
    TextColumn,
    TimeElapsedColumn,
)
from rich.table import Table

from databento_stream_downloader.config import DownloadConfig, RunMode
from databento_stream_downloader.constants import DATASET, RAW_PREFIX
from databento_stream_downloader.databento_client import DatabentoClient
from databento_stream_downloader.dbn import validate_dbn_metadata
from databento_stream_downloader.errors import (
    DegradedError,
    FatalConfigError,
    FatalError,
    InterruptedDownloadError,
    RetryableError,
    ShutdownRequestedError,
    ValidationError,
)
from databento_stream_downloader.models import (
    CostEstimate,
    CostQuery,
    DownloadResult,
    StreamQuery,
)
from databento_stream_downloader.paths import canonical_path
from databento_stream_downloader.pricing import decimal_dollars_to_cents
from databento_stream_downloader.symbols import (
    load_default_symbols,
    load_first_data_utc_dates,
)

_TMP_GLOB = ".*.tmp"
_TMP_MIN_AGE_SECONDS = 5 * 60
_LEDGER_FILE = "download-ledger.jsonl"
_LOCK_FILE = ".run.lock"
_WINDOWS_LOCK_OFFSET = 1 << 20
_BUCKET_COST_WARN_FRACTION = Decimal("0.25")
_NETWORK_FILESYSTEM_TYPES = frozenset(
    {
        "9p",
        "afs",
        "cifs",
        "ceph",
        "fuse.s3fs",
        "glusterfs",
        "gpfs",
        "juicefs",
        "lustre",
        "nfs",
        "nfs4",
        "s3fs",
        "smb",
        "smbfs",
        "sshfs",
    }
)
_NONINTERACTIVE_REFUSAL = (
    "[bold red]Refusing non-interactive paid download without --yes.[/bold red]"
)
_EOF_REFUSAL = "[bold red]Confirmation aborted by EOF.[/bold red]"

DownloadOutcome = Literal["placed", "cached", "no_data", "failed"]
LOGGER = structlog.get_logger(__name__)


@dataclass(slots=True)
class _DirectoryFsyncTracker:
    _count: int = 0
    _lock: Lock = field(default_factory=Lock)

    def record_skip(self) -> None:
        with self._lock:
            self._count += 1

    def count(self) -> int:
        with self._lock:
            return self._count


class DownloaderClient(Protocol):
    """Client operations needed by the runner.

    Implementations should raise the downloader error taxonomy:
    FatalError/FatalConfigError for unrecoverable failures, RetryableError after
    exhausted transient retries, and DegradedError only for semantic no-data.
    Unknown exception types are treated as per-partition failures by _stream_one.
    """

    def estimate_cost(self, query: CostQuery) -> Decimal: ...

    def estimate_size(self, query: CostQuery) -> int: ...

    def stream_to_file(self, query: StreamQuery, output_path: Path) -> None: ...

    def write_empty_file(self, query: StreamQuery, output_path: Path) -> None: ...


@dataclass(frozen=True, slots=True)
class WorkItem:
    """One missing partition to download."""

    symbol: str
    schema: str
    day: date


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
    estimated_bytes_by_item = _allocate_estimated_billable_bytes(work, estimates)
    estimated_cost_cents_by_item = _allocate_estimated_cost_cents(work, estimates)
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


def _check_cost_cap(
    config: DownloadConfig,
    total_cents: int,
    console: Console,
) -> None:
    if config.max_cost_cents is None:
        console.print(
            "[bold red]Refusing download without an estimated-cost planning cap "
            "via --max-cost-cents "
            "or DATABENTO_MAX_COST_CENTS, even when the estimate is $0.00."
            "[/bold red]"
        )
        raise SystemExit(2)
    _refuse_ambiguous_zero_cap(config, console)
    if total_cents > config.max_cost_cents:
        console.print(
            "[bold red]Estimated cost exceeds planning cap:[/bold red] "
            f"estimate={_money(total_cents)}, "
            f"planning_cap={_money(config.max_cost_cents)}"
        )
        raise SystemExit(2)


def _refuse_ambiguous_zero_cap(config: DownloadConfig, console: Console) -> None:
    if config.max_cost_cents == 0 and not config.allow_free_only:
        console.print(
            "[bold red]Refusing ambiguous zero planning cap:[/bold red] "
            "pass --allow-free-only with --max-cost-cents 0 for intentionally "
            "free-only runs."
        )
        raise SystemExit(2)


def _check_bucket_cost_caps(
    config: DownloadConfig,
    estimates: list[CostEstimate],
    console: Console,
) -> None:
    _refuse_ambiguous_zero_cap(config, console)
    for estimate in estimates:
        label = f"{estimate.symbol}/{estimate.schema}"
        if (
            config.max_cost_cents_per_bucket is not None
            and estimate.cost_cents > config.max_cost_cents_per_bucket
        ):
            console.print(
                "[bold red]Estimated bucket cost exceeds per-bucket "
                "planning cap:[/bold red] "
                f"bucket={label}, estimate={_money(estimate.cost_cents)}, "
                f"planning_cap={_money(config.max_cost_cents_per_bucket)}"
            )
            raise SystemExit(2)
        if config.max_cost_cents is None:
            continue
        if config.max_cost_cents == 0:
            if estimate.cost_cents > 0:
                console.print(
                    "[bold red]Estimated bucket cost exceeds free-only "
                    "planning cap:[/bold red] "
                    f"bucket={label}, estimate={_money(estimate.cost_cents)}, "
                    "planning_cap=$0.00"
                )
                raise SystemExit(2)
            continue
        warn_threshold = (
            Decimal(config.max_cost_cents) / Decimal(100) * _BUCKET_COST_WARN_FRACTION
        )
        if estimate.cost_dollars > warn_threshold:
            console.print(
                "[yellow]Warning:[/yellow] estimated bucket cost exceeds 25% "
                f"of global planning cap: bucket={label}, "
                f"estimate={_money(estimate.cost_cents)}, "
                f"global_planning_cap={_money(config.max_cost_cents)}"
            )


def _estimate_costs(
    client: DownloaderClient,
    work: list[WorkItem],
    *,
    max_workers: int,
) -> list[CostEstimate]:
    estimates_by_key: dict[tuple[str, str], tuple[Decimal, int]] = {}
    ranges = _cost_ranges(work)
    workers = max(1, min(max_workers, len(ranges)))

    pool = ThreadPoolExecutor(max_workers=workers)
    futures: list[Future[tuple[str, str, Decimal, int]]] = []
    wait_for_estimates = True
    try:
        futures = [
            pool.submit(_estimate_range, client, symbol, schema, start, end)
            for symbol, schema, start, end in ranges
        ]
        for future in as_completed(futures):
            try:
                symbol, schema, cost, size_bytes = future.result()
            except Exception:
                wait_for_estimates = False
                _cancel_futures(futures)
                pool.shutdown(wait=False, cancel_futures=True)
                raise
            key = (symbol, schema)
            current_cost, current_size = estimates_by_key.get(key, (Decimal("0"), 0))
            estimates_by_key[key] = (
                current_cost + cost,
                current_size + size_bytes,
            )
    finally:
        pool.shutdown(wait=wait_for_estimates, cancel_futures=not wait_for_estimates)

    estimates = [
        CostEstimate(
            symbol=symbol,
            schema=schema,
            cost_cents=decimal_dollars_to_cents(cost),
            size_bytes=size_bytes,
            cost_dollars=cost,
        )
        for (symbol, schema), (cost, size_bytes) in estimates_by_key.items()
    ]
    estimates.sort(key=lambda estimate: (estimate.symbol, estimate.schema))
    for estimate in estimates:
        LOGGER.info(
            "cost_estimated",
            symbol=estimate.symbol,
            schema=estimate.schema,
            cost_cents=estimate.cost_cents,
            billable_bytes=estimate.size_bytes,
        )
    return estimates


def _estimate_range(
    client: DownloaderClient,
    symbol: str,
    schema: str,
    start: date,
    end: date,
) -> tuple[str, str, Decimal, int]:
    query = CostQuery(
        dataset=DATASET,
        symbol=symbol,
        schema=schema,
        start=start,
        end=end,
    )
    cost = client.estimate_cost(query)
    size_bytes = client.estimate_size(query)
    return (symbol, schema, cost, size_bytes)


def _cost_ranges(work: list[WorkItem]) -> list[tuple[str, str, date, date]]:
    days_by_key: dict[tuple[str, str], list[date]] = {}
    for item in work:
        days_by_key.setdefault((item.symbol, item.schema), []).append(item.day)

    ranges: list[tuple[str, str, date, date]] = []
    for (symbol, schema), days in sorted(days_by_key.items()):
        sorted_days = sorted(set(days))
        start = sorted_days[0]
        previous = start
        for day in sorted_days[1:]:
            if day == previous + timedelta(days=1):
                previous = day
                continue
            ranges.append((symbol, schema, start, previous + timedelta(days=1)))
            start = day
            previous = day
        ranges.append((symbol, schema, start, previous + timedelta(days=1)))
    return ranges


def _check_disk_space(
    data_dir: Path,
    estimated_billable_bytes: int,
    console: Console,
) -> None:
    existing = _nearest_existing_parent(data_dir)
    free_bytes = shutil.disk_usage(existing).free
    if estimated_billable_bytes > free_bytes:
        console.print(
            "[bold red]Insufficient free disk space:[/bold red] "
            f"estimated billable size {_bytes(estimated_billable_bytes)}, "
            f"available {_bytes(free_bytes)} under {existing}. The Databento "
            "billable-size estimate is a conservative disk-space heuristic."
        )
        raise SystemExit(2)


def _reject_known_network_filesystem(data_dir: Path) -> None:
    mount = _mount_for_path(data_dir)
    if mount is None:
        if sys.platform != "linux":
            LOGGER.warning(
                "network_filesystem_detection_unavailable",
                platform=sys.platform,
                data_dir=str(data_dir),
            )
        return
    mount_point, fs_type = mount
    if fs_type.lower() not in _NETWORK_FILESYSTEM_TYPES:
        return
    msg = (
        f"data_dir appears to be on network filesystem {fs_type!r} "
        f"mounted at {mount_point}; use a local filesystem or an external lock"
    )
    raise FatalConfigError(msg)


def _mount_for_path(path: Path) -> tuple[Path, str] | None:
    mounts = _linux_mount_entries()
    if not mounts:
        return None
    resolved = path.resolve(strict=False)
    candidates = [
        (mount_point, fs_type)
        for mount_point, fs_type in mounts
        if resolved.is_relative_to(mount_point)
    ]
    if not candidates:
        return None
    return max(candidates, key=lambda item: len(item[0].parts))


def _linux_mount_entries() -> list[tuple[Path, str]]:
    mounts_path = Path("/proc/mounts")
    if not mounts_path.exists():
        return []
    entries: list[tuple[Path, str]] = []
    for line in mounts_path.read_text(encoding="utf-8").splitlines():
        parts = line.split()
        if len(parts) < 3:
            continue
        entries.append((Path(parts[1]).resolve(strict=False), parts[2]))
    return entries


def _validate_runtime_config(
    config: DownloadConfig,
    fsync_tracker: _DirectoryFsyncTracker | None = None,
) -> None:
    if config.data_dir.exists() and not config.data_dir.is_dir():
        msg = f"data_dir must be a directory, got file: {config.data_dir}"
        raise FatalConfigError(msg)
    _reject_known_network_filesystem(config.data_dir)
    config.data_dir.mkdir(parents=True, exist_ok=True)
    if os.name != "nt" and config.data_dir.stat().st_mode & 0o002:
        msg = f"data_dir must not be world-writable: {config.data_dir}"
        raise FatalConfigError(msg)
    fd, probe_name = tempfile.mkstemp(
        dir=config.data_dir,
        prefix=".write_test.",
        text=True,
    )
    probe = Path(probe_name)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as file:
            fd = -1
            file.write("ok")
            file.flush()
            os.fsync(file.fileno())
        _fsync_directory(config.data_dir, fsync_tracker)
    except OSError as exc:
        msg = f"data_dir is not writable: {config.data_dir}"
        raise FatalConfigError(msg) from exc
    finally:
        if fd >= 0:
            os.close(fd)
        probe.unlink(missing_ok=True)
        _fsync_directory(config.data_dir, fsync_tracker)


@contextmanager
def _exclusive_run_lock(
    data_dir: Path,
    run_id: str,
    console: Console,
    fsync_tracker: _DirectoryFsyncTracker | None = None,
) -> Generator[None]:
    lock_path = data_dir / _LOCK_FILE
    lock_file = lock_path.open("a+", encoding="utf-8")
    locked = False
    try:
        if not _try_lock_file(lock_file):
            console.print(
                f"[bold red]Another downloader run is active:[/bold red] {lock_path}"
            )
            raise SystemExit(2)
        locked = True
        payload = {"run_id": run_id}
        lock_file.seek(0)
        lock_file.truncate()
        json.dump(payload, lock_file, sort_keys=True)
        lock_file.write("\n")
        lock_file.flush()
        os.fsync(lock_file.fileno())
        _fsync_directory(data_dir, fsync_tracker)
        yield
    finally:
        if locked:
            _unlock_file(lock_file)
        lock_file.close()
        _fsync_directory(data_dir, fsync_tracker)


def _try_lock_file(file: TextIO) -> bool:
    if os.name == "nt":
        import msvcrt

        try:
            file.seek(_WINDOWS_LOCK_OFFSET)
            msvcrt.locking(file.fileno(), msvcrt.LK_NBLCK, 1)
        except OSError:
            return False
        file.seek(0)
        return True
    import fcntl

    try:
        fcntl.flock(file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError:
        return False
    return True


def _unlock_file(file: TextIO) -> None:
    if os.name == "nt":
        import msvcrt

        try:
            file.seek(_WINDOWS_LOCK_OFFSET)
            msvcrt.locking(file.fileno(), msvcrt.LK_UNLCK, 1)
        except OSError:
            return
        file.seek(0)
        return
    import fcntl

    fcntl.flock(file.fileno(), fcntl.LOCK_UN)


def _nearest_existing_parent(path: Path) -> Path:
    current = path.resolve()
    while not current.exists():
        current = current.parent
    return current


def _stream_missing(
    config: DownloadConfig,
    client: DownloaderClient,
    console: Console,
    work: list[WorkItem],
    *,
    error_console: Console | None = None,
    estimated_bytes_by_item: dict[WorkItem, int] | None = None,
    estimated_cost_cents_by_item: dict[WorkItem, int] | None = None,
    fsync_tracker: _DirectoryFsyncTracker | None = None,
) -> DownloadResult:
    placed = 0
    no_data = 0
    failed = 0
    completed_work = 0
    landed_estimated_cents = 0
    landed_estimated_bytes = 0
    failure_console = error_console or Console(stderr=True)

    progress = Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        TaskProgressColumn(),
        TimeElapsedColumn(),
        console=console,
    )
    pool = ThreadPoolExecutor(max_workers=config.max_workers)
    futures: list[Future[tuple[DownloadOutcome, str]]] = []
    future_items: dict[Future[tuple[DownloadOutcome, str]], WorkItem] = {}
    work_days_by_key: dict[tuple[str, str], set[date]] = {}
    no_data_days_by_key: dict[tuple[str, str], set[date]] = {}
    for item in work:
        key = (item.symbol, item.schema)
        work_days_by_key.setdefault(key, set()).add(item.day)
    fatal = False
    interrupted = False
    try:
        progress.start()
        task = progress.add_task("Downloading", total=len(work))
        futures = [
            pool.submit(
                _stream_one,
                config,
                client,
                item,
                (estimated_bytes_by_item or {}).get(item),
                fsync_tracker,
            )
            for item in work
        ]
        future_items = dict(zip(futures, work, strict=True))
        for future in as_completed(futures):
            try:
                outcome, label = future.result()
            except FatalError:
                fatal = True
                _cancel_futures(futures)
                pool.shutdown(wait=False, cancel_futures=True)
                raise
            except Exception:
                fatal = True
                _cancel_futures(futures)
                pool.shutdown(wait=False, cancel_futures=True)
                raise
            item = future_items[future]
            if outcome == "placed":
                placed += 1
            elif outcome == "cached":
                pass
            elif outcome == "no_data":
                no_data += 1
                key = (item.symbol, item.schema)
                no_data_days_by_key.setdefault(key, set()).add(item.day)
                if not progress.console.quiet:
                    progress.console.print(f"  [yellow]⚠[/yellow] no data {label}")
            else:
                failed += 1
                if not progress.console.quiet:
                    progress.console.print(f"  [red]✗[/red] {label}")
                else:
                    failure_console.print(f"  [red]✗[/red] {label}")
            if outcome in ("placed", "no_data"):
                landed_estimated_bytes += (estimated_bytes_by_item or {}).get(
                    item,
                    0,
                )
                landed_estimated_cents += (estimated_cost_cents_by_item or {}).get(
                    item,
                    0,
                )
                if (
                    config.max_cost_cents is not None
                    and landed_estimated_cents > config.max_cost_cents
                ):
                    fatal = True
                    _cancel_futures(futures)
                    pool.shutdown(wait=False, cancel_futures=True)
                    msg = (
                        "in-flight planned cost exceeded planning cap: "
                        f"landed={_money(landed_estimated_cents)}, "
                        f"planning_cap={_money(config.max_cost_cents)}"
                    )
                    raise FatalConfigError(msg)
            LOGGER.info(
                "partition_completed",
                symbol=item.symbol,
                schema=item.schema,
                day=item.day.isoformat(),
                outcome=outcome,
                label=label,
            )
            completed_work += 1
            progress.advance(task)
    except (KeyboardInterrupt, ShutdownRequestedError) as exc:
        interrupted = True
        _cancel_futures(futures)
        pool.shutdown(wait=False, cancel_futures=True)
        if isinstance(exc, ShutdownRequestedError):
            raise
        raise InterruptedDownloadError("download interrupted by user") from exc
    finally:
        progress.stop()
        if fatal or interrupted:
            pool.shutdown(wait=False, cancel_futures=True)
        else:
            pool.shutdown(wait=True, cancel_futures=False)

    if completed_work != len(work):
        if interrupted or fatal:
            raise InterruptedDownloadError("download did not complete")
        msg = (
            f"download work accounting failed: completed={completed_work}, "
            f"work={len(work)}"
        )
        raise RuntimeError(msg)

    cached = _total_partitions(config) - placed - no_data - failed
    _raise_on_suspicious_all_no_data(
        work_days_by_key,
        no_data_days_by_key,
        threshold_weekdays=config.suspicious_no_data_weekdays,
    )
    return DownloadResult(
        total=_total_partitions(config),
        placed=placed,
        cached=cached,
        no_data=no_data,
        failed=failed,
        estimated_cost_cents_landed=landed_estimated_cents,
        estimated_billable_bytes_landed=landed_estimated_bytes,
    )


def _raise_on_suspicious_all_no_data(
    work_days_by_key: dict[tuple[str, str], set[date]],
    no_data_days_by_key: dict[tuple[str, str], set[date]],
    *,
    threshold_weekdays: int,
) -> None:
    first_data_utc = load_first_data_utc_dates()
    for (symbol, schema), work_days in work_days_by_key.items():
        no_data_days = no_data_days_by_key.get((symbol, schema), set())
        if no_data_days != work_days:
            continue
        first_data_day = first_data_utc.get(symbol)
        expected_weekdays = _expected_weekdays_after_first_data(
            work_days,
            first_data_day,
        )
        if expected_weekdays < threshold_weekdays:
            continue
        msg = (
            f"all {len(work_days)} requested partitions returned no data for "
            f"{symbol}/{schema}, including {expected_weekdays} weekdays on or "
            "after the configured first UTC data day; check the parent symbol "
            "before treating these files as canonical coverage"
        )
        raise FatalConfigError(msg)


def _expected_weekdays_after_first_data(
    days: set[date],
    first_data_day: date | None,
) -> int:
    return sum(
        1
        for day in days
        if day.weekday() < 5 and (first_data_day is None or day >= first_data_day)
    )


def _cancel_futures(futures: Iterable[Future[Any]]) -> None:
    for pending in futures:
        _ = pending.cancel()


def _deep_validate_cap(estimated_billable_bytes: int | None) -> int | None:
    if estimated_billable_bytes is None:
        return None
    return max(estimated_billable_bytes * 2, 64 * 1024 * 1024)


def _stream_one(
    config: DownloadConfig,
    client: DownloaderClient,
    item: WorkItem,
    estimated_billable_bytes: int | None = None,
    fsync_tracker: _DirectoryFsyncTracker | None = None,
) -> tuple[DownloadOutcome, str]:
    day = item.day
    dest = canonical_path(config.data_dir, item.symbol, item.schema, day)
    label = f"{item.symbol}/{item.schema} {day.isoformat()}"
    if dest.exists():
        return ("cached", label)
    dest.parent.mkdir(parents=True, exist_ok=True)
    tmp = dest.with_name(f".{dest.stem}.tmp")
    query = StreamQuery(
        dataset=DATASET,
        symbol=item.symbol,
        schema=item.schema,
        start=day,
        end=day + timedelta(days=1),
    )
    try:
        client.stream_to_file(query, tmp)
        validate_dbn_metadata(
            query,
            tmp,
            deep=config.deep_validate,
            strict=config.strict_validate,
            max_decompressed_bytes=_deep_validate_cap(estimated_billable_bytes),
        )
        digest = _place_tmp(tmp, dest, fsync_tracker)
        _write_sha256_sidecar(dest, digest, fsync_tracker)
        return ("placed", label)
    except DegradedError:
        try:
            client.write_empty_file(query, tmp)
            validate_dbn_metadata(
                query,
                tmp,
                deep=config.deep_validate,
                strict=config.strict_validate,
                max_decompressed_bytes=_deep_validate_cap(estimated_billable_bytes),
            )
            digest = _place_tmp(tmp, dest, fsync_tracker)
            _write_sha256_sidecar(dest, digest, fsync_tracker)
            return ("no_data", label)
        except ValidationError as exc:
            tmp.unlink(missing_ok=True)
            return ("failed", f"{label}: validation error: {exc}")
        except Exception:
            tmp.unlink(missing_ok=True)
            raise
    except FatalError:
        tmp.unlink(missing_ok=True)
        raise
    except RetryableError as exc:
        tmp.unlink(missing_ok=True)
        return ("failed", f"{label}: retryable error: {exc}")
    except ValidationError as exc:
        tmp.unlink(missing_ok=True)
        return ("failed", f"{label}: validation error: {exc}")
    except Exception:
        tmp.unlink(missing_ok=True)
        raise


def _place_tmp(
    tmp: Path,
    dest: Path,
    fsync_tracker: _DirectoryFsyncTracker | None = None,
) -> str:
    digest = _fsync_and_sha256_file(tmp)
    os.replace(tmp, dest)
    _fsync_directory(dest.parent, fsync_tracker)
    return digest


def _write_sha256_sidecar(
    path: Path,
    digest: str | None = None,
    fsync_tracker: _DirectoryFsyncTracker | None = None,
) -> None:
    digest = digest or _sha256_file(path)
    sidecar = _sha256_sidecar_path(path)
    tmp = sidecar.with_name(f".{sidecar.name}.tmp")
    with tmp.open("w", encoding="ascii") as file:
        file.write(f"{digest}  {path.name}\n")
        file.flush()
        os.fsync(file.fileno())
    _ = _place_tmp(tmp, sidecar, fsync_tracker)


def _validate_sha256_sidecar(path: Path) -> None:
    expected = _read_sha256_sidecar(path)
    actual = _sha256_file(path)
    if actual != expected:
        msg = f"SHA256 sidecar mismatch: expected {expected}, got {actual}"
        raise ValidationError(msg)


def _read_sha256_sidecar(path: Path) -> str:
    sidecar = _sha256_sidecar_path(path)
    if not sidecar.exists():
        raise ValidationError("SHA256 sidecar missing")
    try:
        digest, filename = sidecar.read_text(encoding="ascii").split(maxsplit=1)
    except (OSError, UnicodeDecodeError, ValueError) as exc:
        raise ValidationError("SHA256 sidecar malformed") from exc
    if len(digest) != 64 or any(char not in "0123456789abcdef" for char in digest):
        raise ValidationError("SHA256 sidecar malformed")
    if filename.strip() != path.name:
        raise ValidationError("SHA256 sidecar filename mismatch")
    return digest


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as file:
        for chunk in iter(lambda: file.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _sha256_sidecar_path(path: Path) -> Path:
    return path.with_suffix(path.suffix + ".sha256")


def _fsync_and_sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    mode = "r+b" if os.name == "nt" else "rb"
    with path.open(mode) as file:
        for chunk in iter(lambda: file.read(1024 * 1024), b""):
            digest.update(chunk)
        os.fsync(file.fileno())
    return digest.hexdigest()


def _fsync_directory(
    path: Path,
    fsync_tracker: _DirectoryFsyncTracker | None = None,
) -> None:
    try:
        fd = os.open(path, os.O_RDONLY)
    except OSError as exc:
        _record_directory_fsync_skipped(path, exc, fsync_tracker)
        return
    try:
        os.fsync(fd)
    except OSError as exc:
        _record_directory_fsync_skipped(path, exc, fsync_tracker)
    finally:
        os.close(fd)


def _record_directory_fsync_skipped(
    path: Path,
    exc: OSError,
    fsync_tracker: _DirectoryFsyncTracker | None = None,
) -> None:
    if fsync_tracker is not None:
        fsync_tracker.record_skip()
    LOGGER.warning("directory_fsync_skipped", path=str(path), error=str(exc))


def _validate(
    config: DownloadConfig,
    console: Console,
    items: list[WorkItem],
) -> int:
    console.print("\n[bold]Validating downloads...[/bold]")
    issues = 0
    checked = 0
    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        TaskProgressColumn(),
        TimeElapsedColumn(),
        console=console,
    ) as progress:
        task = progress.add_task("Validating", total=len(items))
        workers = max(1, min(config.max_workers, len(items)))
        with ThreadPoolExecutor(max_workers=workers) as pool:
            futures = [pool.submit(_validate_one, config, item) for item in items]
            for future in as_completed(futures):
                item, error = future.result()
                if error is None:
                    checked += 1
                else:
                    issues += 1
                    progress.console.print(
                        f"  [red]✗[/red] {item.symbol}/{item.schema} "
                        f"{item.day}: {error}"
                    )
                progress.advance(task)
    if issues:
        console.print(f"  [red]✗[/red] {issues} validation issues")
        return issues
    console.print(f"  [green]✓[/green] {checked} files validated")
    return 0


def _validate_one(
    config: DownloadConfig,
    item: WorkItem,
) -> tuple[WorkItem, str | None]:
    path = canonical_path(config.data_dir, item.symbol, item.schema, item.day)
    query = StreamQuery(
        dataset=DATASET,
        symbol=item.symbol,
        schema=item.schema,
        start=item.day,
        end=item.day + timedelta(days=1),
    )
    try:
        validate_dbn_metadata(
            query,
            path,
            deep=config.deep_validate,
            strict=config.strict_validate,
        )
        _validate_sha256_sidecar(path)
    except ValidationError as exc:
        LOGGER.warning(
            "validation_failed",
            symbol=item.symbol,
            schema=item.schema,
            day=item.day.isoformat(),
            error=str(exc),
        )
        return (item, str(exc))
    return (item, None)


def _work_items_from_all(
    items: list[WorkItem],
    existing_items: set[WorkItem],
) -> list[WorkItem]:
    return [item for item in items if item not in existing_items]


def _sorted_items(items: Iterable[WorkItem]) -> list[WorkItem]:
    return sorted(items, key=lambda item: (item.symbol, item.schema, item.day))


def _all_items(config: DownloadConfig) -> list[WorkItem]:
    items: list[WorkItem] = []
    first_data_utc = load_first_data_utc_dates()
    for symbol in config.symbols:
        symbol_start = _effective_start(config, symbol, first_data_utc)
        if symbol_start > config.end:
            continue
        for schema in config.schemas:
            day = symbol_start
            while day <= config.end:
                items.append(WorkItem(symbol=symbol, schema=schema, day=day))
                day += timedelta(days=1)
    return items


def _cached_items(all_items: list[WorkItem], missing: list[WorkItem]) -> list[WorkItem]:
    missing_set = set(missing)
    return [item for item in all_items if item not in missing_set]


def _existing_items(config: DownloadConfig) -> set[WorkItem]:
    items: set[WorkItem] = set()
    first_data_utc = load_first_data_utc_dates()
    end = config.end
    for symbol in config.symbols:
        start = _effective_start(config, symbol, first_data_utc)
        if start > end:
            continue
        for schema in config.schemas:
            directory = config.data_dir / RAW_PREFIX / symbol / schema
            if not directory.exists():
                continue
            _warn_suspicious_archive_files(directory)
            for path in directory.glob("*.dbn.zst"):
                try:
                    day = date.fromisoformat(path.name.removesuffix(".dbn.zst"))
                except ValueError:
                    continue
                if start <= day <= end:
                    items.add(WorkItem(symbol=symbol, schema=schema, day=day))
    return items


def _warn_suspicious_archive_files(directory: Path) -> None:
    for path in directory.iterdir():
        name = path.name
        if (
            ".dbn.zst" in name
            and not name.endswith(".dbn.zst")
            and not name.endswith(".dbn.zst.sha256")
            and not name.endswith(".tmp")
            and not name.startswith(".")
        ):
            LOGGER.warning("suspicious_archive_file_ignored", path=str(path))


def _repair_missing_sidecars(
    config: DownloadConfig,
    items: Collection[WorkItem],
    console: Console,
    *,
    fsync_tracker: _DirectoryFsyncTracker | None = None,
) -> int:
    needs_repair = [item for item in items if _sidecar_needs_repair(config, item)]
    if not needs_repair:
        return 0
    issues = 0
    workers = max(1, min(config.max_workers, len(needs_repair)))
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = [
            pool.submit(_repair_one_sidecar, config, item, fsync_tracker)
            for item in needs_repair
        ]
        for future in as_completed(futures):
            item, error, repaired = future.result()
            if error is not None:
                issues += 1
                console.print(
                    f"  [red]✗[/red] {item.symbol}/{item.schema} "
                    f"{item.day}: cannot repair sidecar: {error}"
                )
            if repaired:
                LOGGER.info(
                    "sidecar_repaired",
                    symbol=item.symbol,
                    schema=item.schema,
                    day=item.day.isoformat(),
                )
    return issues


def _sidecar_needs_repair(config: DownloadConfig, item: WorkItem) -> bool:
    path = canonical_path(config.data_dir, item.symbol, item.schema, item.day)
    if not path.exists():
        return False
    return _sidecar_repair_state(path) != "ok"


def _repair_one_sidecar(
    config: DownloadConfig,
    item: WorkItem,
    fsync_tracker: _DirectoryFsyncTracker | None = None,
) -> tuple[WorkItem, str | None, bool]:
    path = canonical_path(config.data_dir, item.symbol, item.schema, item.day)
    if not path.exists():
        return (item, None, False)
    sidecar_state = _sidecar_repair_state(path)
    if sidecar_state == "ok":
        return (item, None, False)
    if sidecar_state == "mismatch":
        try:
            _validate_sha256_sidecar(path)
        except ValidationError as exc:
            return (item, str(exc), False)
    query = StreamQuery(
        dataset=DATASET,
        symbol=item.symbol,
        schema=item.schema,
        start=item.day,
        end=item.day + timedelta(days=1),
    )
    try:
        validate_dbn_metadata(
            query,
            path,
            deep=False,
            strict=False,
        )
    except ValidationError as exc:
        return (item, str(exc), False)
    _write_sha256_sidecar(path, fsync_tracker=fsync_tracker)
    return (item, None, True)


def _sidecar_repair_state(path: Path) -> str:
    sidecar = _sha256_sidecar_path(path)
    if not sidecar.exists():
        return "missing"
    try:
        _ = _read_sha256_sidecar(path)
    except ValidationError:
        return "malformed"
    try:
        _validate_sha256_sidecar(path)
    except ValidationError:
        return "mismatch"
    return "ok"


def _allocate_estimated_billable_bytes(
    work: list[WorkItem],
    estimates: list[CostEstimate],
) -> dict[WorkItem, int]:
    return _allocate_estimated_values(
        work,
        {
            (estimate.symbol, estimate.schema): estimate.size_bytes
            for estimate in estimates
        },
    )


def _allocate_estimated_cost_cents(
    work: list[WorkItem],
    estimates: list[CostEstimate],
) -> dict[WorkItem, int]:
    return _allocate_estimated_values(
        work,
        {
            (estimate.symbol, estimate.schema): estimate.cost_cents
            for estimate in estimates
        },
    )


def _allocate_estimated_values(
    work: list[WorkItem],
    values_by_key: dict[tuple[str, str], int],
) -> dict[WorkItem, int]:
    items_by_key: dict[tuple[str, str], list[WorkItem]] = {}
    for item in work:
        key = (item.symbol, item.schema)
        items_by_key.setdefault(key, []).append(item)
    allocation: dict[WorkItem, int] = {}
    for key, items in items_by_key.items():
        if key not in values_by_key:
            msg = f"missing estimate for work key: {key}"
            raise RuntimeError(msg)
        total = values_by_key[key]
        base, remainder = divmod(total, len(items))
        for index, item in enumerate(items):
            allocation[item] = base + (1 if index < remainder else 0)
    return allocation


def _total_estimated_cents(estimates: list[CostEstimate]) -> int:
    return decimal_dollars_to_cents(
        sum((estimate.cost_dollars for estimate in estimates), Decimal("0"))
    )


def _print_retry_summary(client: DownloaderClient, console: Console) -> None:
    retry_count = getattr(client, "retry_count", None)
    if isinstance(retry_count, int):
        console.print(f"Databento retries: {retry_count}")
    else:
        console.print("Databento retries: unavailable for injected client")


def _retry_count_total(client: DownloaderClient) -> int:
    retry_count = getattr(client, "retry_count", None)
    return retry_count if isinstance(retry_count, int) else 0


def _retry_counts_by_operation(client: DownloaderClient) -> dict[str, int]:
    retry_counts = getattr(client, "retry_counts_by_operation", None)
    if isinstance(retry_counts, dict):
        typed = cast("dict[object, object]", retry_counts)
        if all(
            isinstance(key, str) and isinstance(value, int)
            for key, value in typed.items()
        ):
            return cast("dict[str, int]", retry_counts)
    return {}


def _write_run_ledger(
    *,
    config: DownloadConfig,
    client: DownloaderClient,
    fsync_tracker: _DirectoryFsyncTracker,
    run_id: str,
    result: DownloadResult,
    validation_issues: int,
    exit_code: int,
    interrupted: bool,
    elapsed_seconds: float,
    estimated_cost_cents: int,
    estimated_billable_bytes: int,
    started_at: str,
    ended_at: str,
) -> None:
    ledger_path = config.data_dir / _LEDGER_FILE
    _rotate_ledger_if_needed(ledger_path, config.ledger_rotate_mb, fsync_tracker)
    package_version = importlib.metadata.version("databento-stream-downloader")
    host = platform.node()
    user = getpass.getuser()
    universe_sha256 = _universe_semantic_sha256()
    symbols_sha256 = hashlib.sha256(
        "\n".join(config.symbols).encode("utf-8"),
    ).hexdigest()
    payload = {
        "ledger_schema_version": 3,
        "run_id": run_id,
        "dataset": DATASET,
        "package_version": package_version,
        "host": host,
        "user": user,
        "mode": config.mode.value,
        "deep_validate": config.deep_validate,
        "strict_validate": config.strict_validate,
        "validate_cached": config.validate_cached,
        "started_at": started_at,
        "ended_at": ended_at,
        "symbols": list(config.symbols),
        "schemas": list(config.schemas),
        "start": config.start.isoformat(),
        "end": config.end.isoformat(),
        "placed": result.placed,
        "cached": result.cached,
        "no_data": result.no_data,
        "failed": result.failed,
        "validation_issues": validation_issues,
        "exit_code": exit_code,
        "interrupted": interrupted,
        "estimated_cost_cents": estimated_cost_cents,
        "estimated_cost_cents_landed": result.estimated_cost_cents_landed,
        "estimated_billable_bytes": estimated_billable_bytes,
        "estimated_billable_bytes_landed": result.estimated_billable_bytes_landed,
        "retry_count_total": _retry_count_total(client),
        "retry_count_by_operation": _retry_counts_by_operation(client),
        "directory_fsync_skipped_count": fsync_tracker.count(),
        "elapsed_seconds": round(elapsed_seconds, 3),
        "universe_sha256": universe_sha256,
        "symbols_sha256": symbols_sha256,
    }
    ledger_path.parent.mkdir(parents=True, exist_ok=True)
    with ledger_path.open("a", encoding="utf-8") as file:
        file.write(json.dumps(payload, sort_keys=True))
        file.write("\n")
        file.flush()
        os.fsync(file.fileno())
    _fsync_directory(ledger_path.parent, fsync_tracker)


def _rotate_ledger_if_needed(
    ledger_path: Path,
    rotate_mb: int,
    fsync_tracker: _DirectoryFsyncTracker | None = None,
) -> None:
    if not ledger_path.exists():
        return
    max_bytes = rotate_mb * 1024 * 1024
    if ledger_path.stat().st_size <= max_bytes:
        return
    rotated = ledger_path.with_name(
        f"{ledger_path.stem}.{datetime.now(UTC).strftime('%Y%m%dT%H%M%SZ')}"
        f".{uuid.uuid4().hex[:8]}"
        f"{ledger_path.suffix}"
    )
    os.replace(ledger_path, rotated)
    _fsync_directory(ledger_path.parent, fsync_tracker)
    LOGGER.info(
        "ledger_rotated",
        old_path=str(ledger_path),
        new_path=str(rotated),
        rotate_mb=rotate_mb,
    )


def _universe_semantic_sha256() -> str:
    first_data_utc = {
        symbol: value.isoformat()
        for symbol, value in sorted(load_first_data_utc_dates().items())
    }
    payload = {
        "symbols": list(load_default_symbols()),
        "first_data_utc": first_data_utc,
    }
    return hashlib.sha256(
        json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()


def _sweep_stale_tmp_files(config: DownloadConfig) -> None:
    cutoff = time.time() - _TMP_MIN_AGE_SECONDS
    for symbol in config.symbols:
        for schema in config.schemas:
            root = config.data_dir / RAW_PREFIX / symbol / schema
            if not root.exists():
                continue
            for tmp in root.glob(_TMP_GLOB):
                if tmp.is_file() and tmp.stat().st_mtime < cutoff:
                    tmp.unlink(missing_ok=True)
                    LOGGER.warning(
                        "stale_tmp_removed",
                        path=str(tmp),
                        min_age_seconds=_TMP_MIN_AGE_SECONDS,
                        message=(
                            "removed abandoned temporary file; this should only "
                            "happen after SIGKILL or process crash"
                        ),
                    )


def _utc_now() -> str:
    return datetime.now(UTC).isoformat()


def _total_partitions(config: DownloadConfig) -> int:
    first_data_utc = load_first_data_utc_dates()
    total = 0
    for symbol in config.symbols:
        start = _effective_start(config, symbol, first_data_utc)
        if start > config.end:
            continue
        total += len(config.schemas) * ((config.end - start).days + 1)
    return total


def _effective_start(
    config: DownloadConfig,
    symbol: str,
    first_data_utc: dict[str, date],
) -> date:
    first_data_day = first_data_utc.get(symbol)
    if first_data_day is None:
        return config.start
    return max(config.start, first_data_day)


def _print_costs(
    console: Console,
    estimates: list[CostEstimate],
    total_cents: int,
    max_cost_cents: int | None,
) -> None:
    table = Table(title="Download Plan")
    table.add_column("Symbol")
    table.add_column("Schema")
    table.add_column("Size", justify="right")
    table.add_column("Cost", justify="right")
    ranked = sorted(
        estimates,
        key=lambda item: (-item.cost_cents, -item.size_bytes, item.symbol, item.schema),
    )
    if len(ranked) > 15:
        table.add_section()
        table.add_row("Top 10", "", "", "")
        for item in ranked[:10]:
            table.add_row(
                item.symbol,
                item.schema,
                _bytes(item.size_bytes),
                _money(item.cost_cents),
            )
        table.add_section()
        table.add_row("Full breakdown", "", "", "")
    total_bytes = 0
    for item in ranked:
        total_bytes += item.size_bytes
        table.add_row(
            item.symbol,
            item.schema,
            _bytes(item.size_bytes),
            _money(item.cost_cents),
        )
    table.add_section()
    table.add_row("", "Total", _bytes(total_bytes), _money(total_cents))
    if max_cost_cents is not None:
        table.add_row("", "Planning cap", "", _money(max_cost_cents))
    console.print(table)


def _money(cents: int) -> str:
    if cents < 0:
        msg = f"cents must be non-negative, got {cents}"
        raise ValueError(msg)
    return f"${cents // 100:,}.{cents % 100:02d}"


def _bytes(size: int) -> str:
    if size >= 1_099_511_627_776:
        return f"{size / 1_099_511_627_776:.2f} TiB"
    if size >= 1_073_741_824:
        return f"{size / 1_073_741_824:.2f} GiB"
    if size >= 1_048_576:
        return f"{size / 1_048_576:.1f} MiB"
    if size >= 1024:
        return f"{size / 1024:.1f} KiB"
    return f"{size} B"
