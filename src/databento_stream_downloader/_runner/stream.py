"""Streaming stage orchestration."""

from __future__ import annotations

import time
from collections.abc import Iterable, Sized
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait
from dataclasses import dataclass, field
from datetime import date, timedelta
from pathlib import Path
from threading import Lock

import structlog
from rich.console import Console, Group
from rich.live import Live
from rich.panel import Panel
from rich.progress import (
    BarColumn,
    MofNCompleteColumn,
    Progress,
    SpinnerColumn,
    TaskProgressColumn,
    TextColumn,
    TimeElapsedColumn,
    TimeRemainingColumn,
)
from rich.table import Table
from rich.text import Text

from databento_stream_downloader._runner.concurrency import (
    _cancel_futures,
    _InFlightRegistry,
)
from databento_stream_downloader._runner.format import _bytes, _money
from databento_stream_downloader._runner.fsio import _place_tmp, _write_sha256_sidecar
from databento_stream_downloader._runner.types import (
    DownloaderClient,
    DownloadOutcome,
    WorkItem,
    _DirectoryFsyncTracker,
)
from databento_stream_downloader._runner.validation import (
    _raise_on_suspicious_all_no_data,
)
from databento_stream_downloader._runner.work import _total_partitions
from databento_stream_downloader.config import DownloadConfig
from databento_stream_downloader.constants import DATASET
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
from databento_stream_downloader.models import DownloadResult, StreamQuery
from databento_stream_downloader.paths import canonical_path

LOGGER = structlog.get_logger(__name__)
_LARGE_STREAM_BYTES = 512 * 1024 * 1024
_MAX_LARGE_STREAMS = 2
_SPEED_WINDOW_SECONDS = 20.0
_StreamResult = tuple[DownloadOutcome, str]
WorkKey = tuple[str, str]


@dataclass(frozen=True, slots=True)
class _PendingWork:
    item: WorkItem
    estimated_bytes: int | None
    estimated_cost_cents: int


@dataclass(frozen=True, slots=True)
class _ActiveDownload:
    item: WorkItem
    tmp_path: Path
    estimated_bytes: int | None


@dataclass(slots=True)
class _OutcomeCounts:
    placed: int = 0
    no_data: int = 0
    failed: int = 0
    _lock: Lock = field(default_factory=Lock)

    def record(self, outcome: DownloadOutcome) -> None:
        with self._lock:
            if outcome == "placed":
                self.placed += 1
            elif outcome == "no_data":
                self.no_data += 1
            elif outcome == "failed":
                self.failed += 1

    def snapshot(self) -> tuple[int, int, int]:
        with self._lock:
            return (self.placed, self.no_data, self.failed)


@dataclass(slots=True)
class _LiveProgressState:
    active_workers: int = 0
    download_samples: dict[Path, list[tuple[float, int]]] = field(default_factory=dict)
    _lock: Lock = field(default_factory=Lock)

    def set_active_workers(self, active_workers: int) -> None:
        with self._lock:
            self.active_workers = active_workers

    def snapshot_active_workers(self) -> int:
        with self._lock:
            return self.active_workers

    def speed_for(self, path: Path, downloaded_bytes: int) -> int | None:
        now = _progress_time()
        with self._lock:
            samples = self.download_samples.setdefault(path, [])
            samples.append((now, downloaded_bytes))
            cutoff = now - _SPEED_WINDOW_SECONDS
            while len(samples) > 2 and samples[1][0] < cutoff:
                samples.pop(0)
            if len(samples) < 2:
                return None
            previous_time, previous_bytes = samples[0]
        elapsed = now - previous_time
        if elapsed <= 0:
            return None
        return max(0, int((downloaded_bytes - previous_bytes) / elapsed))

    def prune_downloads(self, paths: set[Path]) -> None:
        with self._lock:
            stale_paths = set(self.download_samples) - paths
            for path in stale_paths:
                self.download_samples.pop(path, None)


@dataclass(frozen=True, slots=True)
class _ProgressPanelRenderable:
    progress: Progress
    tracker: _InFlightRegistry[_ActiveDownload]
    counts: _OutcomeCounts
    max_workers: int
    state: _LiveProgressState

    def __rich__(self) -> Panel:
        return _render_progress_panel(
            self.progress,
            self.tracker,
            self.counts,
            self.max_workers,
            self.state,
        )


def _render_progress_panel(
    progress: Progress,
    tracker: _InFlightRegistry[_ActiveDownload],
    counts: _OutcomeCounts,
    max_workers: int,
    state: _LiveProgressState,
) -> Panel:
    placed, no_data, failed = counts.snapshot()
    in_flight = tracker.snapshot()
    state.prune_downloads({active.tmp_path for active in in_flight})

    activity = Table.grid(padding=(0, 1))
    activity.add_column(no_wrap=True)
    if in_flight:
        for active in in_flight:
            item = active.item
            activity.add_row(
                Text.assemble(
                    ("▸ ", "dim"),
                    (f"{item.symbol}/{item.schema}", "cyan"),
                    "  ",
                    (item.day.isoformat(), "white"),
                    "  ",
                    (
                        _download_progress_label(active, state),
                        "magenta",
                    ),
                ),
            )
    else:
        activity.add_row(Text("(idle)", style="dim italic"))

    counts_line = Text.assemble(
        ("✓ ", "green"),
        (f"{placed:,} placed", "green"),
        ("    ⊘ ", "yellow"),
        (f"{no_data:,} no_data", "yellow"),
        ("    ✗ ", "red bold" if failed else "red"),
        (f"{failed:,} failed", "red bold" if failed else "red"),
    )
    workers_line = Text.assemble(
        ("Workers ", "dim"),
        (f"{state.snapshot_active_workers()}", "bold"),
        (f" / {max_workers} active", "dim"),
    )

    return Panel(
        Group(progress, Text(""), workers_line, activity, Text(""), counts_line),
        title="[bold]Databento stream download[/bold]",
        border_style="cyan",
        padding=(1, 2),
    )


def _download_progress_label(
    active: _ActiveDownload,
    state: _LiveProgressState | None = None,
) -> str:
    downloaded_bytes = _stat_downloaded_bytes(active.tmp_path)
    if state is None:
        return f"{_bytes(downloaded_bytes)} downloaded"
    bytes_per_second = state.speed_for(active.tmp_path, downloaded_bytes)
    if bytes_per_second is None:
        return f"{_bytes(downloaded_bytes)} downloaded; measuring speed"
    return f"{_bytes(downloaded_bytes)} downloaded; {_bytes(bytes_per_second)}/s avg"


def _stat_downloaded_bytes(path: Path) -> int:
    try:
        return path.stat().st_size
    except OSError:
        return 0


def _progress_time() -> float:
    return time.monotonic()


def _build_progress(quiet: bool, console: Console) -> Progress:
    return Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(bar_width=None),
        MofNCompleteColumn(),
        TaskProgressColumn(),
        TextColumn("•"),
        TimeElapsedColumn(),
        TextColumn("•"),
        TimeRemainingColumn(compact=True, elapsed_when_finished=True),
        console=console,
        transient=False,
        expand=True,
        disable=quiet,
    )


def _display_active_workers(
    total_work: int, completed_work: int, max_workers: int
) -> int:
    remaining = max(0, total_work - completed_work)
    return min(max_workers, remaining)


def _stream_missing(
    config: DownloadConfig,
    client: DownloaderClient,
    console: Console,
    work: Iterable[WorkItem],
    *,
    total_work: int | None = None,
    error_console: Console | None = None,
    estimated_bytes_by_item: dict[WorkItem, int] | None = None,
    estimated_cost_cents_by_item: dict[WorkItem, int] | None = None,
    estimated_bytes_by_key: dict[WorkKey, int] | None = None,
    estimated_cost_cents_by_key: dict[WorkKey, int] | None = None,
    work_counts_by_key: dict[WorkKey, int] | None = None,
    expected_weekdays_by_key: dict[WorkKey, int] | None = None,
    fsync_tracker: _DirectoryFsyncTracker | None = None,
) -> DownloadResult:
    placed = 0
    no_data = 0
    failed = 0
    completed_work = 0
    landed_estimated_cents = 0
    landed_estimated_bytes = 0
    failure_console = error_console or Console(stderr=True)
    estimated_bytes = estimated_bytes_by_item or {}
    estimated_costs = estimated_cost_cents_by_item or {}
    total_work = total_work if total_work is not None else _len_or_none(work)
    if total_work is None:
        msg = "streaming work requires total_work for non-sized iterables"
        raise RuntimeError(msg)
    work_iter = iter(work)

    quiet = bool(getattr(console, "quiet", False))
    progress = _build_progress(quiet, console)
    in_flight_tracker = _InFlightRegistry[_ActiveDownload]()
    counts = _OutcomeCounts()
    live_state = _LiveProgressState()

    pool = ThreadPoolExecutor(max_workers=config.max_workers)
    futures: list[Future[_StreamResult]] = []
    active_futures: set[Future[_StreamResult]] = set()
    future_items: dict[Future[_StreamResult], WorkItem] = {}
    future_costs: dict[Future[_StreamResult], int] = {}
    future_bytes: dict[Future[_StreamResult], int | None] = {}
    future_large_streams: dict[Future[_StreamResult], bool] = {}
    work_days_by_key: dict[WorkKey, set[date]] = {}
    no_data_days_by_key: dict[WorkKey, set[date]] = {}
    no_data_counts_by_key: dict[WorkKey, int] = {}
    if work_counts_by_key is None or expected_weekdays_by_key is None:
        if not isinstance(work, Sized):
            msg = "no-data accounting requires summary maps for non-sized iterables"
            raise RuntimeError(msg)
        for item in work:
            key = (item.symbol, item.schema)
            work_days_by_key.setdefault(key, set()).add(item.day)
        work_iter = iter(work)
    allocation_indexes_by_key: dict[WorkKey, int] = {}
    pending_work: _PendingWork | None = None
    work_exhausted = False
    committed_estimated_cents = 0
    outstanding_estimated_cents = 0
    active_large_streams = 0
    fatal = False
    interrupted = False
    live: Live | None = None

    def _planning_cap_error(next_cost_cents: int) -> FatalConfigError:
        msg = (
            "in-flight planned cost exceeded planning cap: "
            f"committed={_money(committed_estimated_cents)}, "
            f"outstanding={_money(outstanding_estimated_cents)}, "
            f"next={_money(next_cost_cents)}, "
            f"planning_cap={_money(config.max_cost_cents or 0)}"
        )
        return FatalConfigError(msg)

    def _next_pending_work() -> _PendingWork | None:
        nonlocal pending_work, work_exhausted
        if pending_work is not None:
            return pending_work
        if work_exhausted:
            return None
        try:
            item = next(work_iter)
        except StopIteration:
            work_exhausted = True
            return None
        key = (item.symbol, item.schema)
        item_cost_cents = _allocated_estimate_for_item(
            item,
            estimated_costs,
            estimated_cost_cents_by_key,
            work_counts_by_key,
            allocation_indexes_by_key,
        )
        item_bytes = _allocated_estimate_for_item(
            item,
            estimated_bytes,
            estimated_bytes_by_key,
            work_counts_by_key,
            allocation_indexes_by_key,
        )
        pending_work = _PendingWork(item, item_bytes, item_cost_cents)
        allocation_indexes_by_key[key] = allocation_indexes_by_key.get(key, 0) + 1
        return pending_work

    def _submit_admissible() -> None:
        nonlocal active_large_streams, fatal, outstanding_estimated_cents, pending_work
        try:
            while len(active_futures) < config.max_workers:
                pending = _next_pending_work()
                if pending is None:
                    return
                item = pending.item
                item_cost_cents = pending.estimated_cost_cents
                is_large_stream = _is_large_stream(pending.estimated_bytes)
                if is_large_stream and active_large_streams >= _MAX_LARGE_STREAMS:
                    return
                if (
                    config.max_cost_cents is not None
                    and committed_estimated_cents
                    + outstanding_estimated_cents
                    + item_cost_cents
                    > config.max_cost_cents
                ):
                    if active_futures:
                        return
                    fatal = True
                    raise _planning_cap_error(item_cost_cents)

                future = pool.submit(
                    _stream_one,
                    config,
                    client,
                    item,
                    pending.estimated_bytes,
                    fsync_tracker,
                    in_flight_tracker,
                )
                futures.append(future)
                active_futures.add(future)
                future_items[future] = item
                future_costs[future] = item_cost_cents
                future_bytes[future] = pending.estimated_bytes
                future_large_streams[future] = is_large_stream
                outstanding_estimated_cents += item_cost_cents
                if is_large_stream:
                    active_large_streams += 1
                pending_work = None
        finally:
            live_state.set_active_workers(len(active_futures))

    if not quiet:
        live = Live(
            _ProgressPanelRenderable(
                progress,
                in_flight_tracker,
                counts,
                config.max_workers,
                live_state,
            ),
            console=console,
            refresh_per_second=8,
            transient=False,
        )
    try:
        if live is not None:
            live.start()
        else:
            progress.start()
        task = progress.add_task("Downloading", total=total_work)
        _submit_admissible()
        while active_futures:
            done, _pending = wait(active_futures, return_when=FIRST_COMPLETED)
            for future in done:
                active_futures.remove(future)
                if future_large_streams.pop(future, False):
                    active_large_streams -= 1
                live_state.set_active_workers(len(active_futures))
                item = future_items[future]
                item_cost_cents = future_costs[future]
                outstanding_estimated_cents -= item_cost_cents
                try:
                    outcome, label = future.result()
                except FatalError:
                    fatal = True
                    _cancel_futures(futures)
                    raise
                except Exception:
                    fatal = True
                    _cancel_futures(futures)
                    raise
                counts.record(outcome)
                if outcome == "placed":
                    placed += 1
                elif outcome == "cached":
                    pass
                elif outcome == "no_data":
                    no_data += 1
                    key = (item.symbol, item.schema)
                    no_data_counts_by_key[key] = no_data_counts_by_key.get(key, 0) + 1
                    if work_counts_by_key is None or expected_weekdays_by_key is None:
                        no_data_days_by_key.setdefault(key, set()).add(item.day)
                    if not progress.console.quiet:
                        progress.console.print(
                            f"  [yellow]⚠[/yellow] no data {label}"
                        )
                else:
                    failed += 1
                    failure_console.print(f"  [red]✗[/red] {label}")
                if outcome != "cached":
                    committed_estimated_cents += item_cost_cents
                if outcome in ("placed", "no_data"):
                    landed_estimated_bytes += future_bytes.get(future) or 0
                    landed_estimated_cents += item_cost_cents
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
                _submit_admissible()
    except (KeyboardInterrupt, ShutdownRequestedError) as exc:
        interrupted = True
        _cancel_futures(futures)
        if isinstance(exc, ShutdownRequestedError):
            raise
        raise InterruptedDownloadError("download interrupted by user") from exc
    finally:
        if live is not None:
            live.stop()
        else:
            progress.stop()
        if fatal or interrupted:
            # Wait on user interrupts before SystemExit unwinds so CPython does
            # not have to join executor threads during interpreter shutdown.
            pool.shutdown(wait=interrupted, cancel_futures=True)
        else:
            pool.shutdown(wait=True, cancel_futures=False)

    if completed_work != total_work:
        if interrupted or fatal:
            raise InterruptedDownloadError("download did not complete")
        msg = (
            f"download work accounting failed: completed={completed_work}, "
            f"work={total_work}"
        )
        raise RuntimeError(msg)

    cached = _total_partitions(config) - placed - no_data - failed
    if work_counts_by_key is not None and expected_weekdays_by_key is not None:
        _raise_on_suspicious_all_no_data_counts(
            work_counts_by_key,
            no_data_counts_by_key,
            expected_weekdays_by_key,
            threshold_weekdays=config.suspicious_no_data_weekdays,
        )
    else:
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


def _allocated_estimate_for_item(
    item: WorkItem,
    estimated_by_item: dict[WorkItem, int],
    estimated_by_key: dict[WorkKey, int] | None,
    work_counts_by_key: dict[WorkKey, int] | None,
    allocation_indexes_by_key: dict[WorkKey, int],
) -> int:
    if estimated_by_key is None:
        return estimated_by_item.get(item, 0)
    if work_counts_by_key is None:
        msg = "per-key estimate allocation requires work counts"
        raise RuntimeError(msg)
    key = (item.symbol, item.schema)
    total = estimated_by_key.get(key, 0)
    count = work_counts_by_key.get(key)
    if count is None or count <= 0:
        msg = f"missing work count for estimate key: {key}"
        raise RuntimeError(msg)
    index = allocation_indexes_by_key.get(key, 0)
    base, remainder = divmod(total, count)
    return base + (1 if index < remainder else 0)


def _is_large_stream(estimated_bytes: int | None) -> bool:
    return estimated_bytes is not None and estimated_bytes >= _LARGE_STREAM_BYTES


def _raise_on_suspicious_all_no_data_counts(
    work_counts_by_key: dict[WorkKey, int],
    no_data_counts_by_key: dict[WorkKey, int],
    expected_weekdays_by_key: dict[WorkKey, int],
    *,
    threshold_weekdays: int,
) -> None:
    for (symbol, schema), work_count in work_counts_by_key.items():
        if no_data_counts_by_key.get((symbol, schema), 0) != work_count:
            continue
        expected_weekdays = expected_weekdays_by_key.get((symbol, schema), 0)
        if expected_weekdays < threshold_weekdays:
            continue
        msg = (
            f"all {work_count} requested partitions returned no data for "
            f"{symbol}/{schema}, including {expected_weekdays} weekdays on or "
            "after the configured first UTC data day; check the parent symbol "
            "before treating these files as canonical coverage"
        )
        raise FatalConfigError(msg)


def _len_or_none(items: Iterable[WorkItem]) -> int | None:
    if isinstance(items, Sized):
        return len(items)
    return None


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
    in_flight: _InFlightRegistry[_ActiveDownload] | None = None,
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
    token = (
        in_flight.add(_ActiveDownload(item, tmp, estimated_billable_bytes))
        if in_flight is not None
        else None
    )
    try:
        try:
            tmp.unlink(missing_ok=True)
            client.stream_to_file(query, tmp)
            if config.validate_on_write:
                validate_dbn_metadata(
                    query,
                    tmp,
                    deep=config.deep_validate,
                    strict=config.strict_validate,
                    max_decompressed_bytes=_deep_validate_cap(estimated_billable_bytes),
                )
            digest = _place_tmp(
                tmp,
                dest,
                fsync_tracker,
                fsync_writes=config.fsync_writes,
                compute_digest=config.write_sidecars,
            )
            if config.write_sidecars:
                _write_sha256_sidecar(
                    dest,
                    digest,
                    fsync_tracker,
                    fsync_writes=config.fsync_writes,
                )
            return ("placed", label)
        except DegradedError:
            try:
                client.write_empty_file(query, tmp)
                if config.validate_on_write:
                    validate_dbn_metadata(
                        query,
                        tmp,
                        deep=config.deep_validate,
                        strict=config.strict_validate,
                        max_decompressed_bytes=_deep_validate_cap(
                            estimated_billable_bytes,
                        ),
                    )
                digest = _place_tmp(
                    tmp,
                    dest,
                    fsync_tracker,
                    fsync_writes=config.fsync_writes,
                    compute_digest=config.write_sidecars,
                )
                if config.write_sidecars:
                    _write_sha256_sidecar(
                        dest,
                        digest,
                        fsync_tracker,
                        fsync_writes=config.fsync_writes,
                    )
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
    finally:
        if in_flight is not None and token is not None:
            in_flight.remove(token)


__all__ = [
    "_deep_validate_cap",
    "_display_active_workers",
    "_stream_missing",
    "_stream_one",
]
