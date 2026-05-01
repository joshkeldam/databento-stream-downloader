"""Streaming orchestration for the sync tool with a Rich Live panel."""

from __future__ import annotations

import time
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait
from dataclasses import dataclass, field
from threading import Lock

import structlog
from rich.console import Console, Group
from rich.live import Live
from rich.panel import Panel
from rich.progress import (
    BarColumn,
    DownloadColumn,
    Progress,
    SpinnerColumn,
    TaskID,
    TaskProgressColumn,
    TextColumn,
    TimeElapsedColumn,
    TimeRemainingColumn,
    TransferSpeedColumn,
)
from rich.table import Table
from rich.text import Text

from databento_stream_downloader._runner.concurrency import (
    _cancel_futures,
    _InFlightRegistry,
)
from databento_stream_downloader._runner.format import _bytes
from databento_stream_downloader._sync.transfer import (
    delete_one,
    download_one,
    upload_one,
)
from databento_stream_downloader._sync.types import (
    SyncConfig,
    SyncDirection,
    SyncItem,
    SyncOutcome,
    SyncPlan,
    _SyncOutcomeCounts,
)
from databento_stream_downloader.errors import (
    FatalError,
    InterruptedDownloadError,
    ShutdownRequestedError,
)
from databento_stream_downloader.s3_client import S3Client

LOGGER = structlog.get_logger(__name__)
_IN_FLIGHT_DISPLAY_LIMIT = 20
_SPEED_WINDOW_SECONDS = 20.0


@dataclass(slots=True)
class _TransferProgress:
    total_bytes: int
    transferred_bytes: int = 0
    samples: list[tuple[float, int]] = field(default_factory=list)
    _lock: Lock = field(default_factory=Lock)

    def record(self, bytes_delta: int) -> None:
        now = time.monotonic()
        with self._lock:
            self.transferred_bytes += max(0, int(bytes_delta))
            if not self.samples:
                self.samples.append((now, 0))
            self.samples.append((now, self.transferred_bytes))
            cutoff = now - _SPEED_WINDOW_SECONDS
            while len(self.samples) > 2 and self.samples[1][0] < cutoff:
                self.samples.pop(0)

    def snapshot(self) -> tuple[int, int | None]:
        with self._lock:
            transferred = self.transferred_bytes
            samples = tuple(self.samples)
        if len(samples) < 2:
            return transferred, None
        first_time, first_bytes = samples[0]
        last_time, last_bytes = samples[-1]
        elapsed = last_time - first_time
        if elapsed <= 0:
            return transferred, None
        return transferred, max(0, int((last_bytes - first_bytes) / elapsed))

    def total_transferred(self) -> int:
        with self._lock:
            return self.transferred_bytes


@dataclass(frozen=True, slots=True)
class _SyncPanelRenderable:
    progress: Progress
    tracker: _InFlightRegistry[SyncItem]
    counts: _SyncOutcomeCounts
    config: SyncConfig
    progress_by_key: dict[str, _TransferProgress]

    def __rich__(self) -> Panel:
        return _render_sync_panel(
            self.progress,
            self.tracker,
            self.counts,
            self.config,
            self.progress_by_key,
        )


def _build_progress(quiet: bool, console: Console) -> Progress:
    return Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(bar_width=None),
        DownloadColumn(),
        TaskProgressColumn(),
        TextColumn("•"),
        TransferSpeedColumn(),
        TextColumn("•"),
        TimeElapsedColumn(),
        TextColumn("•"),
        TimeRemainingColumn(compact=True, elapsed_when_finished=True),
        console=console,
        transient=False,
        expand=True,
        disable=quiet,
    )


def _render_sync_panel(
    progress: Progress,
    tracker: _InFlightRegistry[SyncItem],
    counts: _SyncOutcomeCounts,
    config: SyncConfig,
    progress_by_key: dict[str, _TransferProgress] | None = None,
) -> Panel:
    is_push = config.direction is SyncDirection.PUSH
    transferred, skipped, deleted, failed = counts.snapshot()
    in_flight = tracker.snapshot()
    active_count = len(in_flight)

    activity = Table.grid(padding=(0, 1))
    activity.add_column(no_wrap=True)
    if in_flight:
        for item in in_flight[:_IN_FLIGHT_DISPLAY_LIMIT]:
            progress_label = _transfer_progress_label(
                item,
                progress_by_key.get(item.s3_key) if progress_by_key else None,
            )
            activity.add_row(
                Text.assemble(
                    ("▸ ", "dim"),
                    (item.s3_key, "cyan"),
                    "  ",
                    (progress_label, "magenta"),
                ),
            )
        remaining = active_count - _IN_FLIGHT_DISPLAY_LIMIT
        if remaining > 0:
            activity.add_row(Text(f"... and {remaining} more", style="dim italic"))
    else:
        activity.add_row(Text("(idle)", style="dim italic"))

    transfer_glyph = "↑" if is_push else "↓"
    transfer_word = "uploaded" if is_push else "downloaded"
    counts_line = Text.assemble(
        (f"{transfer_glyph} ", "green"),
        (f"{transferred:,} {transfer_word}", "green"),
        ("    ⊘ ", "yellow"),
        (f"{skipped:,} skipped", "yellow"),
        ("    ⌫ ", "magenta"),
        (f"{deleted:,} deleted", "magenta"),
        ("    ✗ ", "red bold" if failed else "red"),
        (f"{failed:,} failed", "red bold" if failed else "red"),
    )
    workers_line = Text.assemble(
        ("Workers ", "dim"),
        (f"{active_count}", "bold"),
        (f" / {config.max_workers} active", "dim"),
    )

    title = "Pushing to S3" if is_push else "Pulling from S3"
    return Panel(
        Group(progress, Text(""), workers_line, activity, Text(""), counts_line),
        title=f"[bold]{title}[/bold]",
        border_style="cyan",
        padding=(1, 2),
    )


def _transfer_progress_label(
    item: SyncItem,
    transfer_progress: _TransferProgress | None,
) -> str:
    if transfer_progress is None:
        return f"{_bytes(item.size_bytes)} total"
    transferred, bytes_per_second = transfer_progress.snapshot()
    total = max(item.size_bytes, 0)
    percent = (transferred / total * 100) if total else 100.0
    label = f"{_bytes(transferred)} / {_bytes(total)} ({percent:.1f}%)"
    if bytes_per_second is None:
        return f"{label}; measuring speed"
    return f"{label}; {_bytes(bytes_per_second)}/s"


def _sync_run(
    config: SyncConfig,
    client: S3Client,
    console: Console,
    plan: SyncPlan,
    *,
    error_console: Console | None = None,
) -> tuple[int, int, int, int]:
    """Run a sync plan to completion.

    Returns ``(transferred, skipped, deleted, failed)``.
    """
    failure_console = error_console or Console(stderr=True)
    counts = _SyncOutcomeCounts()
    in_flight: _InFlightRegistry[SyncItem] = _InFlightRegistry()
    progress_by_key: dict[str, _TransferProgress] = {}
    quiet = bool(getattr(console, "quiet", False))
    progress = _build_progress(quiet, console)
    progress_lock = Lock()

    is_push = config.direction is SyncDirection.PUSH

    def _submit_transfer(
        pool: ThreadPoolExecutor, item: SyncItem
    ) -> Future[tuple[SyncOutcome, str, int]]:
        transfer_progress = progress_by_key.setdefault(
            item.s3_key,
            _TransferProgress(total_bytes=item.size_bytes),
        )

        def _record_progress(bytes_delta: int) -> None:
            transfer_progress.record(bytes_delta)

        if is_push:
            return pool.submit(
                upload_one,
                client,
                item,
                in_flight,
                _record_progress,
            )
        return pool.submit(
            download_one,
            client,
            item,
            in_flight,
            verify_sha256=config.verify_sha256,
            fsync_writes=config.fsync_writes,
            progress_callback=_record_progress,
        )

    def _submit_delete(
        pool: ThreadPoolExecutor, item: SyncItem
    ) -> Future[tuple[SyncOutcome, str, int]]:
        return pool.submit(
            delete_one,
            client,
            item,
            in_flight,
            delete_remote=is_push,
        )

    pool = ThreadPoolExecutor(max_workers=config.max_workers)
    futures: list[Future[tuple[SyncOutcome, str, int]]] = []
    active_futures: set[Future[tuple[SyncOutcome, str, int]]] = set()
    future_items: dict[Future[tuple[SyncOutcome, str, int]], SyncItem] = {}
    fatal = False
    interrupted = False
    task = progress.add_task(
        "Pushing" if is_push else "Pulling",
        total=plan.total_transfer_bytes,
    )

    live: Live | None = None
    if not quiet:
        live = Live(
            _SyncPanelRenderable(
                progress,
                in_flight,
                counts,
                config,
                progress_by_key,
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

        for item in plan.transfers:
            future = _submit_transfer(pool, item)
            futures.append(future)
            active_futures.add(future)
            future_items[future] = item
        for item in plan.deletes:
            future = _submit_delete(pool, item)
            futures.append(future)
            active_futures.add(future)
            future_items[future] = item

        while active_futures:
            _sync_progress_bar(progress, task, progress_by_key, progress_lock)
            done, _pending = wait(
                active_futures,
                timeout=0.25,
                return_when=FIRST_COMPLETED,
            )
            if not done:
                continue
            for future in done:
                active_futures.remove(future)
                try:
                    outcome, label, bytes_moved = future.result()
                except FatalError:
                    fatal = True
                    _cancel_futures(futures)
                    raise
                except Exception:
                    fatal = True
                    _cancel_futures(futures)
                    raise
                counts.record(outcome)
                item = future_items[future]
                if outcome == "transferred" and bytes_moved:
                    transfer_progress = progress_by_key.get(item.s3_key)
                    transferred = (
                        transfer_progress.snapshot()[0]
                        if transfer_progress is not None
                        else 0
                    )
                    remaining = max(0, bytes_moved - transferred)
                    if remaining:
                        if transfer_progress is not None:
                            transfer_progress.record(remaining)
                        _sync_progress_bar(
                            progress,
                            task,
                            progress_by_key,
                            progress_lock,
                        )
                if outcome == "failed":
                    failure_console.print(f"  [red]✗[/red] {label}")
                LOGGER.info(
                    "sync_item",
                    outcome=outcome,
                    label=label,
                    bytes=bytes_moved,
                )
    except (KeyboardInterrupt, ShutdownRequestedError) as exc:
        interrupted = True
        _cancel_futures(futures)
        if isinstance(exc, ShutdownRequestedError):
            raise
        raise InterruptedDownloadError("sync interrupted by user") from exc
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

    return counts.snapshot()


def _sync_progress_bar(
    progress: Progress,
    task: TaskID | None,
    progress_by_key: dict[str, _TransferProgress],
    progress_lock: Lock,
) -> None:
    if task is None:
        return
    transferred = sum(item.total_transferred() for item in progress_by_key.values())
    with progress_lock:
        current = progress.tasks[task].completed
        advance = max(0, transferred - int(current))
        if advance:
            progress.advance(task, advance=advance)


__all__ = ["_render_sync_panel", "_sync_run"]
