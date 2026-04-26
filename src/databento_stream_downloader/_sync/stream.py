"""Streaming orchestration for the sync tool with a Rich Live panel."""

from __future__ import annotations

from concurrent.futures import Future, ThreadPoolExecutor, as_completed

import structlog
from rich.console import Console, Group
from rich.live import Live
from rich.panel import Panel
from rich.progress import (
    BarColumn,
    DownloadColumn,
    Progress,
    SpinnerColumn,
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
_IN_FLIGHT_DISPLAY_LIMIT = 8


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
) -> Panel:
    is_push = config.direction is SyncDirection.PUSH
    transferred, skipped, deleted, failed = counts.snapshot()
    in_flight = tracker.snapshot()
    active_count = len(in_flight)

    activity = Table.grid(padding=(0, 1))
    activity.add_column(no_wrap=True)
    if in_flight:
        for item in in_flight[:_IN_FLIGHT_DISPLAY_LIMIT]:
            activity.add_row(
                Text.assemble(
                    ("▸ ", "dim"),
                    (item.s3_key, "cyan"),
                ),
            )
        remaining = active_count - _IN_FLIGHT_DISPLAY_LIMIT
        if remaining > 0:
            activity.add_row(Text(f"… and {remaining} more", style="dim italic"))
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

    title = (
        "Pushing to S3"
        if is_push
        else "Pulling from S3"
    )
    return Panel(
        Group(progress, Text(""), workers_line, activity, Text(""), counts_line),
        title=f"[bold]{title}[/bold]",
        border_style="cyan",
        padding=(1, 2),
    )


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
    quiet = bool(getattr(console, "quiet", False))
    progress = _build_progress(quiet, console)

    is_push = config.direction is SyncDirection.PUSH

    def _submit_transfer(pool: ThreadPoolExecutor, item: SyncItem) -> Future[
        tuple[SyncOutcome, str, int]
    ]:
        if is_push:
            return pool.submit(upload_one, client, item, in_flight)
        return pool.submit(
            download_one,
            client,
            item,
            in_flight,
            verify_sha256=config.verify_sha256,
            fsync_writes=config.fsync_writes,
        )

    def _submit_delete(pool: ThreadPoolExecutor, item: SyncItem) -> Future[
        tuple[SyncOutcome, str, int]
    ]:
        return pool.submit(
            delete_one,
            client,
            item,
            in_flight,
            delete_remote=is_push,
        )

    pool = ThreadPoolExecutor(max_workers=config.max_workers)
    futures: list[Future[tuple[SyncOutcome, str, int]]] = []
    fatal = False
    interrupted = False

    live: Live | None = None
    if not quiet:
        live = Live(
            _render_sync_panel(progress, in_flight, counts, config),
            console=console,
            refresh_per_second=8,
            transient=False,
        )
    try:
        if live is not None:
            live.start()
        else:
            progress.start()
        task = progress.add_task(
            "Pushing" if is_push else "Pulling",
            total=plan.total_transfer_bytes,
        )

        futures = [_submit_transfer(pool, item) for item in plan.transfers]
        futures.extend(_submit_delete(pool, item) for item in plan.deletes)

        for future in as_completed(futures):
            try:
                outcome, label, bytes_moved = future.result()
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
            counts.record(outcome)
            if outcome == "transferred" and bytes_moved:
                progress.advance(task, advance=bytes_moved)
            if outcome == "failed":
                failure_console.print(f"  [red]✗[/red] {label}")
            LOGGER.info(
                "sync_item",
                outcome=outcome,
                label=label,
                bytes=bytes_moved,
            )
            if live is not None:
                live.update(_render_sync_panel(progress, in_flight, counts, config))
    except (KeyboardInterrupt, ShutdownRequestedError) as exc:
        interrupted = True
        _cancel_futures(futures)
        pool.shutdown(wait=False, cancel_futures=True)
        if isinstance(exc, ShutdownRequestedError):
            raise
        raise InterruptedDownloadError("sync interrupted by user") from exc
    finally:
        if live is not None:
            live.stop()
        else:
            progress.stop()
        if fatal or interrupted:
            pool.shutdown(wait=False, cancel_futures=True)
        else:
            pool.shutdown(wait=True, cancel_futures=False)

    return counts.snapshot()


__all__ = ["_render_sync_panel", "_sync_run"]
