"""Tests for S3 sync progress rendering helpers."""
# pyright: reportPrivateUsage=false

from __future__ import annotations

import io
from pathlib import Path

from rich.console import Console

from databento_stream_downloader._runner.concurrency import _InFlightRegistry
from databento_stream_downloader._sync.stream import (
    _IN_FLIGHT_DISPLAY_LIMIT,
    _build_progress,
    _render_sync_panel,
    _SyncPanelRenderable,
    _transfer_progress_label,
    _TransferProgress,
)
from databento_stream_downloader._sync.types import (
    SyncConfig,
    SyncDirection,
    SyncItem,
    _SyncOutcomeCounts,
)
from databento_stream_downloader.config import RunMode


def _sync_config(tmp_path: Path) -> SyncConfig:
    return SyncConfig(
        direction=SyncDirection.PUSH,
        data_dir=tmp_path,
        bucket="bucket",
        mode=RunMode.EXECUTE,
        max_workers=4,
        yes=True,
    )


def test_transfer_progress_label_reports_total_when_unmeasured(tmp_path: Path) -> None:
    item = SyncItem(
        local_path=tmp_path / "file.dbn.zst",
        s3_key="raw/file.dbn.zst",
        size_bytes=1024,
        op="transfer",
    )

    assert _transfer_progress_label(item, None) == "1.0 KiB total"


def test_transfer_progress_label_reports_speed(tmp_path: Path) -> None:
    item = SyncItem(
        local_path=tmp_path / "file.dbn.zst",
        s3_key="raw/file.dbn.zst",
        size_bytes=1024,
        op="transfer",
    )
    progress = _TransferProgress(total_bytes=1024)
    progress.samples = [(1.0, 0), (3.0, 1024)]
    progress.transferred_bytes = 1024

    assert (
        _transfer_progress_label(item, progress)
        == "1.0 KiB / 1.0 KiB (100.0%); 512 B/s"
    )
    assert progress.total_transferred() == 1024


def test_sync_panel_renders_active_transfers_and_overflow(tmp_path: Path) -> None:
    tracker: _InFlightRegistry[SyncItem] = _InFlightRegistry()
    progress_by_key: dict[str, _TransferProgress] = {}
    for index in range(_IN_FLIGHT_DISPLAY_LIMIT + 1):
        item = SyncItem(
            local_path=tmp_path / f"{index}.dbn.zst",
            s3_key=f"raw/{index}.dbn.zst",
            size_bytes=1024,
            op="transfer",
        )
        tracker.add(item)
        transfer_progress = _TransferProgress(total_bytes=1024)
        transfer_progress.record(128)
        progress_by_key[item.s3_key] = transfer_progress

    output_buffer = io.StringIO()
    console = Console(file=output_buffer, force_terminal=False)
    progress = _build_progress(quiet=True, console=console)
    progress.add_task("Pushing", total=1024)
    counts = _SyncOutcomeCounts(transferred=1, skipped=2, deleted=3, failed=4)
    config = _sync_config(tmp_path)

    panel = _render_sync_panel(progress, tracker, counts, config, progress_by_key)
    renderable = _SyncPanelRenderable(
        progress=progress,
        tracker=tracker,
        counts=counts,
        config=config,
        progress_by_key=progress_by_key,
    )

    console.print(panel)
    console.print(renderable.__rich__())
    output = output_buffer.getvalue()
    assert "... and 1 more" in output
    assert "Workers 21 / 4 active" in output
    assert "1 uploaded" in output
