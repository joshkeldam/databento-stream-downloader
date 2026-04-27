"""Plan panel rendering for the sync tool."""

from __future__ import annotations

from rich import box
from rich.console import Console, Group
from rich.panel import Panel
from rich.table import Table
from rich.text import Text

from databento_stream_downloader._runner.format import _bytes
from databento_stream_downloader._sync.types import SyncConfig, SyncDirection, SyncPlan


def _print_sync_plan(
    console: Console,
    config: SyncConfig,
    plan: SyncPlan,
) -> None:
    console.print(_render_sync_plan_panel(config, plan))


def _render_sync_plan_panel(config: SyncConfig, plan: SyncPlan) -> Panel:
    is_push = config.direction is SyncDirection.PUSH
    transfer_label = "To upload" if is_push else "To download"
    extraneous_label = "Remote-only" if is_push else "Local-only"
    delete_label = "S3 deletes" if is_push else "Local deletes"

    summary = Table.grid(padding=(0, 3))
    summary.add_column(style="dim", no_wrap=True)
    summary.add_column(no_wrap=False)
    summary.add_row("Bucket", f"s3://{config.bucket}")
    if config.prefix:
        summary.add_row("Prefix", config.prefix)
    if config.region:
        summary.add_row("Region", config.region)
    direction_text = "↑ push (local → s3)" if is_push else "↓ pull (s3 → local)"
    summary.add_row("Direction", direction_text)
    summary.add_row("Planning", config.planning_mode.value)

    counts = Table.grid(padding=(0, 3))
    counts.add_column(style="dim", no_wrap=True)
    counts.add_column(no_wrap=False)
    counts.add_row(
        transfer_label,
        Text.assemble(
            (f"{len(plan.transfers):,} files", "bold"),
            "   ",
            (_bytes(plan.total_transfer_bytes), "bold cyan"),
        ),
    )
    delete_text: Text
    if plan.deletes:
        delete_text = Text(f"{len(plan.deletes):,} files", style="bold red")
    else:
        delete_text = Text("0 files")
    counts.add_row(delete_label, delete_text)
    counts.add_row("Already synced", f"{plan.skipped:,} files")
    if plan.extraneous:
        hint = "" if config.delete else "  (use --delete to remove)"
        counts.add_row(
            extraneous_label,
            Text.assemble((f"{len(plan.extraneous):,} files", "yellow"), (hint, "dim")),
        )
    elif not is_push and not plan.transfers:
        counts.add_row(extraneous_label, "0 files")

    largest = max(plan.transfers, key=lambda i: i.size_bytes, default=None)
    largest_row: Text | None = None
    if largest is not None:
        largest_row = Text.assemble(
            (largest.s3_key, "cyan"),
            "  ",
            (_bytes(largest.size_bytes), "white"),
        )

    body = [summary, Text(""), counts]
    if largest_row is not None:
        body.extend([Text(""), Text("Largest pending:", style="dim"), largest_row])

    title = "Sync plan: push" if is_push else "Sync plan: pull"
    return Panel(
        Group(*body),
        title=f"[bold]{title}[/bold]",
        border_style="cyan",
        padding=(1, 2),
        box=box.ROUNDED,
    )


__all__ = ["_print_sync_plan", "_render_sync_plan_panel"]
