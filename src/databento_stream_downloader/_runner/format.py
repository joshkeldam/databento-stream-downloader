"""Runner console formatting helpers."""

from __future__ import annotations

from pathlib import Path

from rich import box
from rich.console import Console, Group
from rich.panel import Panel
from rich.table import Table
from rich.text import Text

from databento_stream_downloader.models import CostEstimate


def _print_costs(
    console: Console,
    estimates: list[CostEstimate],
    total_cents: int,
    max_cost_cents: int | None,
    *,
    archive: Path | None = None,
) -> None:
    console.print(_render_plan_panel(estimates, total_cents, max_cost_cents, archive))


def _render_plan_panel(
    estimates: list[CostEstimate],
    total_cents: int,
    max_cost_cents: int | None,
    archive: Path | None,
) -> Panel:
    ranked = sorted(
        estimates,
        key=lambda item: (-item.cost_cents, -item.size_bytes, item.symbol, item.schema),
    )
    total_bytes = sum(item.size_bytes for item in ranked)

    table = Table(
        box=box.SIMPLE_HEAD,
        show_edge=False,
        pad_edge=False,
        expand=False,
        header_style="dim",
    )
    table.add_column("Symbol", style="cyan", no_wrap=True)
    table.add_column("Schema", style="white", no_wrap=True)
    table.add_column("Billable size", justify="right", style="white", no_wrap=True)
    table.add_column("Cost", justify="right", style="white", no_wrap=True)
    for item in ranked:
        table.add_row(
            item.symbol,
            item.schema,
            _bytes(item.size_bytes),
            _money(item.cost_cents),
        )
    table.add_section()
    table.add_row(
        Text(""),
        Text("Total", style="bold"),
        Text(_bytes(total_bytes), style="bold"),
        Text(_money(total_cents), style="bold"),
    )

    summary = Table.grid(padding=(0, 3))
    summary.add_column(style="dim", no_wrap=True)
    summary.add_column(no_wrap=False)
    summary.add_row("Buckets", f"{len(ranked):,}")
    summary.add_row("Size basis", "uncompressed billable bytes")
    if max_cost_cents is not None:
        summary.add_row("Planning cap", _money(max_cost_cents))
        headroom_cents = max(0, max_cost_cents - total_cents)
        if max_cost_cents > 0:
            pct = headroom_cents * 100 // max_cost_cents
            headroom = Text.assemble(
                (_money(headroom_cents), "green" if headroom_cents else "yellow"),
                (f"  ({pct}%)", "dim"),
            )
        else:
            headroom = Text(_money(headroom_cents), style="green")
        summary.add_row("Headroom", headroom)
    if archive is not None:
        summary.add_row("Archive", str(archive))

    return Panel(
        Group(table, Text(""), summary),
        title="[bold]Download plan[/bold]",
        border_style="cyan",
        padding=(1, 2),
    )


def _money(cents: int) -> str:
    if cents < 0:
        msg = f"cents must be non-negative, got {cents}"
        raise ValueError(msg)
    return f"${cents // 100:,}.{cents % 100:02d}"


def _bytes(size: int) -> str:
    if size < 0:
        msg = f"size must be non-negative, got {size}"
        raise ValueError(msg)
    if size >= 1_099_511_627_776:
        return f"{size / 1_099_511_627_776:.2f} TiB"
    if size >= 1_073_741_824:
        return f"{size / 1_073_741_824:.2f} GiB"
    if size >= 1_048_576:
        return f"{size / 1_048_576:.1f} MiB"
    if size >= 1024:
        return f"{size / 1024:.1f} KiB"
    return f"{size} B"


__all__ = ["_bytes", "_money", "_print_costs", "_render_plan_panel"]
