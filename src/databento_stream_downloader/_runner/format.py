"""Runner console formatting helpers."""

from __future__ import annotations

from rich.console import Console
from rich.table import Table

from databento_stream_downloader.models import CostEstimate


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


__all__ = ["_bytes", "_money", "_print_costs"]
