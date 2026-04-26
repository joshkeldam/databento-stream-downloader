"""Cost estimation, cap checks, and estimate allocation."""

from __future__ import annotations

import shutil
from concurrent.futures import Future, ThreadPoolExecutor, as_completed
from datetime import date, timedelta
from decimal import Decimal
from pathlib import Path

import structlog
from rich.console import Console

from databento_stream_downloader._runner.concurrency import _cancel_futures
from databento_stream_downloader._runner.format import _bytes, _money
from databento_stream_downloader._runner.fsio import _nearest_existing_parent
from databento_stream_downloader._runner.types import DownloaderClient, WorkItem
from databento_stream_downloader.config import DownloadConfig
from databento_stream_downloader.constants import DATASET
from databento_stream_downloader.models import CostEstimate, CostQuery
from databento_stream_downloader.pricing import decimal_dollars_to_cents

_BUCKET_COST_WARN_FRACTION = Decimal("0.25")
# Databento's metadata API rate-limits concurrent requests; 40 is the
# empirical ceiling for the cost-estimation phase before 429s appear.
_MAX_COST_WORKERS = 40
LOGGER = structlog.get_logger(__name__)


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
        warn_threshold_cents = _bucket_cost_warn_threshold_cents(config.max_cost_cents)
        if estimate.cost_cents > warn_threshold_cents:
            console.print(
                "[yellow]Warning:[/yellow] estimated bucket cost exceeds 25% "
                f"of global planning cap: bucket={label}, "
                f"estimate={_money(estimate.cost_cents)}, "
                f"global_planning_cap={_money(config.max_cost_cents)}"
            )


def _warn_in_flight_planning_exposure(
    config: DownloadConfig,
    work: list[WorkItem],
    estimated_cost_cents_by_item: dict[WorkItem, int],
    console: Console,
) -> None:
    if config.max_cost_cents is None:
        return
    effective_workers = min(config.max_workers, len(work))
    if effective_workers <= 1:
        return
    exposure_cents = sum(
        sorted(estimated_cost_cents_by_item.values(), reverse=True)[:effective_workers]
    )
    if exposure_cents == 0:
        return
    console.print(
        "[yellow]Warning:[/yellow] with "
        f"{effective_workers} concurrent workers, up to {effective_workers} "
        f"partitions worth {_money(exposure_cents)} planned cost may be in flight "
        "before the in-flight planning guard can react."
    )
    LOGGER.warning(
        "in_flight_planning_exposure",
        workers=effective_workers,
        partitions=effective_workers,
        planned_cost_cents=exposure_cents,
        planning_cap_cents=config.max_cost_cents,
    )


def _estimate_costs(
    client: DownloaderClient,
    work: list[WorkItem],
    *,
    max_workers: int,
) -> list[CostEstimate]:
    estimates_by_key: dict[tuple[str, str], tuple[Decimal, int]] = {}
    ranges = _cost_ranges(work)
    workers = max(1, min(max_workers, _MAX_COST_WORKERS, len(ranges)))

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


def _bucket_cost_warn_threshold_cents(max_cost_cents: int) -> int:
    return int(Decimal(max_cost_cents) * _BUCKET_COST_WARN_FRACTION)


__all__ = [
    "_allocate_estimated_billable_bytes",
    "_allocate_estimated_cost_cents",
    "_allocate_estimated_values",
    "_bucket_cost_warn_threshold_cents",
    "_check_bucket_cost_caps",
    "_check_cost_cap",
    "_check_disk_space",
    "_cost_ranges",
    "_estimate_costs",
    "_estimate_range",
    "_refuse_ambiguous_zero_cap",
    "_total_estimated_cents",
    "_warn_in_flight_planning_exposure",
]
