"""Cost estimation, cap checks, and estimate allocation."""

from __future__ import annotations

import heapq
import shutil
from collections.abc import Iterable
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait
from datetime import date, timedelta
from decimal import Decimal
from pathlib import Path

import structlog
from rich.console import Console
from rich.progress import (
    BarColumn,
    MofNCompleteColumn,
    Progress,
    SpinnerColumn,
    TaskID,
    TextColumn,
    TimeElapsedColumn,
)

from databento_stream_downloader._runner.concurrency import _cancel_futures
from databento_stream_downloader._runner.format import _bytes, _money
from databento_stream_downloader._runner.fsio import _nearest_existing_parent
from databento_stream_downloader._runner.types import DownloaderClient, WorkItem
from databento_stream_downloader._runner.work import WorkKey
from databento_stream_downloader.config import DownloadConfig
from databento_stream_downloader.constants import DATASET
from databento_stream_downloader.errors import RetryableError
from databento_stream_downloader.models import CostEstimate, CostQuery
from databento_stream_downloader.pricing import decimal_dollars_to_cents

_BUCKET_COST_WARN_FRACTION = Decimal("0.25")
# Mirrors the working Helix downloader path: estimate one span per missing
# (symbol, schema) pair, with bounded metadata concurrency.
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
    _warn_in_flight_planning_exposure_for_costs(
        config,
        estimated_cost_cents_by_item.values(),
        total_work=len(work),
        console=console,
    )


def _warn_in_flight_planning_exposure_for_costs(
    config: DownloadConfig,
    estimated_cost_cents: Iterable[int],
    *,
    total_work: int,
    console: Console,
) -> None:
    if config.max_cost_cents is None:
        return
    effective_workers = min(config.max_workers, total_work)
    if effective_workers <= 1:
        return
    exposure_cents = sum(heapq.nlargest(effective_workers, estimated_cost_cents))
    if exposure_cents <= config.max_cost_cents:
        return
    if not config.allow_burst_exposure:
        console.print(
            "[bold red]Refusing download because concurrent burst exposure "
            "exceeds the planning cap:[/bold red] "
            f"workers={effective_workers}, "
            f"largest_concurrent_window={_money(exposure_cents)}, "
            f"planning_cap={_money(config.max_cost_cents)}. "
            "Lower --workers or pass --allow-burst-exposure."
        )
        LOGGER.warning(
            "cost_cap_burst_exposure_refused",
            workers=effective_workers,
            partitions=effective_workers,
            planned_cost_cents=exposure_cents,
            planning_cap_cents=config.max_cost_cents,
        )
        raise SystemExit(2)
    console.print(
        "[yellow]Warning:[/yellow] cost-cap admission control may run fewer than "
        f"{effective_workers} concurrent workers because the next "
        f"{effective_workers} partitions total {_money(exposure_cents)} against "
        f"planning cap {_money(config.max_cost_cents)}."
    )
    LOGGER.warning(
        "cost_cap_admission_throttling_possible",
        workers=effective_workers,
        partitions=effective_workers,
        planned_cost_cents=exposure_cents,
        planning_cap_cents=config.max_cost_cents,
    )


def _estimate_costs(
    client: DownloaderClient,
    work: Iterable[WorkItem],
    *,
    max_workers: int,
    console: Console | None = None,
) -> list[CostEstimate]:
    estimates_by_key: dict[tuple[str, str], tuple[Decimal, int]] = {}
    ranges = _cost_ranges(work)
    if not ranges:
        return []
    workers = max(1, min(max_workers, _MAX_COST_WORKERS, len(ranges)))
    progress = _build_estimate_progress(console)
    started_task_id: TaskID | None = None
    completed_task_id: TaskID | None = None
    if progress is not None:
        if console is None:
            raise RuntimeError("estimate progress requires a console")
        console.print(
            "Estimating Databento cost and billable size for "
            f"{len(ranges):,} symbol/schema span"
            f"{'' if len(ranges) == 1 else 's'} with {workers} metadata "
            f"worker{'' if workers == 1 else 's'}..."
        )
        started_task_id = progress.add_task("Price quote requests", total=len(ranges))
        completed_task_id = progress.add_task(
            "Price quote complete (0 active)",
            total=len(ranges),
        )
        progress.start()

    try:
        if workers == 1:
            _estimate_ranges_serial(
                client,
                ranges,
                estimates_by_key,
                progress,
                started_task_id,
                completed_task_id,
            )
            return _build_estimates(estimates_by_key)

        pool = ThreadPoolExecutor(max_workers=workers)
        futures: list[Future[tuple[str, str, Decimal, int]]] = []
        active_futures: set[Future[tuple[str, str, Decimal, int]]] = set()
        wait_for_estimates = True
        pool_closed = False
        ranges_iter = iter(ranges)

        def _submit_next() -> None:
            _set_estimate_progress_active(
                progress, completed_task_id, len(active_futures)
            )
            while len(active_futures) < workers:
                try:
                    symbol, schema, start, end = next(ranges_iter)
                except StopIteration:
                    break
                future = pool.submit(
                    _estimate_range,
                    client,
                    symbol,
                    schema,
                    start,
                    end,
                )
                futures.append(future)
                active_futures.add(future)
                _advance_estimate_progress(progress, started_task_id)
            _set_estimate_progress_active(
                progress, completed_task_id, len(active_futures)
            )

        try:
            _submit_next()
            while active_futures:
                done, _pending = wait(active_futures, return_when=FIRST_COMPLETED)
                for future in done:
                    active_futures.remove(future)
                    _set_estimate_progress_active(
                        progress,
                        completed_task_id,
                        len(active_futures),
                    )
                    try:
                        symbol, schema, cost, size_bytes = future.result()
                    except RetryableError as exc:
                        wait_for_estimates = False
                        _cancel_futures(futures)
                        pool.shutdown(wait=True, cancel_futures=True)
                        pool_closed = True
                        LOGGER.warning(
                            "cost_estimate_parallel_retryable_failure_retrying_serial",
                            workers=workers,
                            spans=len(ranges),
                            error=str(exc),
                        )
                        _reset_estimate_progress(
                            progress,
                            started_task_id,
                            completed_task_id,
                            len(ranges),
                        )
                        estimates_by_key.clear()
                        _estimate_ranges_serial(
                            client,
                            ranges,
                            estimates_by_key,
                            progress,
                            started_task_id,
                            completed_task_id,
                        )
                        return _build_estimates(estimates_by_key)
                    except Exception:
                        wait_for_estimates = False
                        _cancel_futures(futures)
                        pool.shutdown(wait=True, cancel_futures=True)
                        pool_closed = True
                        raise
                    _add_estimate(estimates_by_key, (symbol, schema, cost, size_bytes))
                    _advance_estimate_progress(progress, completed_task_id)
                    _submit_next()
            _set_estimate_progress_active(progress, completed_task_id, 0)
        finally:
            if not pool_closed:
                pool.shutdown(
                    wait=wait_for_estimates,
                    cancel_futures=not wait_for_estimates,
                )

        return _build_estimates(estimates_by_key)
    finally:
        if progress is not None:
            progress.stop()


def _estimate_ranges_serial(
    client: DownloaderClient,
    ranges: list[tuple[str, str, date, date]],
    estimates_by_key: dict[tuple[str, str], tuple[Decimal, int]],
    progress: Progress | None,
    started_task_id: TaskID | None,
    completed_task_id: TaskID | None,
) -> None:
    for symbol, schema, start, end in ranges:
        _set_estimate_progress_active(progress, completed_task_id, 1)
        _advance_estimate_progress(progress, started_task_id)
        _add_estimate(
            estimates_by_key,
            _estimate_range(client, symbol, schema, start, end),
        )
        _advance_estimate_progress(progress, completed_task_id)
    _set_estimate_progress_active(progress, completed_task_id, 0)


def _build_estimate_progress(console: Console | None) -> Progress | None:
    if console is None or bool(getattr(console, "quiet", False)):
        return None
    return Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(bar_width=None),
        MofNCompleteColumn(),
        TextColumn("•"),
        TimeElapsedColumn(),
        console=console,
        transient=False,
        expand=True,
    )


def _advance_estimate_progress(
    progress: Progress | None,
    task_id: TaskID | None,
) -> None:
    if progress is None or task_id is None:
        return
    progress.advance(task_id)


def _set_estimate_progress_active(
    progress: Progress | None,
    task_id: TaskID | None,
    active_workers: int,
) -> None:
    if progress is None or task_id is None:
        return
    progress.update(
        task_id,
        description=f"Price quote complete ({active_workers} active)",
    )


def _reset_estimate_progress(
    progress: Progress | None,
    started_task_id: TaskID | None,
    completed_task_id: TaskID | None,
    total: int,
) -> None:
    if progress is None or started_task_id is None or completed_task_id is None:
        return
    progress.update(
        started_task_id,
        completed=0,
        total=total,
        description="Price quote requests (serial retry)",
    )
    progress.update(
        completed_task_id,
        completed=0,
        total=total,
        description="Price quote complete (serial retry)",
    )


def _add_estimate(
    estimates_by_key: dict[tuple[str, str], tuple[Decimal, int]],
    estimate: tuple[str, str, Decimal, int],
) -> None:
    symbol, schema, cost, size_bytes = estimate
    key = (symbol, schema)
    current_cost, current_size = estimates_by_key.get(key, (Decimal("0"), 0))
    estimates_by_key[key] = (
        current_cost + cost,
        current_size + size_bytes,
    )


def _build_estimates(
    estimates_by_key: dict[tuple[str, str], tuple[Decimal, int]],
) -> list[CostEstimate]:
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
    cost = _estimate_cost_range(client, symbol, schema, start, end)
    size_bytes = _estimate_size_range(client, symbol, schema, start, end)
    return (symbol, schema, cost, size_bytes)


def _estimate_cost_range(
    client: DownloaderClient,
    symbol: str,
    schema: str,
    start: date,
    end: date,
) -> Decimal:
    query = CostQuery(
        dataset=DATASET,
        symbol=symbol,
        schema=schema,
        start=start,
        end=end,
    )
    try:
        return client.estimate_cost(query)
    except RetryableError as exc:
        return _split_or_raise_cost_range(
            client,
            symbol,
            schema,
            start,
            end,
            operation="get_cost",
            exc=exc,
        )


def _estimate_size_range(
    client: DownloaderClient,
    symbol: str,
    schema: str,
    start: date,
    end: date,
) -> int:
    query = CostQuery(
        dataset=DATASET,
        symbol=symbol,
        schema=schema,
        start=start,
        end=end,
    )
    try:
        return client.estimate_size(query)
    except RetryableError as exc:
        return _split_or_raise_size_range(
            client,
            symbol,
            schema,
            start,
            end,
            operation="get_billable_size",
            exc=exc,
        )


def _split_or_raise_cost_range(
    client: DownloaderClient,
    symbol: str,
    schema: str,
    start: date,
    end: date,
    *,
    operation: str,
    exc: RetryableError,
) -> Decimal:
    days = (end - start).days
    if days <= 1:
        raise RetryableError(
            f"{symbol}/{schema} {start.isoformat()}..{end.isoformat()} "
            f"{operation} failed: {exc}"
        ) from exc

    midpoint = start + timedelta(days=max(1, days // 2))
    _log_estimate_range_split(symbol, schema, start, end, midpoint, operation, exc)
    left_cost = _estimate_cost_range(
        client,
        symbol,
        schema,
        start,
        midpoint,
    )
    right_cost = _estimate_cost_range(
        client,
        symbol,
        schema,
        midpoint,
        end,
    )
    return left_cost + right_cost


def _split_or_raise_size_range(
    client: DownloaderClient,
    symbol: str,
    schema: str,
    start: date,
    end: date,
    *,
    operation: str,
    exc: RetryableError,
) -> int:
    days = (end - start).days
    if days <= 1:
        raise RetryableError(
            f"{symbol}/{schema} {start.isoformat()}..{end.isoformat()} "
            f"{operation} failed: {exc}"
        ) from exc

    midpoint = start + timedelta(days=max(1, days // 2))
    _log_estimate_range_split(symbol, schema, start, end, midpoint, operation, exc)
    left_size = _estimate_size_range(
        client,
        symbol,
        schema,
        start,
        midpoint,
    )
    right_size = _estimate_size_range(
        client,
        symbol,
        schema,
        midpoint,
        end,
    )
    return left_size + right_size


def _log_estimate_range_split(
    symbol: str,
    schema: str,
    start: date,
    end: date,
    midpoint: date,
    operation: str,
    exc: RetryableError,
) -> None:
    LOGGER.warning(
        "cost_estimate_range_split",
        symbol=symbol,
        schema=schema,
        start=start.isoformat(),
        end=end.isoformat(),
        midpoint=midpoint.isoformat(),
        operation=operation,
        error=str(exc),
    )


def _cost_ranges(work: Iterable[WorkItem]) -> list[tuple[str, str, date, date]]:
    ranges_by_key: dict[WorkKey, tuple[date, date]] = {}
    for item in work:
        key = (item.symbol, item.schema)
        current = ranges_by_key.get(key)
        if current is None:
            ranges_by_key[key] = (item.day, item.day)
            continue
        start, end = current
        ranges_by_key[key] = (min(start, item.day), max(end, item.day))

    ranges: list[tuple[str, str, date, date]] = []
    for (symbol, schema), (start, end) in sorted(ranges_by_key.items()):
        ranges.append((symbol, schema, start, end + timedelta(days=1)))
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
    "_warn_in_flight_planning_exposure_for_costs",
]
