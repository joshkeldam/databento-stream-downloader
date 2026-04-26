"""Cached validation, sidecar repair, and coverage heuristics."""

from __future__ import annotations

from collections.abc import Collection
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, timedelta
from pathlib import Path

import structlog
from rich.console import Console
from rich.progress import (
    BarColumn,
    Progress,
    SpinnerColumn,
    TaskProgressColumn,
    TextColumn,
    TimeElapsedColumn,
)

from databento_stream_downloader._runner.fsio import (
    _read_sha256_sidecar,
    _sha256_sidecar_path,
    _validate_sha256_sidecar,
    _write_sha256_sidecar,
)
from databento_stream_downloader._runner.types import (
    WorkItem,
    _DirectoryFsyncTracker,
)
from databento_stream_downloader.config import DownloadConfig
from databento_stream_downloader.constants import DATASET
from databento_stream_downloader.dbn import validate_dbn_metadata
from databento_stream_downloader.errors import FatalConfigError, ValidationError
from databento_stream_downloader.models import StreamQuery
from databento_stream_downloader.paths import canonical_path
from databento_stream_downloader.symbols import load_first_data_utc_dates

LOGGER = structlog.get_logger(__name__)


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


__all__ = [
    "_expected_weekdays_after_first_data",
    "_raise_on_suspicious_all_no_data",
    "_repair_missing_sidecars",
    "_repair_one_sidecar",
    "_sidecar_needs_repair",
    "_sidecar_repair_state",
    "_validate",
    "_validate_one",
]
