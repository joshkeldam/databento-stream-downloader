"""Runner work-item discovery and partition accounting."""

from __future__ import annotations

from collections.abc import Iterable
from datetime import date, timedelta
from pathlib import Path

import structlog

from databento_stream_downloader._runner.types import WorkItem
from databento_stream_downloader.config import DownloadConfig
from databento_stream_downloader.constants import RAW_PREFIX
from databento_stream_downloader.symbols import load_first_data_utc_dates

LOGGER = structlog.get_logger(__name__)


def _work_items_from_all(
    items: list[WorkItem],
    existing_items: set[WorkItem],
) -> list[WorkItem]:
    return [item for item in items if item not in existing_items]


def _sorted_items(items: Iterable[WorkItem]) -> list[WorkItem]:
    return sorted(items, key=lambda item: (item.symbol, item.schema, item.day))


def _all_items(config: DownloadConfig) -> list[WorkItem]:
    items: list[WorkItem] = []
    first_data_utc = load_first_data_utc_dates()
    for symbol in config.symbols:
        symbol_start = _effective_start(config, symbol, first_data_utc)
        if symbol_start > config.end:
            continue
        for schema in config.schemas:
            day = symbol_start
            while day <= config.end:
                items.append(WorkItem(symbol=symbol, schema=schema, day=day))
                day += timedelta(days=1)
    return items


def _cached_items(all_items: list[WorkItem], missing: list[WorkItem]) -> list[WorkItem]:
    missing_set = set(missing)
    return [item for item in all_items if item not in missing_set]


def _existing_items(config: DownloadConfig) -> set[WorkItem]:
    items: set[WorkItem] = set()
    first_data_utc = load_first_data_utc_dates()
    end = config.end
    for symbol in config.symbols:
        start = _effective_start(config, symbol, first_data_utc)
        if start > end:
            continue
        for schema in config.schemas:
            directory = config.data_dir / RAW_PREFIX / symbol / schema
            if not directory.exists():
                continue
            _warn_suspicious_archive_files(directory)
            for path in directory.glob("*.dbn.zst"):
                try:
                    day = date.fromisoformat(path.name.removesuffix(".dbn.zst"))
                except ValueError:
                    continue
                if start <= day <= end:
                    items.add(WorkItem(symbol=symbol, schema=schema, day=day))
    return items


def _warn_suspicious_archive_files(directory: Path) -> None:
    for path in directory.iterdir():
        name = path.name
        if (
            ".dbn.zst" in name
            and not name.endswith(".dbn.zst")
            and not name.endswith(".dbn.zst.sha256")
            and not name.endswith(".tmp")
            and not name.startswith(".")
        ):
            LOGGER.warning("suspicious_archive_file_ignored", path=str(path))


def _total_partitions(config: DownloadConfig) -> int:
    first_data_utc = load_first_data_utc_dates()
    total = 0
    for symbol in config.symbols:
        start = _effective_start(config, symbol, first_data_utc)
        if start > config.end:
            continue
        total += len(config.schemas) * ((config.end - start).days + 1)
    return total


def _effective_start(
    config: DownloadConfig,
    symbol: str,
    first_data_utc: dict[str, date],
) -> date:
    first_data_day = first_data_utc.get(symbol)
    if first_data_day is None:
        return config.start
    return max(config.start, first_data_day)


__all__ = [
    "_all_items",
    "_cached_items",
    "_effective_start",
    "_existing_items",
    "_sorted_items",
    "_total_partitions",
    "_warn_suspicious_archive_files",
    "_work_items_from_all",
]
