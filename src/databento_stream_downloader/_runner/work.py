"""Runner work-item discovery and partition accounting."""

from __future__ import annotations

import re
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path

import structlog

from databento_stream_downloader._runner.types import WorkItem
from databento_stream_downloader.config import DownloadConfig
from databento_stream_downloader.constants import RAW_PREFIX
from databento_stream_downloader.symbols import load_first_data_utc_dates

LOGGER = structlog.get_logger(__name__)
_CANONICAL_DBN_NAME_RE = re.compile(r"^\d{4}-\d{2}-\d{2}\.dbn\.zst$")
WorkKey = tuple[str, str]


@dataclass(frozen=True, slots=True)
class MissingWorkSummary:
    total: int
    counts_by_key: dict[WorkKey, int]
    expected_weekdays_by_key: dict[WorkKey, int]


def _work_items_from_all(
    items: Iterable[WorkItem],
    existing_items: set[WorkItem],
) -> list[WorkItem]:
    return [item for item in items if item not in existing_items]


def _sorted_items(items: Iterable[WorkItem]) -> list[WorkItem]:
    return sorted(items, key=lambda item: (item.symbol, item.schema, item.day))


def _all_items(config: DownloadConfig) -> list[WorkItem]:
    return list(_iter_all_items(config))


def _iter_all_items(config: DownloadConfig) -> Iterable[WorkItem]:
    first_data_utc = load_first_data_utc_dates()
    for symbol in config.symbols:
        symbol_start = _effective_start(config, symbol, first_data_utc)
        if symbol_start > config.end:
            continue
        for schema in config.schemas:
            day = symbol_start
            while day <= config.end:
                yield WorkItem(symbol=symbol, schema=schema, day=day)
                day += timedelta(days=1)


def _missing_items(
    config: DownloadConfig,
    existing_items: set[WorkItem],
) -> list[WorkItem]:
    return _work_items_from_all(_iter_all_items(config), existing_items)


def _iter_missing_items(config: DownloadConfig) -> Iterable[WorkItem]:
    root = config.data_dir.resolve(strict=False)
    for item in _iter_all_items(config):
        path = (
            config.data_dir
            / RAW_PREFIX
            / item.symbol
            / item.schema
            / (f"{item.day.isoformat()}.dbn.zst")
        )
        if not path.exists() or _unsafe_archive_entry(path, root):
            yield item


def _summarize_missing_items(config: DownloadConfig) -> MissingWorkSummary:
    total = 0
    counts_by_key: dict[WorkKey, int] = {}
    expected_weekdays_by_key: dict[WorkKey, int] = {}
    for item in _iter_missing_items(config):
        key = (item.symbol, item.schema)
        total += 1
        counts_by_key[key] = counts_by_key.get(key, 0) + 1
        if item.day.weekday() < 5:
            expected_weekdays_by_key[key] = expected_weekdays_by_key.get(key, 0) + 1
    return MissingWorkSummary(
        total=total,
        counts_by_key=counts_by_key,
        expected_weekdays_by_key=expected_weekdays_by_key,
    )


def _cached_items(
    all_items: Iterable[WorkItem],
    missing: Iterable[WorkItem],
) -> list[WorkItem]:
    missing_set = set(missing)
    return [item for item in all_items if item not in missing_set]


def _existing_items(config: DownloadConfig) -> set[WorkItem]:
    return set(_iter_existing_items(config))


def _iter_existing_items(config: DownloadConfig) -> Iterable[WorkItem]:
    root = config.data_dir.resolve(strict=False)
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
            for path in directory.iterdir():
                _warn_if_suspicious_archive_file(path)
                if _unsafe_archive_entry(path, root):
                    LOGGER.warning("unsafe_archive_file_ignored", path=str(path))
                    continue
                if _CANONICAL_DBN_NAME_RE.fullmatch(path.name) is None:
                    continue
                try:
                    day = date.fromisoformat(path.name.removesuffix(".dbn.zst"))
                except ValueError:
                    continue
                if start <= day <= end:
                    yield WorkItem(symbol=symbol, schema=schema, day=day)


def _unsafe_archive_entry(path: Path, root: Path) -> bool:
    return path.is_symlink() or not path.resolve(strict=False).is_relative_to(root)


def _warn_suspicious_archive_files(directory: Path) -> None:
    for path in directory.iterdir():
        _warn_if_suspicious_archive_file(path)


def _warn_if_suspicious_archive_file(path: Path) -> None:
    name = path.name
    if (
        ".dbn.zst" in name
        and _CANONICAL_DBN_NAME_RE.fullmatch(name) is None
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
    "_iter_all_items",
    "_iter_existing_items",
    "_iter_missing_items",
    "_missing_items",
    "_sorted_items",
    "_summarize_missing_items",
    "_total_partitions",
    "_unsafe_archive_entry",
    "_warn_if_suspicious_archive_file",
    "_warn_suspicious_archive_files",
    "_work_items_from_all",
]
