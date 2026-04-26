"""Streaming stage orchestration."""

from __future__ import annotations

from concurrent.futures import Future, ThreadPoolExecutor, as_completed
from datetime import date, timedelta

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

from databento_stream_downloader._runner.concurrency import _cancel_futures
from databento_stream_downloader._runner.format import _money
from databento_stream_downloader._runner.fsio import _place_tmp, _write_sha256_sidecar
from databento_stream_downloader._runner.types import (
    DownloaderClient,
    DownloadOutcome,
    WorkItem,
    _DirectoryFsyncTracker,
)
from databento_stream_downloader._runner.validation import (
    _raise_on_suspicious_all_no_data,
)
from databento_stream_downloader._runner.work import _total_partitions
from databento_stream_downloader.config import DownloadConfig
from databento_stream_downloader.constants import DATASET
from databento_stream_downloader.dbn import validate_dbn_metadata
from databento_stream_downloader.errors import (
    DegradedError,
    FatalConfigError,
    FatalError,
    InterruptedDownloadError,
    RetryableError,
    ShutdownRequestedError,
    ValidationError,
)
from databento_stream_downloader.models import DownloadResult, StreamQuery
from databento_stream_downloader.paths import canonical_path

LOGGER = structlog.get_logger(__name__)


def _stream_missing(
    config: DownloadConfig,
    client: DownloaderClient,
    console: Console,
    work: list[WorkItem],
    *,
    error_console: Console | None = None,
    estimated_bytes_by_item: dict[WorkItem, int] | None = None,
    estimated_cost_cents_by_item: dict[WorkItem, int] | None = None,
    fsync_tracker: _DirectoryFsyncTracker | None = None,
) -> DownloadResult:
    placed = 0
    no_data = 0
    failed = 0
    completed_work = 0
    landed_estimated_cents = 0
    landed_estimated_bytes = 0
    failure_console = error_console or Console(stderr=True)

    progress = Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        TaskProgressColumn(),
        TimeElapsedColumn(),
        console=console,
    )
    pool = ThreadPoolExecutor(max_workers=config.max_workers)
    futures: list[Future[tuple[DownloadOutcome, str]]] = []
    future_items: dict[Future[tuple[DownloadOutcome, str]], WorkItem] = {}
    work_days_by_key: dict[tuple[str, str], set[date]] = {}
    no_data_days_by_key: dict[tuple[str, str], set[date]] = {}
    for item in work:
        key = (item.symbol, item.schema)
        work_days_by_key.setdefault(key, set()).add(item.day)
    fatal = False
    interrupted = False
    try:
        progress.start()
        task = progress.add_task("Downloading", total=len(work))
        futures = [
            pool.submit(
                _stream_one,
                config,
                client,
                item,
                (estimated_bytes_by_item or {}).get(item),
                fsync_tracker,
            )
            for item in work
        ]
        future_items = dict(zip(futures, work, strict=True))
        for future in as_completed(futures):
            try:
                outcome, label = future.result()
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
            item = future_items[future]
            if outcome == "placed":
                placed += 1
            elif outcome == "cached":
                pass
            elif outcome == "no_data":
                no_data += 1
                key = (item.symbol, item.schema)
                no_data_days_by_key.setdefault(key, set()).add(item.day)
                if not progress.console.quiet:
                    progress.console.print(f"  [yellow]⚠[/yellow] no data {label}")
            else:
                failed += 1
                failure_console.print(f"  [red]✗[/red] {label}")
            if outcome in ("placed", "no_data"):
                landed_estimated_bytes += (estimated_bytes_by_item or {}).get(
                    item,
                    0,
                )
                landed_estimated_cents += (estimated_cost_cents_by_item or {}).get(
                    item,
                    0,
                )
                if (
                    config.max_cost_cents is not None
                    and landed_estimated_cents > config.max_cost_cents
                ):
                    fatal = True
                    _cancel_futures(futures)
                    pool.shutdown(wait=False, cancel_futures=True)
                    msg = (
                        "in-flight planned cost exceeded planning cap: "
                        f"landed={_money(landed_estimated_cents)}, "
                        f"planning_cap={_money(config.max_cost_cents)}"
                    )
                    raise FatalConfigError(msg)
            LOGGER.info(
                "partition_completed",
                symbol=item.symbol,
                schema=item.schema,
                day=item.day.isoformat(),
                outcome=outcome,
                label=label,
            )
            completed_work += 1
            progress.advance(task)
    except (KeyboardInterrupt, ShutdownRequestedError) as exc:
        interrupted = True
        _cancel_futures(futures)
        pool.shutdown(wait=False, cancel_futures=True)
        if isinstance(exc, ShutdownRequestedError):
            raise
        raise InterruptedDownloadError("download interrupted by user") from exc
    finally:
        progress.stop()
        if fatal or interrupted:
            pool.shutdown(wait=False, cancel_futures=True)
        else:
            pool.shutdown(wait=True, cancel_futures=False)

    if completed_work != len(work):
        if interrupted or fatal:
            raise InterruptedDownloadError("download did not complete")
        msg = (
            f"download work accounting failed: completed={completed_work}, "
            f"work={len(work)}"
        )
        raise RuntimeError(msg)

    cached = _total_partitions(config) - placed - no_data - failed
    _raise_on_suspicious_all_no_data(
        work_days_by_key,
        no_data_days_by_key,
        threshold_weekdays=config.suspicious_no_data_weekdays,
    )
    return DownloadResult(
        total=_total_partitions(config),
        placed=placed,
        cached=cached,
        no_data=no_data,
        failed=failed,
        estimated_cost_cents_landed=landed_estimated_cents,
        estimated_billable_bytes_landed=landed_estimated_bytes,
    )


def _deep_validate_cap(estimated_billable_bytes: int | None) -> int | None:
    if estimated_billable_bytes is None:
        return None
    return max(estimated_billable_bytes * 2, 64 * 1024 * 1024)


def _stream_one(
    config: DownloadConfig,
    client: DownloaderClient,
    item: WorkItem,
    estimated_billable_bytes: int | None = None,
    fsync_tracker: _DirectoryFsyncTracker | None = None,
) -> tuple[DownloadOutcome, str]:
    day = item.day
    dest = canonical_path(config.data_dir, item.symbol, item.schema, day)
    label = f"{item.symbol}/{item.schema} {day.isoformat()}"
    if dest.exists():
        return ("cached", label)
    dest.parent.mkdir(parents=True, exist_ok=True)
    tmp = dest.with_name(f".{dest.stem}.tmp")
    query = StreamQuery(
        dataset=DATASET,
        symbol=item.symbol,
        schema=item.schema,
        start=day,
        end=day + timedelta(days=1),
    )
    try:
        client.stream_to_file(query, tmp)
        validate_dbn_metadata(
            query,
            tmp,
            deep=config.deep_validate,
            strict=config.strict_validate,
            max_decompressed_bytes=_deep_validate_cap(estimated_billable_bytes),
        )
        digest = _place_tmp(tmp, dest, fsync_tracker)
        _write_sha256_sidecar(dest, digest, fsync_tracker)
        return ("placed", label)
    except DegradedError:
        try:
            client.write_empty_file(query, tmp)
            validate_dbn_metadata(
                query,
                tmp,
                deep=config.deep_validate,
                strict=config.strict_validate,
                max_decompressed_bytes=_deep_validate_cap(estimated_billable_bytes),
            )
            digest = _place_tmp(tmp, dest, fsync_tracker)
            _write_sha256_sidecar(dest, digest, fsync_tracker)
            return ("no_data", label)
        except ValidationError as exc:
            tmp.unlink(missing_ok=True)
            return ("failed", f"{label}: validation error: {exc}")
        except Exception:
            tmp.unlink(missing_ok=True)
            raise
    except FatalError:
        tmp.unlink(missing_ok=True)
        raise
    except RetryableError as exc:
        tmp.unlink(missing_ok=True)
        return ("failed", f"{label}: retryable error: {exc}")
    except ValidationError as exc:
        tmp.unlink(missing_ok=True)
        return ("failed", f"{label}: validation error: {exc}")
    except Exception:
        tmp.unlink(missing_ok=True)
        raise


__all__ = [
    "_deep_validate_cap",
    "_stream_missing",
    "_stream_one",
]
