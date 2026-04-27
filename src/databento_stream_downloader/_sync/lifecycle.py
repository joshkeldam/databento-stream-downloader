"""Sync run lifecycle: preflight → inventory → plan + confirm → transfer."""

from __future__ import annotations

import sys
import uuid
from typing import TYPE_CHECKING

import structlog
from rich.console import Console

from databento_stream_downloader._runner.fsio import _exclusive_run_lock
from databento_stream_downloader._sync.format import _print_sync_plan
from databento_stream_downloader._sync.inventory import (
    compute_plan,
    list_remote,
    s3_key_for,
    walk_local,
)
from databento_stream_downloader._sync.stream import _sync_run
from databento_stream_downloader._sync.types import (
    PlanningMode,
    SyncConfig,
    SyncDirection,
)
from databento_stream_downloader.archive_manifest import (
    ARCHIVE_MANIFEST_FILE,
    archive_manifest_path,
)
from databento_stream_downloader.config import RunMode
from databento_stream_downloader.errors import FatalError, RetryableError
from databento_stream_downloader.s3_client import S3Client

if TYPE_CHECKING:
    pass

LOGGER = structlog.get_logger(__name__)
_NONINTERACTIVE_REFUSAL = (
    "[bold red]Refusing to proceed without --yes:[/bold red] no interactive "
    "stdin available."
)


def run_sync(
    config: SyncConfig,
    *,
    console: Console | None = None,
    error_console: Console | None = None,
    client: S3Client | None = None,
) -> int:
    """Top-level entry point for a sync run.

    Returns the process exit code: 0 on clean completion, 1 if any items
    failed, 2 on safety/config refusal.
    """
    console = console or Console()
    error_console = error_console or Console(stderr=True)
    client = client or S3Client(bucket=config.bucket, region=config.region)

    LOGGER.info(
        "sync_started",
        direction=config.direction.value,
        bucket=config.bucket,
        prefix=config.prefix,
        region=config.region,
        data_dir=str(config.data_dir),
        workers=config.max_workers,
        delete=config.delete,
        verify_sha256=config.verify_sha256,
    )

    config.data_dir.mkdir(parents=True, exist_ok=True)
    with _exclusive_run_lock(
        config.data_dir,
        str(uuid.uuid4()),
        error_console,
        fsync_writes=config.fsync_writes,
    ):
        planning_mode = _effective_planning_mode(config)
        local = walk_local(config.data_dir)
        remote = list_remote(
            client,
            config.prefix,
            data_dir=config.data_dir,
            planning_mode=planning_mode,
        )

        plan = compute_plan(
            config.direction,
            local,
            remote,
            data_dir=config.data_dir,
            prefix=config.prefix,
            delete=config.delete,
            planning_mode=planning_mode,
        )
        _print_sync_plan(console, config, plan)

        if not plan.transfers and not plan.deletes:
            console.print("\n[green]Nothing to do.[/green]")
            return 0

        if config.mode is RunMode.DRY_RUN:
            return 0

        if not config.yes:
            if not sys.stdin.isatty():
                error_console.print(_NONINTERACTIVE_REFUSAL)
                return 2
            try:
                if plan.deletes:
                    prompt = (
                        "\n[bold red]This will DELETE files.[/bold red] "
                        "Type [bold]delete[/bold] to confirm: "
                    )
                    answer = console.input(prompt)
                    if answer.strip().lower() != "delete":
                        console.print("Aborted.")
                        return 0
                else:
                    answer = console.input(
                        "\n[bold]Proceed?[/bold] [dim]\\[y/N][/dim] ",
                    )
                    if answer.strip().lower() not in ("y", "yes"):
                        console.print("Aborted.")
                        return 0
            except EOFError:
                error_console.print(_NONINTERACTIVE_REFUSAL)
                return 2

        transferred, skipped, deleted, failed = _sync_run(
            config,
            client,
            console,
            plan,
            error_console=error_console,
        )
        if config.direction is SyncDirection.PUSH:
            failed += _push_manifest_to_s3(config, client, error_console)
        direction_word = (
            "Pushed" if config.direction is SyncDirection.PUSH else "Pulled"
        )
        console.print(
            f"\n[bold]{direction_word}:[/bold] transferred={transferred:,} "
            f"skipped={skipped:,} deleted={deleted:,} failed={failed:,}",
        )
        LOGGER.info(
            "sync_completed",
            direction=config.direction.value,
            transferred=transferred,
            skipped=skipped,
            deleted=deleted,
            failed=failed,
        )
        return 1 if failed else 0


def _effective_planning_mode(config: SyncConfig) -> PlanningMode:
    if (
        config.direction is SyncDirection.PULL
        and config.verify_sha256
        and config.planning_mode is PlanningMode.SIZE
    ):
        return PlanningMode.HEAD_METADATA
    return config.planning_mode


def _push_manifest_to_s3(
    config: SyncConfig,
    client: S3Client,
    error_console: Console,
) -> int:
    manifest = archive_manifest_path(config.data_dir)
    if not manifest.is_file():
        return 0
    key = s3_key_for(ARCHIVE_MANIFEST_FILE, config.prefix)
    try:
        client.upload_file(
            manifest,
            key,
            extra_args={"ContentType": "application/x-jsonlines"},
        )
    except (FatalError, RetryableError, OSError) as exc:
        error_console.print(
            "  [red]✗[/red] archive manifest upload failed: "
            f"{type(exc).__name__}: {exc}"
        )
        return 1
    LOGGER.info("archive_manifest_synced_to_s3", key=key)
    return 0


__all__ = ["_effective_planning_mode", "_push_manifest_to_s3", "run_sync"]
