"""Sync run lifecycle: preflight → inventory → plan + confirm → transfer."""

from __future__ import annotations

import sys
from typing import TYPE_CHECKING

import structlog
from rich.console import Console

from databento_stream_downloader._sync.format import _print_sync_plan
from databento_stream_downloader._sync.inventory import (
    compute_plan,
    list_remote,
    walk_local,
)
from databento_stream_downloader._sync.stream import _sync_run
from databento_stream_downloader._sync.types import SyncConfig, SyncDirection
from databento_stream_downloader.config import RunMode
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
    local = walk_local(config.data_dir)
    remote = list_remote(client, config.prefix)

    plan = compute_plan(
        config.direction,
        local,
        remote,
        data_dir=config.data_dir,
        prefix=config.prefix,
        delete=config.delete,
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
    direction_word = "Pushed" if config.direction is SyncDirection.PUSH else "Pulled"
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


__all__ = ["run_sync"]
