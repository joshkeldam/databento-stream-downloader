"""Structured logging setup."""

from __future__ import annotations

import logging
import sys
from pathlib import Path
from typing import Literal, TextIO

import structlog

LogFormat = Literal["json", "pretty"]


def configure_logging(
    *,
    log_format: LogFormat,
    log_file: Path | None,
    verbose: bool,
    force: bool = False,
) -> None:
    """Configure process-wide structured logging."""
    stream: TextIO
    if log_file is None:
        stream = sys.stderr
    else:
        log_file.parent.mkdir(parents=True, exist_ok=True)
        stream = log_file.open("a", encoding="utf-8")

    renderer: structlog.types.Processor
    if log_format == "json":
        renderer = structlog.processors.JSONRenderer()
    else:
        renderer = structlog.dev.ConsoleRenderer(colors=log_file is None)

    level = _log_level(
        log_format=log_format,
        log_file=log_file,
        verbose=verbose,
    )
    logging.basicConfig(
        stream=stream,
        level=level,
        format="%(message)s",
        force=force,
    )
    logging.captureWarnings(True)
    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            structlog.processors.add_log_level,
            structlog.processors.TimeStamper(fmt="iso", utc=True),
            renderer,
        ],
        wrapper_class=structlog.make_filtering_bound_logger(level),
        logger_factory=structlog.stdlib.LoggerFactory(),
        cache_logger_on_first_use=False,
    )


def _log_level(*, log_format: LogFormat, log_file: Path | None, verbose: bool) -> int:
    if verbose:
        return logging.INFO
    if log_file is None and log_format == "pretty":
        return logging.ERROR
    return logging.WARNING
