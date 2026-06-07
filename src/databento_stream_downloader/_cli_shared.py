"""Small shared CLI helpers that keep command imports cheap."""

from __future__ import annotations

import argparse
import os
import signal
from collections.abc import Callable, Generator
from contextlib import contextmanager
from datetime import date
from typing import NoReturn

from pydantic import ValidationError

from databento_stream_downloader.errors import ShutdownRequestedError


def parse_date(value: str) -> date:
    if "T" in value or ":" in value:
        msg = f"Invalid date format: {value!r}. Expected YYYY-MM-DD, not a timestamp."
        raise argparse.ArgumentTypeError(msg)
    try:
        return date.fromisoformat(value)
    except ValueError:
        msg = f"Invalid date format: {value!r}. Expected YYYY-MM-DD."
        raise argparse.ArgumentTypeError(msg) from None


def parse_nonnegative_int(value: str) -> int:
    try:
        parsed = int(value)
    except ValueError:
        msg = f"Invalid integer: {value!r}."
        raise argparse.ArgumentTypeError(msg) from None
    if parsed < 0:
        msg = f"Expected a non-negative integer, got {value!r}."
        raise argparse.ArgumentTypeError(msg)
    return parsed


def parse_workers(value: str) -> int:
    workers = parse_nonnegative_int(value)
    if workers < 1 or workers > 100:
        msg = f"workers must be between 1 and 100, got {workers}."
        raise argparse.ArgumentTypeError(msg)
    return workers


def parse_positive_float(value: str) -> float:
    try:
        parsed = float(value)
    except ValueError:
        msg = f"Invalid float: {value!r}."
        raise argparse.ArgumentTypeError(msg) from None
    if parsed <= 0:
        msg = f"Expected a positive float, got {value!r}."
        raise argparse.ArgumentTypeError(msg)
    return parsed


def format_validation_error(exc: ValidationError) -> str:
    details: list[str] = []
    for error in exc.errors(include_url=False, include_context=False):
        location = ".".join(str(part) for part in error["loc"])
        message = str(error["msg"])
        if location:
            details.append(f"{location}: {message}")
        else:
            details.append(message)
    return "\n".join(details)


@contextmanager
def installed_signal_handlers(
    *,
    exit_process: Callable[[int], NoReturn] = os._exit,
) -> Generator[None]:
    sigint_received = False
    previous_sigint = signal.getsignal(signal.SIGINT)
    previous_sigterm = None

    def handle_sigint(_signum: int, _frame: object) -> None:
        nonlocal sigint_received
        if sigint_received:
            exit_process(130)
        sigint_received = True
        raise KeyboardInterrupt

    signal.signal(signal.SIGINT, handle_sigint)
    if hasattr(signal, "SIGTERM"):

        def handle_sigterm(_signum: int, _frame: object) -> None:
            raise ShutdownRequestedError("SIGTERM received")

        previous_sigterm = signal.getsignal(signal.SIGTERM)
        signal.signal(signal.SIGTERM, handle_sigterm)
    try:
        yield
    finally:
        signal.signal(signal.SIGINT, previous_sigint)
        if previous_sigterm is not None:
            signal.signal(signal.SIGTERM, previous_sigterm)


__all__ = [
    "format_validation_error",
    "installed_signal_handlers",
    "parse_date",
    "parse_nonnegative_int",
    "parse_positive_float",
    "parse_workers",
]
