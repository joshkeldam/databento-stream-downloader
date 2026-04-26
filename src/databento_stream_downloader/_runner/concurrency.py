"""Small concurrency helpers used by runner stages."""

from __future__ import annotations

from collections.abc import Iterable
from concurrent.futures import Future
from typing import Any


def _cancel_futures(futures: Iterable[Future[Any]]) -> None:
    for pending in futures:
        _ = pending.cancel()


__all__ = ["_cancel_futures"]
