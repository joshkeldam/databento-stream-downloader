"""Small concurrency helpers used by runner and sync stages."""

from __future__ import annotations

from collections.abc import Iterable
from concurrent.futures import Future
from dataclasses import dataclass, field
from threading import Lock
from typing import Any


def _cancel_futures(futures: Iterable[Future[Any]]) -> None:
    for pending in futures:
        _ = pending.cancel()


@dataclass(slots=True)
class _InFlightRegistry[T]:
    """Thread-safe registry of items currently being processed.

    Generic over the item type so the same primitive can track download
    `WorkItem`s and sync `SyncItem`s.
    """

    _items: dict[int, T] = field(default_factory=dict)
    _next_token: int = 0
    _lock: Lock = field(default_factory=Lock)

    def add(self, item: T) -> int:
        with self._lock:
            self._next_token += 1
            token = self._next_token
            self._items[token] = item
            return token

    def remove(self, token: int) -> None:
        with self._lock:
            self._items.pop(token, None)

    def snapshot(self) -> list[T]:
        with self._lock:
            return list(self._items.values())

    def __len__(self) -> int:
        with self._lock:
            return len(self._items)


__all__ = ["_InFlightRegistry", "_cancel_futures"]
