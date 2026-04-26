"""Query and result models."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from decimal import Decimal


@dataclass(frozen=True, slots=True)
class CostQuery:
    """Cost query for one symbol/schema range."""

    dataset: str
    symbol: str
    schema: str
    start: date
    end: date


@dataclass(frozen=True, slots=True)
class StreamQuery:
    """Single-day stream query."""

    dataset: str
    symbol: str
    schema: str
    start: date
    end: date


@dataclass(frozen=True, slots=True)
class CostEstimate:
    """Cost and size estimate for one symbol/schema pair."""

    symbol: str
    schema: str
    cost_cents: int
    size_bytes: int
    cost_dollars: Decimal


@dataclass(frozen=True, slots=True)
class DownloadResult:
    """Aggregate download result."""

    total: int
    placed: int
    cached: int
    no_data: int
    failed: int
    estimated_cost_cents_landed: int = 0
    estimated_billable_bytes_landed: int = 0

    def __post_init__(self) -> None:
        if min(self.placed, self.cached, self.no_data, self.failed) < 0:
            msg = "download result counts must be non-negative"
            raise ValueError(msg)
        non_cached = self.placed + self.no_data + self.failed
        if non_cached > self.total:
            msg = (
                "download result invariant failed: "
                f"non_cached={non_cached}, total={self.total}"
            )
            raise ValueError(msg)
