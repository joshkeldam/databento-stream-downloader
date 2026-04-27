"""Tests for the Databento SDK adapter."""
# pyright: reportPrivateUsage=false

from __future__ import annotations

import errno
import warnings
from concurrent.futures import ThreadPoolExecutor
from datetime import date
from decimal import Decimal
from pathlib import Path
from threading import Barrier
from types import SimpleNamespace
from typing import Any, ClassVar, NoReturn, cast

import databento
import pytest
from databento.common.error import (
    BentoClientError,
    BentoError,
    BentoServerError,
    BentoWarning,
)

import databento_stream_downloader.databento_client as databento_client
from databento_stream_downloader.databento_client import (
    _RETRY_AFTER_CAP_SECONDS,
    DatabentoClient,
    _apply_sdk_timeout,
    _is_retryable_os_error,
    _is_semantic_no_data_422,
    _raise_classified,
    _retry_after_seconds,
)
from databento_stream_downloader.errors import DegradedError, FatalError, RetryableError
from databento_stream_downloader.models import CostQuery, StreamQuery
from databento_stream_downloader.pricing import (
    decimal_dollars_to_cents,
    dollars_to_decimal,
)


def _client(*, sleeps: list[float]) -> DatabentoClient:
    return DatabentoClient(
        "test-key",
        sleep=sleeps.append,
        jitter=lambda _low, high: high,
    )


def test_no_data_bento_warning_is_suppressed_globally() -> None:
    """The "No data found" BentoWarning is filtered for the lifetime of the process.

    A persistent filter is the only way to suppress these warnings without
    serializing every Databento SDK call on a process-global lock. Other
    BentoWarnings continue to propagate.
    """
    with warnings.catch_warnings(record=True) as caught:
        warnings.resetwarnings()
        # Re-install the filter our module installs at import time, since
        # catch_warnings() snapshots and restores the filter list.
        warnings.filterwarnings(
            "ignore",
            message=r".*No data found.*",
            category=BentoWarning,
        )
        warnings.warn(
            "No data found for the request you submitted.",
            BentoWarning,
            stacklevel=1,
        )
        warnings.warn(
            "Some other Bento problem.",
            BentoWarning,
            stacklevel=1,
        )

    assert len(caught) == 1
    assert "Some other" in str(caught[0].message)


def test_cost_estimate_conversion_rounds_half_up() -> None:
    assert decimal_dollars_to_cents(dollars_to_decimal(0.0)) == 0
    assert decimal_dollars_to_cents(dollars_to_decimal(0.004)) == 0
    assert decimal_dollars_to_cents(dollars_to_decimal(0.005)) == 1
    assert decimal_dollars_to_cents(dollars_to_decimal(12.345)) == 1235
    assert decimal_dollars_to_cents(dollars_to_decimal("12.345")) == 1235


@pytest.mark.parametrize("value", [float("nan"), float("inf"), -0.01])
def test_cost_estimate_conversion_rejects_invalid_values(value: float) -> None:
    with pytest.raises(FatalError, match="invalid Databento cost estimate"):
        dollars_to_decimal(value)


def test_retry_exhaustion_uses_backoff_without_final_sleep() -> None:
    sleeps: list[float] = []
    client = _client(sleeps=sleeps)

    def fail() -> NoReturn:
        raise BentoServerError(http_status=500, message="server")

    with pytest.raises(RetryableError, match="last_error=BentoServerError"):
        client._with_retry(fail)

    assert sleeps == [1.0, 2.0]


def test_metadata_retry_exhaustion_uses_longer_backoff() -> None:
    sleeps: list[float] = []
    client = _client(sleeps=sleeps)

    def fail() -> NoReturn:
        raise BentoClientError(http_status=429, message="rate limit")

    with pytest.raises(RetryableError, match="last_error=BentoClientError"):
        client._with_retry(fail, operation="estimate_cost")

    assert sleeps == [2.0, 4.0, 8.0, 16.0, 32.0]
    assert client.retry_counts_by_operation == {"estimate_cost": 5}


def test_stream_retry_exhaustion_uses_mbo_backoff() -> None:
    sleeps: list[float] = []
    client = _client(sleeps=sleeps)

    def fail() -> NoReturn:
        raise BentoError("Error streaming response: reset")

    with pytest.raises(RetryableError, match="6 attempts"):
        client._with_retry(fail, operation="stream_to_file")

    assert sleeps == [2.0, 4.0, 8.0, 16.0, 32.0]
    assert client.retry_counts_by_operation == {"stream_to_file": 5}


def test_retry_succeeds_after_transient_error() -> None:
    sleeps: list[float] = []
    client = _client(sleeps=sleeps)
    attempts = 0

    def flaky() -> str:
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise BentoClientError(http_status=429, message="rate limit")
        return "ok"

    assert client._with_retry(flaky) == "ok"
    assert sleeps == [1.0]
    assert client.retry_count == 1
    assert client.retry_counts_by_operation == {"request": 1}


def test_retry_attempts_are_debug_logged_and_counted(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sleeps: list[float] = []
    events: list[tuple[str, str, dict[str, object]]] = []

    class Logger:
        def debug(self, event: str, **kwargs: object) -> None:
            events.append(("debug", event, kwargs))

        def warning(self, event: str, **kwargs: object) -> None:
            events.append(("warning", event, kwargs))

    monkeypatch.setattr(
        "databento_stream_downloader.databento_client.LOGGER",
        Logger(),
    )
    client = _client(sleeps=sleeps)
    attempts = 0

    def flaky() -> str:
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise BentoClientError(http_status=429, message="rate limit")
        return "ok"

    assert client._with_retry(flaky, operation="estimate_cost") == "ok"

    assert client.retry_counts_by_operation == {"estimate_cost": 1}
    assert [event for level, event, _kwargs in events if level == "debug"] == [
        "retrying_databento_request"
    ]
    assert [event for level, event, _kwargs in events if level == "warning"] == []


def test_retry_exhaustion_warns_once_with_aggregate_context(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sleeps: list[float] = []
    events: list[tuple[str, str, dict[str, object]]] = []

    class Logger:
        def debug(self, event: str, **kwargs: object) -> None:
            events.append(("debug", event, kwargs))

        def warning(self, event: str, **kwargs: object) -> None:
            events.append(("warning", event, kwargs))

    monkeypatch.setattr(
        "databento_stream_downloader.databento_client.LOGGER",
        Logger(),
    )
    client = _client(sleeps=sleeps)

    def fail() -> NoReturn:
        raise BentoClientError(http_status=429, message="rate limit")

    with pytest.raises(RetryableError):
        client._with_retry(fail, operation="estimate_cost")

    assert client.retry_counts_by_operation == {"estimate_cost": 5}
    assert [event for level, event, _kwargs in events if level == "warning"] == [
        "databento_retries_exhausted"
    ]
    warning_kwargs = next(
        kwargs for level, _event, kwargs in events if level == "warning"
    )
    assert warning_kwargs["retry_sleeps"] == 5
    assert warning_kwargs["attempts"] == 6


def test_retry_honors_retry_after_header() -> None:
    sleeps: list[float] = []
    client = _client(sleeps=sleeps)
    attempts = 0

    def flaky() -> str:
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise BentoClientError(
                http_status=429,
                message="rate limit",
                headers={"Retry-After": "7"},
            )
        return "ok"

    assert client._with_retry(flaky) == "ok"
    assert sleeps == [7.0]


def test_retry_after_numeric_header_is_capped() -> None:
    exc = BentoClientError(
        http_status=429,
        message="rate limit",
        headers={"Retry-After": "9999999"},
    )

    assert _retry_after_seconds(exc) == _RETRY_AFTER_CAP_SECONDS


def test_retry_after_parses_missing_header_as_none() -> None:
    assert _retry_after_seconds(BentoClientError(http_status=429, message="x")) is None


def test_retry_after_parses_response_headers() -> None:
    class ResponseError(Exception):
        response = SimpleNamespace(headers={"Retry-After": "3"})

    exc = ResponseError()

    assert _retry_after_seconds(exc) == 3.0


def test_retry_after_parses_http_date(monkeypatch: pytest.MonkeyPatch) -> None:
    class HeaderError(Exception):
        headers: ClassVar[dict[str, str]] = {
            "Retry-After": "Wed, 21 Oct 2030 07:28:00 GMT"
        }

    monkeypatch.setattr("time.time", lambda: 0.0)

    delay = _retry_after_seconds(HeaderError())

    assert delay is not None
    assert delay > 0


def test_retry_after_http_date_is_capped(monkeypatch: pytest.MonkeyPatch) -> None:
    class HeaderError(Exception):
        headers: ClassVar[dict[str, str]] = {
            "Retry-After": "Wed, 21 Oct 2030 07:28:00 GMT"
        }

    monkeypatch.setattr("time.time", lambda: 0.0)

    assert _retry_after_seconds(HeaderError()) == _RETRY_AFTER_CAP_SECONDS


def test_retry_after_ignores_invalid_header() -> None:
    class HeaderError(Exception):
        headers: ClassVar[dict[str, str]] = {"Retry-After": "not a date"}

    exc = HeaderError()

    assert _retry_after_seconds(exc) is None


def test_retry_after_ignores_missing_and_empty_values() -> None:
    class NoGetHeadersError(Exception):
        headers: ClassVar[object] = object()

    class EmptyHeaderError(Exception):
        headers: ClassVar[dict[str, str]] = {"Retry-After": " "}

    assert _retry_after_seconds(NoGetHeadersError()) is None
    assert _retry_after_seconds(EmptyHeaderError()) is None


def test_retry_after_ignores_naive_http_date() -> None:
    class HeaderError(Exception):
        headers: ClassVar[dict[str, str]] = {"Retry-After": "Wed, 21 Oct 2030 07:28:00"}

    assert _retry_after_seconds(HeaderError()) is None


def test_retry_handles_sdk_wrapped_network_error() -> None:
    sleeps: list[float] = []
    client = _client(sleeps=sleeps)
    attempts = 0

    def flaky() -> str:
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise BentoError("Error streaming response: reset")
        return "ok"

    assert client._with_retry(flaky) == "ok"
    assert sleeps == [1.0]


def test_retry_does_not_retry_local_disk_errors() -> None:
    sleeps: list[float] = []
    client = _client(sleeps=sleeps)

    def fail() -> NoReturn:
        raise OSError(errno.ENOSPC, "no space left on device")

    with pytest.raises(FatalError, match="non-retryable local I/O error"):
        client._with_retry(fail)

    assert sleeps == []


def test_retry_retries_connection_reset_os_error() -> None:
    sleeps: list[float] = []
    client = _client(sleeps=sleeps)
    attempts = 0

    def flaky() -> str:
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise OSError(errno.ECONNRESET, "connection reset")
        return "ok"

    assert client._with_retry(flaky) == "ok"
    assert sleeps == [1.0]


def test_retryable_os_error_accepts_timeout_subclasses() -> None:
    assert _is_retryable_os_error(TimeoutError("timeout")) is True


def test_retryable_os_error_accepts_dns_resolution_failures() -> None:
    """DNS hiccups (gaierror) and requests.ConnectionError wrappers retry."""
    import socket

    assert _is_retryable_os_error(socket.gaierror(8, "nodename nor servname")) is True
    # requests.ConnectionError is OSError-derived but carries errno=None and
    # surfaces transport detail in its message. Simulate the shape.
    wrapped = OSError(
        "HTTPSConnectionPool(host='hist.databento.com', port=443): Max retries "
        "exceeded with url: /v0/timeseries.get_range "
        "(Caused by NameResolutionError(\"Failed to resolve 'hist.databento.com'\"))",
    )
    assert _is_retryable_os_error(wrapped) is True
    # Sanity: an unrelated OSError without a known marker stays non-retryable.
    assert _is_retryable_os_error(OSError("disk quota exceeded")) is False


def test_retry_retries_project_retryable_errors() -> None:
    sleeps: list[float] = []
    client = _client(sleeps=sleeps)
    attempts = 0

    def flaky() -> str:
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise RetryableError("transient")
        return "ok"

    assert client._with_retry(flaky) == "ok"
    assert sleeps == [1.0]


def test_retry_rejects_non_retryable_bento_error() -> None:
    sleeps: list[float] = []
    client = _client(sleeps=sleeps)

    def fail() -> NoReturn:
        raise BentoError("decode failed")

    with pytest.raises(BentoError, match="decode failed"):
        client._with_retry(fail)

    assert sleeps == []


def test_sdk_timeout_is_applied_to_metadata_and_timeseries_clients() -> None:
    class Api:
        TIMEOUT = 100.0

    class Historical:
        metadata = Api()
        timeseries = Api()

    client = Historical()

    _apply_sdk_timeout(cast("Any", client), 12.5)

    assert client.metadata.TIMEOUT == 12.5
    assert client.timeseries.TIMEOUT == 12.5
    assert Api.TIMEOUT == 100.0


def test_sdk_timeout_warning_when_api_surface_lacks_timeout(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    events: list[tuple[str, dict[str, object]]] = []

    class Logger:
        def warning(self, event: str, **kwargs: object) -> None:
            events.append((event, kwargs))

    class Historical:
        metadata = object()
        timeseries = None

    monkeypatch.setattr(
        "databento_stream_downloader.databento_client.LOGGER",
        Logger(),
    )

    _apply_sdk_timeout(cast("Any", Historical()), 12.5)

    assert [event for event, _kwargs in events] == [
        "sdk_timeout_not_applied",
        "sdk_timeout_not_applied",
    ]


def test_sdk_timeout_warning_when_assignment_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    events: list[tuple[str, dict[str, object]]] = []

    class Logger:
        def warning(self, event: str, **kwargs: object) -> None:
            events.append((event, kwargs))

    class ReadOnlyTimeout:
        def __get__(self, _instance: object, _owner: object) -> float:
            return 100.0

        def __set__(self, _instance: object, _value: object) -> None:
            raise AttributeError("read-only")

    class Api:
        TIMEOUT = ReadOnlyTimeout()

    class Historical:
        metadata = Api()
        timeseries = Api()

    monkeypatch.setattr(
        "databento_stream_downloader.databento_client.LOGGER",
        Logger(),
    )

    _apply_sdk_timeout(cast("Any", Historical()), 12.5)

    assert [kwargs["reason"] for _event, kwargs in events] == [
        "assignment_failed",
        "assignment_failed",
    ]


def test_sdk_timeout_warning_when_assignment_does_not_take(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    events: list[tuple[str, dict[str, object]]] = []

    class Logger:
        def warning(self, event: str, **kwargs: object) -> None:
            events.append((event, kwargs))

    class Api:
        TIMEOUT = 100.0

        def __setattr__(self, _name: str, _value: object) -> None:
            return

    class Historical:
        metadata = Api()
        timeseries = Api()

    monkeypatch.setattr(
        "databento_stream_downloader.databento_client.LOGGER",
        Logger(),
    )

    _apply_sdk_timeout(cast("Any", Historical()), 12.5)

    assert [kwargs["reason"] for _event, kwargs in events] == [
        "assignment_not_observed",
        "assignment_not_observed",
    ]


def test_sdk_timeout_contract_matches_pinned_databento_sdk(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    events: list[tuple[str, dict[str, object]]] = []

    class Logger:
        def warning(self, event: str, **kwargs: object) -> None:
            events.append((event, kwargs))

    monkeypatch.setattr(
        "databento_stream_downloader.databento_client.LOGGER",
        Logger(),
    )
    client = databento.Historical(key="test-key")

    _apply_sdk_timeout(client, 12.5)

    assert client.metadata.TIMEOUT == 12.5
    assert client.timeseries.TIMEOUT == 12.5
    assert not any(event == "sdk_timeout_not_applied" for event, _kwargs in events)


def test_sdk_client_is_thread_local(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    created: list[str] = []

    class Api:
        TIMEOUT = 100.0

    class Historical:
        def __init__(self, *, key: str) -> None:
            created.append(key)
            self.metadata = Api()
            self.timeseries = Api()

    monkeypatch.setattr(
        "databento_stream_downloader.databento_client.databento.Historical",
        Historical,
    )
    client = DatabentoClient("test-key", request_timeout_seconds=12.5)

    first = client._client()
    second = client._client()
    barrier = Barrier(4)

    def get_client(_index: int) -> object:
        barrier.wait()
        return client._client()

    with ThreadPoolExecutor(max_workers=4) as pool:
        threaded = list(pool.map(get_client, range(4)))

    assert first is second
    assert all(item is not first for item in threaded)
    assert len({id(item) for item in threaded}) == 4
    assert created == ["test-key"] * 5
    assert first.metadata.TIMEOUT == 12.5
    assert all(cast("Any", item).metadata.TIMEOUT == 12.5 for item in threaded)


@pytest.mark.parametrize(
    "message",
    [
        "No records found in the requested time range",
        "No data available for this request",
        "The request time range falls entirely inside a weekend.",
    ],
)
def test_422_classifier_accepts_only_semantic_no_data(message: str) -> None:
    assert _is_semantic_no_data_422(BentoClientError(http_status=422, message=message))


@pytest.mark.parametrize(
    "message",
    [
        "Unsupported schema for dataset",
        "Invalid parent symbol",
        "Malformed parameter",
        "No data because unsupported schema for dataset",
        "failed: no data center available",
    ],
)
def test_422_classifier_rejects_invalid_requests(message: str) -> None:
    assert not _is_semantic_no_data_422(
        BentoClientError(http_status=422, message=message)
    )


def test_stream_to_file_materializes_only_semantic_no_data_422(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    client = _client(sleeps=[])
    query = StreamQuery(
        dataset="GLBX.MDP3",
        symbol="ES.FUT",
        schema="mbo",
        start=date(2026, 4, 1),
        end=date(2026, 4, 2),
    )

    class Timeseries:
        def get_range(self, **_kwargs: object) -> None:
            raise BentoClientError(
                http_status=422,
                message="No records found in the requested time range",
            )

    class Historical:
        timeseries = Timeseries()

    monkeypatch.setattr(client, "_client", lambda: Historical())

    with pytest.raises(DegradedError):
        client.stream_to_file(query, tmp_path / "out.dbn.zst")


def test_stream_to_file_rejects_invalid_422_as_fatal(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    client = _client(sleeps=[])
    query = StreamQuery(
        dataset="GLBX.MDP3",
        symbol="ES.FUT",
        schema="mbo",
        start=date(2026, 4, 1),
        end=date(2026, 4, 2),
    )

    class Timeseries:
        def get_range(self, **_kwargs: object) -> None:
            raise BentoClientError(
                http_status=422,
                message="Unsupported schema for dataset",
            )

    class Historical:
        timeseries = Timeseries()

    monkeypatch.setattr(client, "_client", lambda: Historical())

    with pytest.raises(FatalError, match="Databento rejected request"):
        client.stream_to_file(query, tmp_path / "out.dbn.zst")


@pytest.mark.parametrize(
    ("status", "message"),
    [
        (401, "authentication failed"),
        (402, "insufficient funds"),
        (403, "access denied"),
    ],
)
def test_stream_to_file_classifies_account_errors(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    status: int,
    message: str,
) -> None:
    client = _client(sleeps=[])
    query = StreamQuery(
        dataset="GLBX.MDP3",
        symbol="ES.FUT",
        schema="mbo",
        start=date(2026, 4, 1),
        end=date(2026, 4, 2),
    )

    class Timeseries:
        def get_range(self, **_kwargs: object) -> None:
            raise BentoClientError(http_status=status, message="account")

    class Historical:
        timeseries = Timeseries()

    monkeypatch.setattr(client, "_client", lambda: Historical())

    with pytest.raises(FatalError, match=message):
        client.stream_to_file(query, tmp_path / "out.dbn.zst")


def test_estimate_cost_classifies_structured_client_errors(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = _client(sleeps=[])

    class Metadata:
        def get_cost(self, **_kwargs: object) -> float:
            raise BentoClientError(http_status=401, message="bad key")

    class Historical:
        metadata = Metadata()

    monkeypatch.setattr(client, "_client", lambda: Historical())

    with pytest.raises(FatalError, match="authentication failed"):
        client.estimate_cost(
            CostQuery(
                dataset="GLBX.MDP3",
                symbol="ES.FUT",
                schema="mbo",
                start=date(2026, 4, 1),
                end=date(2026, 4, 2),
            )
        )


def test_estimate_cost_wraps_unstructured_bento_errors(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = _client(sleeps=[])

    class Metadata:
        def get_cost(self, **_kwargs: object) -> float:
            raise BentoError("metadata decode failed")

    class Historical:
        metadata = Metadata()

    monkeypatch.setattr(client, "_client", lambda: Historical())

    with pytest.raises(FatalError, match="metadata decode failed"):
        client.estimate_cost(
            CostQuery(
                dataset="GLBX.MDP3",
                symbol="ES.FUT",
                schema="mbo",
                start=date(2026, 4, 1),
                end=date(2026, 4, 2),
            )
        )


def test_estimate_cost_retries_without_list_price_fallback(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = _client(sleeps=[])

    class Metadata:
        def get_cost(self, **_kwargs: object) -> float:
            raise BentoServerError(http_status=503, message="")

        def get_billable_size(self, **_kwargs: object) -> int:
            raise AssertionError("estimate_cost should not use size fallback")

        def list_unit_prices(self, **_kwargs: object) -> list[dict[str, object]]:
            raise AssertionError("estimate_cost should not use unit-price fallback")

    class Historical:
        metadata = Metadata()

    monkeypatch.setattr(client, "_client", lambda: Historical())

    with pytest.raises(RetryableError, match="6 attempts"):
        client.estimate_cost(
            CostQuery(
                dataset="GLBX.MDP3",
                symbol="6A.FUT",
                schema="mbo",
                start=date(2026, 4, 1),
                end=date(2026, 4, 2),
            )
        )


def test_estimate_size_classifies_structured_client_errors(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = _client(sleeps=[])

    class Metadata:
        def get_billable_size(self, **_kwargs: object) -> int:
            raise BentoClientError(http_status=403, message="denied")

    class Historical:
        metadata = Metadata()

    monkeypatch.setattr(client, "_client", lambda: Historical())

    with pytest.raises(FatalError, match="access denied"):
        client.estimate_size(
            CostQuery(
                dataset="GLBX.MDP3",
                symbol="ES.FUT",
                schema="mbo",
                start=date(2026, 4, 1),
                end=date(2026, 4, 2),
            )
        )


def test_estimate_size_wraps_unstructured_bento_errors(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = _client(sleeps=[])

    class Metadata:
        def get_billable_size(self, **_kwargs: object) -> int:
            raise BentoError("metadata decode failed")

    class Historical:
        metadata = Metadata()

    monkeypatch.setattr(client, "_client", lambda: Historical())

    with pytest.raises(FatalError, match="metadata decode failed"):
        client.estimate_size(
            CostQuery(
                dataset="GLBX.MDP3",
                symbol="ES.FUT",
                schema="mbo",
                start=date(2026, 4, 1),
                end=date(2026, 4, 2),
            )
        )


def test_client_methods_delegate_to_sdk_shape(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    client = _client(sleeps=[])
    output = tmp_path / "out.dbn.zst"

    class Metadata:
        def get_cost(self, **_kwargs: object) -> float:
            return 1.23

        def get_billable_size(self, **_kwargs: object) -> int:
            return 456

    class Timeseries:
        def get_range(self, **kwargs: object) -> None:
            Path(str(kwargs["path"])).write_bytes(b"dbn")

    class Historical:
        metadata = Metadata()
        timeseries = Timeseries()

    monkeypatch.setattr(client, "_client", lambda: Historical())
    cost_query = CostQuery(
        dataset="GLBX.MDP3",
        symbol="ES.FUT",
        schema="mbo",
        start=date(2026, 4, 1),
        end=date(2026, 4, 2),
    )
    stream_query = StreamQuery(
        dataset="GLBX.MDP3",
        symbol="ES.FUT",
        schema="mbo",
        start=date(2026, 4, 1),
        end=date(2026, 4, 2),
    )

    assert client.estimate_cost(cost_query) == Decimal("1.23")
    assert client.estimate_size(cost_query) == 456
    client.stream_to_file(stream_query, output)
    assert output.read_bytes() == b"dbn"


def test_stream_to_file_cleans_output_before_each_retry(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    client = _client(sleeps=[])
    output = tmp_path / "out.dbn.zst"
    output.write_bytes(b"partial")
    attempts = 0

    class Timeseries:
        def get_range(self, **kwargs: object) -> None:
            nonlocal attempts
            attempts += 1
            path = Path(str(kwargs["path"]))
            assert not path.exists()
            if attempts == 1:
                path.write_bytes(b"partial")
                raise BentoError("Error streaming response: reset")
            path.write_bytes(b"complete")

    class Historical:
        timeseries = Timeseries()

    monkeypatch.setattr(client, "_client", lambda: Historical())

    client.stream_to_file(
        StreamQuery(
            dataset="GLBX.MDP3",
            symbol="ES.FUT",
            schema="mbo",
            start=date(2026, 4, 1),
            end=date(2026, 4, 2),
        ),
        output,
    )

    assert output.read_bytes() == b"complete"
    assert attempts == 2


def test_stream_to_file_suppresses_retry_after_large_partial(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setattr(databento_client, "_STREAM_PARTIAL_RETRY_CUTOFF_BYTES", 5)
    client = _client(sleeps=[])
    output = tmp_path / "out.dbn.zst"
    attempts = 0

    class Timeseries:
        def get_range(self, **kwargs: object) -> None:
            nonlocal attempts
            attempts += 1
            Path(str(kwargs["path"])).write_bytes(b"large-partial")
            raise BentoError("Error streaming response: reset")

    class Historical:
        timeseries = Timeseries()

    monkeypatch.setattr(client, "_client", lambda: Historical())

    with pytest.raises(RetryableError, match="after 1 attempt"):
        client.stream_to_file(
            StreamQuery(
                dataset="GLBX.MDP3",
                symbol="ES.FUT",
                schema="mbo",
                start=date(2026, 4, 1),
                end=date(2026, 4, 2),
            ),
            output,
        )

    assert attempts == 1
    assert output.read_bytes() == b"large-partial"
    assert client.retry_counts_by_operation == {}


def test_stream_to_file_uses_patient_timeseries_timeout(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    client = DatabentoClient("test-key", request_timeout_seconds=12.5)

    class Timeseries:
        TIMEOUT = 100.0

        def get_range(self, **kwargs: object) -> None:
            Path(str(kwargs["path"])).write_bytes(b"complete")

    class Historical:
        timeseries = Timeseries()

    historical = Historical()
    monkeypatch.setattr(client, "_client", lambda: historical)

    client.stream_to_file(
        StreamQuery(
            dataset="GLBX.MDP3",
            symbol="ES.FUT",
            schema="mbo",
            start=date(2026, 4, 1),
            end=date(2026, 4, 2),
        ),
        tmp_path / "out.dbn.zst",
    )

    assert (
        historical.timeseries.TIMEOUT
        == databento_client._STREAM_REQUEST_TIMEOUT_SECONDS
    )


@pytest.mark.parametrize("status", [401, 402, 403])
def test_classification_marks_auth_and_payment_fatal(status: int) -> None:
    with pytest.raises(FatalError):
        _raise_classified(BentoClientError(http_status=status, message="fatal"))


@pytest.mark.parametrize("status", [429, 500, 503])
def test_classification_marks_retryable_statuses(status: int) -> None:
    with pytest.raises(RetryableError):
        _raise_classified(BentoServerError(http_status=status, message="retry"))
