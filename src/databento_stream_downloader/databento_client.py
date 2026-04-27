"""Databento Historical API client."""

from __future__ import annotations

import errno
import re
import socket
import time
import warnings
from collections.abc import Callable
from decimal import Decimal
from email.utils import parsedate_to_datetime
from pathlib import Path
from random import uniform
from threading import Lock, local
from typing import Never, TypeVar, cast

import databento
import structlog
from databento.common.error import (
    BentoClientError,
    BentoError,
    BentoServerError,
    BentoWarning,
)

from databento_stream_downloader.config import DEFAULT_REQUEST_TIMEOUT_SECONDS
from databento_stream_downloader.dbn import write_empty_dbn_file
from databento_stream_downloader.errors import (
    DegradedError,
    FatalAPIError,
    FatalConfigError,
    RetryableError,
)
from databento_stream_downloader.models import CostQuery, StreamQuery
from databento_stream_downloader.pricing import dollars_to_decimal

_T = TypeVar("_T")
LOGGER = structlog.get_logger(__name__)
_MISSING = object()

_HTTP_TOO_MANY_REQUESTS = 429
_HTTP_REQUEST_TIMEOUT = 408
_HTTP_UNPROCESSABLE = 422
_HTTP_UNAUTHORIZED = 401
_HTTP_FORBIDDEN = 403
_HTTP_PAYMENT_REQUIRED = 402
_HTTP_SERVER_ERROR_MIN = 500
_MAX_RETRIES = 3
_MAX_METADATA_RETRIES = 6
_MAX_STREAM_RETRIES = 6
_RETRY_BASE_SECONDS = 1.0
_METADATA_RETRY_BASE_SECONDS = 2.0
_STREAM_RETRY_BASE_SECONDS = 2.0
_RETRY_CAP_SECONDS = 8.0
_METADATA_RETRY_CAP_SECONDS = 60.0
_STREAM_RETRY_CAP_SECONDS = 60.0
_RETRY_AFTER_CAP_SECONDS = _RETRY_CAP_SECONDS * 4
_RETRYABLE_OS_ERRORS = {
    errno.ECONNABORTED,
    errno.ECONNRESET,
    errno.EPIPE,
    errno.ETIMEDOUT,
}
_RETRYABLE_OS_ERROR_MESSAGE_MARKERS = (
    # DNS lookup transient failures (gaierror, urllib3 NameResolutionError).
    "name resolution",
    "nameresolutionerror",
    "failed to resolve",
    "nodename nor servname",
    "name or service not known",
    "temporary failure in name resolution",
    # urllib3-wrapped transport errors that surface via requests.ConnectionError
    # without a populated errno.
    "max retries exceeded",
    "connection aborted",
    "connection reset",
    "connection refused",
    "remote end closed",
    "broken pipe",
    "read timed out",
    "connection timed out",
)
_NO_DATA_422_PATTERNS = (
    re.compile(r"\bno records?\b"),
    re.compile(r"\bno records? found\b"),
    re.compile(r"\bno data found\b"),
    re.compile(r"\bno data available\b"),
    re.compile(r"\brequest time range falls entirely inside a weekend\b"),
)
_INVALID_422_MARKERS = (
    "invalid",
    "unsupported",
    "malformed",
    "parameter",
    "schema",
    "symbol",
    "stype",
    "dataset",
)
# Install the "No data found" BentoWarning suppression once at module import.
# A per-call `warnings.catch_warnings()` would mutate Python's process-global
# warning filter and require a lock — that lock serialized every SDK call
# across all worker threads, eliminating real concurrency. Installing the
# filter persistently is safe because BentoWarning("No data found") is only
# ever emitted by SDK calls.
warnings.filterwarnings(
    "ignore",
    message=r".*No data found.*",
    category=BentoWarning,
)


class DatabentoClient:
    """Thin Databento SDK adapter."""

    def __init__(
        self,
        api_key: str,
        *,
        sleep: Callable[[float], None] = time.sleep,
        jitter: Callable[[float, float], float] = uniform,
        request_timeout_seconds: float = DEFAULT_REQUEST_TIMEOUT_SECONDS,
    ) -> None:
        self._api_key = api_key
        self._sleep = sleep
        self._jitter = jitter
        self._request_timeout_seconds = request_timeout_seconds
        self._client_state = local()
        self._retry_count = 0
        self._retry_counts_by_operation: dict[str, int] = {}
        self._retry_lock = Lock()

    def estimate_cost(self, query: CostQuery) -> Decimal:
        client = self._client()
        try:
            cost = self._with_retry(
                lambda: client.metadata.get_cost(
                    dataset=query.dataset,
                    symbols=query.symbol,
                    stype_in="parent",
                    schema=query.schema,
                    start=query.start.isoformat(),
                    end=query.end.isoformat(),
                ),
                operation="estimate_cost",
            )
        except (BentoClientError, BentoServerError) as exc:
            return _raise_classified(exc)
        except BentoError as exc:
            raise FatalAPIError(str(exc)) from exc
        return dollars_to_decimal(cost)

    def estimate_size(self, query: CostQuery) -> int:
        client = self._client()
        try:
            return self._with_retry(
                lambda: client.metadata.get_billable_size(
                    dataset=query.dataset,
                    symbols=query.symbol,
                    stype_in="parent",
                    schema=query.schema,
                    start=query.start.isoformat(),
                    end=query.end.isoformat(),
                ),
                operation="estimate_size",
            )
        except (BentoClientError, BentoServerError) as exc:
            return _raise_classified(exc)
        except BentoError as exc:
            raise FatalAPIError(str(exc)) from exc

    def stream_to_file(self, query: StreamQuery, output_path: Path) -> None:
        client = self._client()

        def _stream() -> object:
            # The Databento SDK opens path= targets with exclusive "x+b" and
            # does not support byte-range resume, so retry attempts must start
            # from a clean temp path.
            output_path.unlink(missing_ok=True)
            return client.timeseries.get_range(
                dataset=query.dataset,
                symbols=query.symbol,
                stype_in="parent",
                schema=query.schema,
                start=query.start.isoformat(),
                end=query.end.isoformat(),
                path=str(output_path),
            )

        try:
            self._with_retry(_stream, operation="stream_to_file")
        except RetryableError:
            raise
        except (BentoClientError, BentoServerError) as exc:
            status = int(getattr(exc, "http_status", 0) or 0)
            if status == _HTTP_UNPROCESSABLE:
                if _is_semantic_no_data_422(exc):
                    raise DegradedError(str(exc)) from exc
                raise FatalAPIError(f"Databento rejected request: {exc}") from exc
            if status == _HTTP_PAYMENT_REQUIRED:
                raise FatalAPIError(f"insufficient funds: {exc}") from exc
            if status == _HTTP_UNAUTHORIZED:
                raise FatalAPIError(f"authentication failed: {exc}") from exc
            if status == _HTTP_FORBIDDEN:
                raise FatalAPIError(f"access denied: {exc}") from exc
            return _raise_classified(exc)

    def write_empty_file(self, query: StreamQuery, output_path: Path) -> None:
        """Write a metadata-valid zero-record file."""
        write_empty_dbn_file(query, output_path)

    def _client(self) -> databento.Historical:
        instance = getattr(self._client_state, "instance", None)
        if instance is None:
            instance = databento.Historical(key=self._api_key)
            _apply_sdk_timeout(instance, self._request_timeout_seconds)
            self._client_state.instance = instance
        return cast("databento.Historical", instance)

    def _with_retry(self, fn: Callable[[], _T], *, operation: str = "request") -> _T:
        last_exc: BaseException | None = None
        max_attempts, base_seconds, cap_seconds = _retry_policy(operation)
        for attempt in range(max_attempts):
            try:
                return fn()
            except RetryableError as exc:
                last_exc = exc
                if attempt < max_attempts - 1:
                    self._record_retry(operation)
                    self._sleep(
                        _logged_retry_delay(
                            exc,
                            attempt,
                            self._jitter,
                            operation,
                            max_attempts=max_attempts,
                            base_seconds=base_seconds,
                            cap_seconds=cap_seconds,
                        )
                    )
            except (BentoClientError, BentoServerError) as exc:
                last_exc = exc
                status = int(getattr(exc, "http_status", 0) or 0)
                is_retryable = (
                    status in (_HTTP_REQUEST_TIMEOUT, _HTTP_TOO_MANY_REQUESTS)
                    or status >= _HTTP_SERVER_ERROR_MIN
                )
                if not is_retryable:
                    raise
                if attempt < max_attempts - 1:
                    self._record_retry(operation)
                    self._sleep(
                        _logged_retry_delay(
                            exc,
                            attempt,
                            self._jitter,
                            operation,
                            max_attempts=max_attempts,
                            base_seconds=base_seconds,
                            cap_seconds=cap_seconds,
                        )
                    )
            except BentoError as exc:
                if not _is_retryable_bento_error(exc):
                    LOGGER.warning(
                        "bento_error_not_retryable",
                        error=str(exc),
                        error_type=type(exc).__name__,
                    )
                    raise
                last_exc = exc
                if attempt < max_attempts - 1:
                    self._record_retry(operation)
                    self._sleep(
                        _logged_retry_delay(
                            exc,
                            attempt,
                            self._jitter,
                            operation,
                            max_attempts=max_attempts,
                            base_seconds=base_seconds,
                            cap_seconds=cap_seconds,
                        )
                    )
            except OSError as exc:
                if not _is_retryable_os_error(exc):
                    raise FatalConfigError(
                        f"non-retryable local I/O error: {exc}"
                    ) from exc
                last_exc = exc
                if attempt < max_attempts - 1:
                    self._record_retry(operation)
                    self._sleep(
                        _logged_retry_delay(
                            exc,
                            attempt,
                            self._jitter,
                            operation,
                            max_attempts=max_attempts,
                            base_seconds=base_seconds,
                            cap_seconds=cap_seconds,
                        )
                    )
        last_detail = ""
        if last_exc is not None:
            last_detail = f"; last_error={type(last_exc).__name__}: {last_exc}"
            LOGGER.warning(
                "databento_retries_exhausted",
                operation=operation,
                retry_sleeps=max_attempts - 1,
                attempts=max_attempts,
                last_error_type=type(last_exc).__name__,
                last_error=str(last_exc),
            )
        attempt_word = "attempt" if max_attempts == 1 else "attempts"
        retry_sleeps = max_attempts - 1
        sleep_word = "sleep" if retry_sleeps == 1 else "sleeps"
        msg = (
            f"retry attempts exhausted after {max_attempts} {attempt_word} "
            f"and {retry_sleeps} retry {sleep_word}{last_detail}"
        )
        raise RetryableError(msg) from last_exc

    @property
    def retry_count(self) -> int:
        """Number of retry sleeps attempted by this client instance."""
        with self._retry_lock:
            return self._retry_count

    @property
    def retry_counts_by_operation(self) -> dict[str, int]:
        """Retry sleeps grouped by Databento operation name."""
        with self._retry_lock:
            return dict(self._retry_counts_by_operation)

    def _record_retry(self, operation: str) -> None:
        with self._retry_lock:
            self._retry_count += 1
            self._retry_counts_by_operation[operation] = (
                self._retry_counts_by_operation.get(operation, 0) + 1
            )


def _logged_retry_delay(
    exc: BaseException,
    attempt: int,
    jitter: Callable[[float, float], float],
    operation: str,
    *,
    max_attempts: int,
    base_seconds: float,
    cap_seconds: float,
) -> float:
    retry_after = _retry_after_seconds(exc)
    if retry_after is not None:
        delay = retry_after
    else:
        cap = min(base_seconds * (2**attempt), cap_seconds)
        delay = jitter(0, cap)
    LOGGER.debug(
        "retrying_databento_request",
        attempt=attempt + 1,
        max_retries=max_attempts,
        delay_seconds=round(delay, 3),
        status=getattr(exc, "http_status", None),
        error_type=type(exc).__name__,
        error=str(exc),
        operation=operation,
        stream_restarts_from_byte_zero=operation == "stream_to_file",
    )
    return delay


def _retry_policy(operation: str) -> tuple[int, float, float]:
    if operation == "stream_to_file":
        return (
            _MAX_STREAM_RETRIES,
            _STREAM_RETRY_BASE_SECONDS,
            _STREAM_RETRY_CAP_SECONDS,
        )
    if operation in {"estimate_cost", "estimate_size"}:
        return (
            _MAX_METADATA_RETRIES,
            _METADATA_RETRY_BASE_SECONDS,
            _METADATA_RETRY_CAP_SECONDS,
        )
    return _MAX_RETRIES, _RETRY_BASE_SECONDS, _RETRY_CAP_SECONDS


def _apply_sdk_timeout(client: databento.Historical, timeout_seconds: float) -> None:
    for api_name in ("metadata", "timeseries"):
        api = getattr(client, api_name, None)
        if api is None or not hasattr(api, "TIMEOUT"):
            LOGGER.warning(
                "sdk_timeout_not_applied",
                api=api_name,
                timeout_seconds=timeout_seconds,
            )
            continue
        # Databento 0.75 reads TIMEOUT through the API instance. Setting this
        # instance attribute avoids changing the SDK class default process-wide.
        api_obj = api
        api_type = type(cast("object", api))
        class_timeout_before = getattr(api_type, "TIMEOUT", _MISSING)
        try:
            api_obj.TIMEOUT = timeout_seconds
        except (AttributeError, TypeError) as exc:
            LOGGER.warning(
                "sdk_timeout_not_applied",
                api=api_name,
                timeout_seconds=timeout_seconds,
                reason="assignment_failed",
                error_type=type(exc).__name__,
            )
            continue
        if getattr(api_obj, "TIMEOUT", _MISSING) != timeout_seconds:
            LOGGER.warning(
                "sdk_timeout_not_applied",
                api=api_name,
                timeout_seconds=timeout_seconds,
                reason="assignment_not_observed",
            )
            continue
        class_timeout_after = getattr(api_type, "TIMEOUT", _MISSING)
        if class_timeout_after != class_timeout_before:
            LOGGER.warning(
                "sdk_timeout_not_applied",
                api=api_name,
                timeout_seconds=timeout_seconds,
                reason="class_timeout_mutated",
            )


def _is_semantic_no_data_422(exc: BaseException) -> bool:
    message = str(exc).lower()
    has_no_data_marker = any(
        pattern.search(message) for pattern in _NO_DATA_422_PATTERNS
    )
    has_invalid_marker = any(marker in message for marker in _INVALID_422_MARKERS)
    return has_no_data_marker and not has_invalid_marker


def _retry_after_seconds(exc: BaseException) -> float | None:
    headers = getattr(exc, "headers", None)
    if headers is None:
        response = getattr(exc, "response", None)
        headers = getattr(response, "headers", None)
    if not headers:
        return None
    raw_value = None
    try:
        raw_value = headers.get("Retry-After")
    except AttributeError:
        return None
    if raw_value is None:
        return None
    value = str(raw_value).strip()
    if not value:
        return None
    if value.isdecimal():
        return min(max(0.0, float(value)), _RETRY_AFTER_CAP_SECONDS)
    try:
        retry_at = parsedate_to_datetime(value)
    except (TypeError, ValueError):
        return None
    if retry_at.tzinfo is None:
        return None
    # HTTP-date values are UTC instants; time.time() is seconds since the Unix
    # epoch, so subtracting them is timezone-stable.
    return min(
        max(0.0, retry_at.timestamp() - time.time()),
        _RETRY_AFTER_CAP_SECONDS,
    )


def _is_retryable_os_error(exc: OSError) -> bool:
    if isinstance(exc, ConnectionError | TimeoutError | socket.gaierror):
        return True
    if exc.errno in _RETRYABLE_OS_ERRORS:
        return True
    # requests.exceptions.ConnectionError (and other urllib3 wrappers) inherit
    # from OSError but carry errno=None, so fall back to message-based matching
    # for transient transport failures we should retry.
    message = str(exc).lower()
    return any(marker in message for marker in _RETRYABLE_OS_ERROR_MESSAGE_MARKERS)


def _is_retryable_bento_error(exc: BentoError) -> bool:
    message = str(exc).lower()
    return (
        message.startswith("error streaming response")
        or "connection" in message
        or "timeout" in message
        or "temporarily unavailable" in message
    )


def _raise_classified(exc: BentoClientError | BentoServerError) -> Never:
    status = int(getattr(exc, "http_status", 0) or 0)
    if status == _HTTP_PAYMENT_REQUIRED:
        raise FatalAPIError(f"insufficient funds: {exc}") from exc
    if status == _HTTP_UNAUTHORIZED:
        raise FatalAPIError(f"authentication failed: {exc}") from exc
    if status == _HTTP_FORBIDDEN:
        raise FatalAPIError(f"access denied: {exc}") from exc
    if (
        status in (_HTTP_REQUEST_TIMEOUT, _HTTP_TOO_MANY_REQUESTS)
        or status >= _HTTP_SERVER_ERROR_MIN
    ):
        raise RetryableError(str(exc)) from exc
    raise FatalAPIError(str(exc)) from exc
