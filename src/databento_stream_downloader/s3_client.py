"""S3 client wrapper for the sync tool.

boto3 low-level clients are documented as thread-safe, so a single client is
shared across worker threads. Retry classification mirrors
``databento_client.py``: transient transport failures and HTTP 408/429/5xx
are retryable; auth/credentials failures are fatal config errors; everything
else falls through to the caller as a fatal API error.
"""

from __future__ import annotations

from collections.abc import Callable, Iterator
from pathlib import Path
from typing import Any, Never, cast

import boto3
import structlog
from boto3.exceptions import (
    RetriesExceededError,
    S3TransferFailedError,
    S3UploadFailedError,
)
from boto3.s3.transfer import TransferConfig
from botocore.client import Config
from botocore.exceptions import (
    BotoCoreError,
    ClientError,
    ConnectionClosedError,
    ConnectTimeoutError,
    CredentialRetrievalError,
    EndpointConnectionError,
    NoCredentialsError,
    PartialCredentialsError,
    ReadTimeoutError,
)
from mypy_boto3_s3.client import S3Client as Boto3S3Client

from databento_stream_downloader.errors import (
    FatalAPIError,
    FatalConfigError,
    RetryableError,
)

LOGGER = structlog.get_logger(__name__)

_HTTP_REQUEST_TIMEOUT = 408
_HTTP_TOO_MANY_REQUESTS = 429
_HTTP_UNAUTHORIZED = 401
_HTTP_FORBIDDEN = 403
_HTTP_NOT_FOUND = 404
_HTTP_SERVER_ERROR_MIN = 500
DEFAULT_S3_CONNECT_TIMEOUT_SECONDS = 10.0
DEFAULT_S3_READ_TIMEOUT_SECONDS = 120.0
DEFAULT_S3_MAX_POOL_CONNECTIONS = 64

_TRANSIENT_BOTOCORE_MARKERS = (
    "connection",
    "connection was closed",
    "could not connect",
    "endpoint",
    "read timeout",
    "timed out",
    "timeout",
    "temporarily unavailable",
    "throttl",
    "slowdown",
    "500",
    "502",
    "503",
    "504",
)


class S3Client:
    """Thin boto3 S3 wrapper with retry classification.

    boto3 already retries internally via the adaptive retry mode configured
    on the underlying client; this wrapper translates the surviving failures
    into the downloader's error taxonomy so the orchestrator can decide
    whether to retry the partition or fail the run.
    """

    def __init__(
        self,
        bucket: str,
        *,
        region: str | None = None,
        max_attempts: int = 5,
        connect_timeout_seconds: float = DEFAULT_S3_CONNECT_TIMEOUT_SECONDS,
        read_timeout_seconds: float = DEFAULT_S3_READ_TIMEOUT_SECONDS,
        max_pool_connections: int = DEFAULT_S3_MAX_POOL_CONNECTIONS,
        client: Boto3S3Client | None = None,
    ) -> None:
        self._bucket = bucket
        self._region = region
        retry_cfg = Config(
            retries={"max_attempts": max_attempts, "mode": "adaptive"},
            connect_timeout=connect_timeout_seconds,
            read_timeout=read_timeout_seconds,
            max_pool_connections=max_pool_connections,
            tcp_keepalive=True,
        )
        if client is None:
            # boto3-stubs declares the boto3.client overloads with
            # `-> Unknown` return types, so basedpyright cannot infer the
            # concrete S3Client even with the stubs installed. Silence the
            # one call site rather than relax checks repo-wide.
            client = boto3.client(  # pyright: ignore[reportUnknownMemberType]
                "s3",
                region_name=region,
                config=retry_cfg,
            )
        self._client: Boto3S3Client = client
        self._transfer_config = TransferConfig(max_concurrency=1, use_threads=False)

    @property
    def bucket(self) -> str:
        return self._bucket

    @property
    def client(self) -> Boto3S3Client:
        return self._client

    def upload_file(
        self,
        local_path: Path,
        key: str,
        *,
        extra_args: dict[str, Any] | None = None,
        callback: Callable[[int], None] | None = None,
    ) -> None:
        try:
            self._client.upload_file(
                str(local_path),
                self._bucket,
                key,
                ExtraArgs=extra_args or {},
                Callback=callback,
                Config=self._transfer_config,
            )
        except (
            EndpointConnectionError,
            ConnectionClosedError,
            ReadTimeoutError,
            ConnectTimeoutError,
        ) as exc:
            raise RetryableError(f"S3 upload transient failure: {exc}") from exc
        except _TRANSFER_ERRORS as exc:
            _classify_boto_core_error(exc, "upload", key)
        except _CREDENTIAL_ERRORS as exc:
            raise FatalConfigError(f"AWS credentials missing: {exc}") from exc
        except ClientError as exc:
            _classify_client_error(exc, key)
        except BotoCoreError as exc:
            _classify_boto_core_error(exc, "upload", key)

    def download_file(
        self,
        key: str,
        local_path: Path,
        *,
        callback: Callable[[int], None] | None = None,
    ) -> None:
        try:
            self._client.download_file(
                self._bucket,
                key,
                str(local_path),
                Callback=callback,
                Config=self._transfer_config,
            )
        except (
            EndpointConnectionError,
            ConnectionClosedError,
            ReadTimeoutError,
            ConnectTimeoutError,
        ) as exc:
            raise RetryableError(f"S3 download transient failure: {exc}") from exc
        except _TRANSFER_ERRORS as exc:
            _classify_boto_core_error(exc, "download", key)
        except _CREDENTIAL_ERRORS as exc:
            raise FatalConfigError(f"AWS credentials missing: {exc}") from exc
        except ClientError as exc:
            _classify_client_error(exc, key)
        except BotoCoreError as exc:
            _classify_boto_core_error(exc, "download", key)

    def head_object(self, key: str) -> dict[str, Any] | None:
        """Return HEAD metadata or None when the object does not exist."""
        try:
            return dict(self._client.head_object(Bucket=self._bucket, Key=key))
        except ClientError as exc:
            status = _status_from_client_error(exc)
            if status == _HTTP_NOT_FOUND:
                return None
            _classify_client_error(exc, key)
        except (
            EndpointConnectionError,
            ConnectionClosedError,
            ReadTimeoutError,
            ConnectTimeoutError,
        ) as exc:
            raise RetryableError(f"S3 head_object transient failure: {exc}") from exc
        except _CREDENTIAL_ERRORS as exc:
            raise FatalConfigError(f"AWS credentials missing: {exc}") from exc
        except BotoCoreError as exc:
            _classify_boto_core_error(exc, "head_object", key)

    def delete_object(self, key: str) -> None:
        try:
            _ = self._client.delete_object(Bucket=self._bucket, Key=key)
        except (
            EndpointConnectionError,
            ConnectionClosedError,
            ReadTimeoutError,
            ConnectTimeoutError,
        ) as exc:
            raise RetryableError(f"S3 delete transient failure: {exc}") from exc
        except _CREDENTIAL_ERRORS as exc:
            raise FatalConfigError(f"AWS credentials missing: {exc}") from exc
        except ClientError as exc:
            _classify_client_error(exc, key)
        except BotoCoreError as exc:
            _classify_boto_core_error(exc, "delete", key)

    def read_object_bytes(self, key: str, *, max_bytes: int) -> bytes:
        """Read up to ``max_bytes`` from an S3 object."""
        try:
            response = cast(
                "dict[str, Any]",
                self._client.get_object(
                    Bucket=self._bucket,
                    Key=key,
                    Range=f"bytes=0-{max_bytes - 1}",
                ),
            )
            body = response["Body"]
            return cast("bytes", body.read(max_bytes))
        except (
            EndpointConnectionError,
            ConnectionClosedError,
            ReadTimeoutError,
            ConnectTimeoutError,
        ) as exc:
            raise RetryableError(f"S3 get_object transient failure: {exc}") from exc
        except _CREDENTIAL_ERRORS as exc:
            raise FatalConfigError(f"AWS credentials missing: {exc}") from exc
        except ClientError as exc:
            _classify_client_error(exc, key)
        except BotoCoreError as exc:
            _classify_boto_core_error(exc, "get_object", key)

    def list_objects(self, prefix: str) -> Iterator[dict[str, Any]]:
        """Yield S3 object metadata dicts under the given prefix."""
        try:
            paginator = self._client.get_paginator("list_objects_v2")
            for page in paginator.paginate(Bucket=self._bucket, Prefix=prefix):
                for obj in page.get("Contents", ()):
                    yield dict(obj)
        except (
            EndpointConnectionError,
            ConnectionClosedError,
            ReadTimeoutError,
            ConnectTimeoutError,
        ) as exc:
            raise RetryableError(f"S3 list transient failure: {exc}") from exc
        except _CREDENTIAL_ERRORS as exc:
            raise FatalConfigError(f"AWS credentials missing: {exc}") from exc
        except ClientError as exc:
            _classify_client_error(exc, prefix)
        except BotoCoreError as exc:
            _classify_boto_core_error(exc, "list", prefix)


def _status_from_client_error(exc: ClientError) -> int:
    response = cast("dict[str, Any]", exc.response or {})
    metadata = cast("dict[str, Any]", response.get("ResponseMetadata", {}) or {})
    return int(metadata.get("HTTPStatusCode", 0) or 0)


def _classify_client_error(exc: ClientError, key: str) -> Never:
    status = _status_from_client_error(exc)
    is_transient = (
        status in (_HTTP_REQUEST_TIMEOUT, _HTTP_TOO_MANY_REQUESTS)
        or status >= _HTTP_SERVER_ERROR_MIN
    )
    if is_transient:
        raise RetryableError(f"S3 transient {status} on {key}: {exc}") from exc
    if status in (_HTTP_UNAUTHORIZED, _HTTP_FORBIDDEN):
        raise FatalConfigError(f"S3 access denied on {key}: {exc}") from exc
    raise FatalAPIError(f"S3 client error on {key}: {exc}") from exc


_CREDENTIAL_ERRORS = (
    NoCredentialsError,
    PartialCredentialsError,
    CredentialRetrievalError,
)
_TRANSFER_ERRORS = (
    S3UploadFailedError,
    S3TransferFailedError,
    RetriesExceededError,
)
type _ClassifiedS3Error = (
    BotoCoreError | S3UploadFailedError | S3TransferFailedError | RetriesExceededError
)


def _classify_boto_core_error(
    exc: _ClassifiedS3Error,
    operation: str,
    key: str,
) -> Never:
    message = str(exc).lower()
    if any(marker in message for marker in _TRANSIENT_BOTOCORE_MARKERS):
        error = f"S3 {operation} transient failure on {key}: {exc}"
        raise RetryableError(error) from exc
    raise FatalAPIError(f"S3 {operation} failed on {key}: {exc}") from exc


__all__ = [
    "DEFAULT_S3_CONNECT_TIMEOUT_SECONDS",
    "DEFAULT_S3_MAX_POOL_CONNECTIONS",
    "DEFAULT_S3_READ_TIMEOUT_SECONDS",
    "S3Client",
]
