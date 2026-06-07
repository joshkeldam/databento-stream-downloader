"""Standalone Databento stream downloader."""

from __future__ import annotations

import importlib.metadata
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from databento_stream_downloader.config import DownloadConfig, RunMode
    from databento_stream_downloader.dbn import validate_dbn_metadata
    from databento_stream_downloader.errors import (
        DegradedError,
        DownloaderError,
        ErrorCategory,
        FatalAPIError,
        FatalConfigError,
        FatalError,
        InterruptedDownloadError,
        RetryableError,
        ValidationError,
    )
    from databento_stream_downloader.observability import (
        LogFormat,
        configure_logging,
    )
    from databento_stream_downloader.paths import canonical_path
    from databento_stream_downloader.runner import (
        DownloaderClient,
        run_download,
        run_download_with_client,
    )
    from databento_stream_downloader.settings import EnvSettings
    from databento_stream_downloader.symbols import load_default_symbols

try:
    __version__ = importlib.metadata.version("databento-stream-downloader")
except importlib.metadata.PackageNotFoundError:
    __version__ = "0.0.0+editable"


def __getattr__(name: str) -> Any:
    if name in {"DownloadConfig", "RunMode"}:
        from databento_stream_downloader import config

        return getattr(config, name)
    if name in {
        "DegradedError",
        "DownloaderError",
        "ErrorCategory",
        "FatalAPIError",
        "FatalConfigError",
        "FatalError",
        "InterruptedDownloadError",
        "RetryableError",
        "ValidationError",
    }:
        from databento_stream_downloader import errors

        return getattr(errors, name)
    if name in {"LogFormat", "configure_logging"}:
        from databento_stream_downloader import observability

        return getattr(observability, name)
    if name == "canonical_path":
        from databento_stream_downloader.paths import canonical_path

        return canonical_path
    if name == "EnvSettings":
        from databento_stream_downloader.settings import EnvSettings

        return EnvSettings
    if name == "load_default_symbols":
        from databento_stream_downloader.symbols import load_default_symbols

        return load_default_symbols
    if name == "validate_dbn_metadata":
        from databento_stream_downloader.dbn import validate_dbn_metadata

        return validate_dbn_metadata
    if name in {"DownloaderClient", "run_download", "run_download_with_client"}:
        from databento_stream_downloader import runner

        return getattr(runner, name)
    msg = f"module {__name__!r} has no attribute {name!r}"
    raise AttributeError(msg)


__all__ = [
    "DegradedError",
    "DownloadConfig",
    "DownloaderClient",
    "DownloaderError",
    "EnvSettings",
    "ErrorCategory",
    "FatalAPIError",
    "FatalConfigError",
    "FatalError",
    "InterruptedDownloadError",
    "LogFormat",
    "RetryableError",
    "RunMode",
    "ValidationError",
    "__version__",
    "canonical_path",
    "configure_logging",
    "load_default_symbols",
    "run_download",
    "run_download_with_client",
    "validate_dbn_metadata",
]
