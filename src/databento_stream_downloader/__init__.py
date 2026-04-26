"""Standalone Databento stream downloader."""

import importlib.metadata

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
