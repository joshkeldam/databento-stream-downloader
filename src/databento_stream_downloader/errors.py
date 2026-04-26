"""Downloader error taxonomy."""

from __future__ import annotations

from enum import StrEnum
from typing import ClassVar


class ErrorCategory(StrEnum):
    """Operational error categories."""

    FATAL = "fatal"
    RETRYABLE = "retryable"
    DEGRADED = "degraded"
    VALIDATION = "validation"
    INTERRUPTED = "interrupted"


class DownloaderError(Exception):
    """Base error for downloader failures."""

    category: ClassVar[ErrorCategory]


class FatalError(DownloaderError):
    """Unrecoverable failure that must halt the run."""

    category = ErrorCategory.FATAL


class FatalAPIError(FatalError):
    """Unrecoverable API/account/access failure."""


class FatalConfigError(FatalError):
    """Unrecoverable local configuration or safety failure."""


class RetryableError(DownloaderError):
    """Transient failure after retries are exhausted."""

    category = ErrorCategory.RETRYABLE


class DegradedError(DownloaderError):
    """Semantic no-data response that still materializes coverage."""

    category = ErrorCategory.DEGRADED


class ValidationError(DownloaderError):
    """Downloaded artifact failed validation."""

    category = ErrorCategory.VALIDATION


class InterruptedDownloadError(DownloaderError):
    """User interrupted the run."""

    category = ErrorCategory.INTERRUPTED


class ShutdownRequestedError(DownloaderError):
    """Process received a graceful shutdown signal."""

    category = ErrorCategory.INTERRUPTED
