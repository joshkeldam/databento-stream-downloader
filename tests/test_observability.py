"""Structured logging tests."""

from __future__ import annotations

import logging
from pathlib import Path

import pytest
import structlog

from databento_stream_downloader.observability import configure_logging


def test_configure_logging_writes_json_file_when_verbose(tmp_path: Path) -> None:
    log_file = tmp_path / "logs" / "events.jsonl"

    configure_logging(log_format="json", log_file=log_file, verbose=True, force=True)
    structlog.get_logger("test").info("event_written", value=1)
    logging.shutdown()

    text = log_file.read_text(encoding="utf-8")
    assert "event_written" in text
    assert '"value": 1' in text


def test_configure_logging_keeps_json_file_at_warning_without_verbose(
    tmp_path: Path,
) -> None:
    log_file = tmp_path / "logs" / "events.jsonl"

    configure_logging(log_format="json", log_file=log_file, verbose=False, force=True)
    logger = structlog.get_logger("test")
    logger.info("info_suppressed")
    logger.warning("warning_written", value=2)
    logging.shutdown()

    text = log_file.read_text(encoding="utf-8")
    assert "warning_written" in text
    assert "info_suppressed" not in text


def test_pretty_default_suppresses_structured_warning_noise(
    capsys: pytest.CaptureFixture[str],
) -> None:
    configure_logging(log_format="pretty", log_file=None, verbose=False, force=True)

    structlog.get_logger("test").warning("warning_suppressed")
    logging.shutdown()

    captured = capsys.readouterr()
    assert "warning_suppressed" not in captured.err
