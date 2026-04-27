"""Enforce per-file coverage floors from coverage.py JSON output."""

from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any, cast

_FLOORS = {
    "src/databento_stream_downloader/cli.py": 88.0,
    "src/databento_stream_downloader/databento_client.py": 90.0,
}


def main() -> None:
    if len(sys.argv) != 2:
        raise SystemExit("usage: check_coverage_floors.py COVERAGE_JSON")
    report_path = Path(sys.argv[1])
    report = cast("dict[str, Any]", json.loads(report_path.read_text(encoding="utf-8")))
    files = {
        filename.replace("\\", "/"): file_report
        for filename, file_report in cast(
            "dict[str, Any]", report.get("files", {})
        ).items()
    }
    failures: list[str] = []
    for filename, floor in _FLOORS.items():
        file_report = cast("dict[str, Any] | None", files.get(filename))
        if file_report is None:
            failures.append(f"{filename}: missing from coverage report")
            continue
        summary = cast("dict[str, Any]", file_report.get("summary", {}))
        covered = float(summary.get("percent_covered", 0.0))
        if covered < floor:
            failures.append(f"{filename}: {covered:.2f}% below required {floor:.2f}%")
    if failures:
        raise SystemExit("\n".join(failures))


if __name__ == "__main__":
    main()
