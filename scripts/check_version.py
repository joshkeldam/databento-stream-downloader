#!/usr/bin/env python3
"""Assert pyproject version matches latest changelog release header."""

from __future__ import annotations

import re
import sys
import tomllib
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def main() -> int:
    pyproject = tomllib.loads((ROOT / "pyproject.toml").read_text(encoding="utf-8"))
    pyproject_version = pyproject["project"]["version"]
    changelog = (ROOT / "CHANGELOG.md").read_text(encoding="utf-8")
    match = re.search(r"^## \[(\d+\.\d+\.\d+)\] - \d{4}-\d{2}-\d{2}$", changelog, re.M)
    if match is None:
        sys.stderr.write("could not find latest released CHANGELOG.md header\n")
        return 1
    changelog_version = match.group(1)
    if pyproject_version != changelog_version:
        sys.stderr.write(
            "version mismatch: "
            f"pyproject={pyproject_version}, changelog={changelog_version}\n"
        )
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
