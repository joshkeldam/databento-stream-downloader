#!/usr/bin/env python3
"""Validate download-ledger.jsonl records against versioned ledger contracts."""

from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any, cast

SCHEMAS_DIR = Path(__file__).resolve().parents[1] / "schemas"


def main() -> int:
    if len(sys.argv) != 2:
        _err("usage: validate_ledger.py path/to/download-ledger.jsonl")
        return 2
    ledger_path = Path(sys.argv[1])
    schemas = _load_schemas()
    issues = 0
    lines = ledger_path.read_text(encoding="utf-8").splitlines()
    for line_number, line in enumerate(lines, 1):
        if not line.strip():
            continue
        try:
            record = json.loads(line)
        except json.JSONDecodeError as exc:
            _err(f"{ledger_path}:{line_number}: invalid JSON: {exc}")
            issues += 1
            continue
        issues += _validate_record(ledger_path, line_number, record, schemas)
    if issues:
        _err(f"{issues} ledger validation issue(s)")
        return 1
    sys.stdout.write("ledger valid\n")
    return 0


def _validate_record(
    ledger_path: Path,
    line_number: int,
    record: Any,
    schemas: dict[int, dict[str, Any]],
) -> int:
    if not isinstance(record, dict):
        _err(f"{ledger_path}:{line_number}: record must be an object")
        return 1
    typed_record = cast("dict[str, object]", record)
    version = typed_record.get("ledger_schema_version")
    if not isinstance(version, int) or version not in schemas:
        _err(f"{ledger_path}:{line_number}: unsupported ledger_schema_version")
        return 1
    schema = schemas[version]
    required = set(cast("list[str]", schema["required"]))
    properties = cast("dict[str, dict[str, Any]]", schema["properties"])
    issues = 0
    missing = sorted(required - typed_record.keys())
    for field in missing:
        _err(f"{ledger_path}:{line_number}: missing required field {field}")
        issues += 1
    for field, value in typed_record.items():
        spec = properties.get(field)
        if spec is None:
            continue
        if not _matches_type(value, spec):
            _err(
                f"{ledger_path}:{line_number}: field {field} "
                f"has invalid value {value!r}"
            )
            issues += 1
    return issues


def _load_schemas() -> dict[int, dict[str, Any]]:
    schemas: dict[int, dict[str, Any]] = {}
    for path in sorted(SCHEMAS_DIR.glob("ledger_schema_v*.json")):
        schema = cast("dict[str, Any]", json.loads(path.read_text(encoding="utf-8")))
        properties = cast("dict[str, dict[str, Any]]", schema["properties"])
        version_spec = properties["ledger_schema_version"]
        version = int(version_spec["const"])
        schemas[version] = schema
    return schemas


def _matches_type(value: Any, spec: dict[str, Any]) -> bool:
    if "const" in spec:
        return value == spec["const"]
    if "enum" in spec:
        return value in spec["enum"]
    expected = spec.get("type")
    if expected == "string":
        return isinstance(value, str) and len(value) >= spec.get("minLength", 0)
    if expected == "boolean":
        return isinstance(value, bool)
    if expected == "integer":
        return isinstance(value, int) and value >= spec.get("minimum", -(2**63))
    if expected == "number":
        minimum = spec.get("minimum", float("-inf"))
        return isinstance(value, int | float) and value >= minimum
    if expected == "array":
        return isinstance(value, list)
    if expected == "object":
        return isinstance(value, dict)
    return True


def _err(message: str) -> None:
    sys.stderr.write(f"{message}\n")


if __name__ == "__main__":
    raise SystemExit(main())
