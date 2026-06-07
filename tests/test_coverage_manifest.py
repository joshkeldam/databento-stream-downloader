"""Tests for coverage manifest generation."""
# pyright: reportPrivateUsage=false

from __future__ import annotations

import json
from datetime import date
from pathlib import Path

from databento_stream_downloader._sync.inventory import _RemoteEntry
from databento_stream_downloader.config import DownloadConfig, RunMode
from databento_stream_downloader.coverage_manifest import (
    MANIFEST_FILENAME,
    _build_groups,
    _ExpectedScope,
    _ManifestFile,
    _parse_canonical_relkey,
    _parse_sidecar_digest,
    _read_previous_manifest_scope,
    write_coverage_manifest,
    write_download_coverage_manifest,
)


def _config(tmp_path: Path) -> DownloadConfig:
    return DownloadConfig(
        data_dir=tmp_path,
        symbols=("ES.FUT",),
        schemas=("mbo",),
        start=date(2026, 4, 1),
        end=date(2026, 4, 3),
        mode=RunMode.EXECUTE,
        max_cost_cents=10_000,
        yes=True,
    )


def test_download_coverage_manifest_records_local_holes(tmp_path: Path) -> None:
    path = tmp_path / "raw" / "glbx-mdp3" / "ES.FUT" / "mbo"
    path.mkdir(parents=True)
    (path / "2026-04-01.dbn.zst").write_bytes(b"day1")
    (path / "2026-04-03.dbn.zst").write_bytes(b"day3")

    manifest_path = write_download_coverage_manifest(_config(tmp_path), run_id="run")

    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    assert manifest["run_id"] == "run"
    assert manifest["totals"]["expected_partitions"] == 3
    assert manifest["totals"]["local_files"] == 2
    assert manifest["totals"]["local_missing_partitions"] == 1
    assert manifest["groups"][0]["missing_local_dates"] == ["2026-04-02"]
    assert {item["relkey"] for item in manifest["local_files"]} == {
        "raw/glbx-mdp3/ES.FUT/mbo/2026-04-01.dbn.zst",
        "raw/glbx-mdp3/ES.FUT/mbo/2026-04-03.dbn.zst",
    }


def test_sync_coverage_manifest_records_s3_gaps_from_previous_scope(
    tmp_path: Path,
) -> None:
    path = tmp_path / "raw" / "glbx-mdp3" / "ES.FUT" / "mbo"
    path.mkdir(parents=True)
    (path / "2026-04-01.dbn.zst").write_bytes(b"day1")
    (path / "2026-04-02.dbn.zst").write_bytes(b"day2")
    write_download_coverage_manifest(_config(tmp_path), run_id="download")

    write_coverage_manifest(
        data_dir=tmp_path,
        remote={
            "raw/glbx-mdp3/ES.FUT/mbo/2026-04-01.dbn.zst": _RemoteEntry(
                key="raw/glbx-mdp3/ES.FUT/mbo/2026-04-01.dbn.zst",
                size=4,
            ),
        },
        bucket="bucket",
        prefix="archive",
        sync_direction="push",
        run_id="sync",
    )

    manifest = json.loads((tmp_path / MANIFEST_FILENAME).read_text(encoding="utf-8"))
    assert manifest["s3"] == {
        "bucket": "bucket",
        "prefix": "archive",
        "inventory_available": True,
    }
    assert manifest["totals"]["s3_files"] == 1
    assert manifest["totals"]["local_files_not_in_s3"] == 1
    assert manifest["groups"][0]["missing_s3_dates"] == [
        "2026-04-02",
        "2026-04-03",
    ]
    assert manifest["groups"][0]["local_dates_not_in_s3"] == ["2026-04-02"]


def test_download_manifest_is_scoped_to_current_download_request(
    tmp_path: Path,
) -> None:
    path = tmp_path / "raw" / "glbx-mdp3" / "ES.FUT"
    (path / "definition").mkdir(parents=True)
    (path / "mbo").mkdir(parents=True)
    (path / "definition" / "2026-04-01.dbn.zst").write_bytes(b"def")
    write_download_coverage_manifest(
        DownloadConfig(
            data_dir=tmp_path,
            symbols=("ES.FUT",),
            schemas=("definition",),
            start=date(2026, 4, 1),
            end=date(2026, 4, 1),
            mode=RunMode.EXECUTE,
            max_cost_cents=10_000,
            yes=True,
        ),
    )

    (path / "mbo" / "2026-04-02.dbn.zst").write_bytes(b"mbo")
    write_download_coverage_manifest(
        DownloadConfig(
            data_dir=tmp_path,
            symbols=("ES.FUT",),
            schemas=("mbo",),
            start=date(2026, 4, 2),
            end=date(2026, 4, 2),
            mode=RunMode.EXECUTE,
            max_cost_cents=10_000,
            yes=True,
        ),
    )

    manifest = json.loads((tmp_path / MANIFEST_FILENAME).read_text(encoding="utf-8"))
    groups = {(group["symbol"], group["schema"]): group for group in manifest["groups"]}
    assert manifest["totals"]["expected_partitions"] == 1
    assert ("ES.FUT", "definition") not in groups
    assert groups[("ES.FUT", "mbo")]["local_present"] == 1


def test_manifest_infers_scope_and_records_sidecar_without_download_config(
    tmp_path: Path,
) -> None:
    path = tmp_path / "raw" / "glbx-mdp3" / "ES.FUT" / "mbo"
    path.mkdir(parents=True)
    local_file = path / "2026-04-01.dbn.zst"
    local_file.write_bytes(b"day1")
    digest = "a" * 64
    local_file.with_suffix(local_file.suffix + ".sha256").write_text(
        f"{digest}  2026-04-01.dbn.zst\n",
        encoding="ascii",
    )

    manifest_path = write_coverage_manifest(
        data_dir=tmp_path,
        remote={
            "raw/glbx-mdp3/ES.FUT/mbo/2026-04-02.dbn.zst": _RemoteEntry(
                key="raw/glbx-mdp3/ES.FUT/mbo/2026-04-02.dbn.zst",
                size=4,
                sha256="b" * 64,
            ),
            "raw/glbx-mdp3/ES.FUT/mbo/2026-04-04.dbn.zst": _RemoteEntry(
                key="raw/glbx-mdp3/ES.FUT/mbo/2026-04-04.dbn.zst",
                size=4,
            ),
            "raw/glbx-mdp3/ES.FUT/mbo/not-a-date.dbn.zst": _RemoteEntry(
                key="raw/glbx-mdp3/ES.FUT/mbo/not-a-date.dbn.zst",
                size=4,
            ),
        },
    )

    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    assert manifest["request"] is None
    assert manifest["totals"]["expected_partitions"] == 4
    assert manifest["groups"][0]["missing_local_dates"] == [
        "2026-04-02",
        "2026-04-03",
        "2026-04-04",
    ]
    assert manifest["groups"][0]["missing_s3_dates"] == [
        "2026-04-01",
        "2026-04-03",
    ]
    assert manifest["local_files"][0]["sha256"] == digest
    assert len(manifest["s3_files"]) == 2


def test_manifest_helpers_ignore_malformed_previous_scope(
    tmp_path: Path,
) -> None:
    manifest_path = tmp_path / MANIFEST_FILENAME
    manifest_path.write_text(
        json.dumps(
            {
                "groups": [
                    "not-a-group",
                    {"symbol": "ES.FUT", "schema": "mbo"},
                    {
                        "symbol": "ES.FUT",
                        "schema": "mbo",
                        "date_start": "bad-date",
                        "date_end": "2026-04-01",
                    },
                    {
                        "symbol": "ES.FUT",
                        "schema": "mbo",
                        "date_start": "2026-04-03",
                        "date_end": "2026-04-01",
                    },
                ],
            },
        ),
        encoding="utf-8",
    )

    assert _read_previous_manifest_scope(manifest_path) is None
    manifest_path.write_text("{", encoding="utf-8")
    assert _read_previous_manifest_scope(manifest_path) is None


def test_manifest_helper_invalid_inputs_return_none() -> None:
    assert _parse_canonical_relkey("not/raw/file.dbn.zst") is None
    assert _parse_canonical_relkey("raw/glbx-mdp3/ES.FUT/mbo/file.txt") is None
    assert (
        _parse_canonical_relkey("raw/glbx-mdp3/ES.FUT/mbo/not-a-date.dbn.zst") is None
    )
    assert _parse_sidecar_digest("not-a-digest") is None
    assert _parse_sidecar_digest(f"{'g' * 64} file.dbn.zst") is None


def test_build_groups_infers_dates_when_expected_scope_is_empty() -> None:
    local_files = {
        "raw/glbx-mdp3/ES.FUT/mbo/2026-04-01.dbn.zst": _ManifestFile(
            relkey="raw/glbx-mdp3/ES.FUT/mbo/2026-04-01.dbn.zst",
            symbol="ES.FUT",
            schema="mbo",
            day=date(2026, 4, 1),
            size_bytes=1,
        ),
        "raw/glbx-mdp3/ES.FUT/mbo/2026-04-03.dbn.zst": _ManifestFile(
            relkey="raw/glbx-mdp3/ES.FUT/mbo/2026-04-03.dbn.zst",
            symbol="ES.FUT",
            schema="mbo",
            day=date(2026, 4, 3),
            size_bytes=1,
        ),
    }

    groups = _build_groups(local_files, {}, _ExpectedScope(dates_by_group={}))

    assert groups[0]["expected_partitions"] == 3
    assert groups[0]["missing_local_dates"] == ["2026-04-02"]
