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


def test_manifest_merges_previous_scope_with_new_download_scope(
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
    assert manifest["totals"]["expected_partitions"] == 2
    assert groups[("ES.FUT", "definition")]["local_present"] == 1
    assert groups[("ES.FUT", "mbo")]["local_present"] == 1
