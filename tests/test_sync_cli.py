"""End-to-end tests for the sync CLI using moto's mock_aws."""
# pyright: reportPrivateUsage=false, reportUnknownMemberType=false, reportUnknownVariableType=false, reportUnknownArgumentType=false

from __future__ import annotations

import sys
from collections.abc import Iterator
from pathlib import Path

import boto3
import pytest
from moto import mock_aws

from databento_stream_downloader import sync_cli
from databento_stream_downloader._sync import lifecycle as sync_lifecycle
from databento_stream_downloader._sync.types import SyncConfig, SyncDirection
from databento_stream_downloader.config import RunMode
from databento_stream_downloader.s3_client import S3Client

_BUCKET = "test-archive"
_REGION = "us-east-1"


@pytest.fixture
def s3_bucket() -> Iterator[None]:
    with mock_aws():
        boto3.client("s3", region_name=_REGION).create_bucket(Bucket=_BUCKET)
        yield


def _seed_archive(root: Path) -> None:
    base = root / "raw" / "glbx-mdp3" / "ES.FUT" / "mbo"
    base.mkdir(parents=True)
    (base / "2026-04-01.dbn.zst").write_bytes(b"day1-bytes")
    (base / "2026-04-02.dbn.zst").write_bytes(b"day2-bytes-longer")


def test_push_uploads_then_idempotent_rerun_uploads_nothing(
    s3_bucket: None,
    tmp_path: Path,
) -> None:
    data = tmp_path / "data"
    _seed_archive(data)

    config = SyncConfig(
        direction=SyncDirection.PUSH,
        data_dir=data,
        bucket=_BUCKET,
        region=_REGION,
        mode=RunMode.EXECUTE,
        max_workers=2,
        yes=True,
    )

    client = S3Client(_BUCKET, region=_REGION)
    exit_code = sync_lifecycle.run_sync(config, client=client)
    assert exit_code == 0

    keys = sorted(obj["Key"] for obj in client.list_objects(""))
    assert keys == [
        "raw/glbx-mdp3/ES.FUT/mbo/2026-04-01.dbn.zst",
        "raw/glbx-mdp3/ES.FUT/mbo/2026-04-02.dbn.zst",
    ]

    # Re-run: nothing to do.
    exit_code = sync_lifecycle.run_sync(config, client=client)
    assert exit_code == 0


def test_pull_restores_archive_into_empty_dir(
    s3_bucket: None,
    tmp_path: Path,
) -> None:
    seed = tmp_path / "seed"
    _seed_archive(seed)
    push_client = S3Client(_BUCKET, region=_REGION)
    push_config = SyncConfig(
        direction=SyncDirection.PUSH,
        data_dir=seed,
        bucket=_BUCKET,
        region=_REGION,
        mode=RunMode.EXECUTE,
        max_workers=2,
        yes=True,
    )
    sync_lifecycle.run_sync(push_config, client=push_client)

    target = tmp_path / "fresh"
    pull_client = S3Client(_BUCKET, region=_REGION)
    pull_config = SyncConfig(
        direction=SyncDirection.PULL,
        data_dir=target,
        bucket=_BUCKET,
        region=_REGION,
        mode=RunMode.EXECUTE,
        max_workers=2,
        yes=True,
    )
    exit_code = sync_lifecycle.run_sync(pull_config, client=pull_client)
    assert exit_code == 0

    expected = target / "raw" / "glbx-mdp3" / "ES.FUT" / "mbo" / "2026-04-01.dbn.zst"
    assert expected.read_bytes() == b"day1-bytes"


def test_round_trip_byte_equal(
    s3_bucket: None,
    tmp_path: Path,
) -> None:
    src = tmp_path / "src"
    _seed_archive(src)
    push_client = S3Client(_BUCKET, region=_REGION)
    sync_lifecycle.run_sync(
        SyncConfig(
            direction=SyncDirection.PUSH,
            data_dir=src,
            bucket=_BUCKET,
            region=_REGION,
            mode=RunMode.EXECUTE,
            yes=True,
        ),
        client=push_client,
    )

    dest = tmp_path / "dest"
    pull_client = S3Client(_BUCKET, region=_REGION)
    sync_lifecycle.run_sync(
        SyncConfig(
            direction=SyncDirection.PULL,
            data_dir=dest,
            bucket=_BUCKET,
            region=_REGION,
            mode=RunMode.EXECUTE,
            yes=True,
        ),
        client=pull_client,
    )

    src_files = sorted(p.relative_to(src) for p in src.rglob("*") if p.is_file())
    dest_files = sorted(p.relative_to(dest) for p in dest.rglob("*") if p.is_file())
    assert src_files == dest_files
    for rel in src_files:
        assert (src / rel).read_bytes() == (dest / rel).read_bytes()


def test_push_with_delete_removes_extraneous_remote(
    s3_bucket: None,
    tmp_path: Path,
) -> None:
    seed = tmp_path / "data"
    _seed_archive(seed)
    push_client = S3Client(_BUCKET, region=_REGION)
    sync_lifecycle.run_sync(
        SyncConfig(
            direction=SyncDirection.PUSH,
            data_dir=seed,
            bucket=_BUCKET,
            region=_REGION,
            mode=RunMode.EXECUTE,
            yes=True,
        ),
        client=push_client,
    )

    # Plant an extraneous object directly on S3.
    boto3.client("s3", region_name=_REGION).put_object(
        Bucket=_BUCKET,
        Key="raw/extraneous.dbn.zst",
        Body=b"orphan",
    )
    assert push_client.head_object("raw/extraneous.dbn.zst") is not None

    # --delete + --yes removes the orphan; --yes still skips the typed prompt
    # because the lifecycle treats yes=True as "all confirmations bypassed".
    sync_lifecycle.run_sync(
        SyncConfig(
            direction=SyncDirection.PUSH,
            data_dir=seed,
            bucket=_BUCKET,
            region=_REGION,
            mode=RunMode.EXECUTE,
            delete=True,
            yes=True,
        ),
        client=push_client,
    )
    assert push_client.head_object("raw/extraneous.dbn.zst") is None


def test_dry_run_makes_no_changes(
    s3_bucket: None,
    tmp_path: Path,
) -> None:
    seed = tmp_path / "data"
    _seed_archive(seed)
    client = S3Client(_BUCKET, region=_REGION)
    exit_code = sync_lifecycle.run_sync(
        SyncConfig(
            direction=SyncDirection.PUSH,
            data_dir=seed,
            bucket=_BUCKET,
            region=_REGION,
            mode=RunMode.DRY_RUN,
            yes=True,
        ),
        client=client,
    )
    assert exit_code == 0
    assert list(client.list_objects("")) == []


def test_main_routes_push_subcommand(
    s3_bucket: None,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    seed = tmp_path / "data"
    _seed_archive(seed)
    monkeypatch.setenv("DATABENTO_S3_BUCKET", _BUCKET)
    monkeypatch.setenv("DATABENTO_S3_REGION", _REGION)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "databento-stream-sync",
            "push",
            "--data-dir",
            str(seed),
            "--workers",
            "2",
            "--yes",
        ],
    )
    sync_cli.main()
    client = S3Client(_BUCKET, region=_REGION)
    keys = sorted(obj["Key"] for obj in client.list_objects(""))
    assert len(keys) == 2


def test_main_rejects_missing_bucket(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.delenv("DATABENTO_S3_BUCKET", raising=False)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "databento-stream-sync",
            "push",
            "--data-dir",
            str(tmp_path),
            "--yes",
        ],
    )
    with pytest.raises(SystemExit) as exc_info:
        sync_cli.main()
    assert exc_info.value.code == 2
