"""Tests for the S3Client wrapper using moto's mock_aws."""
# pyright: reportPrivateUsage=false, reportUnknownMemberType=false, reportUnknownVariableType=false, reportUnknownArgumentType=false

from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path

import boto3
import pytest
from botocore.exceptions import ClientError
from moto import mock_aws

from databento_stream_downloader.errors import (
    FatalAPIError,
    FatalConfigError,
    RetryableError,
)
from databento_stream_downloader.s3_client import (
    S3Client,
    _classify_client_error,
    _status_from_client_error,
)

_BUCKET = "test-archive"
_REGION = "us-east-1"


@pytest.fixture
def s3_bucket() -> Iterator[None]:
    with mock_aws():
        client = boto3.client("s3", region_name=_REGION)
        client.create_bucket(Bucket=_BUCKET)
        yield


def test_upload_then_download_round_trip(
    s3_bucket: None,
    tmp_path: Path,
) -> None:
    src = tmp_path / "in" / "file.dbn.zst"
    src.parent.mkdir()
    src.write_bytes(b"hello world")

    client = S3Client(_BUCKET, region=_REGION)
    client.upload_file(
        src,
        "raw/glbx-mdp3/ES.FUT/mbo/2026-04-01.dbn.zst",
        extra_args={"Metadata": {"sha256": "abc123"}},
    )

    head = client.head_object("raw/glbx-mdp3/ES.FUT/mbo/2026-04-01.dbn.zst")
    assert head is not None
    assert head["ContentLength"] == 11
    assert head["Metadata"] == {"sha256": "abc123"}

    dest = tmp_path / "out" / "downloaded.dbn.zst"
    dest.parent.mkdir()
    client.download_file("raw/glbx-mdp3/ES.FUT/mbo/2026-04-01.dbn.zst", dest)
    assert dest.read_bytes() == b"hello world"


def test_upload_and_download_callbacks_report_bytes(
    s3_bucket: None,
    tmp_path: Path,
) -> None:
    src = tmp_path / "in" / "file.dbn.zst"
    src.parent.mkdir()
    src.write_bytes(b"x" * 1024)
    upload_chunks: list[int] = []
    download_chunks: list[int] = []

    client = S3Client(_BUCKET, region=_REGION)
    client.upload_file(
        src,
        "raw/glbx-mdp3/ES.FUT/mbo/2026-04-01.dbn.zst",
        callback=upload_chunks.append,
    )
    dest = tmp_path / "out" / "downloaded.dbn.zst"
    dest.parent.mkdir()
    client.download_file(
        "raw/glbx-mdp3/ES.FUT/mbo/2026-04-01.dbn.zst",
        dest,
        callback=download_chunks.append,
    )

    assert sum(upload_chunks) == 1024
    assert sum(download_chunks) == 1024
    assert dest.read_bytes() == src.read_bytes()


def test_head_object_returns_none_for_missing_key(
    s3_bucket: None,
) -> None:
    client = S3Client(_BUCKET, region=_REGION)
    assert client.head_object("does/not/exist.dbn.zst") is None


def test_list_objects_paginates_under_prefix(
    s3_bucket: None,
    tmp_path: Path,
) -> None:
    src = tmp_path / "f.dbn.zst"
    src.write_bytes(b"x")
    client = S3Client(_BUCKET, region=_REGION)
    for i in range(5):
        client.upload_file(src, f"archives/file-{i}.dbn.zst")
    keys = sorted(obj["Key"] for obj in client.list_objects("archives/"))
    assert len(keys) == 5
    assert keys[0] == "archives/file-0.dbn.zst"


def test_read_object_bytes_returns_capped_range(
    s3_bucket: None,
    tmp_path: Path,
) -> None:
    src = tmp_path / "f.sha256"
    src.write_bytes(b"abcdef")
    client = S3Client(_BUCKET, region=_REGION)
    client.upload_file(src, "archives/f.sha256")

    assert client.read_object_bytes("archives/f.sha256", max_bytes=3) == b"abc"


def test_delete_object_removes_key(
    s3_bucket: None,
    tmp_path: Path,
) -> None:
    src = tmp_path / "f.dbn.zst"
    src.write_bytes(b"x")
    client = S3Client(_BUCKET, region=_REGION)
    client.upload_file(src, "archives/condemned.dbn.zst")
    assert client.head_object("archives/condemned.dbn.zst") is not None
    client.delete_object("archives/condemned.dbn.zst")
    assert client.head_object("archives/condemned.dbn.zst") is None


def test_classify_client_error_routes_by_http_status() -> None:
    def _err(status: int) -> ClientError:
        return ClientError(
            error_response={  # pyright: ignore[reportArgumentType]
                "Error": {"Code": "X", "Message": "y"},
                "ResponseMetadata": {"HTTPStatusCode": status},
            },
            operation_name="Whatever",
        )

    assert _status_from_client_error(_err(503)) == 503

    with pytest.raises(RetryableError):
        _classify_client_error(_err(429), "key")
    with pytest.raises(RetryableError):
        _classify_client_error(_err(503), "key")
    with pytest.raises(FatalConfigError):
        _classify_client_error(_err(401), "key")
    with pytest.raises(FatalConfigError):
        _classify_client_error(_err(403), "key")
    with pytest.raises(FatalAPIError):
        _classify_client_error(_err(400), "key")


def test_status_from_client_error_handles_missing_metadata() -> None:
    exc = ClientError(
        error_response={"Error": {"Code": "X", "Message": "y"}},
        operation_name="Op",
    )
    assert _status_from_client_error(exc) == 0
