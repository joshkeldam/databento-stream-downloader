"""Tests for sync error paths and uncovered branches."""
# pyright: reportPrivateUsage=false, reportUnknownMemberType=false, reportUnknownVariableType=false, reportUnknownArgumentType=false, reportUnknownLambdaType=false

from __future__ import annotations

import io
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import boto3
import pytest
from botocore.exceptions import (
    ClientError,
    ConnectTimeoutError,
    EndpointConnectionError,
    NoCredentialsError,
    ReadTimeoutError,
)
from moto import mock_aws
from rich.console import Console

from databento_stream_downloader._runner.fsio import _exclusive_run_lock
from databento_stream_downloader._sync import lifecycle as sync_lifecycle
from databento_stream_downloader._sync.transfer import (
    delete_one,
    download_one,
    upload_one,
)
from databento_stream_downloader._sync.types import (
    PlanningMode,
    SyncConfig,
    SyncDirection,
    SyncItem,
)
from databento_stream_downloader.config import RunMode
from databento_stream_downloader.errors import (
    FatalConfigError,
    RetryableError,
)
from databento_stream_downloader.s3_client import S3Client

_BUCKET = "errs-bucket"
_REGION = "us-east-1"


@pytest.fixture
def s3_bucket() -> Iterator[None]:
    with mock_aws():
        boto3.client("s3", region_name=_REGION).create_bucket(Bucket=_BUCKET)
        yield


# --------------------------------------------------------------- transfer.py


class _RetryableUploadClient:
    bucket = _BUCKET

    def upload_file(self, *_a: Any, **_kw: Any) -> None:
        raise RetryableError("transient")

    def download_file(self, *_a: Any, **_kw: Any) -> None:
        raise RetryableError("transient")

    def delete_object(self, *_a: Any, **_kw: Any) -> None:
        raise RetryableError("transient")


class _OSErrorUploadClient:
    bucket = _BUCKET

    def upload_file(self, *_a: Any, **_kw: Any) -> None:
        raise OSError("disk full")

    def download_file(self, *_a: Any, **_kw: Any) -> None:
        raise OSError("disk full")


def test_upload_one_marks_failed_on_retryable(tmp_path: Path) -> None:
    src = tmp_path / "f.dbn.zst"
    src.write_bytes(b"abc")
    item = SyncItem(local_path=src, s3_key="raw/f.dbn.zst", size_bytes=3, op="transfer")

    outcome, label, moved = upload_one(
        _RetryableUploadClient(),  # type: ignore[arg-type]
        item,
        in_flight=None,
    )

    assert outcome == "failed"
    assert "retryable error" in label
    assert moved == 0


def test_upload_one_marks_failed_on_local_oserror(tmp_path: Path) -> None:
    src = tmp_path / "f.dbn.zst"
    src.write_bytes(b"abc")
    item = SyncItem(local_path=src, s3_key="raw/f.dbn.zst", size_bytes=3, op="transfer")

    outcome, label, _moved = upload_one(
        _OSErrorUploadClient(),  # type: ignore[arg-type]
        item,
        in_flight=None,
    )

    assert outcome == "failed"
    assert "local I/O error" in label


def test_upload_one_attaches_metadata_when_sidecar_present(
    s3_bucket: None,
    tmp_path: Path,
) -> None:
    src = tmp_path / "f.dbn.zst"
    src.write_bytes(b"abc")
    item = SyncItem(
        local_path=src,
        s3_key="raw/f.dbn.zst",
        size_bytes=3,
        op="transfer",
        sha256="ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad",
    )
    client = S3Client(_BUCKET, region=_REGION)
    upload_one(client, item, in_flight=None)

    head = client.head_object("raw/f.dbn.zst")
    assert head is not None
    assert head["Metadata"]["sha256"] == item.sha256


def test_download_one_retries_then_fails_on_oserror(tmp_path: Path) -> None:
    item = SyncItem(
        local_path=tmp_path / "out" / "f.dbn.zst",
        s3_key="raw/f.dbn.zst",
        size_bytes=3,
        op="transfer",
    )
    outcome, label, _moved = download_one(
        _OSErrorUploadClient(),  # type: ignore[arg-type]
        item,
        in_flight=None,
        verify_sha256=False,
        fsync_writes=False,
    )
    assert outcome == "failed"
    assert "local I/O error" in label


def test_download_one_retryable_failure_returns_failed(tmp_path: Path) -> None:
    item = SyncItem(
        local_path=tmp_path / "out" / "f.dbn.zst",
        s3_key="raw/f.dbn.zst",
        size_bytes=3,
        op="transfer",
    )
    outcome, label, _moved = download_one(
        _RetryableUploadClient(),  # type: ignore[arg-type]
        item,
        in_flight=None,
        verify_sha256=False,
        fsync_writes=False,
    )
    assert outcome == "failed"
    assert "retryable error" in label


def test_download_one_verifies_sha256_and_rejects_mismatch(
    s3_bucket: None,
    tmp_path: Path,
) -> None:
    boto3.client("s3", region_name=_REGION).put_object(
        Bucket=_BUCKET,
        Key="raw/f.dbn.zst",
        Body=b"abc",
    )
    dest = tmp_path / "out" / "f.dbn.zst"
    item = SyncItem(
        local_path=dest,
        s3_key="raw/f.dbn.zst",
        size_bytes=3,
        op="transfer",
        sha256="0" * 64,  # wrong
    )
    dest.parent.mkdir(parents=True)
    client = S3Client(_BUCKET, region=_REGION)
    outcome, label, _moved = download_one(
        client,
        item,
        in_flight=None,
        verify_sha256=True,
        fsync_writes=False,
    )
    assert outcome == "failed"
    assert "sha256 mismatch" in label
    assert not dest.exists()  # tmp cleaned, no destination written


def test_download_one_verifies_sha256_and_accepts_match(
    s3_bucket: None,
    tmp_path: Path,
) -> None:
    boto3.client("s3", region_name=_REGION).put_object(
        Bucket=_BUCKET,
        Key="raw/f.dbn.zst",
        Body=b"abc",
    )
    digest = "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
    dest = tmp_path / "out" / "f.dbn.zst"
    item = SyncItem(
        local_path=dest,
        s3_key="raw/f.dbn.zst",
        size_bytes=3,
        op="transfer",
        sha256=digest,
    )
    dest.parent.mkdir(parents=True)
    client = S3Client(_BUCKET, region=_REGION)
    outcome, _label, moved = download_one(
        client,
        item,
        in_flight=None,
        verify_sha256=True,
        fsync_writes=False,
    )
    assert outcome == "transferred"
    assert moved == 3
    assert dest.read_bytes() == b"abc"


def test_delete_one_local_path_removes_local_file(tmp_path: Path) -> None:
    target = tmp_path / "f.dbn.zst"
    target.write_bytes(b"x")
    item = SyncItem(
        local_path=target,
        s3_key="raw/f.dbn.zst",
        size_bytes=1,
        op="delete",
    )
    outcome, _label, _moved = delete_one(
        _RetryableUploadClient(),  # type: ignore[arg-type]
        item,
        in_flight=None,
        delete_remote=False,
    )
    assert outcome == "deleted"
    assert not target.exists()


def test_delete_one_remote_path_invokes_client(s3_bucket: None) -> None:
    boto3.client("s3", region_name=_REGION).put_object(
        Bucket=_BUCKET,
        Key="raw/orphan.dbn.zst",
        Body=b"x",
    )
    client = S3Client(_BUCKET, region=_REGION)
    item = SyncItem(
        local_path=None,
        s3_key="raw/orphan.dbn.zst",
        size_bytes=1,
        op="delete",
    )
    outcome, _label, _moved = delete_one(
        client,
        item,
        in_flight=None,
        delete_remote=True,
    )
    assert outcome == "deleted"
    assert client.head_object("raw/orphan.dbn.zst") is None


def test_delete_one_returns_failed_on_retryable_remote(tmp_path: Path) -> None:
    item = SyncItem(
        local_path=None,
        s3_key="raw/orphan.dbn.zst",
        size_bytes=1,
        op="delete",
    )
    outcome, label, _moved = delete_one(
        _RetryableUploadClient(),  # type: ignore[arg-type]
        item,
        in_flight=None,
        delete_remote=True,
    )
    assert outcome == "failed"
    assert "retryable error" in label


# -------------------------------------------------------------- s3_client.py


class _FailingClient:
    def __init__(self, exc: BaseException) -> None:
        self._exc = exc

    def upload_file(self, *_a: Any, **_kw: Any) -> None:
        raise self._exc

    def download_file(self, *_a: Any, **_kw: Any) -> None:
        raise self._exc

    def head_object(self, **_kw: Any) -> Any:
        raise self._exc

    def delete_object(self, **_kw: Any) -> Any:
        raise self._exc

    def get_paginator(self, *_a: Any, **_kw: Any) -> Any:
        raise self._exc


def _make(client_exc: BaseException) -> S3Client:
    return S3Client(_BUCKET, region=_REGION, client=_FailingClient(client_exc))  # type: ignore[arg-type]


@pytest.mark.parametrize(
    "exc",
    [
        EndpointConnectionError(endpoint_url="https://x"),
        ConnectTimeoutError(endpoint_url="https://x"),
        ReadTimeoutError(endpoint_url="https://x"),
    ],
)
def test_upload_translates_transport_errors_to_retryable(
    tmp_path: Path,
    exc: BaseException,
) -> None:
    src = tmp_path / "f"
    src.write_bytes(b"x")
    client = _make(exc)
    with pytest.raises(RetryableError):
        client.upload_file(src, "k")


def test_upload_translates_no_credentials(tmp_path: Path) -> None:
    src = tmp_path / "f"
    src.write_bytes(b"x")
    client = _make(NoCredentialsError())
    with pytest.raises(FatalConfigError):
        client.upload_file(src, "k")


def test_download_translates_no_credentials(tmp_path: Path) -> None:
    client = _make(NoCredentialsError())
    with pytest.raises(FatalConfigError):
        client.download_file("k", tmp_path / "out")


def test_head_object_translates_transport_error_to_retryable() -> None:
    client = _make(EndpointConnectionError(endpoint_url="https://x"))
    with pytest.raises(RetryableError):
        client.head_object("k")


def test_delete_translates_transport_error_to_retryable() -> None:
    client = _make(EndpointConnectionError(endpoint_url="https://x"))
    with pytest.raises(RetryableError):
        client.delete_object("k")


def test_list_objects_translates_no_credentials() -> None:
    client = _make(NoCredentialsError())
    with pytest.raises(FatalConfigError):
        list(client.list_objects("p"))


def test_head_object_returns_none_on_404() -> None:
    err = ClientError(
        error_response={  # pyright: ignore[reportArgumentType]
            "Error": {"Code": "NoSuchKey", "Message": "x"},
            "ResponseMetadata": {"HTTPStatusCode": 404},
        },
        operation_name="HeadObject",
    )
    client = _make(err)
    assert client.head_object("k") is None


# -------------------------------------------------------------- lifecycle.py


def test_run_sync_returns_zero_when_nothing_to_do(
    s3_bucket: None,
    tmp_path: Path,
) -> None:
    config = SyncConfig(
        direction=SyncDirection.PULL,
        data_dir=tmp_path / "data",
        bucket=_BUCKET,
        region=_REGION,
        mode=RunMode.EXECUTE,
        yes=True,
    )
    client = S3Client(_BUCKET, region=_REGION)
    assert sync_lifecycle.run_sync(config, client=client) == 0


def test_run_sync_rejects_concurrent_archive_lock(
    s3_bucket: None,
    tmp_path: Path,
) -> None:
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    config = SyncConfig(
        direction=SyncDirection.PULL,
        data_dir=data_dir,
        bucket=_BUCKET,
        region=_REGION,
        mode=RunMode.EXECUTE,
        yes=True,
    )
    client = S3Client(_BUCKET, region=_REGION)

    with (
        _exclusive_run_lock(
            data_dir,
            "download-run",
            Console(file=io.StringIO(), force_terminal=False),
            fsync_writes=False,
        ),
        pytest.raises(SystemExit) as exc_info,
    ):
        sync_lifecycle.run_sync(config, client=client)

    assert exc_info.value.code == 2


def test_run_sync_dry_run_returns_zero_without_transfer(
    s3_bucket: None,
    tmp_path: Path,
) -> None:
    archive = tmp_path / "data" / "raw"
    archive.mkdir(parents=True)
    (archive / "f.dbn.zst").write_bytes(b"x")
    config = SyncConfig(
        direction=SyncDirection.PUSH,
        data_dir=tmp_path / "data",
        bucket=_BUCKET,
        region=_REGION,
        mode=RunMode.DRY_RUN,
        yes=True,
    )
    client = S3Client(_BUCKET, region=_REGION)
    assert sync_lifecycle.run_sync(config, client=client) == 0
    # nothing landed on S3
    assert list(client.list_objects("")) == []


def test_run_sync_aborts_when_user_says_no(
    s3_bucket: None,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    archive = tmp_path / "data" / "raw"
    archive.mkdir(parents=True)
    (archive / "f.dbn.zst").write_bytes(b"x")
    config = SyncConfig(
        direction=SyncDirection.PUSH,
        data_dir=tmp_path / "data",
        bucket=_BUCKET,
        region=_REGION,
        mode=RunMode.EXECUTE,
    )
    monkeypatch.setattr("sys.stdin", io.StringIO("n\n"))
    # Make stdin look interactive.
    monkeypatch.setattr("sys.stdin.isatty", lambda: True, raising=False)
    console = Console(file=io.StringIO(), force_terminal=False)
    error_console = Console(file=io.StringIO(), force_terminal=False)
    client = S3Client(_BUCKET, region=_REGION)

    # Inject answer via Console.input by monkeypatching.
    monkeypatch.setattr(Console, "input", lambda self, prompt="": "n")
    code = sync_lifecycle.run_sync(
        config,
        client=client,
        console=console,
        error_console=error_console,
    )
    assert code == 0
    assert list(client.list_objects("")) == []


def test_run_sync_proceeds_when_user_says_yes(
    s3_bucket: None,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    archive = tmp_path / "data" / "raw"
    archive.mkdir(parents=True)
    (archive / "f.dbn.zst").write_bytes(b"x")
    config = SyncConfig(
        direction=SyncDirection.PUSH,
        data_dir=tmp_path / "data",
        bucket=_BUCKET,
        region=_REGION,
        mode=RunMode.EXECUTE,
    )
    monkeypatch.setattr("sys.stdin.isatty", lambda: True, raising=False)
    monkeypatch.setattr(Console, "input", lambda self, prompt="": "y")
    console = Console(file=io.StringIO(), force_terminal=False, quiet=True)
    error_console = Console(file=io.StringIO(), force_terminal=False)
    client = S3Client(_BUCKET, region=_REGION)
    code = sync_lifecycle.run_sync(
        config,
        client=client,
        console=console,
        error_console=error_console,
    )
    assert code == 0
    assert client.head_object("raw/f.dbn.zst") is not None


def test_run_sync_refuses_non_interactive_without_yes(
    s3_bucket: None,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    archive = tmp_path / "data" / "raw"
    archive.mkdir(parents=True)
    (archive / "f.dbn.zst").write_bytes(b"x")
    config = SyncConfig(
        direction=SyncDirection.PUSH,
        data_dir=tmp_path / "data",
        bucket=_BUCKET,
        region=_REGION,
        mode=RunMode.EXECUTE,
    )
    monkeypatch.setattr("sys.stdin.isatty", lambda: False, raising=False)
    client = S3Client(_BUCKET, region=_REGION)
    code = sync_lifecycle.run_sync(config, client=client)
    assert code == 2


def test_run_sync_with_delete_requires_typed_confirmation(
    s3_bucket: None,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    archive = tmp_path / "data" / "raw"
    archive.mkdir(parents=True)
    (archive / "f.dbn.zst").write_bytes(b"x")
    boto3.client("s3", region_name=_REGION).put_object(
        Bucket=_BUCKET,
        Key="raw/orphan.dbn.zst",
        Body=b"o",
    )
    config = SyncConfig(
        direction=SyncDirection.PUSH,
        data_dir=tmp_path / "data",
        bucket=_BUCKET,
        region=_REGION,
        mode=RunMode.EXECUTE,
        delete=True,
    )
    monkeypatch.setattr("sys.stdin.isatty", lambda: True, raising=False)
    monkeypatch.setattr(Console, "input", lambda self, prompt="": "y")  # not "delete"
    client = S3Client(_BUCKET, region=_REGION)
    code = sync_lifecycle.run_sync(config, client=client)
    assert code == 0
    # Orphan still present because delete was not typed-confirmed.
    assert client.head_object("raw/orphan.dbn.zst") is not None


# ------------------------------------------------------------- sync_cli.py


def test_sync_cli_main_dry_run(
    s3_bucket: None,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    archive = tmp_path / "data" / "raw"
    archive.mkdir(parents=True)
    (archive / "f.dbn.zst").write_bytes(b"x")
    monkeypatch.setenv("DATABENTO_S3_BUCKET", _BUCKET)
    monkeypatch.setenv("DATABENTO_S3_REGION", _REGION)
    import sys

    monkeypatch.setattr(
        sys,
        "argv",
        [
            "databento-stream-sync",
            "push",
            "--data-dir",
            str(tmp_path / "data"),
            "--dry-run",
            "--yes",
        ],
    )
    from databento_stream_downloader import sync_cli

    sync_cli.main()


def _argv_for_push(tmp_path: Path) -> list[str]:
    return [
        "databento-stream-sync",
        "push",
        "--data-dir",
        str(tmp_path / "data"),
        "--bucket",
        _BUCKET,
        "--region",
        _REGION,
        "--yes",
    ]


@pytest.mark.parametrize(
    ("exc", "expected_code"),
    [
        (
            __import__(
                "databento_stream_downloader.errors",
                fromlist=["InterruptedDownloadError"],
            ).InterruptedDownloadError("interrupted"),
            130,
        ),
        (KeyboardInterrupt(), 143),
        (
            __import__(
                "databento_stream_downloader.errors",
                fromlist=["ShutdownRequestedError"],
            ).ShutdownRequestedError("term"),
            143,
        ),
        (
            __import__(
                "databento_stream_downloader.errors",
                fromlist=["FatalConfigError"],
            ).FatalConfigError("bad config"),
            2,
        ),
        (
            __import__(
                "databento_stream_downloader.errors",
                fromlist=["FatalAPIError"],
            ).FatalAPIError("api boom"),
            1,
        ),
        (
            __import__(
                "databento_stream_downloader.errors",
                fromlist=["FatalError"],
            ).FatalError("generic fatal"),
            1,
        ),
        (RuntimeError("boom"), 4),
    ],
)
def test_sync_cli_main_translates_exceptions_to_exit_codes(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    exc: BaseException,
    expected_code: int,
) -> None:
    (tmp_path / "data" / "raw").mkdir(parents=True)
    (tmp_path / "data" / "raw" / "f.dbn.zst").write_bytes(b"x")
    import sys

    monkeypatch.setattr(sys, "argv", _argv_for_push(tmp_path))

    from databento_stream_downloader import sync_cli

    def _boom(*_a: Any, **_kw: Any) -> int:
        raise exc

    monkeypatch.setattr(sync_cli, "run_sync", _boom)
    with pytest.raises(SystemExit) as exc_info:
        sync_cli.main()
    assert exc_info.value.code == expected_code


def test_sync_cli_main_propagates_non_zero_run_sync_exit_code(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    (tmp_path / "data" / "raw").mkdir(parents=True)
    (tmp_path / "data" / "raw" / "f.dbn.zst").write_bytes(b"x")
    import sys

    monkeypatch.setattr(sys, "argv", _argv_for_push(tmp_path))

    from databento_stream_downloader import sync_cli

    monkeypatch.setattr(sync_cli, "run_sync", lambda *a, **kw: 1)
    with pytest.raises(SystemExit) as exc_info:
        sync_cli.main()
    assert exc_info.value.code == 1


def test_sync_run_prints_failed_label_to_error_console(
    s3_bucket: None,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    archive = tmp_path / "data" / "raw"
    archive.mkdir(parents=True)
    (archive / "f.dbn.zst").write_bytes(b"x")

    from databento_stream_downloader._sync import stream as sync_stream

    def _failing_upload(*_a: Any, **_kw: Any) -> tuple[str, str, int]:
        return ("failed", "raw/f.dbn.zst: induced failure", 0)

    monkeypatch.setattr(sync_stream, "upload_one", _failing_upload)
    err_buf = io.StringIO()
    error_console = Console(file=err_buf, force_terminal=False)
    config = SyncConfig(
        direction=SyncDirection.PUSH,
        data_dir=tmp_path / "data",
        bucket=_BUCKET,
        region=_REGION,
        mode=RunMode.EXECUTE,
        yes=True,
    )
    code = sync_lifecycle.run_sync(
        config,
        client=S3Client(_BUCKET, region=_REGION),
        console=Console(file=io.StringIO(), force_terminal=False, quiet=True),
        error_console=error_console,
    )
    assert code == 1
    assert "induced failure" in err_buf.getvalue()


def test_sync_run_propagates_fatal_error_from_worker(
    s3_bucket: None,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    archive = tmp_path / "data" / "raw"
    archive.mkdir(parents=True)
    (archive / "f.dbn.zst").write_bytes(b"x")

    from databento_stream_downloader._sync import stream as sync_stream
    from databento_stream_downloader.errors import FatalError

    def _fatal_upload(*_a: Any, **_kw: Any) -> tuple[str, str, int]:
        raise FatalError("worker exploded")

    monkeypatch.setattr(sync_stream, "upload_one", _fatal_upload)
    config = SyncConfig(
        direction=SyncDirection.PUSH,
        data_dir=tmp_path / "data",
        bucket=_BUCKET,
        region=_REGION,
        mode=RunMode.EXECUTE,
        yes=True,
    )
    with pytest.raises(FatalError):
        sync_lifecycle.run_sync(
            config,
            client=S3Client(_BUCKET, region=_REGION),
            console=Console(file=io.StringIO(), force_terminal=False, quiet=True),
            error_console=Console(file=io.StringIO(), force_terminal=False),
        )


def test_sync_config_rejects_fsync_writes_for_push() -> None:
    with pytest.raises(Exception, match="fsync-writes only applies to pull"):
        SyncConfig(
            direction=SyncDirection.PUSH,
            data_dir=Path.cwd() / "data",
            bucket="b",
            mode=RunMode.EXECUTE,
            fsync_writes=True,
        )


def test_sync_config_allows_fsync_writes_for_pull() -> None:
    cfg = SyncConfig(
        direction=SyncDirection.PULL,
        data_dir=Path.cwd() / "data",
        bucket="b",
        mode=RunMode.EXECUTE,
        fsync_writes=True,
    )
    assert cfg.fsync_writes is True


def test_verify_sha256_pull_uses_head_metadata_planning() -> None:
    cfg = SyncConfig(
        direction=SyncDirection.PULL,
        data_dir=Path.cwd() / "data",
        bucket="b",
        mode=RunMode.EXECUTE,
        verify_sha256=True,
    )

    assert sync_lifecycle._effective_planning_mode(cfg) is PlanningMode.HEAD_METADATA


def test_sync_config_rejects_blank_bucket() -> None:
    with pytest.raises(Exception):  # noqa: B017,PT011
        SyncConfig(
            direction=SyncDirection.PUSH,
            data_dir=Path.cwd() / "data",
            bucket="   ",
            mode=RunMode.EXECUTE,
        )


def test_sync_config_rejects_dotdot_in_prefix() -> None:
    with pytest.raises(Exception):  # noqa: B017,PT011
        SyncConfig(
            direction=SyncDirection.PUSH,
            data_dir=Path.cwd() / "data",
            bucket="b",
            prefix="archives/../etc",
            mode=RunMode.EXECUTE,
        )


def test_s3_client_upload_translates_client_error_4xx_to_fatal_api(
    tmp_path: Path,
) -> None:
    err = ClientError(
        error_response={  # pyright: ignore[reportArgumentType]
            "Error": {"Code": "X", "Message": "y"},
            "ResponseMetadata": {"HTTPStatusCode": 400},
        },
        operation_name="Upload",
    )
    src = tmp_path / "f"
    src.write_bytes(b"x")
    client = _make(err)
    from databento_stream_downloader.errors import FatalAPIError

    with pytest.raises(FatalAPIError):
        client.upload_file(src, "k")


def test_s3_client_download_translates_client_error(tmp_path: Path) -> None:
    err = ClientError(
        error_response={  # pyright: ignore[reportArgumentType]
            "Error": {"Code": "X", "Message": "y"},
            "ResponseMetadata": {"HTTPStatusCode": 503},
        },
        operation_name="Download",
    )
    client = _make(err)
    with pytest.raises(RetryableError):
        client.download_file("k", tmp_path / "out")


def test_s3_client_head_object_translates_client_error_to_fatal_config() -> None:
    err = ClientError(
        error_response={  # pyright: ignore[reportArgumentType]
            "Error": {"Code": "X", "Message": "y"},
            "ResponseMetadata": {"HTTPStatusCode": 403},
        },
        operation_name="HeadObject",
    )
    client = _make(err)
    with pytest.raises(FatalConfigError):
        client.head_object("k")


def test_s3_client_delete_translates_client_error_to_retryable() -> None:
    err = ClientError(
        error_response={  # pyright: ignore[reportArgumentType]
            "Error": {"Code": "X", "Message": "y"},
            "ResponseMetadata": {"HTTPStatusCode": 500},
        },
        operation_name="DeleteObject",
    )
    client = _make(err)
    with pytest.raises(RetryableError):
        client.delete_object("k")


def test_sync_cli_main_high_worker_count_warning(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    (tmp_path / "data" / "raw").mkdir(parents=True)
    (tmp_path / "data" / "raw" / "f.dbn.zst").write_bytes(b"x")
    import sys

    monkeypatch.setattr(
        sys,
        "argv",
        [
            "databento-stream-sync",
            "push",
            "--data-dir",
            str(tmp_path / "data"),
            "--bucket",
            _BUCKET,
            "--region",
            _REGION,
            "--workers",
            "20",
            "--dry-run",
            "--yes",
        ],
    )
    from databento_stream_downloader import sync_cli

    monkeypatch.setattr(sync_cli, "run_sync", lambda *a, **kw: 0)
    sync_cli.main()
    captured = capsys.readouterr()
    assert "high worker count configured" in captured.err
