"""Tests for the sync inventory walk and plan computation."""
# pyright: reportPrivateUsage=false

from __future__ import annotations

from pathlib import Path
from typing import cast

import pytest

from databento_stream_downloader._sync.inventory import (
    _LocalEntry,
    _RemoteEntry,
    _should_skip,
    compute_plan,
    list_remote,
    s3_key_for,
    walk_local,
)
from databento_stream_downloader._sync.types import PlanningMode, SyncDirection
from databento_stream_downloader.coverage_manifest import MANIFEST_FILENAME
from databento_stream_downloader.s3_client import S3Client


def test_should_skip_excludes_transient_and_lock_files() -> None:
    assert _should_skip(".run.lock") is True
    assert _should_skip(".tmp") is False  # only when prefixed and ending .tmp
    assert _should_skip(".foo.tmp") is True
    assert _should_skip(".foo.tmp.partial") is True
    assert _should_skip(".DS_Store") is True
    assert _should_skip(".write_test.abc") is True
    assert _should_skip("archive-manifest.jsonl") is True
    assert _should_skip(MANIFEST_FILENAME) is True
    assert _should_skip("download-ledger.jsonl") is False
    assert _should_skip("2026-04-01.dbn.zst") is False
    assert _should_skip("2026-04-01.dbn.zst.sha256") is False


def test_walk_local_skips_transients_and_collects_sidecars(tmp_path: Path) -> None:
    archive = tmp_path / "data" / "raw" / "glbx-mdp3" / "ES.FUT" / "mbo"
    archive.mkdir(parents=True)
    (archive / "2026-04-01.dbn.zst").write_bytes(b"abc")
    (archive / "2026-04-01.dbn.zst.sha256").write_text(
        "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad  "
        "2026-04-01.dbn.zst\n",
        encoding="ascii",
    )
    (archive / ".lurking.tmp").write_bytes(b"junk")
    (tmp_path / "data" / ".run.lock").write_text("{}\n", encoding="utf-8")
    (tmp_path / "data" / "download-ledger.jsonl").write_text(
        '{"x": 1}\n',
        encoding="utf-8",
    )
    (tmp_path / "data" / "archive-manifest.jsonl").write_text(
        '{"x": 1}\n',
        encoding="utf-8",
    )
    (tmp_path / "data" / MANIFEST_FILENAME).write_text(
        '{"x": 1}\n',
        encoding="utf-8",
    )

    entries = walk_local(tmp_path / "data")

    keys = sorted(entries.keys())
    assert "raw/glbx-mdp3/ES.FUT/mbo/2026-04-01.dbn.zst" in keys
    assert "raw/glbx-mdp3/ES.FUT/mbo/2026-04-01.dbn.zst.sha256" in keys
    assert "download-ledger.jsonl" in keys
    assert "archive-manifest.jsonl" not in keys
    assert MANIFEST_FILENAME not in keys
    assert all("tmp" not in key for key in keys)
    assert all("run.lock" not in key for key in keys)

    data_entry = entries["raw/glbx-mdp3/ES.FUT/mbo/2026-04-01.dbn.zst"]
    assert data_entry.size == 3
    assert data_entry.sidecar is not None
    assert data_entry.sidecar.name == "2026-04-01.dbn.zst.sha256"


def test_s3_key_for_handles_empty_prefix() -> None:
    assert s3_key_for("raw/foo/bar.dbn.zst", "") == "raw/foo/bar.dbn.zst"
    assert s3_key_for("raw/foo/bar.dbn.zst", "archives") == (
        "archives/raw/foo/bar.dbn.zst"
    )


@pytest.fixture
def sample_local(tmp_path: Path) -> dict[str, _LocalEntry]:
    a = tmp_path / "data" / "raw" / "a.dbn.zst"
    b = tmp_path / "data" / "raw" / "b.dbn.zst"
    a.parent.mkdir(parents=True)
    a.write_bytes(b"a" * 100)
    b.write_bytes(b"b" * 200)
    return {
        "raw/a.dbn.zst": _LocalEntry(
            path=a,
            key="raw/a.dbn.zst",
            size=100,
            sidecar=None,
        ),
        "raw/b.dbn.zst": _LocalEntry(
            path=b,
            key="raw/b.dbn.zst",
            size=200,
            sidecar=None,
        ),
    }


def test_compute_plan_push_uploads_missing_and_size_mismatched(
    tmp_path: Path,
    sample_local: dict[str, _LocalEntry],
) -> None:
    remote = {
        "raw/a.dbn.zst": _RemoteEntry(key="raw/a.dbn.zst", size=100),  # match
        "raw/b.dbn.zst": _RemoteEntry(key="raw/b.dbn.zst", size=999),  # size diff
        "raw/c.dbn.zst": _RemoteEntry(key="raw/c.dbn.zst", size=50),  # extraneous
    }

    plan = compute_plan(
        SyncDirection.PUSH,
        sample_local,
        remote,
        data_dir=tmp_path / "data",
        prefix="",
        delete=False,
    )

    assert plan.skipped == 1
    assert {item.s3_key for item in plan.transfers} == {"raw/b.dbn.zst"}
    assert plan.deletes == ()
    assert plan.extraneous == ("raw/c.dbn.zst",)
    assert plan.total_transfer_bytes == 200


def test_compute_plan_size_mode_skips_same_size_digest_mismatch(
    tmp_path: Path,
) -> None:
    path = tmp_path / "data" / "raw" / "same.dbn.zst"
    local = {
        "raw/same.dbn.zst": _LocalEntry(
            path=path,
            key="raw/same.dbn.zst",
            size=3,
            sidecar=None,
            sha256="a" * 64,
        )
    }
    remote = {
        "raw/same.dbn.zst": _RemoteEntry(
            key="raw/same.dbn.zst",
            size=3,
            sha256="b" * 64,
        )
    }

    plan = compute_plan(
        SyncDirection.PUSH,
        local,
        remote,
        data_dir=tmp_path / "data",
        prefix="",
        delete=False,
    )

    assert plan.transfers == ()
    assert plan.skipped == 1


def test_compute_plan_sidecar_mode_transfers_same_size_digest_mismatch(
    tmp_path: Path,
) -> None:
    path = tmp_path / "data" / "raw" / "same.dbn.zst"
    local = {
        "raw/same.dbn.zst": _LocalEntry(
            path=path,
            key="raw/same.dbn.zst",
            size=3,
            sidecar=None,
            sha256="a" * 64,
        )
    }
    remote = {
        "raw/same.dbn.zst": _RemoteEntry(
            key="raw/same.dbn.zst",
            size=3,
            sha256="b" * 64,
        )
    }

    plan = compute_plan(
        SyncDirection.PUSH,
        local,
        remote,
        data_dir=tmp_path / "data",
        prefix="",
        delete=False,
        planning_mode=PlanningMode.SIDECAR,
    )

    assert {item.s3_key for item in plan.transfers} == {"raw/same.dbn.zst"}
    assert plan.skipped == 0


def test_compute_plan_digest_mismatch_transfers_paired_sidecar(
    tmp_path: Path,
) -> None:
    data = tmp_path / "data" / "raw" / "same.dbn.zst"
    sidecar = tmp_path / "data" / "raw" / "same.dbn.zst.sha256"
    local = {
        "raw/same.dbn.zst": _LocalEntry(
            path=data,
            key="raw/same.dbn.zst",
            size=3,
            sidecar=sidecar,
            sha256="a" * 64,
        ),
        "raw/same.dbn.zst.sha256": _LocalEntry(
            path=sidecar,
            key="raw/same.dbn.zst.sha256",
            size=80,
            sidecar=None,
        ),
    }
    remote = {
        "raw/same.dbn.zst": _RemoteEntry(
            key="raw/same.dbn.zst",
            size=3,
            sha256="b" * 64,
        ),
        "raw/same.dbn.zst.sha256": _RemoteEntry(
            key="raw/same.dbn.zst.sha256",
            size=80,
        ),
    }

    plan = compute_plan(
        SyncDirection.PUSH,
        local,
        remote,
        data_dir=tmp_path / "data",
        prefix="",
        delete=False,
        planning_mode=PlanningMode.SIDECAR,
    )

    assert {item.s3_key for item in plan.transfers} == {
        "raw/same.dbn.zst",
        "raw/same.dbn.zst.sha256",
    }


def test_compute_plan_pull_carries_remote_digest_for_verification(
    tmp_path: Path,
) -> None:
    key = "raw/same.dbn.zst"
    local = {
        key: _LocalEntry(
            path=tmp_path / "data" / key,
            key=key,
            size=3,
            sidecar=None,
            sha256="a" * 64,
        )
    }
    remote = {key: _RemoteEntry(key=key, size=3, sha256="b" * 64)}

    plan = compute_plan(
        SyncDirection.PULL,
        local,
        remote,
        data_dir=tmp_path / "data",
        prefix="",
        delete=False,
        planning_mode=PlanningMode.HEAD_METADATA,
    )

    assert len(plan.transfers) == 1
    assert plan.transfers[0].sha256 == "b" * 64


def test_compute_plan_push_with_delete_promotes_extraneous(
    tmp_path: Path,
    sample_local: dict[str, _LocalEntry],
) -> None:
    remote = {
        "raw/c.dbn.zst": _RemoteEntry(key="raw/c.dbn.zst", size=50),
    }

    plan = compute_plan(
        SyncDirection.PUSH,
        sample_local,
        remote,
        data_dir=tmp_path / "data",
        prefix="",
        delete=True,
    )

    assert plan.extraneous == ()
    assert {item.s3_key for item in plan.deletes} == {"raw/c.dbn.zst"}
    assert all(item.local_path is None for item in plan.deletes)
    assert {item.s3_key for item in plan.transfers} == {
        "raw/a.dbn.zst",
        "raw/b.dbn.zst",
    }


def test_compute_plan_pull_inverse_of_push(
    tmp_path: Path,
    sample_local: dict[str, _LocalEntry],
) -> None:
    remote = {
        "raw/a.dbn.zst": _RemoteEntry(key="raw/a.dbn.zst", size=100),
        "raw/b.dbn.zst": _RemoteEntry(key="raw/b.dbn.zst", size=200),
        "raw/c.dbn.zst": _RemoteEntry(key="raw/c.dbn.zst", size=50),  # only on s3
    }
    plan = compute_plan(
        SyncDirection.PULL,
        sample_local,
        remote,
        data_dir=tmp_path / "data",
        prefix="",
        delete=False,
    )

    assert plan.skipped == 2
    assert {item.s3_key for item in plan.transfers} == {"raw/c.dbn.zst"}
    assert plan.deletes == ()
    assert plan.extraneous == ()  # no local-only items
    assert plan.total_transfer_bytes == 50
    assert next(iter(plan.transfers)).local_path == (
        tmp_path / "data" / "raw" / "c.dbn.zst"
    ).resolve(strict=False)


@pytest.mark.parametrize(
    "key",
    [
        "../escaped.dbn.zst",
        r"..\escaped.dbn.zst",
        "/etc/passwd",
        r"C:\escaped.dbn.zst",
        "raw/bad\x00key.dbn.zst",
    ],
)
def test_compute_plan_pull_rejects_unsafe_remote_relkeys(
    tmp_path: Path,
    key: str,
) -> None:
    remote = {key: _RemoteEntry(key=key, size=1)}

    with pytest.raises(ValueError, match="remote S3 key"):
        compute_plan(
            SyncDirection.PULL,
            {},
            remote,
            data_dir=tmp_path / "data",
            prefix="",
            delete=False,
        )


def test_compute_plan_pull_rejects_resolved_escape(tmp_path: Path) -> None:
    key = "raw/link/escaped.dbn.zst"
    data_dir = tmp_path / "data"
    link = data_dir / "raw" / "link"
    link.parent.mkdir(parents=True)
    link.symlink_to(tmp_path)

    with pytest.raises(ValueError, match="escapes data directory"):
        compute_plan(
            SyncDirection.PULL,
            {},
            {key: _RemoteEntry(key=key, size=1)},
            data_dir=data_dir,
            prefix="",
            delete=False,
        )


class _FakeS3Client:
    def __init__(self, objects: list[dict[str, object]]) -> None:
        self._objects = objects

    def list_objects(self, prefix: str) -> list[dict[str, object]]:
        assert prefix == "archive/"
        return self._objects

    def read_object_bytes(self, key: str, *, max_bytes: int) -> bytes:
        assert key == "archive/raw/f.dbn.zst.sha256"
        assert max_bytes > 0
        return (("c" * 64) + "  f.dbn.zst\n").encode("ascii")

    def head_object(self, key: str) -> dict[str, object]:
        assert key == "archive/raw/f.dbn.zst"
        return {"Metadata": {"sha256": "d" * 64}}


def test_list_remote_rejects_unsafe_s3_keys(tmp_path: Path) -> None:
    client = _FakeS3Client(
        [{"Key": "archive/../../../tmp/escaped.txt", "Size": 1}],
    )

    with pytest.raises(ValueError, match="remote S3 key"):
        list_remote(cast("S3Client", client), "archive", data_dir=tmp_path / "data")


def test_list_remote_sidecar_mode_reads_remote_sidecar_digest(tmp_path: Path) -> None:
    client = _FakeS3Client(
        [
            {"Key": "archive/raw/f.dbn.zst", "Size": 3},
            {"Key": "archive/raw/f.dbn.zst.sha256", "Size": 80},
        ],
    )

    remote = list_remote(
        cast("S3Client", client),
        "archive",
        data_dir=tmp_path / "data",
        planning_mode=PlanningMode.SIDECAR,
    )

    assert remote["raw/f.dbn.zst"].sha256 == "c" * 64


def test_list_remote_head_metadata_mode_reads_remote_digest(tmp_path: Path) -> None:
    client = _FakeS3Client([{"Key": "archive/raw/f.dbn.zst", "Size": 3}])

    remote = list_remote(
        cast("S3Client", client),
        "archive",
        data_dir=tmp_path / "data",
        planning_mode=PlanningMode.HEAD_METADATA,
    )

    assert remote["raw/f.dbn.zst"].sha256 == "d" * 64


def test_compute_plan_pull_with_delete_removes_local_only(
    tmp_path: Path,
    sample_local: dict[str, _LocalEntry],
) -> None:
    # Remote has only "a"; local has "a" and "b". With --delete, "b" is local-only.
    remote = {
        "raw/a.dbn.zst": _RemoteEntry(key="raw/a.dbn.zst", size=100),
    }
    plan = compute_plan(
        SyncDirection.PULL,
        sample_local,
        remote,
        data_dir=tmp_path / "data",
        prefix="",
        delete=True,
    )
    assert {item.s3_key for item in plan.deletes} == {"raw/b.dbn.zst"}


def test_compute_plan_prefix_is_prepended_to_keys(
    tmp_path: Path,
    sample_local: dict[str, _LocalEntry],
) -> None:
    plan = compute_plan(
        SyncDirection.PUSH,
        sample_local,
        {},
        data_dir=tmp_path / "data",
        prefix="archives/databento",
        delete=False,
    )
    assert {item.s3_key for item in plan.transfers} == {
        "archives/databento/raw/a.dbn.zst",
        "archives/databento/raw/b.dbn.zst",
    }


def test_walk_local_returns_empty_for_missing_dir(tmp_path: Path) -> None:
    from databento_stream_downloader._sync.inventory import walk_local

    assert walk_local(tmp_path / "does-not-exist") == {}


def test_compute_plan_attaches_sidecar_digest_to_push_items(tmp_path: Path) -> None:
    from databento_stream_downloader._sync.inventory import (
        _read_sidecar_digest,
        walk_local,
    )

    archive = tmp_path / "data" / "raw"
    archive.mkdir(parents=True)
    (archive / "f.dbn.zst").write_bytes(b"abc")
    digest = "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
    (archive / "f.dbn.zst.sha256").write_text(
        f"{digest}  f.dbn.zst\n",
        encoding="ascii",
    )

    local = walk_local(tmp_path / "data")
    plan = compute_plan(
        SyncDirection.PUSH,
        local,
        {},
        data_dir=tmp_path / "data",
        prefix="",
        delete=False,
    )
    transfer_items = {item.s3_key: item for item in plan.transfers}
    assert transfer_items["raw/f.dbn.zst"].sha256 == digest

    # malformed sidecars yield None without crashing
    bad = tmp_path / "bad.sha256"
    bad.write_text("not-a-valid-sidecar\n", encoding="ascii")
    assert _read_sidecar_digest(bad) is None
    bad.write_text(("z" * 64) + "  f\n", encoding="ascii")
    assert _read_sidecar_digest(bad) is None  # non-hex
    bad.write_text(("a" * 32) + "  f\n", encoding="ascii")
    assert _read_sidecar_digest(bad) is None  # too short
    assert _read_sidecar_digest(tmp_path / "missing.sha256") is None  # missing file
