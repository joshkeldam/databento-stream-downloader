"""Local + remote inventory walks and plan computation."""

from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

import structlog

from databento_stream_downloader._runner.fsio import _sha256_file
from databento_stream_downloader._sync.types import (
    SyncDirection,
    SyncItem,
    SyncPlan,
)

if TYPE_CHECKING:
    from databento_stream_downloader.s3_client import S3Client

LOGGER = structlog.get_logger(__name__)

# Files we never sync — transient run state and OS noise.
_SKIP_PATTERNS = (
    re.compile(r"^\.run\.lock$"),
    re.compile(r"^\.[^/]*\.tmp$"),
    re.compile(r"^\.[^/]*\.tmp\..*$"),
    re.compile(r"^\.DS_Store$"),
    re.compile(r"^\.write_test\..*$"),
)


@dataclass(frozen=True, slots=True)
class _LocalEntry:
    path: Path
    key: str
    size: int
    sidecar: Path | None


@dataclass(frozen=True, slots=True)
class _RemoteEntry:
    key: str
    size: int


def _should_skip(name: str) -> bool:
    return any(pat.fullmatch(name) for pat in _SKIP_PATTERNS)


def walk_local(data_dir: Path) -> dict[str, _LocalEntry]:
    """Walk `data_dir` and return entries keyed by relpath.

    Skips lock, temp, and OS-noise files. SHA256 sidecars are tracked
    alongside their data files but are also synced themselves so a fresh
    pull restores the integrity proof.
    """
    out: dict[str, _LocalEntry] = {}
    if not data_dir.is_dir():
        return out
    for path in sorted(data_dir.rglob("*")):
        if not path.is_file():
            continue
        if _should_skip(path.name):
            continue
        try:
            size = path.stat().st_size
        except OSError:
            LOGGER.warning("local_stat_failed", path=str(path))
            continue
        key = path.relative_to(data_dir).as_posix()
        sidecar = path.with_suffix(path.suffix + ".sha256")
        out[key] = _LocalEntry(
            path=path,
            key=key,
            size=size,
            sidecar=sidecar if sidecar.is_file() else None,
        )
    return out


def list_remote(client: S3Client, prefix: str) -> dict[str, _RemoteEntry]:
    """List objects under `prefix` and return entries keyed by archive-relative path.

    The S3 prefix is stripped from each Key so the result aligns with the
    local relpaths returned by `walk_local`.
    """
    list_prefix = f"{prefix}/" if prefix else ""
    out: dict[str, _RemoteEntry] = {}
    for obj in client.list_objects(list_prefix):
        key = str(obj.get("Key", ""))
        if not key:
            continue
        if list_prefix and not key.startswith(list_prefix):
            continue
        relkey = key[len(list_prefix) :] if list_prefix else key
        size = int(obj.get("Size", 0) or 0)
        out[relkey] = _RemoteEntry(key=relkey, size=size)
    return out


def s3_key_for(local_relkey: str, prefix: str) -> str:
    """Compose the full S3 Key for an archive-relative path."""
    if not prefix:
        return local_relkey
    return f"{prefix}/{local_relkey}"


def compute_plan(
    direction: SyncDirection,
    local: dict[str, _LocalEntry],
    remote: dict[str, _RemoteEntry],
    *,
    data_dir: Path,
    prefix: str,
    delete: bool,
) -> SyncPlan:
    """Compare inventories and produce a SyncPlan for the given direction.

    Difference detection uses size match. SHA256 verification is opt-in
    elsewhere (the transfer step rechecks against the local sidecar /
    remote Metadata.sha256 when --verify-sha256 is set).
    """
    transfers: list[SyncItem] = []
    deletes: list[SyncItem] = []
    extraneous: list[str] = []
    skipped = 0
    total_bytes = 0

    if direction is SyncDirection.PUSH:
        # Source = local; Destination = remote
        for key, lentry in local.items():
            rentry = remote.get(key)
            if rentry is None or rentry.size != lentry.size:
                transfers.append(_make_transfer_item(lentry, prefix))
                total_bytes += lentry.size
            else:
                skipped += 1
        for key in remote.keys() - local.keys():
            if delete:
                deletes.append(
                    SyncItem(
                        local_path=data_dir / key,
                        s3_key=s3_key_for(key, prefix),
                        size_bytes=remote[key].size,
                        op="delete",
                    ),
                )
            else:
                extraneous.append(key)
    else:
        # PULL: source = remote; destination = local
        for key, rentry in remote.items():
            lentry = local.get(key)
            if lentry is None or lentry.size != rentry.size:
                transfers.append(
                    SyncItem(
                        local_path=data_dir / key,
                        s3_key=s3_key_for(key, prefix),
                        size_bytes=rentry.size,
                        op="transfer",
                    ),
                )
                total_bytes += rentry.size
            else:
                skipped += 1
        for key in local.keys() - remote.keys():
            if delete:
                deletes.append(
                    SyncItem(
                        local_path=local[key].path,
                        s3_key=s3_key_for(key, prefix),
                        size_bytes=local[key].size,
                        op="delete",
                    ),
                )
            else:
                extraneous.append(key)

    return SyncPlan(
        transfers=tuple(transfers),
        deletes=tuple(deletes),
        skipped=skipped,
        extraneous=tuple(sorted(extraneous)),
        total_transfer_bytes=total_bytes,
    )


def _make_transfer_item(lentry: _LocalEntry, prefix: str) -> SyncItem:
    sha = _read_sidecar_digest(lentry.sidecar) if lentry.sidecar is not None else None
    return SyncItem(
        local_path=lentry.path,
        s3_key=s3_key_for(lentry.key, prefix),
        size_bytes=lentry.size,
        op="transfer",
        sha256=sha,
    )


def _read_sidecar_digest(sidecar: Path) -> str | None:
    """Best-effort read of `digest  filename\\n` sidecar; None on any error."""
    try:
        digest, _ = sidecar.read_text(encoding="ascii").split(maxsplit=1)
    except OSError, UnicodeDecodeError, ValueError:
        return None
    if len(digest) != 64 or any(ch not in "0123456789abcdef" for ch in digest):
        return None
    return digest


def compute_local_sha256(path: Path) -> str:
    """Compute SHA256 over a local file (used by --verify-sha256 in PULL)."""
    return _sha256_file(path)


__all__ = [
    "_LocalEntry",
    "_RemoteEntry",
    "_should_skip",
    "compute_local_sha256",
    "compute_plan",
    "list_remote",
    "s3_key_for",
    "walk_local",
]
