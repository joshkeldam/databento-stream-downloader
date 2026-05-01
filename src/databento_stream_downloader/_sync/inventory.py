"""Local + remote inventory walks and plan computation."""

from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any, cast

import structlog

from databento_stream_downloader._runner.fsio import _sha256_file
from databento_stream_downloader._sync.types import (
    PlanningMode,
    SyncDirection,
    SyncItem,
    SyncPlan,
)
from databento_stream_downloader.coverage_manifest import MANIFEST_FILENAME

if TYPE_CHECKING:
    from databento_stream_downloader.s3_client import S3Client

LOGGER = structlog.get_logger(__name__)

# Files we never sync — transient run state and OS noise.
_SKIP_PATTERNS = (
    re.compile(r"^\.run\.lock$"),
    re.compile(r"^archive-manifest\.jsonl$"),
    re.compile(rf"^{re.escape(MANIFEST_FILENAME)}$"),
    re.compile(r"^\.[^/]*\.tmp$"),
    re.compile(r"^\.[^/]*\.tmp\..*$"),
    re.compile(r"^\.DS_Store$"),
    re.compile(r"^\.write_test\..*$"),
)
_WINDOWS_DRIVE_RE = re.compile(r"(?:^|[\\/])[A-Za-z]:")
_REMOTE_SIDECAR_MAX_BYTES = 512


@dataclass(frozen=True, slots=True)
class _LocalEntry:
    path: Path
    key: str
    size: int
    sidecar: Path | None
    sha256: str | None = None


@dataclass(frozen=True, slots=True)
class _RemoteEntry:
    key: str
    size: int
    sha256: str | None = None


def _should_skip(name: str) -> bool:
    return any(pat.fullmatch(name) for pat in _SKIP_PATTERNS)


def _validate_remote_relkey(relkey: str, data_dir: Path) -> Path:
    if not relkey:
        msg = "remote S3 key resolves to an empty archive path"
        raise ValueError(msg)
    if "\x00" in relkey:
        msg = f"remote S3 key contains NUL byte: {relkey!r}"
        raise ValueError(msg)
    if relkey.startswith("/"):
        msg = f"remote S3 key must be relative: {relkey!r}"
        raise ValueError(msg)
    if _WINDOWS_DRIVE_RE.search(relkey):
        msg = f"remote S3 key contains a Windows drive prefix: {relkey!r}"
        raise ValueError(msg)
    if "\\" in relkey:
        msg = f"remote S3 key contains a Windows path separator: {relkey!r}"
        raise ValueError(msg)
    if ".." in relkey:
        msg = f"remote S3 key contains a parent directory segment: {relkey!r}"
        raise ValueError(msg)

    root = data_dir.resolve(strict=False)
    path = (data_dir / relkey).resolve(strict=False)
    if not path.is_relative_to(root):
        msg = f"remote S3 key escapes data directory: {relkey!r}"
        raise ValueError(msg)
    return path


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
        has_sidecar = sidecar.is_file()
        out[key] = _LocalEntry(
            path=path,
            key=key,
            size=size,
            sidecar=sidecar if has_sidecar else None,
            sha256=_read_sidecar_digest(sidecar) if has_sidecar else None,
        )
    return out


def list_remote(
    client: S3Client,
    prefix: str,
    *,
    data_dir: Path,
    planning_mode: PlanningMode = PlanningMode.SIZE,
) -> dict[str, _RemoteEntry]:
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
        if _should_skip(Path(relkey).name):
            continue
        _validate_remote_relkey(relkey, data_dir)
        size = int(obj.get("Size", 0) or 0)
        out[relkey] = _RemoteEntry(key=relkey, size=size)
    if planning_mode is PlanningMode.SIDECAR:
        _attach_remote_sidecar_digests(client, out, prefix)
    elif planning_mode is PlanningMode.HEAD_METADATA:
        _attach_remote_metadata_digests(client, out, prefix)
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
    planning_mode: PlanningMode = PlanningMode.SIZE,
) -> SyncPlan:
    """Compare inventories and produce a SyncPlan for the given direction.

    Difference detection always checks size. Digest-aware planning modes also
    compare SHA256 values when both inventory sides expose them.
    """
    transfers: list[SyncItem] = []
    deletes: list[SyncItem] = []
    extraneous: list[str] = []
    transfer_keys: set[str] = set()
    skipped = 0
    total_bytes = 0

    def append_transfer(key: str, item: SyncItem) -> None:
        nonlocal total_bytes
        if key in transfer_keys:
            return
        transfers.append(item)
        transfer_keys.add(key)
        total_bytes += item.size_bytes

    if direction is SyncDirection.PUSH:
        # Source = local; Destination = remote
        for key, lentry in local.items():
            if key in transfer_keys:
                continue
            rentry = remote.get(key)
            if rentry is None or not _entries_match(lentry, rentry, planning_mode):
                append_transfer(key, _make_transfer_item(lentry, prefix))
                sidecar_key = f"{key}.sha256"
                sidecar_entry = local.get(sidecar_key)
                if sidecar_entry is not None:
                    append_transfer(
                        sidecar_key,
                        _make_transfer_item(sidecar_entry, prefix),
                    )
            else:
                skipped += 1
        for key in remote.keys() - local.keys():
            if delete:
                deletes.append(
                    SyncItem(
                        local_path=None,
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
            if key in transfer_keys:
                continue
            local_path = _validate_remote_relkey(key, data_dir)
            lentry = local.get(key)
            if lentry is None or not _entries_match(lentry, rentry, planning_mode):
                append_transfer(
                    key,
                    _make_pull_transfer_item(
                        key,
                        rentry,
                        local_path,
                        prefix,
                    ),
                )
                sidecar_key = f"{key}.sha256"
                sidecar_entry = remote.get(sidecar_key)
                if sidecar_entry is not None:
                    append_transfer(
                        sidecar_key,
                        _make_pull_transfer_item(
                            sidecar_key,
                            sidecar_entry,
                            _validate_remote_relkey(sidecar_key, data_dir),
                            prefix,
                        ),
                    )
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
    return SyncItem(
        local_path=lentry.path,
        s3_key=s3_key_for(lentry.key, prefix),
        size_bytes=lentry.size,
        op="transfer",
        sha256=lentry.sha256,
    )


def _make_pull_transfer_item(
    key: str,
    rentry: _RemoteEntry,
    local_path: Path,
    prefix: str,
) -> SyncItem:
    return SyncItem(
        local_path=local_path,
        s3_key=s3_key_for(key, prefix),
        size_bytes=rentry.size,
        op="transfer",
        sha256=rentry.sha256,
    )


def _read_sidecar_digest(sidecar: Path) -> str | None:
    """Best-effort read of `digest  filename\\n` sidecar; None on any error."""
    try:
        return _parse_sidecar_digest(sidecar.read_text(encoding="ascii"))
    except OSError, UnicodeDecodeError:
        return None


def _parse_sidecar_digest(text: str) -> str | None:
    try:
        digest, _ = text.split(maxsplit=1)
    except ValueError:
        return None
    return _normalize_sha256(digest)


def _normalize_sha256(value: object) -> str | None:
    if not isinstance(value, str):
        return None
    digest = value.strip().lower()
    if len(digest) != 64 or any(ch not in "0123456789abcdef" for ch in digest):
        return None
    return digest


def _entries_match(
    lentry: _LocalEntry,
    rentry: _RemoteEntry,
    planning_mode: PlanningMode,
) -> bool:
    if lentry.size != rentry.size:
        return False
    if (
        planning_mode is not PlanningMode.SIZE
        and lentry.sha256 is not None
        and rentry.sha256 is not None
    ):
        return lentry.sha256 == rentry.sha256
    return True


def _attach_remote_sidecar_digests(
    client: S3Client,
    remote: dict[str, _RemoteEntry],
    prefix: str,
) -> None:
    for key, entry in tuple(remote.items()):
        sidecar_key = f"{key}.sha256"
        if sidecar_key not in remote:
            continue
        try:
            raw = client.read_object_bytes(
                s3_key_for(sidecar_key, prefix),
                max_bytes=_REMOTE_SIDECAR_MAX_BYTES,
            )
            digest = _parse_sidecar_digest(raw.decode("ascii"))
        except UnicodeDecodeError:
            digest = None
        remote[key] = _RemoteEntry(key=entry.key, size=entry.size, sha256=digest)


def _attach_remote_metadata_digests(
    client: S3Client,
    remote: dict[str, _RemoteEntry],
    prefix: str,
) -> None:
    for key, entry in tuple(remote.items()):
        head = client.head_object(s3_key_for(key, prefix))
        if head is None:
            continue
        metadata = cast("dict[str, Any]", head.get("Metadata", {}) or {})
        digest = _normalize_sha256(metadata.get("sha256"))
        remote[key] = _RemoteEntry(key=entry.key, size=entry.size, sha256=digest)


def compute_local_sha256(path: Path) -> str:
    """Compute SHA256 over a local file (used by --verify-sha256 in PULL)."""
    return _sha256_file(path)


__all__ = [
    "_LocalEntry",
    "_RemoteEntry",
    "_parse_sidecar_digest",
    "_should_skip",
    "compute_local_sha256",
    "compute_plan",
    "list_remote",
    "s3_key_for",
    "walk_local",
]
