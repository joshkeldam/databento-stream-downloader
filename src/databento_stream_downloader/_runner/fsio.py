"""Filesystem, locking, hashing, and sidecar helpers for runner stages."""

from __future__ import annotations

import hashlib
import json
import os
import sys
import tempfile
import time
from collections.abc import Generator
from contextlib import contextmanager
from pathlib import Path
from typing import TextIO

import structlog
from rich.console import Console

from databento_stream_downloader._runner.types import _DirectoryFsyncTracker
from databento_stream_downloader.config import DownloadConfig
from databento_stream_downloader.constants import RAW_PREFIX
from databento_stream_downloader.errors import FatalConfigError, ValidationError

_TMP_GLOB = ".*.tmp"
_TMP_MIN_AGE_SECONDS = 5 * 60
_LOCK_FILE = ".run.lock"
_WINDOWS_LOCK_OFFSET = 1 << 20
_NETWORK_FILESYSTEM_TYPES = frozenset(
    {
        "9p",
        "afs",
        "cifs",
        "ceph",
        "fuse.s3fs",
        "glusterfs",
        "gpfs",
        "juicefs",
        "lustre",
        "nfs",
        "nfs4",
        "s3fs",
        "smb",
        "smbfs",
        "sshfs",
    }
)
LOGGER = structlog.get_logger(__name__)


def _validate_runtime_config(
    config: DownloadConfig,
    fsync_tracker: _DirectoryFsyncTracker | None = None,
) -> None:
    if config.data_dir.exists() and not config.data_dir.is_dir():
        msg = f"data_dir must be a directory, got file: {config.data_dir}"
        raise FatalConfigError(msg)
    _reject_known_network_filesystem(config.data_dir)
    config.data_dir.mkdir(parents=True, exist_ok=True)
    if os.name != "nt" and config.data_dir.stat().st_mode & 0o002:
        msg = f"data_dir must not be world-writable: {config.data_dir}"
        raise FatalConfigError(msg)
    fd, probe_name = tempfile.mkstemp(
        dir=config.data_dir,
        prefix=".write_test.",
        text=True,
    )
    probe = Path(probe_name)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as file:
            fd = -1
            file.write("ok")
            file.flush()
            os.fsync(file.fileno())
        _fsync_directory(config.data_dir, fsync_tracker)
    except OSError as exc:
        msg = f"data_dir is not writable: {config.data_dir}"
        raise FatalConfigError(msg) from exc
    finally:
        if fd >= 0:
            os.close(fd)
        probe.unlink(missing_ok=True)
        _fsync_directory(config.data_dir, fsync_tracker)


@contextmanager
def _exclusive_run_lock(
    data_dir: Path,
    run_id: str,
    console: Console,
    fsync_tracker: _DirectoryFsyncTracker | None = None,
) -> Generator[None]:
    lock_path = data_dir / _LOCK_FILE
    lock_file = lock_path.open("a+", encoding="utf-8")
    locked = False
    try:
        if not _try_lock_file(lock_file):
            console.print(
                f"[bold red]Another downloader run is active:[/bold red] {lock_path}"
            )
            raise SystemExit(2)
        locked = True
        payload = {"run_id": run_id}
        lock_file.seek(0)
        lock_file.truncate()
        json.dump(payload, lock_file, sort_keys=True)
        lock_file.write("\n")
        lock_file.flush()
        os.fsync(lock_file.fileno())
        _fsync_directory(data_dir, fsync_tracker)
        yield
    finally:
        if locked:
            _unlock_file(lock_file)
        lock_file.close()
        _fsync_directory(data_dir, fsync_tracker)


def _try_lock_file(file: TextIO) -> bool:
    if os.name == "nt":
        import msvcrt

        try:
            file.seek(_WINDOWS_LOCK_OFFSET)
            msvcrt.locking(file.fileno(), msvcrt.LK_NBLCK, 1)
        except OSError:
            return False
        file.seek(0)
        return True
    import fcntl

    try:
        fcntl.flock(file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError:
        return False
    return True


def _unlock_file(file: TextIO) -> None:
    if os.name == "nt":
        import msvcrt

        try:
            file.seek(_WINDOWS_LOCK_OFFSET)
            msvcrt.locking(file.fileno(), msvcrt.LK_UNLCK, 1)
        except OSError:
            return
        file.seek(0)
        return
    import fcntl

    fcntl.flock(file.fileno(), fcntl.LOCK_UN)


def _reject_known_network_filesystem(data_dir: Path) -> None:
    mount = _mount_for_path(data_dir)
    if mount is None:
        if sys.platform != "linux":
            LOGGER.warning(
                "network_filesystem_detection_unavailable",
                platform=sys.platform,
                data_dir=str(data_dir),
            )
        return
    mount_point, fs_type = mount
    if fs_type.lower() not in _NETWORK_FILESYSTEM_TYPES:
        return
    msg = (
        f"data_dir appears to be on network filesystem {fs_type!r} "
        f"mounted at {mount_point}; use a local filesystem or an external lock"
    )
    raise FatalConfigError(msg)


def _mount_for_path(path: Path) -> tuple[Path, str] | None:
    mounts = _linux_mount_entries()
    if not mounts:
        return None
    resolved = path.resolve(strict=False)
    candidates = [
        (mount_point, fs_type)
        for mount_point, fs_type in mounts
        if resolved.is_relative_to(mount_point)
    ]
    if not candidates:
        return None
    return max(candidates, key=lambda item: len(item[0].parts))


def _linux_mount_entries() -> list[tuple[Path, str]]:
    mounts_path = Path("/proc/mounts")
    if not mounts_path.exists():
        return []
    entries: list[tuple[Path, str]] = []
    for line in mounts_path.read_text(encoding="utf-8").splitlines():
        parts = line.split()
        if len(parts) < 3:
            continue
        entries.append((Path(parts[1]).resolve(strict=False), parts[2]))
    return entries


def _nearest_existing_parent(path: Path) -> Path:
    current = path.resolve()
    while not current.exists():
        current = current.parent
    return current


def _place_tmp(
    tmp: Path,
    dest: Path,
    fsync_tracker: _DirectoryFsyncTracker | None = None,
) -> str:
    digest = _fsync_and_sha256_file(tmp)
    os.replace(tmp, dest)
    _fsync_directory(dest.parent, fsync_tracker)
    return digest


def _write_sha256_sidecar(
    path: Path,
    digest: str | None = None,
    fsync_tracker: _DirectoryFsyncTracker | None = None,
) -> None:
    digest = digest or _sha256_file(path)
    sidecar = _sha256_sidecar_path(path)
    tmp = sidecar.with_name(f".{sidecar.name}.tmp")
    with tmp.open("w", encoding="ascii") as file:
        file.write(f"{digest}  {path.name}\n")
        file.flush()
        os.fsync(file.fileno())
    _ = _place_tmp(tmp, sidecar, fsync_tracker)


def _validate_sha256_sidecar(path: Path) -> None:
    expected = _read_sha256_sidecar(path)
    actual = _sha256_file(path)
    if actual != expected:
        msg = f"SHA256 sidecar mismatch: expected {expected}, got {actual}"
        raise ValidationError(msg)


def _read_sha256_sidecar(path: Path) -> str:
    sidecar = _sha256_sidecar_path(path)
    if not sidecar.exists():
        raise ValidationError("SHA256 sidecar missing")
    try:
        digest, filename = sidecar.read_text(encoding="ascii").split(maxsplit=1)
    except (OSError, UnicodeDecodeError, ValueError) as exc:
        raise ValidationError("SHA256 sidecar malformed") from exc
    if len(digest) != 64 or any(char not in "0123456789abcdef" for char in digest):
        raise ValidationError("SHA256 sidecar malformed")
    if filename.strip() != path.name:
        raise ValidationError("SHA256 sidecar filename mismatch")
    return digest


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as file:
        for chunk in iter(lambda: file.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _sha256_sidecar_path(path: Path) -> Path:
    return path.with_suffix(path.suffix + ".sha256")


def _fsync_and_sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    mode = "r+b" if os.name == "nt" else "rb"
    with path.open(mode) as file:
        for chunk in iter(lambda: file.read(1024 * 1024), b""):
            digest.update(chunk)
        os.fsync(file.fileno())
    return digest.hexdigest()


def _fsync_directory(
    path: Path,
    fsync_tracker: _DirectoryFsyncTracker | None = None,
) -> None:
    try:
        fd = os.open(path, os.O_RDONLY)
    except OSError as exc:
        _record_directory_fsync_skipped(path, exc, fsync_tracker)
        return
    try:
        os.fsync(fd)
    except OSError as exc:
        _record_directory_fsync_skipped(path, exc, fsync_tracker)
    finally:
        os.close(fd)


def _record_directory_fsync_skipped(
    path: Path,
    exc: OSError,
    fsync_tracker: _DirectoryFsyncTracker | None = None,
) -> None:
    if fsync_tracker is not None:
        fsync_tracker.record_skip()
    LOGGER.warning("directory_fsync_skipped", path=str(path), error=str(exc))


def _sweep_stale_tmp_files(config: DownloadConfig) -> None:
    cutoff = time.time() - _TMP_MIN_AGE_SECONDS
    for symbol in config.symbols:
        for schema in config.schemas:
            root = config.data_dir / RAW_PREFIX / symbol / schema
            if not root.exists():
                continue
            for tmp in root.glob(_TMP_GLOB):
                if tmp.is_file() and tmp.stat().st_mtime < cutoff:
                    tmp.unlink(missing_ok=True)
                    LOGGER.warning(
                        "stale_tmp_removed",
                        path=str(tmp),
                        min_age_seconds=_TMP_MIN_AGE_SECONDS,
                        message=(
                            "removed abandoned temporary file; this should only "
                            "happen after SIGKILL or process crash"
                        ),
                    )


__all__ = [
    "_NETWORK_FILESYSTEM_TYPES",
    "_TMP_GLOB",
    "_TMP_MIN_AGE_SECONDS",
    "_exclusive_run_lock",
    "_fsync_and_sha256_file",
    "_fsync_directory",
    "_linux_mount_entries",
    "_mount_for_path",
    "_nearest_existing_parent",
    "_place_tmp",
    "_read_sha256_sidecar",
    "_record_directory_fsync_skipped",
    "_reject_known_network_filesystem",
    "_sha256_file",
    "_sha256_sidecar_path",
    "_sweep_stale_tmp_files",
    "_try_lock_file",
    "_unlock_file",
    "_validate_runtime_config",
    "_validate_sha256_sidecar",
    "_write_sha256_sidecar",
]
