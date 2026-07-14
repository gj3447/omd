"""Process- and thread-level singleton guard for one registered repo worker."""

from __future__ import annotations

import fcntl
import os
import stat
import threading
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator

from .contracts import RegisteredRepository


_LOCKS_GUARD = threading.Lock()
_PROCESS_LOCKS: dict[Path, threading.RLock] = {}


def _process_lock(path: Path) -> threading.RLock:
    with _LOCKS_GUARD:
        return _PROCESS_LOCKS.setdefault(path, threading.RLock())


def worker_lock_path(repository: RegisteredRepository) -> Path:
    return repository.git_common_dir / "omd-repo-daemon.lock"


@contextmanager
def worker_lock(repository: RegisteredRepository) -> Iterator[None]:
    # The key is the canonical Git instance, not the configurable repo_id
    # alias or state directory. Separate daemon configurations therefore
    # serialize on the same repository-wide lock.
    from .repository import assert_live_repository

    assert_live_repository(repository)
    lock_path = worker_lock_path(repository)
    thread_lock = _process_lock(lock_path)
    with thread_lock:
        descriptor = os.open(
            lock_path,
            os.O_RDWR | os.O_CREAT | getattr(os, "O_NOFOLLOW", 0),
            0o600,
        )
        try:
            metadata = os.fstat(descriptor)
            if (
                not stat.S_ISREG(metadata.st_mode)
                or metadata.st_uid != os.getuid()
                or metadata.st_mode & (stat.S_IWGRP | stat.S_IWOTH)
            ):
                raise RuntimeError("repository worker lock is not a daemon-owned file")
            fcntl.flock(descriptor, fcntl.LOCK_EX)
            yield
        finally:
            fcntl.flock(descriptor, fcntl.LOCK_UN)
            os.close(descriptor)
