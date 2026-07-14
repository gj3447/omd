from __future__ import annotations

import subprocess
import sys
from dataclasses import replace

import pytest

from omd_server.v2.repo.worker import worker_lock, worker_lock_path

from .conftest import RepoFixture


def test_alias_configurations_share_one_cross_process_repo_lock(
    repo_fixture: RepoFixture,
) -> None:
    alias = replace(
        repo_fixture.registration,
        repo_id="alias",
        state_dir=repo_fixture.state / "alias-state",
    )
    assert worker_lock_path(alias) == worker_lock_path(repo_fixture.registration)
    script = """
import fcntl, os, sys
fd = os.open(sys.argv[1], os.O_RDWR | os.O_CREAT, 0o600)
try:
    fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
except BlockingIOError:
    raise SystemExit(0)
raise SystemExit(1)
"""

    with worker_lock(repo_fixture.registration):
        child = subprocess.run(
            [sys.executable, "-c", script, str(worker_lock_path(alias))],
            check=False,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )

    assert child.returncode == 0, child.stderr.decode("utf-8", "replace")


def test_group_or_world_writable_repo_lock_is_rejected(
    repo_fixture: RepoFixture,
) -> None:
    path = worker_lock_path(repo_fixture.registration)
    path.write_text("untrusted\n", encoding="utf-8")
    path.chmod(0o666)

    with pytest.raises(RuntimeError, match="daemon-owned"):
        with worker_lock(repo_fixture.registration):
            raise AssertionError("untrusted lock must not be entered")
