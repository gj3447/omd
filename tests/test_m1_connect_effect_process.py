"""Process-boundary proofs for the M1 split-effect fence.

These tests intentionally use real subprocesses.  An in-process ``flock`` test
cannot prove either the cross-database repository domain or the lifetime of an
open file description inherited by an external-effect child.
"""

from __future__ import annotations

import json
import os
import signal
import sqlite3
import subprocess
import sys
import textwrap
import time
from pathlib import Path

import pytest

try:
    import fcntl
except ImportError:  # pragma: no cover - the production feature is POSIX-only.
    fcntl = None


pytestmark = pytest.mark.skipif(fcntl is None, reason="requires POSIX flock")

_ROOT = str(Path(__file__).resolve().parents[1])


def _git(args: list[str], cwd: Path) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["git", *args], cwd=str(cwd), check=True, capture_output=True, text=True,
    )


def _repo(tmp_path: Path) -> tuple[Path, str]:
    repo = tmp_path / "repo"
    repo.mkdir()
    _git(["init", "-b", "main"], repo)
    _git(["config", "user.name", "test"], repo)
    _git(["config", "user.email", "test@example.com"], repo)
    (repo / "value.py").write_text("VALUE = 1\n")
    _git(["add", "-A"], repo)
    _git(["commit", "-m", "base"], repo)
    base = _git(["rev-parse", "HEAD"], repo).stdout.strip()
    _git(["checkout", "-b", "dev"], repo)
    return repo, base


def _common_dir(repo: Path) -> Path:
    value = _git(["rev-parse", "--git-common-dir"], repo).stdout.strip()
    path = Path(value)
    if not path.is_absolute():
        path = repo / path
    return path.resolve()


def _write_script(path: Path, source: str) -> Path:
    path.write_text(textwrap.dedent(source))
    return path


def _diagnostic(proc: subprocess.Popen[str]) -> str:
    if proc.poll() is None:
        return f"pid={proc.pid} still running"
    stdout, stderr = proc.communicate(timeout=1)
    return f"rc={proc.returncode} stdout={stdout[-1200:]!r} stderr={stderr[-2400:]!r}"


def _wait_for(predicate, *, timeout: float = 10.0,
              proc: subprocess.Popen[str] | None = None) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        if proc is not None and proc.poll() is not None:
            pytest.fail(f"helper exited before signal: {_diagnostic(proc)}")
        time.sleep(0.01)
    detail = f"; {_diagnostic(proc)}" if proc is not None else ""
    pytest.fail(f"timed out waiting for subprocess condition{detail}")


def _run_script(script: Path, *args: object) -> dict:
    result = subprocess.run(
        [sys.executable, str(script), *(str(arg) for arg in args)],
        cwd=_ROOT, capture_output=True, text=True, timeout=15,
    )
    assert result.returncode == 0, (
        f"helper failed rc={result.returncode}\nstdout={result.stdout}\nstderr={result.stderr}"
    )
    return json.loads(result.stdout.strip())


def _lock_available(path: Path) -> bool:
    fd = os.open(path, os.O_RDWR | os.O_CREAT, 0o600)
    try:
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            return False
        return True
    finally:
        # As in production, close is the release operation.  Avoid LOCK_UN so
        # this helper also respects shared open-file-description semantics.
        os.close(fd)


def _stop_process(proc: subprocess.Popen[str] | None) -> None:
    if proc is not None and proc.poll() is None:
        proc.kill()
        proc.wait(timeout=5)


def _pid_alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    return True


_HOLD_EFFECT = r"""
    import sys
    import time
    from pathlib import Path

    sys.path.insert(0, sys.argv[1])
    from omd_server import Coordinator

    db, repo, worktrees, ready, release = sys.argv[2:]
    omd = Coordinator(
        db, repo=repo, worktrees_dir=worktrees, integration_branch="main",
        agent_ttl=None, enforce_single_coordinator=False,
    )
    with omd._connect_effect(blocking=True) as acquired:
        if not acquired:
            raise RuntimeError("blocking effect acquisition unexpectedly failed")
        Path(ready).write_text("locked\n")
        while not Path(release).exists():
            time.sleep(0.01)
"""


_PROBE_ENSURE = r"""
    import json
    import sys
    from pathlib import Path

    sys.path.insert(0, sys.argv[1])
    from omd_server import Coordinator
    from omd_server.gitio import GitRepo

    db, repo, worktrees, touched = sys.argv[2:]
    original = GitRepo.ensure_integration_worktree

    def observed(self, path, branch):
        with Path(touched).open("a") as stream:
            stream.write("ensure\n")
        return original(self, path, branch)

    GitRepo.ensure_integration_worktree = observed

    class Events:
        def __init__(self):
            self.items = []
        def emit(self, event, cid, **attrs):
            self.items.append({"event": event, "cid": cid, "attrs": attrs})

    events = Events()
    omd = Coordinator(
        db, repo=repo, worktrees_dir=worktrees, integration_branch="main",
        agent_ttl=None, enforce_single_coordinator=False, events=events,
    )
    print(json.dumps({"events": events.items}), flush=True)
"""


@pytest.mark.parametrize("linked_alias", [False, True], ids=["same-root", "linked-worktree"])
def test_repository_effect_lock_fences_recovery_across_different_databases(
    tmp_path, linked_alias
):
    """A DB-local fence must not let DB B race DB A on one integration repo."""
    repo, _ = _repo(tmp_path)
    probe_repo = repo
    if linked_alias:
        probe_repo = tmp_path / "repo-alias"
        _git(["worktree", "add", "-b", "alias", str(probe_repo)], repo)
    holder_script = _write_script(tmp_path / "hold_effect.py", _HOLD_EFFECT)
    probe_script = _write_script(tmp_path / "probe_ensure.py", _PROBE_ENSURE)
    ready = tmp_path / "holder.ready"
    release = tmp_path / "holder.release"
    touched = tmp_path / "integration.ensure.called"
    db_a = tmp_path / "a.db"
    db_b = tmp_path / "b.db"
    repo_lock = _common_dir(repo) / "omd-connect-effect.lock"

    holder = subprocess.Popen(
        [
            sys.executable, str(holder_script), _ROOT, str(db_a), str(repo),
            str(tmp_path / "wt-a"), str(ready), str(release),
        ],
        cwd=_ROOT, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
    )
    try:
        _wait_for(ready.exists, proc=holder)
        assert not _lock_available(repo_lock), "process A must own the shared repo domain"

        probe = _run_script(
            probe_script, _ROOT, db_b, probe_repo, tmp_path / "wt-b", touched,
        )

        assert not touched.exists(), (
            "DB B startup called ensure_integration_worktree while DB A owned the repo effect"
        )
        assert [item["event"] for item in probe["events"]] == [
            "leader_acquired",
            "leader_resigned",
            "connect_recovery_skipped",
        ]
        assert holder.poll() is None
    finally:
        release.touch()
        if holder.poll() is None:
            try:
                holder.wait(timeout=5)
            except subprocess.TimeoutExpired:
                _stop_process(holder)

    _wait_for(lambda: _lock_available(repo_lock), timeout=5)


def test_pending_migration_is_fenced_by_same_db_effect_across_processes(tmp_path):
    repo, _ = _repo(tmp_path)
    db = tmp_path / "omd.db"
    holder_script = _write_script(tmp_path / "hold_effect.py", _HOLD_EFFECT)
    ready = tmp_path / "holder.ready"
    release = tmp_path / "holder.release"

    from omd_server import Coordinator

    seeded = Coordinator(
        str(db), repo=str(repo), worktrees_dir=str(tmp_path / "seed-wt"),
        integration_branch="main", agent_ttl=None,
        enforce_single_coordinator=False,
    )
    seeded.close()
    seeded.store.db.close()

    holder = subprocess.Popen(
        [
            sys.executable, str(holder_script), _ROOT, str(db), str(repo),
            str(tmp_path / "holder-wt"), str(ready), str(release),
        ],
        cwd=_ROOT, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
    )
    try:
        _wait_for(ready.exists, proc=holder)
        with sqlite3.connect(db) as raw:
            raw.execute("DELETE FROM meta WHERE key='schema_version'")

        with pytest.raises(RuntimeError, match="exclusive split-effect authority"):
            Coordinator(
                str(db), repo=str(repo), worktrees_dir=str(tmp_path / "probe-wt"),
                integration_branch="main", agent_ttl=None,
                enforce_single_coordinator=False,
            )
        with sqlite3.connect(db) as raw:
            assert raw.execute(
                "SELECT value FROM meta WHERE key='schema_version'"
            ).fetchone() is None
    finally:
        release.touch()
        if holder.poll() is None:
            try:
                holder.wait(timeout=5)
            except subprocess.TimeoutExpired:
                _stop_process(holder)

    recovered = Coordinator(
        str(db), repo=str(repo), worktrees_dir=str(tmp_path / "recovered-wt"),
        integration_branch="main", agent_ttl=None,
        enforce_single_coordinator=False,
    )
    assert recovered.store.schema_current() is True


_BLOCKING_CHECK = r"""
    import os
    import sys
    import time
    from pathlib import Path

    ready = Path(sys.argv[1])
    pending = ready.with_suffix(".tmp")
    pending.write_text(str(os.getpid()))
    os.replace(pending, ready)
    deadline = time.monotonic() + 30.0
    while time.monotonic() < deadline:
        time.sleep(1)
    raise SystemExit(124)
"""


_CRASHING_CONNECT = r"""
    import sys
    from pathlib import Path

    sys.path.insert(0, sys.argv[1])
    from omd_server import Coordinator

    db, repo, worktrees, checker, checker_ready = sys.argv[2:]
    omd = Coordinator(
        db, repo=repo, worktrees_dir=worktrees, integration_branch="main",
        agent_ttl=None, enforce_single_coordinator=False,
        integration_check=[sys.executable, checker, checker_ready],
        integration_check_timeout=60.0, require_integration_check=True,
        merge_timeout=60.0,
    )
    begun = omd.begin("T", "ag", ["value.py"], ttl=30.0)
    if not begun.get("ok"):
        raise RuntimeError(f"begin failed: {begun}")
    Path(begun["worktree"], "value.py").write_text("VALUE = 2\n")
    committed = omd.commit("T", "change value", "ag", begun["fence"])
    if not committed.get("ok"):
        raise RuntimeError(f"commit failed: {committed}")
    finished = omd.finish("T", "ag", begun["fence"])
    if finished.get("state") != "DONE":
        raise RuntimeError(f"finish failed: {finished}")
    # The test SIGKILLs this process after the checker announces that Phase B
    # is live.  A normal return here would be a test setup failure.
    result = omd.connect(
        "T", "ag", begun["fence"], request_id="req-effect-crash",
    )
    raise RuntimeError(f"connect unexpectedly returned: {result}")
"""


_PROBE_RECOVERY = r"""
    import json
    import sqlite3
    import sys

    sys.path.insert(0, sys.argv[1])
    from omd_server import Coordinator

    db, repo, worktrees = sys.argv[2:]

    def task_state():
        connection = sqlite3.connect(db)
        try:
            row = connection.execute(
                "SELECT state FROM tasks WHERE task_id = 'T'"
            ).fetchone()
            return row[0] if row else None
        finally:
            connection.close()

    class Events:
        def __init__(self):
            self.items = []
        def emit(self, event, cid, **attrs):
            self.items.append({"event": event, "cid": cid, "attrs": attrs})

    before = task_state()
    events = Events()
    omd = Coordinator(
        db, repo=repo, worktrees_dir=worktrees, integration_branch="main",
        agent_ttl=None, enforce_single_coordinator=False, events=events,
    )
    task = omd.store.get_task("T")
    idem = omd.store.get_idem("req-effect-crash")
    result = {
        "before": before,
        "after": task["state"],
        "held_tokens": len(omd.store.all_held_merge_tokens()),
        "idem_status": idem["status"] if idem else None,
        "merge_in_progress": omd.git.has_merge_in_progress(omd.integration_worktree),
        "events": events.items,
    }
    print(json.dumps(result), flush=True)
"""


def test_inherited_effect_fd_fences_recovery_after_parent_sigkill(tmp_path):
    """The checker child, not its dead parent, determines effect-lock lifetime."""
    repo, base = _repo(tmp_path)
    checker = _write_script(tmp_path / "blocking_check.py", _BLOCKING_CHECK)
    connector = _write_script(tmp_path / "crashing_connect.py", _CRASHING_CONNECT)
    probe = _write_script(tmp_path / "probe_recovery.py", _PROBE_RECOVERY)
    checker_ready = tmp_path / "checker.ready"
    db = tmp_path / "omd.db"
    db_lock = Path(str(db.resolve()) + ".connect-effect.lock")
    repo_lock = _common_dir(repo) / "omd-connect-effect.lock"
    checker_pid: int | None = None

    parent = subprocess.Popen(
        [
            sys.executable, str(connector), _ROOT, str(db), str(repo),
            str(tmp_path / "worktrees"), str(checker), str(checker_ready),
        ],
        cwd=_ROOT, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
    )
    try:
        _wait_for(
            lambda: checker_ready.exists() and bool(checker_ready.read_text().strip()),
            proc=parent,
        )
        checker_pid = int(checker_ready.read_text().strip())

        # Crash the Coordinator while its pass_fds child is still evaluating the
        # candidate merge.  The child is in a separate session and survives.
        parent.kill()
        assert parent.wait(timeout=5) < 0
        assert not _lock_available(db_lock)
        assert not _lock_available(repo_lock)

        live = _run_script(probe, _ROOT, db, repo, tmp_path / "probe-wt-live")
        assert live["before"] == live["after"] == "CONNECTING"
        assert live["held_tokens"] == 1
        assert live["idem_status"] == "INFLIGHT"
        assert live["merge_in_progress"] is True
        assert [item["event"] for item in live["events"]] == [
            "connect_recovery_skipped"
        ]

        os.kill(checker_pid, signal.SIGKILL)
        _wait_for(
            lambda: _lock_available(db_lock) and _lock_available(repo_lock),
            timeout=5,
        )

        recovered = _run_script(
            probe, _ROOT, db, repo, tmp_path / "probe-wt-recovered",
        )
        recovery_events = [
            item for item in recovered["events"]
            if item["event"] == "connect_recovered"
        ]
        assert recovered["before"] == "CONNECTING"
        assert recovered["after"] == "DONE"
        assert recovered["held_tokens"] == 0
        assert recovered["idem_status"] == "RETRYABLE"
        assert recovered["merge_in_progress"] is False
        assert len(recovery_events) == 1
        assert recovery_events[0]["attrs"]["outcome"] == "rollback"

        replay = _run_script(
            probe, _ROOT, db, repo, tmp_path / "probe-wt-replay",
        )
        assert replay["before"] == replay["after"] == "DONE"
        assert replay["held_tokens"] == 0
        assert replay["idem_status"] == "RETRYABLE"
        assert replay["merge_in_progress"] is False
        assert not any(
            item["event"] == "connect_recovered" for item in replay["events"]
        ), "the durable crash cut must be reconciled exactly once"
        assert _git(["rev-parse", "main"], repo).stdout.strip() == base
    finally:
        _stop_process(parent)
        cleanup_pid = checker_pid
        if cleanup_pid is None and checker_ready.exists():
            try:
                cleanup_pid = int(checker_ready.read_text().strip())
            except ValueError:
                cleanup_pid = None
        if cleanup_pid is not None:
            try:
                os.kill(cleanup_pid, signal.SIGKILL)
            except ProcessLookupError:
                cleanup_pid = None
            if cleanup_pid is not None:
                _wait_for(lambda: not _pid_alive(cleanup_pid), timeout=5)
