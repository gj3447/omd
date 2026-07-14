"""Q11: CLOUD CONNECT는 operator-configured integration check가 green일 때만 MERGED다."""

from __future__ import annotations

import subprocess
import sys
import threading
import time
from pathlib import Path

import pytest

from omd_server import Coordinator
from omd_server.gitio import GitRollbackError


def _git(args, cwd, *, check=True):
    return subprocess.run(
        ["git", *args], cwd=str(cwd), check=check, capture_output=True, text=True,
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
    # 사용자 root HEAD를 건드리지 않게 main은 전용 integration worktree에 양보한다.
    _git(["checkout", "-b", "dev"], repo)
    return repo, base


def _coordinator(tmp_path: Path, repo: Path, check, **kwargs) -> Coordinator:
    config = {
        "repo": str(repo),
        "worktrees_dir": str(tmp_path / "worktrees"),
        "integration_branch": "main",
        "agent_ttl": None,
        "integration_check": check,
        "integration_check_timeout": 3.0,
        "require_integration_check": True,
    }
    config.update(kwargs)
    return Coordinator(str(tmp_path / "omd.db"), **config)


def _develop_value(omd: Coordinator, value: int = 2):
    started = omd.begin("T", "ag", ["value.py"], ttl=30.0)
    assert started["ok"] is True
    Path(started["worktree"], "value.py").write_text(f"VALUE = {value}\n")
    committed = omd.commit("T", "change value", "ag", started["fence"])
    assert committed["ok"] is True
    omd.finish("T", "ag", started["fence"])
    return started


def test_red_combined_tree_is_not_committed_or_recorded_merged(tmp_path):
    repo, base = _repo(tmp_path)
    check = [
        sys.executable, "-c",
        "from value import VALUE; print('combined', VALUE); assert VALUE == 1",
    ]
    omd = _coordinator(tmp_path, repo, check)
    started = _develop_value(omd, 2)

    out = omd.connect("T", "ag", started["fence"])

    assert out["ok"] is False
    assert out["state"] == "DONE"
    assert out["retryable"] is True
    assert out["reason"] == "integration_check_failed"
    assert out["check_returncode"] != 0
    assert "combined 2" in out["check_stdout"]
    assert _git(["rev-parse", "main"], repo).stdout.strip() == base
    assert omd.store.get_task("T")["state"] == "DONE"
    orbit = omd.store.get_orbit(started["orbit_id"])
    assert orbit["state"] == "HELD" and orbit["merging"] == 0
    assert omd.store.all_held_merge_tokens() == []
    assert omd.git.branch_in_integration(
        omd.integration_worktree, "main", omd._trailer("T")
    ) is None


def test_green_combined_tree_commits_then_records_merged(tmp_path):
    repo, base = _repo(tmp_path)
    check = [sys.executable, "-c", "from value import VALUE; assert VALUE == 2"]
    omd = _coordinator(tmp_path, repo, check)
    started = _develop_value(omd, 2)

    out = omd.connect("T", "ag", started["fence"])

    assert out["ok"] is True and out["state"] == "MERGED"
    assert out["merge_sha"] != base
    assert _git(["rev-parse", "main"], repo).stdout.strip() == out["merge_sha"]
    assert omd.store.all_held_merge_tokens() == []


def test_barrier_trip_uses_same_gate_and_breaks_on_red_member(tmp_path):
    repo, base = _repo(tmp_path)
    check = [sys.executable, "-c", "from value import VALUE; assert VALUE == 1"]
    omd = _coordinator(tmp_path, repo, check)
    begun = _develop_value(omd, 2)
    assert omd.barrier_declare("B", ["T"])["ok"] is True

    out = omd.barrier_arrive("B", "ag", "T", fence=begun["fence"])

    assert out["ok"] is False and out["state"] == "BROKEN"
    assert omd.barrier_status("B")["state"] == "BROKEN"
    assert omd.store.get_task("T")["state"] == "DONE"
    assert _git(["rev-parse", "main"], repo).stdout.strip() == base


def test_required_gate_fails_closed_when_configuration_is_missing(tmp_path):
    repo, _ = _repo(tmp_path)

    with pytest.raises(ValueError, match="integration_check"):
        Coordinator(
            str(tmp_path / "missing-check.db"), repo=str(repo),
            integration_branch="main", require_integration_check=True,
        )
    with pytest.raises(ValueError, match="repo"):
        Coordinator(
            str(tmp_path / "missing-repo.db"), integration_check=[sys.executable, "-V"],
        )


def test_connecting_task_is_not_zombie_reclaimed_while_bounded_check_runs(tmp_path):
    repo, _ = _repo(tmp_path)
    omd = _coordinator(
        tmp_path, repo, [sys.executable, "-c", "pass"],
        integration_check_timeout=1.0, agent_ttl=0.05,
    )
    begun = _develop_value(omd, 2)
    check_started = threading.Event()
    release_check = threading.Event()

    def blocked_check(argv, cwd, *, timeout, output_limit):
        check_started.set()
        if not release_check.wait(2.0):
            raise AssertionError("test did not release integration check")
        return 0, "", "", False

    omd.git._run_operator_check = blocked_check
    observed = {}

    def connect():
        try:
            observed["result"] = omd.connect("T", "ag", begun["fence"])
        except BaseException as exc:  # 전달용 — thread 예외를 놓치지 않는다.
            observed["error"] = exc

    thread = threading.Thread(target=connect)
    thread.start()
    assert check_started.wait(2.0)
    time.sleep(0.1)  # default agent_ttl(0.05) 초과
    omd.sweep()

    assert omd.store.get_task("T")["state"] == "CONNECTING"
    assert omd.store.get_agent("ag")["state"] == "WORKING"
    assert omd.store.get_orbit(begun["orbit_id"])["merging"] == 1

    release_check.set()
    thread.join(timeout=3.0)
    assert not thread.is_alive()
    assert "error" not in observed
    assert observed["result"]["ok"] is True
    assert observed["result"]["state"] == "MERGED"


def test_unprovable_checker_rollback_keeps_connect_fail_stopped(tmp_path):
    repo, base = _repo(tmp_path)
    check = [
        sys.executable, "-c",
        "from pathlib import Path; Path('value.py').write_text('VALUE = 999\\n')",
    ]
    omd = _coordinator(tmp_path, repo, check)
    begun = _develop_value(omd, 2)

    out = omd.connect("T", "ag", begun["fence"])

    assert out["ok"] is False
    assert out["state"] == "CONNECTING"
    assert out["retryable"] is False
    assert out["reason"] == "integration_rollback_failed"
    assert _git(["rev-parse", "main"], repo).stdout.strip() == base
    assert omd.store.get_task("T")["state"] == "CONNECTING"
    pinned = omd.store.get_orbit(begun["orbit_id"])
    assert pinned["merging"] == 1 and pinned["merge_deadline"] is None
    assert len(omd.store.all_held_merge_tokens()) == 1

    omd.resign()
    with pytest.raises(GitRollbackError):
        _coordinator(tmp_path, repo, check)
    assert omd.store.get_task("T")["state"] == "CONNECTING"
    assert len(omd.store.all_held_merge_tokens()) == 1


def test_restart_aborts_uncommitted_candidate_and_restores_done(tmp_path):
    repo, base = _repo(tmp_path)
    check = [sys.executable, "-c", "from value import VALUE; assert VALUE == 2"]
    omd = _coordinator(tmp_path, repo, check)
    begun = _develop_value(omd, 2)
    phase_a = omd._connect_phase_a("T", "ag", begun["fence"])
    assert phase_a["ok"] is True
    task = omd.store.get_task("T")
    assert task["integration_base_sha"] == base

    wt = omd._ensure_integration_wt()
    omd.git._git(
        *omd.git._IDENT, "merge", "--no-ff", "--no-commit", "-m", "candidate",
        phase_a["intent"]["branch"], cwd=wt,
    )
    assert omd.git.has_merge_in_progress(wt)
    omd.resign()
    omd.close()

    recovered = _coordinator(tmp_path, repo, check)

    assert recovered.store.get_task("T")["state"] == "DONE"
    assert recovered.store.get_task("T")["integration_base_sha"] is None
    assert recovered.store.all_held_merge_tokens() == []
    orbit = recovered.store.get_orbit(begun["orbit_id"])
    assert orbit["state"] == "HELD" and orbit["merging"] == 0
    assert _git(["rev-parse", "main"], repo).stdout.strip() == base
    assert not recovered.git.has_merge_in_progress(recovered.integration_worktree)
