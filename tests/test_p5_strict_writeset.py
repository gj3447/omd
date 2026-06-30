"""P5 — strict-writeset commit 게이트. INV-P5: strict on 이면 궤도-밖 경로 커밋은 commit-time 거부+롤백."""
import subprocess
from pathlib import Path

from omd_server import Coordinator


def _git(args, cwd):
    subprocess.run(["git", *args], cwd=str(cwd), check=True, capture_output=True, text=True)


def _init(root):
    root.mkdir()
    _git(["init", "-b", "main"], root)
    _git(["config", "user.name", "t"], root)
    _git(["config", "user.email", "t@t"], root)
    (root / "README.md").write_text("base\n")
    _git(["add", "-A"], root)
    _git(["commit", "-m", "base"], root)
    _git(["checkout", "-b", "dev"], root)


def _drive_to_worktree(omd, writes):
    omd.declare("A", writes=writes)
    omd.next_task("agA")
    omd.claim("agA", writes, task_id="A")
    return omd.start("A", "agA")["worktree"]


def _mk(tmp_path, *, strict):
    repo = tmp_path / "repo"; _init(repo)
    return Coordinator(db_path=str(tmp_path / "o.db"), repo=str(repo),
                       worktrees_dir=str(tmp_path / "wt"), integration_branch="main",
                       strict_writeset=strict), repo


def test_strict_rejects_out_of_bounds_at_commit(tmp_path):
    omd, _ = _mk(tmp_path, strict=True)
    wt = Path(_drive_to_worktree(omd, ["a/**"]))
    (wt / "a").mkdir(parents=True); (wt / "a" / "x.py").write_text("x=1\n")        # 안
    (wt / "b").mkdir(parents=True); (wt / "b" / "foo.py").write_text("foo=1\n")    # 밖
    r = omd.commit("A", "feat with out-of-bounds")
    assert r["ok"] is False and r["reason"] == "writeset_violation"
    assert r["offending"] == ["b/foo.py"] and r["reverted"] is True
    # 롤백: 작업물 보존(파일 존재) + 커밋은 안 남음(soft reset)
    assert (wt / "b" / "foo.py").exists() and (wt / "a" / "x.py").exists()
    log = subprocess.run(["git", "log", "--oneline"], cwd=str(wt), capture_output=True, text=True).stdout
    assert "out-of-bounds" not in log


def test_strict_allows_in_bounds(tmp_path):
    omd, _ = _mk(tmp_path, strict=True)
    wt = Path(_drive_to_worktree(omd, ["a/**"]))
    (wt / "a").mkdir(parents=True); (wt / "a" / "x.py").write_text("x=1\n")
    r = omd.commit("A", "feat in bounds")
    assert r["ok"] is True and "writeset_violation" not in r


def test_off_mode_is_advisory(tmp_path):
    omd, _ = _mk(tmp_path, strict=False)   # 기본 off
    wt = Path(_drive_to_worktree(omd, ["a/**"]))
    (wt / "a").mkdir(parents=True); (wt / "a" / "x.py").write_text("x=1\n")
    (wt / "b").mkdir(parents=True); (wt / "b" / "foo.py").write_text("foo=1\n")
    r = omd.commit("A", "feat oob advisory")
    assert r["ok"] is True and r.get("writeset_violation") is True
    assert r["offending"] == ["b/foo.py"]   # 경고만(하위호환), 커밋은 남음


def test_env_OMD_STRICT_WRITESET(tmp_path, monkeypatch):
    monkeypatch.setenv("OMD_STRICT_WRITESET", "1")
    omd, _ = _mk(tmp_path, strict=False)
    assert omd.strict_writeset is True
