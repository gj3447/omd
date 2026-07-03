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


def test_strict_excludes_out_of_bounds_at_commit(tmp_path):
    """strict: 궤도-밖 경로는 commit 에서 자동 제외(history 진입 X·working tree 보존). in-orbit 만 커밋."""
    omd, _ = _mk(tmp_path, strict=True)
    wt = Path(_drive_to_worktree(omd, ["a/**"]))
    (wt / "a").mkdir(parents=True); (wt / "a" / "x.py").write_text("x=1\n")        # 안
    (wt / "b").mkdir(parents=True); (wt / "b" / "foo.py").write_text("foo=1\n")    # 밖
    r = omd.commit("A", "feat in-orbit, b/foo.py excluded")
    assert r["ok"] is True and r["excluded_out_of_orbit"] == ["b/foo.py"]
    # 밖-경로는 working tree 에 보존되되 커밋엔 안 들어감
    assert (wt / "b" / "foo.py").exists()
    committed = subprocess.run(["git", "show", "--name-only", "--format=", "HEAD"],
                               cwd=str(wt), capture_output=True, text=True).stdout
    assert "a/x.py" in committed and "b/foo.py" not in committed


def test_strict_no_livelock_recommit(tmp_path):
    """위반 후에도 in-orbit 재커밋이 막히지 않음(no wedge) — 밖-경로는 매번 일관 제외."""
    omd, _ = _mk(tmp_path, strict=True)
    wt = Path(_drive_to_worktree(omd, ["a/**"]))
    (wt / "a").mkdir(parents=True); (wt / "a" / "x.py").write_text("x=1\n")
    (wt / "b").mkdir(parents=True); (wt / "b" / "foo.py").write_text("foo=1\n")   # 밖(잔존)
    r1 = omd.commit("A", "first in-orbit")
    assert r1["ok"] is True and r1["excluded_out_of_orbit"] == ["b/foo.py"]
    # 밖-파일이 working tree 에 남아도 추가 in-orbit 작업 재커밋이 성공해야(livelock 0)
    (wt / "a" / "y.py").write_text("y=1\n")
    r2 = omd.commit("A", "second in-orbit after violation")
    assert r2["ok"] is True
    committed = subprocess.run(["git", "show", "--name-only", "--format=", "HEAD"],
                               cwd=str(wt), capture_output=True, text=True).stdout
    assert "a/y.py" in committed   # 새 in-orbit 작업이 실제로 랜딩(wedge 아님)


def test_strict_only_out_of_orbit_is_nothing_in_orbit(tmp_path):
    """밖-경로만 있고 in-orbit 변경이 0이면 ok:False(nothing_in_orbit) — 빈 커밋 안 만듦."""
    omd, _ = _mk(tmp_path, strict=True)
    wt = Path(_drive_to_worktree(omd, ["a/**"]))
    (wt / "b").mkdir(parents=True); (wt / "b" / "foo.py").write_text("foo=1\n")   # 밖만
    r = omd.commit("A", "only out-of-orbit")
    assert r["ok"] is False and r["reason"] == "nothing_in_orbit"
    assert r["excluded"] == ["b/foo.py"]


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
