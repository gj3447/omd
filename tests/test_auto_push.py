"""OMD 내장 commit→sync: connect(=merge) 직후 통합 브랜치를 remote 로 auto-push.
operator "커밋하면 바로 sync"의 OMD 본체 구현 — 로컬 통합브랜치 누적 divergence 방지."""

import subprocess
from pathlib import Path

from omd_server import Coordinator


def _git(args, cwd):
    subprocess.run(["git", *args], cwd=str(cwd), check=True, capture_output=True, text=True)


def _rev(remote_gitdir, ref="main"):
    return subprocess.run(["git", "--git-dir", str(remote_gitdir), "rev-parse", ref],
                          capture_output=True, text=True).stdout.strip()


def _setup(tmp_path):
    """bare remote + 로컬 repo(main=통합, dev=사용자 HEAD) + origin 배선 + main seed push."""
    remote = tmp_path / "remote.git"
    remote.mkdir()
    _git(["init", "--bare", "-b", "main"], remote)
    repo = tmp_path / "repo"
    repo.mkdir()
    _git(["init", "-b", "main"], repo)
    _git(["config", "user.name", "t"], repo)
    _git(["config", "user.email", "t@t"], repo)
    (repo / "README.md").write_text("base\n")
    _git(["add", "-A"], repo)
    _git(["commit", "-m", "base"], repo)
    _git(["remote", "add", "origin", str(remote)], repo)
    _git(["push", "origin", "main"], repo)        # remote main = base
    _git(["checkout", "-b", "dev"], repo)          # 사용자 HEAD = dev
    return repo, remote


def _run_one_task(omd):
    omd.declare("A", writes=["a/**"])
    omd.next_task("agA")
    omd.claim("agA", ["a/**"], task_id="A")
    sa = omd.start("A", "agA")
    (Path(sa["worktree"]) / "a").mkdir(parents=True)
    (Path(sa["worktree"]) / "a" / "x.py").write_text("x = 1\n")
    omd.commit("A", "feat: a/x")
    omd.finish("A")
    return omd.connect("A")


def test_auto_push_syncs_integration_to_remote(tmp_path):
    repo, remote = _setup(tmp_path)
    base = _rev(remote)
    omd = Coordinator(db_path=str(tmp_path / "omd.db"), repo=str(repo),
                      worktrees_dir=str(tmp_path / "wt"), integration_branch="main",
                      auto_push="origin")
    ra = _run_one_task(omd)
    assert ra["ok"] and ra["state"] == "MERGED", ra
    after = _rev(remote)
    assert after != base, "auto_push='origin' 인데 remote main 이 안 밀렸다"
    log = subprocess.run(["git", "--git-dir", str(remote), "log", "--oneline", "main"],
                         capture_output=True, text=True).stdout
    assert "CLOUD CONNECT A" in log, log


def test_no_auto_push_leaves_remote_untouched(tmp_path):
    repo, remote = _setup(tmp_path)
    base = _rev(remote)
    omd = Coordinator(db_path=str(tmp_path / "omd.db"), repo=str(repo),
                      worktrees_dir=str(tmp_path / "wt"), integration_branch="main")  # auto_push=None
    ra = _run_one_task(omd)
    assert ra["ok"] and ra["state"] == "MERGED", ra
    assert _rev(remote) == base, "auto_push off(기본)인데 remote 가 바뀌었다(기존동작 위반)"


def test_auto_push_from_env(tmp_path, monkeypatch):
    repo, remote = _setup(tmp_path)
    base = _rev(remote)
    monkeypatch.setenv("OMD_AUTO_PUSH", "origin")
    omd = Coordinator(db_path=str(tmp_path / "omd.db"), repo=str(repo),
                      worktrees_dir=str(tmp_path / "wt"), integration_branch="main")
    assert omd.auto_push == "origin"
    ra = _run_one_task(omd)
    assert ra["ok"], ra
    assert _rev(remote) != base, "OMD_AUTO_PUSH env 가 안 먹혔다"
