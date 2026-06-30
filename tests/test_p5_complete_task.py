"""P5 — complete_task 원샷 wrapper. INV: ok:True ⟺ 최종 MERGED. 단계 거부 fail-loud(stage) 전파."""
import subprocess
from pathlib import Path

from omd_server import Coordinator


def _git(args, cwd):
    subprocess.run(["git", *args], cwd=str(cwd), check=True, capture_output=True, text=True)


def _rev(gitdir, ref="main"):
    return subprocess.run(["git", "--git-dir", str(gitdir), "rev-parse", ref],
                          capture_output=True, text=True).stdout.strip()


def _init(root, *, with_remote=False, tmp=None):
    root.mkdir()
    _git(["init", "-b", "main"], root)
    _git(["config", "user.name", "t"], root)
    _git(["config", "user.email", "t@t"], root)
    (root / "README.md").write_text("base\n")
    _git(["add", "-A"], root)
    _git(["commit", "-m", "base"], root)
    remote = None
    if with_remote:
        remote = tmp / "remote.git"; remote.mkdir()
        _git(["init", "--bare", "-b", "main"], remote)
        _git(["remote", "add", "origin", str(remote)], root)
        _git(["push", "origin", "main"], root)
    _git(["checkout", "-b", "dev"], root)
    return remote


def _claim_and_write(omd, files: dict):
    omd.declare("A", writes=["a/**"])
    omd.next_task("agA")
    omd.claim("agA", ["a/**"], task_id="A")
    wt = Path(omd.start("A", "agA")["worktree"])
    for rel, content in files.items():
        p = wt / rel
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(content)
    return wt


def test_oneshot_merges_in_one_call(tmp_path):
    repo = tmp_path / "repo"; _init(repo)
    omd = Coordinator(db_path=str(tmp_path / "o.db"), repo=str(repo),
                      worktrees_dir=str(tmp_path / "wt"), integration_branch="main")
    _claim_and_write(omd, {"a/x.py": "x=1\n"})
    r = omd.complete_task("A", "feat a/x")
    assert r["ok"] is True and r["state"] == "MERGED" and r["stage"] == "connect"
    assert omd.store.get_task("A")["state"] == "MERGED"   # INV: store 권위


def test_writeset_violation_fails_at_connect_no_false_merged(tmp_path):
    repo = tmp_path / "repo"; _init(repo)
    omd = Coordinator(db_path=str(tmp_path / "o.db"), repo=str(repo),
                      worktrees_dir=str(tmp_path / "wt"), integration_branch="main")
    _claim_and_write(omd, {"a/x.py": "x=1\n", "b/foo.py": "foo=1\n"})   # b/foo.py = 밖
    r = omd.complete_task("A", "feat oob")
    assert r["ok"] is False and r["stage"] == "connect" and r.get("reason") == "writeset_violation"
    assert omd.store.get_task("A")["state"] != "MERGED"   # MERGED 거짓주장 0


def test_msg_none_skips_commit(tmp_path):
    repo = tmp_path / "repo"; _init(repo)
    omd = Coordinator(db_path=str(tmp_path / "o.db"), repo=str(repo),
                      worktrees_dir=str(tmp_path / "wt"), integration_branch="main")
    wt = _claim_and_write(omd, {"a/x.py": "x=1\n"})
    omd.commit("A", "pre-commit")                  # 미리 커밋
    r = omd.complete_task("A")                      # msg=None → commit skip
    assert r["ok"] is True and r["state"] == "MERGED" and r["committed"] is False


def test_push_override_syncs_remote(tmp_path):
    repo = tmp_path / "repo"
    remote = _init(repo, with_remote=True, tmp=tmp_path)
    base = _rev(remote)
    omd = Coordinator(db_path=str(tmp_path / "o.db"), repo=str(repo),
                      worktrees_dir=str(tmp_path / "wt"), integration_branch="main")
    _claim_and_write(omd, {"a/x.py": "x=1\n"})
    r = omd.complete_task("A", "feat a/x", push="origin")
    assert r["ok"] is True and r["state"] == "MERGED"
    assert _rev(remote) != base   # complete_task push override → remote main 전진
