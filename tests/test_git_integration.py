"""End-to-end: 실물 git 레포에서 OMD 명제 증명 —
서로소(입체) write-set 두 물방울이 각자 worktree에서 작업 → CLOUD CONNECT(merge) 무충돌."""

import subprocess
from pathlib import Path

from omd_server import Coordinator


def _git(args, cwd):
    subprocess.run(["git", *args], cwd=str(cwd), check=True, capture_output=True, text=True)


def _init_repo(root: Path):
    """root는 사용자 HEAD = 별도 dev 브랜치. main은 OMD 전용 통합 브랜치(§D11)로 남겨
    전용 통합 worktree(<root>-omd-integration)만 main을 체크아웃한다 — 사용자 HEAD 불침범."""
    root.mkdir()
    _git(["init", "-b", "main"], root)
    _git(["config", "user.name", "t"], root)
    _git(["config", "user.email", "t@t"], root)
    (root / "README.md").write_text("base\n")
    _git(["add", "-A"], root)
    _git(["commit", "-m", "base"], root)
    _git(["checkout", "-b", "dev"], root)   # 사용자 HEAD를 dev로 — main(통합)은 비워둠


def test_end_to_end_disjoint_merge(tmp_path):
    repo = tmp_path / "repo"
    _init_repo(repo)
    omd = Coordinator(db_path=str(tmp_path / "omd.db"), repo=str(repo),
                      worktrees_dir=str(tmp_path / "wt"), integration_branch="main")

    omd.declare("A", writes=["a/**"])
    omd.declare("B", writes=["b/**"])

    # 물방울 A: 자기 궤도(a/**) + worktree에서 a/x.py 작성
    assert omd.next_task("agA")["task_id"] == "A"
    omd.claim("agA", ["a/**"], task_id="A")
    sa = omd.start("A", "agA")
    (Path(sa["worktree"]) / "a").mkdir(parents=True)
    (Path(sa["worktree"]) / "a" / "x.py").write_text("x = 1\n")
    omd.commit("A", "feat: a/x")
    omd.finish("A")

    # 물방울 B: 서로소 궤도(b/**) → next가 A의 활성 궤도와 안 겹쳐 배정
    assert omd.next_task("agB")["task_id"] == "B"
    omd.claim("agB", ["b/**"], task_id="B")
    sb = omd.start("B", "agB")
    (Path(sb["worktree"]) / "b").mkdir(parents=True)
    (Path(sb["worktree"]) / "b" / "y.py").write_text("y = 2\n")
    omd.commit("B", "feat: b/y")
    omd.finish("B")

    # CLOUD CONNECT (응결=실제 merge, split-phase) — 둘 다 무충돌
    ra = omd.connect("A")
    rb = omd.connect("B")
    assert ra["ok"] and ra["state"] == "MERGED", ra
    assert rb["ok"] and rb["state"] == "MERGED", rb

    # 통합 worktree(=main 체크아웃)에 두 파일 모두 존재 = 분열 0. 사용자 HEAD(repo/dev)는 불변.
    integ = Path(omd.integration_worktree)
    assert (integ / "a" / "x.py").exists()
    assert (integ / "b" / "y.py").exists()
    assert not (repo / "a").exists() and not (repo / "b").exists()   # 사용자 HEAD 불침범
    log = subprocess.run(["git", "log", "--oneline", "main"], cwd=str(repo),
                         capture_output=True, text=True).stdout
    assert "CLOUD CONNECT A" in log and "CLOUD CONNECT B" in log


def test_reclaim_deletes_branch_so_restart_works(tmp_path):
    """P0-8: 회수가 worktree+브랜치를 지워야 requeue된 작업을 다시 start할 수 있다.
    (예전엔 브랜치가 남아 `worktree add -b omd/A`가 실패 → 작업 영구 wedge.)"""
    repo = tmp_path / "repo"
    _init_repo(repo)
    omd = Coordinator(db_path=str(tmp_path / "omd.db"), repo=str(repo),
                      worktrees_dir=str(tmp_path / "wt"), integration_branch="main")
    omd.declare("A", writes=["a/**"])
    omd.next_task("agA")
    omd.claim("agA", ["a/**"], task_id="A")
    omd.start("A", "agA")
    assert omd.git.branch_exists("omd/A")

    omd.bail("agA")                                   # worktree + 브랜치 삭제 + 작업 requeue
    assert not omd.git.branch_exists("omd/A")          # 브랜치 지워짐(P0-8)
    assert omd.store.get_task("A")["state"] == "PENDING"

    # 새 물방울이 requeue된 같은 작업을 다시 start — 브랜치 충돌 없이 성공
    assert omd.next_task("agB")["task_id"] == "A"
    omd.claim("agB", ["a/**"], task_id="A")
    s2 = omd.start("A", "agB")
    assert s2["worktree"] and s2["state"] == "IN_ORBIT"


def test_connect_stale_lease_does_not_merge(tmp_path):
    """작업 중 lease 만료 → fencing으로 merge 거부(통합 브랜치 불변)."""
    import time
    repo = tmp_path / "repo"
    _init_repo(repo)
    omd = Coordinator(db_path=str(tmp_path / "omd.db"), repo=str(repo),
                      worktrees_dir=str(tmp_path / "wt"), integration_branch="main")
    omd.declare("A", writes=["a/**"])
    omd.next_task("agA")
    omd.claim("agA", ["a/**"], "write", ttl=0.05, task_id="A")
    sa = omd.start("A", "agA")
    (Path(sa["worktree"]) / "a").mkdir(parents=True)
    (Path(sa["worktree"]) / "a" / "x.py").write_text("x = 1\n")
    omd.commit("A", "feat: a/x")
    omd.finish("A")
    time.sleep(0.08)
    res = omd.connect("A")
    assert res["ok"] is False and "stale" in res["reason"]
    # 통합 worktree에 머지 안 됨(Phase A에서 거부 — Phase B git 미실행)
    assert not (Path(omd.integration_worktree) / "a" / "x.py").exists()
