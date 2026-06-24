"""P0-5/§D11 — merge는 항상 고정된 통합 브랜치에 착지(root HEAD 드리프트 무관).
크로스프로세스 동시 merge 직렬화는 connect의 _cs() BEGIN IMMEDIATE(D1)가 담당."""
import subprocess
from pathlib import Path
from omd_server import Coordinator


def _git(a, cwd): subprocess.run(["git", *a], cwd=str(cwd), check=True, capture_output=True, text=True)
def _has(ref, cwd): return subprocess.run(["git","cat-file","-e",ref],cwd=str(cwd),
                                          capture_output=True).returncode == 0
def _init(r):
    r.mkdir(); _git(["init","-b","main"],r); _git(["config","user.name","t"],r); _git(["config","user.email","t@t"],r)
    (r/"README.md").write_text("base\n"); _git(["add","-A"],r); _git(["commit","-m","base"],r)


def test_merge_lands_on_integration_despite_head_drift(tmp_path):
    repo = tmp_path/"repo"; _init(repo)
    omd = Coordinator(db_path=str(tmp_path/"o.db"), repo=str(repo), worktrees_dir=str(tmp_path/"wt"))
    assert omd.integration_branch == "main"
    omd.declare("A", writes=["a/**"]); omd.next_task("agA"); omd.claim("agA",["a/**"],task_id="A")
    s = omd.start("A","agA")
    (Path(s["worktree"])/"a").mkdir(); (Path(s["worktree"])/"a"/"x.py").write_text("x=1\n")
    omd.commit("A","feat"); omd.finish("A")
    # root HEAD를 통합 브랜치에서 딴 곳으로 드리프트
    _git(["checkout","-b","decoy"], repo); assert omd.git.current_branch()=="decoy"
    r = omd.connect("A")
    assert r["ok"] and r["state"]=="MERGED", r
    assert _has("main:a/x.py", repo)            # 통합 브랜치에 착지
    assert not _has("decoy:a/x.py", repo)       # 엉뚱한 브랜치 오염 없음
