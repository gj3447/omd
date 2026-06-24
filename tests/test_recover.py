"""P0-6/§D8 — 이중쓰기 크래시 복구. connect 도중(merge↔DB 사이) 크래시로 CONNECTING
고착된 task를 재기동(_recover)이 git 기준으로 재조정: 착지=finalize / 미착지=requeue."""
import subprocess
from pathlib import Path
from omd_server import Coordinator


def _git(a, cwd): subprocess.run(["git", *a], cwd=str(cwd), check=True, capture_output=True, text=True)
def _init(r):
    r.mkdir(); _git(["init","-b","main"],r); _git(["config","user.name","t"],r); _git(["config","user.email","t@t"],r)
    (r/"README.md").write_text("base\n"); _git(["add","-A"],r); _git(["commit","-m","base"],r)
def _to_done(omd, tid, wt_dir):
    omd.declare(tid, writes=[f"{tid}/**"]); omd.next_task(f"ag{tid}"); omd.claim(f"ag{tid}",[f"{tid}/**"],task_id=tid)
    s = omd.start(tid, f"ag{tid}")
    (Path(s["worktree"])/tid).mkdir(); (Path(s["worktree"])/tid/"x.py").write_text("x=1\n")
    omd.commit(tid,"feat"); omd.finish(tid); return s


def test_recover_finalizes_landed_merge(tmp_path):
    repo = tmp_path/"repo"; _init(repo); db=str(tmp_path/"o.db"); wt=str(tmp_path/"wt")
    omd1 = Coordinator(db_path=db, repo=str(repo), worktrees_dir=wt)
    _to_done(omd1, "A", wt)
    # 크래시 시뮬: CONNECTING으로 표시 + 실제 merge 착지, 그러나 DB finalize 전에 사망
    omd1.store.set_task("A", state="CONNECTING")
    omd1.git.merge("omd/A", "CLOUD CONNECT A")          # merge 착지
    assert (repo/"A"/"x.py").exists()                   # git엔 머지됨
    assert omd1.store.get_task("A")["state"] == "CONNECTING"  # DB는 고착
    # 재기동 → _recover
    omd2 = Coordinator(db_path=db, repo=str(repo), worktrees_dir=wt)
    assert omd2.store.get_task("A")["state"] == "MERGED"
    assert all(o["state"]=="RELEASED" for o in omd2.store.orbits_for_task("A") if o["mode"]=="write")


def test_recover_requeues_unlanded(tmp_path):
    repo = tmp_path/"repo"; _init(repo); db=str(tmp_path/"o.db"); wt=str(tmp_path/"wt")
    omd1 = Coordinator(db_path=db, repo=str(repo), worktrees_dir=wt)
    _to_done(omd1, "A", wt)
    omd1.store.set_task("A", state="CONNECTING")        # 크래시: merge 전에 사망(미착지)
    omd2 = Coordinator(db_path=db, repo=str(repo), worktrees_dir=wt)
    assert omd2.store.get_task("A")["state"] == "PENDING"   # requeue=재시도 가능
    assert not (repo/"A"/"x.py").exists()                   # 미착지=통합 불변
    assert not omd2.git.branch_exists("omd/A")              # 정리됨
