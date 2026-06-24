"""P0-4 — connect는 시작시 고정한 fence와 일치하는 HELD 궤도에서만 merge.
lease가 도중 만료→재부여(ABA)되면 state는 HELD로 돌아와도 fence가 달라 거부."""
import subprocess
from pathlib import Path
from omd_server import Coordinator


def _git(a, cwd): subprocess.run(["git", *a], cwd=str(cwd), check=True, capture_output=True, text=True)
def _init(r):
    r.mkdir(); _git(["init","-b","main"],r); _git(["config","user.name","t"],r); _git(["config","user.email","t@t"],r)
    (r/"README.md").write_text("base\n"); _git(["add","-A"],r); _git(["commit","-m","base"],r)


def test_connect_rejects_fence_drift_aba(tmp_path):
    repo = tmp_path/"repo"; _init(repo)
    omd = Coordinator(db_path=str(tmp_path/"o.db"), repo=str(repo), worktrees_dir=str(tmp_path/"wt"))
    omd.declare("A", writes=["a/**"]); omd.next_task("agA"); omd.claim("agA",["a/**"],task_id="A")
    s = omd.start("A","agA")
    (Path(s["worktree"])/"a").mkdir(); (Path(s["worktree"])/"a"/"x.py").write_text("x=1\n")
    omd.commit("A","feat"); omd.finish("A")
    # ABA: 같은 궤도가 다른 fence로 재부여(HELD 유지) — captured와 불일치
    orb = [o for o in omd.store.orbits_for_task("A") if o["mode"]=="write"][0]
    omd.store.set_orbit(orb["orbit_id"], state="HELD", fence=orb["fence"]+99)
    r = omd.connect("A")
    assert r["ok"] is False and "fence" in r["reason"], r
    assert not (repo/"a"/"x.py").exists()   # merge 거부 → 통합 브랜치 불변


def test_connect_ok_when_fence_matches(tmp_path):
    repo = tmp_path/"repo"; _init(repo)
    omd = Coordinator(db_path=str(tmp_path/"o.db"), repo=str(repo), worktrees_dir=str(tmp_path/"wt"))
    omd.declare("A", writes=["a/**"]); omd.next_task("agA"); omd.claim("agA",["a/**"],task_id="A")
    s = omd.start("A","agA")
    (Path(s["worktree"])/"a").mkdir(); (Path(s["worktree"])/"a"/"x.py").write_text("x=1\n")
    omd.commit("A","feat"); omd.finish("A")
    assert omd.store.get_task("A")["captured_fence"] is not None   # fence가 실제 캡처됨
    r = omd.connect("A")
    assert r["ok"] is True and r["state"]=="MERGED", r
