"""증분5 — D6 잔여: finish/commit/connect 의 owner+fence 가드 + bail_epoch 부활방지.

§D6 표대로 finish/commit/connect 가 (agent,fence)를 주면 task.owner==agent ∧ write-orbit
HELD ∧ fence==f 를 재검증한다(FENCED_OUT). 오추방됐다 살아난 좀비가 남의 작업을 finish/commit/
connect 해 분열을 부르는 경로를 닫는다. bail_epoch 는 GC-pause 좀비의 부활을 차단한다 —
회수가 epoch 를 bump 하고, 회수 전 epoch 를 든 변이는 전부 FENCED_OUT.

크래시/사망/오추방 실패경로: 보유자가 죽으면(reclaim) 모든 후속 변이가 FENCED_OUT 으로 기상되어
영구 hang/고아/분열이 없음을 직접 확인한다.
"""

import time

from pathlib import Path

from omd_server import Coordinator


def _setup_task(omd, task="A", agent="agA", paths="a/**"):
    omd.declare(task, writes=[paths])
    omd.next_task(agent)
    r = omd.claim(agent, [paths], "write", task_id=task)
    omd.start(task, agent)
    return r


# ---------- finish: owner+fence 가드 ----------
def test_finish_rejects_stale_fence(tmp_path):
    """작업 중 write-orbit fence 가 bump(ABA)됐으면 finish 는 FENCED_OUT — task 미완료."""
    omd = Coordinator(db_path=str(tmp_path / "omd.db"))
    r = _setup_task(omd)
    captured = r["fence"]
    with omd.store.tx():  # ABA: 같은 궤도가 HELD 인 채 fence 만 바뀜(만료→재부여 모사)
        omd.store.set_orbit(r["orbit_id"], state="HELD", fence=captured + 100)
    res = omd.finish("A", "agA", captured)
    assert res["ok"] is False and res.get("fenced_out"), res
    assert omd.store.get_task("A")["state"] == "IN_ORBIT"  # 완료 안 됨


def test_finish_rejects_non_owner(tmp_path):
    """다른 agent 가 남의 task 를 finish 하려 하면 거부(owner 가드)."""
    omd = Coordinator(db_path=str(tmp_path / "omd.db"))
    r = _setup_task(omd)
    res = omd.finish("A", "agZ", r["fence"])
    assert res["ok"] is False and res["reason"] == "not owner", res
    assert omd.store.get_task("A")["state"] == "IN_ORBIT"


def test_finish_owner_fence_ok(tmp_path):
    omd = Coordinator(db_path=str(tmp_path / "omd.db"))
    r = _setup_task(omd)
    res = omd.finish("A", "agA", r["fence"])
    assert res["task_id"] == "A" and res["state"] == "DONE", res
    assert omd.store.get_flag("A") == "done"


def test_finish_backcompat_no_args(tmp_path):
    """무인자 finish(증분2까지) 는 그대로 동작(가드 opt-in)."""
    omd = Coordinator(db_path=str(tmp_path / "omd.db"))
    _setup_task(omd)
    assert omd.finish("A")["state"] == "DONE"


# ---------- commit: owner+fence 가드 (repo 바인딩) ----------
def _init_repo(root: Path):
    import subprocess
    def g(*a):
        subprocess.run(["git", *a], cwd=str(root), check=True, capture_output=True, text=True)
    root.mkdir()
    g("init", "-b", "main"); g("config", "user.name", "t"); g("config", "user.email", "t@t")
    (root / "README.md").write_text("base\n")
    g("add", "-A"); g("commit", "-m", "base"); g("checkout", "-b", "dev")


def test_commit_rejects_stale_fence(tmp_path):
    """오추방 좀비가 남의 worktree 를 커밋 못 하게 — fence 불일치면 FENCED_OUT(커밋 안 함)."""
    repo = tmp_path / "repo"; _init_repo(repo)
    omd = Coordinator(db_path=str(tmp_path / "omd.db"), repo=str(repo),
                      worktrees_dir=str(tmp_path / "wt"), integration_branch="main")
    r = _setup_task(omd)
    s = omd.store.get_task("A")
    (Path(s["worktree"]) / "a").mkdir(parents=True)
    (Path(s["worktree"]) / "a" / "x.py").write_text("x = 1\n")
    res = omd.commit("A", "feat: a/x", "agA", r["fence"] + 99)  # 낡은 fence
    assert res["ok"] is False and res.get("fenced_out"), res


def test_commit_owner_fence_ok(tmp_path):
    repo = tmp_path / "repo"; _init_repo(repo)
    omd = Coordinator(db_path=str(tmp_path / "omd.db"), repo=str(repo),
                      worktrees_dir=str(tmp_path / "wt"), integration_branch="main")
    r = _setup_task(omd)
    s = omd.store.get_task("A")
    (Path(s["worktree"]) / "a").mkdir(parents=True)
    (Path(s["worktree"]) / "a" / "x.py").write_text("x = 1\n")
    assert omd.commit("A", "feat: a/x", "agA", r["fence"])["ok"]


# ---------- connect: bail_epoch / 좀비 부활 차단 ----------
def test_connect_after_reclaim_is_fenced_out(tmp_path):
    """크래시 실패경로: write-orbit 보유 task agent 가 회수(좀비)되면, 그 agent 의 connect 는
    FENCED_OUT — 회수로 궤도가 EXPIRED 됐고 agent 가 RETIRED 라 부활 못 함(분열/고아 없음)."""
    repo = tmp_path / "repo"; _init_repo(repo)
    omd = Coordinator(db_path=str(tmp_path / "omd.db"), repo=str(repo),
                      worktrees_dir=str(tmp_path / "wt"), integration_branch="main",
                      agent_ttl=0.03)
    r = _setup_task(omd)
    s = omd.store.get_task("A")
    (Path(s["worktree"]) / "a").mkdir(parents=True)
    (Path(s["worktree"]) / "a" / "x.py").write_text("x = 1\n")
    omd.commit("A", "feat: a/x", "agA", r["fence"])
    omd.finish("A", "agA", r["fence"])
    # agA 가 GC-pause 로 heartbeat 끊김 → 좀비 회수.
    time.sleep(0.05)
    omd.reclaim_zombies()
    assert omd.store.get_agent("agA")["state"] == "RETIRED"
    # 살아난 좀비가 connect 시도 → bail_epoch/state 가드로 FENCED_OUT, merge 없음.
    be_before = r["bail_epoch"]
    res = omd.connect("A", "agA", r["fence"], bail_epoch=be_before)
    assert res["ok"] is False and res.get("fenced_out"), res
    assert not (Path(omd.integration_worktree) / "a" / "x.py").exists()


def test_stale_bail_epoch_blocks_resurrected_zombie(tmp_path):
    """순수 bail_epoch 이빨: GC-pause 좀비가 회수 뒤 깨어나 옛 bail_epoch 로 claim 하려 하면
    FENCED_OUT — heartbeat 의 state 리셋으로도 못 우회(epoch 단조)."""
    omd = Coordinator(db_path=str(tmp_path / "omd.db"))
    r = omd.claim("agA", ["a/**"], "write")
    be0 = r["bail_epoch"]
    omd.bail("agA")                       # 회수 → bail_epoch bump, RETIRED
    # 좀비가 heartbeat 로 부활 시도 → RETIRED 라 fenced_out 회신(부활 안 됨)
    hb = omd.heartbeat("agA")
    assert hb.get("fenced_out"), hb
    # 좀비가 옛 epoch 로 claim 시도 → 차단
    res = omd.claim("agA", ["b/**"], "write", bail_epoch=be0)
    assert res.get("fenced_out"), res


def test_renew_with_stale_bail_epoch_blocked(tmp_path):
    """회수된 좀비의 renew 는 bail_epoch 불일치로 FENCED_OUT(잃은 궤도 부활 방지)."""
    omd = Coordinator(db_path=str(tmp_path / "omd.db"))
    r = omd.claim("agA", ["a/**"], "write")
    be0 = r["bail_epoch"]
    omd.bail("agA")
    res = omd.renew(r["orbit_id"], "agA", r["fence"], bail_epoch=be0)
    assert res.get("fenced_out"), res


def test_heartbeat_returns_bail_epoch(tmp_path):
    """살아있는 agent 의 heartbeat 는 현재 bail_epoch 회신(물방울이 변이에 실어 보냄)."""
    omd = Coordinator(db_path=str(tmp_path / "omd.db"))
    omd.claim("agA", ["a/**"], "write")
    hb = omd.heartbeat("agA")
    assert hb["ok"] and hb["bail_epoch"] == 0
