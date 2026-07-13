"""begin() 원샷 onboarding 계약 가드 (P1 채택 자동화 enabler + P5 verb 접기).

declare→(deps 게이트)→claim→promote→start 를 한 호출로 접는다 — "그냥 begin 하면 OMD 안에서
격리". complete_task(finish→connect) 의 start-side dual. fail-loud: claim 이 서로소 아니면
worktree 발사 없이 {ok:False, stage:'claim'} 로 전파(기아·미통합 스트랜드 방지).
"""
from omd_server.core import Coordinator


def _omd():
    return Coordinator(db_path=":memory:", agent_ttl=None, allow_memory_db=True)


def test_begin_fresh_task_reaches_in_orbit_and_holds_lease():
    omd = _omd()
    r = omd.begin("A", "agA", writes=["src/a/**"])
    assert r["ok"] is True
    assert r["stage"] == "started"
    assert r["state"] == "IN_ORBIT"
    assert r["fence"] == 1                          # 첫 claim → HELD fence 1
    t = omd.store.get_task("A")                      # store 권위(자기보고 아님)
    assert t["state"] == "IN_ORBIT" and t["agent_id"] == "agA"


def test_begin_conflicting_writeset_fails_loud_without_start():
    omd = _omd()
    omd.begin("A", "agA", writes=["src/**"])         # agA 가 src/** 쥠
    r = omd.begin("B", "agB", writes=["src/a/**"])   # 겹침 → claim PENDING
    assert r["ok"] is False
    assert r["stage"] == "claim"
    assert r["state"] in ("PENDING", "DENIED")
    assert omd.store.get_task("B")["state"] != "IN_ORBIT"   # 시작 안 됨(worktree 낭비 0)


def test_begin_blocks_on_unsatisfied_deps():
    omd = _omd()
    omd.declare("dep", writes=["d/**"])              # dep 미MERGED
    r = omd.begin("A", "agA", writes=["src/**"], deps=["dep"])
    assert r["ok"] is False
    assert r["stage"] == "deps"
    assert "dep" in r["unmet"]
    assert omd.store.get_task("A")["state"] != "IN_ORBIT"


def test_begin_idempotent_restart_returns_existing():
    omd = _omd()
    r1 = omd.begin("A", "agA", writes=["src/**"], request_id="req-1")
    r2 = omd.begin("A", "agA", writes=["src/**"], request_id="req-1")
    assert r2["ok"] is True
    assert r2["state"] == "IN_ORBIT"
    assert r1["fence"] == r2["fence"]                # 재발사·중복 orbit 없음(같은 fence)


def test_begin_ok_only_when_store_confirms_in_orbit():
    # INV: ok:True ⟺ store state == IN_ORBIT (거부 은폐 금지, complete_task INV 대칭).
    omd = _omd()
    r = omd.begin("A", "agA", writes=["x/**"])
    assert (r["ok"] is True) == (omd.store.get_task("A")["state"] == "IN_ORBIT")
