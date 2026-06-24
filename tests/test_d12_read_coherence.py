"""증분9 — D12 read-set 코히런스 / 유령 읽기.

§D12 의 핵심: reads 는 저장되나 미사용이었다 → consumer 가 producer 영역을 read 하고 옛 base
위에서 작업하는 동안 producer 가 그 영역에 새 파일(읽을 때 없던 유령)을 응결하면, consumer 는
**조용히 낡은 뷰 위에 빌드**해 자기 머지는 성공하되 로직이 틀린다(SINGULON 은 write-disjointness
만 보장).

기제(증분9):
  - 통합 generation 추적: 응결 1건마다 integration_gen +1. 그 gen 에 통합으로 들어간 write-globs
    를 merge_log 에 기록.
  - consumer 의 read claim 은 그 task 의 read_synced_gen 을 박는다(현 gen). read↔write 는
    배타적이므로(producer 가 그 영역을 쓰려면 read 가 비어야 함) consumer 는 읽은 뒤 read 궤도를
    **release** 한다 — 그래도 read_synced_gen 은 task 에 남아 코히런스를 추적한다.
  - consumer 의 connect 는 read_synced_gen *이후*의 merge 중 자기 선언 reads 와 겹치는 게 있으면
    거부(read_stale) → rebase/재독 후 read_refresh(task) 로 재앵커하고 재시도. = 옛 base 위
    빌드 차단.

정상경로 + 크래시/사망/오추방 실패경로 + fence/owner 거부 + 변이검증(가드 무력화 시 RED).
"""

import time

import pytest

from omd_server import Coordinator


# ---------- 헬퍼 (DB-only — git 없이 상태 불변식 직접 검증) ----------
def _mk(tmp_path, **kw):
    return Coordinator(db_path=str(tmp_path / "omd.db"), **kw)


def _producer_merge(omd, task, area):
    """producer task 를 응결(MERGED)까지. write claim → start → finish → connect."""
    omd.declare(task, writes=[f"{area}/**"])
    omd.next_task(f"ag{task}")
    omd.claim(f"ag{task}", [f"{area}/**"], "write", task_id=task)
    omd.start(task, f"ag{task}")
    omd.finish(task)
    return omd.connect(task)


def _consumer_reads_then_writes(omd, task, write_area, read_area, *, release_read=True):
    """consumer: read_area 를 read claim(코히런스 앵커) 후 release(read↔write 배타), write_area
    write claim 보유. (write_claim, read_claim) 반환."""
    omd.declare(task, writes=[f"{write_area}/**"], reads=[f"{read_area}/**"])
    omd.next_task(f"ag{task}")
    w = omd.claim(f"ag{task}", [f"{write_area}/**"], "write", task_id=task)
    r = omd.claim(f"ag{task}", [f"{read_area}/**"], "read", task_id=task)
    omd.start(task, f"ag{task}")
    if release_read:
        omd.release(r["orbit_id"], f"ag{task}", r["fence"])
    return w, r


# ---------- 1) integration_gen 추적 + read_synced_gen 앵커 ----------
def test_read_claim_anchors_task_synced_gen(tmp_path):
    omd = _mk(tmp_path)
    assert omd.store.integration_gen() == 0
    _producer_merge(omd, "P", "src/db")
    assert omd.store.integration_gen() == 1, "응결 1건 = gen +1"
    # 이후 read claim 한 consumer 는 gen=1 을 task 에 앵커.
    omd.declare("C", writes=["app/**"], reads=["src/api/**"])
    omd.next_task("agC")
    omd.claim("agC", ["app/**"], "write", task_id="C")
    omd.claim("agC", ["src/api/**"], "read", task_id="C")
    assert omd.store.get_task("C")["read_synced_gen"] == 1


# ---------- 2) 유령 읽기 → connect 차단 (정상경로 핵심) ----------
def test_ghost_read_blocks_consumer_connect(tmp_path):
    omd = _mk(tmp_path)
    # consumer C 가 src/api 를 gen 0 에서 읽고 작업 시작.
    w, _ = _consumer_reads_then_writes(omd, "C", write_area="app", read_area="src/api")
    assert omd.store.get_task("C")["read_synced_gen"] == 0
    # producer P 가 src/api 에 유령을 응결 → gen 1, merge_log[1] ⊇ src/api.
    res_p = _producer_merge(omd, "P", "src/api")
    assert res_p["ok"] and res_p["gen"] == 1
    # C 의 connect 는 read_synced_gen(0) 이후 src/api 응결이 자기 reads 와 겹쳐 거부.
    omd.finish("C")
    res_c = omd.connect("C", "agC", w["fence"])
    assert res_c["ok"] is False and res_c["reason"] == "read_stale", res_c
    assert any("src/api" in g for g in res_c["ghost_globs"]), res_c


# ---------- 3) read_refresh 후 connect 통과 (정상 회복경로) ----------
def test_read_refresh_clears_ghost_and_unblocks_connect(tmp_path):
    omd = _mk(tmp_path)
    w, _ = _consumer_reads_then_writes(omd, "C", write_area="app", read_area="src/api")
    _producer_merge(omd, "P", "src/api")
    omd.finish("C")
    assert omd.connect("C", "agC", w["fence"])["reason"] == "read_stale"
    # rebase/재독 완료 선언 → task 의 read-set 을 현 gen 으로 재앵커.
    rr = omd.read_refresh("C", "agC", w["fence"])
    assert rr["ok"] and rr["read_gen"] == omd.store.integration_gen()
    assert omd.store.get_task("C")["read_synced_gen"] == 1
    # 이제 connect 통과.
    res = omd.connect("C", "agC", w["fence"])
    assert res["ok"] and res["state"] == "MERGED", res


# ---------- 4) 겹치지 않는 응결은 차단하지 않음 (거짓-양성 없음) ----------
def test_non_overlapping_merge_does_not_block(tmp_path):
    omd = _mk(tmp_path)
    w, _ = _consumer_reads_then_writes(omd, "C", write_area="app", read_area="src/api")
    # producer 가 **다른** 영역(src/db)을 응결 → C 의 src/api read 와 무관.
    res_p = _producer_merge(omd, "P", "src/db")
    assert res_p["ok"]
    omd.finish("C")
    res = omd.connect("C", "agC", w["fence"])
    assert res["ok"] is True and res["state"] == "MERGED", res


# ---------- 5) read 를 한 적 없는 task 는 코히런스 무관 ----------
def test_task_without_reads_never_blocked(tmp_path):
    omd = _mk(tmp_path)
    # P 먼저 src/api 응결.
    _producer_merge(omd, "P", "src/api")
    # Q 는 read 선언/ claim 없음 → read_synced_gen=None → 유령 검사 무관.
    omd.declare("Q", writes=["app/**"])
    omd.next_task("agQ")
    wq = omd.claim("agQ", ["app/**"], "write", task_id="Q")
    omd.start("Q", "agQ"); omd.finish("Q")
    assert omd.store.get_task("Q")["read_synced_gen"] is None
    assert omd.connect("Q", "agQ", wq["fence"])["ok"] is True


# ---------- 6) D3 플래그 신호 — live read-궤도가 stale 표시되면 flag 신호 ----------
def test_live_read_orbit_stale_emits_d3_flag(tmp_path):
    omd = _mk(tmp_path)
    # release_read=False → read 궤도를 살려둔다. producer 의 write 는 PENDING 되지만
    # _mark_stale_reads 가 응결 시 live read 궤도를 stale 표시 + D3 플래그 신호한다.
    # (이를 위해 producer 가 read 와 *겹치되 배타충돌은 별개 경로*인 케이스 — 여기선
    #  consumer read 를 살려두고, producer 는 read 와 다른 키로 들어오되 글로브가 겹치게.)
    w, r = _consumer_reads_then_writes(omd, "C", write_area="app", read_area="src/api",
                                       release_read=False)
    key = omd._read_stale_key(r["orbit_id"])
    assert omd.store.get_flag_row(key) is None
    # producer 의 write claim 은 read 와 배타충돌 → PENDING. 강제로 응결 경로를 태우기 위해
    # _mark_stale_reads 를 직접 호출(merge Phase C 가 부르는 그 루틴) 해 신호 발생을 검증.
    with omd._cs():
        gen = omd.store.bump_integration_gen()
        omd.store.append_merge_log(gen, "P", ["src/api/**"])
        affected = omd._mark_stale_reads("P", gen, ["src/api/**"])
    assert r["orbit_id"] in affected
    f = omd.store.get_flag_row(key)
    assert f is not None and f["status"] == "LIVE", f
    assert omd.store.get_orbit(r["orbit_id"])["stale"] == 1
    # read_refresh(task) 가 live read 궤도도 재앵커 + 신호 청산.
    omd.read_refresh("C", "agC", w["fence"])
    assert omd.store.get_orbit(r["orbit_id"])["stale"] == 0
    assert omd.store.get_flag_row(key)["status"] == "CLEARED"


# ---------- 7) 크래시/사망: 유령 가진 consumer 가 죽으면 자동 회수(고아 0, 부활차단) ----------
def test_dead_consumer_is_reclaimed_and_revival_blocked(tmp_path):
    omd = _mk(tmp_path, agent_ttl=0.05)
    w, _ = _consumer_reads_then_writes(omd, "C", write_area="app", read_area="src/api")
    _producer_merge(omd, "P", "src/api")
    omd.finish("C")
    assert omd.connect("C", "agC", w["fence"])["reason"] == "read_stale"
    # consumer agC 사망(heartbeat 끊김) → 다음 sweep 이 write-궤도 회수(고아 없음).
    # (C 는 finish 후 DONE 이라 task requeue 대상이 아니지만, write-lease 는 회수돼야 한다.)
    time.sleep(0.06)
    omd.sweep()
    assert omd.store.get_orbit(w["orbit_id"])["state"] != "HELD"   # 고아 없음(lease 회수)
    assert omd.store.get_agent("agC")["state"] == "RETIRED"        # 좀비 회수됨
    # 죽은 consumer 가 부활해 read_refresh/connect 시도해도 부활차단(stale bail_epoch).
    rr = omd.read_refresh("C", "agC", w["fence"], bail_epoch=0)
    assert rr["ok"] is False and rr.get("fenced_out"), rr
    cc = omd.connect("C", "agC", w["fence"], bail_epoch=0)
    assert cc["ok"] is False and cc.get("fenced_out"), cc


# ---------- 8) fence/owner 거부 — 남의 task 를 refresh 못 한다 ----------
def test_read_refresh_rejects_wrong_owner_and_fence(tmp_path):
    omd = _mk(tmp_path)
    w, _ = _consumer_reads_then_writes(omd, "C", write_area="app", read_area="src/api")
    _producer_merge(omd, "P", "src/api")
    omd.finish("C")
    # 잘못된 owner.
    bad_owner = omd.read_refresh("C", "intruder", w["fence"])
    assert bad_owner["ok"] is False and bad_owner.get("fenced_out"), bad_owner
    # 잘못된 fence(stale).
    bad_fence = omd.read_refresh("C", "agC", w["fence"] + 999)
    assert bad_fence["ok"] is False and bad_fence.get("fenced_out"), bad_fence
    # 여전히 차단(가드가 안 풀렸다).
    assert omd.connect("C", "agC", w["fence"])["reason"] == "read_stale"


# ---------- 9) 멱등 — read_refresh request_id 재시도는 같은 결과 ----------
def test_read_refresh_idempotent(tmp_path):
    omd = _mk(tmp_path)
    w, _ = _consumer_reads_then_writes(omd, "C", write_area="app", read_area="src/api")
    _producer_merge(omd, "P", "src/api")
    omd.finish("C")
    a = omd.read_refresh("C", "agC", w["fence"], request_id="req-1")
    b = omd.read_refresh("C", "agC", w["fence"], request_id="req-1")
    assert a["ok"] and b.get("replayed") and b["read_gen"] == a["read_gen"]


# ============ 변이검증(mutation check) — 가드 무력화하면 RED ============
def test_MUTATION_guard_present_ghost_blocks_connect(tmp_path):
    """가드 존재성 못박기: 유령 읽기가 있는 상태에서 connect 는 절대 MERGED 가 되면 안 된다.
    Phase A 의 _ghost_reads 거부 분기를 무력화하면 이 단언이 깨진다(RED → 유령 읽기 분열)."""
    omd = _mk(tmp_path)
    w, _ = _consumer_reads_then_writes(omd, "C", write_area="app", read_area="src/api")
    _producer_merge(omd, "P", "src/api")
    omd.finish("C")
    res = omd.connect("C", "agC", w["fence"])
    assert res.get("state") != "MERGED", "유령 읽기인데 응결되면 옛-base-위-빌드 분열!"
    assert res["reason"] == "read_stale"
