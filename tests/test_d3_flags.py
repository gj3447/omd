"""증분6 — D3 플래그: EPHEMERAL(=lease) vs LATCH(영속·단조) + flag_wait register→poll.

§D3/§1.2/§3.H 의 핵심:
  - LATCH: 영속·단조 사실(done(1)<merged(2)). 하향 set 거부('un-finish 불가'), 동값 멱등,
    소유 개념 없음, 회수 대상 아님(producer 죽어도 살아남음).
  - EPHEMERAL: 소유 신호(build_running 등) = owned+TTL+heartbeat lease. 보유자 죽으면 자동
    clear/BROKEN, reclaim/sweep 단일루틴이 거둔다. owner CAS(타 agent 재set 거부).
  - flag_wait: register→poll(서버 비블로킹). timeout 필수. epoch 재검사로 ABA/유령기상 안전.
    producer 죽으면 BROKEN/PRODUCER_DEAD 로 기상(영구 hang 없음).
  - 의존 해제는 =done 이 아니라 =merged 에 건다(§3.H).

크래시/사망/오추방 실패경로 + fence/owner 거부를 직접 확인한다 — EPHEMERAL 보유자가
죽으면 자동 BROKEN 되어 대기자가 PRODUCER_DEAD 로 기상(영구 hang/고아 0).
"""

import os
import time

import pytest

from omd_server import Coordinator, Emitter

GATES = os.path.join(os.path.dirname(__file__), os.pardir, "gates")


# ---------- LATCH: 단조 (done < merged) ----------
def test_latch_set_and_get(tmp_path):
    omd = Coordinator(db_path=str(tmp_path / "omd.db"))
    r = omd.flag_set("A", "done")
    assert r["ok"] and r["flag_type"] == "LATCH" and r["rank"] == 1
    g = omd.flag_get("A")
    assert g["value"] == "done" and g["status"] == "LIVE" and g["flag_type"] == "LATCH"


def test_latch_monotonic_upgrade(tmp_path):
    omd = Coordinator(db_path=str(tmp_path / "omd.db"))
    omd.flag_set("A", "done")
    r = omd.flag_set("A", "merged")
    assert r["ok"] and r["rank"] == 2 and r["epoch"] == 1


def test_latch_downgrade_rejected(tmp_path):
    """단조 — merged 후 done 으로 되돌리기(un-finish)는 거부. 값 불변."""
    omd = Coordinator(db_path=str(tmp_path / "omd.db"))
    omd.flag_set("A", "done")
    omd.flag_set("A", "merged")
    r = omd.flag_set("A", "done")
    assert r["ok"] is False and r["reason"] == "monotonic downgrade rejected", r
    assert omd.flag_get("A")["value"] == "merged"  # 불변


def test_latch_same_value_idempotent(tmp_path):
    """동값 재발행 = 멱등 no-op(epoch 안 올림)."""
    omd = Coordinator(db_path=str(tmp_path / "omd.db"))
    omd.flag_set("A", "done")
    r = omd.flag_set("A", "done")
    assert r["noop"] and r["epoch"] == 0


def test_latch_survives_producer_death(tmp_path):
    """LATCH 는 회수 대상이 아님 — producer 가 죽어도(bail) 사실은 살아남는다."""
    omd = Coordinator(db_path=str(tmp_path / "omd.db"))
    omd.flag_set("A", "merged", agent_id="agP")
    omd.bail("agP")
    assert omd.flag_get("A")["value"] == "merged"      # 영속
    assert omd.flag_get("A")["status"] == "LIVE"


def test_latch_cannot_be_cleared(tmp_path):
    omd = Coordinator(db_path=str(tmp_path / "omd.db"))
    omd.flag_set("A", "done")
    r = omd.flag_clear("A")
    assert r["ok"] is False and "LATCH" in r["reason"]


# ---------- EPHEMERAL: 소유 신호 (owned + TTL lease) ----------
def test_ephemeral_set_creates_lease(tmp_path):
    omd = Coordinator(db_path=str(tmp_path / "omd.db"))
    r = omd.flag_set("build", "running", agent_id="agP", flag_type="EPHEMERAL", ttl=100)
    assert r["ok"] and r["flag_type"] == "EPHEMERAL" and r["lease_id"]
    lease = omd.store.get_orbit(r["lease_id"])
    assert lease["kind"] == "flag_ephemeral" and lease["state"] == "HELD"
    assert lease["agent_id"] == "agP"


def test_ephemeral_requires_owner(tmp_path):
    omd = Coordinator(db_path=str(tmp_path / "omd.db"))
    r = omd.flag_set("build", "running", flag_type="EPHEMERAL")
    assert r["ok"] is False and "owner" in r["reason"]


def test_ephemeral_owner_cas(tmp_path):
    """owner CAS(§D6 보강) — LIVE EPHEMERAL 플래그는 같은 owner 만 재set, 타 agent 거부."""
    omd = Coordinator(db_path=str(tmp_path / "omd.db"))
    omd.flag_set("build", "running", agent_id="agA", flag_type="EPHEMERAL", ttl=100)
    r = omd.flag_set("build", "running", agent_id="agB", flag_type="EPHEMERAL")
    assert r["ok"] is False and r["reason"] == "not flag owner", r


def test_ephemeral_clear_by_owner(tmp_path):
    """정상 경로: owner 가 작업 끝나면 clear → lease 해제 + CLEARED."""
    omd = Coordinator(db_path=str(tmp_path / "omd.db"))
    r = omd.flag_set("build", "running", agent_id="agA", flag_type="EPHEMERAL", ttl=100)
    c = omd.flag_clear("build", "agA")
    assert c["ok"] and c["status"] == "CLEARED"
    assert omd.store.get_orbit(r["lease_id"])["state"] == "RELEASED"


def test_ephemeral_clear_non_owner_rejected(tmp_path):
    omd = Coordinator(db_path=str(tmp_path / "omd.db"))
    omd.flag_set("build", "running", agent_id="agA", flag_type="EPHEMERAL", ttl=100)
    r = omd.flag_clear("build", "agB")
    assert r["ok"] is False and r["reason"] == "not flag owner"


def test_latch_ephemeral_type_mismatch_rejected(tmp_path):
    """같은 key 를 LATCH 로 세운 뒤 EPHEMERAL 로 덮으려 하면(또는 역) 거부 — 종류 혼동 차단."""
    omd = Coordinator(db_path=str(tmp_path / "omd.db"))
    omd.flag_set("x", "done")  # LATCH
    r = omd.flag_set("x", "running", agent_id="agA", flag_type="EPHEMERAL")
    assert r["ok"] is False and "LATCH" in r["reason"]
    omd2 = Coordinator(db_path=str(tmp_path / "omd2.db"))
    omd2.flag_set("y", "running", agent_id="agA", flag_type="EPHEMERAL", ttl=100)
    r2 = omd2.flag_set("y", "done")  # LATCH over EPHEMERAL
    assert r2["ok"] is False and "EPHEMERAL" in r2["reason"]


# ---------- flag_wait: register → poll ----------
def test_wait_already_satisfied(tmp_path):
    omd = Coordinator(db_path=str(tmp_path / "omd.db"))
    omd.flag_set("A", "merged")
    r = omd.flag_wait("A", "merged", 5.0, "agW")
    assert r["state"] == "SATISFIED"


def test_wait_satisfied_after_set(tmp_path):
    """등록 후 producer 가 set → poll 이 SATISFIED 로 기상."""
    omd = Coordinator(db_path=str(tmp_path / "omd.db"))
    w = omd.flag_wait("A", "merged", 5.0, "agW")
    assert w["state"] == "WAITING"
    assert omd.flag_wait_poll(w["waiter_id"])["state"] == "WAITING"
    omd.flag_set("A", "merged")
    assert omd.flag_wait_poll(w["waiter_id"])["state"] == "SATISFIED"


def test_wait_timeout_required(tmp_path):
    """timeout=None 거부 — 영구 hang 방지(§D3 'wait 는 timeout 필수')."""
    omd = Coordinator(db_path=str(tmp_path / "omd.db"))
    r = omd.flag_wait("A", "done", None, "agW")
    assert r["ok"] is False and "timeout" in r["reason"]


def test_wait_timeout_fires(tmp_path):
    omd = Coordinator(db_path=str(tmp_path / "omd.db"))
    w = omd.flag_wait("A", "done", 0.01, "agW")
    time.sleep(0.02)
    assert omd.flag_wait_poll(w["waiter_id"])["state"] == "TIMEOUT"


def test_wait_dependency_release_on_merged_not_done(tmp_path):
    """§3.H — 의존 해제는 =merged 에 건다. =merged 대기자는 done 으로 만족 안 되고
    merged 로만 만족(이른 의존 해제가 입체 창을 재오픈하는 것을 막음)."""
    omd = Coordinator(db_path=str(tmp_path / "omd.db"))
    w = omd.flag_wait("producer", "merged", 5.0, "consumer")
    omd.flag_set("producer", "done")    # finish — 아직 머지 전
    assert omd.flag_wait_poll(w["waiter_id"])["state"] == "WAITING"  # 해제 안 됨
    omd.flag_set("producer", "merged")  # 응결 완료
    assert omd.flag_wait_poll(w["waiter_id"])["state"] == "SATISFIED"


def test_wait_done_satisfied_by_merged(tmp_path):
    """want='done' 은 상위 랭크 merged 로도 만족(merged ⊃ done 단조)."""
    omd = Coordinator(db_path=str(tmp_path / "omd.db"))
    omd.flag_set("p", "merged")
    assert omd.flag_wait("p", "done", 5.0, "c")["state"] == "SATISFIED"


def test_poll_unknown_waiter(tmp_path):
    omd = Coordinator(db_path=str(tmp_path / "omd.db"))
    r = omd.flag_wait_poll("fw-nope")
    assert r["ok"] is False


def test_poll_epoch_recheck_aba_safe(tmp_path):
    """ABA/유령기상 안전: EPHEMERAL set→clear→set 으로 같은 value 가 다시 LIVE 가 돼도,
    want 가 그 value 면 정상 SATISFIED — epoch 재검사라 유령 기상(이미 처리된 wakeup의
    재현)이 종단 상태를 덮어쓰지 않는다. 여기선 want=value 만족이 정확히 한 번 일어남을 본다."""
    omd = Coordinator(db_path=str(tmp_path / "omd.db"))
    w = omd.flag_wait("build", "running", 5.0, "agW")
    omd.flag_set("build", "running", agent_id="agA", flag_type="EPHEMERAL", ttl=100)
    assert omd.flag_wait_poll(w["waiter_id"])["state"] == "SATISFIED"
    # 종단(SATISFIED)은 이후 clear/재set 로 안 바뀜 — 멱등 재폴.
    omd.flag_clear("build", "agA")
    assert omd.flag_wait_poll(w["waiter_id"])["state"] == "SATISFIED"


# ---------- 크래시/사망/오추방 실패경로 ----------
def test_ephemeral_producer_bail_breaks_flag_and_wakes_waiter(tmp_path):
    """사용자 핵심 시나리오: producer 가 EPHEMERAL 신호를 세우고 대기자가 기다리는데
    producer 가 긴급탈출(bail)하면 → 플래그 자동 BROKEN + 대기자 PRODUCER_DEAD 로 기상
    (영구 데드락 없음). 단일 reclaim 루틴이 거둔다(§1.1/§1.2)."""
    omd = Coordinator(db_path=str(tmp_path / "omd.db"))
    r = omd.flag_set("build", "running", agent_id="agP", flag_type="EPHEMERAL", ttl=100)
    w = omd.flag_wait("build", "done", 5.0, "agW")
    assert w["state"] == "WAITING"
    omd.bail("agP")   # 긴급 탈출
    poll = omd.flag_wait_poll(w["waiter_id"])
    assert poll["state"] == "BROKEN" and poll["reason"] == "producer_dead", poll
    assert omd.flag_get("build")["status"] == "BROKEN"
    assert omd.store.get_orbit(r["lease_id"])["state"] == "EXPIRED"  # lease 거둬짐


def test_ephemeral_producer_zombie_reclaim_breaks_flag(tmp_path):
    """비자발(kill -9 모사): heartbeat 끊겨 좀비 회수되면 같은 루틴이 EPHEMERAL 플래그를
    BROKEN — bail 과 수렴(둘 다 단일 reclaim)."""
    omd = Coordinator(db_path=str(tmp_path / "omd.db"), agent_ttl=0.03)
    omd.flag_set("build", "running", agent_id="agP", flag_type="EPHEMERAL", ttl=100)
    w = omd.flag_wait("build", "done", 5.0, "agW")
    time.sleep(0.05)
    omd.reclaim_zombies()
    assert omd.flag_get("build")["status"] == "BROKEN"
    assert omd.flag_wait_poll(w["waiter_id"])["state"] == "BROKEN"


def test_ephemeral_lease_ttl_expiry_breaks_flag(tmp_path):
    """producer 가 GC-pause 로 renew 깜빡(또는 죽음) → flag_ephemeral lease TTL 만료 →
    sweep 이 자동 BROKEN + 대기자 PRODUCER_DEAD. agent_ttl 없이도(lease TTL 만으로) 풀린다."""
    omd = Coordinator(db_path=str(tmp_path / "omd.db"), agent_ttl=None)
    omd.flag_set("build", "running", agent_id="agP", flag_type="EPHEMERAL", ttl=0.02)
    w = omd.flag_wait("build", "done", 5.0, "agW")
    time.sleep(0.05)
    poll = omd.flag_wait_poll(w["waiter_id"])  # poll 내부 sweep 이 lease 만료 반영
    assert poll["state"] == "BROKEN" and poll["reason"] == "producer_dead", poll


def test_heartbeat_renews_flag_lease(tmp_path):
    """건강한 producer: heartbeat 한 번이 자기 flag_ephemeral lease 를 갱신 — renew 깜빡으로
    자기 신호가 BROKEN 되지 않음(§1.2)."""
    omd = Coordinator(db_path=str(tmp_path / "omd.db"), agent_ttl=None)
    r = omd.flag_set("build", "running", agent_id="agP", flag_type="EPHEMERAL", ttl=0.05)
    time.sleep(0.03)
    hb = omd.heartbeat("agP")
    assert hb["flag_leases_renewed"] == 1
    time.sleep(0.03)  # 원래 TTL(0.05)은 지났지만 heartbeat 가 연장함
    omd.sweep()
    assert omd.flag_get("build")["status"] == "LIVE"  # 아직 살아있음
    assert omd.store.get_orbit(r["lease_id"])["state"] == "HELD"


def test_reclaimed_zombie_cannot_set_flag(tmp_path):
    """오추방/회수된 좀비는 flag_set 못 함(bail_epoch/state 가드, §D6) — 부활 차단."""
    omd = Coordinator(db_path=str(tmp_path / "omd.db"))
    omd.flag_set("build", "running", agent_id="agP", flag_type="EPHEMERAL", ttl=100)
    omd.bail("agP")  # agP RETIRED
    r = omd.flag_set("other", "x", agent_id="agP", flag_type="EPHEMERAL")
    assert r.get("fenced_out"), r


def test_request_id_idempotent_flag_set(tmp_path):
    """§D9: 같은 request_id 재시도는 두 번째 효과(예: 두 lease)를 안 만든다."""
    omd = Coordinator(db_path=str(tmp_path / "omd.db"))
    r1 = omd.flag_set("build", "running", agent_id="agA", flag_type="EPHEMERAL",
                      ttl=100, request_id="req-1")
    r2 = omd.flag_set("build", "running", agent_id="agA", flag_type="EPHEMERAL",
                      ttl=100, request_id="req-1")
    assert r2.get("replayed") and r2["lease_id"] == r1["lease_id"]
    # flag_ephemeral lease 가 정확히 1개여야(재시도 누수 0).
    leases = omd.store.flag_leases_owned_by("agA", ("HELD",))
    assert len(leases) == 1


# ---------- LTDD 트레이스 게이트 ----------
def test_flag_producer_death_trace_arrives(tmp_path):
    """LTDD(증분6): EPHEMERAL set → producer 사망 → flag_broken(producer_dead) 트레이스가
    순서대로 store 에 도착(gates/flag.yaml). 관측가능 동작 = 자동 clear 의 외부 증거."""
    pytest.importorskip("ooptdd")
    from ooptdd.backends import MemoryBackend, memory as mem
    from ooptdd.gate import evaluate, evidence_tier, load_gate

    mem.reset()
    backend = MemoryBackend()
    omd = Coordinator(db_path=str(tmp_path / "omd.db"), events=Emitter(backend))
    omd.flag_set("build", "running", agent_id="omd-flag-demo", flag_type="EPHEMERAL",
                 ttl=100)
    omd.bail("omd-flag-demo")   # producer 사망 → flag_broken

    res = evaluate(backend, load_gate(os.path.join(GATES, "flag.yaml")))
    assert res["ok"], res
    assert evidence_tier(res) == "arrived"
