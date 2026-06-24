"""증분7 — D4 크래시 안전 세마포어: permit=lease, 가용 = max − count(ACTIVE).

§D4/§D7/§G/§1.2 의 핵심:
  - permit 은 정수 카운터가 아니라 **owned+TTL+fenced lease**(orbits.kind='sem_permit').
    가용 = max − count(ACTIVE permit) — 보유자가 죽으면 permit 이 EXPIRED 되며 슬롯이
    구조적으로 복구된다(누수 0). 정수 카운터의 고전 버그(죽을 때마다 새서 결국 0=영구정지)를
    원천 차단.
  - 임계구역(D1)에서 check-then-grant 가 원자 → **초과배정 불가**(두 acquirer 가 동시에
    N-1 보고 둘 다 N+1번째 부여하는 레이스 차단).
  - no-overtaking(§D7): 가용 슬롯이 생겨도 먼저 줄선 자가 있으면 양보(기아 방지).
  - 멱등 reuse: 이미 보유하면 재발급 안 함(MCP 재시도 안전).
  - reclaim 단일루틴이 죽은 보유자 permit 을 거둬 슬롯 복구(bail/좀비/TTL-만료 셋 다 수렴).

크래시/사망/오추방 실패경로 + fence/owner 거부를 직접 확인한다.
"""

import os
import threading
import time

import pytest

from omd_server import Coordinator, Emitter

GATES = os.path.join(os.path.dirname(__file__), os.pardir, "gates")


# ---------- 정상 경로 ----------
def test_declare_and_acquire(tmp_path):
    omd = Coordinator(db_path=str(tmp_path / "omd.db"))
    assert omd.sem_declare("build", 2)["ok"]
    r = omd.acquire("agA", "build", ttl=100)
    assert r["ok"] and r["state"] == "ACQUIRED" and r["permit_id"]
    p = omd.store.get_orbit(r["permit_id"])
    assert p["kind"] == "sem_permit" and p["state"] == "HELD" and p["agent_id"] == "agA"
    assert p["resource_key"] == "build"


def test_acquire_unknown_sem(tmp_path):
    omd = Coordinator(db_path=str(tmp_path / "omd.db"))
    r = omd.acquire("agA", "nope")
    assert r["ok"] is False and "no such semaphore" in r["reason"]


def test_capacity_not_exceeded(tmp_path):
    """max=2 면 정확히 2개까지만 ACTIVE. 3번째는 no_wait 면 FAIL."""
    omd = Coordinator(db_path=str(tmp_path / "omd.db"))
    omd.sem_declare("build", 2)
    assert omd.acquire("agA", "build")["state"] == "ACQUIRED"
    assert omd.acquire("agB", "build")["state"] == "ACQUIRED"
    r = omd.acquire("agC", "build", no_wait=True)
    assert r["state"] == "FAIL" and r["ok"] is False
    assert omd.sem_status("build")["active"] == 2
    assert omd.sem_status("build")["available"] == 0


def test_available_is_count_not_counter(tmp_path):
    """가용 = max − count(ACTIVE). release 하면 즉시 가용 복구(저장 정수가 아님)."""
    omd = Coordinator(db_path=str(tmp_path / "omd.db"))
    omd.sem_declare("build", 1)
    a = omd.acquire("agA", "build")
    assert omd.sem_status("build")["available"] == 0
    omd.sem_release(a["permit_id"], "agA", a["fence"])
    assert omd.sem_status("build")["available"] == 1
    assert omd.acquire("agB", "build")["state"] == "ACQUIRED"


def test_idempotent_reuse(tmp_path):
    """이미 ACTIVE permit 을 쥔 agent 의 재acquire = 같은 permit 반환(재발급 안 함). 누수 0."""
    omd = Coordinator(db_path=str(tmp_path / "omd.db"))
    omd.sem_declare("build", 2)
    r1 = omd.acquire("agA", "build")
    r2 = omd.acquire("agA", "build")
    assert r2["permit_id"] == r1["permit_id"] and r2.get("reuse")
    # 활성 permit 은 정확히 1개여야(중복 발급 0).
    assert len(omd.store.active_permits("build")) == 1
    assert omd.sem_status("build")["active"] == 1


def test_request_id_idempotent(tmp_path):
    """§D9: 같은 request_id 재시도는 두 번째 permit 을 안 만든다."""
    omd = Coordinator(db_path=str(tmp_path / "omd.db"))
    omd.sem_declare("build", 2)
    r1 = omd.acquire("agA", "build", request_id="req-1")
    r2 = omd.acquire("agA", "build", request_id="req-1")
    assert r2.get("replayed") and r2["permit_id"] == r1["permit_id"]
    assert len(omd.store.active_permits("build")) == 1


# ---------- 초과배정 불가 (동시 acquire 레이스) ----------
def test_no_overprovision_under_concurrency(tmp_path):
    """N 동시 acquire, max=3 → 정확히 3개만 ACTIVE(초과배정 0). 임계구역이 check-then-grant 를
    원자화하므로 두 acquirer 가 같은 슬롯을 동시 보고 둘 다 부여하는 레이스가 없다(§D4)."""
    omd = Coordinator(db_path=str(tmp_path / "omd.db"))
    omd.sem_declare("build", 3)
    results = {}
    barrier = threading.Barrier(8)

    def worker(i):
        barrier.wait()
        results[i] = omd.acquire(f"ag{i}", "build", no_wait=True)

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    acquired = [i for i, r in results.items() if r["state"] == "ACQUIRED"]
    assert len(acquired) == 3, results
    # store 진실: 활성 permit 도 정확히 3.
    assert len(omd.store.active_permits("build")) == 3


# ---------- no-overtaking (§D7) ----------
def test_no_overtaking(tmp_path):
    """max=1. agA 보유 → agB 대기 등록 → 슬롯 1개 나면 (먼저 줄선) agB 가 받고, 나중에 온
    agC 는 못 끼어든다(작은 acquire 스트림이 head 대기자를 굶기는 것 방지)."""
    omd = Coordinator(db_path=str(tmp_path / "omd.db"))
    omd.sem_declare("build", 1)
    a = omd.acquire("agA", "build")
    wB = omd.acquire("agB", "build")           # 대기(no_wait=False 기본)
    assert wB["state"] == "WAITING"
    # agC 가 끼어들기 시도 — 먼저 줄선 agB 가 있으니 즉시 부여받지 못하고 대기.
    wC = omd.acquire("agC", "build")
    assert wC["state"] == "WAITING"
    omd.sem_release(a["permit_id"], "agA", a["fence"])  # 슬롯 1개 복구
    # head(agB) 가 받는다 — agC 아님.
    assert omd.acquire_poll(wB["waiter_id"])["state"] == "ACQUIRED"
    assert omd.acquire_poll(wC["waiter_id"])["state"] == "WAITING"


def test_fresh_acquire_yields_to_queued_waiter(tmp_path):
    """no-overtaking 가드의 이빨(territory): 빈 슬롯이 있어도 **이미 줄선 대기자**가 있으면
    새로 온 acquire 는 즉시 부여받지 못하고 양보(대기 등록)해야 한다 — 작은 acquire 스트림이
    head 대기자를 영구 기아시키는 writer-starvation 방지(§D7).

    정상 경로(release/sweep)는 promote 가 즉시 head 에게 슬롯을 주므로 '빈 슬롯 + 대기자' 상태가
    거의 안 생긴다. 그래서 그 상태를 직접 만들어(대기자 등록 후 promote 안 함) 새 acquire 가
    가드에 걸리는지 본다 — 가드를 끄면 새 agent 가 슬롯을 가로채 RED."""
    omd = Coordinator(db_path=str(tmp_path / "omd.db"))
    omd.sem_declare("build", 1)
    a = omd.acquire("agA", "build")
    wB = omd.acquire("agB", "build")          # 대기 등록
    assert wB["state"] == "WAITING"
    # 슬롯을 promote 없이 비운다(store 직접 — 보유자 사망의 한 순간을 모사하되 promote 전).
    omd.store.set_orbit(a["permit_id"], state="EXPIRED")
    # 이제 빈 슬롯(가용 1) + 줄선 agB 가 공존. 새로 온 agC 의 acquire 는 가로채면 안 됨.
    rC = omd.acquire("agC", "build")
    assert rC["state"] == "WAITING", rC      # 양보 — agB 가 head
    # agB 가 poll 하면 자기가 받는다(head, no-overtaking 보존).
    assert omd.acquire_poll(wB["waiter_id"])["state"] == "ACQUIRED"


def test_priority_ordering(tmp_path):
    """우선순위 높은 대기자가 먼저(우선순위 DESC). 같은 우선순위는 FIFO."""
    omd = Coordinator(db_path=str(tmp_path / "omd.db"))
    omd.sem_declare("build", 1)
    a = omd.acquire("agA", "build")
    wLow = omd.acquire("agLow", "build", priority=0)
    wHigh = omd.acquire("agHigh", "build", priority=5)
    assert wLow["state"] == "WAITING" and wHigh["state"] == "WAITING"
    omd.sem_release(a["permit_id"], "agA", a["fence"])
    assert omd.acquire_poll(wHigh["waiter_id"])["state"] == "ACQUIRED"
    assert omd.acquire_poll(wLow["waiter_id"])["state"] == "WAITING"


# ---------- fence/owner 거부 (§D6) ----------
def test_release_wrong_owner_rejected(tmp_path):
    omd = Coordinator(db_path=str(tmp_path / "omd.db"))
    omd.sem_declare("build", 2)
    a = omd.acquire("agA", "build")
    r = omd.sem_release(a["permit_id"], "agB", a["fence"])  # 남이 해제 시도
    assert r["ok"] is False and r["reason"] == "not owner"
    assert omd.store.get_orbit(a["permit_id"])["state"] == "HELD"  # 안 풀림


def test_release_stale_fence_rejected(tmp_path):
    omd = Coordinator(db_path=str(tmp_path / "omd.db"))
    omd.sem_declare("build", 2)
    a = omd.acquire("agA", "build")
    r = omd.sem_release(a["permit_id"], "agA", a["fence"] + 999)  # 낡은 fence
    assert r["ok"] is False and r.get("fenced_out")
    assert omd.store.get_orbit(a["permit_id"])["state"] == "HELD"


def test_release_idempotent(tmp_path):
    """이미 RELEASED 면 멱등 OK(MCP 재시도 안전)."""
    omd = Coordinator(db_path=str(tmp_path / "omd.db"))
    omd.sem_declare("build", 2)
    a = omd.acquire("agA", "build")
    omd.sem_release(a["permit_id"], "agA", a["fence"])
    r = omd.sem_release(a["permit_id"], "agA", a["fence"])
    assert r["ok"] and r.get("noop")


# ---------- 크래시/사망/오추방 실패경로 ----------
def test_bail_holder_recovers_slot(tmp_path):
    """사용자 핵심 시나리오: permit 보유자가 긴급탈출(bail)하면 → permit EXPIRE → 슬롯 복구.
    영구 정지 없음. 단일 reclaim 루틴이 거둔다(§1.1)."""
    omd = Coordinator(db_path=str(tmp_path / "omd.db"))
    omd.sem_declare("build", 1)
    a = omd.acquire("agA", "build")
    assert omd.sem_status("build")["available"] == 0
    omd.bail("agA")    # 긴급 탈출
    assert omd.store.get_orbit(a["permit_id"])["state"] == "EXPIRED"  # permit 거둬짐
    assert omd.sem_status("build")["available"] == 1                  # 슬롯 복구
    assert omd.acquire("agB", "build")["state"] == "ACQUIRED"         # 재사용 가능


def test_bail_holder_wakes_waiter(tmp_path):
    """보유자 사망 → 대기자가 영구 hang 하지 않고 복구된 슬롯을 받는다(poll 로 기상)."""
    omd = Coordinator(db_path=str(tmp_path / "omd.db"))
    omd.sem_declare("build", 1)
    a = omd.acquire("agA", "build")
    wB = omd.acquire("agB", "build")
    assert wB["state"] == "WAITING"
    omd.bail("agA")
    assert omd.acquire_poll(wB["waiter_id"])["state"] == "ACQUIRED"


def test_zombie_reclaim_recovers_slot(tmp_path):
    """비자발(kill -9 모사): heartbeat 끊겨 좀비 회수 → permit EXPIRE → 슬롯 복구. bail 과 수렴."""
    omd = Coordinator(db_path=str(tmp_path / "omd.db"), agent_ttl=0.03)
    omd.sem_declare("build", 1)
    a = omd.acquire("agA", "build", ttl=100)   # permit TTL 은 길게 — heartbeat 만료가 먼저
    time.sleep(0.05)
    omd.reclaim_zombies()
    assert omd.store.get_orbit(a["permit_id"])["state"] == "EXPIRED"
    assert omd.sem_status("build")["available"] == 1


def test_permit_ttl_expiry_recovers_slot(tmp_path):
    """permit TTL 만료(GC-pause/renew 깜빡) → sweep 이 EXPIRE → 슬롯 복구. agent_ttl 없이도.
    정수 카운터였다면 영구 누수됐을 슬롯이 lease 라서 자동 복구된다(§D4 핵심)."""
    omd = Coordinator(db_path=str(tmp_path / "omd.db"), agent_ttl=None)
    omd.sem_declare("build", 1)
    a = omd.acquire("agA", "build", ttl=0.02)
    time.sleep(0.05)
    assert omd.sem_status("build")["available"] == 1  # sem_status 내부 sweep 이 복구
    assert omd.store.get_orbit(a["permit_id"])["state"] == "EXPIRED"


def test_heartbeat_renews_permit(tmp_path):
    """건강한 보유자: heartbeat 한 번이 자기 permit 을 연장 — renew 깜빡으로 슬롯을 잃지 않음(§G)."""
    omd = Coordinator(db_path=str(tmp_path / "omd.db"), agent_ttl=None)
    omd.sem_declare("build", 1)
    a = omd.acquire("agA", "build", ttl=0.05)
    time.sleep(0.03)
    hb = omd.heartbeat("agA")
    assert hb["sem_permits_renewed"] == 1
    time.sleep(0.03)   # 원래 TTL(0.05)은 지났지만 heartbeat 가 연장함
    omd.sweep()
    assert omd.store.get_orbit(a["permit_id"])["state"] == "HELD"  # 아직 살아있음
    assert omd.sem_status("build")["available"] == 0


def test_reclaimed_zombie_cannot_acquire(tmp_path):
    """오추방/회수된 좀비는 acquire 못 함(bail_epoch/state 가드, §D6) — 부활 차단."""
    omd = Coordinator(db_path=str(tmp_path / "omd.db"))
    omd.sem_declare("build", 2)
    omd.acquire("agP", "build")
    omd.bail("agP")  # agP RETIRED
    r = omd.acquire("agP", "build")
    assert r.get("fenced_out"), r


def test_waiter_death_cancels_wait(tmp_path):
    """대기 중이던 agent 가 죽으면 그 대기 등록이 취소되고, 슬롯은 다음 대기자에게."""
    omd = Coordinator(db_path=str(tmp_path / "omd.db"))
    omd.sem_declare("build", 1)
    a = omd.acquire("agA", "build")
    wB = omd.acquire("agB", "build")
    wC = omd.acquire("agC", "build")
    omd.bail("agB")   # 대기 중 agB 사망
    assert omd.acquire_poll(wB["waiter_id"])["state"] == "CANCELLED"
    omd.sem_release(a["permit_id"], "agA", a["fence"])
    # agB 가 취소됐으니 슬롯은 agC 로.
    assert omd.acquire_poll(wC["waiter_id"])["state"] == "ACQUIRED"


def test_wait_timeout(tmp_path):
    """슬롯이 안 나면 대기자는 TTL deadline 후 TIMEOUT(영구 hang 없음)."""
    omd = Coordinator(db_path=str(tmp_path / "omd.db"))
    omd.sem_declare("build", 1)
    omd.acquire("agA", "build", ttl=100)
    wB = omd.acquire("agB", "build", ttl=0.02)
    time.sleep(0.05)
    assert omd.acquire_poll(wB["waiter_id"])["state"] == "TIMEOUT"


# ---------- LTDD 트레이스 게이트 ----------
def test_sem_holder_death_trace_arrives(tmp_path):
    """LTDD(증분7): acquire(sem_acquired) → 보유자 사망 → sem_permit_reclaimed 트레이스가
    순서대로 store 에 도착(gates/semaphore.yaml). 관측가능 동작 = 슬롯 복구의 외부 증거."""
    pytest.importorskip("ooptdd")
    from ooptdd.backends import MemoryBackend, memory as mem
    from ooptdd.gate import evaluate, evidence_tier, load_gate

    mem.reset()
    backend = MemoryBackend()
    omd = Coordinator(db_path=str(tmp_path / "omd.db"), events=Emitter(backend))
    omd.sem_declare("build", 1)
    omd.acquire("omd-sem-demo", "build", ttl=100)
    omd.bail("omd-sem-demo")   # 보유자 사망 → sem_permit_reclaimed

    res = evaluate(backend, load_gate(os.path.join(GATES, "semaphore.yaml")))
    assert res["ok"], res
    assert evidence_tier(res) == "arrived"
