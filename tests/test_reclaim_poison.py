"""GAP-1 — 좀비회수/bail 재큐 상한(max_reclaims) + POISONED 영구 terminal.

이전: `_reclaim_agent_inline` 이 CLAIMED/IN_ORBIT/CONNECTING 태스크를 abort→requeue(PENDING)로
무제한 재순환시켜, flapping/poison 태스크(매번 크래시)가 슬롯을 영구 잠식할 수 있었다(상한 0).

이제: per-task `reclaims` 카운터가 max_reclaims 를 **초과**하면 requeue 대신 abort→poison
(POISONED, 영구 terminal). sweep/next 가 절대 다시 집지 않는다(무한루프 0). typed 종단 + 감사 이벤트.
"""

import time

from omd_server import Coordinator, Emitter


class _Capture:
    """emit 이벤트를 그대로 모으는 관측 backend(ship(list) 규약)."""

    def __init__(self):
        self.events = []

    def ship(self, envelopes):
        self.events.extend(envelopes)


def _run_one_cycle(omd, task_id, agent):
    """한 회수 사이클: next→claim→start(→IN_ORBIT) 후 bail(자발 회수) → abort→requeue/poison."""
    omd.next_task(agent)
    omd.claim(agent, ["a/**"], task_id=task_id)
    omd.start(task_id, agent)
    return omd.bail(agent)


# ---------- 1) 가드 발동: 상한 초과 → POISONED(더 이상 requeue 안 함) ----------
def test_reclaim_poisons_task_after_exceeding_max_reclaims(tmp_path):
    omd = Coordinator(db_path=str(tmp_path / "omd.db"), max_reclaims=2)
    omd.declare("T", writes=["a/**"])

    # 회수 1,2: reclaims=1,2 (<=2) → 여전히 requeue(PENDING).
    for i in (1, 2):
        res = _run_one_cycle(omd, "T", f"ag{i}")
        assert "T" in res["tasks"] and not res["poisoned"], res
        t = omd.store.get_task("T")
        assert t["state"] == "PENDING" and t["reclaims"] == i, t

    # 회수 3: reclaims=3 (>2) → requeue 대신 POISONED(영구 terminal).
    res = _run_one_cycle(omd, "T", "ag3")
    assert res["poisoned"] == ["T"] and "T" not in res["tasks"], res
    t = omd.store.get_task("T")
    assert t["state"] == "POISONED" and t["reclaims"] == 3, t
    assert t["agent_id"] is None, t


# ---------- 2) POISONED 는 sweep/next 가 절대 다시 집지 않음(무한루프 0) ----------
def test_poisoned_task_is_never_requeued_or_dispatched(tmp_path):
    omd = Coordinator(db_path=str(tmp_path / "omd.db"), max_reclaims=1)
    omd.declare("T", writes=["a/**"])
    _run_one_cycle(omd, "T", "ag1")                     # reclaims=1 (<=1) → requeue
    res = _run_one_cycle(omd, "T", "ag2")               # reclaims=2 (>1) → POISONED
    assert res["poisoned"] == ["T"], res
    assert omd.store.get_task("T")["state"] == "POISONED"

    # next_task 는 POISONED 를 후보에서 제외 → 다른 READY 없으니 None.
    assert omd.next_task("ag3") is None
    # sweep 도 POISONED 를 되살리지 않는다.
    omd.sweep()
    assert omd.store.get_task("T")["state"] == "POISONED"


# ---------- 3) 정상 경로 불변: 상한 미만 회수는 그대로 requeue, 카운터만 증가 ----------
def test_normal_reclaim_still_requeues_below_cap(tmp_path):
    omd = Coordinator(db_path=str(tmp_path / "omd.db"))     # 기본 max_reclaims=3
    assert omd.max_reclaims == 3
    omd.declare("T", writes=["a/**"])
    # 신규 태스크 카운터 = 0.
    assert omd.store.get_task("T")["reclaims"] == 0
    # 회수 한 번 → 여전히 PENDING(재개 가능), 카운터 1.
    res = _run_one_cycle(omd, "T", "ag1")
    assert "T" in res["tasks"] and not res["poisoned"], res
    t = omd.store.get_task("T")
    assert t["state"] == "PENDING" and t["reclaims"] == 1, t
    # 다시 정상 재배정 가능(고아 아님).
    assert omd.next_task("ag2")["task_id"] == "T"


# ---------- 4) 상한 미도달 태스크는 관여 없음(다른 태스크 회수가 오염 안 시킴) ----------
def test_reclaim_counter_is_per_task(tmp_path):
    omd = Coordinator(db_path=str(tmp_path / "omd.db"), max_reclaims=1)
    omd.declare("T", writes=["a/**"])
    omd.declare("U", writes=["b/**"])
    # T 만 두 번 회수 → POISONED. U 는 손대지 않음.
    _run_one_cycle(omd, "T", "agT1")
    _run_one_cycle(omd, "T", "agT2")
    assert omd.store.get_task("T")["state"] == "POISONED"
    u = omd.store.get_task("U")
    assert u["state"] == "PENDING" and u["reclaims"] == 0, u


# ---------- 5) 설정 노브: max_reclaims<=0 → 첫 회수에서 즉시 POISON ----------
def test_max_reclaims_zero_poisons_on_first_reclaim(tmp_path):
    omd = Coordinator(db_path=str(tmp_path / "omd.db"), max_reclaims=0)
    omd.declare("T", writes=["a/**"])
    res = _run_one_cycle(omd, "T", "ag1")
    assert res["poisoned"] == ["T"] and not res["tasks"], res
    assert omd.store.get_task("T")["state"] == "POISONED"


# ---------- 6) 감사 레코드: task_poisoned 이벤트(reason=max_reclaims) 방출 ----------
def test_poison_emits_audit_event(tmp_path):
    cap = _Capture()
    omd = Coordinator(db_path=str(tmp_path / "omd.db"), max_reclaims=0,
                      events=Emitter(cap))
    omd.declare("T", writes=["a/**"])
    _run_one_cycle(omd, "T", "ag1")
    poison_events = [e for e in cap.events if e["event"] == "task_poisoned"]
    assert len(poison_events) == 1, cap.events
    ev = poison_events[0]
    assert ev["task"] == "T" and ev["reason"] == "max_reclaims"
    assert ev["reclaims"] == 1 and ev["limit"] == 0


# ---------- 7) 비자발(좀비 heartbeat 만료) 회수도 상한을 적용 ----------
def test_involuntary_zombie_reclaim_also_poisons(tmp_path):
    omd = Coordinator(db_path=str(tmp_path / "omd.db"), agent_ttl=0.03, max_reclaims=0)
    omd.declare("T", writes=["a/**"])
    omd.next_task("ag1")
    omd.claim("ag1", ["a/**"], task_id="T")
    omd.start("T", "ag1")
    time.sleep(0.05)
    out = omd.reclaim_zombies()                 # heartbeat 만료 → involuntary 회수
    assert out["reclaimed"] == ["ag1"], out     # 회수된 agent id 목록
    # 자발 bail 과 동일 루틴(_reclaim_agent_inline)이라 상한 적용 → 태스크 POISONED.
    assert omd.store.get_task("T")["state"] == "POISONED"
