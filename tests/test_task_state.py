"""RED-first 단위가드: omd_server.task_state 순수모듈 (K8s-흡수 직교 condition).

tasks.state 는 전이-가드된 authoritative lifecycle(fsm.py)로 불변 — 이 모듈은 그 위에 K8s식
*직교 condition* 4개(deps_satisfied·held·heartbeat_fresh·merge_ready)를 store-join 에서 파생하고,
phase-summary 를 그 fold 의 *관측 rollup* 으로 낸다(client set 경로 없음, node_state.py 선례).

순수 단위 — SQLite 없이 _FakeStore 로 3개 store 메서드(get_task/orbits_for_task/get_agent)만 만족.
"""
import json

from omd_server import task_state


class _FakeStore:
    """task_state 가 부르는 3개 store 메서드만 흉내내는 순수 stub."""

    def __init__(self, tasks=None, orbits=None, agents=None):
        self._tasks = tasks or {}
        self._orbits = orbits or {}
        self._agents = agents or {}

    def get_task(self, tid):
        return self._tasks.get(tid)

    def orbits_for_task(self, tid):
        return self._orbits.get(tid, [])

    def get_agent(self, aid):
        return self._agents.get(aid)


def _task(task_id="t1", deps=(), state="PENDING", agent_id=None):
    return {"task_id": task_id, "deps": json.dumps(list(deps)),
            "state": state, "agent_id": agent_id}


# ---- deps_satisfied: core.py:1115 인라인 술어의 SSOT 추출 (dangling-dep-blocks 의미 보존) ----

def test_deps_satisfied_vacuous_over_empty():
    assert task_state.deps_satisfied(_task(deps=()), _FakeStore()) is True


def test_deps_satisfied_all_merged():
    store = _FakeStore(tasks={"d1": {"state": "MERGED"}, "d2": {"state": "MERGED"}})
    assert task_state.deps_satisfied(_task(deps=("d1", "d2")), store) is True


def test_deps_satisfied_dangling_dep_blocks_forever():
    # 존재하지 않는 dep-id(get_task->None)는 영구 False — core.py:1115 `or {}` 의미와 동일.
    store = _FakeStore(tasks={"d1": {"state": "MERGED"}})
    assert task_state.deps_satisfied(_task(deps=("d1", "ghost")), store) is False


def test_deps_satisfied_unmerged_dep_blocks():
    store = _FakeStore(tasks={"d1": {"state": "DONE"}})
    assert task_state.deps_satisfied(_task(deps=("d1",)), store) is False


# ---- held: 이 task 가 배타/공유 write-orbit 을 HELD 로 쥐었나 ----

def test_held_true_for_held_write_orbit():
    store = _FakeStore(orbits={"t1": [{"mode": "write", "state": "HELD"}]})
    assert task_state.held(_task(), store) is True


def test_held_true_for_held_shared_orbit():
    store = _FakeStore(orbits={"t1": [{"mode": "shared", "state": "HELD"}]})
    assert task_state.held(_task(), store) is True


def test_held_false_when_released():
    store = _FakeStore(orbits={"t1": [{"mode": "write", "state": "RELEASED"}]})
    assert task_state.held(_task(), store) is False


def test_held_false_when_no_orbit():
    assert task_state.held(_task(), _FakeStore()) is False


# ---- heartbeat_fresh: dead-σ 비차단(None) — absence≠refutation ----

def test_heartbeat_fresh_none_when_unclaimed():
    # agent 미배정 task 는 None(False 아님) — 관측 summary 가 허위 'Stalled' 로 안 읽힌다.
    out = task_state.heartbeat_fresh(_task(agent_id=None), _FakeStore(),
                                     now=100.0, agent_ttl=90.0)
    assert out is None


def test_heartbeat_fresh_none_when_ttl_disabled():
    store = _FakeStore(agents={"a1": {"last_heartbeat": 0.0, "liveness_ttl": None}})
    out = task_state.heartbeat_fresh(_task(agent_id="a1"), store,
                                     now=100.0, agent_ttl=None)
    assert out is None


def test_heartbeat_fresh_true_within_window():
    store = _FakeStore(agents={"a1": {"last_heartbeat": 95.0, "liveness_ttl": None}})
    out = task_state.heartbeat_fresh(_task(agent_id="a1"), store,
                                     now=100.0, agent_ttl=90.0)
    assert out is True


def test_heartbeat_fresh_false_when_stale():
    store = _FakeStore(agents={"a1": {"last_heartbeat": 5.0, "liveness_ttl": None}})
    out = task_state.heartbeat_fresh(_task(agent_id="a1"), store,
                                     now=100.0, agent_ttl=90.0)
    assert out is False


def test_heartbeat_fresh_uses_per_agent_liveness_ttl():
    # liveness_ttl 선언 시 default agent_ttl 대신 자기 창을 쓴다(F2 per-agent 페이스).
    store = _FakeStore(agents={"a1": {"last_heartbeat": 50.0, "liveness_ttl": 3600.0}})
    out = task_state.heartbeat_fresh(_task(agent_id="a1"), store,
                                     now=100.0, agent_ttl=90.0)
    assert out is True


# ---- merge_ready = DONE ∧ held ∧ deps_satisfied ----

def test_merge_ready_true_only_when_done_held_deps():
    store = _FakeStore(orbits={"t1": [{"mode": "write", "state": "HELD"}]})
    c = task_state.task_conditions(_task(state="DONE"), store, now=100.0, agent_ttl=None)
    assert c["merge_ready"] is True


def test_merge_ready_false_when_not_done():
    store = _FakeStore(orbits={"t1": [{"mode": "write", "state": "HELD"}]})
    c = task_state.task_conditions(_task(state="IN_ORBIT"), store, now=100.0, agent_ttl=None)
    assert c["merge_ready"] is False


def test_merge_ready_false_when_not_held():
    c = task_state.task_conditions(_task(state="DONE"), _FakeStore(), now=100.0, agent_ttl=None)
    assert c["merge_ready"] is False


# ---- task_conditions: 4-bool 집계 dict ----

def test_task_conditions_shape():
    c = task_state.task_conditions(_task(), _FakeStore(), now=100.0, agent_ttl=None)
    assert set(c) == {"deps_satisfied", "held", "heartbeat_fresh", "merge_ready"}


# ---- derive_task_phase: 관측 rollup (fsm_state 가 authoritative, summary 는 관측 전용) ----

def _c(deps=True, held=False, hb=None, mr=False):
    return {"deps_satisfied": deps, "held": held, "heartbeat_fresh": hb, "merge_ready": mr}


def test_summary_blocked_when_deps_unmet():
    assert task_state.derive_task_phase(_c(deps=False), "PENDING") == "Blocked"


def test_summary_ready_when_deps_met_idle():
    assert task_state.derive_task_phase(_c(deps=True), "READY") == "Ready"


def test_summary_working_when_held():
    assert task_state.derive_task_phase(_c(deps=True, held=True, hb=True), "IN_ORBIT") == "Working"


def test_summary_merge_ready():
    assert task_state.derive_task_phase(_c(deps=True, held=True, hb=True, mr=True), "DONE") == "MergeReady"


def test_summary_stalled_only_when_heartbeat_false():
    # hb False(명시적 침묵)만 Stalled — None(미청구)은 Stalled 아님(dead-σ).
    assert task_state.derive_task_phase(_c(deps=True, held=True, hb=False), "IN_ORBIT") == "Stalled"


def test_summary_terminal_passthrough():
    # 종료 상태는 그대로 — 관측 rollup 이 authoritative lifecycle 을 뒤엎지 않는다.
    assert task_state.derive_task_phase(_c(deps=True), "MERGED") == "MERGED"
    assert task_state.derive_task_phase(_c(deps=False), "ABORTED") == "ABORTED"
