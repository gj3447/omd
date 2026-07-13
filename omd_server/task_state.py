"""K8s-흡수 직교 condition — tasks.state(fsm authoritative) 위의 관측 rollup.

tasks.state 는 전이-가드된 authoritative lifecycle(fsm.py) 로 불변. 이 모듈은 그 위에 store-join
에서 *직교 condition* 4개(deps_satisfied·held·heartbeat_fresh·merge_ready)를 파생하고,
phase-summary 를 그 fold 의 *관측 rollup* 으로 낸다(client set 경로 없음 — K8s Pod.status.conditions
선례). fsm_state 가 authoritative — summary 는 관측 전용이라 lifecycle 을 뒤엎지 않는다.

순수 — store 는 get_task/orbits_for_task/get_agent 3메서드만 요구(SQLite 불필요, stub 단위테스트 가능).
deps_satisfied 는 core.next_task 인라인 술어(core.py `(get_task(d) or {}).get('state')=='MERGED'`)의
SSOT 추출 — dangling-dep(존재하지 않는 dep-id) 영구차단 의미를 보존한다.
"""
from __future__ import annotations

import json

WRITE_MODES = ("write", "shared")
TERMINAL = ("MERGED", "ABORTED")   # 관측 rollup 이 뒤엎지 않는 종료 상태(passthrough)


def _deps(task) -> list:
    raw = task.get("deps")
    if not raw:
        return []
    return raw if isinstance(raw, list) else json.loads(raw)


def deps_satisfied(task, store) -> bool:
    """모든 dep 이 MERGED 인가(빈 deps 는 vacuous True). 존재하지 않는 dep-id(get_task->None)는
    영구 False — core.next_task 인라인 술어와 동일 의미(SSOT, dangling-dep-blocks-forever)."""
    return all((store.get_task(d) or {}).get("state") == "MERGED" for d in _deps(task))


def held(task, store) -> bool:
    """이 task 가 배타/공유 write-orbit 을 HELD 로 쥐었나(read↔read 는 held 아님)."""
    return any(o.get("mode") in WRITE_MODES and o.get("state") == "HELD"
               for o in store.orbits_for_task(task["task_id"]))


def heartbeat_fresh(task, store, now, agent_ttl):
    """청구 agent 의 heartbeat 가 생존창 안인가. 미청구/ttl-비활성/미상은 None(dead-σ 비차단 —
    absence≠refutation: 관측 summary 가 허위 'Stalled' 로 안 읽히게). per-agent liveness_ttl 이
    선언돼 있으면 그 창을, 없으면 default agent_ttl 을 쓴다(F2 per-agent 페이스)."""
    aid = task.get("agent_id")
    if not aid:
        return None
    agent = store.get_agent(aid)
    if not agent:
        return None
    ttl = agent.get("liveness_ttl")
    if ttl is None:
        ttl = agent_ttl
    if ttl is None:
        return None
    last = agent.get("last_heartbeat")
    if last is None:
        return None
    return (now - last) <= ttl


def task_conditions(task, store, now, agent_ttl) -> dict:
    """4-bool 직교 condition 집계. merge_ready = DONE ∧ held ∧ deps_satisfied."""
    d = deps_satisfied(task, store)
    h = held(task, store)
    return {
        "deps_satisfied": d,
        "held": h,
        "heartbeat_fresh": heartbeat_fresh(task, store, now, agent_ttl),
        "merge_ready": task.get("state") == "DONE" and h and d,
    }


def derive_task_phase(conditions, fsm_state) -> str:
    """관측 rollup(fsm_state authoritative). 종료상태 passthrough → MergeReady → Blocked(deps
    미충족) → Stalled(held ∧ heartbeat 명시적 False)/Working(held) → Ready. heartbeat None(미청구)
    은 Stalled 아님 — dead-σ 는 침묵이지 반증이 아니다."""
    if fsm_state in TERMINAL:
        return fsm_state
    if conditions.get("merge_ready"):
        return "MergeReady"
    if not conditions.get("deps_satisfied"):
        return "Blocked"
    if conditions.get("held"):
        return "Stalled" if conditions.get("heartbeat_fresh") is False else "Working"
    return "Ready"
