"""Orbit / Task 상태머신 — pytransitions 기반 (deep-research 추천 A).

상태 자체는 SQLite에 별도 영속(store.py). 여기 FSM은 **전이 합법성 검증기**:
저장된 state로 머신을 재수화 → trigger → 불법이면 MachineError. 새 state를 돌려줌.
"""

from __future__ import annotations

from transitions import Machine

ORBIT_STATES = ["PENDING", "HELD", "RELEASED", "EXPIRED", "DENIED"]
ORBIT_TRANSITIONS = [
    {"trigger": "hold", "source": "PENDING", "dest": "HELD"},     # 요청 즉시 grant(입체)
    {"trigger": "grant", "source": "PENDING", "dest": "HELD"},    # 대기 후 promote
    {"trigger": "renew", "source": "HELD", "dest": "HELD"},
    {"trigger": "release", "source": "HELD", "dest": "RELEASED"},
    {"trigger": "expire", "source": "HELD", "dest": "EXPIRED"},
    {"trigger": "deny", "source": "PENDING", "dest": "DENIED"},
]

TASK_STATES = [
    "PENDING", "BLOCKED", "READY", "CLAIMED", "IN_ORBIT",
    "DONE", "CONNECTING", "MERGED", "ABORTED", "POISONED",
]
TASK_TRANSITIONS = [
    {"trigger": "block", "source": "PENDING", "dest": "BLOCKED"},
    {"trigger": "ready", "source": ["PENDING", "BLOCKED"], "dest": "READY"},
    {"trigger": "claim", "source": "READY", "dest": "CLAIMED"},
    {"trigger": "start", "source": "CLAIMED", "dest": "IN_ORBIT"},
    {"trigger": "finish", "source": "IN_ORBIT", "dest": "DONE"},
    {"trigger": "connect", "source": "DONE", "dest": "CONNECTING"},
    {"trigger": "merged", "source": "CONNECTING", "dest": "MERGED"},
    # D8/P0-6: split-phase connect 가 Phase B(락밖 merge)에서 실패하거나, 재기동 복구가
    # git상 미머지로 판정하면 CONNECTING→DONE 으로 되돌린다(connect 재호출 가능 = 재시도가능).
    {"trigger": "rollback", "source": "CONNECTING", "dest": "DONE"},
    {"trigger": "abort", "source": "*", "dest": "ABORTED"},
    {"trigger": "requeue", "source": "ABORTED", "dest": "PENDING"},
    # GAP-1: 좀비회수/bail 이 무한 abort→requeue 로 flapping/poison 태스크를 영구 재순환시키는
    # 것을 막는 **영구 terminal**. per-task reclaims 카운터가 max_reclaims 를 초과하면 requeue
    # 대신 POISONED 로 종결한다 — sweep/next 가 절대 다시 집지 않는 typed 종단(무한루프 0).
    {"trigger": "poison", "source": "ABORTED", "dest": "POISONED"},
]


# 증분8(§D5): 응결 랑데부 배리어. 세대-스탬프 + BROKEN 종단.
#   ARMED → TRIPPING → TRIPPED → CONSUMED  ⊕  (any non-terminal) → BROKEN
# 참가자 사망(도착 전/후)·타임아웃 → break → 도착해 있던 전원이 BROKEN 으로 기상(영구 hang 0).
BARRIER_STATES = ["ARMED", "TRIPPING", "TRIPPED", "CONSUMED", "BROKEN"]
BARRIER_TRANSITIONS = [
    {"trigger": "fill", "source": "ARMED", "dest": "TRIPPING"},      # 전원 도착 → 응결 시작
    {"trigger": "trip", "source": "TRIPPING", "dest": "TRIPPED"},    # 응결 완료(전 task MERGED)
    {"trigger": "consume", "source": "TRIPPED", "dest": "CONSUMED"}, # 결과 수거
    # BROKEN: 비종단 어디서든(사망/타임아웃/abort). 한 번 깨지면 전원 BROKEN 으로 기상.
    {"trigger": "break_", "source": ["ARMED", "TRIPPING"], "dest": "BROKEN"},
]


class _M:
    """state 속성만 갖는 빈 모델."""


def _machine(states, transitions, state):
    m = _M()
    Machine(
        model=m, states=states, transitions=transitions,
        initial=state, auto_transitions=False, ignore_invalid_triggers=False,
    )
    return m


def _spec(kind):
    if kind == "orbit":
        return ORBIT_STATES, ORBIT_TRANSITIONS
    if kind == "barrier":
        return BARRIER_STATES, BARRIER_TRANSITIONS
    return TASK_STATES, TASK_TRANSITIONS


def advance(kind: str, state: str, trigger: str) -> str:
    """(kind, 현재 state)에서 trigger 적용 → 새 state. 불법 전이면 MachineError."""
    states, trans = _spec(kind)
    m = _machine(states, trans, state)
    getattr(m, trigger)()
    return m.state
