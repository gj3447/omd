"""증분4 — P0-10 / §D7: task 의존 DAG 사이클 검출.

전(前): 상호의존(A after B, B after A)이면 둘 다 영구 BLOCKED(`next_task`의 deps-게이트가
영원히 풀리지 않음). 이제 declare/depend 가 사이클을 만드는 엣지를 거부한다(그래프 불변).

검증축:
 1) depend 가 사이클(A→B→A)을 dep_cycle 로 거부, 그래프 불변.
 2) declare(deps=...) 가 사이클을 거부.
 3) self-dep 거부.
 4) 유효 DAG 는 수락(+ next_task 가 정상 unblock).
 5) (이빨) 게이트가 없었다면 상호의존이 영구 BLOCKED 임을 직접 보인다(territory check).
"""

import json

from omd_server import Coordinator


def _deps(omd, task_id):
    return json.loads(omd.store.get_task(task_id)["deps"] or "[]")


# ---------- 1) depend 가 사이클을 거부 (그래프 불변) ----------
def test_depend_rejects_back_edge_cycle():
    omd = Coordinator(allow_memory_db=True)
    omd.declare("A", writes=["a/**"])
    omd.declare("B", writes=["b/**"])
    # A after B (정상)
    r1 = omd.depend("A", "B")
    assert r1["ok"] and _deps(omd, "A") == ["B"]
    # B after A 는 사이클(A→B→A) → 거부, 그래프 불변.
    r2 = omd.depend("B", "A")
    assert r2["ok"] is False and r2["reason"] == "dep_cycle", r2
    assert "cycle" in r2 and "A" in r2["cycle"] and "B" in r2["cycle"]
    assert _deps(omd, "B") == [], "거부됐는데 B의 deps가 변했다(그래프 불변 위반)"


def test_depend_rejects_longer_cycle():
    """A→B→C→A (3-노드 사이클)도 잡힌다."""
    omd = Coordinator(allow_memory_db=True)
    for t in ("A", "B", "C"):
        omd.declare(t, writes=[f"{t.lower()}/**"])
    assert omd.depend("A", "B")["ok"]      # A after B
    assert omd.depend("B", "C")["ok"]      # B after C
    r = omd.depend("C", "A")               # C after A → A→B→C→A 사이클
    assert r["ok"] is False and r["reason"] == "dep_cycle", r
    assert _deps(omd, "C") == []


# ---------- 2) declare(deps=) 가 사이클을 거부 ----------
def test_declare_rejects_cycle_in_deps():
    omd = Coordinator(allow_memory_db=True)
    omd.declare("A", writes=["a/**"], deps=["B"])     # A after B (B 아직 없어도 엣지 등록)
    omd.declare("B", writes=["b/**"])                  # 정상
    # 이제 B 를 A 에 의존시키면 사이클: B after A + A after B.
    r = omd.declare("B", writes=["b/**"], deps=["A"])
    assert r["ok"] is False and r["reason"] == "dep_cycle", r
    # B 의 deps 는 갱신 안 됨(거부) — 원래 빈 채.
    assert _deps(omd, "B") == []


# ---------- 3) self-dep 거부 ----------
def test_self_dependency_rejected():
    omd = Coordinator(allow_memory_db=True)
    omd.declare("A", writes=["a/**"])
    r = omd.depend("A", "A")
    assert r["ok"] is False and r["reason"] == "dep_cycle", r
    assert _deps(omd, "A") == []
    # declare 경로의 self-dep 도 거부.
    r2 = omd.declare("S", writes=["s/**"], deps=["S"])
    assert r2["ok"] is False and r2["reason"] == "dep_cycle", r2
    assert omd.store.get_task("S") is None, "거부된 self-dep declare 가 task 를 만들었다"


# ---------- 4) 유효 DAG 수락 + unblock ----------
def test_valid_dag_accepted_and_unblocks():
    omd = Coordinator(allow_memory_db=True)
    omd.declare("base", writes=["src/base/**"])
    r = omd.declare("dependent", writes=["src/x/**"], deps=["base"])
    assert r["ok"] and r["state"] == "PENDING", r
    # base 미완 → dependent 건너뜀, base 추천.
    assert omd.next_task("ag")["task_id"] == "base"
    # base 완주(머지)시키면 dependent 가 unblock.
    omd.claim("ag", ["src/base/**"], task_id="base")
    omd.start("base", "ag")
    omd.finish("base")
    omd.connect("base")                       # repo 미바인딩 → DB-only MERGED
    assert omd.store.get_task("base")["state"] == "MERGED"
    assert omd.next_task("ag2")["task_id"] == "dependent"


def test_depend_is_idempotent_for_existing_edge():
    omd = Coordinator(allow_memory_db=True)
    omd.declare("A", writes=["a/**"])
    omd.declare("B", writes=["b/**"])
    omd.depend("A", "B")
    r = omd.depend("A", "B")                   # 이미 있는 엣지 — noop, 사이클 검사 불필요
    assert r["ok"] and r.get("noop") and _deps(omd, "A") == ["B"]


# ---------- 5) 이빨: 게이트 없으면 상호의존이 영구 BLOCKED ----------
def test_mutual_deps_would_be_permanently_blocked_without_gate():
    """territory check — 사이클 게이트가 *없다고 가정*하고 상호의존을 강제로 심으면
    next_task 가 둘 다 영원히 못 고른다(둘 다 영구 BLOCKED). 게이트가 이 상태를 애초에
    못 만들게 막는 이유. (게이트를 우회해 store 에 직접 사이클을 심는다.)"""
    omd = Coordinator(allow_memory_db=True)
    omd.declare("A", writes=["a/**"])
    omd.declare("B", writes=["b/**"])
    # 게이트 우회: 직접 상호의존 사이클을 store 에 심는다(A after B, B after A).
    with omd.store.tx():
        omd.store.set_task_deps("A", ["B"])
        omd.store.set_task_deps("B", ["A"])
    # 둘 다 deps 가 MERGED 가 아니므로 영원히 unblock 불가 → next_task 가 아무것도 못 고름.
    assert omd.next_task("ag") is None, "사이클인데 작업이 추천됨(deps 게이트 모순)"
