"""D2 — 긴급 탈출(bail) + 통합 회수 루틴. 사용자 핵심 우려("긴급탈출 시 고아 자원")의 답.

자발 bail과 비자발 좀비회수가 단일 루틴을 공유 → 어떤 보유물(궤도/작업)도 고아가 안 된다.
"""

import os
import time

import pytest

from omd_server import Coordinator, Emitter

GATES = os.path.join(os.path.dirname(__file__), os.pardir, "gates")


def test_bail_frees_all_held_and_requeues_and_promotes(tmp_path):
    """긴급탈출: 보유 궤도 해제 + 진행중 작업 requeue + 대기자 promote (고아 0)."""
    omd = Coordinator(db_path=str(tmp_path / "omd.db"))
    omd.declare("T", writes=["a/**"])
    omd.next_task("agA")
    omd.claim("agA", ["a/**"], task_id="T")
    omd.start("T", "agA")
    waiting = omd.claim("agB", ["a/**"], "write")          # agA와 겹침 → PENDING
    assert waiting["state"] == "PENDING"

    res = omd.bail("agA")
    assert res["orbits"] and "T" in res["tasks"]            # 궤도 해제 + 작업 requeue
    assert omd.store.get_task("T")["state"] == "PENDING"    # 작업이 고아가 아님(재배정 가능)
    assert omd.store.get_agent("agA")["state"] == "RETIRED"
    assert omd.store.get_orbit(waiting["orbit_id"])["state"] == "HELD"   # 대기자 해방


def test_bail_idempotent(tmp_path):
    """bail 도중 죽어도 안전 — 두 번째 호출은 no-op(이중해제 없음)."""
    omd = Coordinator(db_path=str(tmp_path / "omd.db"))
    omd.claim("agA", ["a/**"], "write")
    r1 = omd.bail("agA")
    r2 = omd.bail("agA")
    assert r1["orbits"] and r2.get("noop")


def test_voluntary_and_involuntary_converge(tmp_path):
    """자발 bail과 비자발 좀비회수는 동일 효과 — 단일 루틴."""
    a = Coordinator(db_path=str(tmp_path / "a.db"))
    a.claim("x", ["p/**"], "write")
    a.bail("x")

    b = Coordinator(db_path=str(tmp_path / "b.db"), agent_ttl=0.03)
    b.claim("x", ["p/**"], "write")
    time.sleep(0.05)
    b.reclaim_zombies()

    for c in (a, b):
        assert c.store.orbits_held_by_agent("x") == []      # 궤도 더 이상 HELD 아님
        assert c.store.get_agent("x")["state"] == "RETIRED"


def test_default_reclamation_is_on(tmp_path):
    """P0-7: 기본 Coordinator는 회수 ON(agent_ttl 비-None) — 죽은 물방울이 영구 고아가 안 되게."""
    assert Coordinator(db_path=str(tmp_path / "omd.db")).agent_ttl is not None


def test_bail_trace_arrives(tmp_path):
    """LTDD: bail의 관측가능 트레이스(orbit_released[reason=bail] → agent_reclaimed)가 도착."""
    pytest.importorskip("ooptdd")
    from ooptdd.backends import MemoryBackend, memory as mem
    from ooptdd.gate import evaluate, evidence_tier, load_gate

    mem.reset()
    backend = MemoryBackend()
    omd = Coordinator(db_path=str(tmp_path / "omd.db"), events=Emitter(backend))
    omd.claim("omd-bail-demo", ["a/**"], "write")
    omd.bail("omd-bail-demo")

    deadline = time.monotonic() + 2.0
    while True:
        res = evaluate(backend, load_gate(os.path.join(GATES, "bail.yaml")))
        if res["ok"] or time.monotonic() >= deadline:
            break
        time.sleep(0.01)
    assert res["ok"], res
    assert evidence_tier(res) == "arrived"
    omd.close()
