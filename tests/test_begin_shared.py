"""begin()의 write/shared 다중 lease 원자성·task fence 회귀 가드.

shared는 write의 암묵적 예외 문법이 아니다. 현재 glob 모델에는 ``a/** EXCEPT a/hot.py``를
표현할 수 없으므로 두 클래스는 선언부터 서로소여야 한다. 서로소인 다중 write-like lease의
외부 task fence는 배리어와 동일하게 ``max(individual fences)``다.
"""

from omd_server import Coordinator


def _omd(tmp_path):
    return Coordinator(str(tmp_path / "omd.db"), agent_ttl=None)


def test_declare_rejects_overlapping_write_and_shared_without_mutation(tmp_path):
    omd = _omd(tmp_path)

    r = omd.declare(
        "T", writes=["constants/**"], shared=["constants/env.py"]
    )

    assert r["ok"] is False
    assert r["reason"] == "write_shared_overlap"
    assert r["overlaps"] == [
        {"write": "constants/**", "shared": "constants/env.py"}
    ]
    assert omd.store.get_task("T") is None
    assert omd.store.orbits_for_task("T") == []


def test_begin_rejects_overlapping_write_and_shared_at_declare_stage(tmp_path):
    omd = _omd(tmp_path)

    r = omd.begin(
        "T", "ag", writes=["constants/**"], shared=["constants/env.py"]
    )

    assert r["ok"] is False
    assert r["stage"] == "declare"
    assert r["reason"] == "write_shared_overlap"
    assert omd.store.get_task("T") is None


def test_begin_disjoint_write_and_shared_returns_max_task_fence(tmp_path):
    omd = _omd(tmp_path)

    r = omd.begin(
        "T", "ag", writes=["src/**"], shared=["constants/env.py"]
    )

    assert r["ok"] is True
    live = [o for o in omd.store.orbits_for_task("T") if o["state"] == "HELD"]
    assert {o["mode"] for o in live} == {"write", "shared"}
    assert r["fence"] == max(o["fence"] for o in live)
    assert r["fences"] == {
        o["mode"]: o["fence"] for o in live
    }

    finished = omd.finish("T", "ag", r["fence"])
    assert finished["state"] == "DONE"
    connected = omd.connect("T", "ag", r["fence"])
    assert connected["ok"] is True
    assert connected["state"] == "MERGED"


def test_begin_shared_conflict_does_not_leave_exclusive_lease_or_start(tmp_path):
    omd = _omd(tmp_path)
    blocker = omd.claim("blocker", ["constants/**"], mode="write")

    r = omd.begin(
        "T", "ag", writes=["src/**"], shared=["constants/env.py"]
    )

    assert r["ok"] is False
    assert r["stage"] == "claim"
    assert r["mode"] == "shared"
    assert r["state"] == "PENDING"
    task = omd.store.get_task("T")
    assert task["state"] == "PENDING"
    assert task["worktree"] is None
    owned = omd.store.orbits_owned_by_agent("ag")
    assert len(owned) == 1
    assert owned[0]["mode"] == "shared"
    assert owned[0]["state"] == "PENDING"
    assert not any(o["mode"] == "write" and o["state"] == "HELD" for o in owned)

    omd.release(blocker["orbit_id"], "blocker", blocker["fence"])
    assert omd.store.orbits_owned_by_agent("ag")[0]["state"] == "HELD"


def test_begin_retry_after_shared_promotion_completes_with_same_request_id(tmp_path):
    omd = _omd(tmp_path)
    blocker = omd.claim("blocker", ["constants/**"], mode="write")

    first = omd.begin(
        "T", "ag", writes=["src/**"], shared=["constants/env.py"],
        request_id="begin-T",
    )
    assert first["state"] == "PENDING"

    omd.release(blocker["orbit_id"], "blocker", blocker["fence"])
    second = omd.begin(
        "T", "ag", writes=["src/**"], shared=["constants/env.py"],
        request_id="begin-T",
    )

    assert second["ok"] is True
    assert second["state"] == "IN_ORBIT"
    assert len(omd.store.orbits_owned_by_agent("ag")) == 2

