"""OOPTDD 드라이버 회귀가드: omd_server.ooptdd_scenarios.run 이 cid 상관키로 LTDD 게이트 이벤트를
실 Coordinator 에서 방출함을 고정(spec/omd_concurrency_ooptdd.yaml 의 gate 와 동기 — drift 시 RED).
"""
from omd_server.ooptdd_scenarios import run


class _Collector:
    def __init__(self):
        self.evs = []

    def ship(self, envs):
        self.evs.extend(envs)


def test_driver_emits_all_gate_events_under_cid():
    c = _Collector()
    cid = "ooptdd-regression-cid"
    run(c, cid)
    got = {e["event"] for e in c.evs if e.get("cid") == cid}
    for want in ("orbit_requested", "orbit_granted", "flag_set", "depend_rejected",
                 "barrier_declared", "task_conditions"):
        assert want in got, f"gate event {want} 미방출 — spec/omd_concurrency_ooptdd.yaml 게이트와 drift"


def test_task_conditions_deps_satisfied():
    """REQ-TASK-CONDITIONS 의 where:{deps_satisfied:true} 게이트 — promote 된 task 의 직교 condition
    관측이 cid 로 도착하고 deps_satisfied=True(vacuous over empty deps)로 value-pin(K8s-흡수)."""
    c = _Collector()
    cid = "ooptdd-cond-cid"
    run(c, cid)
    tc = [e for e in c.evs if e.get("cid") == cid and e["event"] == "task_conditions"]
    assert tc and tc[0].get("deps_satisfied") is True, \
        "task_conditions deps_satisfied=True 이어야 (직교 condition 관측 LTDD 증거)"


def test_first_grant_fence_is_one():
    """REQ-FENCE-MONOTONE 의 where:{fence:1} 게이트 — 첫 grant 의 fence 는 1(value-pinned)."""
    c = _Collector()
    cid = "ooptdd-fence-cid"
    run(c, cid)
    g = [e for e in c.evs if e.get("cid") == cid and e["event"] == "orbit_granted"]
    assert g and g[0].get("fence") == 1, "첫 grant fence=1 이어야 (fencing 단조성 LTDD 증거)"
