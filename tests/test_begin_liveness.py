"""Q10: begin()이 가짜 recurring heartbeat 없이 유계 silence window를 선언하는 계약."""

import math
import time

import pytest

from omd_server import Coordinator


def _omd(tmp_path, *, agent_ttl=0.05):
    return Coordinator(str(tmp_path / "omd.db"), agent_ttl=agent_ttl)


def test_begin_declared_liveness_survives_default_crash_fast_window(tmp_path):
    omd = _omd(tmp_path)
    r = omd.begin(
        "T", "ag", ["src/**"], ttl=1.0, liveness_ttl=0.3
    )

    time.sleep(0.12)
    omd.sweep()

    assert omd.store.get_task("T")["state"] == "IN_ORBIT"
    assert omd.store.get_agent("ag")["state"] == "WORKING"
    assert omd.store.orbits_held_by_agent("ag")
    assert r["liveness_ttl"] == 0.3


def test_begin_declared_liveness_is_bounded_and_eventually_reclaimed(tmp_path):
    omd = _omd(tmp_path)
    r = omd.begin(
        "T", "ag", ["src/**"], ttl=1.0, liveness_ttl=0.08
    )

    time.sleep(0.14)
    omd.sweep()

    assert omd.store.get_agent("ag")["state"] == "RETIRED"
    assert omd.store.get_task("T")["state"] == "PENDING"
    assert omd.store.orbits_held_by_agent("ag") == []
    stale = omd.renew(r["orbit_id"], "ag", r["fence"], bail_epoch=r["bail_epoch"])
    assert stale["ok"] is False and stale["fenced_out"] is True


def test_begin_without_liveness_declaration_remains_crash_fast(tmp_path):
    omd = _omd(tmp_path)
    omd.begin("T", "ag", ["src/**"], ttl=1.0)

    time.sleep(0.12)
    omd.sweep()

    assert omd.store.get_agent("ag")["state"] == "RETIRED"
    assert omd.store.get_task("T")["state"] == "PENDING"


@pytest.mark.parametrize("bad", [0.0, -1.0, math.nan, math.inf, -math.inf])
def test_begin_rejects_non_positive_or_non_finite_orbit_ttl(tmp_path, bad):
    omd = _omd(tmp_path, agent_ttl=None)

    r = omd.begin("T", "ag", ["src/**"], ttl=bad)

    assert r == {
        "ok": False,
        "stage": "validate",
        "reason": "invalid_ttl",
        "ttl": bad,
    }
    assert omd.store.get_task("T") is None


@pytest.mark.parametrize("bad", [0.0, -1.0, math.nan, math.inf, -math.inf])
def test_begin_rejects_non_positive_or_non_finite_liveness_ttl(tmp_path, bad):
    omd = _omd(tmp_path, agent_ttl=None)

    r = omd.begin("T", "ag", ["src/**"], ttl=10.0, liveness_ttl=bad)

    assert r["ok"] is False
    assert r["stage"] == "validate"
    assert r["reason"] == "invalid_liveness_ttl"
    assert omd.store.get_task("T") is None


def test_begin_rejects_liveness_window_longer_than_orbit_lease(tmp_path):
    omd = _omd(tmp_path, agent_ttl=None)

    r = omd.begin("T", "ag", ["src/**"], ttl=5.0, liveness_ttl=6.0)

    assert r == {
        "ok": False,
        "stage": "validate",
        "reason": "liveness_exceeds_orbit_ttl",
        "ttl": 5.0,
        "liveness_ttl": 6.0,
    }
    assert omd.store.get_task("T") is None


@pytest.mark.parametrize("bad", [0.0, -1.0, math.nan, math.inf, -math.inf])
def test_heartbeat_rejects_unbounded_or_non_positive_liveness_window(tmp_path, bad):
    omd = _omd(tmp_path, agent_ttl=1.0)

    r = omd.heartbeat("ag", ttl=bad)

    assert r["ok"] is False
    assert r["reason"] == "invalid_liveness_ttl"
    assert omd.store.get_agent("ag") is None


def test_begin_returns_explicit_renewal_descriptors(tmp_path):
    omd = _omd(tmp_path, agent_ttl=None)

    r = omd.begin(
        "T", "ag", ["src/**"], shared=["constants/env.py"],
        ttl=30.0, liveness_ttl=20.0,
    )

    assert r["orbit_id"] in {o["orbit_id"] for o in r["orbits"]}
    assert r["fence"] == max(o["fence"] for o in r["orbits"])
    assert r["bail_epoch"] == 0
    assert {(o["mode"], tuple(o["paths"])) for o in r["orbits"]} == {
        ("write", ("src/**",)),
        ("shared", ("constants/env.py",)),
    }
    assert all(o["state"] == "HELD" for o in r["orbits"])
