"""D6 (부분) — release/renew 의 소유+fence 가드. P0-3 + 오추방 좀비 차단.

release(orbit_id)가 누구나 남의 궤도를 해제하던 버그(P0-3)를 닫는다: 소유 agent + 현재 fence가
일치해야만 해제/갱신된다. 회수(EXPIRED)된 좀비의 renew는 FENCED_OUT으로 잃은 lease를 못 살린다.
"""

import time

from omd_server import Coordinator


def test_release_rejects_non_owner(tmp_path):
    omd = Coordinator(db_path=str(tmp_path / "omd.db"))
    r = omd.claim("agA", ["a/**"], "write")
    bad = omd.release(r["orbit_id"], "agZ", r["fence"])          # 다른 agent
    assert bad["ok"] is False and bad["reason"] == "not owner"
    assert omd.store.get_orbit(r["orbit_id"])["state"] == "HELD"  # 여전히 보유(해제 안 됨)


def test_release_rejects_stale_fence(tmp_path):
    omd = Coordinator(db_path=str(tmp_path / "omd.db"))
    r = omd.claim("agA", ["a/**"], "write")
    bad = omd.release(r["orbit_id"], "agA", r["fence"] + 99)     # 낡은 fence
    assert bad["ok"] is False and bad.get("fenced_out")
    assert omd.store.get_orbit(r["orbit_id"])["state"] == "HELD"


def test_release_owner_fence_ok(tmp_path):
    omd = Coordinator(db_path=str(tmp_path / "omd.db"))
    r = omd.claim("agA", ["a/**"], "write")
    assert omd.release(r["orbit_id"], "agA", r["fence"])["ok"]
    assert omd.store.get_orbit(r["orbit_id"])["state"] == "RELEASED"


def test_release_idempotent_replay(tmp_path):
    """MCP at-least-once: 같은 release 재시도는 멱등 no-op."""
    omd = Coordinator(db_path=str(tmp_path / "omd.db"))
    r = omd.claim("agA", ["a/**"], "write")
    omd.release(r["orbit_id"], "agA", r["fence"])
    again = omd.release(r["orbit_id"], "agA", r["fence"])
    assert again["ok"] and again.get("noop")


def test_zombie_renew_is_fenced_out(tmp_path):
    """오추방(회수)된 물방울의 renew는 FENCED_OUT — 잃은 궤도를 못 연장(부활 차단)."""
    omd = Coordinator(db_path=str(tmp_path / "omd.db"), agent_ttl=0.03)
    r = omd.claim("agA", ["a/**"], "write")
    time.sleep(0.05)
    omd.reclaim_zombies()                                        # agA 궤도 EXPIRED
    res = omd.renew(r["orbit_id"], "agA", r["fence"])
    assert res["ok"] is False and res.get("fenced_out")
