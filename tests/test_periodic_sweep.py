"""periodic_sweep — 백그라운드 만료-lease 회수(§D3/D4). inline-only(동사 호출 시점만) → 유휴
후 첫 호출 spike 를 해소한다. opt-in(sweep_interval), default off(스레드 0), close() clean join.
스레드 안전: 변이는 전부 _cs(RLock 직렬화) + store(check_same_thread=False, WAL)."""
import os
import tempfile
import time

from omd_server.core import Coordinator


def _db():
    return os.path.join(tempfile.mkdtemp(prefix="omd-sweep-"), "omd.db")


def test_default_no_background_thread():
    omd = Coordinator(db_path=_db(), agent_ttl=None)
    assert omd._sweep_thread is None            # 기본 off — 하위호환(inline-only 유지)


def test_close_idempotent_without_thread():
    omd = Coordinator(db_path=_db(), agent_ttl=None)
    omd.close()
    omd.close()                                  # 스레드 없어도 no-op, 예외 없음


def test_background_sweep_reclaims_expired_lease_without_any_verb():
    # ttl 지난 orbit 을 *어떤 동사 호출도 없이* 백그라운드가 회수(유휴 spike 해소 증거).
    omd = Coordinator(db_path=_db(), agent_ttl=None, sweep_interval=0.02)
    try:
        c = omd.claim("agZ", ["z/**"], ttl=0.05)
        assert c["state"] == "HELD"
        oid = c["orbit_id"]
        deadline = time.time() + 2.0
        held = {oid}
        while time.time() < deadline:            # 폴링만(변이 동사 호출 없음)
            held = {o["orbit_id"] for o in omd.store.held_orbits()}
            if oid not in held:
                break
            time.sleep(0.02)
        assert oid not in held, "백그라운드 sweep 이 만료 lease 를 회수했어야"
    finally:
        omd.close()


def test_close_stops_and_joins_thread():
    omd = Coordinator(db_path=_db(), agent_ttl=1.0, sweep_interval=0.02)
    assert omd._sweep_thread is not None and omd._sweep_thread.is_alive()
    omd.close()
    assert omd._sweep_thread is None             # 정지 + join 완료


def test_context_manager_closes_thread():
    with Coordinator(db_path=_db(), agent_ttl=None, sweep_interval=0.02) as omd:
        assert omd._sweep_thread.is_alive()
        th = omd._sweep_thread
    assert not th.is_alive()                      # __exit__ 이 close()
