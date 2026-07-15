"""periodic_sweep — embedded default deadline/lease authority tick.

Omitted configuration is autonomous; explicit ``None``/``0`` preserves a
deterministic inline-only surface. ``close()`` cleanly joins every live writer.
"""
import gc
import os
import tempfile
import threading
import time
import weakref

import pytest

from omd_server import Emitter, core
from omd_server.core import Coordinator, CoordinatorConflict


def _db():
    return os.path.join(tempfile.mkdtemp(prefix="omd-sweep-"), "omd.db")


def _wait_until(predicate, timeout=2.0):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(0.01)
    assert predicate(), "condition did not become true before timeout"


def test_omitted_embedded_interval_starts_default_thread(monkeypatch):
    monkeypatch.setattr(core, "DEFAULT_EMBEDDED_SWEEP_INTERVAL", 0.01)
    omd = Coordinator(db_path=_db(), agent_ttl=None)
    try:
        assert omd._sweep_interval == 0.01
        assert omd._sweep_thread is not None and omd._sweep_thread.is_alive()
        assert omd._heartbeat_thread is not None
        assert omd._heartbeat_thread.is_alive()
    finally:
        omd.close()


@pytest.mark.parametrize("value", [True, -1, float("nan"), float("inf"), "bad"])
def test_invalid_embedded_sweep_interval_fails_before_db_creation(tmp_path, value):
    db = tmp_path / "invalid.db"
    with pytest.raises(ValueError, match="sweep_interval"):
        Coordinator(db_path=str(db), agent_ttl=None, sweep_interval=value)
    assert not db.exists()


def test_invalid_background_autostart_fails_before_db_creation(tmp_path):
    db = tmp_path / "invalid-autostart.db"
    with pytest.raises(ValueError, match="autostart_background_workers"):
        Coordinator(
            db_path=str(db),
            agent_ttl=None,
            autostart_background_workers="yes",
        )
    assert not db.exists()


def test_close_idempotent_without_thread():
    omd = Coordinator(db_path=_db(), agent_ttl=None, sweep_interval=None)
    omd.close()
    omd.close()                                  # 스레드 없어도 no-op, 예외 없음


def test_default_sweep_delivers_idle_wait_timeout_without_foreground_verb(
    monkeypatch,
):
    monkeypatch.setattr(core, "DEFAULT_EMBEDDED_SWEEP_INTERVAL", 0.01)
    omd = Coordinator(
        db_path=_db(),
        agent_ttl=None,
        admission_wait_timeout=0.04,
    )
    try:
        omd.claim("holder", ["src/**"], ttl=10.0)
        waiting = omd.claim("waiter", ["src/**"], request_id="idle-wait")
        _wait_until(
            lambda: omd.store.get_orbit(waiting["orbit_id"])["state"] != "PENDING"
        )
        row = omd.store.get_orbit(waiting["orbit_id"])
        assert row["state"] == "DENIED"
        assert row["decision_type"] == "WAIT_TIMEOUT"
    finally:
        omd.close()


@pytest.mark.parametrize("off", [None, 0])
def test_explicit_off_remains_inline_only_until_a_foreground_tick(off):
    omd = Coordinator(
        db_path=_db(),
        agent_ttl=None,
        admission_wait_timeout=0.03,
        sweep_interval=off,
    )
    try:
        assert omd._sweep_thread is None
        assert omd._heartbeat_thread is None
        omd.claim("holder", ["src/**"], ttl=10.0)
        waiting = omd.claim("waiter", ["src/**"], request_id="manual-wait")
        time.sleep(0.08)
        assert omd.store.get_orbit(waiting["orbit_id"])["state"] == "PENDING"
        omd.status()
        row = omd.store.get_orbit(waiting["orbit_id"])
        assert row["state"] == "DENIED"
        assert row["decision_type"] == "WAIT_TIMEOUT"
    finally:
        omd.close()


def test_close_joins_default_thread_and_prevents_post_close_timeout(monkeypatch):
    monkeypatch.setattr(core, "DEFAULT_EMBEDDED_SWEEP_INTERVAL", 0.01)
    omd = Coordinator(
        db_path=_db(),
        agent_ttl=None,
        admission_wait_timeout=0.05,
    )
    omd.claim("holder", ["src/**"], ttl=10.0)
    waiting = omd.claim("waiter", ["src/**"], request_id="close-wait")
    thread = omd._sweep_thread
    omd.close()
    time.sleep(0.08)
    assert thread is not None and not thread.is_alive()
    assert omd._sweep_thread is None
    assert omd.store.get_orbit(waiting["orbit_id"])["state"] == "PENDING"


def test_due_wait_timeout_precedes_due_holder_expiry_promotion(monkeypatch):
    # First tick occurs after both deadlines. Reconciliation must timeout the
    # waiter before considering promotion, even though its holder also expires.
    monkeypatch.setattr(core, "DEFAULT_EMBEDDED_SWEEP_INTERVAL", 0.05)
    omd = Coordinator(
        db_path=_db(),
        agent_ttl=None,
        admission_wait_timeout=0.02,
    )
    try:
        omd.claim("holder", ["src/**"], ttl=0.02)
        waiting = omd.claim("waiter", ["src/**"], request_id="ordered-timeout")
        _wait_until(
            lambda: omd.store.get_orbit(waiting["orbit_id"])["state"] != "PENDING"
        )
        row = omd.store.get_orbit(waiting["orbit_id"])
        assert row["state"] == "DENIED"
        assert row["decision_type"] == "WAIT_TIMEOUT"
    finally:
        omd.close()


def test_default_thread_does_not_retain_an_unreferenced_coordinator(monkeypatch):
    monkeypatch.setattr(core, "DEFAULT_EMBEDDED_SWEEP_INTERVAL", 0.01)
    omd = Coordinator(db_path=_db(), agent_ttl=None, leader_ttl=60.0)
    omd.resign()
    coordinator_ref = weakref.ref(omd)
    thread = omd._sweep_thread
    heartbeat_thread = omd._heartbeat_thread
    del omd
    gc.collect()
    assert coordinator_ref() is None
    thread.join(timeout=1.0)
    heartbeat_thread.join(timeout=1.0)
    assert not thread.is_alive()
    assert not heartbeat_thread.is_alive()


def test_default_worker_heartbeats_before_a_slower_authority_sweep(monkeypatch):
    monkeypatch.setattr(core, "DEFAULT_EMBEDDED_SWEEP_INTERVAL", 0.20)
    db = _db()
    omd = Coordinator(db_path=db, agent_ttl=None, leader_ttl=0.06)
    try:
        initial = omd.store.get_leader()["last_heartbeat"]
        _wait_until(
            lambda: omd.store.get_leader()["last_heartbeat"] > initial,
            timeout=1.0,
        )
        # The authority sweep has not reached its first 200ms tick, but the
        # lifecycle heartbeat already preserved the live epoch.
        with pytest.raises(CoordinatorConflict, match="another live coordinator"):
            Coordinator(
                db_path=db,
                agent_ttl=None,
                coordinator_id="takeover-probe",
                leader_ttl=0.06,
                sweep_interval=None,
            )
    finally:
        omd.close()
        omd.resign()


@pytest.mark.parametrize("failed_worker", ["omd-heartbeat-", "omd-sweep-"])
def test_default_worker_start_failure_rolls_back_leader_lease(
    tmp_path, monkeypatch, failed_worker
):
    db = str(tmp_path / "thread-start-failure.db")
    monkeypatch.setattr(core, "DEFAULT_EMBEDDED_SWEEP_INTERVAL", 0.01)
    real_start = threading.Thread.start

    def fail_sweep_start(thread):
        if thread.name.startswith(failed_worker):
            raise RuntimeError("no thread slots")
        return real_start(thread)

    monkeypatch.setattr(threading.Thread, "start", fail_sweep_start)
    with pytest.raises(RuntimeError, match="no thread slots"):
        Coordinator(db_path=db, agent_ttl=None)

    monkeypatch.setattr(threading.Thread, "start", real_start)
    restarted = Coordinator(db_path=db, agent_ttl=None, sweep_interval=None)
    try:
        assert restarted.leader_epoch == 2
    finally:
        restarted.close()
        restarted.resign()


def test_heartbeat_is_independent_from_a_slow_running_sweep(tmp_path):
    db = str(tmp_path / "slow-sweep.db")
    entered = threading.Event()
    release = threading.Event()
    omd = Coordinator(
        db_path=db,
        agent_ttl=None,
        leader_ttl=0.09,
        sweep_interval=None,
        autostart_background_workers=False,
    )

    def blocking_sweep():
        entered.set()
        assert release.wait(2.0)
        return {"expired": []}

    omd.sweep = blocking_sweep
    omd.start_background_workers(sweep_interval=0.01)
    try:
        assert entered.wait(1.0)
        time.sleep(0.13)
        with pytest.raises(CoordinatorConflict, match="another live coordinator"):
            Coordinator(
                db_path=db,
                agent_ttl=None,
                coordinator_id="slow-sweep-takeover-probe",
                leader_ttl=0.09,
                sweep_interval=None,
            )
    finally:
        release.set()
        omd.close()
        omd.resign()


def test_close_keeps_heartbeat_until_accepted_outbox_effect_finishes(tmp_path):
    class BlockingSink:
        def __init__(self):
            self.started = threading.Event()
            self.release = threading.Event()

        def ship(self, envelopes):
            return None

        def ship_strict(self, envelopes):
            self.started.set()
            assert self.release.wait(2.0)

    db = str(tmp_path / "close-effect.db")
    sink = BlockingSink()
    omd = Coordinator(
        db_path=db,
        agent_ttl=None,
        leader_ttl=0.06,
        sweep_interval=0.01,
        events=Emitter(sink),
    )
    omd.claim("agent", ["src/**"], request_id="blocking-notification")
    assert sink.started.wait(1.0)

    close_errors = []

    def run_close():
        try:
            omd.close()
        except BaseException as exc:  # pragma: no cover - asserted below.
            close_errors.append(exc)

    closer = threading.Thread(target=run_close)
    closer.start()
    time.sleep(0.10)
    assert closer.is_alive(), "close must still be joining the accepted effect"
    with pytest.raises(CoordinatorConflict, match="another live coordinator"):
        Coordinator(
            db_path=db,
            agent_ttl=None,
            coordinator_id="close-takeover-probe",
            leader_ttl=0.06,
            sweep_interval=None,
        )

    sink.release.set()
    closer.join(timeout=2.0)
    assert not closer.is_alive()
    assert not close_errors
    omd.resign()
    with Coordinator(
        db_path=db,
        agent_ttl=None,
        coordinator_id="post-close-owner",
        sweep_interval=None,
    ) as reopened:
        assert reopened.leader_epoch == 2


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
    assert omd._heartbeat_thread is None


def test_deferred_start_is_idempotent_and_close_is_terminal():
    omd = Coordinator(db_path=_db(), agent_ttl=None, sweep_interval=None)
    started = omd.start_sweep(0.02)
    assert started == {"ok": True, "enabled": True, "interval": 0.02}
    assert omd.start_sweep(0.02)["already"] is True
    with pytest.raises(RuntimeError, match="different interval"):
        omd.start_sweep(0.03)
    first = omd._sweep_thread
    omd.close()
    assert not first.is_alive()
    with pytest.raises(RuntimeError, match="periodic sweep is closed"):
        omd.start_sweep(0.02)


def test_close_linearizes_against_a_concurrent_sweep_start(monkeypatch):
    omd = Coordinator(db_path=_db(), agent_ttl=None, sweep_interval=None)
    omd.start_sweep(0.20)
    old_thread = omd._sweep_thread
    join_entered = threading.Event()
    release_join = threading.Event()
    real_join = threading.Thread.join

    def gated_join(thread, timeout=None):
        if thread is old_thread:
            join_entered.set()
            assert release_join.wait(1.0)
        return real_join(thread, timeout)

    monkeypatch.setattr(threading.Thread, "join", gated_join)
    close_errors = []
    start_errors = []

    def run_close():
        try:
            omd.close()
        except BaseException as exc:  # pragma: no cover - asserted below.
            close_errors.append(exc)

    def run_start():
        try:
            omd.start_sweep(0.20)
        except BaseException as exc:
            start_errors.append(exc)

    closer = threading.Thread(target=run_close)
    closer.start()
    assert join_entered.wait(1.0)
    starter = threading.Thread(target=run_start)
    starter.start()
    time.sleep(0.02)
    assert starter.is_alive(), "start must wait behind the close lifecycle fence"
    release_join.set()
    closer.join(timeout=1.0)
    starter.join(timeout=1.0)

    assert not close_errors
    assert len(start_errors) == 1
    assert isinstance(start_errors[0], RuntimeError)
    assert "periodic sweep is closed" in str(start_errors[0])
    assert old_thread is not None and not old_thread.is_alive()
    assert omd._sweep_thread is None
    omd.resign()


def test_context_manager_closes_thread_and_resigns_for_immediate_reopen():
    db = _db()
    with Coordinator(db_path=db, agent_ttl=None, sweep_interval=0.02) as omd:
        assert omd._sweep_thread.is_alive()
        th = omd._sweep_thread
    assert not th.is_alive()                      # __exit__ 이 close()
    with Coordinator(db_path=db, agent_ttl=None, sweep_interval=None) as reopened:
        assert reopened.leader_epoch == 2
