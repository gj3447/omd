"""M1d durable admission notification outbox crash/replay contracts."""

from __future__ import annotations

import json
import sqlite3
import threading
import time

import pytest

from omd_server import Coordinator, Emitter
from omd_server.admission import canonical_json, sha256_json
from omd_server.core import ADMISSION_NOTIFICATION_EVENTS
from omd_server.admission_contract import load_spec
from omd_server.sinks import MultiSink
from omd_server.store import SCHEMA_VERSION


class Collector:
    def __init__(self, *, fail=False):
        self.fail = fail
        self.events = []
        self.coordinator = None

    def ship(self, envelopes):
        if self.coordinator is not None:
            assert self.coordinator.store._txn_depth == 0
            assert not self.coordinator._lock._is_owned()
        if self.fail:
            raise RuntimeError("sink down")
        self.events.extend(dict(envelope) for envelope in envelopes)

    @property
    def notifications(self):
        return [
            event for event in self.events
            if event.get("notification_schema") == "admission_notification/v1"
        ]


def _omd(tmp_path, *, collector=None, **kwargs):
    omd = Coordinator(
        str(tmp_path / "omd.db"),
        agent_ttl=None,
        enforce_single_coordinator=False,
        events=Emitter(collector) if collector is not None else None,
        **kwargs,
    )
    if collector is not None:
        collector.coordinator = omd
    return omd


def _wait_until(predicate, *, timeout=3.0):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(0.01)
    assert predicate(), "condition did not become true before timeout"


def _outbox_row(omd, event_id):
    with omd._cs():
        return omd.store.get_admission_outbox(event_id)


def test_authority_and_notification_commit_together_then_deliver_after_commit(tmp_path):
    collector = Collector()
    omd = _omd(tmp_path, collector=collector)

    claim = omd.claim("agent", ["src/**"], request_id="claim-1")

    assert claim["state"] == "HELD"
    rows = omd.store.admission_outbox_rows()
    assert len(rows) == 1
    row = rows[0]
    _wait_until(
        lambda: _outbox_row(omd, row["event_id"])["state"] == "DELIVERED"
    )
    row = _outbox_row(omd, row["event_id"])
    assert row["state"] == "DELIVERED" and row["attempts"] == 1
    assert row["transition_kind"] == "ADMISSION_GRANTED"
    assert row["request_id"] == "claim-1" and row["request_generation"] == 0
    assert len(collector.notifications) == 1
    delivered = collector.notifications[0]
    assert delivered["event"] == "orbit_granted"
    assert delivered["event_id"] == row["event_id"]
    assert delivered["decision_id"] == claim["decision_id"]
    assert delivered["fence"] == claim["fence"]


def test_outbox_insert_failure_rolls_back_authority_fact(tmp_path, monkeypatch):
    collector = Collector()
    omd = _omd(tmp_path, collector=collector)
    before = (omd.store.current_seq(), omd.store.current_fence())

    def fail_enqueue(**kwargs):
        raise RuntimeError("outbox unavailable")

    monkeypatch.setattr(omd.store, "enqueue_admission_outbox", fail_enqueue)
    with pytest.raises(RuntimeError, match="outbox unavailable"):
        omd.claim("agent", ["src/**"], request_id="claim-1")

    assert omd.store.orbit_by_request("claim-1") is None
    assert omd.store.admission_outbox_rows() == []
    assert (omd.store.current_seq(), omd.store.current_fence()) == before
    assert collector.notifications == []


def test_authority_write_failure_rolls_back_preceding_outbox_insert(
    tmp_path, monkeypatch
):
    collector = Collector()
    omd = _omd(tmp_path, collector=collector)
    before = (omd.store.current_seq(), omd.store.current_fence())

    def fail_add_orbit(**kwargs):
        raise RuntimeError("orbit write unavailable")

    monkeypatch.setattr(omd.store, "add_orbit", fail_add_orbit)
    with pytest.raises(RuntimeError, match="orbit write unavailable"):
        omd.claim("agent", ["src/**"], request_id="claim-1")

    assert omd.store.admission_outbox_rows() == []
    assert (omd.store.current_seq(), omd.store.current_fence()) == before
    assert collector.notifications == []


def test_sink_failure_preserves_accepted_lease_and_retries_same_event_id(tmp_path):
    broken = Collector(fail=True)
    omd = _omd(tmp_path)

    claim = omd.claim("agent", ["src/**"], request_id="claim-1")
    pending = omd.store.admission_outbox_rows()[0]
    broken.coordinator = omd
    omd.events = Emitter(broken)
    omd.flush_admission_outbox(now=pending["available_at"])
    row = omd.store.get_admission_outbox(pending["event_id"])

    assert claim["state"] == "HELD"
    assert omd.store.get_orbit(claim["orbit_id"])["state"] == "HELD"
    assert row["state"] == "PENDING" and row["attempts"] == 1
    assert "sink down" in row["last_error"]

    healthy = Collector()
    healthy.coordinator = omd
    omd.events = Emitter(healthy)
    result = omd.flush_admission_outbox(now=row["available_at"])

    delivered = omd.store.get_admission_outbox(row["event_id"])
    assert result["delivered"] == 1 and delivered["state"] == "DELIVERED"
    assert delivered["attempts"] == 2
    assert [event["event_id"] for event in healthy.notifications] == [row["event_id"]]
    omd.close()


def test_restart_replays_commit_before_delivery(tmp_path):
    db = str(tmp_path / "omd.db")
    first = _omd(tmp_path)
    claim = first.claim("agent", ["src/**"], request_id="claim-1")
    row = first.store.admission_outbox_rows()[0]
    assert row["state"] == "PENDING" and row["attempts"] == 0
    first.close()
    first.store.db.close()

    collector = Collector()
    reopened = Coordinator(
        db,
        agent_ttl=None,
        enforce_single_coordinator=False,
        events=Emitter(collector),
    )
    collector.coordinator = reopened

    _wait_until(
        lambda: _outbox_row(reopened, row["event_id"])["state"] == "DELIVERED"
    )
    replayed = _outbox_row(reopened, row["event_id"])
    assert reopened.store.get_orbit(claim["orbit_id"])["state"] == "HELD"
    assert replayed["state"] == "DELIVERED" and replayed["attempts"] == 1
    assert [event["event_id"] for event in collector.notifications] == [row["event_id"]]


def test_deliver_before_ack_replays_with_same_stable_event_id(tmp_path):
    db = str(tmp_path / "omd.db")
    first = _omd(tmp_path)
    first.claim("agent", ["src/**"], request_id="claim-1")
    pending = first.store.admission_outbox_rows()[0]
    with first.store.tx():
        claimed = first.store.claim_next_admission_outbox(
            "dead-worker", pending["available_at"], lease_ttl=0.001
        )
    collector = Collector()
    collector.ship([json.loads(claimed["payload"])])
    first.close()
    first.store.db.close()

    reopened = Coordinator(
        db,
        agent_ttl=None,
        enforce_single_coordinator=False,
        events=Emitter(collector),
    )
    collector.coordinator = reopened

    _wait_until(
        lambda: _outbox_row(reopened, pending["event_id"])["state"] == "DELIVERED"
    )
    row = _outbox_row(reopened, pending["event_id"])
    ids = [event["event_id"] for event in collector.notifications]
    assert ids == [pending["event_id"], pending["event_id"]]
    assert row["state"] == "DELIVERED" and row["attempts"] == 2


def test_stale_claim_token_cannot_ack_or_retry_reclaimed_delivery(tmp_path):
    omd = _omd(tmp_path)
    omd.claim("agent", ["src/**"], request_id="claim-1")
    pending = omd.store.admission_outbox_rows()[0]
    now = pending["available_at"]
    with omd.store.tx():
        first = omd.store.claim_next_admission_outbox("worker-a", now, lease_ttl=1.0)
    with omd.store.tx():
        second = omd.store.claim_next_admission_outbox(
            "worker-b", now + 2.0, lease_ttl=1.0
        )
        assert not omd.store.ack_admission_outbox(
            first["event_id"], first["claim_token"], now + 2.0
        )
        assert not omd.store.retry_admission_outbox(
            first["event_id"], first["claim_token"], now + 3.0, "stale"
        )
        assert omd.store.ack_admission_outbox(
            second["event_id"], second["claim_token"], now + 2.0
        )
        assert not omd.store.ack_admission_outbox(
            second["event_id"], second["claim_token"], now + 2.0
        )


def test_fsm_denial_is_a_durable_notification_contract(tmp_path):
    omd = _omd(tmp_path)
    payload = {
        "repository_id": "repo",
        "request_id": "claim-denied",
        "request_generation": 0,
        "orbit_id": "orbit-denied",
        "event_id": "evt-denied",
        "owner_agent": "agent",
        "observed_at": 1.0,
    }

    with omd.store.tx():
        effect_key = omd._enqueue_admission_notification(
            "ADMISSION_DENIED", payload, {"owner_agent": "agent"}
        )

    row = omd.store.get_admission_outbox("evt-denied")
    envelope = json.loads(row["payload"])
    assert row["transition_kind"] == "ADMISSION_DENIED"
    assert envelope["event"] == "orbit_denied"
    assert envelope["effect_key"] == effect_key


def test_poison_stream_does_not_block_another_request_stream(tmp_path):
    omd = _omd(tmp_path)
    first = omd.claim("a", ["a/**"], request_id="request-a")
    omd.claim("b", ["b/**"], request_id="request-b")
    omd.release(first["orbit_id"], "a", first["fence"])
    rows = omd.store.admission_outbox_rows()
    assert [row["transition_kind"] for row in rows] == [
        "ADMISSION_GRANTED", "ADMISSION_GRANTED", "RELEASE"
    ]
    now = max(row["available_at"] for row in rows)

    with omd.store.tx():
        poison = omd.store.claim_next_admission_outbox("worker", now, lease_ttl=10.0)
        assert poison["request_id"] == "request-a"
        assert omd.store.retry_admission_outbox(
            poison["event_id"], poison["claim_token"], now + 100.0, "poison"
        )
    with omd.store.tx():
        other = omd.store.claim_next_admission_outbox("worker", now, lease_ttl=10.0)

    assert other["request_id"] == "request-b"
    assert other["transition_kind"] == "ADMISSION_GRANTED"
    assert omd.store.get_admission_outbox(rows[2]["event_id"])["state"] == "PENDING"


def test_exact_claim_replay_does_not_create_a_second_notification(tmp_path):
    collector = Collector()
    omd = _omd(tmp_path, collector=collector)
    first = omd.claim("agent", ["src/**"], request_id="claim-1")
    replay = omd.claim("agent", ["src/**"], request_id="claim-1")

    assert replay["replayed"] is True and replay["orbit_id"] == first["orbit_id"]
    assert len(omd.store.admission_outbox_rows()) == 1
    _wait_until(lambda: len(collector.notifications) == 1)
    assert len(collector.notifications) == 1


def test_begin_savepoint_rollback_emits_no_ghost_grant(tmp_path):
    collector = Collector()
    omd = _omd(tmp_path, collector=collector)
    omd.claim("other", ["other/**"], request_id="batch:claim-shared")
    _wait_until(lambda: len(collector.notifications) == 1)
    collector.events.clear()
    before = len(omd.store.admission_outbox_rows())

    result = omd.begin(
        "task-A",
        "A",
        ["x/**"],
        shared=["y/**"],
        request_id="batch",
    )

    assert result["ok"] is False and result["rollback"] == "transaction"
    assert len(omd.store.admission_outbox_rows()) == before
    assert not [
        event for event in collector.notifications
        if event.get("owner_agent") == "A" and event["event"] == "orbit_granted"
    ]


def test_aging_schema_migrates_outbox_table_and_completion_version(tmp_path):
    db = str(tmp_path / "omd.db")
    first = _omd(tmp_path)
    historical = {}
    for state in ("PENDING", "HELD", "RELEASED", "EXPIRED", "DENIED"):
        path = "blocked/**" if state in {"PENDING", "HELD"} else f"{state.lower()}/**"
        claim = first.claim(
            f"agent-{state.lower()}",
            [path],
            request_id=f"pre-outbox-{state.lower()}",
        )
        historical[state] = claim["orbit_id"]
    with first.store.tx():
        for state, orbit_id in historical.items():
            first.store.set_orbit(orbit_id, state=state)
        first.store.set_meta("schema_version", "omd/2026-07-16-m1-aging")
        first.store.db.execute("DROP TABLE admission_outbox")
    first.close()
    first.store.db.close()

    reopened = Coordinator(
        db, agent_ttl=None, enforce_single_coordinator=False
    )
    assert reopened.store.get_meta("schema_version") == SCHEMA_VERSION
    assert reopened.store.db.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' "
        "AND name='admission_outbox'"
    ).fetchone() is not None
    assert {
        state: reopened.store.get_orbit(orbit_id)["state"]
        for state, orbit_id in historical.items()
    } == {state: state for state in historical}
    # The predecessor schema had no notification facts. Migration preserves
    # authority history but never fabricates historical external effects.
    assert reopened.store.admission_outbox_rows() == []
    assert reopened.store.db.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' "
        "AND name='admission_outbox_dependencies'"
    ).fetchone() is not None
    reopened.close()


def test_crash_before_outbox_version_marker_resumes_without_losing_rows(tmp_path):
    db = str(tmp_path / "omd.db")
    first = _omd(tmp_path)
    first.claim("agent", ["src/**"], request_id="claim-1")
    before = first.store.admission_outbox_rows()[0]
    with first.store.tx():
        # Exact crash cut: additive outbox DDL/row exist, but the final durable
        # completion marker still names the predecessor generation.
        first.store.set_meta("schema_version", "omd/2026-07-16-m1-aging")
    first.close()
    first.store.db.close()

    reopened = Coordinator(
        db, agent_ttl=None, enforce_single_coordinator=False
    )
    after = reopened.store.get_admission_outbox(before["event_id"])

    assert reopened.store.get_meta("schema_version") == SCHEMA_VERSION
    assert after["payload"] == before["payload"]
    assert after["payload_sha256"] == before["payload_sha256"]
    assert after["state"] == "PENDING" and after["attempts"] == 0
    reopened.close()


def test_outbox_effect_key_collision_with_different_payload_fails_closed(tmp_path):
    omd = _omd(tmp_path)
    now = 1.0
    common = {
        "event_id": "evt-1",
        "effect_key": "effect-1",
        "schema_version": "admission_notification/v1",
        "repository_id": "repo",
        "request_id": "request",
        "request_generation": 0,
        "orbit_id": "orbit",
        "transition_kind": "ADMISSION_GRANTED",
        "correlation_id": "agent",
        "payload": "{}",
        "payload_sha256": "digest-a",
        "created_at": now,
    }
    with omd.store.tx():
        omd.store.enqueue_admission_outbox(**common)
    with omd.store.tx(), pytest.raises(RuntimeError, match="collision"):
        omd.store.enqueue_admission_outbox(
            **{**common, "event_id": "evt-2", "payload_sha256": "digest-b"}
        )
    with sqlite3.connect(tmp_path / "omd.db") as connection:
        assert connection.execute(
            "SELECT COUNT(*) FROM admission_outbox"
        ).fetchone()[0] == 1


def test_state_changing_fsm_events_equal_durable_notification_whitelist():
    model = load_spec()
    state_changing = {
        transition["event"]
        for machine in model["machines"]
        for transition in machine["transitions"]
        if transition["from"] != transition["to"]
    }
    assert set(ADMISSION_NOTIFICATION_EVENTS) == state_changing


def test_emit_only_events_port_keeps_authority_and_pending_outbox(tmp_path):
    class EmitOnly:
        service = "legacy-events"

        def __init__(self):
            self.events = []

        def emit(self, event, cid, **attrs):
            self.events.append({"event": event, "cid": cid, **attrs})

    port = EmitOnly()
    omd = Coordinator(
        str(tmp_path / "omd.db"),
        agent_ttl=None,
        enforce_single_coordinator=False,
        events=port,
    )

    claim = omd.claim("agent", ["src/**"], request_id="claim-1")
    row = omd.store.admission_outbox_rows()[0]

    assert claim["state"] == "HELD"
    assert row["state"] == "PENDING" and row["attempts"] == 0
    assert json.loads(row["payload"])["service"] == "legacy-events"
    assert omd._outbox_timer is None


def test_outbox_mutators_require_authority_transaction(tmp_path):
    omd = _omd(tmp_path)
    common = {
        "event_id": "evt-1",
        "effect_key": "effect-1",
        "schema_version": "admission_notification/v1",
        "repository_id": "repo",
        "request_id": "request",
        "request_generation": 0,
        "orbit_id": "orbit",
        "transition_kind": "ADMISSION_GRANTED",
        "correlation_id": "agent",
        "payload": "{}",
        "payload_sha256": "digest",
        "created_at": 1.0,
    }

    with pytest.raises(RuntimeError, match="active authority transaction"):
        omd.store.enqueue_admission_outbox(**common)
    with pytest.raises(RuntimeError, match="active authority transaction"):
        omd.store.claim_next_admission_outbox("worker", 1.0, lease_ttl=1.0)
    with pytest.raises(RuntimeError, match="active authority transaction"):
        omd.store.ack_admission_outbox("evt-1", "token", 1.0)
    with pytest.raises(RuntimeError, match="active authority transaction"):
        omd.store.retry_admission_outbox("evt-1", "token", 2.0, "error")
    assert omd.store.admission_outbox_rows() == []


def test_idle_retry_wakes_without_sweep_or_new_authority_edge(tmp_path, monkeypatch):
    monkeypatch.setattr(
        "omd_server.core.ADMISSION_OUTBOX_MAX_RETRY_DELAY", 0.05
    )
    omd = _omd(tmp_path)
    omd.claim("agent", ["src/**"], request_id="claim-1")
    row = omd.store.admission_outbox_rows()[0]
    broken = Collector(fail=True)
    broken.coordinator = omd
    omd.events = Emitter(broken)

    result = omd.flush_admission_outbox(now=row["available_at"])
    assert result["failed"] == 1
    failed = omd.store.get_admission_outbox(row["event_id"])
    healthy = Collector()
    healthy.coordinator = omd
    omd.events = Emitter(healthy)

    _wait_until(
        lambda: _outbox_row(omd, row["event_id"])["state"] == "DELIVERED"
    )
    delivered = _outbox_row(omd, row["event_id"])
    assert delivered["attempts"] == 2
    assert [event["event_id"] for event in healthy.notifications] == [
        row["event_id"]
    ]
    assert failed["available_at"] <= delivered["delivered_at"]
    omd.close()


def test_startup_dispatch_drains_more_than_one_batch_without_sweep(tmp_path):
    db = str(tmp_path / "omd.db")
    first = _omd(tmp_path)
    with first.store.tx():
        for index in range(101):
            payload = {
                "repository_id": first.repository_id,
                "request_id": f"request-{index}",
                "request_generation": 0,
                "orbit_id": f"orbit-{index}",
                "event_id": f"evt-{index}",
                "owner_agent": f"agent-{index}",
                "observed_at": 1.0,
            }
            first._enqueue_admission_notification(
                "ADMISSION_DENIED", payload, {"owner_agent": f"agent-{index}"}
            )
    first.close()
    first.store.db.close()

    collector = Collector()
    reopened = Coordinator(
        db,
        agent_ttl=None,
        enforce_single_coordinator=False,
        events=Emitter(collector),
    )
    collector.coordinator = reopened

    _wait_until(
        lambda: reopened.store.admission_outbox_stats()["delivered"] == 101,
        timeout=5.0,
    )
    assert len(collector.notifications) == 101
    assert reopened.store.admission_outbox_stats()["pending"] == 0
    reopened.close()


def test_restart_wakes_when_existing_delivery_lease_expires(tmp_path):
    db = str(tmp_path / "omd.db")
    first = _omd(tmp_path)
    first.claim("agent", ["src/**"], request_id="claim-1")
    row = first.store.admission_outbox_rows()[0]
    now = time.time()
    with first.store.tx():
        claimed = first.store.claim_next_admission_outbox(
            "dead-worker", now, lease_ttl=0.05
        )
    first.close()
    first.store.db.close()

    collector = Collector()
    reopened = Coordinator(
        db,
        agent_ttl=None,
        enforce_single_coordinator=False,
        events=Emitter(collector),
    )
    collector.coordinator = reopened

    _wait_until(
        lambda: _outbox_row(reopened, row["event_id"])["state"] == "DELIVERED"
    )
    replayed = _outbox_row(reopened, row["event_id"])
    assert replayed["attempts"] == claimed["attempts"] + 1
    reopened.close()


def test_nested_or_empty_strict_fanout_never_false_acks(tmp_path):
    omd = _omd(tmp_path)
    omd.claim("agent", ["src/**"], request_id="claim-1")
    row = omd.store.admission_outbox_rows()[0]

    omd.events = Emitter(MultiSink([]))
    empty = omd.flush_admission_outbox(now=row["available_at"])
    assert empty["failed"] == 1
    failed = omd.store.get_admission_outbox(row["event_id"])

    broken = Collector(fail=True)
    healthy = Collector()
    omd.events = Emitter(MultiSink([MultiSink([broken]), healthy]))
    nested = omd.flush_admission_outbox(now=failed["available_at"])
    assert nested["failed"] == 1
    retried = omd.store.get_admission_outbox(row["event_id"])
    assert retried["state"] == "PENDING" and retried["attempts"] == 2
    assert [event["event_id"] for event in healthy.notifications] == [row["event_id"]]

    broken.fail = False
    delivered = omd.flush_admission_outbox(now=retried["available_at"])
    assert delivered["delivered"] == 1
    assert [event["event_id"] for event in healthy.notifications] == [
        row["event_id"],
        row["event_id"],
    ]
    omd.close()


def test_notifier_io_never_holds_repository_effect_authority(tmp_path):
    class BlockingCollector(Collector):
        def __init__(self):
            super().__init__()
            self.entered = threading.Event()
            self.release = threading.Event()

        def ship(self, envelopes):
            self.entered.set()
            assert self.release.wait(3.0)
            super().ship(envelopes)

    omd = _omd(tmp_path, notification_timeout=1.0)
    omd.claim("agent", ["src/**"], request_id="claim-1")
    blocking = BlockingCollector()
    blocking.coordinator = omd
    omd.events = Emitter(blocking)

    result = omd.bail("agent")

    assert result["agent"] == "agent"
    assert blocking.entered.wait(1.0)
    with omd._connect_effect(blocking=False) as owns_effect:
        assert owns_effect is True
    blocking.release.set()
    _wait_until(
        lambda: [event["event"] for event in blocking.events][-2:]
        == ["orbit_released", "agent_reclaimed"]
    )
    released, reclaimed = blocking.events[-2:]
    assert released["reason"] == "bail"
    assert reclaimed["notification_schema"] == "coordination_notification/v1"
    omd.close()


def test_hung_stream_is_bounded_and_does_not_block_another_stream(tmp_path):
    class BlockFirst(Collector):
        def __init__(self):
            super().__init__()
            self.entered = threading.Event()
            self.release = threading.Event()

        def ship(self, envelopes):
            envelope = envelopes[0]
            if envelope["request_id"] == "request-a":
                self.entered.set()
                assert self.release.wait(3.0)
            super().ship(envelopes)

    omd = _omd(
        tmp_path,
        notification_timeout=0.05,
        notification_max_inflight=2,
    )
    omd.claim("a", ["a/**"], request_id="request-a")
    omd.claim("b", ["b/**"], request_id="request-b")
    rows = omd.store.admission_outbox_rows()
    sink = BlockFirst()
    sink.coordinator = omd
    omd.events = Emitter(sink)

    result = omd.flush_admission_outbox(
        limit=2, now=max(row["available_at"] for row in rows)
    )

    assert sink.entered.is_set()
    assert result["failed"] == 1 and result["delivered"] == 1
    assert [event["request_id"] for event in sink.notifications] == ["request-b"]
    first = omd.store.get_admission_outbox(rows[0]["event_id"])
    with omd._notification_attempt_lock:
        original_worker = omd._notification_attempts[rows[0]["event_id"]]["thread"]
    omd.flush_admission_outbox(limit=1, now=first["available_at"])
    with omd._notification_attempt_lock:
        assert len(omd._notification_attempts) == 1
        assert (
            omd._notification_attempts[rows[0]["event_id"]]["thread"]
            is original_worker
        )

    sink.release.set()
    _wait_until(lambda: not original_worker.is_alive())
    retried = omd.store.get_admission_outbox(rows[0]["event_id"])
    omd.flush_admission_outbox(limit=1, now=retried["available_at"])
    assert omd.store.get_admission_outbox(rows[0]["event_id"])["state"] == "DELIVERED"
    omd.close()


def test_retry_backoff_starts_after_slow_failure_and_other_stream_runs(
    tmp_path, monkeypatch
):
    class SlowSelective(Collector):
        def ship(self, envelopes):
            if envelopes[0]["request_id"] == "request-a":
                time.sleep(0.08)
                raise RuntimeError("slow sink failure")
            super().ship(envelopes)

    monkeypatch.setattr(
        "omd_server.core.ADMISSION_OUTBOX_MAX_RETRY_DELAY", 0.05
    )
    omd = _omd(tmp_path, notification_timeout=0.5)
    omd.claim("a", ["a/**"], request_id="request-a")
    omd.claim("b", ["b/**"], request_id="request-b")
    rows = omd.store.admission_outbox_rows()
    sink = SlowSelective()
    sink.coordinator = omd
    omd.events = Emitter(sink)

    started_at = time.time()
    result = omd.flush_admission_outbox(limit=2)

    failed = omd.store.get_admission_outbox(rows[0]["event_id"])
    assert result["failed"] == 1 and result["delivered"] == 1
    assert failed["attempts"] == 1
    assert failed["available_at"] >= started_at + 0.1
    assert [event["request_id"] for event in sink.notifications] == ["request-b"]
    omd.close()


def test_close_waits_for_active_dispatch_and_notifier_effect(tmp_path):
    class BlockingCollector(Collector):
        def __init__(self):
            super().__init__()
            self.entered = threading.Event()
            self.release = threading.Event()

        def ship(self, envelopes):
            self.entered.set()
            assert self.release.wait(3.0)
            super().ship(envelopes)

    omd = _omd(tmp_path, notification_timeout=0.05)
    omd.claim("agent", ["src/**"], request_id="claim-1")
    sink = BlockingCollector()
    sink.coordinator = omd
    omd.events = Emitter(sink)
    omd._wake_admission_outbox()
    assert sink.entered.wait(1.0)
    release = threading.Timer(0.1, sink.release.set)
    release.start()

    started_at = time.monotonic()
    omd.close()
    elapsed = time.monotonic() - started_at

    assert elapsed >= 0.05
    with omd._notification_attempt_lock:
        assert omd._notification_attempts == {}
    release.join()


def test_notifier_start_failure_cleans_registry_and_preserves_retry(
    tmp_path, monkeypatch
):
    omd = _omd(tmp_path)
    omd.claim("agent", ["src/**"], request_id="claim-1")
    row = omd.store.admission_outbox_rows()[0]
    collector = Collector()
    collector.coordinator = omd
    omd.events = Emitter(collector)
    original_start = threading.Thread.start

    def fail_notify_start(worker):
        if worker.name.startswith("omd-notify-"):
            raise RuntimeError("thread start unavailable")
        return original_start(worker)

    monkeypatch.setattr(threading.Thread, "start", fail_notify_start)
    result = omd.flush_admission_outbox(now=row["available_at"])

    failed = omd.store.get_admission_outbox(row["event_id"])
    assert result["failed"] == 1 and failed["state"] == "PENDING"
    assert "thread start unavailable" in failed["last_error"]
    with omd._notification_attempt_lock:
        assert omd._notification_attempts == {}
    omd.close()


def test_close_fences_and_joins_manual_flush(tmp_path):
    class BlockingCollector(Collector):
        def __init__(self):
            super().__init__()
            self.entered = threading.Event()
            self.release = threading.Event()

        def ship(self, envelopes):
            self.entered.set()
            assert self.release.wait(3.0)
            super().ship(envelopes)

    omd = _omd(tmp_path, notification_timeout=1.0)
    omd.claim("agent", ["src/**"], request_id="claim-1")
    row = omd.store.admission_outbox_rows()[0]
    sink = BlockingCollector()
    sink.coordinator = omd
    omd.events = Emitter(sink)
    flush_errors = []

    def run_flush():
        try:
            omd.flush_admission_outbox(now=row["available_at"])
        except BaseException as exc:  # surfaced in the assertion thread.
            flush_errors.append(exc)

    flush_thread = threading.Thread(
        target=run_flush,
        name="manual-outbox-flush",
    )
    flush_thread.start()
    assert sink.entered.wait(1.0)
    close_done = threading.Event()
    close_thread = threading.Thread(
        target=lambda: (omd.close(), close_done.set()),
        name="coordinator-close",
    )
    close_thread.start()
    assert not close_done.wait(0.05)

    sink.release.set()
    flush_thread.join(2.0)
    close_thread.join(2.0)

    assert not flush_thread.is_alive() and not close_thread.is_alive()
    assert flush_errors == [] and close_done.is_set()
    assert omd.store.get_admission_outbox(row["event_id"])["state"] == "DELIVERED"
    attempts_before = omd.store.get_admission_outbox(row["event_id"])["attempts"]
    with pytest.raises(RuntimeError, match="dispatcher is closed"):
        omd.flush_admission_outbox(now=row["available_at"])
    assert omd.store.get_admission_outbox(row["event_id"])["attempts"] == attempts_before


def test_completed_late_outcome_registry_is_bounded(tmp_path):
    class SlowPort:
        backend = object()
        service = "omd"

        @staticmethod
        def deliver(envelope):
            del envelope
            time.sleep(0.03)

        @staticmethod
        def emit(*args, **kwargs):
            del args, kwargs

    omd = _omd(
        tmp_path,
        notification_timeout=0.005,
        notification_max_inflight=1,
    )
    omd.events = SlowPort()

    for index in range(5):
        with pytest.raises(TimeoutError):
            omd._deliver_admission_notification({"event_id": f"evt-{index}"})
        _wait_until(
            lambda: all(
                attempt["done"].is_set()
                for attempt in omd._notification_attempts.values()
            )
        )
        with omd._notification_attempt_lock:
            assert len(omd._notification_attempts) <= 1
    omd.close()


def test_notifier_baseexception_is_retried_not_acked(tmp_path):
    class ExitCollector(Collector):
        def ship(self, envelopes):
            del envelopes
            raise SystemExit("sink worker exited")

    omd = _omd(tmp_path)
    omd.claim("agent", ["src/**"], request_id="claim-1")
    row = omd.store.admission_outbox_rows()[0]
    sink = ExitCollector()
    sink.coordinator = omd
    omd.events = Emitter(sink)

    result = omd.flush_admission_outbox(now=row["available_at"])

    failed = omd.store.get_admission_outbox(row["event_id"])
    assert result["failed"] == 1 and failed["state"] == "PENDING"
    assert "SystemExit" in failed["last_error"]
    omd.close()


def test_late_baseexception_outcome_is_normalized_on_reuse(tmp_path):
    class SlowExitCollector(Collector):
        def ship(self, envelopes):
            del envelopes
            time.sleep(0.03)
            raise SystemExit("late sink worker exit")

    omd = _omd(tmp_path, notification_timeout=0.005)
    omd.claim("agent", ["src/**"], request_id="claim-1")
    row = omd.store.admission_outbox_rows()[0]
    sink = SlowExitCollector()
    sink.coordinator = omd
    omd.events = Emitter(sink)

    first = omd.flush_admission_outbox(now=row["available_at"])
    assert first["failed"] == 1
    _wait_until(
        lambda: omd._notification_attempts[row["event_id"]]["done"].is_set()
    )
    pending = omd.store.get_admission_outbox(row["event_id"])

    second = omd.flush_admission_outbox(now=pending["available_at"])

    retried = omd.store.get_admission_outbox(row["event_id"])
    assert second["failed"] == 1 and retried["state"] == "PENDING"
    assert retried["attempts"] == 2 and "SystemExit" in retried["last_error"]
    omd.close()


def test_empty_agent_reclaim_is_outboxed_and_rollback_has_no_ghost(
    tmp_path, monkeypatch
):
    collector = Collector()
    omd = _omd(tmp_path, collector=collector)
    omd.heartbeat("empty-agent")
    original_reconcile = omd._reconcile_admission
    calls = 0

    def fail_late_reconcile(*args, **kwargs):
        nonlocal calls
        calls += 1
        if calls == 2:
            raise RuntimeError("late reconcile failed")
        return original_reconcile(*args, **kwargs)

    monkeypatch.setattr(omd, "_reconcile_admission", fail_late_reconcile)
    with pytest.raises(RuntimeError, match="late reconcile failed"):
        omd.bail("empty-agent")

    agent = omd.store.get_agent("empty-agent")
    assert agent["state"] == "WORKING" and agent["bail_epoch"] == 0
    assert omd.store.admission_outbox_rows() == []
    assert not any(
        event.get("event") == "agent_reclaimed" for event in collector.events
    )

    monkeypatch.setattr(omd, "_reconcile_admission", original_reconcile)
    result = omd.bail("empty-agent")
    assert result["orbits"] == []
    _wait_until(
        lambda: any(event["event"] == "agent_reclaimed" for event in collector.events)
    )
    event = next(event for event in collector.events if event["event"] == "agent_reclaimed")
    assert event["notification_schema"] == "coordination_notification/v1"
    assert event["orbits"] == 0 and event["predecessor_event_ids"] == []
    assert event["orbit_id"].startswith("agent-reclaim:")
    omd.close()


def test_agent_reclaimed_waits_for_every_released_orbit_stream(tmp_path):
    class SelectiveCollector(Collector):
        def __init__(self, failed_event_id):
            super().__init__()
            self.failed_event_id = failed_event_id
            self.fail_selected = True

        def ship(self, envelopes):
            if (
                self.fail_selected
                and envelopes[0]["event_id"] == self.failed_event_id
            ):
                raise RuntimeError("selected release unavailable")
            super().ship(envelopes)

    omd = _omd(tmp_path)
    omd.claim("agent", ["a/**"], request_id="request-a")
    omd.claim("agent", ["b/**"], request_id="request-b")
    initial = Collector()
    initial.coordinator = omd
    omd.events = Emitter(initial)
    omd.flush_admission_outbox()
    omd.events = Emitter()

    omd.bail("agent")
    pending = omd.store.admission_outbox_rows(states=("PENDING",))
    releases = [
        row for row in pending
        if row["transition_kind"] in {
            "LEASE_OWNER_RECLAIMED", "WAIT_OWNER_RECLAIMED"
        }
    ]
    reclaimed = next(row for row in pending if row["transition_kind"] == "AGENT_RECLAIMED")
    assert len(releases) == 2
    assert set(omd.store.admission_outbox_predecessors(reclaimed["event_id"])) == {
        row["event_id"] for row in releases
    }
    sink = SelectiveCollector(releases[0]["event_id"])
    sink.coordinator = omd
    omd.events = Emitter(sink)

    first = omd.flush_admission_outbox(
        limit=10, now=max(row["available_at"] for row in pending)
    )

    assert first["failed"] == 1 and first["delivered"] == 1
    assert omd.store.get_admission_outbox(reclaimed["event_id"])["attempts"] == 0
    assert not any(event["event"] == "agent_reclaimed" for event in sink.events)

    sink.fail_selected = False
    failed_release = omd.store.get_admission_outbox(releases[0]["event_id"])
    omd.flush_admission_outbox(limit=10, now=failed_release["available_at"])
    assert [event["event"] for event in sink.events][-1] == "agent_reclaimed"
    assert all(
        omd.store.get_admission_outbox(row["event_id"])["state"] == "DELIVERED"
        for row in (*releases, reclaimed)
    )
    omd.close()


def test_schedule_next_transient_failure_rearms_dispatcher(tmp_path, monkeypatch):
    class FailOnceCollector(Collector):
        def __init__(self):
            super().__init__()
            self.calls = 0

        def ship(self, envelopes):
            self.calls += 1
            if self.calls == 1:
                raise RuntimeError("transient sink failure")
            super().ship(envelopes)

    omd = _omd(tmp_path)
    omd.claim("agent", ["src/**"], request_id="claim-1")
    row = omd.store.admission_outbox_rows()[0]
    sink = FailOnceCollector()
    sink.coordinator = omd
    omd.events = Emitter(sink)
    original_schedule_next = omd._schedule_next_admission_outbox
    calls = 0

    def fail_schedule_once():
        nonlocal calls
        calls += 1
        if calls == 1:
            raise sqlite3.OperationalError("temporary schedule read failure")
        return original_schedule_next()

    monkeypatch.setattr(omd, "_schedule_next_admission_outbox", fail_schedule_once)
    omd._wake_admission_outbox()

    _wait_until(
        lambda: omd.store.get_admission_outbox(row["event_id"])["state"]
        == "DELIVERED",
        timeout=3.0,
    )
    assert calls >= 2
    assert len(sink.notifications) == 1
    omd.close()


def test_transient_dispatcher_timer_start_failure_retries(tmp_path, monkeypatch):
    collector = Collector()
    original_start = threading.Thread.start
    failures = 0

    def fail_first_outbox_timer(worker):
        nonlocal failures
        if worker.name.startswith("omd-outbox-") and failures == 0:
            failures += 1
            raise RuntimeError("timer runtime temporarily unavailable")
        return original_start(worker)

    monkeypatch.setattr(threading.Thread, "start", fail_first_outbox_timer)
    omd = _omd(tmp_path, collector=collector)
    claim = omd.claim("agent", ["src/**"], request_id="claim-1")
    row = omd.store.admission_outbox_rows()[0]

    assert claim["state"] == "HELD" and failures == 1
    _wait_until(
        lambda: _outbox_row(omd, row["event_id"])["state"] == "DELIVERED"
    )
    assert len(collector.notifications) == 1
    omd.close()


def test_legacy_sink_reentrant_flush_runs_after_authority_unlock(tmp_path):
    omd = _omd(tmp_path)
    omd.claim("agent", ["src/**"], request_id="claim-1")
    omd.declare("before", writes=["before/**"])
    omd.declare("after", writes=["after/**"])

    class ReentrantCollector(Collector):
        def __init__(self):
            super().__init__()
            self.flush_result = None

        def ship(self, envelopes):
            assert not omd._lock._is_owned()
            assert omd.store._txn_depth == 0
            super().ship(envelopes)
            if envelopes[0]["event"] == "depend_added":
                self.flush_result = omd.flush_admission_outbox()

    sink = ReentrantCollector()
    sink.coordinator = omd
    omd.events = Emitter(sink)

    result = omd.depend("after", "before")

    assert result["ok"] is True
    assert sink.flush_result["delivered"] == 1
    assert [event["event"] for event in sink.events] == [
        "depend_added", "orbit_granted"
    ]
    omd.close()


def test_corrupt_event_mapping_is_retried_fail_closed(tmp_path):
    omd = _omd(tmp_path)
    omd.claim("agent", ["src/**"], request_id="claim-1")
    row = omd.store.admission_outbox_rows()[0]
    envelope = json.loads(row["payload"])
    envelope["event"] = "wrong-event"
    encoded = canonical_json(envelope)
    with omd.store.tx():
        omd.store.db.execute(
            "UPDATE admission_outbox SET payload=?,payload_sha256=? WHERE event_id=?",
            (encoded, sha256_json(envelope), row["event_id"]),
        )
    collector = Collector()
    collector.coordinator = omd
    omd.events = Emitter(collector)

    result = omd.flush_admission_outbox(now=row["available_at"])

    failed = omd.store.get_admission_outbox(row["event_id"])
    assert result["failed"] == 1 and failed["state"] == "PENDING"
    assert "does not match transition" in failed["last_error"]
    assert collector.notifications == []
    omd.close()
