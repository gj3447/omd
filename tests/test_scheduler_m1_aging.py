"""Versioned saturating aging and dynamic reservation-cycle safety."""

from __future__ import annotations

import json
import math
import sqlite3

import pytest

from omd_server import Coordinator
from omd_server.admission import (
    ADMISSION_POLICY_VERSION,
    LEGACY_ADMISSION_POLICY_VERSION,
    MAX_ADMISSION_PRIORITY,
    STATIC_ADMISSION_POLICY_VERSION,
    AdmissionRequest,
    QueuePolicy,
    canonical_json,
    decide_admission,
)
from omd_server.store import SCHEMA_VERSION


@pytest.mark.parametrize(
    "quantum", [0, -1, float("nan"), float("inf"), True, "bad", None]
)
def test_invalid_aging_quantum_fails_before_db_creation(tmp_path, quantum):
    db = tmp_path / "invalid-quantum.db"
    with pytest.raises(ValueError, match="aging_quantum"):
        Coordinator(str(db), admission_aging_quantum=quantum)
    assert not db.exists()


@pytest.mark.parametrize(
    "ceiling", [-1, True, 1.5, "2", None, MAX_ADMISSION_PRIORITY + 1]
)
def test_invalid_age_boost_ceiling_fails_before_db_creation(tmp_path, ceiling):
    db = tmp_path / "invalid-ceiling.db"
    with pytest.raises(ValueError, match="max_age_boost"):
        Coordinator(str(db), admission_max_age_boost=ceiling)
    assert not db.exists()


def test_policy_digest_and_saturating_rank_boundaries():
    policy = QueuePolicy(aging_quantum=10.0, max_age_boost=3)
    assert policy.version.endswith(policy.version.rsplit(":", 1)[-1])
    assert len(policy.version.rsplit(":", 1)[-1]) == 64
    assert QueuePolicy(10.0, 3).version == policy.version
    assert QueuePolicy(10.0, 4).version != policy.version

    def effective(observed_at):
        return policy.effective_priority(
            2,
            policy_version=policy.version,
            enqueued_at=100.0,
            observed_at=observed_at,
        )

    assert effective(99.0) == 2
    assert effective(109.999) == 2
    assert effective(110.0) == 3
    assert effective(129.999) == 4
    assert effective(130.0) == 5
    assert effective(10_000.0) == 5
    assert policy.rank_key(
        2,
        1,
        policy_version=policy.version,
        enqueued_at=100.0,
        observed_at=130.0,
    ) < policy.rank_key(
        5,
        2,
        policy_version=policy.version,
        enqueued_at=130.0,
        observed_at=130.0,
    )


def test_subnormal_quantum_saturates_before_floor_overflow():
    policy = QueuePolicy(aging_quantum=5e-324, max_age_boost=10)
    assert policy.effective_priority(
        1,
        policy_version=policy.version,
        enqueued_at=0.0,
        observed_at=1.0,
    ) == 11


def test_signed_64_bit_priority_requires_aging_headroom(tmp_path, monkeypatch):
    clock = {"now": 0.0}
    monkeypatch.setattr("omd_server.core.time.time", lambda: clock["now"])
    omd = Coordinator(
        str(tmp_path / "omd.db"),
        agent_ttl=None,
        admission_aging_quantum=1.0,
        admission_max_age_boost=10,
    )
    holder = omd.claim("holder", ["src/**"], ttl=100.0)
    safe_max = MAX_ADMISSION_PRIORITY - 10
    waiting = omd.claim(
        "waiting", ["src/a.py"], priority=safe_max, ttl=100.0
    )
    assert waiting["state"] == "PENDING"

    clock["now"] = 10.0
    omd.sweep()
    row = omd.store.get_orbit(waiting["orbit_id"])
    assert row["decision_effective_priority"] == MAX_ADMISSION_PRIORITY

    omd.release(holder["orbit_id"], "holder", holder["fence"])
    released = omd.store.get_orbit(holder["orbit_id"])
    assert released["decision_id"] is None
    assert released["decision_schema"] is None
    assert released["decision_observed_at"] is None
    assert released["decision_effective_priority"] is None
    promoted = omd.store.get_orbit(waiting["orbit_id"])
    assert promoted["state"] == "HELD"
    assert promoted["decision_effective_priority"] == MAX_ADMISSION_PRIORITY

    seq = omd.store.current_seq()
    rejected = omd.claim(
        "overflow", ["other/**"], priority=safe_max + 1
    )
    assert rejected["reason"] == "invalid_admission_request"
    assert omd.store.current_seq() == seq
    assert omd.admission_policy.effective_priority(
        MAX_ADMISSION_PRIORITY,
        policy_version=omd.admission_policy.version,
        enqueued_at=0.0,
        observed_at=0.0,
    ) is None
    assert omd.admission_policy.effective_priority(
        MAX_ADMISSION_PRIORITY,
        policy_version=STATIC_ADMISSION_POLICY_VERSION,
        enqueued_at=0.0,
        observed_at=100.0,
    ) == MAX_ADMISSION_PRIORITY
    omd.close()


def test_exact_v1_max_priority_row_replays_before_v2_headroom_gate(tmp_path):
    omd = Coordinator(str(tmp_path / "omd.db"), agent_ttl=None)
    omd.claim("holder", ["legacy/**"], ttl=1_000.0)
    waiting = omd.claim(
        "legacy",
        ["legacy/a.py"],
        priority=0,
        request_id="legacy-max",
    )
    assert waiting["state"] == "PENDING"
    with omd.store.tx():
        omd.store.db.execute(
            "UPDATE orbits SET priority=?, policy_version=? WHERE orbit_id=?",
            (
                MAX_ADMISSION_PRIORITY,
                STATIC_ADMISSION_POLICY_VERSION,
                waiting["orbit_id"],
            ),
        )

    replay = omd.claim(
        "legacy",
        ["legacy/a.py"],
        priority=MAX_ADMISSION_PRIORITY,
        request_id="legacy-max",
    )
    assert replay["state"] == "PENDING"
    assert replay["orbit_id"] == waiting["orbit_id"]
    assert replay["dedup"] is True
    assert omd.store.get_orbit(waiting["orbit_id"])["policy_version"] == (
        STATIC_ADMISSION_POLICY_VERSION
    )
    omd.close()


def test_begin_rejects_priority_without_headroom_before_declare(tmp_path):
    omd = Coordinator(str(tmp_path / "omd.db"), agent_ttl=None)
    result = omd.begin(
        "unsafe-priority",
        "worker",
        ["src/**"],
        priority=MAX_ADMISSION_PRIORITY,
    )
    assert result == {
        "ok": False,
        "stage": "validate",
        "reason": "invalid_priority",
        "priority": MAX_ADMISSION_PRIORITY,
    }
    assert omd.store.get_task("unsafe-priority") is None
    assert omd.store.snapshot()["orbits"] == []
    omd.close()


def test_begin_resumes_existing_v1_max_priority_intent(tmp_path):
    omd = Coordinator(str(tmp_path / "omd.db"), agent_ttl=None)
    omd.claim("holder", ["legacy/**"], ttl=1_000.0)
    omd.declare("legacy-task", writes=["legacy/**"])
    waiting = omd.claim(
        "legacy-worker",
        ["legacy/**"],
        task_id="legacy-task",
        priority=0,
    )
    assert waiting["state"] == "PENDING"
    with omd.store.tx():
        omd.store.db.execute(
            "UPDATE orbits SET priority=?, policy_version=? WHERE orbit_id=?",
            (
                MAX_ADMISSION_PRIORITY,
                STATIC_ADMISSION_POLICY_VERSION,
                waiting["orbit_id"],
            ),
        )
    seq = omd.store.current_seq()

    resumed = omd.begin(
        "legacy-task",
        "legacy-worker",
        ["legacy/**"],
        priority=MAX_ADMISSION_PRIORITY,
    )
    assert resumed["stage"] == "claim"
    assert resumed["state"] == "PENDING"
    assert resumed["orbit_id"] == waiting["orbit_id"]
    assert omd.store.current_seq() == seq

    with omd.store.tx():
        omd.store.db.execute(
            "UPDATE orbits SET wait_deadline=0 WHERE orbit_id=?",
            (waiting["orbit_id"],),
        )
    lost = omd.begin(
        "legacy-task",
        "legacy-worker",
        ["legacy/**"],
        priority=MAX_ADMISSION_PRIORITY,
    )
    assert lost["stage"] == "claim"
    assert lost["reason"] == "invalid_priority"
    assert lost["detail"] == "legacy resume authority is no longer live"
    assert omd.store.get_orbit(waiting["orbit_id"])["state"] == "DENIED"
    omd.close()


@pytest.mark.parametrize(
    "version", [STATIC_ADMISSION_POLICY_VERSION, LEGACY_ADMISSION_POLICY_VERSION]
)
def test_v1_and_legacy_rows_never_receive_v2_age_boost(version):
    policy = QueuePolicy(aging_quantum=1.0, max_age_boost=10)
    assert policy.effective_priority(
        -3,
        policy_version=version,
        enqueued_at=0.0,
        observed_at=10_000.0,
    ) == -3


def test_unknown_or_missing_pending_policy_blocks_fail_closed():
    policy = QueuePolicy(aging_quantum=10.0, max_age_boost=3)
    request = AdmissionRequest.build(
        ["src/a.py"],
        "write",
        100,
        2,
        policy_version=policy.version,
        enqueued_at=20.0,
    )
    base = {
        "orbit_id": "unknown",
        "agent_id": "unknown",
        "mode": "write",
        "pathspec": json.dumps(["src/**"]),
        "priority": -100,
        "queue_seq": 1,
        "enqueued_at": 0.0,
    }
    for row in (base, dict(base, policy_version="unknown-policy")):
        decision = decide_admission(
            request, [], [row], policy=policy, observed_at=20.0
        )
        assert decision.pending_predecessors == ("unknown",)


def test_repository_pins_canonical_policy_and_rejects_drift_before_recovery(tmp_path):
    db = str(tmp_path / "omd.db")
    first = Coordinator(
        db,
        agent_ttl=None,
        admission_aging_quantum=7.0,
        admission_max_age_boost=3,
    )
    expected = QueuePolicy(7.0, 3)
    assert first.admission_policy == expected
    assert first.store.get_meta("admission_queue_policy_version") == expected.version
    assert first.store.get_meta("admission_queue_policy_envelope") == canonical_json(
        expected.envelope
    )
    assert first.store.get_meta("admission_policy_initialized") == "1"
    first.resign()
    first.close()
    first.store.db.close()

    with pytest.raises(ValueError, match="aging configuration conflicts"):
        Coordinator(
            db,
            agent_ttl=None,
            admission_aging_quantum=8.0,
            admission_max_age_boost=3,
        )

    reopened = Coordinator(
        db,
        agent_ttl=None,
        admission_aging_quantum=7.0,
        admission_max_age_boost=3,
    )
    assert reopened.admission_policy.version == expected.version
    reopened.close()


def test_policy_mismatch_does_not_partially_pin_missing_peer_metadata(tmp_path):
    db = str(tmp_path / "omd.db")
    first = Coordinator(db, agent_ttl=None)
    with first.store.tx():
        first.store.db.execute(
            "DELETE FROM meta WHERE key IN "
            "('admission_queue_policy_version', 'admission_queue_policy_envelope')"
        )
    first.resign()
    first.close()
    first.store.db.close()

    with pytest.raises(ValueError, match="admission queue policy is missing"):
        Coordinator(db, agent_ttl=None, admission_queue_capacity=1)
    with sqlite3.connect(db) as connection:
        assert connection.execute(
            "SELECT value FROM meta WHERE key='admission_queue_policy_version'"
        ).fetchone() is None

    with pytest.raises(ValueError, match="admission queue policy is missing"):
        Coordinator(db, agent_ttl=None)

    capacity_db = str(tmp_path / "capacity.db")
    capacity_owner = Coordinator(capacity_db, agent_ttl=None)
    with capacity_owner.store.tx():
        capacity_owner.store.db.execute(
            "DELETE FROM meta WHERE key='admission_queue_capacity'"
        )
    capacity_owner.resign()
    capacity_owner.close()
    capacity_owner.store.db.close()

    with pytest.raises(ValueError, match="admission_queue_capacity is missing"):
        Coordinator(capacity_db, agent_ttl=None)
    with sqlite3.connect(capacity_db) as connection:
        assert connection.execute(
            "SELECT value FROM meta WHERE key='admission_queue_capacity'"
        ).fetchone() is None


def test_current_schema_missing_policy_fails_before_repinning_live_v2_rows(tmp_path):
    db = str(tmp_path / "omd.db")
    first = Coordinator(
        db,
        agent_ttl=None,
        admission_aging_quantum=7.0,
        admission_max_age_boost=3,
    )
    first.claim("holder", ["src/**"], ttl=1_000.0)
    pending = first.claim("waiter", ["src/**"], request_id="waiting")
    assert pending["state"] == "PENDING"
    with first.store.tx():
        first.store.db.execute(
            "DELETE FROM meta WHERE key IN "
            "('admission_queue_policy_version', 'admission_queue_policy_envelope', "
            "'admission_policy_initialized')"
        )
    first.resign()
    first.close()
    first.store.db.close()

    with pytest.raises(ValueError, match="admission queue policy is missing"):
        Coordinator(
            db,
            agent_ttl=None,
            admission_aging_quantum=8.0,
            admission_max_age_boost=3,
        )
    with sqlite3.connect(db) as connection:
        assert connection.execute(
            "SELECT value FROM meta WHERE key='admission_queue_policy_version'"
        ).fetchone() is None
        assert connection.execute(
            "SELECT value FROM meta WHERE key='admission_policy_initialized'"
        ).fetchone() is None
        row = connection.execute(
            "SELECT state, policy_version FROM orbits WHERE orbit_id=?",
            (pending["orbit_id"],),
        ).fetchone()
    assert row == ("PENDING", QueuePolicy(7.0, 3).version)


def test_self_consistent_meta_cannot_reinterpret_persisted_v2_rows(tmp_path):
    db = str(tmp_path / "omd.db")
    policy_a = QueuePolicy(7.0, 3)
    policy_b = QueuePolicy(8.0, 3)
    first = Coordinator(
        db,
        agent_ttl=None,
        admission_aging_quantum=policy_a.aging_quantum,
        admission_max_age_boost=policy_a.max_age_boost,
    )
    first.claim("holder", ["src/**"], ttl=1_000.0)
    pending = first.claim("waiter", ["src/**"], request_id="waiting")
    with first.store.tx():
        first.store.set_meta("admission_queue_policy_version", policy_b.version)
        first.store.set_meta(
            "admission_queue_policy_envelope", canonical_json(policy_b.envelope)
        )
    first.resign()
    first.close()
    first.store.db.close()

    with pytest.raises(ValueError, match="does not match persisted v2 rows"):
        Coordinator(
            db,
            agent_ttl=None,
            admission_aging_quantum=policy_b.aging_quantum,
            admission_max_age_boost=policy_b.max_age_boost,
        )
    with sqlite3.connect(db) as connection:
        assert connection.execute(
            "SELECT value FROM meta WHERE key='admission_queue_policy_version'"
        ).fetchone()[0] == policy_b.version
        row = connection.execute(
            "SELECT state, policy_version FROM orbits WHERE orbit_id=?",
            (pending["orbit_id"],),
        ).fetchone()
    assert row == ("PENDING", policy_a.version)


def test_interrupted_policy_initialization_resumes_once_then_marks_complete(tmp_path):
    db = str(tmp_path / "omd.db")
    first = Coordinator(db, agent_ttl=None)
    with first.store.tx():
        first.store.db.execute(
            "DELETE FROM meta WHERE key IN ("
            "'admission_queue_capacity', "
            "'admission_queue_policy_version', "
            "'admission_queue_policy_envelope', "
            "'admission_policy_initialized')"
        )
    first.resign()
    first.close()
    first.store.db.close()

    resumed = Coordinator(db, agent_ttl=None)
    expected = QueuePolicy()
    assert resumed.store.get_meta("admission_queue_capacity") == "1024"
    assert resumed.store.get_meta("admission_queue_policy_version") == expected.version
    assert resumed.store.get_meta("admission_queue_policy_envelope") == canonical_json(
        expected.envelope
    )
    assert resumed.store.get_meta("admission_policy_initialized") == "1"
    resumed.close()


@pytest.mark.parametrize("payload", ["{not-json", "[]", "null", '"scalar"'])
def test_malformed_durable_policy_fails_closed_and_resigns(tmp_path, payload):
    db = str(tmp_path / "omd.db")
    first = Coordinator(db, agent_ttl=None)
    with first.store.tx():
        first.store.set_meta("admission_queue_policy_envelope", payload)
    first.resign()
    first.close()
    first.store.db.close()

    with pytest.raises(ValueError, match="durable admission queue policy is invalid"):
        Coordinator(db, agent_ttl=None)
    with sqlite3.connect(db) as connection:
        raw = connection.execute(
            "SELECT value FROM meta WHERE key='leader_lease'"
        ).fetchone()[0]
    assert json.loads(raw)["last_heartbeat"] == 0


def test_known_m1_schema_predecessor_migrates_to_aging_schema(tmp_path):
    db = str(tmp_path / "omd.db")
    first = Coordinator(db, agent_ttl=None)
    with first.store.tx():
        first.store.set_meta("schema_version", "omd/2026-07-15-m1")
    first.resign()
    first.close()
    first.store.db.close()

    migrated = Coordinator(db, agent_ttl=None)
    assert migrated.store.get_meta("schema_version") == SCHEMA_VERSION
    columns = {
        row["name"]
        for row in migrated.store.db.execute("PRAGMA table_info(orbits)")
    }
    assert {
        "decision_schema",
        "decision_observed_at",
        "decision_effective_priority",
    } <= columns
    migrated.close()


def test_aging_overtakes_fresh_priority_at_quantum_boundary(tmp_path, monkeypatch):
    clock = {"now": 100.0}
    monkeypatch.setattr("omd_server.core.time.time", lambda: clock["now"])
    omd = Coordinator(
        str(tmp_path / "omd.db"),
        agent_ttl=None,
        admission_aging_quantum=10.0,
        admission_max_age_boost=10,
    )
    holder = omd.claim("holder", ["src/**"], ttl=1_000.0)
    low = omd.claim("low", ["src/**"], priority=0, ttl=100.0)
    assert low["state"] == "PENDING" and low["observed_at"] == 100.0

    clock["now"] = 149.0
    high = omd.claim("high", ["src/a.py"], priority=5, ttl=100.0)
    assert high["state"] == "PENDING"
    assert low["orbit_id"] not in high["pending_predecessors"]

    clock["now"] = 150.0
    omd.release(holder["orbit_id"], "holder", holder["fence"])
    low_row = omd.store.get_orbit(low["orbit_id"])
    high_row = omd.store.get_orbit(high["orbit_id"])
    assert low_row["state"] == "HELD"
    assert high_row["state"] == "PENDING"
    assert low_row["decision_observed_at"] == 150.0
    assert low_row["decision_effective_priority"] == 5
    omd.close()


def test_restart_preserves_policy_and_aging_order(tmp_path, monkeypatch):
    clock = {"now": 100.0}
    monkeypatch.setattr("omd_server.core.time.time", lambda: clock["now"])
    db = str(tmp_path / "omd.db")
    first = Coordinator(
        db,
        agent_ttl=None,
        admission_aging_quantum=10.0,
        admission_max_age_boost=10,
    )
    holder = first.claim("holder", ["src/**"], ttl=1_000.0)
    low = first.claim("low", ["src/**"], priority=0)
    clock["now"] = 149.0
    high = first.claim("high", ["src/a.py"], priority=5)
    version = first.admission_policy.version
    first.resign()
    first.close()
    first.store.db.close()

    clock["now"] = 150.0
    reopened = Coordinator(
        db,
        agent_ttl=None,
        admission_aging_quantum=10.0,
        admission_max_age_boost=10,
    )
    assert reopened.admission_policy.version == version
    reopened.release(holder["orbit_id"], "holder", holder["fence"])
    assert reopened.store.get_orbit(low["orbit_id"])["state"] == "HELD"
    assert reopened.store.get_orbit(high["orbit_id"])["state"] == "PENDING"
    reopened.close()


def test_begin_preflight_and_nested_claim_share_one_observed_time(
    tmp_path, monkeypatch
):
    clock = {"now": 100.0}

    def ticking_clock():
        value = clock["now"]
        clock["now"] += 1.0
        return value

    monkeypatch.setattr("omd_server.core.time.time", ticking_clock)
    omd = Coordinator(
        str(tmp_path / "omd.db"),
        agent_ttl=None,
        admission_aging_quantum=10.0,
    )
    omd.claim("holder", ["src/**"], ttl=1_000.0)
    observed = []
    original = omd._admission_decision

    def capture(*args, **kwargs):
        observed.append(kwargs["observed_at"])
        return original(*args, **kwargs)

    monkeypatch.setattr(omd, "_admission_decision", capture)
    result = omd.begin(
        "task",
        "worker",
        ["src/a.py"],
        request_id="begin-one-clock",
    )
    assert result["stage"] == "claim" and result["state"] == "PENDING"
    assert len(observed) >= 2 and len(set(observed)) == 1
    omd.close()


def test_sweep_reclaim_and_promotion_share_one_observed_time(tmp_path, monkeypatch):
    omd = Coordinator(
        str(tmp_path / "omd.db"),
        agent_ttl=10.0,
        admission_aging_quantum=10.0,
    )
    holder = omd.claim("stale", ["src/**"], ttl=1_000.0)
    waiter = omd.claim("waiter", ["src/**"], request_id="waiting")
    assert holder["state"] == "HELD" and waiter["state"] == "PENDING"
    with omd.store.tx():
        omd.store.db.execute(
            "UPDATE agents SET last_heartbeat=0 WHERE agent_id='stale'"
        )

    clock = {"now": 100.0}
    calls = []

    def ticking_clock():
        value = clock["now"]
        clock["now"] += 1.0
        calls.append(value)
        return value

    stale_observations = []
    original_stale_agents = omd.store.stale_agents

    def capture_stale_agents(now, fallback_ttl):
        stale_observations.append(now)
        return original_stale_agents(now, fallback_ttl)

    decision_observations = []
    original_decision = omd._admission_decision

    def capture_decision(*args, **kwargs):
        decision_observations.append(kwargs["observed_at"])
        return original_decision(*args, **kwargs)

    monkeypatch.setattr("omd_server.core.time.time", ticking_clock)
    monkeypatch.setattr(omd.store, "stale_agents", capture_stale_agents)
    monkeypatch.setattr(omd, "_admission_decision", capture_decision)

    with omd._cs():
        observed_at = omd._sweep_inline()

    promoted = omd.store.get_orbit(waiter["orbit_id"])
    assert calls == [100.0]
    assert stale_observations == [observed_at] == [100.0]
    assert decision_observations and set(decision_observations) == {observed_at}
    assert promoted["state"] == "HELD"
    assert promoted["decision_observed_at"] == observed_at
    omd.close()


def test_rank_change_cycle_denies_latest_participating_ticket(tmp_path, monkeypatch):
    clock = {"now": 0.0}
    monkeypatch.setattr("omd_server.core.time.time", lambda: clock["now"])
    omd = Coordinator(
        str(tmp_path / "omd.db"),
        agent_ttl=None,
        admission_aging_quantum=60.0,
        admission_max_age_boost=10,
    )
    omd.claim("agent-a", ["a/**"], ttl=1_000.0)
    omd.claim("agent-c", ["b/**"], ttl=1_000.0)
    broad = omd.claim("agent-b", ["a/**", "b/**"], priority=0)
    assert broad["state"] == "PENDING"

    clock["now"] = 59.0
    later = omd.claim("agent-a", ["b/**"], priority=1)
    assert later["state"] == "PENDING"
    assert broad["orbit_id"] not in later["pending_predecessors"]

    clock["now"] = 60.0
    omd.sweep()
    broad_row = omd.store.get_orbit(broad["orbit_id"])
    later_row = omd.store.get_orbit(later["orbit_id"])
    assert broad_row["state"] == "PENDING"
    assert later_row["state"] == "DENIED"
    assert later_row["terminal_reason"] == "reservation_cycle_after_rank_change"
    assert later_row["decision_type"] == "PROMOTION_DENIED"
    assert later_row["decision_observed_at"] == 60.0
    assert later_row["decision_effective_priority"] == 1
    assert later_row["queue_seq"] > broad_row["queue_seq"]
    omd.close()


def test_immediate_grant_resolves_new_held_ownership_cycle(tmp_path, monkeypatch):
    clock = {"now": 0.0}
    monkeypatch.setattr("omd_server.core.time.time", lambda: clock["now"])
    omd = Coordinator(str(tmp_path / "omd.db"), agent_ttl=None)

    omd.claim("agent-b", ["x/**"], ttl=1_000.0)
    omd.claim("agent-c", ["z/**"], ttl=1_000.0)
    a_wait = omd.claim("agent-a", ["x/**"])
    b_wait = omd.claim("agent-b", ["y/**", "z/**"])
    assert a_wait["state"] == b_wait["state"] == "PENDING"

    granted = omd.claim("agent-a", ["y/**"], priority=10)
    assert granted["state"] == "HELD"
    b_row = omd.store.get_orbit(b_wait["orbit_id"])
    assert b_row["state"] == "DENIED"
    assert b_row["terminal_reason"] == "reservation_cycle_after_grant"
    assert b_row["queue_seq"] > omd.store.get_orbit(a_wait["orbit_id"])["queue_seq"]
    assert omd._find_cycle(omd._wait_for(clock["now"])) is None
    omd.close()


def test_status_exposes_one_rank_view_and_active_policy(tmp_path, monkeypatch):
    clock = {"now": 10.0}
    monkeypatch.setattr("omd_server.core.time.time", lambda: clock["now"])
    omd = Coordinator(
        str(tmp_path / "omd.db"),
        agent_ttl=None,
        admission_aging_quantum=5.0,
        admission_max_age_boost=2,
    )
    omd.claim("holder", ["src/**"], ttl=1_000.0)
    waiting = omd.claim("waiting", ["src/a.py"], priority=-1)
    clock["now"] = 20.0
    status = omd.status()["admission_queue"]
    assert status["observed_at"] == 20.0
    assert status["policy_version"] == omd.admission_policy.version
    assert status["policy"] == omd.admission_policy.envelope
    row = next(item for item in status["pending"] if item["orbit_id"] == waiting["orbit_id"])
    assert row["base_priority"] == -1
    assert row["age_boost"] == 2
    assert row["effective_priority"] == 1
    omd.close()


def test_default_policy_version_is_content_addressed():
    assert ADMISSION_POLICY_VERSION == QueuePolicy().version
    assert QueuePolicy().envelope["priority_domain"] == (
        "signed-64-with-ceiling-headroom/v1"
    )
    digest = ADMISSION_POLICY_VERSION.rsplit(":", 1)[-1]
    assert len(digest) == 64 and all(ch in "0123456789abcdef" for ch in digest)
    assert math.isfinite(QueuePolicy().aging_quantum)
