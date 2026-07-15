"""M1 fair-admission slice: durable rank, no-overtaking, and cycle safety."""

from __future__ import annotations

import hashlib
import json
import shutil
import time
from pathlib import Path

import pytest

from omd_server import Coordinator
from omd_server.admission import (
    LEGACY_ADMISSION_POLICY_VERSION,
    AdmissionRequest,
    decide_admission,
    modes_compatible,
    rank_key,
)
from benchmarks.produce_scheduler_m1_receipt import build_receipt, produce


ROOT = Path(__file__).resolve().parents[1]
FROZEN_GATE_SHA256 = "7e249d738e941c2a56e6d8846ddc2d5b6489c95a0238d5471301c63bea19c4d1"


def _row(orbit_id, mode, pathspec, *, priority=0, queue_seq=0):
    return {
        "orbit_id": orbit_id,
        "agent_id": orbit_id,
        "mode": mode,
        "pathspec": json.dumps(pathspec),
        "priority": priority,
        "queue_seq": queue_seq,
        "fence": None,
    }


@pytest.mark.parametrize(
    ("left", "right", "compatible"),
    [
        ("read", "read", True),
        ("shared", "shared", True),
        ("read", "write", False),
        ("write", "read", False),
        ("read", "shared", False),
        ("shared", "read", False),
        ("write", "write", False),
        ("write", "shared", False),
        ("shared", "write", False),
    ],
)
def test_mode_compatibility_table(left, right, compatible):
    assert modes_compatible(left, right) is compatible


def test_pure_decision_distinguishes_held_pending_and_disjoint():
    held = [_row("held", "write", ["src/a.py"], queue_seq=0)]
    pending = [_row("older", "write", ["src/**"], priority=10, queue_seq=1)]
    request = AdmissionRequest.build(["src/b.py"], "write", 0, 2)
    decision = decide_admission(request, held, pending)
    assert decision.held_blockers == ()
    assert decision.pending_predecessors == ("older",)
    assert decision.outcome == "QUEUE"

    disjoint = AdmissionRequest.build(["docs/**"], "write", 0, 3)
    assert decide_admission(disjoint, held, pending).outcome == "GRANT"


def test_missing_pending_queue_authority_blocks_fail_closed():
    corrupt_predecessor = [
        _row("missing-ticket", "write", ["src/**"], priority=0, queue_seq=None)
    ]
    request = AdmissionRequest.build(["src/a.py"], "write", 100, 2)
    decision = decide_admission(request, [], corrupt_predecessor)
    assert decision.outcome == "QUEUE"
    assert decision.pending_predecessors == ("missing-ticket",)


def test_blocker_identity_order_is_canonical_under_row_permutation():
    held = [
        _row("held-z", "write", ["src/**"]),
        _row("held-a", "write", ["src/**"]),
    ]
    pending = [
        _row("pending-z", "write", ["src/**"], queue_seq=0),
        _row("pending-a", "write", ["src/**"], queue_seq=1),
    ]
    request = AdmissionRequest.build(["src/a.py"], "write", 0, 3)
    forward = decide_admission(request, held, pending)
    reversed_rows = decide_admission(request, reversed(held), reversed(pending))
    assert forward == reversed_rows
    assert forward.blocker_ids == (
        "held-a", "held-z", "pending-a", "pending-z"
    )


def test_rank_is_priority_desc_then_durable_fifo():
    assert rank_key(10, 99) < rank_key(0, 1)
    assert rank_key(5, 1) < rank_key(5, 2)


def test_locked_no_overtaking_trace_and_bounded_promotion(tmp_path):
    omd = Coordinator(str(tmp_path / "omd.db"), agent_ttl=None)
    holder = omd.claim("holder", ["src/a.py"], priority=0, ttl=60)
    older = omd.claim("older", ["src/**"], priority=10, ttl=17)
    newer = omd.claim("newer", ["src/b.py"], priority=0, ttl=23)

    assert holder["state"] == "HELD"
    assert older["state"] == "PENDING"
    assert newer["state"] == "PENDING"
    assert newer["pending_predecessors"] == [older["orbit_id"]]
    assert holder["queue_seq"] < older["queue_seq"] < newer["queue_seq"]

    omd.release(holder["orbit_id"], "holder", holder["fence"])
    older_row = omd.store.get_orbit(older["orbit_id"])
    newer_row = omd.store.get_orbit(newer["orbit_id"])
    assert older_row["state"] == "HELD"
    assert newer_row["state"] == "PENDING"
    assert 15 <= older_row["expires_at"] - time.time() <= 17

    omd.release(older["orbit_id"], "older", older_row["fence"])
    newer_row = omd.store.get_orbit(newer["orbit_id"])
    assert newer_row["state"] == "HELD"
    assert 21 <= newer_row["expires_at"] - time.time() <= 23
    omd.close()


def test_unrelated_global_head_does_not_block_disjoint_work(tmp_path):
    omd = Coordinator(str(tmp_path / "omd.db"), agent_ttl=None)
    omd.claim("holder", ["src/a.py"])
    omd.claim("head", ["src/**"], priority=100)
    result = omd.claim("docs", ["docs/**"], priority=-100)
    assert result["state"] == "HELD"
    omd.close()


def test_pending_writer_precedes_later_reader_but_compatible_modes_coexist(tmp_path):
    omd = Coordinator(str(tmp_path / "omd.db"), agent_ttl=None)
    holder = omd.claim("reader-a", ["src/a.py"], mode="read")
    writer = omd.claim("writer", ["src/**"], mode="write", priority=10)
    later_reader = omd.claim("reader-b", ["src/b.py"], mode="read")
    assert writer["state"] == "PENDING"
    assert later_reader["state"] == "PENDING"
    assert later_reader["pending_predecessors"] == [writer["orbit_id"]]
    omd.release(holder["orbit_id"], "reader-a", holder["fence"])
    assert omd.store.get_orbit(writer["orbit_id"])["state"] == "HELD"

    other = Coordinator(str(tmp_path / "other.db"), agent_ttl=None)
    assert other.claim("r1", ["x/**"], mode="read")["state"] == "HELD"
    assert other.claim("r2", ["x/**"], mode="read")["state"] == "HELD"
    assert other.claim("s1", ["y/**"], mode="shared")["state"] == "HELD"
    assert other.claim("s2", ["y/**"], mode="shared")["state"] == "HELD"
    omd.close()
    other.close()


def test_reservation_edge_cycle_is_denied_before_exposure(tmp_path):
    omd = Coordinator(str(tmp_path / "omd.db"), agent_ttl=None)
    omd.claim("agent-a", ["src/a.py"], priority=0)
    broad = omd.claim("agent-b", ["src/**"], priority=10)
    assert broad["state"] == "PENDING"  # B -> A through HELD ownership

    # No HELD lease covers src/b.py.  The only blocker is B's higher-ranked
    # reservation, which would add A -> B and close A <-> B.
    cycle = omd.claim("agent-a", ["src/b.py"], priority=0)
    assert cycle["state"] == "DENIED"
    assert cycle["deadlock"] is True
    assert cycle["reason"] == "reservation_cycle"
    assert omd.store.get_orbit(cycle["orbit_id"])["terminal_reason"] == "reservation_cycle"
    omd.close()


def test_restart_preserves_queue_sequence_and_promotion_order(tmp_path):
    db = tmp_path / "omd.db"
    first = Coordinator(str(db), agent_ttl=None)
    holder = first.claim("holder", ["a/**"])
    low = first.claim("low", ["a/**"], priority=0)
    high = first.claim("high", ["a/**"], priority=9)
    before = {
        orbit_id: {
            "queue_seq": first.store.get_orbit(orbit_id)["queue_seq"],
            "wait_deadline": first.store.get_orbit(orbit_id)["wait_deadline"],
            "policy_version": first.store.get_orbit(orbit_id)["policy_version"],
        }
        for orbit_id in (low["orbit_id"], high["orbit_id"])
    }
    first.resign()
    first.close()

    second = Coordinator(str(db), agent_ttl=None)
    for orbit_id in (low["orbit_id"], high["orbit_id"]):
        row = second.store.get_orbit(orbit_id)
        assert row["queue_seq"] == before[orbit_id]["queue_seq"]
        assert row["wait_deadline"] == before[orbit_id]["wait_deadline"]
        assert row["policy_version"] == before[orbit_id]["policy_version"]
    second.release(holder["orbit_id"], "holder", holder["fence"])
    assert second.store.get_orbit(high["orbit_id"])["state"] == "HELD"
    assert second.store.get_orbit(low["orbit_id"])["state"] == "PENDING"
    second.resign()
    second.close()


def test_legacy_pending_rows_are_backfilled_once_in_stable_order(tmp_path):
    db = tmp_path / "omd.db"
    omd = Coordinator(str(db), agent_ttl=None)
    holder = omd.claim("holder", ["a/**"])
    first = omd.claim("first", ["a/**"])
    second = omd.claim("second", ["a/**"])
    with omd.store.tx():
        omd.store.db.execute("DELETE FROM meta WHERE key='schema_version'")
        omd.store.db.execute(
            "UPDATE orbits SET queue_seq=NULL, requested_ttl=NULL, "
            "policy_version=NULL, pathspec_digest=NULL, request_id=NULL, "
            "enqueued_at=NULL, wait_deadline=NULL, created_at=10 WHERE orbit_id=?",
            (first["orbit_id"],),
        )
        omd.store.db.execute(
            "UPDATE orbits SET queue_seq=NULL, created_at=20 WHERE orbit_id=?",
            (second["orbit_id"],),
        )
    omd.resign()
    omd.close()

    migrated = Coordinator(str(db), agent_ttl=None)
    first_row = migrated.store.get_orbit(first["orbit_id"])
    first_seq = first_row["queue_seq"]
    second_seq = migrated.store.get_orbit(second["orbit_id"])["queue_seq"]
    assert first_seq < second_seq
    assert first_row["requested_ttl"] == 600.0
    assert first_row["policy_version"] == LEGACY_ADMISSION_POLICY_VERSION
    assert first_row["pathspec_digest"]
    assert first_row["request_id"] == f"internal:{first['orbit_id']}"
    assert first_row["enqueued_at"] == 10
    assert first_row["wait_deadline"] == 3610
    migrated.resign()
    migrated.close()
    reopened = Coordinator(str(db), agent_ttl=None)
    assert reopened.store.get_orbit(first["orbit_id"])["queue_seq"] == first_seq
    assert reopened.store.get_orbit(second["orbit_id"])["queue_seq"] == second_seq
    # Keep the holder variable live in the fixture to make the intended blocker explicit.
    assert holder["state"] == "HELD"
    reopened.close()


def test_due_pending_wait_times_out_before_it_can_promote(tmp_path):
    omd = Coordinator(str(tmp_path / "omd.db"), agent_ttl=None)
    holder = omd.claim("holder", ["a/**"])
    waiting = omd.claim("waiter", ["a/**"], request_id="timeout-request")
    with omd.store.tx():
        omd.store.set_orbit(waiting["orbit_id"], wait_deadline=time.time() - 1)

    omd.sweep()
    row = omd.store.get_orbit(waiting["orbit_id"])
    assert row["state"] == "DENIED"
    assert row["decision_type"] == "WAIT_TIMEOUT"
    assert row["terminal_reason"] == "wait_timeout"
    assert row["released_at"] is not None

    omd.release(holder["orbit_id"], "holder", holder["fence"])
    assert omd.store.get_orbit(waiting["orbit_id"])["state"] == "DENIED"
    omd.close()


def test_restart_reconciles_due_wait_before_promotion(tmp_path):
    db = tmp_path / "omd.db"
    first = Coordinator(str(db), agent_ttl=None)
    first.claim("holder", ["a/**"])
    waiting = first.claim("waiter", ["a/**"])
    with first.store.tx():
        first.store.set_orbit(waiting["orbit_id"], wait_deadline=time.time() - 1)
    first.resign()
    first.close()

    reopened = Coordinator(str(db), agent_ttl=None)
    row = reopened.store.get_orbit(waiting["orbit_id"])
    assert row["state"] == "DENIED"
    assert row["terminal_reason"] == "wait_timeout"
    reopened.close()


def test_database_move_preserves_repository_identity(tmp_path):
    original = tmp_path / "original.db"
    moved = tmp_path / "restored.db"
    first = Coordinator(str(original), agent_ttl=None)
    first.claim("holder", ["a/**"])
    waiting = first.claim("waiter", ["a/**"], request_id="move-request")
    repository_id = first.repository_id
    original_row = first.store.get_orbit(waiting["orbit_id"])
    first.resign()
    first.close()
    first.store.db.close()
    shutil.copy2(original, moved)

    restored = Coordinator(str(moved), agent_ttl=None)
    assert restored.repository_id == repository_id
    assert restored.store.get_meta("repository_id") == repository_id
    restored_row = restored.store.get_orbit(waiting["orbit_id"])
    assert restored._admission_identity(restored_row)["repository_id"] == repository_id
    assert restored_row["request_id"] == original_row["request_id"]
    assert restored_row["pathspec_digest"] == original_row["pathspec_digest"]
    restored.close()


def test_stale_pending_owner_is_reclaimed_before_release_promotion(tmp_path):
    omd = Coordinator(str(tmp_path / "omd.db"), agent_ttl=10.0)
    holder = omd.claim("holder", ["a/**"])
    waiting = omd.claim("waiter", ["a/**"])
    with omd.store.tx():
        omd.store.db.execute(
            "UPDATE agents SET last_heartbeat=? WHERE agent_id='waiter'",
            (time.time() - 100.0,),
        )
    omd.release(holder["orbit_id"], "holder", holder["fence"])
    row = omd.store.get_orbit(waiting["orbit_id"])
    assert row["state"] == "DENIED"
    assert omd.store.get_agent("waiter")["state"] == "RETIRED"
    omd.close()


def test_begin_preflight_sees_pending_predecessor_without_partial_held(tmp_path):
    omd = Coordinator(str(tmp_path / "omd.db"), agent_ttl=None)
    omd.claim("holder", ["src/a.py"])
    omd.claim("older", ["src/a.py", "shared/**"], priority=10)
    result = omd.begin(
        "task",
        "beginner",
        ["isolated/**"],
        shared=["shared/**"],
        priority=0,
    )
    assert result["ok"] is False and result["stage"] == "claim"
    owned = omd.store.orbits_owned_by_agent("beginner", states=("HELD", "PENDING"))
    assert all(row["state"] != "HELD" for row in owned)
    assert any(row["state"] == "PENDING" for row in owned)
    omd.close()


def test_task_cancel_terminalizes_pending_orbit_before_blocker_release(tmp_path):
    omd = Coordinator(str(tmp_path / "omd.db"), agent_ttl=None)
    holder = omd.claim("holder", ["src/**"])
    omd.declare("cancel-pending", writes=["src/a.py"])
    waiting = omd.claim(
        "waiter",
        ["src/a.py"],
        task_id="cancel-pending",
        request_id="cancel-pending-claim",
    )
    assert waiting["state"] == "PENDING"

    cancelled = omd.cancel("cancel-pending", reason="superseded")
    assert cancelled["ok"] is True
    row = omd.store.get_orbit(waiting["orbit_id"])
    assert row["state"] == "DENIED"
    assert row["decision_type"] == "CANCEL"
    assert row["terminal_reason"] == "task_cancelled"

    omd.release(holder["orbit_id"], "holder", holder["fence"])
    assert omd.store.get_orbit(waiting["orbit_id"])["state"] == "DENIED"
    omd.close()


def test_task_cancel_releases_held_orbit_and_promotes_waiter(tmp_path):
    omd = Coordinator(str(tmp_path / "omd.db"), agent_ttl=None)
    omd.declare("cancel-held", writes=["src/a.py"])
    held = omd.claim("owner", ["src/a.py"], task_id="cancel-held")
    waiting = omd.claim("next", ["src/a.py"])
    assert held["state"] == "HELD"
    assert waiting["state"] == "PENDING"

    cancelled = omd.cancel("cancel-held", reason="lease-only work complete")
    assert cancelled["ok"] is True
    held_row = omd.store.get_orbit(held["orbit_id"])
    waiting_row = omd.store.get_orbit(waiting["orbit_id"])
    assert held_row["state"] == "RELEASED"
    assert held_row["decision_type"] == "RELEASE"
    assert held_row["terminal_reason"] == "task_cancelled"
    assert waiting_row["state"] == "HELD"
    omd.close()


def test_cancelled_task_rejects_new_and_exact_claim_replay_before_fence_exposure(tmp_path):
    omd = Coordinator(str(tmp_path / "omd.db"), agent_ttl=None)
    omd.declare("cancelled", writes=["src/a.py"])
    held = omd.claim(
        "owner", ["src/a.py"], task_id="cancelled", request_id="claim-once"
    )
    assert held["state"] == "HELD"
    assert omd.store.get_idem("claim-once")["status"] == "DONE"

    omd.cancel("cancelled", reason="superseded")
    assert omd.store.get_task("cancelled")["state"] == "ABORTED"
    assert omd.store.get_idem("claim-once") is None
    fence_before = omd.store.current_fence()
    seq_before = omd.store.current_seq()
    orbit_count = len(omd.store.orbits_for_task("cancelled"))

    exact = omd.claim(
        "owner", ["src/a.py"], task_id="cancelled", request_id="claim-once"
    )
    fresh = omd.claim(
        "owner", ["src/a.py"], task_id="cancelled", request_id="claim-new"
    )
    for rejected in (exact, fresh):
        assert rejected["ok"] is False
        assert rejected["state"] == "REJECTED"
        assert rejected["reason"] == "task_not_admission_eligible"
        assert rejected["task_state"] == "ABORTED"
        assert rejected.get("fence") is None
    assert omd.store.current_fence() == fence_before
    assert omd.store.current_seq() == seq_before
    assert len(omd.store.orbits_for_task("cancelled")) == orbit_count
    assert omd.store.get_orbit(held["orbit_id"])["state"] == "RELEASED"
    omd.close()


def test_task_bound_claim_requires_existing_task_and_rejects_merged_task(tmp_path):
    omd = Coordinator(str(tmp_path / "omd.db"), agent_ttl=None)
    missing = omd.claim(
        "owner", ["missing/**"], task_id="missing", request_id="missing-claim"
    )
    assert missing == {
        "ok": False,
        "state": "REJECTED",
        "reason": "no such task",
        "task_id": "missing",
    }
    assert omd.store.get_idem("missing-claim") is None
    assert omd.store.orbits_for_task("missing") == []

    omd.declare("merged", writes=["src/m.py"])
    omd.next_task("owner")
    held = omd.claim(
        "owner", ["src/m.py"], task_id="merged", request_id="merged-claim"
    )
    omd.start("merged", "owner")
    omd.finish("merged")
    connected = omd.connect("merged")
    assert connected["ok"] is True
    assert omd.store.get_task("merged")["state"] == "MERGED"

    fence_before = omd.store.current_fence()
    seq_before = omd.store.current_seq()
    orbit_count = len(omd.store.orbits_for_task("merged"))
    exact = omd.claim(
        "owner", ["src/m.py"], task_id="merged", request_id="merged-claim"
    )
    fresh = omd.claim(
        "owner", ["src/m.py"], task_id="merged", request_id="merged-claim-new"
    )
    for rejected in (exact, fresh):
        assert rejected["ok"] is False
        assert rejected["reason"] == "task_not_admission_eligible"
        assert rejected["task_state"] == "MERGED"
        assert rejected.get("fence") is None
    assert omd.store.current_fence() == fence_before
    assert omd.store.current_seq() == seq_before
    assert len(omd.store.orbits_for_task("merged")) == orbit_count
    assert omd.store.get_orbit(held["orbit_id"])["state"] == "RELEASED"
    omd.close()


def test_restart_repairs_legacy_aborted_task_with_live_pending_orbit(tmp_path):
    db = tmp_path / "omd.db"
    first = Coordinator(str(db), agent_ttl=None)
    holder = first.claim("holder", ["src/**"])
    first.declare("legacy-cancelled", writes=["src/a.py"])
    waiting = first.claim("waiter", ["src/a.py"], task_id="legacy-cancelled")
    assert waiting["state"] == "PENDING"
    with first.store.tx():
        first.store.set_task("legacy-cancelled", state="ABORTED")
    first.resign()
    first.close()

    reopened = Coordinator(str(db), agent_ttl=None)
    repaired = reopened.store.get_orbit(waiting["orbit_id"])
    assert repaired["state"] == "DENIED"
    assert repaired["decision_type"] == "CANCEL"
    assert repaired["terminal_reason"] == "task_cancelled"
    reopened.release(holder["orbit_id"], "holder", holder["fence"])
    assert reopened.store.get_orbit(waiting["orbit_id"])["state"] == "DENIED"
    reopened.close()


def test_frozen_m1_gate_hash_is_unchanged():
    gate = ROOT / "gates" / "scheduler_fairness.yaml"
    assert hashlib.sha256(gate.read_bytes()).hexdigest() == FROZEN_GATE_SHA256


def test_m1_ooptdd_positive_negative_and_restored_use_same_frozen_gate():
    pytest.importorskip("ooptdd")
    gate = ROOT / "gates" / "scheduler_fairness.yaml"
    receipt = produce(gate, "omd-scheduler-m1-newer")
    assert receipt["gate"]["sha256"] == FROZEN_GATE_SHA256
    assert receipt["positive"]["gate_result"]["ok"] is True
    assert receipt["positive"]["observation"]["newer_claim_state"] == "PENDING"
    assert receipt["negative"]["gate_result"]["ok"] is False
    assert receipt["negative"]["observation"]["newer_claim_state"] == "HELD"
    assert receipt["restored_positive"]["gate_result"]["ok"] is True


def test_m1_ooptdd_receipt_binds_materialized_run_without_self_judgment(tmp_path):
    pytest.importorskip("ooptdd")
    run = produce(
        ROOT / "gates" / "scheduler_fairness.yaml", "omd-scheduler-m1-newer"
    )
    run_path = tmp_path / "ooptdd_run.json"
    run_path.write_text(json.dumps(run, sort_keys=True) + "\n", encoding="utf-8")
    receipt = build_receipt(run, run_path)
    assert receipt["positive"]["observed_verdict"] == "green"
    assert receipt["negative_oracle"]["observed_verdict"] == "red"
    assert receipt["negative_oracle"]["restored"] is True
    assert receipt["positive"]["receipt_sha256"] == hashlib.sha256(
        run_path.read_bytes()
    ).hexdigest()
    assert receipt["judgment"] == {
        "status": "AWAITING_INDEPENDENT_JUDGE",
        "supplied_by_producer": False,
    }
