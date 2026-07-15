"""Bounded admission queue and typed overload rejection."""

from __future__ import annotations

import threading

import pytest

from omd_server import Coordinator
from omd_server.admission_contract import project_legacy, step


def _assert_overload_projection(result):
    context = {
        "state": "REQUESTED",
        "repository_id": result["repository_id"],
        "request_id": result["request_id"],
        "orbit_id": result["orbit_id"],
        "request_generation": result["request_generation"],
        "owner_agent": result["owner_agent"],
        "bail_epoch": result["bail_epoch"],
        "mode": result["mode"],
        "pathspec_digest": result["pathspec_digest"],
        "policy_version": result["policy_version"],
    }
    payload = {
        **{key: value for key, value in context.items() if key != "state"},
        "actor": result["actor"],
        "event_id": result["event_id"],
        "authority_snapshot_hash": result["authority_snapshot_hash"],
        "decision_id": result["decision_id"],
        "reason": result["reason"],
        "queue_depth": result["queue_depth"],
        "queue_capacity": result["queue_capacity"],
        "retry_after_at": result["retry_after_at"],
    }
    reduced = step(
        context,
        "ADMISSION_REJECTED",
        payload,
        trusted_authority_snapshot_hash=result["authority_snapshot_hash"],
    )
    assert reduced.accepted and reduced.context["state"] == "REJECTED"
    projection = project_legacy(reduced.context["state"], reduced.context)
    assert projection.state is None and projection.terminal_reason == "queue_full"
    assert reduced.effects == (
        {
            "type": "RecordOverload",
            "repository_id": result["repository_id"],
            "request_id": result["request_id"],
            "orbit_id": result["orbit_id"],
            "request_generation": result["request_generation"],
            "event_id": result["event_id"],
            "authority_snapshot_hash": result["authority_snapshot_hash"],
            "decision_id": result["decision_id"],
            "reason": "queue_full",
            "queue_depth": result["queue_depth"],
            "queue_capacity": result["queue_capacity"],
            "retry_after_at": result["retry_after_at"],
        },
    )
    tampered = dict(payload, queue_depth=payload["queue_depth"] + 1)
    rejected = step(
        context,
        "ADMISSION_REJECTED",
        tampered,
        trusted_authority_snapshot_hash=result["authority_snapshot_hash"],
    )
    assert not rejected.accepted and rejected.reason == "decision_id_mismatch"


@pytest.mark.parametrize("capacity", [-1, True, 1.5, "2", None])
def test_invalid_queue_capacity_fails_before_db_creation(tmp_path, capacity):
    db = tmp_path / "invalid-capacity.db"
    with pytest.raises(ValueError, match="admission_queue_capacity"):
        Coordinator(str(db), admission_queue_capacity=capacity)
    assert not db.exists()


def test_capacity_rejects_without_row_fence_or_ticket_and_replays_terminal_receipt(
    tmp_path,
):
    omd = Coordinator(
        str(tmp_path / "omd.db"), agent_ttl=None, admission_queue_capacity=1
    )
    holder = omd.claim("holder", ["src/**"], request_id="holder")
    waiting = omd.claim("waiting", ["src/a.py"], request_id="waiting")
    assert waiting["state"] == "PENDING"

    seq_before = omd.store.current_seq()
    fence_before = omd.store.current_fence()
    rows_before = len(omd.store.snapshot()["orbits"])
    rejected = omd.claim("overflow", ["src/b.py"], request_id="overflow")

    assert rejected["ok"] is False and rejected["state"] == "REJECTED"
    assert rejected["reason"] == "queue_full"
    assert rejected["code"] == "QUEUE_FULL"
    assert rejected["queue_depth"] == rejected["queue_capacity"] == 1
    assert rejected["retry_after_at"] >= waiting["wait_deadline"]
    assert rejected["retry"] is True
    assert rejected["retry_requires_new_request_id"] is True
    assert len(rejected["decision_id"]) == 64
    assert omd.store.current_seq() == seq_before
    assert omd.store.current_fence() == fence_before
    assert len(omd.store.snapshot()["orbits"]) == rows_before
    assert omd.store.get_orbit(rejected["orbit_id"]) is None
    assert omd.store.get_idem("overflow")["status"] == "DONE"
    _assert_overload_projection(rejected)

    conflict = omd.claim("overflow", ["other/**"], request_id="overflow")
    assert conflict["ok"] is False
    assert conflict["reason"] == "idempotency_conflict"

    omd.release(holder["orbit_id"], "holder", holder["fence"])
    replay = omd.claim("overflow", ["src/b.py"], request_id="overflow")
    assert replay["state"] == "REJECTED" and replay["replayed"] is True
    assert replay["decision_id"] == rejected["decision_id"]

    fresh = omd.claim("overflow", ["src/b.py"], request_id="overflow-retry")
    assert fresh["state"] == "HELD"
    omd.close()


def test_full_queue_does_not_block_disjoint_or_compatible_grants(tmp_path):
    omd = Coordinator(
        str(tmp_path / "omd.db"), agent_ttl=None, admission_queue_capacity=1
    )
    omd.claim("holder", ["hot/**"], mode="write")
    omd.claim("waiting", ["hot/a.py"], mode="write")
    assert len(omd.store.pending_orbits()) == 1

    disjoint = omd.claim("disjoint", ["cold/**"], mode="write")
    compatible = omd.claim("shared", ["shared/**"], mode="shared")
    compatible_two = omd.claim("shared-2", ["shared/a.py"], mode="shared")
    assert {disjoint["state"], compatible["state"], compatible_two["state"]} == {
        "HELD"
    }
    assert len(omd.store.pending_orbits()) == 1
    omd.close()


def test_zero_capacity_is_no_wait_not_no_work(tmp_path):
    omd = Coordinator(
        str(tmp_path / "omd.db"), agent_ttl=None, admission_queue_capacity=0
    )
    omd.claim("holder", ["hot/**"])
    rejected = omd.claim("blocked", ["hot/a.py"], request_id="no-wait")
    granted = omd.claim("disjoint", ["cold/**"])
    assert rejected["state"] == "REJECTED" and rejected["queue_capacity"] == 0
    assert granted["state"] == "HELD"
    assert omd.store.pending_orbits() == []
    omd.close()


def test_two_coordinators_cannot_race_past_the_last_capacity_slot(tmp_path):
    db = str(tmp_path / "omd.db")
    first = Coordinator(
        db,
        agent_ttl=None,
        admission_queue_capacity=1,
        enforce_single_coordinator=False,
    )
    second = Coordinator(
        db,
        agent_ttl=None,
        admission_queue_capacity=1,
        enforce_single_coordinator=False,
    )
    first.claim("holder", ["src/**"])
    barrier = threading.Barrier(3)
    outcomes = []

    def submit(index):
        barrier.wait()
        outcomes.append(
            (first if index == 0 else second).claim(
                f"waiter-{index}",
                [f"src/{index}.py"],
                request_id=f"waiter-{index}",
            )
        )

    threads = [threading.Thread(target=submit, args=(index,)) for index in range(2)]
    for thread in threads:
        thread.start()
    barrier.wait()
    for thread in threads:
        thread.join(timeout=5)
        assert not thread.is_alive()

    assert sorted(result["state"] for result in outcomes) == ["PENDING", "REJECTED"]
    assert len(first.store.pending_orbits()) == 1
    first.close()
    second.close()


def test_restart_counts_existing_pending_rows_against_capacity(tmp_path):
    db = str(tmp_path / "omd.db")
    first = Coordinator(db, agent_ttl=None, admission_queue_capacity=1)
    first.claim("holder", ["src/**"])
    first.claim("waiting", ["src/a.py"])
    first.resign()
    first.close()

    reopened = Coordinator(db, agent_ttl=None, admission_queue_capacity=1)
    rejected = reopened.claim("overflow", ["src/b.py"], request_id="after-restart")
    assert rejected["state"] == "REJECTED"
    assert rejected["queue_depth"] == 1
    reopened.close()


def test_repository_capacity_policy_cannot_drift_between_coordinators(tmp_path):
    db = str(tmp_path / "omd.db")
    first = Coordinator(db, agent_ttl=None, admission_queue_capacity=1)
    first.resign()
    first.close()

    with pytest.raises(ValueError, match="durable repository policy"):
        Coordinator(db, agent_ttl=None, admission_queue_capacity=2)

    reopened = Coordinator(db, agent_ttl=None, admission_queue_capacity=1)
    assert reopened.store.get_meta("admission_queue_capacity") == "1"
    assert reopened.status()["admission_queue"]["capacity"] == 1
    reopened.close()


def test_due_timeout_is_reconciled_before_capacity_check(tmp_path):
    omd = Coordinator(
        str(tmp_path / "omd.db"),
        agent_ttl=None,
        admission_queue_capacity=1,
        admission_wait_timeout=0.01,
    )
    omd.claim("holder", ["src/**"])
    stale = omd.claim("stale", ["src/a.py"])
    with omd.store.tx():
        omd.store.set_orbit(stale["orbit_id"], wait_deadline=1.0)

    replacement = omd.claim("replacement", ["src/b.py"], request_id="replacement")
    assert omd.store.get_orbit(stale["orbit_id"])["decision_type"] == "WAIT_TIMEOUT"
    assert replacement["state"] == "PENDING"
    assert len(omd.store.pending_orbits()) == 1
    omd.close()


def test_begin_propagates_overload_without_partial_orbit_acquisition(tmp_path):
    omd = Coordinator(
        str(tmp_path / "omd.db"), agent_ttl=None, admission_queue_capacity=0
    )
    omd.claim("holder", ["src/**"])
    result = omd.begin(
        "task",
        "worker",
        ["src/a.py"],
        request_id="begin-overload",
    )
    assert result["ok"] is False and result["stage"] == "claim"
    assert result["state"] == "REJECTED"
    assert omd.store.orbits_for_task("task") == []
    omd.close()


def test_task_terminal_state_does_not_hide_exact_overload_replay(tmp_path):
    omd = Coordinator(
        str(tmp_path / "omd.db"), agent_ttl=None, admission_queue_capacity=0
    )
    omd.claim("holder", ["src/**"])
    omd.declare("queued-task", writes=["src/a.py"])
    rejected = omd.claim(
        "worker",
        ["src/a.py"],
        task_id="queued-task",
        request_id="task-overload",
    )
    assert rejected["state"] == "REJECTED" and rejected["code"] == "QUEUE_FULL"

    omd.cancel("queued-task", reason="superseded")
    replay = omd.claim(
        "worker",
        ["src/a.py"],
        task_id="queued-task",
        request_id="task-overload",
    )
    assert replay["state"] == "REJECTED" and replay["code"] == "QUEUE_FULL"
    assert replay["decision_id"] == rejected["decision_id"]
    assert replay["replayed"] is True

    fresh = omd.claim(
        "worker",
        ["src/a.py"],
        task_id="queued-task",
        request_id="task-overload-fresh",
    )
    assert fresh["reason"] == "task_not_admission_eligible"
    assert fresh["task_state"] == "ABORTED"
    omd.close()
