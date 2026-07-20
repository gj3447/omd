"""Explicit non-policy request-generation rollover authority."""

from __future__ import annotations

import json
import threading

import pytest

from omd_server import Coordinator
from omd_server.core import MAX_REQUEST_GENERATION


def _coordinator(tmp_path, **kwargs):
    return Coordinator(
        str(tmp_path / "omd.db"),
        agent_ttl=None,
        sweep_interval=None,
        **kwargs,
    )


def _rows(omd, request_id):
    return omd.store.db.execute(
        "SELECT orbit_id,request_generation,state,decision_type FROM orbits "
        "WHERE kind='orbit' AND request_id=? ORDER BY request_generation",
        (request_id,),
    ).fetchall()


def _release(omd, request_id="rollover-source"):
    first = omd.claim(
        "worker", ["src/**"], request_id=request_id, bail_epoch=0
    )
    assert first["state"] == "HELD"
    assert omd.release(
        first["orbit_id"], "worker", first["fence"], bail_epoch=0
    )["ok"]
    return first


def test_released_claim_rolls_to_exactly_one_fresh_generation(tmp_path):
    omd = _coordinator(tmp_path)
    first = _release(omd)

    rolled = omd.rollover_claim(
        first["orbit_id"],
        "worker",
        0,
        bail_epoch=0,
        request_id="rollover-operation-1",
    )
    replay = omd.rollover_claim(
        first["orbit_id"],
        "worker",
        0,
        bail_epoch=0,
        request_id="rollover-operation-1",
    )

    assert rolled["ok"] is True and rolled["rollover"] is True
    assert rolled["request_id"] == "rollover-source"
    assert rolled["request_generation"] == 1
    assert rolled["orbit_id"] != first["orbit_id"]
    assert replay["replayed"] is True
    assert replay["orbit_id"] == rolled["orbit_id"]
    assert [(row["request_generation"], row["state"]) for row in _rows(
        omd, "rollover-source"
    )] == [(0, "RELEASED"), (1, "HELD")]
    outbox_generations = [
        row["request_generation"]
        for row in omd.store.db.execute(
            "SELECT request_generation FROM admission_outbox "
            "WHERE request_id=? ORDER BY created_at,event_id",
            ("rollover-source",),
        ).fetchall()
    ]
    assert 0 in outbox_generations and 1 in outbox_generations

    # The semantic request cache now points at generation one, never the old
    # generation-zero HELD fence.
    semantic_replay = omd.claim(
        "worker", ["src/**"], request_id="rollover-source", bail_epoch=0
    )
    assert semantic_replay["orbit_id"] == rolled["orbit_id"]
    assert semantic_replay["request_generation"] == 1
    omd.close()


def test_rollover_operation_replay_is_exact_envelope_only(tmp_path):
    omd = _coordinator(tmp_path)
    first = _release(omd)
    rolled = omd.rollover_claim(
        first["orbit_id"], "worker", 0,
        bail_epoch=0, request_id="rollover-exact",
    )
    conflict = omd.rollover_claim(
        first["orbit_id"], "worker", 1,
        bail_epoch=0, request_id="rollover-exact",
    )

    assert rolled["request_generation"] == 1
    assert conflict["ok"] is False
    assert conflict["reason"] == "idempotency_conflict"
    assert len(_rows(omd, "rollover-source")) == 2
    omd.close()


def test_due_lease_is_expired_before_rollover_authorization(tmp_path):
    omd = _coordinator(tmp_path)
    first = omd.claim(
        "worker", ["src/**"], ttl=60, request_id="expiry-source",
        bail_epoch=0,
    )
    with omd.store.tx():
        omd.store.set_orbit(first["orbit_id"], expires_at=0.0)

    rolled = omd.rollover_claim(
        first["orbit_id"], "worker", 0,
        bail_epoch=0, request_id="rollover-expiry",
    )
    assert omd.store.get_orbit(first["orbit_id"])["state"] == "EXPIRED"
    assert rolled["ok"] is True and rolled["request_generation"] == 1
    omd.close()


@pytest.mark.parametrize("terminal", ["cancel", "timeout"])
def test_cancelled_and_timed_out_waits_can_rollover(tmp_path, terminal):
    omd = _coordinator(tmp_path)
    holder = omd.claim("holder", ["src/**"], bail_epoch=0)
    waiting = omd.claim(
        "worker", ["src/**"], request_id=f"{terminal}-source", bail_epoch=0
    )
    assert waiting["state"] == "PENDING"
    if terminal == "cancel":
        cancelled = omd.cancel_wait(
            waiting["orbit_id"], "worker", 0,
            bail_epoch=0, request_id="cancel-operation",
        )
        assert cancelled["state"] == "CANCELLED"
    else:
        with omd.store.tx():
            omd.store.set_orbit(waiting["orbit_id"], wait_deadline=0.0)
        omd.sweep()
        assert omd.store.get_orbit(waiting["orbit_id"])["decision_type"] == "WAIT_TIMEOUT"
    omd.release(holder["orbit_id"], "holder", holder["fence"], bail_epoch=0)

    rolled = omd.rollover_claim(
        waiting["orbit_id"], "worker", 0,
        bail_epoch=0, request_id=f"rollover-{terminal}",
    )
    assert rolled["ok"] is True
    assert rolled["request_generation"] == 1
    assert rolled["state"] == "HELD"
    omd.close()


def test_live_and_policy_denied_predecessors_are_not_rollover_sources(tmp_path):
    omd = _coordinator(tmp_path)
    live = omd.claim(
        "worker", ["live/**"], request_id="live-source", bail_epoch=0
    )
    rejected_live = omd.rollover_claim(
        live["orbit_id"], "worker", 0,
        bail_epoch=0, request_id="rollover-live",
    )
    assert rejected_live["reason"] == "rollover_predecessor_not_supported"

    a = omd.claim("agent-a", ["a/**"], bail_epoch=0)
    b = omd.claim("agent-b", ["b/**"], bail_epoch=0)
    omd.claim("agent-a", ["b/**"], bail_epoch=0)
    denied = omd.claim(
        "agent-b", ["a/**"], request_id="policy-source", bail_epoch=0
    )
    assert denied["state"] == "DENIED"
    rejected_policy = omd.rollover_claim(
        denied["orbit_id"], "agent-b", 0,
        bail_epoch=0, request_id="rollover-policy",
    )
    assert rejected_policy["reason"] == "rollover_predecessor_not_supported"
    assert len(_rows(omd, "policy-source")) == 1
    omd.release(a["orbit_id"], "agent-a", a["fence"], bail_epoch=0)
    omd.release(b["orbit_id"], "agent-b", b["fence"], bail_epoch=0)
    omd.close()


def test_queue_full_remains_fresh_request_id_only(tmp_path):
    omd = _coordinator(tmp_path, admission_queue_capacity=1)
    omd.claim("holder", ["src/**"], bail_epoch=0)
    omd.claim("first-waiter", ["src/**"], bail_epoch=0)
    rejected = omd.claim(
        "worker", ["src/**"], request_id="queue-full-source", bail_epoch=0
    )
    assert rejected["state"] == "REJECTED"
    assert rejected["retry_requires_new_request_id"] is True
    assert omd.store.latest_orbit_by_request("queue-full-source") is None

    rollover = omd.rollover_claim(
        rejected["orbit_id"], "worker", 0,
        bail_epoch=0, request_id="rollover-queue-full",
    )
    assert rollover["ok"] is False
    assert rollover["reason"] == "no such orbit"
    omd.close()


def test_queue_full_during_rollover_creates_no_generation_and_is_retryable(tmp_path):
    omd = _coordinator(tmp_path, admission_queue_capacity=1)
    first = _release(omd)
    holder = omd.claim("holder", ["src/**"], bail_epoch=0)
    waiting = omd.claim("queued", ["src/**"], request_id="queued", bail_epoch=0)
    assert waiting["state"] == "PENDING"

    rejected = omd.rollover_claim(
        first["orbit_id"], "worker", 0,
        bail_epoch=0, request_id="rollover-capacity",
    )
    assert rejected["ok"] is False
    assert rejected["reason"] == "rollover_admission_rejected"
    assert rejected["admission"]["code"] == "QUEUE_FULL"
    assert len(_rows(omd, "rollover-source")) == 1
    assert omd.store.get_idem("rollover-capacity") is None
    assert omd.store.get_idem("rollover-source") is None

    omd.cancel_wait(
        waiting["orbit_id"], "queued", 0,
        bail_epoch=0, request_id="cancel-queued",
    )
    retried = omd.rollover_claim(
        first["orbit_id"], "worker", 0,
        bail_epoch=0, request_id="rollover-capacity",
    )
    assert retried["ok"] is True
    assert retried["state"] == "PENDING"
    assert retried["request_generation"] == 1
    assert len(_rows(omd, "rollover-source")) == 2
    omd.release(holder["orbit_id"], "holder", holder["fence"], bail_epoch=0)
    omd.close()


def test_reclaimed_owner_cannot_use_rollover_to_resurrect(tmp_path):
    omd = _coordinator(tmp_path)
    first = omd.claim(
        "worker", ["src/**"], request_id="reclaimed-source", bail_epoch=0
    )
    omd.bail("worker", request_id="bail-worker")
    predecessor = omd.store.get_orbit(first["orbit_id"])
    assert predecessor["state"] == "EXPIRED"
    assert predecessor["decision_type"] == "LEASE_OWNER_RECLAIMED"

    rejected = omd.rollover_claim(
        first["orbit_id"], "worker", 0,
        bail_epoch=1, request_id="rollover-reclaimed",
    )
    assert rejected["ok"] is False
    assert rejected["reason"] == "rollover_predecessor_not_supported"
    assert len(_rows(omd, "reclaimed-source")) == 1
    omd.close()


def test_generation_above_signed_64_domain_is_invalid_before_lookup(tmp_path):
    omd = _coordinator(tmp_path)
    result = omd.rollover_claim(
        "orb-missing", "worker", MAX_REQUEST_GENERATION + 1,
        bail_epoch=0, request_id="rollover-too-large",
    )
    assert result["ok"] is False
    assert result["reason"] == "invalid_request_generation"
    assert omd.store.get_idem("rollover-too-large") is None
    omd.close()


def test_policy_denial_generation_exhaustion_is_typed(tmp_path):
    omd = _coordinator(tmp_path)
    a = omd.claim("agent-a", ["a/**"], bail_epoch=0)
    b = omd.claim("agent-b", ["b/**"], bail_epoch=0)
    omd.claim("agent-a", ["b/**"], bail_epoch=0)
    denied = omd.claim(
        "agent-b", ["a/**"], request_id="policy-max", bail_epoch=0
    )
    assert denied["state"] == "DENIED"
    with omd.store.tx():
        omd.store.db.execute(
            "UPDATE orbits SET request_generation=? WHERE orbit_id=?",
            (MAX_REQUEST_GENERATION, denied["orbit_id"]),
        )
    seq_before = omd.store.current_seq()
    fence_before = omd.store.current_fence()

    result = omd.claim(
        "agent-b", ["a/**"], request_id="policy-max", bail_epoch=0
    )
    assert result["ok"] is False
    assert result["reason"] == "request_generation_exhausted"
    assert result["request_generation"] == MAX_REQUEST_GENERATION
    assert len(_rows(omd, "policy-max")) == 1
    assert omd.store.current_seq() == seq_before
    assert omd.store.current_fence() == fence_before
    omd.release(a["orbit_id"], "agent-a", a["fence"], bail_epoch=0)
    omd.release(b["orbit_id"], "agent-b", b["fence"], bail_epoch=0)
    omd.close()


def test_rollover_after_operation_cache_gc_remains_at_most_once(tmp_path):
    omd = _coordinator(tmp_path)
    first = _release(omd)
    rolled = omd.rollover_claim(
        first["orbit_id"], "worker", 0,
        bail_epoch=0, request_id="rollover-gc",
    )
    with omd.store.tx():
        omd.store.clear_idem("rollover-gc")

    late_retry = omd.rollover_claim(
        first["orbit_id"], "worker", 0,
        bail_epoch=0, request_id="rollover-gc",
    )
    assert rolled["request_generation"] == 1
    assert late_retry["ok"] is False
    assert late_retry["reason"] == "stale rollover predecessor"
    assert len(_rows(omd, "rollover-source")) == 2
    omd.close()


def test_terminal_task_cannot_be_resurrected_by_rollover(tmp_path):
    omd = _coordinator(tmp_path)
    assert omd.declare("T", writes=["src/**"])["ok"]
    first = omd.claim(
        "worker", ["src/**"], task_id="T",
        request_id="task-source", bail_epoch=0,
    )
    omd.release(first["orbit_id"], "worker", first["fence"], bail_epoch=0)
    assert omd.cancel("T", request_id="cancel-task")["state"] == "ABORTED"

    rejected = omd.rollover_claim(
        first["orbit_id"], "worker", 0,
        bail_epoch=0, request_id="rollover-task",
    )
    assert rejected["ok"] is False
    assert rejected["reason"] == "task_not_admission_eligible"
    assert len(_rows(omd, "task-source")) == 1
    omd.close()


def test_rollover_operation_id_must_differ_from_semantic_claim_id(tmp_path):
    omd = _coordinator(tmp_path)
    first = _release(omd)
    rejected = omd.rollover_claim(
        first["orbit_id"], "worker", 0,
        bail_epoch=0, request_id="rollover-source",
    )
    assert rejected["ok"] is False
    assert rejected["reason"] == "rollover_request_id_must_be_distinct"
    assert len(_rows(omd, "rollover-source")) == 1
    omd.close()


@pytest.mark.parametrize(
    ("overrides", "reason"),
    [
        ({"agent_id": "intruder"}, "not owner"),
        ({"expected_generation": 1}, "stale request_generation"),
        ({"bail_epoch": 1}, "stale bail_epoch"),
    ],
)
def test_stale_rollover_authority_is_mutation_free(tmp_path, overrides, reason):
    omd = _coordinator(tmp_path)
    first = _release(omd)
    before = {
        "seq": omd.store.current_seq(),
        "fence": omd.store.current_fence(),
        "rows": len(_rows(omd, "rollover-source")),
        "outbox": omd.store.db.execute(
            "SELECT COUNT(*) FROM admission_outbox"
        ).fetchone()[0],
    }
    args = {
        "prior_orbit_id": first["orbit_id"],
        "agent_id": "worker",
        "expected_generation": 0,
        "bail_epoch": 0,
        "request_id": f"rollover-stale-{reason}",
    }
    args.update(overrides)
    result = omd.rollover_claim(
        args.pop("prior_orbit_id"),
        args.pop("agent_id"),
        args.pop("expected_generation"),
        **args,
    )
    after = {
        "seq": omd.store.current_seq(),
        "fence": omd.store.current_fence(),
        "rows": len(_rows(omd, "rollover-source")),
        "outbox": omd.store.db.execute(
            "SELECT COUNT(*) FROM admission_outbox"
        ).fetchone()[0],
    }
    assert result["ok"] is False and result["reason"] == reason
    assert after == before
    omd.close()


def test_concurrent_rollovers_create_only_generation_one(tmp_path):
    omd = _coordinator(tmp_path)
    first = _release(omd)
    barrier = threading.Barrier(3)
    results = []

    def run(index):
        barrier.wait()
        results.append(omd.rollover_claim(
            first["orbit_id"], "worker", 0,
            bail_epoch=0, request_id=f"rollover-race-{index}",
        ))

    threads = [threading.Thread(target=run, args=(index,)) for index in range(2)]
    for thread in threads:
        thread.start()
    barrier.wait()
    for thread in threads:
        thread.join(timeout=5)

    assert sum(result.get("ok") is True for result in results) == 1
    assert sorted(row["request_generation"] for row in _rows(
        omd, "rollover-source"
    )) == [0, 1]
    omd.close()


def test_generation_exhaustion_is_typed_and_mutation_free(tmp_path):
    omd = _coordinator(tmp_path)
    first = _release(omd)
    with omd.store.tx():
        omd.store.db.execute(
            "UPDATE orbits SET request_generation=? WHERE orbit_id=?",
            (MAX_REQUEST_GENERATION, first["orbit_id"]),
        )
    before_fence = omd.store.current_fence()
    result = omd.rollover_claim(
        first["orbit_id"], "worker", MAX_REQUEST_GENERATION,
        bail_epoch=0, request_id="rollover-exhausted",
    )
    assert result["ok"] is False
    assert result["reason"] == "request_generation_exhausted"
    assert omd.store.current_fence() == before_fence
    assert len(_rows(omd, "rollover-source")) == 1
    omd.close()


def test_rollover_operation_replays_after_restart(tmp_path):
    omd = _coordinator(tmp_path)
    first = _release(omd)
    rolled = omd.rollover_claim(
        first["orbit_id"], "worker", 0,
        bail_epoch=0, request_id="rollover-restart",
    )
    omd.close()
    omd.resign()

    reopened = _coordinator(tmp_path)
    replay = reopened.rollover_claim(
        first["orbit_id"], "worker", 0,
        bail_epoch=0, request_id="rollover-restart",
    )
    assert replay["replayed"] is True
    assert replay["orbit_id"] == rolled["orbit_id"]
    assert replay["request_generation"] == 1
    reopened.close()


def test_rollover_copies_the_complete_immutable_claim_intent(tmp_path):
    omd = _coordinator(tmp_path)
    first = omd.claim(
        "worker",
        ["z/**", "a/**"],
        "shared",
        ttl=321,
        reason="same intent",
        priority=7,
        request_id="intent-source",
        bail_epoch=0,
    )
    omd.release(first["orbit_id"], "worker", first["fence"], bail_epoch=0)
    rolled = omd.rollover_claim(
        first["orbit_id"], "worker", 0,
        bail_epoch=0, request_id="rollover-intent",
    )
    row = omd.store.get_orbit(rolled["orbit_id"])
    assert json.loads(row["pathspec"]) == ["a/**", "z/**"]
    assert row["mode"] == "shared"
    assert row["requested_ttl"] == 321
    assert row["reason"] == "same intent"
    assert row["priority"] == 7
    omd.close()
