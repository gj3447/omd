"""Standalone M1 admission-wait cancellation authority and projection."""

from __future__ import annotations

import time

import pytest

from omd_server import Coordinator
from omd_server.admission import ADMISSION_POLICY_VERSION
from omd_server.admission_contract import project_legacy, step


SNAPSHOT = "a" * 64


def _pending(omd, *, claim_request_id="claim-wait"):
    holder = omd.claim("holder", ["src/a.py"])
    waiting = omd.claim(
        "waiter",
        ["src/a.py"],
        request_id=claim_request_id,
    )
    assert waiting["state"] == "PENDING"
    assert waiting["bail_epoch"] == 0
    return holder, waiting


def test_semantic_cancel_reduces_to_cancelled_and_legacy_denied():
    context = {
        "state": "PENDING",
        "repository_id": "repo-test",
        "request_id": "claim-request",
        "orbit_id": "orb-test",
        "request_generation": 3,
        "owner_agent": "waiter",
        "bail_epoch": 7,
        "mode": "write",
        "pathspec_digest": "b" * 64,
        "policy_version": ADMISSION_POLICY_VERSION,
    }
    payload = {
        "repository_id": context["repository_id"],
        "request_id": context["request_id"],
        "orbit_id": context["orbit_id"],
        "request_generation": context["request_generation"],
        "actor": "lease-authority",
        "owner_agent": context["owner_agent"],
        "bail_epoch": context["bail_epoch"],
        "authority_snapshot_hash": SNAPSHOT,
        "event_id": "cancel-event",
    }
    reduced = step(
        context,
        "CANCEL",
        payload,
        trusted_authority_snapshot_hash=SNAPSHOT,
    )
    assert reduced.accepted and reduced.context["state"] == "CANCELLED"
    assert [effect["type"] for effect in reduced.effects] == ["RecordCancellation"]
    projection = project_legacy(reduced.context["state"], reduced.context)
    assert projection.state == "DENIED" and projection.terminal_reason == "cancelled"

    for field, value in (
        ("owner_agent", "intruder"),
        ("request_generation", 4),
        ("bail_epoch", 8),
    ):
        stale = dict(payload, **{field: value})
        rejected = step(
            context,
            "CANCEL",
            stale,
            trusted_authority_snapshot_hash=SNAPSHOT,
        )
        assert not rejected.accepted
        assert rejected.reason == f"identity_mismatch:{field}"


@pytest.mark.parametrize(
    ("overrides", "reason"),
    [
        ({"orbit_id": ""}, "invalid_orbit_id"),
        ({"agent_id": ""}, "invalid_agent_id"),
        ({"request_id": ""}, "invalid_request_id"),
        ({"request_generation": True}, "invalid_request_generation"),
        ({"request_generation": -1}, "invalid_request_generation"),
        ({"bail_epoch": True}, "invalid_bail_epoch"),
        ({"bail_epoch": -1}, "invalid_bail_epoch"),
    ],
)
def test_cancel_wait_rejects_invalid_inputs_before_mutation(tmp_path, overrides, reason):
    omd = Coordinator(str(tmp_path / "omd.db"), agent_ttl=None)
    _, waiting = _pending(omd)
    before = omd.store.get_orbit(waiting["orbit_id"])
    args = {
        "orbit_id": waiting["orbit_id"],
        "agent_id": "waiter",
        "request_generation": waiting["request_generation"],
        "bail_epoch": waiting["bail_epoch"],
        "request_id": "cancel-invalid",
    }
    args.update(overrides)
    orbit_id = args.pop("orbit_id")
    agent_id = args.pop("agent_id")
    generation = args.pop("request_generation")
    result = omd.cancel_wait(orbit_id, agent_id, generation, **args)
    assert result["ok"] is False and result["reason"] == reason
    assert omd.store.get_orbit(waiting["orbit_id"]) == before
    assert omd.store.get_idem("cancel-invalid") is None
    omd.close()


def test_cancel_wait_requires_exact_owner_generation_epoch_and_live_agent(tmp_path):
    omd = Coordinator(str(tmp_path / "omd.db"), agent_ttl=None)
    _, waiting = _pending(omd)
    common = {
        "orbit_id": waiting["orbit_id"],
        "agent_id": "waiter",
        "request_generation": waiting["request_generation"],
        "bail_epoch": waiting["bail_epoch"],
    }
    attempts = [
        (dict(common, agent_id="intruder"), "not owner"),
        (dict(common, request_generation=waiting["request_generation"] + 1),
         "stale request_generation"),
        (dict(common, bail_epoch=waiting["bail_epoch"] + 1), "stale bail_epoch"),
    ]
    for index, (args, reason) in enumerate(attempts):
        result = omd.cancel_wait(**args, request_id=f"cancel-auth-{index}")
        assert result["ok"] is False and result["reason"] == reason
        assert omd.store.get_orbit(waiting["orbit_id"])["state"] == "PENDING"

    unknown = omd.cancel_wait(
        "orb-missing", "waiter", 0, bail_epoch=0, request_id="cancel-missing-orbit"
    )
    assert unknown["ok"] is False and unknown["reason"] == "no such orbit"

    with omd.store.tx():
        token_id = omd.store.add_orbit(
            task_id=None,
            agent_id="waiter",
            pathspec=[],
            mode="write",
            state="PENDING",
            kind="merge_token",
            resource_key="integration",
            request_id="token-request",
            request_generation=0,
            bail_epoch=0,
        )
    wrong_kind = omd.cancel_wait(
        token_id, "waiter", 0, bail_epoch=0, request_id="cancel-token"
    )
    assert wrong_kind["ok"] is False
    assert wrong_kind["reason"] == "resource is not a cancellable orbit"

    with omd.store.tx():
        omd.store.db.execute("DELETE FROM agents WHERE agent_id='waiter'")
    missing = omd.cancel_wait(**common, request_id="cancel-missing-agent")
    assert missing["ok"] is False and missing["reason"] == "agent not registered"
    assert omd.store.get_orbit(waiting["orbit_id"])["state"] == "PENDING"
    omd.close()


def test_all_pending_claim_replay_paths_expose_cancellation_epoch(tmp_path):
    omd = Coordinator(str(tmp_path / "omd.db"), agent_ttl=None)
    _, waiting = _pending(omd, claim_request_id="pending-replay")
    exact = omd.claim("waiter", ["src/a.py"], request_id="pending-replay")
    natural = omd.claim("waiter", ["src/a.py"])
    assert exact["dedup"] is True and exact["bail_epoch"] == waiting["bail_epoch"]
    assert natural["dedup"] is True and natural["bail_epoch"] == waiting["bail_epoch"]
    omd.close()


def test_cancel_wait_separates_operation_id_and_replays_after_owner_reclaim(tmp_path):
    omd = Coordinator(str(tmp_path / "omd.db"), agent_ttl=None)
    _, waiting = _pending(omd, claim_request_id="claim-operation")
    args = (
        waiting["orbit_id"],
        "waiter",
        waiting["request_generation"],
    )
    kwargs = {"bail_epoch": waiting["bail_epoch"]}

    reused_claim_id = omd.cancel_wait(
        *args, **kwargs, request_id="claim-operation"
    )
    assert reused_claim_id["ok"] is False
    assert reused_claim_id["reason"] == "idempotency_conflict"
    assert omd.store.get_orbit(waiting["orbit_id"])["state"] == "PENDING"

    cancelled = omd.cancel_wait(*args, **kwargs, request_id="cancel-operation")
    assert cancelled == {
        "ok": True,
        "orbit_id": waiting["orbit_id"],
        "request_id": "claim-operation",
        "cancel_request_id": "cancel-operation",
        "request_generation": waiting["request_generation"],
        "bail_epoch": waiting["bail_epoch"],
        "state": "CANCELLED",
        "legacy_state": "DENIED",
        "terminal_reason": "cancelled",
    }
    row = omd.store.get_orbit(waiting["orbit_id"])
    assert (row["state"], row["decision_type"], row["terminal_reason"]) == (
        "DENIED", "CANCEL", "cancelled"
    )

    with omd.store.tx():
        omd.store.set_agent_state("waiter", "RETIRED")
        omd.store.bump_bail_epoch("waiter")
    replay = omd.cancel_wait(*args, **kwargs, request_id="cancel-operation")
    assert replay["state"] == "CANCELLED" and replay["replayed"] is True
    fresh = omd.cancel_wait(*args, **kwargs, request_id="cancel-after-reclaim")
    assert fresh["ok"] is False and fresh["fenced_out"] is True
    omd.close()


def test_only_pending_or_canonical_cancel_projection_is_accepted(tmp_path):
    omd = Coordinator(str(tmp_path / "omd.db"), agent_ttl=None)
    held = omd.claim("held-owner", ["held/**"], request_id="held-request")
    not_pending = omd.cancel_wait(
        held["orbit_id"],
        "held-owner",
        held["request_generation"],
        bail_epoch=held["bail_epoch"],
        request_id="cancel-held",
    )
    assert not_pending["ok"] is False and not_pending["reason"] == "not PENDING: HELD"

    _, waiting = _pending(omd, claim_request_id="timeout-request")
    with omd.store.tx():
        omd.store.set_orbit(waiting["orbit_id"], wait_deadline=time.time() - 1)
    omd.sweep()
    timed_out = omd.cancel_wait(
        waiting["orbit_id"],
        "waiter",
        waiting["request_generation"],
        bail_epoch=waiting["bail_epoch"],
        request_id="cancel-timeout",
    )
    assert timed_out["ok"] is False and timed_out["reason"] == "not PENDING: DENIED"

    with omd.store.tx():
        omd.store.set_orbit(
            waiting["orbit_id"],
            decision_type="CANCEL",
            terminal_reason="cancelled",
        )
    noop = omd.cancel_wait(
        waiting["orbit_id"],
        "waiter",
        waiting["request_generation"],
        bail_epoch=waiting["bail_epoch"],
        request_id="cancel-noop",
    )
    assert noop["ok"] is True and noop["noop"] is True
    assert noop["state"] == "CANCELLED"
    omd.close()


def test_cancel_wait_promotes_newly_unblocked_successor_exactly_once(tmp_path):
    omd = Coordinator(str(tmp_path / "omd.db"), agent_ttl=None)
    holder = omd.claim("holder", ["a/**"])
    target = omd.claim(
        "target",
        ["a/**", "b/**"],
        priority=10,
        request_id="target-claim",
    )
    successor = omd.claim("successor", ["b/**"], request_id="successor-claim")
    assert target["state"] == successor["state"] == "PENDING"
    assert successor["pending_predecessors"] == [target["orbit_id"]]
    fence_before = omd.store.current_fence()

    cancelled = omd.cancel_wait(
        target["orbit_id"],
        "target",
        target["request_generation"],
        bail_epoch=target["bail_epoch"],
        request_id="target-cancel",
    )
    assert cancelled["state"] == "CANCELLED"
    promoted = omd.store.get_orbit(successor["orbit_id"])
    assert promoted["state"] == "HELD"
    assert omd.store.current_fence() == fence_before + 1

    replay = omd.cancel_wait(
        target["orbit_id"],
        "target",
        target["request_generation"],
        bail_epoch=target["bail_epoch"],
        request_id="target-cancel",
    )
    assert replay["replayed"] is True
    assert omd.store.current_fence() == fence_before + 1
    assert omd.store.get_orbit(holder["orbit_id"])["state"] == "HELD"
    omd.close()


def test_authenticated_cancel_refreshes_liveness_before_reconcile(tmp_path):
    omd = Coordinator(str(tmp_path / "omd.db"), agent_ttl=10.0)
    omd.claim("holder", ["a/**"])
    waiting = omd.claim("waiter", ["a/**"], request_id="wait-claim")
    unrelated = omd.claim("waiter", ["b/**"], request_id="unrelated-claim")
    assert waiting["state"] == "PENDING" and unrelated["state"] == "HELD"
    with omd.store.tx():
        omd.store.db.execute(
            "UPDATE agents SET last_heartbeat=? WHERE agent_id='waiter'",
            (time.time() - 100.0,),
        )

    result = omd.cancel_wait(
        waiting["orbit_id"],
        "waiter",
        waiting["request_generation"],
        bail_epoch=waiting["bail_epoch"],
        request_id="wait-cancel",
    )
    assert result["state"] == "CANCELLED"
    assert omd.store.get_agent("waiter")["state"] == "WORKING"
    assert omd.store.get_orbit(unrelated["orbit_id"])["state"] == "HELD"
    omd.close()
