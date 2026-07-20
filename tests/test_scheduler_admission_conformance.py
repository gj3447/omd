"""Payload-driven semantic FSM and production projection conformance."""

from __future__ import annotations

import copy
import json
from pathlib import Path

import pytest

from omd_server import Coordinator
from omd_server.admission import ADMISSION_POLICY_VERSION, pathspec_digest, sha256_json
from omd_server.admission_contract import (
    DEFAULT_SPEC,
    IDENTITY_FIELDS,
    bind_decision_id,
    project_legacy,
    step,
)


SNAPSHOT = "a" * 64


def _context(state="REQUESTED"):
    return {
        "state": state,
        "repository_id": "repo-test",
        "request_id": "request-test",
        "orbit_id": "orbit-test",
        "request_generation": 0,
        "owner_agent": "owner-test",
        "bail_epoch": 7,
        "mode": "write",
        "pathspec_digest": "b" * 64,
        "policy_version": ADMISSION_POLICY_VERSION,
        "base_priority": 0,
        "effective_priority": 0,
    }


def _decision(context, event_type, **variant):
    payload = {
        **{field: context[field] for field in IDENTITY_FIELDS},
        "actor": "lease-authority",
        "event_id": f"event-{event_type.lower()}",
        "authority_snapshot_hash": SNAPSHOT,
        "base_priority": context.get("base_priority", 0),
        "effective_priority": context.get("effective_priority", 0),
        "observed_at": context.get("observed_at", 100.0),
        **variant,
    }
    return bind_decision_id(event_type, payload)


def test_grant_uses_json_transition_context_updates_and_effect_bindings():
    context = _context()
    payload = _decision(
        context, "ADMISSION_GRANTED", fence=11, lease_deadline=123.0
    )
    result = step(
        context,
        "ADMISSION_GRANTED",
        payload,
        trusted_authority_snapshot_hash=SNAPSHOT,
    )
    assert result.accepted and result.context["state"] == "HELD"
    assert result.context["fence"] == 11
    assert result.context["lease_deadline"] == 123.0
    assert [effect["type"] for effect in result.effects] == ["RecordLeaseGrant"]
    assert result.effects[0]["decision_id"] == payload["decision_id"]
    assert project_legacy(result.context["state"], result.context).state == "HELD"


def test_queue_then_promote_uses_same_identity_and_durable_sequence():
    context = _context()
    queued = _decision(
        context,
        "ADMISSION_QUEUED",
        queue_seq=9,
        enqueued_at=100.0,
        wait_deadline=200.0,
    )
    first = step(
        context,
        "ADMISSION_QUEUED",
        queued,
        trusted_authority_snapshot_hash=SNAPSHOT,
    )
    assert first.accepted and first.context["state"] == "PENDING"
    promoted = _decision(
        first.context,
        "PROMOTION_GRANTED",
        queue_seq=9,
        fence=12,
        lease_deadline=250.0,
    )
    second = step(
        first.context,
        "PROMOTION_GRANTED",
        promoted,
        trusted_authority_snapshot_hash=SNAPSHOT,
        seen_decisions=first.seen_decisions,
    )
    assert second.accepted and second.context["state"] == "HELD"
    assert second.context["queue_seq"] == 9
    assert project_legacy(second.context["state"], second.context).state == "HELD"


def test_exact_decision_replay_is_noop_before_transition_lookup():
    context = _context()
    payload = _decision(
        context, "ADMISSION_GRANTED", fence=1, lease_deadline=10.0
    )
    first = step(
        context,
        "ADMISSION_GRANTED",
        payload,
        trusted_authority_snapshot_hash=SNAPSHOT,
    )
    replay = step(
        first.context,
        "ADMISSION_GRANTED",
        payload,
        trusted_authority_snapshot_hash=SNAPSHOT,
        seen_decisions=first.seen_decisions,
    )
    assert replay.accepted and replay.replayed
    assert replay.context == first.context
    assert replay.effects == ()


@pytest.mark.parametrize("field", IDENTITY_FIELDS)
def test_each_identity_field_mutation_is_rejected_without_state_change(field):
    context = _context()
    payload = _decision(
        context, "ADMISSION_GRANTED", fence=1, lease_deadline=10.0
    )
    mutated = copy.deepcopy(payload)
    value = mutated[field]
    if field == "pathspec_digest":
        mutated[field] = "c" * 64
    else:
        mutated[field] = value + 1 if isinstance(value, int) else f"{value}-mutated"
    # Rebinding proves the guard compares the payload with trusted configuration;
    # this is not merely a stale digest rejection.
    mutated = bind_decision_id("ADMISSION_GRANTED", mutated)
    result = step(
        context,
        "ADMISSION_GRANTED",
        mutated,
        trusted_authority_snapshot_hash=SNAPSHOT,
    )
    assert not result.accepted
    assert result.context == context
    assert result.effects[0]["type"] == "AuditInvalidTransition"
    assert result.reason == f"identity_mismatch:{field}"


def test_trusted_authority_snapshot_and_promotion_sequence_are_guards():
    context = _context("PENDING") | {"queue_seq": 4}
    payload = _decision(
        context,
        "PROMOTION_GRANTED",
        queue_seq=4,
        fence=2,
        lease_deadline=20.0,
    )
    stale = step(
        context,
        "PROMOTION_GRANTED",
        payload,
        trusted_authority_snapshot_hash="c" * 64,
    )
    assert not stale.accepted and stale.reason == "authority_snapshot_mismatch"

    wrong_seq = dict(payload, queue_seq=5)
    wrong_seq = bind_decision_id("PROMOTION_GRANTED", wrong_seq)
    rejected = step(
        context,
        "PROMOTION_GRANTED",
        wrong_seq,
        trusted_authority_snapshot_hash=SNAPSHOT,
    )
    assert not rejected.accepted and rejected.reason == "identity_mismatch:queue_seq"


def test_same_decision_id_with_different_envelope_is_security_conflict():
    context = _context()
    payload = _decision(
        context, "ADMISSION_GRANTED", fence=1, lease_deadline=10.0
    )
    first = step(
        context,
        "ADMISSION_GRANTED",
        payload,
        trusted_authority_snapshot_hash=SNAPSHOT,
    )
    forged = dict(payload, fence=99)  # retain the already-seen decision_id
    conflict = step(
        first.context,
        "ADMISSION_GRANTED",
        forged,
        trusted_authority_snapshot_hash=SNAPSHOT,
        seen_decisions=first.seen_decisions,
    )
    assert not conflict.accepted
    assert conflict.reason == "decision_envelope_conflict"
    assert conflict.context == first.context


def test_missing_required_payload_and_unknown_event_are_audited():
    context = _context()
    missing = _decision(
        context, "ADMISSION_GRANTED", fence=1, lease_deadline=10.0
    )
    missing.pop("owner_agent")
    result = step(
        context,
        "ADMISSION_GRANTED",
        missing,
        trusted_authority_snapshot_hash=SNAPSHOT,
    )
    assert not result.accepted and result.reason.startswith("missing_required:")
    unknown = step(
        context,
        "FINALIZE_COMMITTED",
        {"actor": "x", "event_id": "e"},
        trusted_authority_snapshot_hash=SNAPSHOT,
    )
    assert not unknown.accepted and unknown.reason == "unknown_event"


@pytest.mark.parametrize(
    ("field", "value", "reason"),
    [
        ("pathspec_digest", "not-a-sha", "invalid_property:pathspec_digest:sha256"),
        ("pathspec_digest", "B" * 64, "invalid_property:pathspec_digest:sha256"),
        (
            "request_generation",
            True,
            "invalid_property:request_generation:non-negative integer",
        ),
        ("actor", "", "invalid_property:actor:actor-id"),
        ("lease_deadline", float("inf"), "invalid_property:lease_deadline:timestamp"),
    ],
)
def test_declared_payload_property_types_are_executable_guards(field, value, reason):
    context = _context()
    payload = _decision(
        context, "ADMISSION_GRANTED", fence=1, lease_deadline=10.0
    )
    payload[field] = value
    payload = bind_decision_id("ADMISSION_GRANTED", payload)
    result = step(
        context,
        "ADMISSION_GRANTED",
        payload,
        trusted_authority_snapshot_hash=SNAPSHOT,
    )
    assert not result.accepted
    assert result.context == context
    assert result.reason == reason


def test_payload_must_be_mapping_and_unknown_declared_types_fail_closed():
    context = _context()
    not_object = step(
        context,
        "ADMISSION_GRANTED",
        [],
        trusted_authority_snapshot_hash=SNAPSHOT,
    )
    assert not not_object.accepted and not_object.reason == "payload_not_object"

    model = json.loads(DEFAULT_SPEC.read_text(encoding="utf-8"))
    model["event_schemas"]["ADMISSION_GRANTED"]["properties"]["actor"][
        "type"
    ] = "unrecognized-type"
    payload = _decision(
        context, "ADMISSION_GRANTED", fence=1, lease_deadline=10.0
    )
    rejected = step(
        context,
        "ADMISSION_GRANTED",
        payload,
        trusted_authority_snapshot_hash=SNAPSHOT,
        spec=model,
    )
    assert not rejected.accepted
    assert rejected.reason == "invalid_property:actor:unrecognized-type"


def test_emitted_effect_payload_is_validated_against_declared_schema():
    context = _context()
    model = json.loads(DEFAULT_SPEC.read_text(encoding="utf-8"))
    model["effects"]["RecordLeaseGrant"]["payload_schema"]["properties"][
        "fence"
    ]["type"] = "unrecognized-type"
    payload = _decision(
        context, "ADMISSION_GRANTED", fence=1, lease_deadline=10.0
    )
    rejected = step(
        context,
        "ADMISSION_GRANTED",
        payload,
        trusted_authority_snapshot_hash=SNAPSHOT,
        spec=model,
    )
    assert not rejected.accepted
    assert rejected.reason == (
        "invalid_effect:RecordLeaseGrant:"
        "invalid_property:fence:unrecognized-type"
    )


def test_packaged_fsm_is_byte_identical_to_authoring_source():
    authoring = Path(__file__).resolve().parents[1] / "spec" / "scheduler_admission_fsm.json"
    assert DEFAULT_SPEC.read_bytes() == authoring.read_bytes()


def _row_identity(omd, row):
    return {
        "repository_id": omd.repository_id,
        "request_id": row["request_id"],
        "orbit_id": row["orbit_id"],
        "request_generation": row["request_generation"],
        "owner_agent": row["agent_id"],
        "bail_epoch": row["bail_epoch"],
        "mode": row["mode"],
        "pathspec_digest": row["pathspec_digest"],
        "policy_version": row["policy_version"],
    }


def _production_payload(omd, row):
    identity = _row_identity(omd, row)
    common = {
        **identity,
        "actor": omd.coordinator_id,
        "event_id": "reconstructed-event-id-not-hashed",
        "authority_snapshot_hash": row["authority_snapshot_hash"],
        "base_priority": row["priority"],
        "effective_priority": row["decision_effective_priority"],
        "observed_at": row["decision_observed_at"],
    }
    event_type = row["decision_type"]
    if event_type == "ADMISSION_GRANTED":
        variant = {"fence": row["fence"], "lease_deadline": row["expires_at"]}
    elif event_type == "ADMISSION_QUEUED":
        variant = {
            "queue_seq": row["queue_seq"],
            "enqueued_at": row["enqueued_at"],
            "wait_deadline": row["wait_deadline"],
        }
    elif event_type == "PROMOTION_BLOCKED":
        blockers = json.loads(row["blocker_ids"])
        variant = {
            "queue_seq": row["queue_seq"],
            "blocker_fingerprint": sha256_json(blockers),
        }
    elif event_type == "PROMOTION_GRANTED":
        variant = {
            "queue_seq": row["queue_seq"],
            "fence": row["fence"],
            "lease_deadline": row["expires_at"],
        }
    elif event_type == "PROMOTION_DENIED":
        variant = {"queue_seq": row["queue_seq"], "reason": row["terminal_reason"]}
    else:  # pragma: no cover - fail loudly if production adds a decision unbound here
        raise AssertionError(f"unbound production decision: {event_type}")
    payload = bind_decision_id(event_type, common | variant)
    assert payload["decision_id"] == row["decision_id"]
    return event_type, payload


def test_real_coordinator_decisions_resolve_to_semantic_model_and_legacy_projection(tmp_path):
    omd = Coordinator(
        str(tmp_path / "omd.db"), agent_ttl=None, admission_wait_timeout=17.0
    )
    held = omd.claim("holder", ["src/a.py"], request_id="held-request")
    held_row = omd.store.get_orbit(held["orbit_id"])
    event_type, payload = _production_payload(omd, held_row)
    initial = _row_identity(omd, held_row) | {"state": "REQUESTED"}
    reduced = step(
        initial,
        event_type,
        payload,
        trusted_authority_snapshot_hash=held_row["authority_snapshot_hash"],
    )
    assert reduced.accepted
    assert project_legacy(reduced.context["state"], reduced.context).state == held_row["state"]

    pending = omd.claim("waiter", ["src/**"], request_id="pending-request")
    pending_row = omd.store.get_orbit(pending["orbit_id"])
    assert pending_row["wait_deadline"] == pytest.approx(
        pending_row["enqueued_at"] + 17.0
    )
    event_type, payload = _production_payload(omd, pending_row)
    initial = _row_identity(omd, pending_row) | {"state": "REQUESTED"}
    reduced = step(
        initial,
        event_type,
        payload,
        trusted_authority_snapshot_hash=pending_row["authority_snapshot_hash"],
    )
    assert reduced.accepted
    assert project_legacy(reduced.context["state"], reduced.context).state == pending_row["state"]

    # B waits on A; A's later disjoint request waits only on B's reservation and
    # is denied because that would close a reservation cycle.
    denied = omd.claim("holder", ["src/b.py"], request_id="denied-request")
    denied_row = omd.store.get_orbit(denied["orbit_id"])
    event_type, payload = _production_payload(omd, denied_row)
    queued_context = _row_identity(omd, denied_row) | {
        "state": "PENDING",
        "queue_seq": denied_row["queue_seq"],
    }
    reduced = step(
        queued_context,
        event_type,
        payload,
        trusted_authority_snapshot_hash=denied_row["authority_snapshot_hash"],
    )
    projection = project_legacy(reduced.context["state"], reduced.context)
    assert reduced.accepted and projection.state == denied_row["state"] == "DENIED"
    assert projection.terminal_reason == "reservation_cycle"

    # Stored path digest is also independently reproducible from the persisted paths.
    assert held_row["pathspec_digest"] == pathspec_digest(json.loads(held_row["pathspec"]))
    omd.close()
