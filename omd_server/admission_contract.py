"""Executable payload contract for the semantic OrbitRequest FSM.

The JSON FSM remains the model source of truth.  This reducer loads its event
schemas, transitions, context updates, and effect bindings, while computing the
identity/deadline guards from real payloads.  It is intentionally independent
from the legacy :mod:`omd_server.fsm` enum projection.
"""

from __future__ import annotations

import json
import math
import re
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from typing import Any, Mapping, MutableMapping

from .admission import sha256_json


DEFAULT_SPEC = Path(__file__).with_name("scheduler_admission_fsm.json")

IDENTITY_FIELDS = (
    "repository_id",
    "request_id",
    "orbit_id",
    "request_generation",
    "owner_agent",
    "bail_epoch",
    "mode",
    "pathspec_digest",
    "policy_version",
)
REQUEST_FIELDS = ("repository_id", "request_id", "orbit_id", "request_generation")
OWNER_FIELDS = REQUEST_FIELDS + ("owner_agent", "bail_epoch")
LEASE_FIELDS = OWNER_FIELDS + ("fence",)
SHA256_RE = re.compile(r"^[0-9a-f]{64}$")


@lru_cache(maxsize=4)
def load_spec(path: str = str(DEFAULT_SPEC)) -> dict[str, Any]:
    return json.loads(Path(path).read_text(encoding="utf-8"))


@dataclass(frozen=True)
class GuardResult:
    ok: bool
    reason: str = ""


@dataclass(frozen=True)
class LegacyProjection:
    state: str | None
    terminal_reason: str | None = None


@dataclass(frozen=True)
class StepResult:
    context: dict[str, Any]
    effects: tuple[dict[str, Any], ...]
    seen_decisions: dict[str, dict[str, Any]]
    accepted: bool
    replayed: bool = False
    reason: str = ""


def _variant_fields(contract: Mapping[str, Any], event_type: str) -> list[str]:
    for declared, fields in contract["variant_fields"].items():
        if event_type in declared.split("|"):
            return list(fields)
    raise ValueError(f"event {event_type!r} is not an admission decision")


def _valid_property(value: Any, declaration: Mapping[str, Any]) -> bool:
    declared_type = declaration.get("type")
    if declared_type in {"string", "actor-id"}:
        valid = isinstance(value, str) and bool(value)
    elif declared_type == "sha256":
        valid = isinstance(value, str) and SHA256_RE.fullmatch(value) is not None
    elif declared_type == "non-negative integer":
        valid = isinstance(value, int) and not isinstance(value, bool) and value >= 0
    elif declared_type == "integer":
        valid = isinstance(value, int) and not isinstance(value, bool)
    elif declared_type == "timestamp":
        valid = (
            isinstance(value, (int, float))
            and not isinstance(value, bool)
            and math.isfinite(value)
        )
    else:
        valid = False
    return valid and ("const" not in declaration or value == declaration["const"])


def _validate_payload_schema(
    schema: Mapping[str, Any], payload: Mapping[str, Any]
) -> str | None:
    for field, declaration in schema.get("properties", {}).items():
        if field in payload and not _valid_property(payload[field], declaration):
            return f"invalid_property:{field}:{declaration.get('type', 'unknown')}"
    return None


def canonical_decision_envelope(
    event_type: str,
    payload: Mapping[str, Any],
    *,
    spec: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    model = dict(spec or load_spec())
    contract = model["identity_contracts"]["admission_decision"]
    fields = [
        field for field in contract["common_fields"] if field != "event_type"
    ] + _variant_fields(contract, event_type)
    missing = [field for field in fields if field not in payload]
    if missing:
        raise ValueError(f"missing decision fields: {', '.join(missing)}")
    envelope = {"schema": contract["schema"], "event_type": event_type}
    envelope.update((field, payload[field]) for field in fields)
    return envelope


def admission_decision_id(
    event_type: str,
    payload: Mapping[str, Any],
    *,
    spec: Mapping[str, Any] | None = None,
) -> str:
    return sha256_json(canonical_decision_envelope(event_type, payload, spec=spec))


def bind_decision_id(
    event_type: str,
    payload: Mapping[str, Any],
    *,
    spec: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    out = dict(payload)
    out["decision_id"] = admission_decision_id(event_type, out, spec=spec)
    return out


def _same_fields(
    context: Mapping[str, Any], payload: Mapping[str, Any], fields: tuple[str, ...]
) -> GuardResult:
    for field in fields:
        if context.get(field) != payload.get(field):
            return GuardResult(False, f"identity_mismatch:{field}")
    return GuardResult(True)


def evaluate_decision_guard(
    context: Mapping[str, Any],
    event_type: str,
    payload: Mapping[str, Any],
    *,
    trusted_authority_snapshot_hash: str,
    guard: str | None = None,
    spec: Mapping[str, Any] | None = None,
) -> GuardResult:
    """Compute a declared guard from typed payload and trusted authority state."""
    if payload.get("authority_snapshot_hash") != trusted_authority_snapshot_hash:
        return GuardResult(False, "authority_snapshot_mismatch")

    if guard in ("admission_identity_current", "promotion_identity_current"):
        same = _same_fields(context, payload, IDENTITY_FIELDS)
        if not same.ok:
            return same
        if guard == "promotion_identity_current" and context.get("queue_seq") != payload.get(
            "queue_seq"
        ):
            return GuardResult(False, "identity_mismatch:queue_seq")
        try:
            expected = admission_decision_id(event_type, payload, spec=spec)
        except ValueError as exc:
            return GuardResult(False, f"invalid_decision_envelope:{exc}")
        if payload.get("decision_id") != expected:
            return GuardResult(False, "decision_id_mismatch")
        return GuardResult(True)

    if guard in ("lease_authority_current", "held_owner_reclaim_current"):
        return _same_fields(context, payload, LEASE_FIELDS)
    if guard in ("wait_owner_current", "pending_owner_reclaim_current"):
        same = _same_fields(context, payload, OWNER_FIELDS)
        if not same.ok:
            return same
        if guard == "pending_owner_reclaim_current" and payload.get("no_lease_fence") != 0:
            return GuardResult(False, "pending_reclaim_has_lease_fence")
        return GuardResult(True)
    if guard == "wait_timeout_due":
        same = _same_fields(context, payload, REQUEST_FIELDS)
        if not same.ok:
            return same
        deadline = context.get("wait_deadline")
        observed_at = payload.get("observed_at")
        if (
            not isinstance(deadline, (int, float))
            or isinstance(deadline, bool)
            or not math.isfinite(deadline)
            or not isinstance(observed_at, (int, float))
            or isinstance(observed_at, bool)
            or not math.isfinite(observed_at)
        ):
            return GuardResult(False, "invalid_wait_deadline")
        return GuardResult(
            observed_at >= deadline,
            "wait_deadline_not_due",
        )
    if guard == "lease_timeout_due":
        same = _same_fields(context, payload, REQUEST_FIELDS)
        if not same.ok:
            return same
        if context.get("fence") != payload.get("fence"):
            return GuardResult(False, "identity_mismatch:fence")
        deadline = context.get("lease_deadline")
        observed_at = payload.get("observed_at")
        if (
            not isinstance(deadline, (int, float))
            or isinstance(deadline, bool)
            or not math.isfinite(deadline)
            or not isinstance(observed_at, (int, float))
            or isinstance(observed_at, bool)
            or not math.isfinite(observed_at)
        ):
            return GuardResult(False, "invalid_lease_deadline")
        return GuardResult(
            observed_at >= deadline,
            "lease_deadline_not_due",
        )
    return GuardResult(False, f"unknown_guard:{guard}")


def _resolve(expression: Any, context: Mapping[str, Any], payload: Mapping[str, Any]) -> Any:
    if not isinstance(expression, str):
        return expression
    if expression.startswith("event."):
        return payload.get(expression.removeprefix("event."))
    if expression.startswith("configuration."):
        return context.get(expression.removeprefix("configuration."))
    if expression == "rejection.reason":
        return "invalid_transition"
    return expression


def _audit(
    context: Mapping[str, Any],
    event_type: str,
    payload: Mapping[str, Any],
    reason: str,
    seen: Mapping[str, dict[str, Any]],
) -> StepResult:
    effect = {
        "type": "AuditInvalidTransition",
        "state": context.get("state"),
        "event": event_type,
        "actor": payload.get("actor"),
        "reason": reason,
        "event_id": payload.get("event_id"),
    }
    return StepResult(dict(context), (effect,), dict(seen), False, reason=reason)


def step(
    context: Mapping[str, Any],
    event_type: str,
    payload: Mapping[str, Any],
    *,
    trusted_authority_snapshot_hash: str,
    seen_decisions: Mapping[str, dict[str, Any]] | None = None,
    spec: Mapping[str, Any] | None = None,
) -> StepResult:
    """Reduce one typed event using the JSON-declared semantic FSM."""
    if not isinstance(payload, Mapping):
        return _audit(context, event_type, {}, "payload_not_object", {})
    model = dict(spec or load_spec())
    seen: MutableMapping[str, dict[str, Any]] = dict(seen_decisions or {})
    schema = model.get("event_schemas", {}).get(event_type)
    if schema is None:
        return _audit(context, event_type, payload, "unknown_event", seen)
    missing = [field for field in schema.get("required", []) if field not in payload]
    if missing:
        return _audit(
            context,
            event_type,
            payload,
            f"missing_required:{','.join(missing)}",
            seen,
        )
    schema_error = _validate_payload_schema(schema, payload)
    if schema_error is not None:
        return _audit(context, event_type, payload, schema_error, seen)

    envelope = None
    decision_id = payload.get("decision_id")
    if decision_id is not None:
        try:
            envelope = canonical_decision_envelope(event_type, payload, spec=model)
        except ValueError as exc:
            return _audit(context, event_type, payload, f"invalid_envelope:{exc}", seen)
        prior = seen.get(decision_id)
        if prior is not None:
            if prior == envelope:
                return StepResult(
                    dict(context), (), dict(seen), True, replayed=True
                )
            return _audit(
                context, event_type, payload, "decision_envelope_conflict", seen
            )
        if admission_decision_id(event_type, payload, spec=model) != decision_id:
            return _audit(context, event_type, payload, "decision_id_mismatch", seen)

    machine = model["machines"][0]
    transition = next(
        (
            candidate
            for candidate in machine["transitions"]
            if candidate["from"] == context.get("state")
            and candidate["event"] == event_type
        ),
        None,
    )
    if transition is None:
        return _audit(context, event_type, payload, "invalid_transition", seen)
    guard_result = evaluate_decision_guard(
        context,
        event_type,
        payload,
        trusted_authority_snapshot_hash=trusted_authority_snapshot_hash,
        guard=transition.get("guard"),
        spec=model,
    )
    if not guard_result.ok:
        return _audit(context, event_type, payload, guard_result.reason, seen)

    next_context = dict(context)
    next_context["state"] = transition["to"]
    for field, expression in transition.get("context_updates", {}).items():
        next_context[field] = _resolve(expression, context, payload)
    effects = []
    bindings = transition.get("effect_bindings", {})
    for effect_name in transition.get("effects", []):
        effect = {"type": effect_name}
        effect.update(
            (
                field,
                _resolve(expression, context, payload),
            )
            for field, expression in bindings.get(effect_name, {}).items()
        )
        effect_schema = model.get("effects", {}).get(effect_name, {}).get(
            "payload_schema", {}
        )
        missing_effect = [
            field for field in effect_schema.get("required", [])
            if field not in effect
        ]
        if missing_effect:
            return _audit(
                context,
                event_type,
                payload,
                f"invalid_effect:{effect_name}:missing_required:"
                f"{','.join(missing_effect)}",
                seen,
            )
        effect_error = _validate_payload_schema(effect_schema, effect)
        if effect_error is not None:
            return _audit(
                context,
                event_type,
                payload,
                f"invalid_effect:{effect_name}:{effect_error}",
                seen,
            )
        effects.append(effect)
    if decision_id is not None and envelope is not None:
        seen[decision_id] = envelope
    return StepResult(next_context, tuple(effects), dict(seen), True)


def project_legacy(state: str, context: Mapping[str, Any]) -> LegacyProjection:
    """One-way semantic OrbitRequest -> legacy Orbit projection."""
    if state == "REQUESTED":
        return LegacyProjection(None)
    if state in {"PENDING", "HELD", "RELEASED", "EXPIRED"}:
        return LegacyProjection(state)
    if state == "DENIED":
        return LegacyProjection("DENIED", context.get("terminal_reason") or "policy_or_cycle_denial")
    if state == "CANCELLED":
        return LegacyProjection("DENIED", context.get("terminal_reason") or "cancelled")
    if state == "TIMED_OUT":
        return LegacyProjection("DENIED", context.get("terminal_reason") or "wait_timeout")
    if state == "REJECTED":
        return LegacyProjection(None, context.get("terminal_reason") or "rejected")
    raise ValueError(f"unknown semantic orbit state: {state!r}")
