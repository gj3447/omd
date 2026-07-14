"""Versioned canonical JSON codec for the OMD v2 persistence boundary."""

from __future__ import annotations

import json
from typing import Mapping

from .errors import DomainError, ErrorCode
from .model import (
    ClaimChanged,
    ClaimExpired,
    ClaimGranted,
    ClaimRecord,
    ClaimRegistered,
    ClaimReleased,
    ClaimRenewed,
    ClaimStatus,
    DomainEffect,
    DomainEvent,
    DomainState,
    FenceEntry,
    FenceVector,
    IdempotencyRecord,
    IdempotencyRecorded,
    PendingTimedOut,
    PendingFenced,
    Principal,
)
from .resource import (
    AccessMode,
    CaseMode,
    ClaimSpec,
    RepoPolicy,
    ResourceId,
    SelectorKind,
)


CODEC_VERSION = 2


def _dumps(payload: object) -> str:
    return json.dumps(
        payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False
    )


def _loads(payload: str) -> object:
    return json.loads(payload)


def _principal(value: Principal) -> dict[str, object]:
    return {
        "client_id": value.client_id,
        "agent_id": value.agent_id,
        "session_epoch": value.session_epoch,
    }


def _decode_principal(value: Mapping[str, object]) -> Principal:
    return Principal(
        client_id=str(value["client_id"]),
        agent_id=str(value["agent_id"]),
        session_epoch=int(value["session_epoch"]),
    )


def _resource(value: ResourceId) -> dict[str, object]:
    return {
        "domain_id": value.domain_id,
        "repo_id": value.repo_id,
        "segments": list(value.segments),
        "selector": value.selector.value,
    }


def _decode_resource(value: Mapping[str, object]) -> ResourceId:
    return ResourceId(
        domain_id=str(value["domain_id"]),
        repo_id=str(value["repo_id"]),
        segments=tuple(str(item) for item in value["segments"]),  # type: ignore[arg-type]
        selector=SelectorKind(str(value["selector"])),
    )


def _claim_spec(value: ClaimSpec) -> dict[str, object]:
    return {"resource": _resource(value.resource), "mode": value.mode.value}


def _decode_claim_spec(value: Mapping[str, object]) -> ClaimSpec:
    return ClaimSpec(
        resource=_decode_resource(value["resource"]),  # type: ignore[arg-type]
        mode=AccessMode(str(value["mode"])),
    )


def _fence(value: FenceVector | None) -> dict[str, object] | None:
    if value is None:
        return None
    return {
        "claim_id": value.claim_id,
        "owner": _principal(value.owner),
        "entries": [
            {"resource": _resource(entry.resource), "grant_epoch": entry.grant_epoch}
            for entry in value.entries
        ],
        "vector_digest": value.vector_digest,
    }


def _decode_fence(value: Mapping[str, object] | None) -> FenceVector | None:
    if value is None:
        return None
    entries = value["entries"]  # type: ignore[assignment]
    return FenceVector(
        claim_id=str(value["claim_id"]),
        owner=_decode_principal(value["owner"]),  # type: ignore[arg-type]
        entries=tuple(
            FenceEntry(
                resource=_decode_resource(entry["resource"]),
                grant_epoch=int(entry["grant_epoch"]),
            )
            for entry in entries  # type: ignore[union-attr]
        ),
        vector_digest=str(value["vector_digest"]),
    )


def _claim_record(value: ClaimRecord) -> dict[str, object]:
    return {
        "claim_id": value.claim_id,
        "operation_id": value.operation_id,
        "owner": _principal(value.owner),
        "claims": [_claim_spec(spec) for spec in value.claims],
        "status": value.status.value,
        "enqueue_seq": value.enqueue_seq,
        "enqueued_at_ms": value.enqueued_at_ms,
        "requested_lease_ttl_ms": value.requested_lease_ttl_ms,
        "requested_wait_timeout_ms": value.requested_wait_timeout_ms,
        "wait_deadline_ms": value.wait_deadline_ms,
        "lease_deadline_ms": value.lease_deadline_ms,
        "fence": _fence(value.fence),
    }


def _decode_claim_record(value: Mapping[str, object]) -> ClaimRecord:
    return ClaimRecord(
        claim_id=str(value["claim_id"]),
        operation_id=str(value["operation_id"]),
        owner=_decode_principal(value["owner"]),  # type: ignore[arg-type]
        claims=tuple(
            _decode_claim_spec(item)
            for item in value["claims"]  # type: ignore[union-attr]
        ),
        status=ClaimStatus(str(value["status"])),
        enqueue_seq=int(value["enqueue_seq"]),
        enqueued_at_ms=int(value["enqueued_at_ms"]),
        requested_lease_ttl_ms=int(value["requested_lease_ttl_ms"]),
        requested_wait_timeout_ms=int(value["requested_wait_timeout_ms"]),
        wait_deadline_ms=int(value["wait_deadline_ms"]),
        lease_deadline_ms=(
            None
            if value["lease_deadline_ms"] is None
            else int(value["lease_deadline_ms"])
        ),
        fence=_decode_fence(value["fence"]),  # type: ignore[arg-type]
    )


def encode_error(error: DomainError | None) -> str | None:
    if error is None:
        return None
    return _dumps({"code": error.code.value, "details": list(error.details)})


def decode_error(payload: str | None) -> DomainError | None:
    if payload is None:
        return None
    value = _loads(payload)
    return DomainError(
        ErrorCode(value["code"]),  # type: ignore[index]
        tuple((str(k), str(v)) for k, v in value["details"]),  # type: ignore[index]
    )


def encode_state(state: DomainState) -> str:
    """Encode the aggregate projection; idempotency has a normalized table."""

    payload = {
        "codec_version": CODEC_VERSION,
        "domain_id": state.domain_id,
        "last_now_ms": state.last_now_ms,
        "next_enqueue_seq": state.next_enqueue_seq,
        "next_grant_epoch": state.next_grant_epoch,
        "repo_policies": [
            {
                "repo_id": policy.repo_id,
                "case_mode": policy.case_mode.value,
                "unicode_form": policy.unicode_form,
                "forbidden_symlink_prefixes": [
                    list(prefix) for prefix in policy.forbidden_symlink_prefixes
                ],
            }
            for policy in sorted(state.repo_policies.values(), key=lambda item: item.repo_id)
        ],
        "claims": [
            _claim_record(record)
            for record in sorted(state.claims.values(), key=lambda item: item.claim_id)
        ],
    }
    return _dumps(payload)


def decode_state(
    payload: str,
    *,
    idempotency: Mapping[tuple[str, str], IdempotencyRecord],
    session_epochs: Mapping[tuple[str, str], int],
) -> DomainState:
    value = _loads(payload)
    if int(value["codec_version"]) != CODEC_VERSION:  # type: ignore[index]
        raise ValueError("unsupported state codec version")
    policies = {
        str(item["repo_id"]): RepoPolicy(
            repo_id=str(item["repo_id"]),
            case_mode=CaseMode(str(item["case_mode"])),
            unicode_form=str(item["unicode_form"]),  # type: ignore[arg-type]
            forbidden_symlink_prefixes=tuple(
                tuple(str(segment) for segment in prefix)
                for prefix in item["forbidden_symlink_prefixes"]
            ),
        )
        for item in value["repo_policies"]  # type: ignore[index,union-attr]
    }
    claims = {
        str(item["claim_id"]): _decode_claim_record(item)
        for item in value["claims"]  # type: ignore[index,union-attr]
    }
    return DomainState(
        domain_id=str(value["domain_id"]),  # type: ignore[index]
        last_now_ms=int(value["last_now_ms"]),  # type: ignore[index]
        next_enqueue_seq=int(value["next_enqueue_seq"]),  # type: ignore[index]
        next_grant_epoch=int(value["next_grant_epoch"]),  # type: ignore[index]
        repo_policies=policies,
        session_epochs=session_epochs,
        claims=claims,
        idempotency=idempotency,
    )


def encode_event(event: DomainEvent) -> tuple[str, str]:
    if isinstance(event, IdempotencyRecorded):
        payload = {
            "key": list(event.key),
            "record": {
                "fingerprint": event.record.fingerprint,
                "operation_id": event.record.operation_id,
                "claim_id": event.record.claim_id,
                "frozen_error": encode_error(event.record.frozen_error),
            },
            "occurred_at_ms": event.occurred_at_ms,
        }
        return "idempotency_recorded", _dumps(payload)
    if isinstance(event, ClaimRegistered):
        return "claim_registered", _dumps(
            {"record": _claim_record(event.record), "occurred_at_ms": event.occurred_at_ms}
        )
    if isinstance(event, ClaimGranted):
        return "claim_granted", _dumps(
            {
                "claim_id": event.claim_id,
                "lease_deadline_ms": event.lease_deadline_ms,
                "fence": _fence(event.fence),
                "occurred_at_ms": event.occurred_at_ms,
            }
        )
    if isinstance(event, ClaimRenewed):
        kind = "claim_renewed"
        payload = {
            "claim_id": event.claim_id,
            "lease_deadline_ms": event.lease_deadline_ms,
            "requested_lease_ttl_ms": event.requested_lease_ttl_ms,
            "occurred_at_ms": event.occurred_at_ms,
        }
    elif isinstance(event, ClaimReleased):
        kind = "claim_released"
        payload = {"claim_id": event.claim_id, "occurred_at_ms": event.occurred_at_ms}
    elif isinstance(event, ClaimExpired):
        kind = "claim_expired"
        payload = {"claim_id": event.claim_id, "occurred_at_ms": event.occurred_at_ms}
    elif isinstance(event, PendingTimedOut):
        kind = "pending_timed_out"
        payload = {"claim_id": event.claim_id, "occurred_at_ms": event.occurred_at_ms}
    elif isinstance(event, PendingFenced):
        kind = "pending_fenced"
        payload = {"claim_id": event.claim_id, "occurred_at_ms": event.occurred_at_ms}
    else:  # pragma: no cover
        raise TypeError(type(event).__qualname__)
    return kind, _dumps(payload)


def encode_effect(effect: DomainEffect) -> tuple[str, str]:
    if isinstance(effect, ClaimChanged):
        return "claim_changed", _dumps(
            {
                "claim_id": effect.claim_id,
                "status": effect.status.value,
                "occurred_at_ms": effect.occurred_at_ms,
            }
        )
    raise TypeError(type(effect).__qualname__)  # pragma: no cover


def decode_effect(kind: str, payload: str) -> DomainEffect:
    value = _loads(payload)
    if kind == "claim_changed":
        return ClaimChanged(
            claim_id=str(value["claim_id"]),  # type: ignore[index]
            status=ClaimStatus(str(value["status"])),  # type: ignore[index]
            occurred_at_ms=int(value["occurred_at_ms"]),  # type: ignore[index]
        )
    raise ValueError(f"unsupported effect kind: {kind}")
