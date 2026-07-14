"""Immutable command, state, event, effect, and result contracts for OMD v2."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from enum import Enum
from types import MappingProxyType
from typing import Mapping, TypeAlias

from .errors import DomainError
from .resource import ClaimSpec, RepoPolicy, ResourceId


@dataclass(frozen=True, slots=True)
class Principal:
    client_id: str
    agent_id: str
    session_epoch: int


@dataclass(frozen=True, slots=True)
class ClaimCommand:
    claims: tuple[ClaimSpec, ...]
    lease_ttl_ms: int
    wait_timeout_ms: int

    def __post_init__(self) -> None:
        # Type annotations do not freeze a caller-provided list.  Copy the
        # collection before CommandEnvelope computes its semantic fingerprint.
        object.__setattr__(self, "claims", tuple(self.claims))


@dataclass(frozen=True, slots=True)
class FenceEntry:
    resource: ResourceId
    grant_epoch: int


@dataclass(frozen=True, slots=True)
class FenceVector:
    claim_id: str
    owner: Principal
    entries: tuple[FenceEntry, ...]
    vector_digest: str

    def __post_init__(self) -> None:
        object.__setattr__(self, "entries", tuple(self.entries))


@dataclass(frozen=True, slots=True)
class ReleaseCommand:
    claim_id: str
    fence: FenceVector


@dataclass(frozen=True, slots=True)
class RenewCommand:
    claim_id: str
    fence: FenceVector
    lease_ttl_ms: int


@dataclass(frozen=True, slots=True)
class MaintenanceTick:
    """Explicit state-changing maintenance; observational APIs never run it."""


@dataclass(frozen=True, slots=True)
class MalformedCommand:
    """Canonical application-ingress rejection for a malformed wire mutation.

    Transport/schema failures that never reach the application cannot be
    recorded.  Once a valid principal and request ID reach LeaseService, the
    raw mutation is reduced to a stable digest and durably rejected through
    the same idempotency path as a well-typed command.
    """

    command_name: str
    wire_digest: str
    error: DomainError


Command: TypeAlias = (
    ClaimCommand
    | ReleaseCommand
    | RenewCommand
    | MaintenanceTick
    | MalformedCommand
)


def _resource_payload(resource: object) -> dict[str, object]:
    if not isinstance(resource, ResourceId):
        return {"invalid_type": type(resource).__qualname__}
    return {
        "domain_id": resource.domain_id,
        "repo_id": resource.repo_id,
        "segments": list(resource.segments),
        "selector": (
            resource.selector.value
            if isinstance(resource.selector, Enum)
            else {
                "invalid_type": type(resource.selector).__qualname__,
                "value": str(resource.selector),
            }
        ),
    }


def _fence_payload(fence: FenceVector) -> dict[str, object]:
    entries = sorted(
        fence.entries,
        key=lambda item: json.dumps(
            _resource_payload(item.resource),
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
        ),
    )
    return {
        "claim_id": fence.claim_id,
        "owner": {
            "client_id": fence.owner.client_id,
            "agent_id": fence.owner.agent_id,
            "session_epoch": fence.owner.session_epoch,
        },
        "entries": [
            {"resource": _resource_payload(item.resource), "epoch": item.grant_epoch}
            for item in entries
        ],
        "digest": fence.vector_digest,
    }


def _command_payload(command: Command) -> dict[str, object]:
    if isinstance(command, ClaimCommand):
        def claim_payload(spec: object) -> dict[str, object]:
            if not isinstance(spec, ClaimSpec):
                return {"invalid_type": type(spec).__qualname__}
            return {
                "resource": _resource_payload(spec.resource),
                "mode": (
                    spec.mode.value
                    if isinstance(spec.mode, Enum)
                    else {
                        "invalid_type": type(spec.mode).__qualname__,
                        "value": str(spec.mode),
                    }
                ),
            }

        claims = sorted(
            (claim_payload(spec) for spec in command.claims),
            key=lambda item: json.dumps(
                item, sort_keys=True, separators=(",", ":"), ensure_ascii=False
            ),
        )
        return {
            "type": "claim",
            "claims": claims,
            "lease_ttl_ms": command.lease_ttl_ms,
            "wait_timeout_ms": command.wait_timeout_ms,
        }
    if isinstance(command, ReleaseCommand):
        return {
            "type": "release",
            "claim_id": command.claim_id,
            "fence": _fence_payload(command.fence),
        }
    if isinstance(command, RenewCommand):
        return {
            "type": "renew",
            "claim_id": command.claim_id,
            "fence": _fence_payload(command.fence),
            "lease_ttl_ms": command.lease_ttl_ms,
        }
    if isinstance(command, MaintenanceTick):
        return {"type": "maintenance_tick"}
    if isinstance(command, MalformedCommand):
        return {
            "type": "malformed",
            "command_name": command.command_name,
            "wire_digest": command.wire_digest,
            "error": {
                "code": command.error.code.value,
                "details": list(command.error.details),
            },
        }
    return {"type": f"unsupported:{type(command).__qualname__}"}


def command_fingerprint(
    *,
    protocol_version: int,
    domain_id: str,
    principal: Principal,
    request_id: str,
    command: Command,
) -> str:
    payload = {
        "protocol_version": protocol_version,
        "domain_id": domain_id,
        "principal": {
            "client_id": principal.client_id,
            "agent_id": principal.agent_id,
            "session_epoch": principal.session_epoch,
        },
        "request_id": request_id,
        "command": _command_payload(command),
    }
    encoded = json.dumps(
        payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


@dataclass(frozen=True, slots=True)
class CommandEnvelope:
    protocol_version: int
    domain_id: str
    principal: Principal
    request_id: str
    command: Command
    fingerprint: str = field(init=False)

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "fingerprint",
            command_fingerprint(
                protocol_version=self.protocol_version,
                domain_id=self.domain_id,
                principal=self.principal,
                request_id=self.request_id,
                command=self.command,
            ),
        )

    @classmethod
    def create(
        cls,
        *,
        protocol_version: int,
        domain_id: str,
        principal: Principal,
        request_id: str,
        command: Command,
    ) -> "CommandEnvelope":
        return cls(protocol_version, domain_id, principal, request_id, command)


class ClaimStatus(str, Enum):
    PENDING = "pending"
    ACTIVE = "active"
    RELEASED = "released"
    EXPIRED = "expired"
    TIMED_OUT = "timed_out"
    FENCED = "fenced"


@dataclass(frozen=True, slots=True)
class ClaimRecord:
    claim_id: str
    operation_id: str
    owner: Principal
    claims: tuple[ClaimSpec, ...]
    status: ClaimStatus
    enqueue_seq: int
    enqueued_at_ms: int
    requested_lease_ttl_ms: int
    requested_wait_timeout_ms: int
    wait_deadline_ms: int
    lease_deadline_ms: int | None
    fence: FenceVector | None

    def __post_init__(self) -> None:
        object.__setattr__(self, "claims", tuple(self.claims))


@dataclass(frozen=True, slots=True)
class IdempotencyRecord:
    fingerprint: str
    operation_id: str
    claim_id: str | None
    frozen_error: DomainError | None = None


@dataclass(frozen=True, slots=True)
class DomainState:
    domain_id: str
    last_now_ms: int
    next_enqueue_seq: int
    next_grant_epoch: int
    repo_policies: Mapping[str, RepoPolicy]
    session_epochs: Mapping[tuple[str, str], int]
    claims: Mapping[str, ClaimRecord]
    idempotency: Mapping[tuple[str, str], IdempotencyRecord]

    def __post_init__(self) -> None:
        object.__setattr__(
            self, "repo_policies", MappingProxyType(dict(self.repo_policies))
        )
        object.__setattr__(
            self, "session_epochs", MappingProxyType(dict(self.session_epochs))
        )
        object.__setattr__(self, "claims", MappingProxyType(dict(self.claims)))
        object.__setattr__(self, "idempotency", MappingProxyType(dict(self.idempotency)))

    @classmethod
    def empty(
        cls,
        *,
        domain_id: str,
        repo_policies: tuple[RepoPolicy, ...],
        session_epochs: Mapping[tuple[str, str], int] | None = None,
    ) -> "DomainState":
        policies = {policy.repo_id: policy for policy in repo_policies}
        if len(policies) != len(repo_policies):
            raise ValueError("duplicate repo_id")
        return cls(
            domain_id=domain_id,
            last_now_ms=0,
            next_enqueue_seq=1,
            next_grant_epoch=1,
            repo_policies=policies,
            session_epochs={} if session_epochs is None else session_epochs,
            claims={},
            idempotency={},
        )


@dataclass(frozen=True, slots=True)
class IdempotencyRecorded:
    key: tuple[str, str]
    record: IdempotencyRecord
    occurred_at_ms: int


@dataclass(frozen=True, slots=True)
class ClaimRegistered:
    record: ClaimRecord
    occurred_at_ms: int


@dataclass(frozen=True, slots=True)
class ClaimGranted:
    claim_id: str
    lease_deadline_ms: int
    fence: FenceVector
    occurred_at_ms: int


@dataclass(frozen=True, slots=True)
class ClaimRenewed:
    claim_id: str
    lease_deadline_ms: int
    requested_lease_ttl_ms: int
    occurred_at_ms: int


@dataclass(frozen=True, slots=True)
class ClaimReleased:
    claim_id: str
    occurred_at_ms: int


@dataclass(frozen=True, slots=True)
class ClaimExpired:
    claim_id: str
    occurred_at_ms: int


@dataclass(frozen=True, slots=True)
class PendingTimedOut:
    claim_id: str
    occurred_at_ms: int


@dataclass(frozen=True, slots=True)
class PendingFenced:
    claim_id: str
    occurred_at_ms: int


DomainEvent: TypeAlias = (
    IdempotencyRecorded
    | ClaimRegistered
    | ClaimGranted
    | ClaimRenewed
    | ClaimReleased
    | ClaimExpired
    | PendingTimedOut
    | PendingFenced
)


@dataclass(frozen=True, slots=True)
class ClaimChanged:
    claim_id: str
    status: ClaimStatus
    occurred_at_ms: int


DomainEffect: TypeAlias = ClaimChanged


@dataclass(frozen=True, slots=True)
class Accepted:
    operation_id: str
    claim_id: str | None
    status: ClaimStatus | None
    fence: FenceVector | None
    replayed: bool = False


@dataclass(frozen=True, slots=True)
class Rejected:
    error: DomainError
    replayed: bool = False


CommandResult: TypeAlias = Accepted | Rejected


@dataclass(frozen=True, slots=True)
class Decision:
    events: tuple[DomainEvent, ...]
    effects: tuple[DomainEffect, ...]
    result: CommandResult
