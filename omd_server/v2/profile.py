"""Lease-only application profile for the OMD v2 kernel.

This module is the imperative shell for lease commands only.  It intentionally
does not import the legacy Coordinator, Git adapter, task lifecycle, or merge
workflow.  Observational methods load a projection and never run maintenance.
"""

from __future__ import annotations

import time
import hashlib
import json
from dataclasses import dataclass
from typing import Callable, Mapping

from .errors import DomainError, ErrorCode, ResourceValidationError
from .model import (
    Accepted,
    ClaimCommand,
    ClaimRecord,
    CommandEnvelope,
    FenceEntry,
    FenceVector,
    MaintenanceTick,
    MalformedCommand,
    Principal,
    Rejected,
    ReleaseCommand,
    RenewCommand,
)
from .resource import (
    AccessMode,
    ClaimSpec,
    ResourceId,
    SelectorKind,
    canonicalize_resource,
)
from .store import ExecutionReceipt, SQLiteCoordinationStore


@dataclass(frozen=True, slots=True)
class Capability:
    name: str
    mutates: bool


LEASE_ONLY_CAPABILITIES = (
    Capability("about", False),
    Capability("claim_set", True),
    Capability("renew_claim_set", True),
    Capability("release_claim_set", True),
    Capability("claim_status", False),
    Capability("domain_status", False),
)


@dataclass(frozen=True, slots=True)
class ResourceRequest:
    path: str
    mode: AccessMode
    selector: SelectorKind = SelectorKind.EXACT
    repo_id: str | None = None


ClockMs = Callable[[], int]


def _system_clock_ms() -> int:
    return time.time_ns() // 1_000_000


def _resource_to_wire(resource: ResourceId) -> dict[str, object]:
    return {
        "domain_id": resource.domain_id,
        "repo_id": resource.repo_id,
        "path": "/".join(resource.segments),
        "segments": list(resource.segments),
        "selector": resource.selector.value,
    }


def _principal_to_wire(principal: Principal) -> dict[str, object]:
    return {
        "client_id": principal.client_id,
        "agent_id": principal.agent_id,
        "session_epoch": principal.session_epoch,
    }


def fence_to_wire(fence: FenceVector | None) -> dict[str, object] | None:
    if fence is None:
        return None
    return {
        "claim_id": fence.claim_id,
        "owner": _principal_to_wire(fence.owner),
        "entries": [
            {
                "resource": _resource_to_wire(entry.resource),
                "grant_epoch": entry.grant_epoch,
            }
            for entry in fence.entries
        ],
        "vector_digest": fence.vector_digest,
    }


def fence_from_wire(value: Mapping[str, object]) -> FenceVector:
    owner = value["owner"]
    entries = value["entries"]
    return FenceVector(
        claim_id=str(value["claim_id"]),
        owner=Principal(
            client_id=str(owner["client_id"]),  # type: ignore[index]
            agent_id=str(owner["agent_id"]),  # type: ignore[index]
            session_epoch=int(owner["session_epoch"]),  # type: ignore[index]
        ),
        entries=tuple(
            FenceEntry(
                resource=ResourceId(
                    domain_id=str(entry["resource"]["domain_id"]),
                    repo_id=str(entry["resource"]["repo_id"]),
                    segments=tuple(
                        str(segment)
                        for segment in entry["resource"]["segments"]
                    ),
                    selector=SelectorKind(str(entry["resource"]["selector"])),
                ),
                grant_epoch=int(entry["grant_epoch"]),
            )
            for entry in entries  # type: ignore[union-attr]
        ),
        vector_digest=str(value["vector_digest"]),
    )


def _record_to_wire(record: ClaimRecord) -> dict[str, object]:
    return {
        "claim_id": record.claim_id,
        "operation_id": record.operation_id,
        "status": record.status.value,
        "resources": [
            {**_resource_to_wire(spec.resource), "mode": spec.mode.value}
            for spec in record.claims
        ],
        "enqueue_seq": record.enqueue_seq,
        "enqueued_at_ms": record.enqueued_at_ms,
        "requested_lease_ttl_ms": record.requested_lease_ttl_ms,
        "requested_wait_timeout_ms": record.requested_wait_timeout_ms,
        "wait_deadline_ms": record.wait_deadline_ms,
        "lease_deadline_ms": record.lease_deadline_ms,
        "fence": fence_to_wire(
            record.fence if record.status.value == "active" else None
        ),
    }


def _wire_digest(payload: object) -> str:
    encoded = json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        default=lambda value: {
            "invalid_type": type(value).__qualname__,
            "value": str(value),
        },
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _resource_request_payload(resource: ResourceRequest) -> dict[str, object]:
    return {
        "path": resource.path,
        "mode": getattr(resource.mode, "value", resource.mode),
        "selector": getattr(resource.selector, "value", resource.selector),
        "repo_id": resource.repo_id,
    }


class LeaseService:
    def __init__(
        self,
        store: SQLiteCoordinationStore,
        domain_id: str,
        *,
        clock_ms: ClockMs = _system_clock_ms,
    ) -> None:
        self.store = store
        self.domain_id = domain_id
        self._clock_ms = clock_ms

    def _envelope(
        self, principal: Principal, request_id: str, command
    ) -> CommandEnvelope:
        return CommandEnvelope.create(
            protocol_version=2,
            domain_id=self.domain_id,
            principal=principal,
            request_id=request_id,
            command=command,
        )

    def _receipt_to_wire(self, receipt: ExecutionReceipt) -> dict[str, object]:
        result = receipt.result
        if isinstance(result, Rejected):
            return {
                "ok": False,
                "error": {
                    "code": result.error.code.value,
                    "details": dict(result.error.details),
                },
                "replayed": result.replayed,
                "revision": receipt.revision,
            }
        assert isinstance(result, Accepted)
        payload: dict[str, object] = {
            "ok": True,
            "operation_id": result.operation_id,
            "claim_id": result.claim_id,
            "status": None if result.status is None else result.status.value,
            "fence": fence_to_wire(result.fence),
            "replayed": result.replayed,
            "revision": receipt.revision,
        }
        if result.claim_id is not None:
            record = receipt.state.claims[result.claim_id]
            payload.update(_record_to_wire(record))
            payload["fence"] = fence_to_wire(result.fence)
        return payload

    def _execute_malformed(
        self,
        *,
        principal: Principal,
        request_id: str,
        command_name: str,
        wire_payload: object,
        error: DomainError,
    ) -> dict[str, object]:
        receipt = self.store.execute(
            self._envelope(
                principal,
                request_id,
                MalformedCommand(command_name, _wire_digest(wire_payload), error),
            ),
            clock_ms=self._clock_ms,
        )
        return self._receipt_to_wire(receipt)

    def _repo_policy(self, repo_id: str | None):
        policies = self.store.read_domain(self.domain_id).state.repo_policies
        if repo_id is not None:
            try:
                return policies[repo_id]
            except KeyError as exc:
                raise ValueError(f"unknown repository: {repo_id}") from exc
        if len(policies) != 1:
            raise ValueError("repo_id is required for a multi-repository domain")
        return next(iter(policies.values()))

    def claim_set(
        self,
        *,
        principal: Principal,
        request_id: str,
        resources: tuple[ResourceRequest, ...],
        lease_ttl_ms: int,
        wait_timeout_ms: int,
    ) -> dict[str, object]:
        wire_payload = {
            "resources": [_resource_request_payload(item) for item in resources],
            "lease_ttl_ms": lease_ttl_ms,
            "wait_timeout_ms": wait_timeout_ms,
        }
        try:
            claims = tuple(
                ClaimSpec(
                    resource=canonicalize_resource(
                        domain_id=self.domain_id,
                        policy=self._repo_policy(resource.repo_id),
                        raw_path=resource.path,
                        selector=(
                            resource.selector
                            if isinstance(resource.selector, SelectorKind)
                            else SelectorKind(str(resource.selector))
                        ),
                    ),
                    mode=(
                        resource.mode
                        if isinstance(resource.mode, AccessMode)
                        else AccessMode(str(resource.mode))
                    ),
                )
                for resource in resources
            )
        except ResourceValidationError as exc:
            return self._execute_malformed(
                principal=principal,
                request_id=request_id,
                command_name="claim_set",
                wire_payload=wire_payload,
                error=exc.error,
            )
        except (KeyError, TypeError, ValueError, AttributeError) as exc:
            return self._execute_malformed(
                principal=principal,
                request_id=request_id,
                command_name="claim_set",
                wire_payload=wire_payload,
                error=DomainError.make(
                    ErrorCode.INVALID_RESOURCE, reason=type(exc).__qualname__
                ),
            )
        command = ClaimCommand(claims, lease_ttl_ms, wait_timeout_ms)
        receipt = self.store.execute(
            self._envelope(principal, request_id, command), clock_ms=self._clock_ms
        )
        return self._receipt_to_wire(receipt)

    def release_claim_set(
        self,
        *,
        principal: Principal,
        request_id: str,
        claim_id: str,
        fence: Mapping[str, object],
    ) -> dict[str, object]:
        try:
            proof = fence_from_wire(fence)
        except (KeyError, TypeError, ValueError, IndexError):
            return self._execute_malformed(
                principal=principal,
                request_id=request_id,
                command_name="release_claim_set",
                wire_payload={"claim_id": claim_id, "fence": fence},
                error=DomainError.make(
                    ErrorCode.STALE_FENCE_VECTOR, claim_id=claim_id
                ),
            )
        receipt = self.store.execute(
            self._envelope(
                principal, request_id, ReleaseCommand(claim_id, proof)
            ),
            clock_ms=self._clock_ms,
        )
        return self._receipt_to_wire(receipt)

    def renew_claim_set(
        self,
        *,
        principal: Principal,
        request_id: str,
        claim_id: str,
        fence: Mapping[str, object],
        lease_ttl_ms: int,
    ) -> dict[str, object]:
        try:
            proof = fence_from_wire(fence)
        except (KeyError, TypeError, ValueError, IndexError):
            return self._execute_malformed(
                principal=principal,
                request_id=request_id,
                command_name="renew_claim_set",
                wire_payload={
                    "claim_id": claim_id,
                    "fence": fence,
                    "lease_ttl_ms": lease_ttl_ms,
                },
                error=DomainError.make(
                    ErrorCode.STALE_FENCE_VECTOR, claim_id=claim_id
                ),
            )
        receipt = self.store.execute(
            self._envelope(
                principal,
                request_id,
                RenewCommand(claim_id, proof, lease_ttl_ms),
            ),
            clock_ms=self._clock_ms,
        )
        return self._receipt_to_wire(receipt)

    def maintenance_tick(
        self,
        *,
        principal: Principal,
        request_id: str,
        observed_now_ms: int | None = None,
    ) -> dict[str, object]:
        if observed_now_ms is not None and (
            type(observed_now_ms) is not int or observed_now_ms < 0
        ):
            raise ValueError("observed_now_ms must be a nonnegative integer")
        clock_ms = self._clock_ms
        if observed_now_ms is not None:
            clock_ms = lambda: max(self._clock_ms(), observed_now_ms)
        receipt = self.store.execute(
            self._envelope(principal, request_id, MaintenanceTick()),
            clock_ms=clock_ms,
        )
        return self._receipt_to_wire(receipt)

    def next_maintenance_deadline_ms(self) -> int | None:
        snapshot = self.store.read_domain(self.domain_id)
        deadlines = [
            deadline
            for record in snapshot.state.claims.values()
            for deadline in (
                record.lease_deadline_ms
                if record.status.value == "active"
                else record.wait_deadline_ms
                if record.status.value == "pending"
                else None,
            )
            if deadline is not None
        ]
        return min(deadlines, default=None)

    def now_ms(self) -> int:
        return self._clock_ms()

    def claim_status(
        self, claim_id: str, *, principal: Principal | None = None
    ) -> dict[str, object]:
        snapshot = self.store.read_domain(self.domain_id)
        record = snapshot.state.claims.get(claim_id)
        if record is None:
            return {
                "ok": False,
                "error": {
                    "code": ErrorCode.UNKNOWN_CLAIM.value,
                    "details": {"claim_id": claim_id},
                },
                "revision": snapshot.revision,
            }
        if principal is not None and record.owner != principal:
            return {
                "ok": False,
                "error": {
                    "code": ErrorCode.NOT_OWNER.value,
                    "details": {"claim_id": claim_id},
                },
                "revision": snapshot.revision,
            }
        return {"ok": True, "revision": snapshot.revision, **_record_to_wire(record)}

    def domain_status(
        self, *, principal: Principal | None = None
    ) -> dict[str, object]:
        snapshot = self.store.read_domain(self.domain_id)
        records = sorted(
            snapshot.state.claims.values(),
            key=lambda item: (item.enqueue_seq, item.claim_id),
        )
        visible = (
            records
            if principal is None
            else [record for record in records if record.owner == principal]
        )
        counts: dict[str, int] = {}
        for record in records:
            counts[record.status.value] = counts.get(record.status.value, 0) + 1
        return {
            "ok": True,
            "domain_id": self.domain_id,
            "revision": snapshot.revision,
            "last_now_ms": snapshot.state.last_now_ms,
            "counts": counts,
            "claims": [_record_to_wire(record) for record in visible],
        }
