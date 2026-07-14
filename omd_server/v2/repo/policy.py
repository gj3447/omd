"""Authority, read-set, and write-set gates for immutable Git candidates."""

from __future__ import annotations

from ..model import FenceVector
from ..resource import (
    AccessMode,
    CaseMode,
    ClaimSpec,
    RepoPolicy,
    SelectorKind,
    canonicalize_resource,
    overlaps,
    resource_key,
    validate_resource_id,
)
from .contracts import (
    MutationReservation,
    SagaRecord,
    reservation_claims_digest,
)
from .errors import AuthorityRejected, PathPolicyError, ReadSetStale, WriteSetViolation
from .git import GitPlumbing


def validate_reservation(
    record: SagaRecord,
    reservation: MutationReservation,
    fence: FenceVector | None,
) -> None:
    if (
        reservation.operation_id != record.operation_id
        or reservation.domain_id != record.domain_id
        or reservation.repo_id != record.repo_id
        or reservation.claim_id != record.claim_id
        or reservation.fence_digest != record.fence_digest
    ):
        raise AuthorityRejected("reservation binding mismatch")
    if not reservation.claims:
        raise AuthorityRejected("reservation has no authoritative claims")
    policy = RepoPolicy(repo_id=record.repo_id, case_mode=CaseMode.SENSITIVE)
    for claim in reservation.claims:
        if not isinstance(claim, ClaimSpec) or not isinstance(claim.mode, AccessMode):
            raise AuthorityRejected("reservation claim shape is invalid")
        if claim.resource.domain_id != record.domain_id:
            raise AuthorityRejected("reservation claim domain mismatch")
        if validate_resource_id(claim.resource, policy) is not None:
            raise AuthorityRejected("reservation contains noncanonical resource")
    if not any(item.mode is AccessMode.WRITE for item in reservation.claims):
        raise AuthorityRejected("reservation contains no WRITE authority")
    digest = reservation_claims_digest(reservation.claims)
    if record.authority_claims_digest is not None and digest != record.authority_claims_digest:
        raise AuthorityRejected("reservation claims changed after handoff")
    if fence is not None:
        fence_resources = sorted(resource_key(item.resource) for item in fence.entries)
        claim_resources = sorted(resource_key(item.resource) for item in reservation.claims)
        if fence_resources != claim_resources:
            raise AuthorityRejected("authority claims differ from complete fence")


def _path_resource(record: SagaRecord, path: str):
    try:
        return canonicalize_resource(
            domain_id=record.domain_id,
            policy=RepoPolicy(repo_id=record.repo_id, case_mode=CaseMode.SENSITIVE),
            raw_path=path,
            selector=SelectorKind.EXACT,
        )
    except Exception as exc:
        raise PathPolicyError(f"path cannot enter lease identity: {path!r}") from exc


def audit_reads(
    git: GitPlumbing,
    record: SagaRecord,
    claims: tuple[ClaimSpec, ...],
) -> None:
    read_resources = tuple(
        item.resource for item in claims if item.mode is AccessMode.READ
    )
    if not read_resources:
        return
    stale: list[str] = []
    for delta in git.diff(record.read_base_oid, record.expected_target_oid):
        changed = _path_resource(record, delta.path)
        if any(overlaps(changed, allowed) for allowed in read_resources):
            stale.append(delta.path)
    if stale:
        raise ReadSetStale(
            "target changed READ resources: " + ", ".join(repr(path) for path in stale)
        )


def audit_writes(
    git: GitPlumbing,
    record: SagaRecord,
    tree_oid: str,
    claims: tuple[ClaimSpec, ...],
) -> None:
    writes = tuple(item.resource for item in claims if item.mode is AccessMode.WRITE)
    denied: list[str] = []
    for delta in git.diff(record.expected_target_oid, tree_oid):
        changed = _path_resource(record, delta.path)
        if not any(overlaps(changed, allowed) for allowed in writes):
            denied.append(delta.path)
    if denied:
        raise WriteSetViolation(
            "candidate changes unauthorized paths: "
            + ", ".join(repr(path) for path in denied)
        )
