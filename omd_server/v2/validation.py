"""Deterministic command-envelope and claim validation for OMD v2."""

from __future__ import annotations

from .errors import DomainError, ErrorCode
from .model import ClaimCommand, CommandEnvelope, DomainState, command_fingerprint
from .resource import (
    AccessMode,
    ClaimSpec,
    ResourceId,
    SelectorKind,
    claim_spec_key,
    overlaps,
    validate_resource_id,
)


PROTOCOL_VERSION = 2


def _error(code: ErrorCode, **details: object) -> DomainError:
    return DomainError.make(code, **details)


def _valid_nonempty(value: object) -> bool:
    return isinstance(value, str) and bool(value) and "\x00" not in value


def validate_envelope(
    state: DomainState, request: CommandEnvelope, now_ms: int
) -> DomainError | None:
    if request.protocol_version != PROTOCOL_VERSION:
        return _error(
            ErrorCode.UNSUPPORTED_PROTOCOL,
            expected=PROTOCOL_VERSION,
            actual=request.protocol_version,
        )
    if request.domain_id != state.domain_id:
        return _error(
            ErrorCode.DOMAIN_MISMATCH,
            expected=state.domain_id,
            actual=request.domain_id,
        )
    owner = request.principal
    if (
        not _valid_nonempty(owner.client_id)
        or not _valid_nonempty(owner.agent_id)
        or type(owner.session_epoch) is not int
        or owner.session_epoch < 0
    ):
        return _error(ErrorCode.INVALID_PRINCIPAL)
    current_epoch = state.session_epochs.get((owner.client_id, owner.agent_id))
    if current_epoch != owner.session_epoch:
        return _error(
            ErrorCode.STALE_SESSION,
            client_id=owner.client_id,
            agent_id=owner.agent_id,
            expected=current_epoch,
            actual=owner.session_epoch,
        )
    if not _valid_nonempty(request.request_id):
        return _error(ErrorCode.INVALID_REQUEST_ID)
    expected_fingerprint = command_fingerprint(
        protocol_version=request.protocol_version,
        domain_id=request.domain_id,
        principal=request.principal,
        request_id=request.request_id,
        command=request.command,
    )
    if request.fingerprint != expected_fingerprint:
        return _error(ErrorCode.REQUEST_FINGERPRINT_MISMATCH)
    if type(now_ms) is not int or now_ms < state.last_now_ms:
        return _error(
            ErrorCode.CLOCK_REGRESSION,
            previous=state.last_now_ms,
            actual=now_ms,
        )
    return None


def validate_claim(
    state: DomainState, request: CommandEnvelope, command: ClaimCommand
) -> tuple[tuple[ClaimSpec, ...] | None, DomainError | None]:
    if not command.claims:
        return None, _error(ErrorCode.EMPTY_CLAIM_SET)
    if type(command.lease_ttl_ms) is not int or command.lease_ttl_ms <= 0:
        return None, _error(ErrorCode.INVALID_TTL)
    if type(command.wait_timeout_ms) is not int or command.wait_timeout_ms <= 0:
        return None, _error(ErrorCode.INVALID_WAIT_TIMEOUT)
    if any(not isinstance(spec, ClaimSpec) for spec in command.claims):
        return None, _error(ErrorCode.INVALID_RESOURCE, field="claim_spec")
    for spec in command.claims:
        if not isinstance(spec.mode, AccessMode):
            return None, _error(ErrorCode.INVALID_RESOURCE, field="access_mode")
        if not isinstance(spec.resource, ResourceId):
            return None, _error(ErrorCode.INVALID_RESOURCE, field="resource")
        resource = spec.resource
        if (
            not isinstance(resource.domain_id, str)
            or not isinstance(resource.repo_id, str)
            or not isinstance(resource.selector, SelectorKind)
            or not isinstance(resource.segments, tuple)
            or any(not isinstance(segment, str) for segment in resource.segments)
        ):
            return None, _error(ErrorCode.INVALID_RESOURCE, field="typed_shape")

    claims = tuple(sorted(command.claims, key=claim_spec_key))
    for spec in claims:
        resource = spec.resource
        if resource.domain_id != state.domain_id or resource.domain_id != request.domain_id:
            return None, _error(
                ErrorCode.DOMAIN_MISMATCH,
                resource_domain=resource.domain_id,
                expected=state.domain_id,
            )
        policy = state.repo_policies.get(resource.repo_id)
        if policy is None:
            return None, _error(
                ErrorCode.UNKNOWN_REPOSITORY, repo_id=resource.repo_id
            )
        resource_error = validate_resource_id(resource, policy)
        if resource_error is not None:
            return None, resource_error

    for index, left in enumerate(claims):
        for right in claims[index + 1 :]:
            if overlaps(left.resource, right.resource):
                return None, _error(
                    ErrorCode.SELF_OVERLAPPING_CLAIM_SET,
                    left="/".join(left.resource.segments),
                    right="/".join(right.resource.segments),
                )
    return claims, None
