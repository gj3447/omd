"""Typed, serializable errors for the OMD v2 domain boundary."""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum


class ErrorCode(str, Enum):
    UNSUPPORTED_PROTOCOL = "unsupported_protocol"
    DOMAIN_MISMATCH = "domain_mismatch"
    INVALID_PRINCIPAL = "invalid_principal"
    STALE_SESSION = "stale_session"
    INVALID_REQUEST_ID = "invalid_request_id"
    REQUEST_FINGERPRINT_MISMATCH = "request_fingerprint_mismatch"
    EMPTY_CLAIM_SET = "empty_claim_set"
    INVALID_TTL = "invalid_ttl"
    INVALID_WAIT_TIMEOUT = "invalid_wait_timeout"
    INVALID_RESOURCE = "invalid_resource"
    ABSOLUTE_PATH = "absolute_path"
    PARENT_TRAVERSAL = "parent_traversal"
    CURRENT_DIRECTORY_SEGMENT = "current_directory_segment"
    NON_POSIX_SEPARATOR = "non_posix_separator"
    EMPTY_PATH_SEGMENT = "empty_path_segment"
    UNSUPPORTED_SELECTOR = "unsupported_selector"
    SYMLINK_BOUNDARY = "symlink_boundary"
    UNKNOWN_REPOSITORY = "unknown_repository"
    SELF_OVERLAPPING_CLAIM_SET = "self_overlapping_claim_set"
    IDEMPOTENCY_KEY_REUSE = "idempotency_key_reuse"
    UNKNOWN_CLAIM = "unknown_claim"
    NOT_OWNER = "not_owner"
    STALE_FENCE_VECTOR = "stale_fence_vector"
    CLAIM_NOT_ACTIVE = "claim_not_active"
    CLOCK_REGRESSION = "clock_regression"
    UNSUPPORTED_COMMAND = "unsupported_command"
    INVARIANT_VIOLATION = "invariant_violation"


@dataclass(frozen=True, slots=True)
class DomainError:
    """A stable error variant with deterministic, immutable details."""

    code: ErrorCode
    details: tuple[tuple[str, str], ...] = ()

    @classmethod
    def make(cls, code: ErrorCode, **details: object) -> "DomainError":
        return cls(code, tuple(sorted((key, str(value)) for key, value in details.items())))


class ResourceValidationError(ValueError):
    """Raised only at the trusted raw-path ingress boundary."""

    def __init__(self, error: DomainError):
        self.error = error
        super().__init__(f"{error.code.value}: {dict(error.details)}")


class InvariantViolation(RuntimeError):
    """Signals corrupt state; callers must quarantine rather than guess."""
