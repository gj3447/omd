"""Canonical repository resource identity and overlap semantics.

The v1 engine compared permissive glob strings after ``lstrip('./')``.  V2
instead admits a deliberately small selector grammar and makes repository and
coordination-domain identity part of every resource.
"""

from __future__ import annotations

import unicodedata
from dataclasses import dataclass
from enum import Enum
from typing import Literal

from .errors import DomainError, ErrorCode, ResourceValidationError


class CaseMode(str, Enum):
    SENSITIVE = "sensitive"
    INSENSITIVE = "insensitive"


class SelectorKind(str, Enum):
    EXACT = "exact"
    SUBTREE = "subtree"


class AccessMode(str, Enum):
    READ = "read"
    WRITE = "write"


def _raise(code: ErrorCode, **details: object) -> None:
    raise ResourceValidationError(DomainError.make(code, **details))


def _identifier(value: str, field: str) -> str:
    if not isinstance(value, str) or not value or "\x00" in value:
        _raise(ErrorCode.INVALID_RESOURCE, field=field)
    return value


def _normalize_segment(segment: str, case_mode: CaseMode) -> str:
    normalized = unicodedata.normalize("NFC", segment)
    if case_mode is CaseMode.INSENSITIVE:
        normalized = normalized.casefold()
    return unicodedata.normalize("NFC", normalized)


@dataclass(frozen=True, slots=True)
class RepoPolicy:
    """Stable repository identity plus path comparison policy.

    ``forbidden_symlink_prefixes`` are registry facts, not paths resolved by
    the kernel.  The runtime populates them after inspecting the repository.
    """

    repo_id: str
    case_mode: CaseMode = CaseMode.SENSITIVE
    unicode_form: Literal["NFC"] = "NFC"
    forbidden_symlink_prefixes: tuple[tuple[str, ...], ...] = ()

    def __post_init__(self) -> None:
        _identifier(self.repo_id, "repo_id")
        if self.unicode_form != "NFC":
            _raise(ErrorCode.INVALID_RESOURCE, field="unicode_form")
        canonical_prefixes: list[tuple[str, ...]] = []
        for prefix in self.forbidden_symlink_prefixes:
            if not prefix:
                _raise(ErrorCode.INVALID_RESOURCE, field="symlink_prefix")
            canonical: list[str] = []
            for segment in prefix:
                if (
                    not isinstance(segment, str)
                    or not segment
                    or segment in {".", ".."}
                    or "/" in segment
                    or "\\" in segment
                    or "\x00" in segment
                ):
                    _raise(ErrorCode.INVALID_RESOURCE, field="symlink_prefix")
                canonical.append(_normalize_segment(segment, self.case_mode))
            canonical_prefixes.append(tuple(canonical))
        object.__setattr__(
            self, "forbidden_symlink_prefixes", tuple(sorted(canonical_prefixes))
        )


@dataclass(frozen=True, slots=True)
class ResourceId:
    domain_id: str
    repo_id: str
    segments: tuple[str, ...]
    selector: SelectorKind

    def __post_init__(self) -> None:
        # Keep the command graph deeply immutable even when a caller bypasses
        # annotations and passes a list.
        object.__setattr__(self, "segments", tuple(self.segments))


@dataclass(frozen=True, slots=True)
class ClaimSpec:
    resource: ResourceId
    mode: AccessMode


def canonicalize_resource(
    *,
    domain_id: str,
    policy: RepoPolicy,
    raw_path: str,
    selector: SelectorKind,
) -> ResourceId:
    """Convert one lexical POSIX repo-relative path into a ResourceId.

    Filesystem resolution is intentionally absent.  It would make decisions
    depend on current disk state and could silently cross a symlink boundary.
    """

    _identifier(domain_id, "domain_id")
    if not isinstance(selector, SelectorKind):
        _raise(ErrorCode.UNSUPPORTED_SELECTOR, selector=selector)
    if not isinstance(raw_path, str) or not raw_path or "\x00" in raw_path:
        _raise(ErrorCode.INVALID_RESOURCE, field="raw_path")
    if raw_path.startswith("/"):
        _raise(ErrorCode.ABSOLUTE_PATH, path=raw_path)
    if "\\" in raw_path:
        _raise(ErrorCode.NON_POSIX_SEPARATOR, path=raw_path)
    if any(marker in raw_path for marker in ("*", "?", "[")):
        _raise(ErrorCode.UNSUPPORTED_SELECTOR, path=raw_path)

    raw_segments = raw_path.split("/")
    if any(segment == "" for segment in raw_segments):
        _raise(ErrorCode.EMPTY_PATH_SEGMENT, path=raw_path)
    if any(segment == ".." for segment in raw_segments):
        _raise(ErrorCode.PARENT_TRAVERSAL, path=raw_path)
    if any(segment == "." for segment in raw_segments):
        _raise(ErrorCode.CURRENT_DIRECTORY_SEGMENT, path=raw_path)

    segments = tuple(
        _normalize_segment(segment, policy.case_mode) for segment in raw_segments
    )
    for prefix in policy.forbidden_symlink_prefixes:
        if segments[: len(prefix)] == prefix:
            _raise(
                ErrorCode.SYMLINK_BOUNDARY,
                path=raw_path,
                prefix="/".join(prefix),
            )

    return ResourceId(
        domain_id=domain_id,
        repo_id=policy.repo_id,
        segments=segments,
        selector=selector,
    )


def validate_resource_id(resource: ResourceId, policy: RepoPolicy) -> DomainError | None:
    """Fail closed if a caller bypassed ``canonicalize_resource``."""

    if (
        not isinstance(resource, ResourceId)
        or not isinstance(resource.domain_id, str)
        or not isinstance(resource.repo_id, str)
        or not isinstance(resource.selector, SelectorKind)
        or not isinstance(resource.segments, tuple)
        or any(not isinstance(segment, str) for segment in resource.segments)
    ):
        return DomainError.make(ErrorCode.INVALID_RESOURCE, field="typed_shape")
    if resource.repo_id != policy.repo_id:
        return DomainError.make(
            ErrorCode.UNKNOWN_REPOSITORY, repo_id=resource.repo_id
        )
    if not resource.segments:
        return DomainError.make(ErrorCode.INVALID_RESOURCE, field="segments")
    try:
        canonical = canonicalize_resource(
            domain_id=resource.domain_id,
            policy=policy,
            raw_path="/".join(resource.segments),
            selector=resource.selector,
        )
    except ResourceValidationError as exc:
        return exc.error
    if canonical != resource:
        return DomainError.make(ErrorCode.INVALID_RESOURCE, field="noncanonical")
    return None


def resource_key(resource: ResourceId) -> tuple[str, str, tuple[str, ...], str]:
    return (
        resource.domain_id,
        resource.repo_id,
        resource.segments,
        resource.selector.value,
    )


def claim_spec_key(spec: ClaimSpec) -> tuple[object, ...]:
    return (*resource_key(spec.resource), spec.mode.value)


def _is_prefix(prefix: tuple[str, ...], value: tuple[str, ...]) -> bool:
    return len(prefix) <= len(value) and value[: len(prefix)] == prefix


def overlaps(left: ResourceId, right: ResourceId) -> bool:
    if left.domain_id != right.domain_id or left.repo_id != right.repo_id:
        return False
    if left.selector is SelectorKind.EXACT and right.selector is SelectorKind.EXACT:
        return left.segments == right.segments
    if left.selector is SelectorKind.SUBTREE and right.selector is SelectorKind.EXACT:
        return _is_prefix(left.segments, right.segments)
    if left.selector is SelectorKind.EXACT and right.selector is SelectorKind.SUBTREE:
        return _is_prefix(right.segments, left.segments)
    return _is_prefix(left.segments, right.segments) or _is_prefix(
        right.segments, left.segments
    )


def claim_sets_conflict(left: tuple[ClaimSpec, ...], right: tuple[ClaimSpec, ...]) -> bool:
    return any(
        overlaps(a.resource, b.resource)
        and (a.mode is AccessMode.WRITE or b.mode is AccessMode.WRITE)
        for a in left
        for b in right
    )
