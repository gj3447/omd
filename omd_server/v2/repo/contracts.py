"""Immutable contracts for the worktree-free Git publication saga."""

from __future__ import annotations

import hashlib
import json
import os
import re
import stat
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import Protocol

from ..model import FenceVector
from ..resource import ClaimSpec
from .errors import RepoConfigurationError


SAFE_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")


class SagaStatus(str, Enum):
    INTENT_DURABLE = "intent_durable"
    RESERVED = "reserved"
    INPUTS_PINNED = "inputs_pinned"
    CANDIDATE_READY = "candidate_ready"
    PUBLISHING = "publishing"
    APPLIED = "applied"
    RECEIPTED = "receipted"
    MERGE_CONFLICT = "merge_conflict"
    POLICY_REJECTED = "policy_rejected"
    WRITESET_REJECTED = "writeset_rejected"
    READ_STALE = "read_stale"
    REF_STALE = "ref_stale"
    IN_DOUBT = "in_doubt"

    @property
    def terminal(self) -> bool:
        return self in {
            SagaStatus.RECEIPTED,
            SagaStatus.MERGE_CONFLICT,
            SagaStatus.POLICY_REJECTED,
            SagaStatus.WRITESET_REJECTED,
            SagaStatus.READ_STALE,
            SagaStatus.REF_STALE,
            SagaStatus.IN_DOUBT,
        }

    @property
    def automatically_settleable(self) -> bool:
        return self.terminal and self not in {
            SagaStatus.IN_DOUBT,
            SagaStatus.RECEIPTED,
        }


@dataclass(frozen=True, slots=True)
class IntegrateRequest:
    protocol_version: int
    operation_id: str
    domain_id: str
    client_id: str
    request_id: str
    repo_id: str
    claim_id: str
    fence: FenceVector
    source_oid: str
    read_base_oid: str


def request_fingerprint(request: IntegrateRequest) -> str:
    entries = sorted(
        (
            {
                "domain_id": item.resource.domain_id,
                "repo_id": item.resource.repo_id,
                "segments": list(item.resource.segments),
                "selector": item.resource.selector.value,
                "epoch": item.grant_epoch,
            }
            for item in request.fence.entries
        ),
        key=lambda item: json.dumps(item, sort_keys=True, separators=(",", ":")),
    )
    payload = {
        "protocol_version": request.protocol_version,
        "operation_id": request.operation_id,
        "domain_id": request.domain_id,
        "client_id": request.client_id,
        "request_id": request.request_id,
        "repo_id": request.repo_id,
        "claim_id": request.claim_id,
        "fence": {
            "claim_id": request.fence.claim_id,
            "owner": [
                request.fence.owner.client_id,
                request.fence.owner.agent_id,
                request.fence.owner.session_epoch,
            ],
            "entries": entries,
            "digest": request.fence.vector_digest,
        },
        "source_oid": request.source_oid,
        "read_base_oid": request.read_base_oid,
    }
    raw = json.dumps(
        payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False
    ).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


@dataclass(frozen=True, slots=True)
class CommitMetadata:
    author_name: str
    author_email: str
    author_date: str
    committer_name: str
    committer_email: str
    committer_date: str
    message: str


def deterministic_commit_metadata(
    operation_id: str, source_oid: str, target_oid: str, created_at_ms: int
) -> CommitMetadata:
    timestamp = f"{created_at_ms // 1000} +0000"
    return CommitMetadata(
        author_name="OMD Repo Daemon",
        author_email="repo-daemon@omd.invalid",
        author_date=timestamp,
        committer_name="OMD Repo Daemon",
        committer_email="repo-daemon@omd.invalid",
        committer_date=timestamp,
        message=(
            f"OMD integration {operation_id}\n\n"
            f"OMD-Operation: {operation_id}\n"
            f"OMD-Source: {source_oid}\n"
            f"OMD-Expected-Target: {target_oid}\n"
        ),
    )


@dataclass(frozen=True, slots=True)
class AuthorityRequest:
    operation_id: str
    domain_id: str
    repo_id: str
    claim_id: str
    fence: FenceVector


@dataclass(frozen=True, slots=True)
class MutationReservation:
    """Durable authority handed from the lease domain to one system saga.

    Implementations must keep conflicting grants blocked until ``settle`` is
    durably consumed. ``verify`` is not a fresh lease check: it loads that
    already-pinned authority.
    """

    reservation_id: str
    operation_id: str
    domain_id: str
    repo_id: str
    claim_id: str
    fence_digest: str
    claims: tuple[ClaimSpec, ...]

    def __post_init__(self) -> None:
        object.__setattr__(self, "claims", tuple(self.claims))


def reservation_claims_digest(claims: tuple[ClaimSpec, ...]) -> str:
    payload = sorted(
        (
            {
                "domain_id": item.resource.domain_id,
                "repo_id": item.resource.repo_id,
                "segments": list(item.resource.segments),
                "selector": item.resource.selector.value,
                "mode": item.mode.value,
            }
            for item in claims
        ),
        key=lambda item: json.dumps(item, sort_keys=True, separators=(",", ":")),
    )
    raw = json.dumps(
        payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False
    ).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


class MutationAuthority(Protocol):
    """Durable handoff port.

    ``reserve`` is reserve-or-lookup by operation ID. ``settle`` is idempotent
    by ``(reservation_id, outcome)`` and must return success after a lost prior
    response. Definite policy refusal raises ``AuthorityRejected``; transport
    uncertainty uses another exception so the saga remains retryable.
    """

    def reserve(self, request: AuthorityRequest) -> MutationReservation: ...

    def verify(self, reservation_id: str) -> MutationReservation: ...

    def settle(self, reservation_id: str, outcome: str) -> None: ...


@dataclass(frozen=True, slots=True)
class RegisteredRepository:
    repo_id: str
    source_path: Path
    git_common_dir: Path
    object_dir: Path
    target_ref: str
    state_dir: Path
    execution_git_dir: Path
    hooks_dir: Path
    object_format: str
    oid_length: int
    git_binary: Path
    git_version: tuple[int, int, int]
    identity_digest: str
    omd_exclusive: bool

    @classmethod
    def inspect(
        cls,
        *,
        repo_id: str,
        path: Path,
        target_ref: str,
        state_dir: Path,
        omd_exclusive: bool = True,
    ) -> "RegisteredRepository":
        from .repository import inspect_repository

        return inspect_repository(
            repo_id=repo_id,
            path=path,
            target_ref=target_ref,
            state_dir=state_dir,
            omd_exclusive=omd_exclusive,
        )


class RepositoryRegistry:
    def __init__(self, repositories: tuple[RegisteredRepository, ...]):
        entries: dict[str, RegisteredRepository] = {}
        common_dirs: dict[Path, str] = {}
        for repository in repositories:
            if repository.repo_id in entries:
                raise RepoConfigurationError(
                    f"duplicate repository id: {repository.repo_id}"
                )
            owner = common_dirs.get(repository.git_common_dir)
            if owner is not None:
                raise RepoConfigurationError(
                    f"Git common dir is already registered as {owner}"
                )
            entries[repository.repo_id] = repository
            common_dirs[repository.git_common_dir] = repository.repo_id
        self._entries = entries

    def get(self, repo_id: str) -> RegisteredRepository:
        try:
            return self._entries[repo_id]
        except KeyError as exc:
            raise RepoConfigurationError(f"unknown repository: {repo_id}") from exc


@dataclass(frozen=True, slots=True)
class DeltaEntry:
    path: str
    old_mode: str
    new_mode: str
    status: str


@dataclass(frozen=True, slots=True)
class SagaRecord:
    operation_id: str
    domain_id: str
    client_id: str
    request_id: str
    fingerprint: str
    repo_id: str
    repository_identity: str
    claim_id: str
    fence: FenceVector
    fence_digest: str
    source_oid: str
    read_base_oid: str
    target_ref: str
    expected_target_oid: str
    metadata: CommitMetadata
    status: SagaStatus
    revision: int
    reservation_id: str | None = None
    authority_claims_digest: str | None = None
    tree_oid: str | None = None
    candidate_oid: str | None = None
    receipt_kind: str | None = None
    error_code: str | None = None
    error_detail: str | None = None
    settled: bool = False
    created_at_ms: int = 0
    updated_at_ms: int = 0


def require_safe_id(value: str, field: str) -> None:
    if not isinstance(value, str) or SAFE_ID.fullmatch(value) is None:
        raise RepoConfigurationError(f"invalid {field}")


def ensure_directory(path: Path) -> Path:
    resolved = Path(os.path.realpath(path))
    resolved.mkdir(mode=0o700, parents=True, exist_ok=True)
    metadata = resolved.stat()
    if (
        not stat.S_ISDIR(metadata.st_mode)
        or metadata.st_uid != os.getuid()
        or metadata.st_mode & (stat.S_IRWXG | stat.S_IRWXO)
    ):
        raise RepoConfigurationError(
            "repo daemon state directories must be owner-only"
        )
    return resolved
