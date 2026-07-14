from __future__ import annotations

import os
import subprocess
from dataclasses import dataclass, field
from pathlib import Path

import pytest

from omd_server.v2.fencing import fence_digest
from omd_server.v2.model import FenceEntry, FenceVector, Principal
from omd_server.v2.repo import (
    AuthorityRejected,
    AuthorityRequest,
    MutationAuthority,
    MutationReservation,
    RegisteredRepository,
    RepositoryRegistry,
)
from omd_server.v2.resource import (
    AccessMode,
    ClaimSpec,
    RepoPolicy,
    SelectorKind,
    canonicalize_resource,
)


def git(repo: Path, *args: str, check: bool = True) -> subprocess.CompletedProcess[bytes]:
    return subprocess.run(
        ["git", "-C", os.fspath(repo), *args],
        check=check,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )


def write(repo: Path, relative: str, content: str) -> None:
    path = repo / relative
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def commit_all(repo: Path, message: str) -> str:
    git(repo, "add", "-A")
    git(repo, "commit", "-m", message)
    return git(repo, "rev-parse", "HEAD").stdout.decode().strip()


@dataclass(slots=True)
class RepoFixture:
    root: Path
    state: Path
    registry: RepositoryRegistry
    registration: RegisteredRepository
    base_oid: str
    source_oid: str

    @property
    def target_oid(self) -> str:
        return git(self.root, "rev-parse", "refs/heads/integration").stdout.decode().strip()

    def make_commit(
        self,
        *,
        branch: str,
        parent: str,
        files: dict[str, str],
        message: str,
    ) -> str:
        git(self.root, "checkout", "-B", branch, parent)
        for path, content in files.items():
            write(self.root, path, content)
        oid = commit_all(self.root, message)
        git(self.root, "checkout", "work")
        return oid


@pytest.fixture
def repo_fixture(tmp_path: Path) -> RepoFixture:
    root = tmp_path / "repo"
    root.mkdir()
    git(root, "init", "--initial-branch=work")
    git(root, "config", "user.name", "OMD Test")
    git(root, "config", "user.email", "omd@example.invalid")
    write(root, "src/base.txt", "base\n")
    write(root, "config/settings.txt", "v1\n")
    base = commit_all(root, "base")
    git(root, "branch", "integration", base)

    git(root, "checkout", "-b", "feature", base)
    write(root, "src/allowed.txt", "from feature\n")
    source = commit_all(root, "feature")
    git(root, "checkout", "work")

    state = tmp_path / "state"
    registration = RegisteredRepository.inspect(
        repo_id="repo",
        path=root,
        target_ref="refs/heads/integration",
        state_dir=state,
    )
    registry = RepositoryRegistry((registration,))
    return RepoFixture(root, state, registry, registration, base, source)


def claim(
    *,
    path: str,
    mode: AccessMode,
    selector: SelectorKind = SelectorKind.EXACT,
    domain_id: str = "symposium",
) -> ClaimSpec:
    return ClaimSpec(
        resource=canonicalize_resource(
            domain_id=domain_id,
            policy=RepoPolicy(repo_id="repo"),
            raw_path=path,
            selector=selector,
        ),
        mode=mode,
    )


def make_fence(claims: tuple[ClaimSpec, ...]) -> FenceVector:
    principal = Principal("client", "agent", 1)
    entries = tuple(FenceEntry(item.resource, 7) for item in claims)
    return FenceVector(
        claim_id="claim-1",
        owner=principal,
        entries=entries,
        vector_digest=fence_digest("claim-1", principal, entries),
    )


@dataclass(slots=True)
class FakeAuthority(MutationAuthority):
    claims: tuple[ClaimSpec, ...]
    active: bool = True
    acquired: dict[str, MutationReservation] = field(default_factory=dict)
    settled: list[tuple[str, str]] = field(default_factory=list)

    def reserve(self, request: AuthorityRequest) -> MutationReservation:
        if not self.active:
            raise AuthorityRejected("authority is not active")
        existing = self.acquired.get(request.operation_id)
        if existing is not None:
            return existing
        reservation = MutationReservation(
            reservation_id=f"reservation:{request.operation_id}",
            operation_id=request.operation_id,
            domain_id=request.domain_id,
            repo_id=request.repo_id,
            claim_id=request.claim_id,
            fence_digest=request.fence.vector_digest,
            claims=self.claims,
        )
        self.acquired[request.operation_id] = reservation
        return reservation

    def verify(self, reservation_id: str) -> MutationReservation:
        if not self.active:
            raise AuthorityRejected("authority is not active")
        for reservation in self.acquired.values():
            if reservation.reservation_id == reservation_id:
                return reservation
        raise RuntimeError("unknown reservation")

    def settle(self, reservation_id: str, outcome: str) -> None:
        if not any(
            item.reservation_id == reservation_id for item in self.acquired.values()
        ):
            raise RuntimeError("unknown reservation")
        marker = (reservation_id, outcome)
        if marker not in self.settled:
            self.settled.append(marker)
