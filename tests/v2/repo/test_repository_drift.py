from __future__ import annotations

import subprocess
from pathlib import Path

import pytest

from omd_server.v2.repo import (
    IntegrateRequest,
    RegisteredRepository,
    RepoConfigurationError,
    RepoSagaService,
    RepositoryRegistry,
    SQLiteRepoSagaStore,
    SagaStatus,
)
from omd_server.v2.resource import AccessMode, SelectorKind
from omd_server.v2.repo.git import GitPlumbing

from .conftest import FakeAuthority, RepoFixture, claim, git, make_fence


def _request(repo: RepoFixture) -> IntegrateRequest:
    claims = (claim(path="src", mode=AccessMode.WRITE, selector=SelectorKind.SUBTREE),)
    return IntegrateRequest(
        protocol_version=1,
        operation_id="op-drift",
        domain_id="symposium",
        client_id="client",
        request_id="request-drift",
        repo_id="repo",
        claim_id="claim-1",
        fence=make_fence(claims),
        source_oid=repo.source_oid,
        read_base_oid=repo.base_oid,
    )


def _service(repo: RepoFixture, authority: FakeAuthority) -> RepoSagaService:
    return RepoSagaService(
        registry=repo.registry,
        store=SQLiteRepoSagaStore(repo.state / "repo-sagas.db"),
        authority=authority,
    )


def _authority() -> FakeAuthority:
    return FakeAuthority(
        (claim(path="src", mode=AccessMode.WRITE, selector=SelectorKind.SUBTREE),)
    )


def test_live_repository_path_swap_is_quarantined_before_touching_clone(
    repo_fixture: RepoFixture, tmp_path: Path
) -> None:
    authority = _authority()
    engine = _service(repo_fixture, authority)
    assert engine.prepare(_request(repo_fixture)).status is SagaStatus.CANDIDATE_READY

    parked = tmp_path / "parked-original"
    repo_fixture.root.rename(parked)
    subprocess.run(
        ["git", "clone", "--no-local", str(parked), str(repo_fixture.root)],
        check=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    git(repo_fixture.root, "branch", "integration", repo_fixture.base_oid)
    replacement_before = git(
        repo_fixture.root, "rev-parse", "refs/heads/integration"
    ).stdout

    result = engine.publish("op-drift")

    assert result.status is SagaStatus.IN_DOUBT
    assert result.error_code == "LIVE_REPOSITORY_DRIFT"
    assert result.settled is False
    assert authority.settled == []
    assert (
        git(repo_fixture.root, "rev-parse", "refs/heads/integration").stdout
        == replacement_before
    )
    assert (
        git(parked, "rev-parse", "refs/heads/integration").stdout.decode().strip()
        == repo_fixture.base_oid
    )


def test_post_registration_alternates_are_quarantined_before_publication(
    repo_fixture: RepoFixture,
) -> None:
    authority = _authority()
    engine = _service(repo_fixture, authority)
    assert engine.prepare(_request(repo_fixture)).status is SagaStatus.CANDIDATE_READY
    before = repo_fixture.target_oid
    alternates = repo_fixture.registration.object_dir / "info" / "alternates"
    alternates.parent.mkdir(parents=True, exist_ok=True)
    alternates.write_text("/untrusted/object-store\n", encoding="utf-8")

    result = engine.publish("op-drift")

    assert result.status is SagaStatus.IN_DOUBT
    assert result.error_code == "LIVE_REPOSITORY_DRIFT"
    assert result.settled is False
    assert authority.settled == []
    assert repo_fixture.target_oid == before


def test_live_drift_during_input_pinning_holds_reservation(
    repo_fixture: RepoFixture, monkeypatch: pytest.MonkeyPatch
) -> None:
    authority = _authority()

    def drift(*_args: object, **_kwargs: object) -> str:
        raise RepoConfigurationError("registered repository identity changed")

    monkeypatch.setattr(GitPlumbing, "pin", drift)
    result = _service(repo_fixture, authority).integrate(_request(repo_fixture))

    assert result.status is SagaStatus.IN_DOUBT
    assert result.error_code == "LIVE_REPOSITORY_DRIFT"
    assert result.reservation_id == "reservation:op-drift"
    assert result.settled is False
    assert authority.settled == []
    assert repo_fixture.target_oid == repo_fixture.base_oid


def test_nonexclusive_target_refuses_ambiguous_retry_contract(
    repo_fixture: RepoFixture,
) -> None:
    authority = _authority()
    nonexclusive = RegisteredRepository.inspect(
        repo_id="repo",
        path=repo_fixture.root,
        target_ref="refs/heads/integration",
        state_dir=repo_fixture.state,
        omd_exclusive=False,
    )
    engine = RepoSagaService(
        registry=RepositoryRegistry((nonexclusive,)),
        store=SQLiteRepoSagaStore(repo_fixture.state / "repo-sagas.db"),
        authority=authority,
    )

    result = engine.integrate(_request(repo_fixture))

    assert result.status is SagaStatus.IN_DOUBT
    assert result.error_code == "NON_EXCLUSIVE_TARGET"
    assert result.settled is False
    assert authority.settled == []
    assert repo_fixture.target_oid == repo_fixture.base_oid


def test_exclusivity_policy_change_is_repository_identity_drift(
    repo_fixture: RepoFixture,
) -> None:
    authority = _authority()
    assert _service(repo_fixture, authority).prepare(_request(repo_fixture)).status is (
        SagaStatus.CANDIDATE_READY
    )
    nonexclusive = RegisteredRepository.inspect(
        repo_id="repo",
        path=repo_fixture.root,
        target_ref="refs/heads/integration",
        state_dir=repo_fixture.state,
        omd_exclusive=False,
    )
    changed = RepoSagaService(
        registry=RepositoryRegistry((nonexclusive,)),
        store=SQLiteRepoSagaStore(repo_fixture.state / "repo-sagas.db"),
        authority=authority,
    )

    result = changed.publish("op-drift")

    assert result.status is SagaStatus.IN_DOUBT
    assert result.error_code == "LIVE_REPOSITORY_DRIFT"
    assert result.settled is False
    assert repo_fixture.target_oid == repo_fixture.base_oid


class SimulatedCrash(BaseException):
    pass


def test_durable_applied_record_settles_even_if_repository_disappears(
    repo_fixture: RepoFixture, tmp_path: Path
) -> None:
    authority = _authority()

    def crash(point: str) -> None:
        if point == "after_applied_recorded":
            raise SimulatedCrash

    crashing = RepoSagaService(
        registry=repo_fixture.registry,
        store=SQLiteRepoSagaStore(repo_fixture.state / "repo-sagas.db"),
        authority=authority,
        fault_injector=crash,
    )
    with pytest.raises(SimulatedCrash):
        crashing.integrate(_request(repo_fixture))
    applied = crashing.store.load("op-drift")
    assert applied.status is SagaStatus.APPLIED

    parked = tmp_path / "applied-repository"
    repo_fixture.root.rename(parked)
    recovered = _service(repo_fixture, authority).recover_all()

    assert [record.status for record in recovered] == [SagaStatus.RECEIPTED]
    assert recovered[0].candidate_oid == applied.candidate_oid
    assert authority.settled == [("reservation:op-drift", "applied")]
