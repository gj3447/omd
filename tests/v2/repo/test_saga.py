from __future__ import annotations

import subprocess
from pathlib import Path

import pytest

from omd_server.v2.repo import (
    IdempotencyConflict,
    IntegrateRequest,
    RepoSagaService,
    SQLiteRepoSagaStore,
    SagaStatus,
)
from omd_server.v2.resource import AccessMode, SelectorKind

from .conftest import FakeAuthority, RepoFixture, claim, git, make_fence


def request(
    repo: RepoFixture,
    *,
    operation_id: str = "op-1",
    source_oid: str | None = None,
) -> IntegrateRequest:
    claims = (claim(path="src", mode=AccessMode.WRITE, selector=SelectorKind.SUBTREE),)
    return IntegrateRequest(
        protocol_version=1,
        operation_id=operation_id,
        domain_id="symposium",
        client_id="client",
        request_id="request-1",
        repo_id="repo",
        claim_id="claim-1",
        fence=make_fence(claims),
        source_oid=source_oid or repo.source_oid,
        read_base_oid=repo.base_oid,
    )


def service(repo: RepoFixture, authority: FakeAuthority, **kwargs: object) -> RepoSagaService:
    return RepoSagaService(
        registry=repo.registry,
        store=SQLiteRepoSagaStore(repo.state / "repo-sagas.db"),
        authority=authority,
        **kwargs,
    )


def test_clean_merge_publishes_deterministic_two_parent_commit(repo_fixture: RepoFixture) -> None:
    claims = (claim(path="src", mode=AccessMode.WRITE, selector=SelectorKind.SUBTREE),)
    authority = FakeAuthority(claims)
    before_index = (repo_fixture.root / ".git" / "index").read_bytes()
    before_status = git(repo_fixture.root, "status", "--porcelain=v2", "-z").stdout

    result = service(repo_fixture, authority).integrate(request(repo_fixture))

    assert result.status is SagaStatus.RECEIPTED
    assert result.candidate_oid == repo_fixture.target_oid
    parents = (
        git(repo_fixture.root, "show", "-s", "--format=%P", result.candidate_oid)
        .stdout.decode()
        .split()
    )
    assert parents == [repo_fixture.base_oid, repo_fixture.source_oid]
    assert (repo_fixture.root / ".git" / "index").read_bytes() == before_index
    assert git(repo_fixture.root, "status", "--porcelain=v2", "-z").stdout == before_status
    assert authority.settled == [("reservation:op-1", "applied")]


def test_same_request_replays_and_changed_input_is_rejected(repo_fixture: RepoFixture) -> None:
    claims = (claim(path="src", mode=AccessMode.WRITE, selector=SelectorKind.SUBTREE),)
    authority = FakeAuthority(claims)
    engine = service(repo_fixture, authority)
    first = engine.integrate(request(repo_fixture))

    assert engine.integrate(request(repo_fixture)) == first

    other = repo_fixture.make_commit(
        branch="feature-2",
        parent=repo_fixture.base_oid,
        files={"src/allowed-2.txt": "other\n"},
        message="other source",
    )
    with pytest.raises(IdempotencyConflict):
        engine.integrate(request(repo_fixture, operation_id="op-2", source_oid=other))


def test_out_of_write_set_change_is_rejected_without_target_mutation(
    repo_fixture: RepoFixture,
) -> None:
    source = repo_fixture.make_commit(
        branch="outside",
        parent=repo_fixture.base_oid,
        files={"outside.txt": "not authorized\n"},
        message="outside",
    )
    claims = (claim(path="src", mode=AccessMode.WRITE, selector=SelectorKind.SUBTREE),)
    authority = FakeAuthority(claims)
    before = repo_fixture.target_oid

    result = service(repo_fixture, authority).integrate(request(repo_fixture, source_oid=source))

    assert result.status is SagaStatus.WRITESET_REJECTED
    assert repo_fixture.target_oid == before


def test_read_set_change_since_read_base_is_rejected(repo_fixture: RepoFixture) -> None:
    target = repo_fixture.make_commit(
        branch="target-change",
        parent=repo_fixture.base_oid,
        files={"config/settings.txt": "v2\n"},
        message="target changes read dependency",
    )
    git(repo_fixture.root, "update-ref", "refs/heads/integration", target, repo_fixture.base_oid)
    claims = (
        claim(path="src", mode=AccessMode.WRITE, selector=SelectorKind.SUBTREE),
        claim(path="config", mode=AccessMode.READ, selector=SelectorKind.SUBTREE),
    )
    authority = FakeAuthority(claims)

    result = service(repo_fixture, authority).integrate(
        IntegrateRequest(
            protocol_version=1,
            operation_id="op-read",
            domain_id="symposium",
            client_id="client",
            request_id="request-read",
            repo_id="repo",
            claim_id="claim-1",
            fence=make_fence(claims),
            source_oid=repo_fixture.source_oid,
            read_base_oid=repo_fixture.base_oid,
        )
    )

    assert result.status is SagaStatus.READ_STALE
    assert repo_fixture.target_oid == target


def test_conflict_tree_stdout_is_never_committed(repo_fixture: RepoFixture) -> None:
    source = repo_fixture.make_commit(
        branch="conflict-source",
        parent=repo_fixture.base_oid,
        files={"src/base.txt": "source\n"},
        message="source conflict",
    )
    target = repo_fixture.make_commit(
        branch="conflict-target",
        parent=repo_fixture.base_oid,
        files={"src/base.txt": "target\n"},
        message="target conflict",
    )
    git(repo_fixture.root, "update-ref", "refs/heads/integration", target, repo_fixture.base_oid)
    claims = (claim(path="src", mode=AccessMode.WRITE, selector=SelectorKind.SUBTREE),)
    authority = FakeAuthority(claims)

    req = IntegrateRequest(
        protocol_version=1,
        operation_id="op-conflict",
        domain_id="symposium",
        client_id="client",
        request_id="request-conflict",
        repo_id="repo",
        claim_id="claim-1",
        fence=make_fence(claims),
        source_oid=source,
        read_base_oid=repo_fixture.base_oid,
    )
    result = service(repo_fixture, authority).integrate(req)

    assert result.status is SagaStatus.MERGE_CONFLICT
    assert repo_fixture.target_oid == target
    assert result.candidate_oid is None


def test_target_move_after_prepare_is_terminal_ref_stale(repo_fixture: RepoFixture) -> None:
    claims = (claim(path="src", mode=AccessMode.WRITE, selector=SelectorKind.SUBTREE),)
    authority = FakeAuthority(claims)
    engine = service(repo_fixture, authority)
    prepared = engine.prepare(request(repo_fixture))
    assert prepared.status is SagaStatus.CANDIDATE_READY

    moved = repo_fixture.make_commit(
        branch="moved",
        parent=repo_fixture.base_oid,
        files={"other.txt": "moved\n"},
        message="move target",
    )
    git(repo_fixture.root, "update-ref", "refs/heads/integration", moved, repo_fixture.base_oid)

    result = engine.publish("op-1")
    assert result.status is SagaStatus.REF_STALE
    assert repo_fixture.target_oid == moved


class SimulatedCrash(BaseException):
    pass


def test_crash_after_ref_cas_recovers_forward_from_exact_candidate(
    repo_fixture: RepoFixture,
) -> None:
    claims = (claim(path="src", mode=AccessMode.WRITE, selector=SelectorKind.SUBTREE),)
    authority = FakeAuthority(claims)

    def crash(point: str) -> None:
        if point == "after_ref_cas":
            raise SimulatedCrash

    crashing = service(repo_fixture, authority, fault_injector=crash)
    with pytest.raises(SimulatedCrash):
        crashing.integrate(request(repo_fixture))

    interrupted = crashing.store.load("op-1")
    assert interrupted.status is SagaStatus.PUBLISHING
    assert repo_fixture.target_oid == interrupted.candidate_oid

    recovered = service(repo_fixture, authority).recover_all()
    assert [item.status for item in recovered] == [SagaStatus.RECEIPTED]
    assert recovered[0].candidate_oid == repo_fixture.target_oid
    assert authority.settled == [("reservation:op-1", "applied")]


@pytest.mark.parametrize(
    "killpoint",
    [
        "after_ref_cas",
        "after_applied_recorded",
        "after_authority_settle_before_receipt",
    ],
)
def test_same_request_replay_drives_publication_to_one_receipt(
    repo_fixture: RepoFixture, killpoint: str
) -> None:
    claims = (claim(path="src", mode=AccessMode.WRITE, selector=SelectorKind.SUBTREE),)
    authority = FakeAuthority(claims)

    def crash(point: str) -> None:
        if point == killpoint:
            raise SimulatedCrash

    with pytest.raises(SimulatedCrash):
        service(repo_fixture, authority, fault_injector=crash).integrate(
            request(repo_fixture)
        )

    replayed = service(repo_fixture, authority).integrate(request(repo_fixture))

    assert replayed.status is SagaStatus.RECEIPTED
    assert replayed.candidate_oid == repo_fixture.target_oid
    assert authority.settled == [("reservation:op-1", "applied")]


@pytest.mark.parametrize(
    "killpoint",
    ["after_intent", "after_authority_reserve_before_record", "after_reservation"],
)
def test_early_crash_recovers_with_persisted_fence(
    repo_fixture: RepoFixture, killpoint: str
) -> None:
    claims = (claim(path="src", mode=AccessMode.WRITE, selector=SelectorKind.SUBTREE),)
    authority = FakeAuthority(claims)

    def crash(point: str) -> None:
        if point == killpoint:
            raise SimulatedCrash

    with pytest.raises(SimulatedCrash):
        service(repo_fixture, authority, fault_injector=crash).integrate(
            request(repo_fixture)
        )

    recovered = service(repo_fixture, authority).recover_all()
    assert [item.status for item in recovered] == [SagaStatus.RECEIPTED]
    assert repo_fixture.target_oid == recovered[0].candidate_oid


def test_authority_loss_before_publish_fails_closed(repo_fixture: RepoFixture) -> None:
    claims = (claim(path="src", mode=AccessMode.WRITE, selector=SelectorKind.SUBTREE),)
    authority = FakeAuthority(claims)
    engine = service(repo_fixture, authority)
    assert engine.prepare(request(repo_fixture)).status is SagaStatus.CANDIDATE_READY
    before = repo_fixture.target_oid
    authority.active = False

    result = engine.publish("op-1")

    assert result.status is SagaStatus.POLICY_REJECTED
    assert repo_fixture.target_oid == before


def test_authority_loss_after_publishing_is_quarantined(
    repo_fixture: RepoFixture,
) -> None:
    claims = (claim(path="src", mode=AccessMode.WRITE, selector=SelectorKind.SUBTREE),)
    authority = FakeAuthority(claims)

    def crash(point: str) -> None:
        if point == "after_publishing_recorded":
            raise SimulatedCrash

    with pytest.raises(SimulatedCrash):
        service(repo_fixture, authority, fault_injector=crash).integrate(
            request(repo_fixture)
        )
    authority.active = False

    recovered = service(repo_fixture, authority).recover_all()

    assert [item.status for item in recovered] == [SagaStatus.IN_DOUBT]
    assert recovered[0].error_code == "PUBLISH_AUTHORITY_LOST"
    assert recovered[0].settled is False
    assert repo_fixture.target_oid == repo_fixture.base_oid


def test_missing_target_before_publication_is_ref_stale(
    repo_fixture: RepoFixture,
) -> None:
    claims = (claim(path="src", mode=AccessMode.WRITE, selector=SelectorKind.SUBTREE),)
    authority = FakeAuthority(claims)
    engine = service(repo_fixture, authority)
    assert engine.prepare(request(repo_fixture)).status is SagaStatus.CANDIDATE_READY
    git(repo_fixture.root, "update-ref", "-d", "refs/heads/integration")

    result = engine.publish("op-1")

    assert result.status is SagaStatus.REF_STALE
    assert result.error_code == "TARGET_UNAVAILABLE_BEFORE_PUBLICATION"
    assert result.settled is True
    assert authority.settled == [("reservation:op-1", "ref_stale")]


def test_read_only_authority_cannot_publish(repo_fixture: RepoFixture) -> None:
    claims = (claim(path="src", mode=AccessMode.READ, selector=SelectorKind.SUBTREE),)
    authority = FakeAuthority(claims)
    req = IntegrateRequest(
        protocol_version=1,
        operation_id="op-read-only",
        domain_id="symposium",
        client_id="client",
        request_id="request-read-only",
        repo_id="repo",
        claim_id="claim-1",
        fence=make_fence(claims),
        source_oid=repo_fixture.source_oid,
        read_base_oid=repo_fixture.base_oid,
    )

    result = service(repo_fixture, authority).integrate(req)

    assert result.status is SagaStatus.POLICY_REJECTED
    assert repo_fixture.target_oid == repo_fixture.base_oid


def test_target_checked_out_after_prepare_cannot_be_updated(
    repo_fixture: RepoFixture,
) -> None:
    claims = (claim(path="src", mode=AccessMode.WRITE, selector=SelectorKind.SUBTREE),)
    authority = FakeAuthority(claims)
    engine = service(repo_fixture, authority)
    assert engine.prepare(request(repo_fixture)).status is SagaStatus.CANDIDATE_READY
    git(repo_fixture.root, "checkout", "integration")

    result = engine.publish("op-1")

    assert result.status is SagaStatus.IN_DOUBT
    assert result.settled is False
    assert repo_fixture.target_oid == repo_fixture.base_oid


def test_private_pins_survive_immediate_gc_before_publication(
    repo_fixture: RepoFixture,
) -> None:
    claims = (claim(path="src", mode=AccessMode.WRITE, selector=SelectorKind.SUBTREE),)
    authority = FakeAuthority(claims)
    engine = service(repo_fixture, authority)
    prepared = engine.prepare(request(repo_fixture))
    assert prepared.status is SagaStatus.CANDIDATE_READY
    git(repo_fixture.root, "branch", "-D", "feature")
    git(repo_fixture.root, "reflog", "expire", "--expire=now", "--all")
    git(repo_fixture.root, "gc", "--prune=now")

    result = engine.publish("op-1")

    assert result.status is SagaStatus.RECEIPTED
    assert result.candidate_oid == repo_fixture.target_oid


def test_reserve_response_loss_retries_by_operation_without_orphan(
    repo_fixture: RepoFixture,
) -> None:
    claims = (claim(path="src", mode=AccessMode.WRITE, selector=SelectorKind.SUBTREE),)

    class LostResponseAuthority(FakeAuthority):
        lost = False

        def reserve(self, authority_request):
            reservation = super().reserve(authority_request)
            if not self.lost:
                self.lost = True
                raise RuntimeError("reservation response lost")
            return reservation

    authority = LostResponseAuthority(claims)
    engine = service(repo_fixture, authority)
    with pytest.raises(RuntimeError, match="response lost"):
        engine.integrate(request(repo_fixture))
    assert engine.store.load("op-1").status is SagaStatus.INTENT_DURABLE
    assert len(authority.acquired) == 1

    recovered = service(repo_fixture, authority).recover_all()
    assert [item.status for item in recovered] == [SagaStatus.RECEIPTED]
    assert len(authority.acquired) == 1


def test_in_doubt_keeps_mutation_reservation_held(repo_fixture: RepoFixture) -> None:
    claims = (claim(path="src", mode=AccessMode.WRITE, selector=SelectorKind.SUBTREE),)
    authority = FakeAuthority(claims)

    def crash(point: str) -> None:
        if point == "after_publishing_recorded":
            raise SimulatedCrash

    with pytest.raises(SimulatedCrash):
        service(repo_fixture, authority, fault_injector=crash).integrate(
            request(repo_fixture)
        )
    moved = repo_fixture.make_commit(
        branch="uncertain-external",
        parent=repo_fixture.base_oid,
        files={"external.txt": "rewrite\n"},
        message="external rewrite",
    )
    git(repo_fixture.root, "update-ref", "refs/heads/integration", moved, repo_fixture.base_oid)

    recovered = service(repo_fixture, authority).recover_all()
    assert [item.status for item in recovered] == [SagaStatus.IN_DOUBT]
    assert recovered[0].settled is False
    assert authority.settled == []
    assert service(repo_fixture, authority).recover_all() == ()


def test_registry_remap_is_quarantined_before_touching_other_clone(
    repo_fixture: RepoFixture, tmp_path: Path
) -> None:
    claims = (claim(path="src", mode=AccessMode.WRITE, selector=SelectorKind.SUBTREE),)
    authority = FakeAuthority(claims)

    def crash(point: str) -> None:
        if point == "after_intent":
            raise SimulatedCrash

    with pytest.raises(SimulatedCrash):
        service(repo_fixture, authority, fault_injector=crash).integrate(
            request(repo_fixture)
        )

    clone = tmp_path / "other-clone"
    subprocess.run(
        ["git", "clone", "--no-local", str(repo_fixture.root), str(clone)],
        check=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    git(clone, "branch", "integration", repo_fixture.base_oid)
    git(clone, "checkout", "-B", "work", repo_fixture.base_oid)
    from omd_server.v2.repo import RegisteredRepository, RepositoryRegistry

    registration = RegisteredRepository.inspect(
        repo_id="repo",
        path=clone,
        target_ref="refs/heads/integration",
        state_dir=repo_fixture.state / "remapped-runtime",
    )
    remapped = RepoSagaService(
        registry=RepositoryRegistry((registration,)),
        store=SQLiteRepoSagaStore(repo_fixture.state / "repo-sagas.db"),
        authority=authority,
    )
    other_before = git(clone, "rev-parse", "refs/heads/integration").stdout.strip()

    recovered = remapped.recover_all()

    assert [item.status for item in recovered] == [SagaStatus.IN_DOUBT]
    assert recovered[0].settled is False
    assert git(clone, "rev-parse", "refs/heads/integration").stdout.strip() == other_before
    assert repo_fixture.target_oid == repo_fixture.base_oid


def test_logical_store_time_clamps_wall_clock_rollback(repo_fixture: RepoFixture) -> None:
    claims = (claim(path="src", mode=AccessMode.WRITE, selector=SelectorKind.SUBTREE),)
    authority = FakeAuthority(claims)
    future = 2_000_000_000.0

    result = service(repo_fixture, authority, clock=lambda: future).integrate(
        request(repo_fixture)
    )

    assert result.status is SagaStatus.RECEIPTED
    assert result.updated_at_ms >= result.created_at_ms == int(future * 1000)


@pytest.mark.parametrize(
    "killpoint",
    [
        "after_inputs_pinned",
        "after_merge_tree",
        "after_commit_tree",
        "after_candidate_recorded",
        "after_publishing_recorded",
        "before_ref_cas",
        "after_applied_recorded",
        "after_authority_settle_before_receipt",
    ],
)
def test_remaining_killpoints_recover_to_one_receipt(
    repo_fixture: RepoFixture, killpoint: str
) -> None:
    claims = (claim(path="src", mode=AccessMode.WRITE, selector=SelectorKind.SUBTREE),)
    authority = FakeAuthority(claims)

    def crash(point: str) -> None:
        if point == killpoint:
            raise SimulatedCrash

    with pytest.raises(SimulatedCrash):
        service(repo_fixture, authority, fault_injector=crash).integrate(
            request(repo_fixture)
        )
    recovered = service(repo_fixture, authority).recover_all()
    assert [item.status for item in recovered] == [SagaStatus.RECEIPTED]
    candidate = recovered[0].candidate_oid
    assert candidate == repo_fixture.target_oid
    first_parent_count = git(
        repo_fixture.root,
        "rev-list",
        "--first-parent",
        "--count",
        f"{repo_fixture.base_oid}..{candidate}",
    ).stdout.decode().strip()
    assert first_parent_count == "1"
