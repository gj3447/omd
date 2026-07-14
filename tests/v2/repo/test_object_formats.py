from __future__ import annotations

import os
import subprocess
from pathlib import Path

import pytest

from omd_server.v2.repo import (
    IntegrateRequest,
    RegisteredRepository,
    RepoSagaService,
    RepositoryRegistry,
    SagaRecord,
    SQLiteRepoSagaStore,
    SagaStatus,
)
from omd_server.v2.resource import (
    AccessMode,
    ClaimSpec,
    RepoPolicy,
    SelectorKind,
    canonicalize_resource,
)

from .conftest import FakeAuthority, RepoFixture, commit_all, git, make_fence, write


def repo_claim(repo_id: str) -> ClaimSpec:
    return ClaimSpec(
        resource=canonicalize_resource(
            domain_id="symposium",
            policy=RepoPolicy(repo_id=repo_id),
            raw_path="src",
            selector=SelectorKind.SUBTREE,
        ),
        mode=AccessMode.WRITE,
    )


def request(
    *,
    repo_id: str,
    source_oid: str,
    read_base_oid: str,
    claims: tuple[ClaimSpec, ...],
    suffix: str,
) -> IntegrateRequest:
    return IntegrateRequest(
        protocol_version=1,
        operation_id=f"op-{suffix}",
        domain_id="symposium",
        client_id="client",
        request_id=f"request-{suffix}",
        repo_id=repo_id,
        claim_id="claim-1",
        fence=make_fence(claims),
        source_oid=source_oid,
        read_base_oid=read_base_oid,
    )


def run(
    *,
    registration: RegisteredRepository,
    state: Path,
    authority: FakeAuthority,
    integrate_request: IntegrateRequest,
) -> SagaRecord:
    return RepoSagaService(
        registry=RepositoryRegistry((registration,)),
        store=SQLiteRepoSagaStore(state / "repo-sagas.db"),
        authority=authority,
    ).integrate(integrate_request)


def init_sha256_repository(path: Path) -> None:
    result = subprocess.run(
        ["git", "init", "--object-format=sha256", os.fspath(path)],
        check=False,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    if result.returncode == 0:
        return
    detail = (result.stdout + result.stderr).lower()
    lacks_object_format = b"object-format" in detail and (
        b"unknown option" in detail or b"unsupported" in detail
    )
    lacks_sha256 = b"sha256" in detail and (
        b"unknown hash" in detail
        or b"unsupported hash" in detail
        or b"invalid object format" in detail
    )
    if lacks_object_format or lacks_sha256:
        pytest.skip("installed Git does not support SHA-256 repositories")
    pytest.fail(
        "Git failed to initialize the SHA-256 acceptance repository: "
        + result.stderr.decode("utf-8", "replace")
    )


def commit_with_raw_index_entry(
    *,
    repo: Path,
    base_oid: str,
    mode: bytes,
    object_oid: str,
    raw_path: bytes,
    index_name: str,
    message: str,
) -> str:
    env = dict(os.environ)
    env["GIT_INDEX_FILE"] = os.fspath(repo.parent / index_name)
    subprocess.run(
        ["git", "-C", os.fspath(repo), "read-tree", base_oid],
        check=True,
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    subprocess.run(
        [
            os.fsencode("git"),
            b"-C",
            os.fsencode(repo),
            b"update-index",
            b"--add",
            b"--cacheinfo",
            mode + b"," + object_oid.encode("ascii") + b"," + raw_path,
        ],
        check=True,
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    tree_oid = subprocess.run(
        ["git", "-C", os.fspath(repo), "write-tree"],
        check=True,
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    ).stdout.decode("ascii").strip()
    return git(
        repo, "commit-tree", tree_oid, "-p", base_oid, "-m", message
    ).stdout.decode("ascii").strip()


def test_sha256_repository_completes_prepare_commit_and_target_cas(
    tmp_path: Path,
) -> None:
    root = tmp_path / "sha256-repo"
    init_sha256_repository(root)
    git(root, "symbolic-ref", "HEAD", "refs/heads/work")
    git(root, "config", "user.name", "OMD SHA-256 Test")
    git(root, "config", "user.email", "omd-sha256@example.invalid")
    write(root, "src/base.txt", "base\n")
    base_oid = commit_all(root, "base")
    git(root, "branch", "integration", base_oid)
    git(root, "checkout", "-b", "feature", base_oid)
    write(root, "src/feature.txt", "sha256 feature\n")
    source_oid = commit_all(root, "feature")
    git(root, "checkout", "work")

    state = tmp_path / "sha256-state"
    registration = RegisteredRepository.inspect(
        repo_id="sha256",
        path=root,
        target_ref="refs/heads/integration",
        state_dir=state,
    )
    claims = (repo_claim("sha256"),)
    result = run(
        registration=registration,
        state=state,
        authority=FakeAuthority(claims),
        integrate_request=request(
            repo_id="sha256",
            source_oid=source_oid,
            read_base_oid=base_oid,
            claims=claims,
            suffix="sha256",
        ),
    )

    assert registration.object_format == "sha256"
    assert registration.oid_length == 64
    assert len(base_oid) == len(source_oid) == 64
    assert result.status is SagaStatus.RECEIPTED
    assert result.candidate_oid is not None
    assert len(result.candidate_oid) == 64
    assert (
        git(root, "rev-parse", "refs/heads/integration").stdout.decode().strip()
        == result.candidate_oid
    )
    parents = git(root, "show", "-s", "--format=%P", result.candidate_oid)
    assert parents.stdout.decode().split() == [base_oid, source_oid]


def test_gitlink_delta_is_rejected_without_target_mutation(
    repo_fixture: RepoFixture,
) -> None:
    source_oid = commit_with_raw_index_entry(
        repo=repo_fixture.root,
        base_oid=repo_fixture.base_oid,
        mode=b"160000",
        object_oid=repo_fixture.base_oid,
        raw_path=b"src/vendor",
        index_name="gitlink.index",
        message="gitlink source",
    )
    claims = (repo_claim("repo"),)
    before = repo_fixture.target_oid

    result = run(
        registration=repo_fixture.registration,
        state=repo_fixture.state,
        authority=FakeAuthority(claims),
        integrate_request=request(
            repo_id="repo",
            source_oid=source_oid,
            read_base_oid=repo_fixture.base_oid,
            claims=claims,
            suffix="gitlink",
        ),
    )

    assert result.status is SagaStatus.POLICY_REJECTED
    assert result.error_detail is not None and "gitlink" in result.error_detail
    assert repo_fixture.target_oid == before


@pytest.mark.skipif(os.name != "posix", reason="raw byte Git paths require POSIX argv")
def test_non_utf8_path_is_rejected_without_target_mutation(
    repo_fixture: RepoFixture,
) -> None:
    blob_oid = subprocess.run(
        ["git", "-C", os.fspath(repo_fixture.root), "hash-object", "-w", "--stdin"],
        input=b"invalid path payload\n",
        check=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    ).stdout.decode("ascii").strip()
    source_oid = commit_with_raw_index_entry(
        repo=repo_fixture.root,
        base_oid=repo_fixture.base_oid,
        mode=b"100644",
        object_oid=blob_oid,
        raw_path=b"src/invalid-\xff.txt",
        index_name="invalid-utf8.index",
        message="invalid utf8 source",
    )
    claims = (repo_claim("repo"),)
    before = repo_fixture.target_oid

    result = run(
        registration=repo_fixture.registration,
        state=repo_fixture.state,
        authority=FakeAuthority(claims),
        integrate_request=request(
            repo_id="repo",
            source_oid=source_oid,
            read_base_oid=repo_fixture.base_oid,
            claims=claims,
            suffix="invalid-utf8",
        ),
    )

    assert result.status is SagaStatus.POLICY_REJECTED
    assert result.error_detail is not None and "UTF-8" in result.error_detail
    assert repo_fixture.target_oid == before
