from __future__ import annotations

import os
import stat
import subprocess
from pathlib import Path

import pytest

from omd_server.v2.repo import (
    IntegrateRequest,
    RegisteredRepository,
    RepoConfigurationError,
    RepoSagaService,
    SQLiteRepoSagaStore,
    SagaStatus,
)
from omd_server.v2.resource import AccessMode, SelectorKind

from .conftest import FakeAuthority, RepoFixture, claim, commit_all, git, make_fence, write


def run(repo: RepoFixture, authority: FakeAuthority, req: IntegrateRequest):
    return RepoSagaService(
        registry=repo.registry,
        store=SQLiteRepoSagaStore(repo.state / "repo-sagas.db"),
        authority=authority,
    ).integrate(req)


def req(repo: RepoFixture, source: str, claims, suffix: str) -> IntegrateRequest:
    return IntegrateRequest(
        protocol_version=1,
        operation_id=f"op-{suffix}",
        domain_id="symposium",
        client_id="client",
        request_id=f"request-{suffix}",
        repo_id="repo",
        claim_id="claim-1",
        fence=make_fence(claims),
        source_oid=source,
        read_base_oid=repo.base_oid,
    )


def test_repo_local_custom_merge_driver_cannot_execute(repo_fixture: RepoFixture) -> None:
    marker = repo_fixture.root.parent / "merge-driver-ran"
    write(repo_fixture.root, ".gitattributes", "*.txt merge=evil\n")
    write(repo_fixture.root, "src/base.txt", "source\n")
    source = commit_all(repo_fixture.root, "source with attributes")
    target = repo_fixture.make_commit(
        branch="evil-target",
        parent=repo_fixture.base_oid,
        files={"src/base.txt": "target\n"},
        message="target conflict",
    )
    git(repo_fixture.root, "update-ref", "refs/heads/integration", target, repo_fixture.base_oid)
    git(
        repo_fixture.root,
        "config",
        "merge.evil.driver",
        f"sh -c 'touch {marker}; cp %B %A'",
    )
    claims = (claim(path="src", mode=AccessMode.WRITE, selector=SelectorKind.SUBTREE),)

    result = run(repo_fixture, FakeAuthority(claims), req(repo_fixture, source, claims, "driver"))

    assert result.status is SagaStatus.MERGE_CONFLICT
    assert not marker.exists()
    assert repo_fixture.target_oid == target


def test_reference_transaction_hook_cannot_execute(repo_fixture: RepoFixture) -> None:
    marker = repo_fixture.root.parent / "hook-ran"
    hook = repo_fixture.root / ".git" / "hooks" / "reference-transaction"
    hook.write_text(f"#!/bin/sh\ntouch '{marker}'\n", encoding="utf-8")
    hook.chmod(0o755)
    claims = (claim(path="src", mode=AccessMode.WRITE, selector=SelectorKind.SUBTREE),)

    result = run(
        repo_fixture,
        FakeAuthority(claims),
        req(repo_fixture, repo_fixture.source_oid, claims, "hook"),
    )

    assert result.status is SagaStatus.RECEIPTED
    assert not marker.exists()


def test_rename_is_audited_as_delete_plus_add(repo_fixture: RepoFixture) -> None:
    git(repo_fixture.root, "checkout", "-B", "rename-source", repo_fixture.base_oid)
    git(repo_fixture.root, "mv", "src/base.txt", "outside.txt")
    source = commit_all(repo_fixture.root, "rename outside")
    git(repo_fixture.root, "checkout", "work")
    claims = (claim(path="src", mode=AccessMode.WRITE, selector=SelectorKind.SUBTREE),)

    result = run(repo_fixture, FakeAuthority(claims), req(repo_fixture, source, claims, "rename"))

    assert result.status is SagaStatus.WRITESET_REJECTED
    assert repo_fixture.target_oid == repo_fixture.base_oid


def test_symlink_delta_is_rejected(repo_fixture: RepoFixture) -> None:
    git(repo_fixture.root, "checkout", "-B", "symlink-source", repo_fixture.base_oid)
    os.symlink("base.txt", repo_fixture.root / "src" / "link.txt")
    source = commit_all(repo_fixture.root, "add symlink")
    git(repo_fixture.root, "checkout", "work")
    claims = (claim(path="src", mode=AccessMode.WRITE, selector=SelectorKind.SUBTREE),)

    result = run(repo_fixture, FakeAuthority(claims), req(repo_fixture, source, claims, "symlink"))

    assert result.status is SagaStatus.POLICY_REJECTED
    assert repo_fixture.target_oid == repo_fixture.base_oid


def test_non_nfc_git_path_is_rejected(repo_fixture: RepoFixture) -> None:
    nfd = "cafe\u0301.txt"
    env = dict(os.environ)
    env["GIT_INDEX_FILE"] = os.fspath(repo_fixture.root.parent / "nfd.index")
    subprocess.run(
        [
            "git",
            "-c",
            "core.precomposeunicode=false",
            "-C",
            os.fspath(repo_fixture.root),
            "read-tree",
            repo_fixture.base_oid,
        ],
        check=True,
        env=env,
    )
    blob = subprocess.run(
        [
            "git",
            "-c",
            "core.precomposeunicode=false",
            "-C",
            os.fspath(repo_fixture.root),
            "hash-object",
            "-w",
            "--stdin",
        ],
        input=b"nfd\n",
        check=True,
        stdout=subprocess.PIPE,
        env=env,
    ).stdout.decode().strip()
    subprocess.run(
        [
            "git",
            "-c",
            "core.precomposeunicode=false",
            "-C",
            os.fspath(repo_fixture.root),
            "update-index",
            "--add",
            "--cacheinfo",
            f"100644,{blob},src/{nfd}",
        ],
        check=True,
        env=env,
    )
    tree = subprocess.run(
        [
            "git",
            "-c",
            "core.precomposeunicode=false",
            "-C",
            os.fspath(repo_fixture.root),
            "write-tree",
        ],
        check=True,
        stdout=subprocess.PIPE,
        env=env,
    ).stdout.decode().strip()
    source = git(
        repo_fixture.root,
        "commit-tree",
        tree,
        "-p",
        repo_fixture.base_oid,
        "-m",
        "nfd path",
    ).stdout.decode().strip()
    claims = (claim(path="src", mode=AccessMode.WRITE, selector=SelectorKind.SUBTREE),)

    result = run(repo_fixture, FakeAuthority(claims), req(repo_fixture, source, claims, "nfd"))

    assert result.status is SagaStatus.POLICY_REJECTED
    assert repo_fixture.target_oid == repo_fixture.base_oid


def test_path_diagnostics_escape_newline_and_terminal_controls(
    repo_fixture: RepoFixture,
) -> None:
    source = repo_fixture.make_commit(
        branch="diagnostic-source",
        parent=repo_fixture.base_oid,
        files={"outside\n\x1b[31m.txt": "untrusted path\n"},
        message="diagnostic path",
    )
    claims = (claim(path="src", mode=AccessMode.WRITE, selector=SelectorKind.SUBTREE),)

    result = run(
        repo_fixture,
        FakeAuthority(claims),
        req(repo_fixture, source, claims, "diagnostic-path"),
    )

    assert result.status is SagaStatus.POLICY_REJECTED
    assert result.error_detail is not None
    assert "\\n" in result.error_detail and "\\x1b" in result.error_detail
    assert "\n" not in result.error_detail and "\x1b" not in result.error_detail


def test_common_dir_path_preserves_trailing_space(
    repo_fixture: RepoFixture, tmp_path: Path
) -> None:
    sibling = tmp_path / "bare"
    spaced = tmp_path / "bare "
    for target in (sibling, spaced):
        subprocess.run(
            ["git", "clone", "--bare", "--no-local", str(repo_fixture.root), str(target)],
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )

    registered = RegisteredRepository.inspect(
        repo_id="spaced",
        path=spaced,
        target_ref="refs/heads/integration",
        state_dir=tmp_path / "spaced-state",
    )

    assert registered.git_common_dir == spaced.resolve()
    assert registered.git_common_dir != sibling.resolve()


def test_registration_rejects_state_directory_inside_user_repository(
    repo_fixture: RepoFixture,
) -> None:
    nested = repo_fixture.root / "daemon-state"

    with pytest.raises(RepoConfigurationError, match="outside the repository"):
        RegisteredRepository.inspect(
            repo_id="nested-state",
            path=repo_fixture.root,
            target_ref="refs/heads/integration",
            state_dir=nested,
        )

    assert not nested.exists()


def test_registration_rejects_repository_nested_under_state_directory(
    repo_fixture: RepoFixture,
) -> None:
    with pytest.raises(RepoConfigurationError, match="outside the repository"):
        RegisteredRepository.inspect(
            repo_id="parent-state",
            path=repo_fixture.root,
            target_ref="refs/heads/integration",
            state_dir=repo_fixture.root.parent,
        )


def test_daemon_state_directories_and_database_are_owner_only(
    repo_fixture: RepoFixture,
) -> None:
    store = SQLiteRepoSagaStore(repo_fixture.state / "permissions.db")

    for directory in (
        repo_fixture.registration.state_dir,
        repo_fixture.registration.state_dir / "home",
        repo_fixture.registration.state_dir / "execution",
    ):
        assert stat.S_IMODE(directory.stat().st_mode) == 0o700
    assert stat.S_IMODE(store.path.stat().st_mode) == 0o600


def test_preplanted_execution_config_is_rejected(repo_fixture: RepoFixture) -> None:
    execution = repo_fixture.registration.execution_git_dir
    execution.parent.mkdir(parents=True, exist_ok=True)
    subprocess.run(
        ["git", "init", "--bare", str(execution)],
        check=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    subprocess.run(
        [
            "git",
            f"--git-dir={execution}",
            "config",
            "merge.evil.driver",
            "sh -c 'touch /tmp/omd-should-not-run'",
        ],
        check=True,
    )
    claims = (claim(path="src", mode=AccessMode.WRITE, selector=SelectorKind.SUBTREE),)

    with pytest.raises(RepoConfigurationError, match="forbidden keys"):
        run(
            repo_fixture,
            FakeAuthority(claims),
            req(repo_fixture, repo_fixture.source_oid, claims, "preplanted"),
        )

    assert repo_fixture.target_oid == repo_fixture.base_oid


def test_preplanted_execution_attributes_fifo_is_rejected_without_reading(
    repo_fixture: RepoFixture,
) -> None:
    attributes = repo_fixture.registration.execution_git_dir / "info" / "attributes"
    attributes.parent.mkdir(parents=True)
    os.mkfifo(attributes)
    claims = (claim(path="src", mode=AccessMode.WRITE, selector=SelectorKind.SUBTREE),)

    with pytest.raises(RepoConfigurationError, match="merge attributes"):
        run(
            repo_fixture,
            FakeAuthority(claims),
            req(repo_fixture, repo_fixture.source_oid, claims, "attributes-fifo"),
        )

    assert repo_fixture.target_oid == repo_fixture.base_oid


def test_global_union_attribute_cannot_hide_content_conflict(
    repo_fixture: RepoFixture,
) -> None:
    attributes = (
        repo_fixture.registration.state_dir / "home" / ".config" / "git" / "attributes"
    )
    attributes.parent.mkdir(parents=True, exist_ok=True)
    attributes.write_text("*.txt merge=union\n", encoding="utf-8")
    source = repo_fixture.make_commit(
        branch="global-attr-source",
        parent=repo_fixture.base_oid,
        files={"src/base.txt": "source\n"},
        message="source conflict",
    )
    target = repo_fixture.make_commit(
        branch="global-attr-target",
        parent=repo_fixture.base_oid,
        files={"src/base.txt": "target\n"},
        message="target conflict",
    )
    git(repo_fixture.root, "update-ref", "refs/heads/integration", target, repo_fixture.base_oid)
    claims = (claim(path="src", mode=AccessMode.WRITE, selector=SelectorKind.SUBTREE),)

    result = run(
        repo_fixture,
        FakeAuthority(claims),
        req(repo_fixture, source, claims, "global-attributes"),
    )

    assert result.status is SagaStatus.MERGE_CONFLICT
    assert repo_fixture.target_oid == target
