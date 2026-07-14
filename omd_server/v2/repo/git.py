"""Bounded Git plumbing adapter with a sanitized candidate-build environment."""

from __future__ import annotations

import os
import re
import stat
import unicodedata
from pathlib import Path

from .contracts import (
    CommitMetadata,
    DeltaEntry,
    RegisteredRepository,
    require_safe_id,
)
from .errors import (
    GitExecutionError,
    MergeConflict,
    PathPolicyError,
    RepoConfigurationError,
)
from .git_process import GitResult, base_env, checked, run_bounded
from .repository import assert_live_repository, target_checked_out


HEX_OID = re.compile(r"^[0-9a-f]+$")
RAW_HEADER = re.compile(
    rb"^:([0-7]{6}) ([0-7]{6}) ([0-9a-f]+) ([0-9a-f]+) ([A-Z])$"
)
EXECUTION_CONFIG_KEYS = {
    "core.repositoryformatversion",
    "core.filemode",
    "core.bare",
    "core.ignorecase",
    "core.precomposeunicode",
    "extensions.objectformat",
}


class GitPlumbing:
    def __init__(
        self,
        repository: RegisteredRepository,
        *,
        timeout_s: float = 30.0,
        output_limit: int = 2 * 1024 * 1024,
    ):
        self.repository = repository
        self.timeout_s = timeout_s
        self.output_limit = output_limit
        assert_live_repository(repository)
        self._env = base_env(repository.state_dir / "home")
        self._execution_ready = False
        self._prepare_execution_repo()

    def _run(
        self,
        args: list[str],
        *,
        execution_repo: bool,
        extra_env: dict[str, str] | None = None,
    ) -> GitResult:
        assert_live_repository(self.repository)
        env = dict(self._env)
        if extra_env:
            env.update(extra_env)
        git_dir = (
            self.repository.execution_git_dir
            if execution_repo
            else self.repository.git_common_dir
        )
        if execution_repo:
            if self._execution_ready:
                self._verify_execution_repo()
            env["GIT_OBJECT_DIRECTORY"] = os.fspath(self.repository.object_dir)
        argv = [
            os.fspath(self.repository.git_binary),
            f"--git-dir={git_dir}",
            "-c",
            f"core.hooksPath={self.repository.hooks_dir}",
            "-c",
            f"core.attributesFile={os.devnull}",
            "-c",
            "core.fsync=all",
            "-c",
            "core.fsyncMethod=fsync",
            *args,
        ]
        return run_bounded(
            argv,
            cwd=self.repository.state_dir,
            env=env,
            timeout_s=self.timeout_s,
            output_limit=self.output_limit,
        )

    def _prepare_execution_repo(self) -> None:
        target = self.repository.execution_git_dir
        if target.is_symlink():
            raise RepoConfigurationError("execution Git dir must not be a symlink")
        if not target.exists():
            template = self.repository.state_dir / "empty-template"
            if template.is_symlink():
                raise RepoConfigurationError("execution template must not be a symlink")
            template.mkdir(mode=0o700, parents=True, exist_ok=True)
            if any(template.iterdir()):
                raise RepoConfigurationError("execution template must be empty")
            result = run_bounded(
                [
                    os.fspath(self.repository.git_binary),
                    "init",
                    "--bare",
                    f"--object-format={self.repository.object_format}",
                    f"--template={template}",
                    os.fspath(target),
                ],
                cwd=self.repository.state_dir,
                env=self._env,
                timeout_s=self.timeout_s,
                output_limit=self.output_limit,
            )
            checked(result, "execution repository initialization")
        self._ensure_controlled_attributes()
        self._verify_execution_repo()
        self._execution_ready = True
        bare = checked(
            self._run(["rev-parse", "--is-bare-repository"], execution_repo=True),
            "execution repository verification",
        )
        if bare.strip() != b"true":
            raise RepoConfigurationError("execution Git dir is not bare")
        fmt = checked(
            self._run(["rev-parse", "--show-object-format"], execution_repo=True),
            "execution object-format verification",
        ).decode("ascii").strip()
        if fmt != self.repository.object_format:
            raise RepoConfigurationError("execution object format changed")

    def _verify_execution_repo(self) -> None:
        target = self.repository.execution_git_dir
        if target.is_symlink() or not target.is_dir():
            raise RepoConfigurationError("execution Git dir is not a real directory")
        if target.stat().st_uid != os.getuid():
            raise RepoConfigurationError("execution Git dir owner changed")
        config = target / "config"
        config_stat = config.lstat()
        if (
            not stat.S_ISREG(config_stat.st_mode)
            or config_stat.st_uid != os.getuid()
            or config_stat.st_mode & (stat.S_IWGRP | stat.S_IWOTH)
        ):
            raise RepoConfigurationError("execution Git config is not trusted")
        listed = checked(
            run_bounded(
                [
                    os.fspath(self.repository.git_binary),
                    "config",
                    "--file",
                    os.fspath(config),
                    "--null",
                    "--name-only",
                    "--list",
                ],
                cwd=self.repository.state_dir,
                env=self._env,
                timeout_s=self.timeout_s,
                output_limit=self.output_limit,
            ),
            "execution config inspection",
        )
        keys = {item.decode("ascii", "strict") for item in listed.split(b"\0") if item}
        unexpected = keys - EXECUTION_CONFIG_KEYS
        if unexpected:
            raise RepoConfigurationError(
                "execution Git config contains forbidden keys: "
                + ", ".join(sorted(unexpected))
            )
        attributes = target / "info" / "attributes"
        attributes_stat = attributes.lstat()
        if (
            not stat.S_ISREG(attributes_stat.st_mode)
            or attributes_stat.st_uid != os.getuid()
            or attributes_stat.st_mode & (stat.S_IWGRP | stat.S_IWOTH)
            or attributes.read_bytes() != b"* merge\n"
        ):
            raise RepoConfigurationError("execution merge attributes are not controlled")

    def _ensure_controlled_attributes(self) -> None:
        info = self.repository.execution_git_dir / "info"
        if info.is_symlink():
            raise RepoConfigurationError("execution info dir must not be a symlink")
        info.mkdir(mode=0o700, parents=True, exist_ok=True)
        attributes = info / "attributes"
        try:
            attributes_stat = attributes.lstat()
        except FileNotFoundError:
            attributes_stat = None
        if attributes_stat is not None:
            if (
                not stat.S_ISREG(attributes_stat.st_mode)
                or attributes_stat.st_uid != os.getuid()
                or attributes_stat.st_mode & (stat.S_IWGRP | stat.S_IWOTH)
                or attributes.read_bytes() != b"* merge\n"
            ):
                raise RepoConfigurationError(
                    "pre-existing execution merge attributes are forbidden"
                )
            return
        descriptor = os.open(
            attributes,
            os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0),
            0o600,
        )
        try:
            os.write(descriptor, b"* merge\n")
            os.fsync(descriptor)
        finally:
            os.close(descriptor)

    def _validate_oid(self, oid: str) -> None:
        if (
            not isinstance(oid, str)
            or len(oid) != self.repository.oid_length
            or HEX_OID.fullmatch(oid) is None
        ):
            raise GitExecutionError("an immutable full object ID is required")

    def verify_object(self, oid: str, expected_type: str) -> str:
        self._validate_oid(oid)
        kind = checked(
            self._run(["cat-file", "-t", oid], execution_repo=True),
            "object type verification",
        ).decode("ascii").strip()
        if kind != expected_type:
            raise GitExecutionError(f"expected {expected_type}, got {kind}")
        return oid

    def read_target(self) -> str:
        symbolic = self._run(
            ["symbolic-ref", "-q", self.repository.target_ref], execution_repo=False
        )
        if symbolic.returncode == 0:
            raise GitExecutionError("registered target became symbolic")
        if symbolic.returncode != 1:
            checked(symbolic, "target symbolic-ref check")
        raw = checked(
            self._run(
                [
                    "rev-parse",
                    "--verify",
                    "--end-of-options",
                    self.repository.target_ref,
                ],
                execution_repo=False,
            ),
            "target ref read",
        ).decode("ascii").strip()
        return self.verify_object(raw, "commit")

    def is_ancestor(self, ancestor: str, descendant: str) -> bool:
        self.verify_object(ancestor, "commit")
        self.verify_object(descendant, "commit")
        result = self._run(
            ["merge-base", "--is-ancestor", ancestor, descendant],
            execution_repo=True,
        )
        if result.returncode == 0:
            return True
        if result.returncode == 1:
            return False
        checked(result, "ancestry check")
        raise AssertionError("unreachable")

    def pin(self, operation_id: str, name: str, oid: str) -> str:
        require_safe_id(operation_id, "operation_id")
        require_safe_id(name, "pin name")
        self.verify_object(oid, "commit")
        ref = f"refs/omd/pins/{operation_id}/{name}"
        result = self._run(
            ["update-ref", "--no-deref", ref, oid, ""], execution_repo=False
        )
        if result.returncode == 0:
            return ref
        existing = self._run(
            ["rev-parse", "--verify", "--end-of-options", ref],
            execution_repo=False,
        )
        if existing.returncode == 0 and existing.stdout.decode("ascii").strip() == oid:
            return ref
        checked(result, f"pin creation for {name}")
        raise AssertionError("unreachable")

    def merge_tree(self, target_oid: str, source_oid: str) -> str:
        self.verify_object(target_oid, "commit")
        self.verify_object(source_oid, "commit")
        result = self._run(
            ["merge-tree", "--write-tree", target_oid, source_oid],
            execution_repo=True,
        )
        if result.returncode == 1:
            raise MergeConflict("Git reported a conflicted merge")
        raw = checked(result, "merge-tree").strip()
        try:
            tree_oid = raw.decode("ascii")
        except UnicodeDecodeError as exc:
            raise GitExecutionError("merge-tree returned non-ASCII output") from exc
        if b"\n" in raw or b"\0" in raw:
            raise GitExecutionError("clean merge-tree output was not one tree ID")
        return self.verify_object(tree_oid, "tree")

    def diff(self, old_oid: str, new_oid: str) -> tuple[DeltaEntry, ...]:
        self._validate_oid(old_oid)
        self._validate_oid(new_oid)
        raw = checked(
            self._run(
                [
                    "diff-tree",
                    "--no-commit-id",
                    "--raw",
                    "-r",
                    "-z",
                    "--no-renames",
                    old_oid,
                    new_oid,
                ],
                execution_repo=True,
            ),
            "tree delta",
        )
        if not raw:
            return ()
        fields = raw.split(b"\0")
        if fields[-1] != b"":
            raise GitExecutionError("tree delta was not NUL terminated")
        fields.pop()
        if len(fields) % 2:
            raise GitExecutionError("unexpected raw tree delta field count")
        result: list[DeltaEntry] = []
        seen_raw: set[bytes] = set()
        seen_nfc: set[str] = set()
        for index in range(0, len(fields), 2):
            header, path_raw = fields[index], fields[index + 1]
            match = RAW_HEADER.fullmatch(header)
            if match is None:
                raise GitExecutionError("invalid raw tree delta header")
            old_mode, new_mode, _old, _new, status_raw = match.groups()
            if path_raw in seen_raw:
                raise PathPolicyError("duplicate path in tree delta")
            seen_raw.add(path_raw)
            try:
                path = path_raw.decode("utf-8", "strict")
            except UnicodeDecodeError as exc:
                raise PathPolicyError("Git path is not valid UTF-8") from exc
            normalized = unicodedata.normalize("NFC", path)
            if path != normalized:
                raise PathPolicyError("Git path is not NFC")
            if normalized in seen_nfc:
                raise PathPolicyError("Git path normalization collision")
            seen_nfc.add(normalized)
            if (
                not path
                or path.startswith("/")
                or "\\" in path
                or "\x00" in path
                or any(segment in {"", ".", ".."} for segment in path.split("/"))
            ):
                raise PathPolicyError("Git path is outside the repo path grammar")
            modes = {old_mode, new_mode} - {b"000000"}
            if b"120000" in modes or b"160000" in modes:
                raise PathPolicyError("symlink and gitlink deltas are not admitted")
            if not modes.issubset({b"100644", b"100755"}):
                raise PathPolicyError("unsupported Git tree mode")
            result.append(
                DeltaEntry(
                    path=path,
                    old_mode=old_mode.decode("ascii"),
                    new_mode=new_mode.decode("ascii"),
                    status=status_raw.decode("ascii"),
                )
            )
        return tuple(result)

    def commit_tree(
        self,
        *,
        tree_oid: str,
        target_oid: str,
        source_oid: str,
        metadata: CommitMetadata,
    ) -> str:
        self.verify_object(tree_oid, "tree")
        self.verify_object(target_oid, "commit")
        self.verify_object(source_oid, "commit")
        env = {
            "GIT_AUTHOR_NAME": metadata.author_name,
            "GIT_AUTHOR_EMAIL": metadata.author_email,
            "GIT_AUTHOR_DATE": metadata.author_date,
            "GIT_COMMITTER_NAME": metadata.committer_name,
            "GIT_COMMITTER_EMAIL": metadata.committer_email,
            "GIT_COMMITTER_DATE": metadata.committer_date,
        }
        raw = checked(
            self._run(
                [
                    "commit-tree",
                    tree_oid,
                    "-p",
                    target_oid,
                    "-p",
                    source_oid,
                    "--no-gpg-sign",
                    "-m",
                    metadata.message,
                ],
                execution_repo=True,
                extra_env=env,
            ),
            "commit-tree",
        ).decode("ascii").strip()
        return self.verify_object(raw, "commit")

    def update_target(self, new_oid: str, expected_old: str, operation_id: str) -> GitResult:
        self.verify_object(new_oid, "commit")
        self.verify_object(expected_old, "commit")
        require_safe_id(operation_id, "operation_id")
        worktrees = checked(
            self._run(
                ["worktree", "list", "--porcelain", "-z"], execution_repo=False
            ),
            "pre-publication worktree inspection",
        )
        if target_checked_out(worktrees, self.repository.target_ref):
            raise GitExecutionError("registered target became checked out")
        return self._run(
            [
                "-c",
                "core.filesRefLockTimeout=5000",
                "update-ref",
                "--no-deref",
                "--create-reflog",
                "-m",
                f"omd integrate op={operation_id}",
                self.repository.target_ref,
                new_oid,
                expected_old,
            ],
            execution_repo=False,
        )
