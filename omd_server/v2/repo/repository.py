"""Registered Git-instance discovery and live identity validation."""

from __future__ import annotations

import hashlib
import os
import re
import shutil
import stat
from pathlib import Path

from .contracts import RegisteredRepository, SagaRecord, ensure_directory, require_safe_id
from .errors import RepoConfigurationError
from .git_process import GitResult, base_env, checked, run_bounded


MIN_GIT = (2, 38, 0)
HEX_OID = re.compile(r"^[0-9a-f]+$")


def _version(raw: bytes) -> tuple[int, int, int]:
    match = re.search(rb"git version (\d+)\.(\d+)\.(\d+)", raw)
    if match is None:
        raise RepoConfigurationError("unable to parse Git version")
    return tuple(int(part) for part in match.groups())  # type: ignore[return-value]


def _decode_git_path(raw: bytes, operation: str) -> Path:
    if not raw.endswith(b"\n") or b"\n" in raw[:-1] or b"\0" in raw:
        raise RepoConfigurationError(f"invalid path output from {operation}")
    try:
        return Path(raw[:-1].decode("utf-8", "strict"))
    except UnicodeDecodeError as exc:
        raise RepoConfigurationError(
            f"non-UTF-8 path from {operation} is outside the alpha boundary"
        ) from exc


def target_checked_out(raw: bytes, target_ref: str) -> bool:
    marker = b"branch " + target_ref.encode("ascii")
    return any(field == marker for field in raw.split(b"\0"))


def _inside(path: Path, parent: Path) -> bool:
    return path == parent or parent in path.parents


def _overlaps(left: Path, right: Path) -> bool:
    return _inside(left, right) or _inside(right, left)


def _reject_alternates(object_dir: Path) -> None:
    for path in (
        object_dir / "info" / "alternates",
        object_dir / "info" / "http-alternates",
    ):
        try:
            metadata = path.lstat()
        except FileNotFoundError:
            continue
        if (
            not stat.S_ISREG(metadata.st_mode)
            or metadata.st_size > 65536
            or path.read_bytes().strip()
        ):
            raise RepoConfigurationError(
                "object alternates are outside the alpha boundary"
            )


def _identity_digest(
    *,
    common_dir: Path,
    object_dir: Path,
    object_format: str,
    target_ref: str,
    omd_exclusive: bool,
    git_binary: Path,
    git_version: tuple[int, int, int],
) -> str:
    common_stat = common_dir.stat()
    object_stat = object_dir.stat()
    binary_stat = git_binary.stat()
    fields = (
        os.fspath(common_dir),
        str(common_stat.st_dev),
        str(common_stat.st_ino),
        os.fspath(object_dir),
        str(object_stat.st_dev),
        str(object_stat.st_ino),
        object_format,
        target_ref,
        "omd-exclusive" if omd_exclusive else "non-exclusive",
        os.fspath(git_binary),
        str(binary_stat.st_dev),
        str(binary_stat.st_ino),
        ".".join(str(item) for item in git_version),
    )
    return hashlib.sha256("\0".join(fields).encode("utf-8")).hexdigest()


def assert_live_repository(repository: RegisteredRepository) -> None:
    try:
        common_dir = repository.git_common_dir.resolve(strict=True)
        object_dir = repository.object_dir.resolve(strict=True)
        git_binary = repository.git_binary.resolve(strict=True)
    except OSError as exc:
        raise RepoConfigurationError("registered repository path disappeared") from exc
    if (
        common_dir != repository.git_common_dir
        or object_dir != repository.object_dir
        or git_binary != repository.git_binary
        or common_dir.stat().st_uid != os.getuid()
        or object_dir.stat().st_uid != os.getuid()
    ):
        raise RepoConfigurationError("registered repository path or owner changed")
    _reject_alternates(object_dir)
    live = _identity_digest(
        common_dir=common_dir,
        object_dir=object_dir,
        object_format=repository.object_format,
        target_ref=repository.target_ref,
        omd_exclusive=repository.omd_exclusive,
        git_binary=git_binary,
        git_version=repository.git_version,
    )
    if live != repository.identity_digest:
        raise RepoConfigurationError("registered repository identity changed")


def record_matches_repository(
    record: SagaRecord, repository: RegisteredRepository
) -> bool:
    return (
        record.repository_identity == repository.identity_digest
        and record.target_ref == repository.target_ref
    )


def repository_drift_error(
    record: SagaRecord, repository: RegisteredRepository
) -> RepoConfigurationError:
    return RepoConfigurationError(
        "registered Git instance changed: "
        f"expected {record.repository_identity}, got {repository.identity_digest}"
    )


def inspect_repository(
    *,
    repo_id: str,
    path: Path,
    target_ref: str,
    state_dir: Path,
    omd_exclusive: bool,
) -> RegisteredRepository:
    require_safe_id(repo_id, "repo_id")
    if not target_ref.startswith("refs/heads/") or target_ref.endswith("/"):
        raise RepoConfigurationError("target must be a full refs/heads/* ref")
    source_path = Path(path).resolve(strict=True)
    git_binary_raw = shutil.which("git")
    if git_binary_raw is None:
        raise RepoConfigurationError("Git executable not found")
    git_binary = Path(git_binary_raw).resolve(strict=True)
    state_path = Path(os.path.realpath(state_dir))
    if _overlaps(state_path, source_path):
        raise RepoConfigurationError("state directory must be outside the repository")
    home = state_path / "home"
    env = base_env(home)

    def run(*args: str) -> GitResult:
        return run_bounded(
            [os.fspath(git_binary), "-C", os.fspath(source_path), *args],
            cwd=source_path,
            env=env,
        )

    version = _version(checked(run("--version"), "git version"))
    if version < MIN_GIT:
        raise RepoConfigurationError("Git 2.38 or newer is required")
    checked(run("check-ref-format", target_ref), "target ref validation")
    symbolic = run("symbolic-ref", "-q", target_ref)
    if symbolic.returncode == 0:
        raise RepoConfigurationError("target ref must not be symbolic")
    if symbolic.returncode != 1:
        checked(symbolic, "target symbolic-ref check")
    common_raw = checked(
        run("rev-parse", "--path-format=absolute", "--git-common-dir"),
        "Git common-dir discovery",
    )
    common_dir = _decode_git_path(
        common_raw, "Git common-dir discovery"
    ).resolve(strict=True)
    object_dir = (common_dir / "objects").resolve(strict=True)
    if _overlaps(state_path, common_dir) or _overlaps(state_path, object_dir):
        raise RepoConfigurationError("state directory must be outside Git storage")
    if common_dir.stat().st_uid != os.getuid() or object_dir.stat().st_uid != os.getuid():
        raise RepoConfigurationError("registered repository must have the daemon owner")
    _reject_alternates(object_dir)
    object_format = checked(
        run("rev-parse", "--show-object-format"), "object-format discovery"
    ).decode("ascii").strip()
    if object_format not in {"sha1", "sha256"}:
        raise RepoConfigurationError(f"unsupported object format: {object_format}")
    oid_length = 40 if object_format == "sha1" else 64
    target_oid = checked(
        run("rev-parse", "--verify", "--end-of-options", target_ref),
        "target resolution",
    ).decode("ascii").strip()
    if len(target_oid) != oid_length or HEX_OID.fullmatch(target_oid) is None:
        raise RepoConfigurationError("target did not resolve to a full object ID")
    if checked(run("cat-file", "-t", target_oid), "target type check").strip() != b"commit":
        raise RepoConfigurationError("target ref must directly name a commit")
    worktrees = checked(
        run("worktree", "list", "--porcelain", "-z"), "worktree inspection"
    )
    if target_checked_out(worktrees, target_ref):
        raise RepoConfigurationError("target ref must not be checked out")
    state = ensure_directory(state_path)
    ensure_directory(home)
    execution_root = ensure_directory(state / "execution")
    identity = _identity_digest(
        common_dir=common_dir,
        object_dir=object_dir,
        object_format=object_format,
        target_ref=target_ref,
        omd_exclusive=omd_exclusive,
        git_binary=git_binary,
        git_version=version,
    )
    return RegisteredRepository(
        repo_id=repo_id,
        source_path=source_path,
        git_common_dir=common_dir,
        object_dir=object_dir,
        target_ref=target_ref,
        state_dir=state,
        execution_git_dir=execution_root / f"{repo_id}.git",
        hooks_dir=Path(os.devnull),
        object_format=object_format,
        oid_length=oid_length,
        git_binary=git_binary,
        git_version=version,
        identity_digest=identity,
        omd_exclusive=omd_exclusive,
    )
