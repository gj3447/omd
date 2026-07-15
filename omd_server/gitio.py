"""실제 git 연동 — 물방울 worktree(자존자 격리) + CLOUD CONNECT(응결=merge).

OMD의 명제를 실물 git에서 집행: 각 물방울은 독립 worktree+브랜치에서 운행하고,
연결은 통합 브랜치로의 `git merge`. write-set이 입체(서로소)면 이 merge는 무충돌.
"""

from __future__ import annotations

import hashlib
import math
import os
import re
import signal
import stat
import subprocess
import threading
import time
from collections.abc import Callable, Sequence
from contextlib import contextmanager
from pathlib import Path


class GitError(RuntimeError):
    pass


class GitNothingToCommit(GitError):
    """staged 변경이 없어 커밋할 게 없음 — 부분문자열/로케일 추측이 아니라 구조적 판별.
    호출부가 '변경 없음 → skip' 을 정확히 분기하게 해 진짜 commit 실패와 안 섞이게 한다."""


class GitTimeout(GitError):
    """merge 서브프로세스가 타임아웃(§E — 무한 hang 방지). abort 대상."""


class GitIntegrationCheckError(GitError):
    """통합 후보 머지는 만들었지만 operator check가 통과하지 못했다.

    stdout/stderr는 진단 폭주를 막기 위해 각 ``output_limit`` 이내로 잘린다.
    이 예외가 노출됐다는 것은 merge abort와 원상복구 증명이 끝났다는 뜻이다. 복구를
    증명하지 못하면 대신 :class:`GitRollbackError`가 발생한다.
    """

    def __init__(self, msg: str, *, argv: Sequence[str], returncode: int | None,
                 stdout: str = "", stderr: str = ""):
        super().__init__(msg)
        self.argv = tuple(argv)
        self.returncode = returncode
        self.stdout = stdout
        self.stderr = stderr


class GitIntegrationCheckFailed(GitIntegrationCheckError):
    """operator check가 non-zero로 끝났거나 실행 자체에 실패했다."""


class GitIntegrationCheckTimeout(GitTimeout, GitIntegrationCheckError):
    """operator check가 제한 시간을 넘겼다(부분 stdout/stderr 포함)."""

    def __init__(self, msg: str, *, argv: Sequence[str], stdout: str = "",
                 stderr: str = ""):
        GitIntegrationCheckError.__init__(
            self, msg, argv=argv, returncode=None, stdout=stdout, stderr=stderr,
        )


class GitIntegrationMutation(GitIntegrationCheckError):
    """check가 후보 merge의 HEAD/index/tracked worktree를 바꿨다."""

    def __init__(self, msg: str, *, argv: Sequence[str], stdout: str = "",
                 stderr: str = "", mutations: Sequence[str] = ()):
        super().__init__(msg, argv=argv, returncode=0, stdout=stdout, stderr=stderr)
        self.mutations = tuple(mutations)


class GitIntegrationPreconditionError(GitError):
    """전용 통합 worktree가 깨끗한 단일 integration HEAD가 아니어서 gate 시작 불가."""


class GitRollbackError(GitError):
    """실패한 후보 merge가 원래 HEAD/clean tracked tree로 돌아갔음을 증명하지 못했다.

    자동 reset으로 증거를 지우지 않는 fail-stop 오류다. 운영자가 worktree를 조사해야 한다.
    """

    def __init__(self, msg: str, *, cause: BaseException, expected_head: str,
                 actual_head: str | None, problems: Sequence[str]):
        super().__init__(msg)
        self.cause = cause
        self.expected_head = expected_head
        self.actual_head = actual_head
        self.problems = tuple(problems)


class GitMergeConflict(GitError):
    """merge 가 내용 충돌로 실패(P3 증분13) — 충돌 경로 목록을 실어 호출부가 진단
    (원인커밋 지목/복구 레시피)을 만들 수 있게 한다. GitError 하위라 기존 호출부 하위호환."""

    def __init__(self, msg, conflicts=None):
        super().__init__(msg)
        self.conflicts = sorted(conflicts or [])


class GitRepo:
    """통합 브랜치를 보유한 메인 레포. 물방울 worktree를 발사/응결.

    응결(merge)은 **사용자 HEAD를 절대 안 건드리는** 전용 통합 worktree에서 한다(§D11):
    `<root>-omd-integration` 을 integration_branch 로 체크아웃해 두고, 거기서만
    `checkout integration_branch` + `merge --no-ff`. 동시 connect는 repo-wide merge_token
    (DB 측 Semaphore max=1)이 직렬화하므로 통합 worktree의 .git/index 경합이 없다.
    """

    # 테스트·헤드리스에서 전역 git config 없이도 커밋되게 신원 주입
    _IDENT = ["-c", "user.name=omd", "-c", "user.email=omd@acme"]
    _REF_LOCK_MARKER_RE = re.compile(
        rb"OMD-BRANCH-REF-LOCK v1 pid=([1-9][0-9]*) nonce=([0-9a-f]{32})\n"
    )
    _REF_LOCK_MARKER_LIMIT = 256

    def __init__(self, root: str):
        self.root = str(Path(root).resolve())

    def _git(self, *args, cwd=None, timeout=None, input_text=None) -> str:
        env = os.environ.copy()
        # Runtime provenance is about the object named by the stored OID, not a
        # mutable replacement/graft overlay that can reinterpret that OID after
        # a crash.  NO_REPLACE does not disable legacy .git/info/grafts, so seal
        # both history-rewrite mechanisms for every authoritative Git query.
        env["GIT_NO_REPLACE_OBJECTS"] = "1"
        env["GIT_GRAFT_FILE"] = os.devnull
        try:
            r = subprocess.run(
                ["git", *args], cwd=cwd or self.root,
                capture_output=True, text=True, timeout=timeout, input=input_text,
                env=env,
            )
        except subprocess.TimeoutExpired:
            raise GitTimeout(f"git {' '.join(args)} → timeout after {timeout}s")
        if r.returncode != 0:
            raise GitError(f"git {' '.join(args)} → {r.returncode}: {r.stderr.strip()}")
        return r.stdout.strip()

    def current_branch(self, cwd=None) -> str:
        return self._git("rev-parse", "--abbrev-ref", "HEAD", cwd=cwd)

    def revision_tip(self, revision: str = "HEAD", cwd=None) -> str:
        """Resolve one immutable full object ID for provenance and split-phase pinning."""
        return self._git("rev-parse", "--verify", revision, cwd=cwd)

    def worktree_changes(self, worktree: str) -> list[str]:
        """Return every staged, unstaged, or untracked task-worktree change."""
        raw = self._git("status", "--porcelain=v1", cwd=worktree)
        return raw.splitlines() if raw else []

    @staticmethod
    def _pid_is_dead(pid: int) -> bool:
        """Return true only when the kernel positively reports that ``pid`` is absent.

        Permission errors and unfamiliar platform errors are treated as a live
        owner.  Stale-lock recovery must prefer a false negative over deleting a
        lock that another process may still own.
        """
        try:
            os.kill(pid, 0)
        except ProcessLookupError:
            return True
        except OSError:
            return False
        return False

    @staticmethod
    def _same_file(lock_path: Path, identity: os.stat_result) -> bool:
        """Revalidate a lock pathname without following a replacement symlink."""
        try:
            current = lock_path.stat(follow_symlinks=False)
        except OSError:
            return False
        return (
            stat.S_ISREG(current.st_mode)
            and current.st_dev == identity.st_dev
            and current.st_ino == identity.st_ino
        )

    @classmethod
    def _reap_stale_omd_ref_lock(cls, lock_path: Path) -> bool:
        """Remove a crashed OMD lock, never an ordinary Git or live OMD lock.

        The marker is deliberately strict and bounded.  A candidate is eligible
        only when it is a regular file containing the exact OMD marker and its PID
        is positively dead.  The pathname's device/inode are then revalidated
        against the opened file immediately before unlinking, so a concurrently
        replaced Git lock is left untouched.  ``True`` means the caller should
        retry ``O_EXCL`` immediately (the file was removed or disappeared).
        """
        flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0)
        if hasattr(os, "O_NOFOLLOW"):
            flags |= os.O_NOFOLLOW
        try:
            existing_fd = os.open(lock_path, flags)
        except FileNotFoundError:
            return True
        except OSError:
            return False
        try:
            identity = os.fstat(existing_fd)
            if not stat.S_ISREG(identity.st_mode):
                return False
            chunks = []
            remaining = cls._REF_LOCK_MARKER_LIMIT + 1
            while remaining:
                chunk = os.read(existing_fd, remaining)
                if not chunk:
                    break
                chunks.append(chunk)
                remaining -= len(chunk)
            marker = b"".join(chunks)
            match = cls._REF_LOCK_MARKER_RE.fullmatch(marker)
            if match is None or not cls._pid_is_dead(int(match.group(1))):
                return False
            if not cls._same_file(lock_path, identity):
                return False
            try:
                lock_path.unlink()
            except FileNotFoundError:
                return True
            return True
        finally:
            os.close(existing_fd)

    @contextmanager
    def _exclusive_omd_lock(self, lock_path: Path, label: str, timeout: float):
        """Acquire one crash-reapable OMD-owned lock file with ``O_EXCL``."""
        lock_path.parent.mkdir(parents=True, exist_ok=True)
        deadline = time.monotonic() + max(0.0, timeout)
        marker = (
            f"OMD-BRANCH-REF-LOCK v1 pid={os.getpid()} "
            f"nonce={os.urandom(16).hex()}\n"
        ).encode("ascii")
        fd = None
        identity = None
        while fd is None:
            try:
                flags = (
                    os.O_CREAT | os.O_EXCL | os.O_WRONLY
                    | getattr(os, "O_CLOEXEC", 0)
                )
                fd = os.open(lock_path, flags, 0o600)
            except FileExistsError as exc:
                if self._reap_stale_omd_ref_lock(lock_path):
                    continue
                if time.monotonic() >= deadline:
                    raise GitError(f"timed out sealing {label}") from exc
                time.sleep(0.01)
                continue
            try:
                written = 0
                while written < len(marker):
                    count = os.write(fd, marker[written:])
                    if count <= 0:
                        raise OSError("short write while marking OMD branch ref lock")
                    written += count
                os.fsync(fd)
                identity = os.fstat(fd)
            except BaseException:
                try:
                    if identity is None:
                        identity = os.fstat(fd)
                    if self._same_file(lock_path, identity):
                        try:
                            lock_path.unlink()
                        except FileNotFoundError:
                            pass
                finally:
                    os.close(fd)
                    fd = None
                raise
        try:
            yield
        finally:
            try:
                if identity is not None and self._same_file(lock_path, identity):
                    try:
                        lock_path.unlink()
                    except FileNotFoundError:
                        pass
            finally:
                os.close(fd)

    @contextmanager
    def branch_ref_lock(self, branch: str, *, timeout: float = 5.0):
        """Seal one task branch ref against concurrent raw Git commits.

        Git itself uses ``<ref>.lock`` for compare-and-swap updates. Holding the
        same lock from validation through the SQLite commit closes raw-writer
        gaps. OMD marker/PID/inode checks make crashed locks safely reapable while
        ordinary Git locks and live OMD owners are never removed.
        """
        ref_path = Path(self._git("rev-parse", "--git-path", f"refs/heads/{branch}"))
        if not ref_path.is_absolute():
            ref_path = Path(self.root, ref_path)
        lock_path = Path(str(ref_path.resolve()) + ".lock")
        with self._exclusive_omd_lock(
            lock_path, f"branch ref {branch}", timeout
        ):
            yield

    @contextmanager
    def integration_operation_lock(self, integration_branch: str, *, timeout: float):
        """Serialize OMD connect/recovery across lease-disabled MCP processes.

        This lock is separate from Git's integration ref lock, so merge/update-ref
        can still publish normally. It spans Phase A→B→C (or all of recovery),
        preventing one coordinator from mistaking another live merge for a
        crash-dangling token when leader leasing is intentionally disabled.
        """
        common = Path(
            self._git("rev-parse", "--path-format=absolute", "--git-common-dir")
        )
        digest = hashlib.sha256(integration_branch.encode("utf-8")).hexdigest()
        lock_path = common / "omd-locks" / f"integration-{digest}.lock"
        with self._exclusive_omd_lock(
            lock_path, f"integration operation {integration_branch}", timeout
        ):
            yield

    def repository_identity(self) -> str:
        """Best-effort logical integration-stream identity.

        Operators should pass Coordinator(repo_id=...) when clones must share a
        stable logical name.  The fallback prefers origin and only then the Git
        common directory; the latter is explicitly a local identity.
        """
        try:
            origin = self._git("remote", "get-url", "origin")
            return "origin-sha256:" + hashlib.sha256(origin.encode()).hexdigest()
        except GitError:
            common = Path(self._git("rev-parse", "--path-format=absolute", "--git-common-dir"))
            return f"local-common-dir:{common.resolve()}"

    def add_worktree(self, branch: str, path: str, base: str = "HEAD") -> str:
        """base에서 새 branch를 만들어 path에 linked worktree로 체크아웃."""
        self._git("worktree", "add", "-b", branch, str(Path(path).resolve()), base)
        return str(Path(path).resolve())

    def ensure_integration_worktree(self, path: str, integration_branch: str) -> str:
        """integration_branch를 체크아웃한 전용 통합 worktree를 보장(멱등).
        이미 있으면 그대로 쓴다. 사용자 HEAD(root)는 절대 건드리지 않는다(§D11)."""
        p = str(Path(path).resolve())
        if Path(p, ".git").exists():
            return p   # 이미 worktree (멱등)
        # 기존(고아) worktree 등록 잔재를 정리한 뒤 새로 등록
        self._git("worktree", "prune")
        self._git("worktree", "add", p, integration_branch)
        return p

    def commit_all(self, worktree: str, msg: str) -> str:
        """worktree의 모든 변경을 스테이지+커밋. 빈 변경이면 GitNothingToCommit(구조적 판별 —
        `git status --porcelain` 빈값 = 커밋할 것 없음; 부분문자열/로케일 추측 안 함)."""
        self._git("add", "-A", cwd=worktree)
        if not self._git("status", "--porcelain", cwd=worktree).strip():
            raise GitNothingToCommit(f"nothing to commit in {worktree}")
        self._git(*self._IDENT, "commit", "-m", msg, cwd=worktree)
        return self._git("rev-parse", "HEAD", cwd=worktree)

    # ---- P5 strict-writeset: 궤도-밖 경로를 commit 전에 staged 에서 제외(no wedge) ----
    def stage_all(self, worktree: str) -> None:
        self._git("add", "-A", cwd=worktree)

    def staged_paths(self, worktree: str) -> list[str]:
        out = self._git("diff", "--cached", "--name-only", cwd=worktree)
        return [p for p in out.splitlines() if p.strip()]

    def unstage(self, worktree: str, paths: list[str]) -> None:
        """staged 에서 paths 만 빼고(working tree 변경은 보존). git restore --staged."""
        if paths:
            self._git("restore", "--staged", "--", *paths, cwd=worktree)

    def commit_staged(self, worktree: str, msg: str) -> str:
        """이미 staged 된 것만 커밋(add 없이). 빈 index 면 GitNothingToCommit."""
        if not self.staged_paths(worktree):
            raise GitNothingToCommit(f"nothing staged in {worktree}")
        self._git(*self._IDENT, "commit", "-m", msg, cwd=worktree)
        return self._git("rev-parse", "HEAD", cwd=worktree)

    def branch_tip(self, branch: str) -> str | None:
        try:
            return self._git("rev-parse", "--verify", "--quiet", f"refs/heads/{branch}")
        except GitError:
            return None

    def changed_paths(self, branch: str, base: str) -> list[str]:
        """`base`(통합 브랜치) 대비 `branch`(물방울 브랜치)가 **건드린 모든 파일 경로**
        (P0-11/§D10 write-set FS 감사용). `git diff --name-only base...branch` =
        merge-base 이후 branch가 바꾼 것만(통합 쪽 변경은 제외) → 이 task가 실제로 쓴 영역.

        rename/delete 도 '건드린 경로'로 센다: rename 은 `R` 상태이지만 `--name-only`는
        **삭제된 원본과 새 경로 둘 다** (renames 비탐지 모드, `--no-renames`)를 내므로
        둘 다 감사 대상에 들어간다(궤도 밖으로 옮기면 잡힘). 경로는 정규화(따옴표/escape 없이)."""
        # core.quotepath=false: 비ASCII 경로를 octal-escape 하지 않고 그대로 — 감사 정확성.
        out = self._git("-c", "core.quotepath=false", "diff", "--name-only",
                        "--no-renames", f"{base}...{branch}")
        return [ln for ln in out.splitlines() if ln.strip()]

    def merge_into(self, integration_worktree: str, integration_branch: str,
                   branch: str, msg: str, *, timeout: float | None = None,
                   check_argv: Sequence[str] | None = None,
                   check_timeout: float = 300.0,
                   check_output_limit: int = 16_384,
                   expected_base: str | None = None,
                   candidate_attestor: Callable[[str, str], None] | None = None) -> str:
        """전용 통합 worktree에서 integration_branch에 branch를 --no-ff merge(§D11).
        Phase B 본체 — **락 밖**에서 호출된다. 충돌/타임아웃이면 `merge --abort` 후 raise.
        msg에 고유 trailer를 넣어 두면 복구가 통합 브랜치에서 머지 여부를 trailer-probe 한다."""
        if check_argv is not None:
            return self._merge_into_checked(
                integration_worktree, integration_branch, branch, msg,
                timeout=timeout, check_argv=check_argv,
                check_timeout=check_timeout, check_output_limit=check_output_limit,
                expected_base=expected_base,
                candidate_attestor=candidate_attestor,
            )
        wt = str(Path(integration_worktree).resolve())
        integration_ref = f"refs/heads/{integration_branch}"
        # 명시적으로 integration_branch를 체크아웃(사용자 HEAD가 아님을 보장).
        self._git("checkout", integration_branch, cwd=wt)
        self._require_symbolic_head(wt, integration_ref, "before merge candidate")
        original_head = self._git("rev-parse", "HEAD", cwd=wt)
        if expected_base is not None and original_head != expected_base:
            raise GitIntegrationPreconditionError(
                "integration branch changed after connect admission: "
                f"expected {expected_base}, got {original_head}"
            )
        expected_merge_head = self._git("rev-parse", "--verify", branch, cwd=wt)
        try:
            self._git(*self._IDENT, "merge", "--no-ff", "--no-commit", "-m", msg, branch,
                      cwd=wt, timeout=timeout)
        except GitError as e:
            if isinstance(e, GitTimeout):
                failure: GitError = GitTimeout(f"merge timeout on {branch}: {e}")
                self._abort_and_verify(
                    wt, original_head, failure, expected_ref=integration_ref
                )
                raise failure
            # P3 증분13: 충돌 검사. rerere.autoUpdate 가 기록된 해소로 *전부* 해소했으면
            # (unmerged 0 + MERGE_HEAD 존재) 아래의 fixed-parent CAS publication으로 진행한다.
            unmerged = self.unmerged_paths(wt)
            in_merge = self._merge_in_progress(wt)
            if not (in_merge and not unmerged):
                failure = (
                    GitMergeConflict(
                        f"merge conflict on {branch}: {e}", conflicts=unmerged
                    ) if in_merge else GitError(f"merge failed on {branch}: {e}")
                )
                self._abort_and_verify(
                    wt, original_head, failure, expected_ref=integration_ref
                )
                raise failure
        if not self._merge_in_progress(wt):
            return self._git("rev-parse", "HEAD", cwd=wt)
        try:
            self._require_symbolic_head(wt, integration_ref, "before publication")
            merge_heads, _merge_message = self._merge_metadata_snapshot(
                wt, expected_merge_head
            )
        except GitError as failure:
            self._abort_and_verify(
                wt, original_head, failure, expected_ref=integration_ref
            )
            raise
        merge_index = self._git("write-tree", cwd=wt)
        before_tracked = self._tracked_worktree_diff(wt)
        if before_tracked:
            failure = GitIntegrationPreconditionError(
                f"candidate merge worktree differs from its index: {before_tracked}"
            )
            self._abort_and_verify(
                wt, original_head, failure, expected_ref=integration_ref
            )
            raise failure
        return self._commit_merge_candidate_cas(
            wt, integration_branch, original_head, merge_index,
            merge_heads=merge_heads, commit_message=msg,
            candidate_attestor=candidate_attestor,
        )

    def _merge_into_checked(self, integration_worktree: str, integration_branch: str,
                            branch: str, msg: str, *, timeout: float | None,
                            check_argv: Sequence[str], check_timeout: float,
                            check_output_limit: int,
                            expected_base: str | None = None,
                            candidate_attestor: Callable[[str, str], None] | None = None,
                            ) -> str:
        """검사 가능한 후보 merge를 만들고 green일 때만 commit한다.

        ``check_argv``는 shell을 거치지 않고 그대로 실행된다. 검사 전후의 HEAD, index tree,
        tracked worktree를 비교하므로 검사는 읽기 전용이어야 한다. red/timeout/mutation은
        ``merge --abort`` 뒤 원 HEAD + no MERGE_HEAD + clean tracked tree를 다시 증명한다.
        """
        argv = self._validate_check_args(check_argv, check_timeout, check_output_limit)
        wt = str(Path(integration_worktree).resolve())
        integration_ref = f"refs/heads/{integration_branch}"
        self._git("checkout", integration_branch, cwd=wt)
        self._require_symbolic_head(wt, integration_ref, "before merge candidate")
        original_head = self._git("rev-parse", "HEAD", cwd=wt)
        if expected_base is not None and original_head != expected_base:
            raise GitIntegrationPreconditionError(
                "integration branch changed after connect admission: "
                f"expected {expected_base}, got {original_head}"
            )
        expected_merge_head = self._git("rev-parse", "--verify", branch, cwd=wt)
        dirty = self._tracked_status(wt)
        if self._merge_in_progress(wt) or dirty:
            details = "MERGE_HEAD exists" if self._merge_in_progress(wt) else dirty
            raise GitIntegrationPreconditionError(
                f"integration worktree is not clean before merge: {details}"
            )

        merge_error: GitError | None = None
        try:
            self._git(*self._IDENT, "merge", "--no-ff", "--no-commit", "-m", msg,
                      branch, cwd=wt, timeout=timeout)
        except GitError as exc:
            merge_error = exc

        if merge_error is not None:
            unmerged = self.unmerged_paths(wt)
            in_merge = self._merge_in_progress(wt)
            # rerere.autoUpdate가 모든 충돌을 해소한 경우에는 아직 non-zero인 merge 명령의
            # 후보 index를 그대로 검사한다. 미해소 충돌/타임아웃은 원상복구 후 기존 타입 유지.
            if not (in_merge and not unmerged and not isinstance(merge_error, GitTimeout)):
                if isinstance(merge_error, GitTimeout):
                    failure: GitError = GitTimeout(
                        f"merge timeout on {branch}: {merge_error}"
                    )
                elif in_merge:
                    failure = GitMergeConflict(
                        f"merge conflict on {branch}: {merge_error}", conflicts=unmerged,
                    )
                else:
                    failure = GitError(f"merge failed on {branch}: {merge_error}")
                self._abort_and_verify(
                    wt, original_head, failure, expected_ref=integration_ref
                )
                raise failure

        # "Already up to date" has no MERGE_HEAD and therefore no candidate commit. Preserve
        # merge_into's historical no-op result; there is nothing new for a pre-commit gate to test.
        if not self._merge_in_progress(wt):
            return self._git("rev-parse", "HEAD", cwd=wt)

        try:
            self._require_symbolic_head(wt, integration_ref, "before operator check")
            merge_heads, merge_message = self._merge_metadata_snapshot(
                wt, expected_merge_head
            )
        except GitError as failure:
            self._abort_and_verify(
                wt, original_head, failure, expected_ref=integration_ref
            )
            raise
        merge_index = self._git("write-tree", cwd=wt)
        before_tracked = self._tracked_worktree_diff(wt)
        if before_tracked:
            failure = GitIntegrationPreconditionError(
                f"candidate merge worktree differs from its index: {before_tracked}"
            )
            self._abort_and_verify(
                wt, original_head, failure, expected_ref=integration_ref
            )
            raise failure

        try:
            returncode, stdout, stderr, timed_out = self._run_operator_check(
                argv, wt, timeout=check_timeout, output_limit=check_output_limit,
            )
        except OSError as exc:
            failure = GitIntegrationCheckFailed(
                f"integration check could not start: {exc}", argv=argv,
                returncode=None, stderr=str(exc),
            )
            self._abort_and_verify(
                wt, original_head, failure, expected_ref=integration_ref
            )
            raise failure

        if timed_out:
            failure = GitIntegrationCheckTimeout(
                f"integration check timed out after {check_timeout}s",
                argv=argv, stdout=stdout, stderr=stderr,
            )
            self._abort_and_verify(
                wt, original_head, failure, expected_ref=integration_ref
            )
            raise failure
        if returncode != 0:
            failure = GitIntegrationCheckFailed(
                f"integration check exited with {returncode}", argv=argv,
                returncode=returncode, stdout=stdout, stderr=stderr,
            )
            self._abort_and_verify(
                wt, original_head, failure, expected_ref=integration_ref
            )
            raise failure

        mutations: list[str] = []
        try:
            current_head = self._git("rev-parse", "HEAD", cwd=wt)
            if current_head != original_head:
                mutations.append(f"HEAD {original_head} -> {current_head}")
        except GitError as exc:
            mutations.append(f"HEAD became unreadable: {exc}")
        try:
            current_symbolic_head = self._symbolic_head(wt)
            if current_symbolic_head != integration_ref:
                mutations.append(
                    f"symbolic HEAD {integration_ref} -> {current_symbolic_head}"
                )
        except GitError as exc:
            mutations.append(f"symbolic HEAD became unreadable: {exc}")
        try:
            current_index = self._git("write-tree", cwd=wt)
            if current_index != merge_index:
                mutations.append(f"index {merge_index} -> {current_index}")
        except GitError as exc:
            mutations.append(f"index became unreadable: {exc}")
        try:
            tracked_diff = self._tracked_worktree_diff(wt)
            if tracked_diff:
                mutations.append(f"tracked worktree changed: {tracked_diff}")
        except GitError as exc:
            mutations.append(f"tracked worktree became unreadable: {exc}")
        try:
            current_heads, current_message = self._merge_metadata_snapshot(
                wt, expected_merge_head
            )
            if current_heads != merge_heads:
                mutations.append(
                    f"MERGE_HEAD {merge_heads!r} -> {current_heads!r}"
                )
            if current_message != merge_message:
                mutations.append("MERGE_MSG changed")
        except (GitError, OSError) as exc:
            mutations.append(f"MERGE_HEAD/MERGE_MSG became invalid: {exc}")
        if mutations:
            failure = GitIntegrationMutation(
                "integration check mutated its candidate merge", argv=argv,
                stdout=stdout, stderr=stderr, mutations=mutations,
            )
            self._abort_and_verify(
                wt, original_head, failure, expected_ref=integration_ref
            )
            raise failure

        return self._commit_merge_candidate_cas(
            wt, integration_branch, original_head, merge_index,
            merge_heads=merge_heads, commit_message=msg,
            candidate_attestor=candidate_attestor,
        )

    def _worktree_git_path(self, worktree: str, name: str) -> Path:
        raw = self._git("rev-parse", "--git-path", name, cwd=worktree)
        path = Path(raw)
        return path if path.is_absolute() else Path(worktree, path)

    def _symbolic_head(self, worktree: str) -> str:
        """Return the exact symbolic ref checked out by one worktree."""
        return self._git("symbolic-ref", "-q", "HEAD", cwd=worktree)

    def _require_symbolic_head(
        self, worktree: str, expected_ref: str, context: str,
    ) -> str:
        """Reject detached or retargeted integration worktree HEAD identity."""
        try:
            actual_ref = self._symbolic_head(worktree)
        except GitError as exc:
            raise GitIntegrationPreconditionError(
                f"integration worktree HEAD is not symbolic {context}"
            ) from exc
        if actual_ref != expected_ref:
            raise GitIntegrationPreconditionError(
                f"integration worktree HEAD changed {context}: "
                f"expected {expected_ref}, got {actual_ref}"
            )
        return actual_ref

    def _merge_metadata_snapshot(
        self, worktree: str, expected_merge_head: str,
    ) -> tuple[tuple[str, ...], str]:
        """Freeze and validate the candidate's non-worktree Git metadata."""

        try:
            merge_heads = tuple(
                line.strip()
                for line in self._worktree_git_path(
                    worktree, "MERGE_HEAD"
                ).read_text().splitlines()
                if line.strip()
            )
            merge_message_path = self._worktree_git_path(worktree, "MERGE_MSG")
            merge_message = merge_message_path.read_text()
        except OSError as exc:
            raise GitIntegrationPreconditionError(
                f"cannot read merge candidate metadata: {exc}"
            ) from exc
        if merge_heads != (expected_merge_head,):
            raise GitIntegrationPreconditionError(
                "merge candidate parent identity changed: "
                f"expected {(expected_merge_head,)!r}, got {merge_heads!r}"
            )
        return merge_heads, merge_message

    def _discard_merge_candidate_without_ref_update(
        self, worktree: str, integration_ref: str, cause: BaseException,
    ) -> None:
        """Align index/worktree to the current ref without ever rewriting it.

        This is the CAS-failure path: another writer already owns the ref.  Using
        ``merge --abort`` or ``reset --hard`` could move that ref back and erase
        the winner, so cleanup is limited to merge-state removal plus ``read-tree``.
        """
        problems: list[str] = []
        current_tip: str | None = None
        try:
            current_tip = self._git("rev-parse", integration_ref, cwd=worktree)
        except GitError as exc:
            problems.append(f"cannot read winning integration ref: {exc}")
        if self._merge_in_progress(worktree):
            try:
                self._git("merge", "--quit", cwd=worktree)
            except GitError as exc:
                problems.append(f"merge --quit failed: {exc}")
        if current_tip is not None:
            try:
                self._git("read-tree", "--reset", "-u", current_tip, cwd=worktree)
            except GitError as exc:
                problems.append(f"cannot align worktree to winning ref: {exc}")
        try:
            actual_head = self._git("rev-parse", "HEAD", cwd=worktree)
            if current_tip is not None and actual_head != current_tip:
                problems.append(f"HEAD is {actual_head}, winner is {current_tip}")
        except GitError as exc:
            actual_head = None
            problems.append(f"cannot read HEAD after CAS failure: {exc}")
        if self._merge_in_progress(worktree):
            problems.append("MERGE_HEAD still exists after CAS failure")
        try:
            dirty = self._tracked_status(worktree)
            if dirty:
                problems.append(f"tracked tree is not clean after CAS failure: {dirty}")
        except GitError as exc:
            problems.append(f"cannot prove tracked tree clean after CAS failure: {exc}")
        if problems:
            raise GitRollbackError(
                "integration CAS cleanup proof failed: " + "; ".join(problems),
                cause=cause, expected_head=current_tip or "<unreadable>",
                actual_head=actual_head, problems=problems,
            ) from cause

    def _commit_merge_candidate_cas(
        self, worktree: str, integration_branch: str,
        original_head: str, merge_index: str,
        *, merge_heads: Sequence[str], commit_message: str,
        candidate_attestor: Callable[[str, str], None] | None = None,
    ) -> str:
        """Create a fixed-parent merge commit, then install it with one ref CAS.

        ``git commit`` reads symbolic HEAD again immediately before updating it;
        an external ref move could therefore become an unaudited first parent.
        ``commit-tree`` freezes the audited tree and parents without changing refs,
        and ``update-ref <new> <expected-old>`` is the single atomic publication.
        """
        integration_ref = f"refs/heads/{integration_branch}"
        if len(merge_heads) != 1 or not merge_heads[0]:
            failure = GitIntegrationPreconditionError(
                "checked merge parent snapshot is incomplete"
            )
            self._abort_and_verify(
                worktree, original_head, failure, expected_ref=integration_ref
            )
            raise failure
        try:
            self._require_symbolic_head(
                worktree, integration_ref, "before merge publication"
            )
        except GitError as failure:
            self._abort_and_verify(
                worktree, original_head, failure, expected_ref=integration_ref
            )
            raise

        args = [*self._IDENT, "commit-tree", merge_index, "-p", original_head]
        for parent in merge_heads:
            args.extend(("-p", parent))
        args.extend(("-F", "-"))
        try:
            candidate_sha = self._git(
                *args, cwd=worktree, input_text=commit_message
            )
        except GitError as failure:
            self._abort_and_verify(
                worktree, original_head, failure, expected_ref=integration_ref
            )
            raise

        # The candidate object exists, but the integration ref still names the
        # captured base.  Persist the exact OID/tree in the trusted coordination
        # DB before publication so recovery never has to trust a discoverable
        # trailer or recompute a rerere-resolved tree.
        if candidate_attestor is not None:
            try:
                self._require_symbolic_head(
                    worktree, integration_ref, "before candidate attestation"
                )
                candidate_attestor(candidate_sha, merge_index)
                self._require_symbolic_head(
                    worktree, integration_ref, "after candidate attestation"
                )
            except Exception as exc:
                failure = (
                    exc if isinstance(exc, GitError)
                    else GitIntegrationPreconditionError(
                        f"candidate attestation failed: {exc}"
                    )
                )
                self._abort_and_verify(
                    worktree, original_head, failure, expected_ref=integration_ref
                )
                if failure is exc:
                    raise
                raise failure from exc

        try:
            self._git(
                "update-ref", integration_ref, candidate_sha, original_head,
                cwd=worktree,
            )
        except GitError as failure:
            self._discard_merge_candidate_without_ref_update(
                worktree, integration_ref, failure
            )
            raise GitIntegrationPreconditionError(
                "integration branch changed before checked merge publication"
            ) from failure

        try:
            self._git("merge", "--quit", cwd=worktree)
        except GitError as failure:
            raise GitRollbackError(
                "checked merge was published but merge state could not be cleared",
                cause=failure, expected_head=candidate_sha,
                actual_head=self._git("rev-parse", "HEAD", cwd=worktree),
                problems=(str(failure),),
            ) from failure

        current_tip = self._git("rev-parse", integration_ref, cwd=worktree)
        if current_tip != candidate_sha:
            failure = GitIntegrationPreconditionError(
                "integration branch moved immediately after checked merge CAS"
            )
            self._discard_merge_candidate_without_ref_update(
                worktree, integration_ref, failure
            )
            raise failure
        problems = []
        try:
            symbolic_head = self._symbolic_head(worktree)
            if symbolic_head != integration_ref:
                problems.append(
                    f"symbolic HEAD is {symbolic_head}, expected {integration_ref}"
                )
        except GitError as exc:
            problems.append(f"cannot prove symbolic HEAD after CAS: {exc}")
        try:
            actual_head = self._git("rev-parse", "HEAD", cwd=worktree)
            if actual_head != candidate_sha:
                problems.append(f"HEAD is {actual_head}, expected {candidate_sha}")
        except GitError as exc:
            actual_head = None
            problems.append(f"cannot read HEAD after CAS: {exc}")
        if self._merge_in_progress(worktree):
            problems.append("MERGE_HEAD remains after checked merge CAS")
        current_index = self._git("write-tree", cwd=worktree)
        if current_index != merge_index:
            problems.append(f"index is {current_index}, expected {merge_index}")
        try:
            tracked_status = self._tracked_status(worktree)
            if tracked_status:
                problems.append(
                    f"tracked status differs from published tree: {tracked_status}"
                )
        except GitError as exc:
            problems.append(f"cannot prove tracked status clean after CAS: {exc}")
        if problems:
            raise GitRollbackError(
                "checked merge publication proof failed: " + "; ".join(problems),
                cause=GitError("post-CAS verification failed"),
                expected_head=candidate_sha, actual_head=actual_head,
                problems=problems,
            )
        return candidate_sha

    @staticmethod
    def _validate_check_args(check_argv: Sequence[str], check_timeout: float,
                             check_output_limit: int) -> tuple[str, ...]:
        if isinstance(check_argv, (str, bytes)) or not check_argv:
            raise ValueError("check_argv must be a non-empty sequence of strings")
        if not all(isinstance(arg, str) for arg in check_argv):
            raise TypeError("every check_argv element must be a string")
        if not math.isfinite(check_timeout) or check_timeout <= 0:
            raise ValueError("check_timeout must be positive and finite")
        if check_output_limit <= 0:
            raise ValueError("check_output_limit must be positive")
        return tuple(check_argv)

    @staticmethod
    def _run_operator_check(argv: Sequence[str], cwd: str, *, timeout: float,
                            output_limit: int) -> tuple[int, str, str, bool]:
        """argv를 shell=False로 실행하고 각 stream을 bounded buffer로 계속 drain한다."""

        class _Bounded:
            _MARKER = b"\n...[truncated]"

            def __init__(self, limit: int):
                self.limit = limit
                self.data = bytearray()
                self.truncated = False

            def feed(self, chunk: bytes) -> None:
                remaining = self.limit - len(self.data)
                if remaining > 0:
                    self.data.extend(chunk[:remaining])
                if len(chunk) > remaining:
                    self.truncated = True

            def text(self) -> str:
                raw = bytes(self.data)
                if self.truncated:
                    keep = max(0, self.limit - len(self._MARKER))
                    raw = raw[:keep] + self._MARKER[:self.limit - keep]
                return raw.decode("utf-8", errors="replace")

        process = subprocess.Popen(
            list(argv), cwd=cwd, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            shell=False, start_new_session=True,
        )
        stdout_buf, stderr_buf = _Bounded(output_limit), _Bounded(output_limit)

        def drain(pipe, target: _Bounded) -> None:
            try:
                while True:
                    chunk = pipe.read(65_536)
                    if not chunk:
                        return
                    target.feed(chunk)
            finally:
                pipe.close()

        assert process.stdout is not None and process.stderr is not None
        readers = [
            threading.Thread(target=drain, args=(process.stdout, stdout_buf), daemon=True),
            threading.Thread(target=drain, args=(process.stderr, stderr_buf), daemon=True),
        ]
        for reader in readers:
            reader.start()
        deadline = time.monotonic() + timeout
        timed_out = False
        try:
            process.wait(timeout=max(0.0, deadline - time.monotonic()))
        except subprocess.TimeoutExpired:
            timed_out = True

        # A checker can exit after spawning a background descendant that inherited its
        # stdout/stderr. The direct process is then done, but an unbounded join would wait
        # forever for EOF. Treat the entire process group + pipe drain as one timeout budget.
        if not timed_out:
            for reader in readers:
                reader.join(timeout=max(0.0, deadline - time.monotonic()))
            timed_out = any(reader.is_alive() for reader in readers)

        if timed_out:
            try:
                os.killpg(process.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            try:
                process.wait(timeout=1.0)
            except subprocess.TimeoutExpired:
                pass
            cleanup_deadline = time.monotonic() + 1.0
            for reader in readers:
                reader.join(timeout=max(0.0, cleanup_deadline - time.monotonic()))
        return process.returncode, stdout_buf.text(), stderr_buf.text(), timed_out

    def _tracked_status(self, worktree: str) -> str:
        return self._git("status", "--porcelain", "--untracked-files=no", cwd=worktree)

    def _tracked_worktree_diff(self, worktree: str) -> list[str]:
        out = self._git("-c", "core.quotepath=false", "diff", "--name-only", cwd=worktree)
        return [line for line in out.splitlines() if line.strip()]

    def _abort_and_verify(self, worktree: str, expected_head: str,
                          cause: BaseException, *,
                          expected_ref: str | None = None) -> None:
        """Discard a candidate without ever moving the integration ref.

        ``merge --abort`` can reset a symbolic branch after an external writer
        has advanced it.  Instead, remove merge metadata and align only the
        index/worktree with the observed ref tip.  Unstaged checker mutations are
        preserved as evidence and therefore make rollback proof fail closed.
        """
        problems: list[str] = []
        actual_head: str | None = None
        try:
            actual_head = self._git("rev-parse", "HEAD", cwd=worktree)
        except GitError as exc:
            problems.append(f"cannot read HEAD: {exc}")
        try:
            unstaged = self._tracked_worktree_diff(worktree)
        except GitError as exc:
            unstaged = ["<unreadable>"]
            problems.append(f"cannot inspect tracked worktree: {exc}")
        preserve_unstaged = bool(unstaged) and isinstance(
            cause, (GitIntegrationCheckError, GitIntegrationPreconditionError)
        )
        if preserve_unstaged:
            problems.append(f"tracked tree is not clean: {unstaged}")
        elif actual_head is not None:
            try:
                if self._merge_in_progress(worktree):
                    self._git("merge", "--quit", cwd=worktree)
                self._git("read-tree", "--reset", "-u", actual_head, cwd=worktree)
            except GitError as exc:
                problems.append(f"non-ref rollback failed: {exc}")
        try:
            actual_head = self._git("rev-parse", "HEAD", cwd=worktree)
            if actual_head != expected_head:
                problems.append(f"HEAD is {actual_head}, expected {expected_head}")
        except GitError as exc:
            problems.append(f"cannot read HEAD: {exc}")
        try:
            merge_head_path = self._git("rev-parse", "--git-path", "MERGE_HEAD",
                                        cwd=worktree)
            merge_head = Path(merge_head_path)
            if not merge_head.is_absolute():
                merge_head = Path(worktree, merge_head)
            if merge_head.exists():
                problems.append("MERGE_HEAD still exists")
        except GitError as exc:
            problems.append(f"cannot prove MERGE_HEAD absent: {exc}")
        try:
            dirty = self._tracked_status(worktree)
            if dirty:
                problems.append(f"tracked status is not clean: {dirty}")
        except GitError as exc:
            problems.append(f"cannot prove tracked tree clean: {exc}")
        if expected_ref is not None:
            try:
                symbolic_head = self._symbolic_head(worktree)
                if symbolic_head != expected_ref:
                    problems.append(
                        f"symbolic HEAD is {symbolic_head}, expected {expected_ref}"
                    )
            except GitError as exc:
                problems.append(f"cannot prove symbolic HEAD after rollback: {exc}")
        if problems:
            raise GitRollbackError(
                "integration rollback proof failed: " + "; ".join(problems),
                cause=cause, expected_head=expected_head, actual_head=actual_head,
                problems=problems,
            ) from cause

    def abort_merge_verified(self, worktree: str, expected_head: str) -> None:
        """재기동 복구용 공개 seam: 후보 merge를 중단하고 영속된 pre-merge HEAD까지 증명한다.

        MERGE_HEAD가 이미 없어도 HEAD/clean tracked tree를 검증한다. 증명 실패는 destructive
        reset으로 덮지 않고 :class:`GitRollbackError`로 fail-stop한다.
        """
        cause = GitIntegrationPreconditionError(
            "recover dangling checked merge with unproven worktree"
        )
        self._abort_and_verify(str(Path(worktree).resolve()), expected_head, cause)

    def abort_merge_preserving_ref(self, worktree: str,
                                   integration_branch: str) -> None:
        """Discard dangling merge state without moving the integration ref.

        A token-only recovery has no durable pre-merge base to restore.  It must
        therefore align the index/worktree to the *currently observed* branch tip
        and fail if that tip changes, never use ``git merge --abort`` (which can
        reset a symbolic branch to stale ``ORIG_HEAD`` and erase an external win).
        """
        cause = GitIntegrationPreconditionError(
            "recover dangling merge token without a durable connect intent"
        )
        self._discard_merge_candidate_without_ref_update(
            str(Path(worktree).resolve()),
            f"refs/heads/{integration_branch}",
            cause,
        )

    def unmerged_paths(self, worktree: str) -> list[str]:
        """머지 진행중 worktree 의 미해소 충돌 경로들(구조적 판별 — diff-filter=U)."""
        out = self._git("diff", "--name-only", "--diff-filter=U", cwd=worktree)
        return [p for p in out.splitlines() if p.strip()]

    def _merge_in_progress(self, worktree: str) -> bool:
        try:
            self._git("rev-parse", "-q", "--verify", "MERGE_HEAD", cwd=worktree)
            return True
        except GitError:
            return False

    def enable_rerere(self):
        """P3 증분13(O2): rerere 활성(멱등, repo 수준 — 모든 worktree 가 rr-cache 공유).
        물방울이 rebase 로 해소한 충돌이 기록되고, 동일충돌 재발 시(재시도/통합 머지) 자동
        재적용된다. autoUpdate 로 해소 경로가 staged 까지 되어 merge_into 가 완성 가능."""
        self._git("config", "rerere.enabled", "true")
        self._git("config", "rerere.autoUpdate", "true")

    def merge_base(self, a: str, b: str, cwd: str | None = None) -> str:
        return self._git("merge-base", a, b, cwd=cwd)

    def commits_touching(self, rng: str, paths: list[str], cwd: str | None = None) -> list[dict]:
        """rng 의 **first-parent** 히스토리에서 paths 를 건드린 커밋들(진단용) —
        sha/parents/author/subject/OMD-Connect trailer. first-parent 필수: 전체 스캔은
        머지에 흡수된 드롭릿 feature 커밋을 우회로 오탐한다(bypass_audit 와 동일 규율).
        --diff-merges=first-parent 로 머지커밋도 1친 대비 diff 로 경로 필터에 걸린다."""
        fmt = "\x1e%H\x1f%P\x1f%an\x1f%s\x1f%(trailers:key=OMD-Connect,valueonly)"
        out = self._git("log", "--first-parent", "--diff-merges=first-parent",
                        f"--format={fmt}", rng, "--", *paths, cwd=cwd)
        rows = []
        for rec in out.split("\x1e"):
            rec = rec.strip("\n")
            if not rec.strip():
                continue
            sha, parents, author, subject, trailers = (rec.split("\x1f") + [""] * 5)[:5]
            rows.append({"sha": sha, "parents": tuple(parents.split()),
                         "author": author, "subject": subject,
                         "trailers": tuple(v for v in trailers.splitlines() if v.strip())})
        return rows

    def push_integration(self, integration_worktree: str, integration_branch: str,
                         remote: str, *, timeout: float | None = None) -> None:
        """통합 worktree 에서 integration_branch 를 remote 로 push(연결=merge 직후 remote sync).
        네트워크 I/O라 connect Phase B(락 밖)에서 호출. 실패(원격 이동·net)는 호출부가 fail-soft —
        merge 는 이미 로컬 반영됨이라 connect 성공은 유지. 비fast-forward면 raise(강제 push 안 함)."""
        wt = str(Path(integration_worktree).resolve())
        self._git("push", remote, integration_branch, cwd=wt, timeout=timeout)

    def branch_in_integration(self, integration_worktree: str, integration_branch: str,
                              trailer: str) -> str | None:
        """통합 브랜치에 주어진 trailer를 가진 머지 커밋이 있으면 그 sha를 반환(없으면 None).
        git=병합의 진실(§D8): `--is-ancestor`가 아니라 trailer-probe로 '이 응결이 실제 일어났나'를 본다.
        trailer는 줄 단위로 정확히 일치(`^…$`)해야 — 'A'가 'AB' 같은 prefix에 오탐되지 않게."""
        wt = str(Path(integration_worktree).resolve())
        # ERE 메타문자 이스케이프(태스크 id에 특수문자가 들어와도 정확매칭 안전).
        pat = "^" + re.sub(r"([.^$*+?()\[\]{}|\\])", r"\\\1", trailer) + "$"
        try:
            out = self._git("log", integration_branch, f"--grep={pat}", "-E",
                            "--format=%H", "-n", "1", cwd=wt)
        except GitError:
            return None
        return out or None

    def commit_has_trailers(self, worktree: str, commit: str,
                            trailers: Sequence[str]) -> bool:
        """Verify exact generated trailer lines on one recovered commit."""
        try:
            body = self._git("show", "-s", "--format=%B", commit, cwd=worktree)
        except GitError:
            return False
        lines = {line.strip() for line in body.splitlines() if line.strip()}
        return all(trailer in lines for trailer in trailers)

    def commit_parents(self, worktree: str, commit: str) -> tuple[str, ...]:
        """Return one commit's ordered, immutable parent identity."""
        parents = self._git(
            "show", "-s", "--format=%P", commit, cwd=worktree
        )
        return tuple(parents.split())

    def commit_tree(self, worktree: str, commit: str) -> str:
        """Return one commit's full tree OID with replacement refs disabled."""
        return self._git("rev-parse", f"{commit}^{{tree}}", cwd=worktree)

    def is_ancestor(self, ancestor: str, descendant: str, cwd=None) -> bool:
        env = os.environ.copy()
        env["GIT_NO_REPLACE_OBJECTS"] = "1"
        env["GIT_GRAFT_FILE"] = os.devnull
        try:
            subprocess.run(
                ["git", "merge-base", "--is-ancestor", ancestor, descendant],
                cwd=cwd or self.root, check=True, capture_output=True, text=True,
                env=env,
            )
            return True
        except subprocess.CalledProcessError as exc:
            if exc.returncode == 1:
                return False
            raise GitError(
                f"git merge-base --is-ancestor failed: {exc.stderr.strip()}"
            ) from exc

    def has_merge_in_progress(self, worktree: str) -> bool:
        """worktree에 dangling MERGE_HEAD(중단 안 된 머지)가 있나 — 복구가 abort 판정."""
        try:
            gitdir = self._git("rev-parse", "--git-path", "MERGE_HEAD", cwd=worktree)
        except GitError:
            return False
        return Path(worktree, gitdir).exists() or Path(gitdir).exists()

    def abort_merge(self, worktree: str):
        """진행중 머지를 중단(멱등 — 머지가 없으면 조용히 무시). 복구·reclaim용."""
        try:
            self._git("merge", "--abort", cwd=worktree)
        except GitError:
            pass

    def remove_worktree(self, path: str, *, force: bool = False,
                        ignore_errors: bool = False) -> bool:
        resolved = str(Path(path).resolve())
        if not Path(resolved).exists():
            return True
        args = ["worktree", "remove"]
        if force:
            args.append("--force")
        args.append(resolved)
        try:
            self._git(*args)
        except GitError:
            if ignore_errors:
                return False
            raise
        return True

    def branch_exists(self, branch: str) -> bool:
        try:
            self._git("rev-parse", "--verify", "--quiet", f"refs/heads/{branch}")
            return True
        except GitError:
            return False

    def delete_branch(self, branch: str):
        """requeue 시 omd/<task> 브랜치 삭제(P0-8). 안 지우면 다음 start()의
        `worktree add -b <branch>`가 '브랜치 이미 존재'로 실패해 task가 영구 wedge된다."""
        try:
            self._git("branch", "-D", branch)
        except GitError:
            pass
