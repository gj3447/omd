"""실제 git 연동 — 물방울 worktree(자존자 격리) + CLOUD CONNECT(응결=merge).

OMD의 명제를 실물 git에서 집행: 각 물방울은 독립 worktree+브랜치에서 운행하고,
연결은 통합 브랜치로의 `git merge`. write-set이 입체(서로소)면 이 merge는 무충돌.
"""

from __future__ import annotations

import re
import subprocess
from pathlib import Path


class GitError(RuntimeError):
    pass


class GitTimeout(GitError):
    """merge 서브프로세스가 타임아웃(§E — 무한 hang 방지). abort 대상."""


class GitRepo:
    """통합 브랜치를 보유한 메인 레포. 물방울 worktree를 발사/응결.

    응결(merge)은 **사용자 HEAD를 절대 안 건드리는** 전용 통합 worktree에서 한다(§D11):
    `<root>-omd-integration` 을 integration_branch 로 체크아웃해 두고, 거기서만
    `checkout integration_branch` + `merge --no-ff`. 동시 connect는 repo-wide merge_token
    (DB 측 Semaphore max=1)이 직렬화하므로 통합 worktree의 .git/index 경합이 없다.
    """

    # 테스트·헤드리스에서 전역 git config 없이도 커밋되게 신원 주입
    _IDENT = ["-c", "user.name=omd", "-c", "user.email=omd@acme"]

    def __init__(self, root: str):
        self.root = str(Path(root).resolve())

    def _git(self, *args, cwd=None, timeout=None) -> str:
        try:
            r = subprocess.run(
                ["git", *args], cwd=cwd or self.root,
                capture_output=True, text=True, timeout=timeout,
            )
        except subprocess.TimeoutExpired:
            raise GitTimeout(f"git {' '.join(args)} → timeout after {timeout}s")
        if r.returncode != 0:
            raise GitError(f"git {' '.join(args)} → {r.returncode}: {r.stderr.strip()}")
        return r.stdout.strip()

    def current_branch(self, cwd=None) -> str:
        return self._git("rev-parse", "--abbrev-ref", "HEAD", cwd=cwd)

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
        """worktree의 모든 변경을 스테이지+커밋. 빈 변경이면 GitError."""
        self._git("add", "-A", cwd=worktree)
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
                   branch: str, msg: str, *, timeout: float | None = None) -> str:
        """전용 통합 worktree에서 integration_branch에 branch를 --no-ff merge(§D11).
        Phase B 본체 — **락 밖**에서 호출된다. 충돌/타임아웃이면 `merge --abort` 후 raise.
        msg에 고유 trailer를 넣어 두면 복구가 통합 브랜치에서 머지 여부를 trailer-probe 한다."""
        wt = str(Path(integration_worktree).resolve())
        # 명시적으로 integration_branch를 체크아웃(사용자 HEAD가 아님을 보장).
        self._git("checkout", integration_branch, cwd=wt)
        try:
            self._git(*self._IDENT, "merge", "--no-ff", "-m", msg, branch,
                      cwd=wt, timeout=timeout)
        except GitError as e:
            try:
                self._git("merge", "--abort", cwd=wt)
            except GitError:
                pass
            if isinstance(e, GitTimeout):
                raise GitTimeout(f"merge timeout on {branch}: {e}")
            raise GitError(f"merge conflict on {branch}: {e}")
        return self._git("rev-parse", "HEAD", cwd=wt)

    def undo_last_commit(self, worktree: str) -> None:
        """방금 commit_all 한 커밋을 되돌리되 working/staged 변경은 보존(P5 strict-writeset 롤백).
        `git reset --soft HEAD~1` — 에이전트가 궤도-밖 경로만 빼고 재커밋 가능. 부모 없으면(첫 커밋)
        GitError → 전파(fail-loud; 드롭릿 브랜치는 base 가 항상 있어 정상)."""
        self._git("reset", "--soft", "HEAD~1", cwd=str(Path(worktree).resolve()))

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

    def remove_worktree(self, path: str):
        try:
            self._git("worktree", "remove", "--force", str(Path(path).resolve()))
        except GitError:
            pass

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
