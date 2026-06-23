"""실제 git 연동 — 물방울 worktree(자존자 격리) + CLOUD CONNECT(응결=merge).

OMD의 명제를 실물 git에서 집행: 각 물방울은 독립 worktree+브랜치에서 운행하고,
연결은 통합 브랜치로의 `git merge`. write-set이 입체(서로소)면 이 merge는 무충돌.
"""

from __future__ import annotations

import subprocess
from pathlib import Path


class GitError(RuntimeError):
    pass


class GitRepo:
    """통합 브랜치를 보유한 메인 레포. 물방울 worktree를 발사/응결."""

    # 테스트·헤드리스에서 전역 git config 없이도 커밋되게 신원 주입
    _IDENT = ["-c", "user.name=omd", "-c", "user.email=omd@airobotics"]

    def __init__(self, root: str):
        self.root = str(Path(root).resolve())

    def _git(self, *args, cwd=None) -> str:
        r = subprocess.run(
            ["git", *args], cwd=cwd or self.root,
            capture_output=True, text=True,
        )
        if r.returncode != 0:
            raise GitError(f"git {' '.join(args)} → {r.returncode}: {r.stderr.strip()}")
        return r.stdout.strip()

    def current_branch(self) -> str:
        return self._git("rev-parse", "--abbrev-ref", "HEAD")

    def add_worktree(self, branch: str, path: str, base: str = "HEAD") -> str:
        """base에서 새 branch를 만들어 path에 linked worktree로 체크아웃."""
        self._git("worktree", "add", "-b", branch, str(Path(path).resolve()), base)
        return str(Path(path).resolve())

    def commit_all(self, worktree: str, msg: str) -> str:
        """worktree의 모든 변경을 스테이지+커밋. 빈 변경이면 GitError."""
        self._git("add", "-A", cwd=worktree)
        self._git(*self._IDENT, "commit", "-m", msg, cwd=worktree)
        return self._git("rev-parse", "HEAD", cwd=worktree)

    def merge(self, branch: str, msg: str) -> str:
        """통합 브랜치(root)에 branch를 --no-ff merge. 충돌이면 abort 후 GitError."""
        try:
            self._git(*self._IDENT, "merge", "--no-ff", "-m", msg, branch)
        except GitError as e:
            try:
                self._git("merge", "--abort")
            except GitError:
                pass
            raise GitError(f"merge conflict on {branch}: {e}")
        return self._git("rev-parse", "HEAD")

    def remove_worktree(self, path: str):
        try:
            self._git("worktree", "remove", "--force", str(Path(path).resolve()))
        except GitError:
            pass
