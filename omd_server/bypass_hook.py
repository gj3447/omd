"""P1 — 우회 커밋의 보호 통합브랜치 push 를 막는 git pre-push hook 생성기/설치기.

git pre-push stdin 프로토콜: 줄마다 `<local_ref> <local_sha> <remote_ref> <remote_sha>`.
push 범위(remote_sha..local_sha; 신규브랜치면 since_ref..local_sha)의 first-parent 에
우회 커밋이 있으면 exit 1 로 push 거부. 우회판정은 bypass_audit.gate 재사용(단일 정본).
"""
from __future__ import annotations

import sys
from pathlib import Path

from .bypass_audit import gate
from .gitio import GitRepo

ZERO = "0" * 40

_HOOK_TMPL = """#!/usr/bin/env bash
# [OMD P1 bypass guard] 우회 커밋(OMD 안 거친 직접커밋/foreign merge)을 보호 통합브랜치 push 시
# 검사. WARN=1 이면 경고만(allow), 아니면 거부(exit 1). (채택 0% 브랜치는 WARN 으로 시작.)
set -euo pipefail
PROTECTED_REF="refs/heads/{branch}"
SINCE="{since_ref}"
PY="{python}"
ZERO="{zero}"
WARN="{warn}"
while read -r local_ref local_sha remote_ref remote_sha; do
  [ "$remote_ref" = "$PROTECTED_REF" ] || continue
  [ "$local_sha" = "$ZERO" ] && continue            # 브랜치 삭제 push
  if [ "$remote_sha" = "$ZERO" ]; then base="$SINCE"; else base="$remote_sha"; fi
  "$PY" -m omd_server.bypass_hook _check "$(git rev-parse --show-toplevel)" "{branch}" "$base" "$local_sha" "$WARN" || exit 1
done
exit 0
"""


def generate_pre_push_hook(integration_branch: str, since_ref: str = "",
                           python: str = "python3", warn_only: bool = False) -> str:
    return _HOOK_TMPL.format(branch=integration_branch, since_ref=since_ref,
                             zero=ZERO, python=python, warn="1" if warn_only else "")


def install_pre_push_hook(repo: str, integration_branch: str, since_ref: str = "",
                          python: str = "python3", warn_only: bool = False) -> str:
    """repo 의 **repo-local** .git/hooks/pre-push 에 설치(기존 있으면 덮어씀). 설치 경로 반환.

    ⚠️ 반드시 `--git-common-dir`(=repo 의 실제 .git, worktree 도 안전)로 repo-local hooks 에
    쓴다. `--git-path hooks` 는 core.hooksPath 가 설정돼 있으면 **그 (공유/글로벌) 경로**를
    돌려줘 남의 hooks 를 덮어버리는 footgun — 절대 쓰지 않는다. 만약 core.hooksPath 가
    repo-local hooks 를 가리지 않으면(글로벌 override 활성) git 은 이 pre-push 를 호출하지
    않으므로 stderr 경고 — 그 경우 CLI 게이트(scripts/omd_bypass_gate.py)를 CI 에서 쓰거나
    repo-local `git config core.hooksPath .git/hooks` 를 설정해야 발화한다.
    """
    git = GitRepo(repo)
    common = Path(git._git("rev-parse", "--git-common-dir"))
    if not common.is_absolute():
        common = Path(git.root) / common
    hooks_dir = common / "hooks"
    hooks_dir.mkdir(parents=True, exist_ok=True)
    p = hooks_dir / "pre-push"
    p.write_text(generate_pre_push_hook(integration_branch, since_ref, python, warn_only))
    p.chmod(0o755)
    # core.hooksPath 가 이 repo-local hooks 를 안 가리면 git 이 이 hook 을 안 부른다 → 경고.
    eff = Path(git._git("rev-parse", "--git-path", "hooks"))
    if not eff.is_absolute():
        eff = Path(git.root) / eff
    if eff.resolve() != hooks_dir.resolve():
        print(f"[omd-bypass] ⚠️ core.hooksPath={eff} 가 설정돼 이 pre-push 는 push 시 자동발화 "
              f"안 함. CLI 게이트(scripts/omd_bypass_gate.py)를 CI 에 쓰거나 "
              f"repo-local `git config core.hooksPath {hooks_dir}` 설정.", file=sys.stderr)
    return str(p)


def _check(repo: str, branch: str, base: str, local_sha: str, warn_only: bool = False) -> int:
    """hook 내부 호출: base..local_sha 범위의 first-parent 우회 검사. base 가 빈값이면 전수.
    push 범위만 보도록 local_sha 를 tip(=gate 의 branch 인자)으로 검사한다. warn_only 면 경고만."""
    since = base if base and base != ZERO else None
    return gate(repo, local_sha, since, warn_only=warn_only, out=sys.stderr)


def main(argv: list[str] | None = None) -> int:
    argv = list(sys.argv[1:] if argv is None else argv)
    if argv and argv[0] == "_check":
        # _check <repo> <branch> <base> <local_sha> [warn(1/'')]
        repo, branch, base, local_sha = argv[1], argv[2], argv[3], argv[4]
        warn_only = len(argv) > 5 and argv[5] == "1"
        return _check(repo, branch, base, local_sha, warn_only)
    print("usage: python -m omd_server.bypass_hook _check <repo> <branch> <base> <local_sha> [warn]",
          file=sys.stderr)
    return 64


if __name__ == "__main__":
    raise SystemExit(main())
