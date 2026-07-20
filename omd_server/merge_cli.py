"""omd-merge — checked-merge 단독 CLI (coordinator DB 미접촉 merge-gate).

`gitio.GitRepo._merge_into_checked`(merge_into 의 check_argv 경로)를 trunk-based
워크플로에서 그대로 쓴다: **verify green + 무변이(check 읽기전용)일 때만** merge
commit 이 생기고, red/timeout/mutation 이면 `merge --abort` 후 원 HEAD + no
MERGE_HEAD + clean tracked tree 를 재증명하고 비정상 종료한다. 충돌도 abort.

대상 브랜치 = `repo` 에 현재 체크아웃된 브랜치(HEAD). 통합 worktree 를 따로 만들지
않고 repo 자체를 gate 장소로 쓴다 — precondition(clean tracked tree)은
`_merge_into_checked` 가 그대로 집행한다. coord DB(store/SQLite)는 일절 안 만진다.

exit code:
  0 = merged (merged_sha 명시; already-up-to-date 포함)
  2 = verify red/timeout/mutation → abort + 원상복구 증명 완료 (restored HEAD 명시)
  3 = merge conflict → abort + 원상복구 (충돌 경로 명시)
  1 = 기타 (usage / precondition / rollback 증명 실패 / git 오류)
"""

from __future__ import annotations

import argparse
import json
import os
import shlex
import sys

from .gitio import (
    GitError,
    GitIntegrationCheckError,
    GitMergeConflict,
    GitRepo,
    GitRollbackError,
)

EXIT_MERGED = 0
EXIT_ERROR = 1
EXIT_VERIFY_ABORT = 2
EXIT_CONFLICT_ABORT = 3


def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="omd-merge",
        description=("checked merge-gate: verify green일 때만 --source를 현재 브랜치에 "
                     "응결(merge commit). red/변이/충돌이면 abort + 원상복구 증명."),
    )
    p.add_argument("repo", nargs="?", default=None,
                   help="git repo 경로 (기본: 현재 디렉터리). 현재 체크아웃된 브랜치가 merge 대상.")
    p.add_argument("--source", required=True,
                   help="merge할 ref (브랜치/sha). 현재 브랜치로 --no-ff merge된다.")
    p.add_argument("--verify-cmd", dest="verify_cmd", default=None, metavar="ARGV",
                   help="green-gate 검증 명령 (shlex.split, shell 미경유). "
                        "이 도구의 존재이유 — 생략은 --no-verify로만 가능.")
    p.add_argument("--no-verify", action="store_true",
                   help="명시적 green-gate 우회 (검증 없이 일반 --no-ff merge).")
    p.add_argument("--timeout", type=float, default=1800.0, metavar="SECONDS",
                   help="merge/verify 각각의 제한 시간(초, 기본 1800).")
    p.add_argument("--json", dest="as_json", action="store_true",
                   help="결과를 JSON 한 덩어리로 stdout에 출력.")
    return p


def _emit(result: dict, as_json: bool, *, ok: bool) -> None:
    if as_json:
        print(json.dumps(result, ensure_ascii=False, indent=2))
        return
    stream = sys.stdout if ok else sys.stderr
    for key, value in result.items():
        print(f"{key}: {value}", file=stream)


def main(argv=None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)

    result: dict = {}

    # usage 검증은 argparse.error(exit 2) 대신 직접 한다 — exit 2는 이 CLI에서
    # 'verify red abort'의 의미로 예약돼 있어 usage 오류와 섞이면 안 된다.
    if args.no_verify and args.verify_cmd is not None:
        result.update(status="usage_error",
                      error="--verify-cmd and --no-verify are mutually exclusive")
        _emit(result, args.as_json, ok=False)
        return EXIT_ERROR
    check_argv: list[str] | None = None
    if not args.no_verify:
        if args.verify_cmd is None:
            result.update(status="usage_error",
                          error="--verify-cmd is required (green-gate is the point of "
                                "omd-merge); bypass only via explicit --no-verify")
            _emit(result, args.as_json, ok=False)
            return EXIT_ERROR
        check_argv = shlex.split(args.verify_cmd)
        if not check_argv:
            result.update(status="usage_error", error="--verify-cmd is empty after shlex.split")
            _emit(result, args.as_json, ok=False)
            return EXIT_ERROR

    git = GitRepo(args.repo if args.repo is not None else os.getcwd())
    result.update(repo=git.root, source=args.source)
    original_head: str | None = None
    try:
        target = git.current_branch()
        if target == "HEAD":
            raise GitError("detached HEAD — omd-merge merges into the currently "
                           "checked-out branch; check out a branch first")
        result["target"] = target
        original_head = git.branch_tip(target, strict=True)
        result["original_head"] = original_head

        msg = f"omd-merge: {args.source} -> {target}"
        if check_argv is None:
            sha = git.merge_into(git.root, target, args.source, msg,
                                 timeout=args.timeout)
        else:
            sha = git.merge_into(git.root, target, args.source, msg,
                                 timeout=args.timeout, check_argv=check_argv,
                                 check_timeout=args.timeout)
    except GitMergeConflict as exc:
        # merge_into가 abort까지 마친 뒤 노출하는 타입 — 원 HEAD로 복구된 상태.
        result.update(status="conflict_aborted", error=str(exc),
                      conflicts=list(exc.conflicts), restored_head=original_head)
        _emit(result, args.as_json, ok=False)
        return EXIT_CONFLICT_ABORT
    except GitIntegrationCheckError as exc:
        # 이 타입이 노출됐다 = merge abort + 원상복구 증명이 끝났다(gitio 계약).
        # GitIntegrationCheckTimeout/GitIntegrationMutation도 여기로 온다.
        result.update(
            status="verify_aborted", error=str(exc), verify_argv=list(exc.argv),
            verify_returncode=exc.returncode, verify_stdout=exc.stdout,
            verify_stderr=exc.stderr, restored_head=original_head,
            rollback_proven=True,
        )
        _emit(result, args.as_json, ok=False)
        return EXIT_VERIFY_ABORT
    except GitRollbackError as exc:
        # 원상복구를 증명 못 함 — fail-stop. 운영자 조사 필요. '기타'로 분류.
        result.update(status="rollback_unproven", error=str(exc),
                      expected_head=exc.expected_head, actual_head=exc.actual_head,
                      problems=list(exc.problems))
        _emit(result, args.as_json, ok=False)
        return EXIT_ERROR
    except GitError as exc:
        # precondition(dirty tree 등)/merge 자체 timeout/기타 git 오류.
        result.update(status="error", error=str(exc))
        _emit(result, args.as_json, ok=False)
        return EXIT_ERROR

    result.update(status="merged", merged_sha=sha,
                  up_to_date=(sha == original_head))
    _emit(result, args.as_json, ok=True)
    return EXIT_MERGED


if __name__ == "__main__":
    sys.exit(main())
