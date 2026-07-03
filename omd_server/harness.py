"""OMD 문제점(P1/P2/P4) 해결 하네스 — 단일 엔트리포인트.

  python -m omd_server.harness --repo . --branch <통합브랜치> --since <ref>

P1 우회 감지 + P2 hot공유파일 + P4 스펙 적합성 게이트를 한 번에 돌려 종합 GO/NO_GO.
각 게이트는 fail-loud(변환-불변식 위반/회귀 시 NO_GO). 종합 exit code = 하나라도 NO_GO 면 비영점.
(P5 strict_writeset·complete_task 는 런타임 Coordinator 동작이라 pytest 로 검증 — 여기선 P4
적합성 게이트가 그 코드 존재를 회귀가드한다.)
"""
from __future__ import annotations

import argparse
import sys

from . import bypass_audit, conformance, hot_files


def run(repo: str, branch: str | None, since: str | None, *,
        min_adoption: float = 1.0, hot_threshold: int = 3,
        max_hot: int | None = None, out=sys.stderr) -> int:
    print("=" * 64, file=out)
    print("OMD 문제 해결 하네스 — P1(우회)/P2(hot파일)/P4(적합성) 종합 게이트", file=out)
    print("=" * 64, file=out)
    rc: dict = {}

    print("\n[1/3] P1 — OMD 우회 감지", file=out)
    if branch:
        rc["p1_bypass"] = bypass_audit.gate(repo, branch, since, min_adoption=min_adoption, out=out)
    else:
        print("  SKIP: --branch 미지정(보호 통합브랜치 없음). P1 게이트 건너뜀.", file=out)
        rc["p1_bypass"] = 0

    print("\n[2/3] P2 — hot/공유파일 경합", file=out)
    rc["p2_hot"] = hot_files.gate(repo, since, threshold=hot_threshold, max_hot=max_hot, out=out)

    print("\n[3/3] P4 — 스펙↔구현 적합성", file=out)
    rc["p4_conformance"] = conformance.gate(out=out)

    worst = max(rc.values()) if rc else 0
    print("\n" + "=" * 64, file=out)
    summary = "  ".join(f"{k}={'GO' if v == 0 else f'NO_GO({v})'}" for k, v in rc.items())
    print(f"종합: {'GO ✅' if worst == 0 else 'NO_GO 🔴'}   {summary}", file=out)
    print("=" * 64, file=out)
    return worst


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description="OMD 문제 해결 하네스 종합 게이트")
    ap.add_argument("--repo", default=".")
    ap.add_argument("--branch", default=None, help="보호 통합브랜치(P1). 미지정시 P1 skip")
    ap.add_argument("--since", default=None, help="이 ref 이후만 검사(P1/P2 noise 차단)")
    ap.add_argument("--min-adoption", type=float, default=1.0)
    ap.add_argument("--hot-threshold", type=int, default=3)
    ap.add_argument("--max-hot", type=int, default=None)
    a = ap.parse_args(argv)
    return run(a.repo, a.branch, a.since, min_adoption=a.min_adoption,
               hot_threshold=a.hot_threshold, max_hot=a.max_hot, out=sys.stdout)


if __name__ == "__main__":
    raise SystemExit(main())
