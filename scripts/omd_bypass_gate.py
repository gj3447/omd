#!/usr/bin/env python3
"""P1 — OMD 우회 감지 fail-loud CLI 게이트 (CI/수동·bpc_field_deploy_gate.py 류).

보호 통합브랜치 first-parent(since..) 에 OMD 안 거친 우회 커밋이 있으면 비영점 exit.
사용:
  python scripts/omd_bypass_gate.py --repo . --branch kjra --since <OMD도입ref> [--min-adoption 1.0]
  python scripts/omd_bypass_gate.py --repo . --branch main --since HEAD~50 --install-hook
"""
import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from omd_server.bypass_audit import gate  # noqa: E402
from omd_server.bypass_hook import install_pre_push_hook  # noqa: E402


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description="OMD 우회 감지 게이트")
    ap.add_argument("--repo", default=".")
    ap.add_argument("--branch", required=True, help="보호 통합브랜치 (예: kjra/main)")
    ap.add_argument("--since", default=None, help="이 ref 이후만 강제(미설정시 전수 — noise 주의)")
    ap.add_argument("--min-adoption", type=float, default=1.0)
    ap.add_argument("--install-hook", action="store_true",
                    help="검사 대신 pre-push hook 설치")
    a = ap.parse_args(argv)
    if a.install_hook:
        p = install_pre_push_hook(a.repo, a.branch, a.since or "", sys.executable)
        print(f"[omd-bypass] pre-push hook 설치: {p}")
        return 0
    return gate(a.repo, a.branch, a.since, min_adoption=a.min_adoption)


if __name__ == "__main__":
    raise SystemExit(main())
