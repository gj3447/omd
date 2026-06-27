"""라이브 멀티에이전트 병렬-dev 세션 회귀가드 — 실 git 머지로 SINGULON(Δ분열=0) 고정.

scripts/multiagent_parallel_session.py — 4 물방울이 서로소 모듈을 실 worktree 에서 개발 후 동시 connect
→ 실 `git merge --no-ff` 4회, 통합브랜치 4파일, 충돌 0, merge_token 상호배제(max held=1), 겹침 직렬화.
"""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scripts"))

from multiagent_parallel_session import run_session  # noqa: E402


def test_disjoint_agents_merge_without_conflict():
    r = run_session(4)
    assert r["all_merged"] is True, r["merged_states"]
    assert r["real_merge_commits"] == 4, r
    assert r["files_in_integration"] == 4, r
    assert r["integration_worktree_clean"] is True, "통합 worktree 더러움 = 머지충돌/index 오염"
    assert r["merge_token_max_held"] == 1, "동시 머지 상호배제 위반(P0-5)"
    assert r["merge_token_leak"] is False, "merge_token 누수"
    assert r["overlap_serialized"] is True, "겹치는 write-set 이 직렬화 안 됨"
    ev = r["ltdd_events"]
    assert ev["task_committed"] == 4 and ev["connect_merged"] == 4, ev
