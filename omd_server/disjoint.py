"""입체(disjoint) 판정 — glob write-set 교집합.

OMD의 핵심 IP. 시판 락/lease(etcd·Consul·Redis…)는 전부 single-key TTL뿐
glob-overlap leasing을 안 줌 (deep-research wlgj8e126 확인). 그래서 자체 구현.

설계 원칙 — **soundness over completeness**: 절대 false-negative를 내지 않는다
(겹치는데 안 겹친다고 판정 금지). 보수적으로 over-report는 허용(병렬도만 약간 손해,
SINGULON 불변식=분열0은 깨지지 않음). 현재 구현 = 와일드카드-앞 literal prefix의
조상/동일 관계로 판정. 더 정밀한 판정(PostgreSQL SSI predicate lock 류)은 후속 과제.
"""

from __future__ import annotations

import re

_WILD = re.compile(r"[*?\[]")


def glob_prefix(g: str) -> str:
    """glob의 첫 와일드카드 앞 디렉토리 prefix. 와일드카드로 시작하면 '' (=전체)."""
    g = g.strip().lstrip("./")
    m = _WILD.search(g)
    if not m:
        return g.rstrip("/")
    head = g[: m.start()]
    if "/" in head:
        return head.rsplit("/", 1)[0].rstrip("/")
    return ""  # 와일드카드가 첫 세그먼트 → 전체를 건드릴 수 있음


def _prefix_conflict(a: str, b: str) -> bool:
    if a == "" or b == "":
        return True  # 한쪽이 전체-매칭 → 무조건 겹침(보수적)
    if a == b:
        return True
    return a.startswith(b + "/") or b.startswith(a + "/")


def globs_overlap(g1: str, g2: str) -> bool:
    """두 glob이 공통 경로를 매칭할 수 있으면 True (보수적)."""
    if g1 == g2:
        return True
    return _prefix_conflict(glob_prefix(g1), glob_prefix(g2))


def sets_overlap(s1, s2) -> bool:
    """두 write-set(glob 리스트)이 입체(서로소)가 아니면 True."""
    return any(globs_overlap(a, b) for a in s1 for b in s2)
