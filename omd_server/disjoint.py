"""입체(disjoint) 판정 — glob write-set 교집합. OMD의 핵심 IP.

시판 락/lease(etcd·Consul·Redis…)는 전부 single-key TTL뿐 glob-overlap leasing을
안 줌 (deep-research wlgj8e126 확인). 그래서 자체 구현.

`globs_overlap(g1,g2)` = "두 glob을 동시에 만족하는 경로가 존재하는가" (=교집합 비공집합).
세그먼트('/') 단위로 분해해 패턴-교집합을 정확히 계산:
  - `**` : 0개 이상의 세그먼트 흡수
  - `*`  : 한 세그먼트 내 0+ 문자(/ 불포함)
  - `?`  : 한 문자
  - 문자클래스 `[...]` 포함 시: **보수적으로 overlap=True** (soundness 우선 — false-negative 금지).

**불변식 안전성**: 절대 false-negative를 내지 않는다(겹치는데 안 겹친다 금지).
char-class만 over-report 가능(병렬도 약간 손해, SINGULON 분열0은 안 깨짐).
"""

from __future__ import annotations

import fnmatch
import re
from functools import lru_cache

_WILD = re.compile(r"[*?\[]")


def glob_prefix(g: str) -> str:
    """glob의 첫 와일드카드 앞 디렉토리 prefix (보조 유틸)."""
    g = g.strip().lstrip("./")
    m = _WILD.search(g)
    if not m:
        return g.rstrip("/")
    head = g[: m.start()]
    if "/" in head:
        return head.rsplit("/", 1)[0].rstrip("/")
    return ""


def _norm(g: str) -> str:
    g = g.strip().lstrip("./")
    if g.endswith("/"):
        g += "**"  # 디렉토리 선언 = 서브트리
    return g


def _seg_intersect(a: str, b: str) -> bool:
    """단일 세그먼트 두 glob 패턴(*,?,literal)이 공통 문자열을 갖는가."""
    if "[" in a or "[" in b:
        return True  # 보수적(soundness)

    @lru_cache(None)
    def go(i: int, j: int) -> bool:
        if i == len(a) and j == len(b):
            return True
        if i < len(a) and a[i] == "*":
            return go(i + 1, j) or (j < len(b) and go(i, j + 1))
        if j < len(b) and b[j] == "*":
            return go(i, j + 1) or (i < len(a) and go(i + 1, j))
        if i < len(a) and j < len(b):
            if a[i] == "?" or b[j] == "?" or a[i] == b[j]:
                return go(i + 1, j + 1)
            return False
        return False

    return go(0, 0)


def _path_intersect(A: tuple, B: tuple) -> bool:
    """세그먼트 시퀀스 두 glob이 공통 경로를 갖는가 (** = 0+ 세그먼트)."""
    @lru_cache(None)
    def go(i: int, j: int) -> bool:
        if i == len(A) and j == len(B):
            return True
        if i < len(A) and A[i] == "**":
            return go(i + 1, j) or (j < len(B) and go(i, j + 1))
        if j < len(B) and B[j] == "**":
            return go(i, j + 1) or (i < len(A) and go(i + 1, j))
        if i < len(A) and j < len(B):
            if _seg_intersect(A[i], B[j]):
                return go(i + 1, j + 1)
            return False
        return False

    return go(0, 0)


def globs_overlap(g1: str, g2: str) -> bool:
    """두 glob이 공통 경로를 매칭할 수 있으면 True."""
    if g1 == g2:
        return True
    return _path_intersect(tuple(_norm(g1).split("/")), tuple(_norm(g2).split("/")))


def sets_overlap(s1, s2) -> bool:
    """두 write-set(glob 리스트)이 입체(서로소)가 아니면 True."""
    return any(globs_overlap(a, b) for a in s1 for b in s2)


# ---- 구체 경로 ∈ glob 판정 (P0-11/§D10 write-set FS 감사) ----
#
# `globs_overlap(g, p)`(p=구체경로)도 "p가 g에 덮이나"에 *거의* 맞지만, char-class를
# **보수적으로 overlap=True** 로 처리해(soundness 우선) **over-report** 한다. claim/next의
# 입체검사에선 over-report가 안전(겹칠 수도 있는 걸 겹친다고 봐 병렬도만 손해)이지만,
# write-set 감사에선 정반대다: over-report = "덮인다"를 잘못 말함 = 궤도 밖 쓰기를 통과시킴
# (= §D10이 경고하는 분열을 놓침). 그래서 감사는 **정확매칭** path_matches_glob 을 쓴다 —
# 절대 false-positive("덮인다")를 내지 않는다. 구체 경로엔 와일드카드가 없으므로 세그먼트
# 단위 fnmatchcase(*,?,[...] 를 한 세그먼트 안에서 정확처리) + ** = 0+ 세그먼트로 정밀.
def _seg_match(pat: str, seg: str) -> bool:
    """단일 경로 세그먼트(슬래시 없음)가 glob 세그먼트 패턴에 정확히 매칭하나.
    fnmatchcase 는 한 세그먼트 내 `*`/`?`/`[...]`를 정확히 처리한다(세그먼트에 `/`가 없으므로
    `*`가 `/`를 가로지르는 fnmatch 의 결함이 발생하지 않는다)."""
    return fnmatch.fnmatchcase(seg, pat)


def path_matches_glob(glob: str, path: str) -> bool:
    """구체 경로 `path`가 glob `glob`에 매칭하나(정확). `**`=0+ 세그먼트, `*`/`?`/`[...]`=한
    세그먼트 내. write-set 감사용 — soundness: 와일드카드 없는 경로엔 false-positive 없음."""
    G = tuple(_norm(glob).split("/"))
    P = tuple(path.strip().lstrip("./").split("/"))

    @lru_cache(None)
    def go(i: int, j: int) -> bool:
        if i == len(G):
            return j == len(P)
        if G[i] == "**":
            if go(i + 1, j):       # ** 가 0 세그먼트 흡수
                return True
            return j < len(P) and go(i, j + 1)  # ** 가 한 세그먼트 더 흡수
        if j < len(P) and _seg_match(G[i], P[j]):
            return go(i + 1, j + 1)
        return False

    return go(0, 0)


def path_in_globs(path: str, globs) -> bool:
    """경로가 claimed write-set(glob 리스트) 중 하나에라도 정확히 덮이면 True.
    `covered = path_in_globs(p, claimed_write_globs)`. 덮이지 않은 경로 = 궤도 밖 쓰기."""
    return any(path_matches_glob(g, path) for g in globs)
