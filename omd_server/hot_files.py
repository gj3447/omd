"""P2 — hot/공유파일 경합 감지 (진단 게이트).

문제(FEEDBACK P2): disjoint write-set + 위반강제(core._writeset_audit→connect_rejected)라
`constants/env.py`·`business_logic.py` 처럼 **여러 task 가 동시에 수정해야 하는 중앙파일**은
①한 궤도만 잡아 직렬화(병렬도↓) 또는 ②안 claim 하고 건드리면 connect 거부 → OMD 가 제일
약한 케이스. 3-way merge/CRDT 경로가 없어 disjoint 만 1급시민.

이 게이트는 최근 히스토리에서 파일별 touch 빈도(distinct 커밋·저자)를 세어 **hot 파일**을
식별 → shared_globs 후보 권장. (disjoint 불변식을 깨지 않는 정보/진단 — 기본 GO, --max-hot
초과 시 NO_GO 로 조일 수 있음.)
"""
from __future__ import annotations

import sys
from collections import Counter, defaultdict
from dataclasses import dataclass, field

from .disjoint import path_in_globs
from .gitio import GitError, GitRepo

_REC = "\x1e"
_SEP = "\x1f"


@dataclass(frozen=True)
class HotFile:
    path: str
    n_commits: int
    n_authors: int


@dataclass
class HotReport:
    since_ref: str | None
    threshold: int
    hot: list = field(default_factory=list)   # HotFile, n_commits desc

    @property
    def has_hot(self) -> bool:
        return bool(self.hot)

    def recommend_shared_globs(self) -> list:
        return [h.path for h in self.hot]


def hot_file_audit(repo: str, since_ref: str | None = None, *,
                   threshold: int = 3, top_n: int = 30) -> HotReport:
    """since_ref..HEAD 의 비-merge 커밋에서 파일별 touch(커밋수·저자수) 집계 → hot(≥threshold)."""
    git = GitRepo(repo)
    rng = f"{since_ref}..HEAD" if since_ref else "HEAD"
    fmt = f"{_REC}%H{_SEP}%an"
    out = git._git("log", "--no-merges", "--no-color", f"--format={fmt}", "--name-only", rng)
    file_commits: Counter = Counter()
    file_authors: dict = defaultdict(set)
    for rec in out.split(_REC):
        rec = rec.strip("\n")
        if not rec.strip():
            continue
        lines = rec.split("\n")
        head, files = lines[0], [ln for ln in lines[1:] if ln.strip()]
        author = head.split(_SEP)[1] if _SEP in head else "?"
        for f in files:
            file_commits[f] += 1
            file_authors[f].add(author)
    hot = [HotFile(p, n, len(file_authors[p])) for p, n in file_commits.items() if n >= threshold]
    hot.sort(key=lambda h: (-h.n_commits, -h.n_authors, h.path))
    return HotReport(since_ref=since_ref, threshold=threshold, hot=hot[:top_n])


def suggest_shared_for_writes(repo: str, writes, since_ref: str | None = None, *,
                              threshold: int = 3, top_n: int = 30) -> list:
    """task 의 배타 write-set(writes globs) 중 *hot*(여러 커밋/저자가 동시수정) 파일만 골라 shared
    레인 **재분할 후보**로 돌려준다 — 감지→행동 루프 닫기(P2). 반환값을 shared=에 넣을 때는
    원래 writes에서도 그 경로를 덮는 glob을 더 작은 서로소 glob들로 분할해야 한다. 현재 glob
    문법에는 ``parent/** EXCEPT parent/hot.py``가 없으므로 writes/shared 중첩 선언은 fail-loud
    거부된다. writes와 안 겹치는 hot 파일은 이 task와 무관하므로 제외한다(전역 나열이 아니라
    타깃 추천)."""
    report = hot_file_audit(repo, since_ref, threshold=threshold, top_n=top_n)
    return [h.path for h in report.hot if path_in_globs(h.path, writes)]


def gate(repo: str, since_ref: str | None = None, *, threshold: int = 3,
         max_hot: int | None = None, out=sys.stderr) -> int:
    """진단 게이트: hot 파일 리포트 + shared_globs 권장. max_hot 초과 시 NO_GO(1), git 실패 2."""
    try:
        r = hot_file_audit(repo, since_ref, threshold=threshold)
    except (GitError, Exception) as e:  # noqa: BLE001 — silent skip 금지
        print(f"[omd-hot-files] NO_GO: git 실패 — {e!r}", file=out)
        return 2
    over = max_hot is not None and len(r.hot) > max_hot
    print(f"[omd-hot-files] since={since_ref} threshold={threshold} "
          f"hot={len(r.hot)} → {'NO_GO' if over else 'GO'}", file=out)
    for h in r.hot:
        print(f"  🔥 {h.path}  (커밋 {h.n_commits}, 저자 {h.n_authors}) — 직렬화/거부 위험", file=out)
    if r.has_hot:
        print("  권장: 위 파일들을 'shared' glob 등급으로 선언(연결 시 3-way merge 허용)하거나 "
              "hot 전용 빠른 직렬 레인 사용.", file=out)
    return 1 if over else 0
