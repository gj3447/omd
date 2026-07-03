"""P1 — OMD 우회(bypass) 감지 게이트 (out-of-band).

문제(FEEDBACK_problems_20260630.md P1): OMD 는 advisory — 에이전트가 OMD 를 우회해
공유 통합브랜치에 직접커밋하면 divergence(실측: BPC 가 OMD 우회 → 공유 kjra +17). OMD 에
우회 감지/차단이 0건이라 "분열=0 사전보장"이 무의미.

불변식: 보호 통합브랜치의 **first-parent** 히스토리 상 since_ref 이후 모든 커밋은 'OMD 경유'
여야 한다 = (parent ≥2 인 `--no-ff` 머지) **AND** line-exact `OMD-Connect: <task>` trailer 보유.
근거: 통합브랜치를 건드리는 OMD 경로는 `gitio.merge_into`(--no-ff 머지, gitio.py:102) **하나뿐**,
그 머지 메시지에 `core._trailer`= `OMD-Connect: <task>`(core.py:565)가 박힌다. 드롭릿 feature
커밋(commit_all, trailer 없음, gitio.py:71)은 머지에 흡수돼 second-parent 쪽에만 있으므로
**first-parent 워크 필수**(ALL 스캔하면 전부 오탐).

분류: ROOT(parent0·제외) / OMD_CONNECT(머지+trailer=정상) / DIRECT_COMMIT(1parent·no-trailer=
직접커밋 우회) / FOREIGN_MERGE(≥2parent·no-trailer=git pull/수동머지 우회) / FORGED_TRAILER
(non-merge+trailer=위조). 우회 1건 또는 adoption<임계 → fail-loud NO_GO. git 실패=silent skip
금지(GitError 전파).
"""
from __future__ import annotations

import sys
from dataclasses import dataclass, field
from enum import Enum

from .gitio import GitError, GitRepo

TRAILER_KEY = "OMD-Connect"
OMD_AUTHOR = "omd"   # gitio._IDENT(user.name=omd) — 진짜 OMD 응결 머지의 작성자(%an)
_SEP = "\x1f"   # unit separator — 커밋 메시지에 안 나오는 제어문자
_REC = "\x1e"   # record separator


class Kind(Enum):
    OMD_CONNECT = "omd_connect"        # 머지(≥2 parent) + trailer + 작성자=OMD = 정상 응결
    DIRECT_COMMIT = "direct_commit"    # 1 parent, trailer 없음 = 직접커밋(우회)
    FOREIGN_MERGE = "foreign_merge"    # ≥2 parent, trailer 없음 = git pull/수동머지(우회)
    FORGED_TRAILER = "forged_trailer"  # non-merge + trailer = 위조(gitio 는 --no-ff 머지만 박음)
    FORGED_MERGE = "forged_merge"      # 머지 + trailer 지만 작성자≠OMD = 수동 위조 머지(우회)
    ROOT = "root"                      # parent 0 = 루트(분류 제외)

    @property
    def is_bypass(self) -> bool:
        return self in (Kind.DIRECT_COMMIT, Kind.FOREIGN_MERGE,
                        Kind.FORGED_TRAILER, Kind.FORGED_MERGE)


@dataclass(frozen=True)
class Commit:
    sha: str
    parents: tuple
    trailers: tuple   # OMD-Connect trailer 값들
    author: str
    subject: str

    @property
    def is_merge(self) -> bool:
        return len(self.parents) >= 2

    @property
    def has_trailer(self) -> bool:
        return any(v.strip() for v in self.trailers)

    @property
    def kind(self) -> "Kind":
        return classify(self)


def classify(c: Commit, omd_author: str = OMD_AUTHOR) -> Kind:
    """parent 수 × trailer × 작성자로 분류. trailer 만 보고 GO 주면 수동 위조 가능 →
    OMD_CONNECT 는 (parent≥2 머지) AND (trailer) AND (작성자=OMD) 셋 다 요구.
    ⚠️ 작성자(%an)도 `git -c user.name=omd` 로 위조 가능 — 이 검사는 캐주얼/실수 우회를 막고
    바를 높일 뿐 암호학적 방어가 아니다. 완전방어는 OMD ledger(store) 의 실제 MERGED task_id
    대조(merge 머신은 등록 task 만 응결) — 그건 store 접근이 있는 곳(서버측 hook)에서 추가."""
    if not c.parents:
        return Kind.ROOT
    if c.has_trailer:
        if not c.is_merge:
            return Kind.FORGED_TRAILER          # non-merge + trailer = 위조
        if omd_author and c.author != omd_author:
            return Kind.FORGED_MERGE            # merge + trailer 지만 작성자≠OMD = 수동 위조
        return Kind.OMD_CONNECT
    return Kind.FOREIGN_MERGE if c.is_merge else Kind.DIRECT_COMMIT


@dataclass
class AuditReport:
    integration_branch: str
    since_ref: str | None
    omd_connect: list = field(default_factory=list)   # Commit
    bypass: list = field(default_factory=list)        # (Commit, Kind)
    n_classified: int = 0

    @property
    def adoption_ratio(self) -> float:
        # OMD경유 / (OMD경유 + 우회). ROOT 제외. 분류대상 0이면 1.0(빈/clean).
        denom = len(self.omd_connect) + len(self.bypass)
        return 1.0 if denom == 0 else len(self.omd_connect) / denom

    @property
    def clean(self) -> bool:
        return not self.bypass


def _log_first_parent(git: GitRepo, rng: str) -> list[Commit]:
    fmt = _SEP.join([
        "%H", "%P",
        f"%(trailers:key={TRAILER_KEY},valueonly=true,separator=%x2c)",
        "%an", "%s",
    ]) + _REC
    out = git._git("log", "--first-parent", "--no-color", f"--format={fmt}", rng)
    commits: list[Commit] = []
    for rec in out.split(_REC):
        rec = rec.strip("\n")
        if not rec.strip():
            continue
        parts = rec.split(_SEP)
        if len(parts) < 5:
            continue
        sha, parents_s, trailers_s, author = parts[0], parts[1], parts[2], parts[3]
        subject = _SEP.join(parts[4:])   # subject 에 _SEP 가 들어올 일은 없지만 안전하게
        parents = tuple(p for p in parents_s.split() if p)
        trailers = tuple(t for t in trailers_s.split(",") if t.strip())
        commits.append(Commit(sha=sha, parents=parents, trailers=trailers,
                              author=author, subject=subject))
    return commits


def bypass_audit(repo: str, integration_branch: str, since_ref: str | None = None) -> AuditReport:
    """통합브랜치 first-parent(since_ref..) 를 스캔해 OMD경유/우회 분류. GitError 전파(fail-loud)."""
    git = GitRepo(repo)
    rng = f"{since_ref}..{integration_branch}" if since_ref else integration_branch
    report = AuditReport(integration_branch=integration_branch, since_ref=since_ref)
    for c in _log_first_parent(git, rng):
        k = c.kind
        if k is Kind.ROOT:
            continue
        report.n_classified += 1
        if k is Kind.OMD_CONNECT:
            report.omd_connect.append(c)
        else:
            report.bypass.append((c, k))
    return report


def gate(repo: str, integration_branch: str, since_ref: str | None = None,
         *, min_adoption: float = 1.0, warn_only: bool = False, out=sys.stderr) -> int:
    """fail-loud 게이트: 우회 1건 또는 adoption < min_adoption → 1(NO_GO); git 실패 → 2; else 0(GO).
    warn_only=True 면 우회를 라우드 경고만 하고 0(GO) 반환 — 채택 0%인 브랜치에 hard-block 을
    걸면 모든 push 가 막히므로(닭-달걀), 채택 전 단계의 안전 적용용. 채택되면 warn_only=False 로."""
    try:
        r = bypass_audit(repo, integration_branch, since_ref)
    except (GitError, Exception) as e:  # noqa: BLE001 — silent skip 금지(OOPTDD trace-ground)
        print(f"[omd-bypass-gate] NO_GO: git 실패(silent skip 금지) — {e!r}", file=out)
        return 2 if not warn_only else 0
    ok = r.clean and r.adoption_ratio >= min_adoption
    tag = "GO" if ok else ("WARN(allow)" if warn_only else "NO_GO")
    print(f"[omd-bypass-gate] branch={integration_branch} since={since_ref} "
          f"omd={len(r.omd_connect)} bypass={len(r.bypass)} "
          f"adoption={r.adoption_ratio:.0%} → {tag}", file=out)
    for c, k in r.bypass:
        sev = "⚠️" if warn_only else "🔴"
        print(f"  {sev} BYPASS[{k.value}] {c.sha[:10]} {c.author}: {c.subject[:70]}", file=out)
    return 0 if (ok or warn_only) else 1
