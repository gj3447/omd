"""R3 overlap 기저율 측정 — '동시에 같은 파일을 서로 다른 저자가 수정'하는 사건이 실존하는가.

읽기전용 git 히스토리 분석 (존속재판 오라클). stdlib만 사용, repo 상태를 절대 바꾸지 않는다.

정의:
  overlap 이벤트 = 같은 파일을 서로 다른 author_email 이 window(기본 72h) 내 간격으로
  수정한 커밋쌍. 파일 단위로 그런 쌍이 1개 이상이면 files_overlap_within_window 에 계수.

한계 (output 에도 명시):
  * rename 추적 안 함 (--name-only 를 --follow 없이 파싱) — rename 을 겪은 파일은
    서로 다른 경로로 나뉘어 과소측정될 수 있다.
  * identity caveat: 단일 운영자가 복수 에이전트를 같은 author_email 로 돌리면
    저자 구분이 불가능해 overlap 이 과소측정된다.

실행: python scripts/overlap_baserate.py <repo> [<repo> ...] [--window-hours 72] \
        [--since "90 days ago"] [--json]
"""
from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys

RENAME_LIMITATION = (
    "rename tracking is OFF (--name-only without --follow): renamed files are "
    "counted as distinct paths, so overlap may be undercounted across renames"
)
IDENTITY_CAVEAT = (
    "identity caveat: if a single operator runs multiple agents under the same "
    "author_email, distinct-author overlap is undercounted (agents are "
    "indistinguishable by email)"
)

_HEADER_RE = re.compile(r"^([0-9a-f]{7,40})\|(.*)\|(\d+)$")


def _git_log(repo: str, since: str) -> str:
    """읽기전용 git log 호출. 실패 시 RuntimeError (stderr 포함)."""
    cmd = [
        "git",
        "-C",
        repo,
        "log",
        "--no-merges",
        f"--since={since}",
        "--name-only",
        "--pretty=format:%H|%ae|%at",
    ]
    proc = subprocess.run(cmd, capture_output=True, text=True)
    if proc.returncode != 0:
        raise RuntimeError(f"git log failed for {repo!r}: {proc.stderr.strip()}")
    return proc.stdout


def parse_log(raw: str) -> list[tuple[str, str, int, list[str]]]:
    """raw git log 출력 → [(commit, author_email, timestamp, [files])]."""
    commits: list[tuple[str, str, int, list[str]]] = []
    current: tuple[str, str, int, list[str]] | None = None
    for line in raw.splitlines():
        line = line.rstrip("\n")
        m = _HEADER_RE.match(line)
        if m:
            if current is not None:
                commits.append(current)
            current = (m.group(1), m.group(2), int(m.group(3)), [])
            continue
        if not line.strip():
            continue
        if current is not None:
            current[3].append(line)
    if current is not None:
        commits.append(current)
    return commits


def analyze_repo(repo: str, window_hours: float = 72.0, since: str = "90 days ago") -> dict:
    """repo 하나의 overlap 기저율 측정 결과 dict (read-only)."""
    raw = _git_log(repo, since)
    commits = parse_log(raw)
    window_seconds = window_hours * 3600.0

    # 파일별 (timestamp, author_email) 수집
    per_file: dict[str, list[tuple[int, str]]] = {}
    authors: set[str] = set()
    for _sha, email, ts, files in commits:
        authors.add(email)
        for f in files:
            per_file.setdefault(f, []).append((ts, email))

    files_multi_author = 0
    overlap_pairs_per_file: dict[str, int] = {}
    for f, touches in per_file.items():
        emails = {e for _ts, e in touches}
        if len(emails) < 2:
            continue
        files_multi_author += 1
        touches_sorted = sorted(touches)
        pairs = 0
        n = len(touches_sorted)
        for i in range(n):
            ts_i, em_i = touches_sorted[i]
            for j in range(i + 1, n):
                ts_j, em_j = touches_sorted[j]
                if ts_j - ts_i > window_seconds:
                    break  # sorted → 이후는 전부 window 밖
                if em_i != em_j:
                    pairs += 1
        if pairs:
            overlap_pairs_per_file[f] = pairs

    files_seen = len(per_file)
    files_overlap = len(overlap_pairs_per_file)
    top = sorted(overlap_pairs_per_file.items(), key=lambda kv: (-kv[1], kv[0]))[:10]

    return {
        "repo": repo,
        "since": since,
        "window_hours": window_hours,
        "commits": len(commits),
        "authors": len(authors),
        "files_seen": files_seen,
        "files_multi_author": files_multi_author,
        "files_overlap_within_window": files_overlap,
        "overlap_ratio": (files_overlap / files_seen) if files_seen else 0.0,
        "top_overlap_files": [{"file": f, "pair_count": c} for f, c in top],
        "limitations": [RENAME_LIMITATION],
        "identity_caveat": IDENTITY_CAVEAT,
    }


def _render_human(result: dict) -> str:
    lines = [
        f"repo: {result['repo']}",
        f"  since={result['since']}  window={result['window_hours']}h",
        f"  commits={result['commits']}  authors={result['authors']}  files_seen={result['files_seen']}",
        f"  files_multi_author (whole period) = {result['files_multi_author']}",
        f"  files_overlap_within_window       = {result['files_overlap_within_window']}",
        f"  overlap_ratio (=within_window/files_seen) = {result['overlap_ratio']:.4f}",
    ]
    if result["top_overlap_files"]:
        lines.append("  top overlap files (distinct-author pair count within window):")
        for entry in result["top_overlap_files"]:
            lines.append(f"    {entry['pair_count']:>5}  {entry['file']}")
    else:
        lines.append("  top overlap files: (none)")
    lines.append(f"  limitation: {RENAME_LIMITATION}")
    lines.append(f"  {IDENTITY_CAVEAT}")
    return "\n".join(lines)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Measure base rate of same-file multi-author overlap in git history (read-only)."
    )
    parser.add_argument("repos", nargs="+", help="git repository path(s)")
    parser.add_argument("--window-hours", type=float, default=72.0)
    parser.add_argument("--since", default="90 days ago")
    parser.add_argument("--json", action="store_true", dest="as_json")
    args = parser.parse_args(argv)

    results = []
    for repo in args.repos:
        try:
            results.append(analyze_repo(repo, args.window_hours, args.since))
        except RuntimeError as exc:
            print(f"error: {exc}", file=sys.stderr)
            return 1

    if args.as_json:
        print(json.dumps({"results": results}, ensure_ascii=False, indent=2))
    else:
        print("\n\n".join(_render_human(r) for r in results))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
