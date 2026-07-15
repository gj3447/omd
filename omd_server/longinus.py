"""Longinus (omd-local) — 코드↔KG 바인딩 drift 감사 (자족, 서버·외부의존 0).

lakatotree `lakatos/longinus.py` 와 **동일 규칙**의 자족 이식(import 아님 — omd 는 lakatos 의존 X).
`docs/longinus_bindings.json` 매니페스트의 각 {sourceId, file, symbol, sha256} 을 *현재* omd
소스에서 심볼 재해석(re-resolve)해 판정:
  - L4 drift : 심볼 소멸/리네임 (def/class/assignment 가 사라짐)
  - L6 drift : def-line 시그니처 변경 (sha256[:16] 불일치 — 의도된 변경이면 재베이스라인)
  - line_hint 는 캐시(줄 밀려도 무드리프트 — 심볼이 정본).

tests/test_longinus_bindings.py 가 매 커밋 가드(omd CI `pytest -q -rs` 자동발견).
실행: `python -m omd_server.longinus` (또는 omd cli 'longinus').
"""
from __future__ import annotations

import hashlib
from importlib.metadata import PackageNotFoundError, distribution
import json
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]            # omd repo root


def _manifest_path() -> Path:
    """Resolve the source-tree or wheel-installed canonical manifest."""
    source = ROOT / "docs" / "longinus_bindings.json"
    if source.exists():
        return source
    try:
        dist = distribution("omd")
        files = dist.files or ()
    except PackageNotFoundError:
        return source
    for relative in files:
        if tuple(relative.parts[-2:]) == ("docs", "longinus_bindings.json"):
            candidate = Path(dist.locate_file(relative))
            if candidate.exists():
                return candidate
    return source


MANIFEST = _manifest_path()


def _load(manifest: Path | None = None) -> dict:
    return json.loads((manifest or MANIFEST).read_text(encoding="utf-8"))


def _resolve(file: str, symbol: str, root: Path = ROOT):
    """심볼의 def/class/assignment 줄을 현재 소스에서 찾음 (줄번호 재유도, 캐시 무시)."""
    path = root / file
    if not path.exists():
        return None, None
    s = re.escape(symbol)
    pats = [rf"^\s*def\s+{s}\s*\(", rf"^\s*async\s+def\s+{s}\s*\(",
            rf"^\s*class\s+{s}\b", rf"^\s*{s}\s*[:=]"]
    for i, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
        if any(re.search(p, line) for p in pats):
            return i, line
    return None, None


def audit(root: Path = ROOT, manifest: Path | None = None) -> dict:
    """전 바인딩 drift 감사. 순수 — 같은 입력 같은 출력."""
    bindings = _load(manifest).get("bindings", [])
    l4, l6, ok = [], [], []
    for b in bindings:
        ln, line = _resolve(b["file"], b["symbol"], root)
        if ln is None:
            l4.append({"sourceId": b["sourceId"], "file": b["file"], "symbol": b["symbol"]})
            continue
        cur = hashlib.sha256(line.encode()).hexdigest()[:16]
        if cur != b.get("sha256"):
            l6.append({"sourceId": b["sourceId"], "file": b["file"], "symbol": b["symbol"],
                       "baseline": b.get("sha256"), "current": cur,
                       "line": ln, "line_hint": b.get("line_hint")})
        else:
            ok.append({"sourceId": b["sourceId"], "line": ln,
                       "line_hint": b.get("line_hint"), "line_drift": ln != b.get("line_hint")})
    return {"ok": not (l4 or l6), "total": len(bindings),
            "passed": len(ok), "l4_drift": l4, "l6_drift": l6, "bindings_ok": ok}


def report(result: dict | None = None) -> str:
    r = result or audit()
    head = f"Longinus(omd) 바인딩 감사 — {r['passed']}/{r['total']} " + ("OK  ✅" if r["ok"] else "DRIFT ❌")
    lines = [head]
    for d in r["l4_drift"]:
        lines.append(f"  L4 심볼소멸: {d['sourceId']} ({d['file']}::{d['symbol']})")
    for d in r["l6_drift"]:
        lines.append(f"  L6 시그니처변경: {d['sourceId']} {d['baseline']}→{d['current']} "
                     f"(L{d['line']}, hint {d['line_hint']}; 의도면 재베이스라인)")
    stale = sum(1 for b in r["bindings_ok"] if b["line_drift"])
    if stale:
        lines.append(f"  ℹ {stale} 바인딩 line_hint stale(캐시) — 심볼 정본 유효, 무드리프트")
    return "\n".join(lines)


if __name__ == "__main__":
    import sys
    r = audit()
    if "--json" in sys.argv:
        print(json.dumps(r, ensure_ascii=False))
    else:
        print(report(r))
    sys.exit(0 if r["ok"] else 1)
