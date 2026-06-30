"""P4 — 스펙↔구현 적합성 게이트.

문제(FEEDBACK P4): CONCURRENCY.md/SERVER_SPEC.md 가 '미구현/design-only/P1·P2 부채'로
*정직히* 표기한 갭들이 있는데 사용자는 production-ready 아님을 모르고 쓸 수 있다. 이 게이트는
각 명세 capability 의 구현상태를 코드에서 자동판정해 DONE/GAP 으로 가시화한다.

- must=True capability(=이 하네스가 닫은 것)가 사라지면 NO_GO(회귀가드 fail-loud).
- must=False(=알려진 잔여 GAP)는 리포트만(정직한 가시화) — 침묵 truncation 금지.
"""
from __future__ import annotations

import re
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Callable

ROOT = Path(__file__).resolve().parent.parent


def _src(root: Path, rel: str) -> str:
    p = root / rel
    return p.read_text(encoding="utf-8") if p.exists() else ""


@dataclass
class Check:
    key: str
    desc: str
    must: bool                       # True=구현필수(회귀가드), False=알려진 잔여 GAP(리포트만)
    probe: Callable[[Path], bool]    # (root)->bool, True=DONE
    note: str = ""                   # GAP 일 때 안내


CHECKS = [
    # ---- 이 하네스가 닫은 것(must=True 회귀가드) ----
    Check("idem_gc", "멱등 캐시 GC(§D9 무한누적 차단)", True,
          lambda r: "def gc_idem" in _src(r, "omd_server/store.py")
          and "gc_idem(" in _src(r, "omd_server/core.py")),
    Check("strict_writeset", "commit-time write-set strict 자동제외(no wedge)(P5)", True,
          lambda r: "strict_writeset" in _src(r, "omd_server/core.py")
          and "def commit_staged" in _src(r, "omd_server/gitio.py")
          and "commit_excluded_out_of_orbit" in _src(r, "omd_server/core.py")),
    Check("auto_push", "connect→remote auto-push(로컬 divergence 방지)", True,
          lambda r: "def push_integration" in _src(r, "omd_server/gitio.py")),
    Check("bypass_gate", "P1 OMD 우회 감지 게이트(out-of-band)", True,
          lambda r: (r / "omd_server" / "bypass_audit.py").exists()),
    Check("complete_task", "P5 원샷 wrapper(verb 망각-스트랜드 방지)", True,
          lambda r: "def complete_task" in _src(r, "omd_server/core.py")),
    Check("hot_file_gate", "P2 hot/공유파일 경합 감지", True,
          lambda r: (r / "omd_server" / "hot_files.py").exists()),
    # ---- 알려진 잔여 GAP(must=False, 정직히 리포트) ----
    Check("periodic_sweep", "주기적 백그라운드 sweep(§D3/D4: 현재 inline-only→유휴 후 spike)", False,
          lambda r: bool(re.search(r"threading\.Thread|_sweep_thread|class .*SweepThread",
                                   _src(r, "omd_server/core.py"))),
          "만료 lease 회수가 동사 호출 시점에만(inline). 백그라운드 스레드는 동시성 리스크라 "
          "미구현 — 도입 시 _cs 직렬화+shutdown join 으로 적대검증 필요."),
    Check("read_coherence_enforce", "D12 read-set 코히런스 enforce(현재 commit 감사=advisory)", False,
          lambda r: "read_coherence" in _src(r, "omd_server/core.py"),
          "소비자가 옛 base 로 머지(phantom read)하는 것 차단이 자문(non-blocking). "
          "blocking enforce 미구현 — 도입 시 read-gen CAS 게이트 필요."),
    Check("barrier_restart_recovery", "배리어-bound 재기동 복구(§D5 미구현 P1/P2)", False,
          lambda r: "barrier_recover" in _src(r, "omd_server/core.py"),
          "코디네이터 크래시 시 배리어 부분트립 복구가 design-only. "
          "현재는 sweep 가 ARMED 배리어 break/shrink 만 반영."),
    Check("durable_fsm", "crash-durable FSM(SERVER_SPEC: 처음엔 미도입)", False,
          lambda r: "durable" in _src(r, "omd_server/fsm.py").lower(),
          "FSM 전이가 crash-durable 아님(deferred). 크래시 내성은 store 영속+sweep 회수에 의존."),
]


def audit(root: Path = ROOT) -> dict:
    results = []
    for c in CHECKS:
        try:
            done = bool(c.probe(root))
        except Exception:  # noqa: BLE001 — probe 실패는 GAP 으로(silent skip 금지)
            done = False
        results.append({"key": c.key, "desc": c.desc, "must": c.must,
                        "done": done, "note": "" if done else c.note})
    regressed = [r for r in results if r["must"] and not r["done"]]   # 필수인데 사라짐
    gaps = [r for r in results if not r["must"] and not r["done"]]    # 알려진 잔여 GAP
    newly = [r for r in results if not r["must"] and r["done"]]       # GAP→DONE(allowlist 갱신 권장)
    return {"results": results, "regressed": regressed, "gaps": gaps, "newly_done": newly,
            "n_done": sum(r["done"] for r in results), "total": len(results),
            "ok": not regressed}


def gate(root: Path = ROOT, out=sys.stderr) -> int:
    """must=True 회귀(필수 capability 소멸) 시 NO_GO(1). 잔여 GAP 는 리포트만(GO 유지)."""
    r = audit(root)
    print(f"[omd-conformance] done={r['n_done']}/{r['total']} "
          f"regressed={len(r['regressed'])} known_gaps={len(r['gaps'])} "
          f"→ {'GO' if r['ok'] else 'NO_GO'}", file=out)
    for x in r["regressed"]:
        print(f"  🔴 REGRESSED(필수 소멸): {x['key']} — {x['desc']}", file=out)
    for x in r["gaps"]:
        print(f"  🟡 GAP(잔여): {x['key']} — {x['desc']}\n       → {x['note']}", file=out)
    for x in r["newly_done"]:
        print(f"  ✅ NEW: {x['key']} 구현됨 — must=True 로 승격 권장", file=out)
    return 0 if r["ok"] else 1
