"""P4 — 스펙↔구현 적합성 게이트.

문제(FEEDBACK P4): CONCURRENCY.md/SERVER_SPEC.md 가 '미구현/design-only/P1·P2 부채'로
*정직히* 표기한 갭들이 있는데 사용자는 production-ready 아님을 모르고 쓸 수 있다. 이 게이트는
각 명세 capability 의 구현상태를 코드에서 자동판정해 DONE/GAP 으로 가시화한다.

- must=True capability(=이 하네스가 닫은 것)가 사라지면 NO_GO(회귀가드 fail-loud).
- must=False(=알려진 잔여 GAP)는 리포트만(정직한 가시화) — 침묵 truncation 금지.
"""
from __future__ import annotations

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
    Check("begin_onboarding", "P1/P5 원샷 onboarding(declare→claim→start 접기 = 채택 자동화 enabler)", True,
          lambda r: "def begin" in _src(r, "omd_server/core.py")
          and 'stage": "claim"' in _src(r, "omd_server/core.py")),
    Check("task_conditions", "K8s-흡수 직교 condition 관측(deps_satisfied SSOT 추출 + phase rollup)", True,
          lambda r: (r / "omd_server" / "task_state.py").exists()
          and "def task_conditions" in _src(r, "omd_server/core.py")),
    Check("hot_file_gate", "P2 hot/공유파일 경합 감지", True,
          lambda r: (r / "omd_server" / "hot_files.py").exists()),
    Check("hot_file_suggest", "P2 Q8 감지→행동 루프: hot 파일 타깃 shared 추천(declare/begin 재사용)", True,
          lambda r: "def suggest_shared_for_writes" in _src(r, "omd_server/hot_files.py")),
    Check("event_sink", "Q6 observability — durable 이벤트 sink(JSONL audit trail, OMD↔shipping decouple)", True,
          lambda r: (r / "omd_server" / "sinks.py").exists()
          and "OMD_EVENT_LOG" in _src(r, "omd_server/server.py")),
    Check("multiproc_ha", "P6 멀티프로세스 HA integration(리더 admission/SIGKILL takeover/GC-pause fence, subprocess)", True,
          lambda r: (r / "tests" / "test_p6_multiproc_ha.py").exists()
          and "subprocess" in _src(r, "tests/test_p6_multiproc_ha.py")
          and "enforce_single_coordinator" in _src(r, "omd_server/core.py")),
    Check("conflict_recovery_ux", "P3 충돌 진단 동봉 + rerere 레인(증분13)", True,
          lambda r: "_diagnose_conflict" in _src(r, "omd_server/core.py")
          and "GitMergeConflict" in _src(r, "omd_server/gitio.py")
          and "def enable_rerere" in _src(r, "omd_server/gitio.py")),
    # ---- 알려진 잔여 GAP(must=False, 정직히 리포트) ----
    Check("periodic_sweep", "주기적 백그라운드 sweep(§D3/D4: opt-in sweep_interval, 유휴 spike 해소 + clean join)", True,
          lambda r: "def _periodic_sweep_loop" in _src(r, "omd_server/core.py")
          and "def close" in _src(r, "omd_server/core.py")
          and "sweep_interval" in _src(r, "omd_server/core.py")),
    Check("read_coherence_enforce", "D12 read-set 코히런스 blocking enforce(connect 유령읽기 차단, §D12 증분9)", True,
          lambda r: "def _ghost_reads" in _src(r, "omd_server/core.py")
          and 'reason="read_stale"' in _src(r, "omd_server/core.py")
          and (r / "tests" / "test_d12_read_coherence.py").exists()),
    Check("barrier_restart_recovery", "§3.D 배리어-bound 재기동 단위복구 + CONSUMED 수거(증분11)", True,
          lambda r: "_barrier_recover" in _src(r, "omd_server/core.py")
          and "def barrier_consume" in _src(r, "omd_server/_barriers.py")),
    Check("crash_recovery", "재기동 크래시 복구(_recover: DB-backed FSM state + git-진실 조정, §D8/P0-6)", True,
          lambda r: "def _recover" in _src(r, "omd_server/core.py")
          and (r / "tests" / "test_git_splitphase_stateful.py").exists()
          and (r / "tests" / "test_stateful_persistent.py").exists()),
    Check("durable_engine", "durable 실행 엔진(DBOS 체크포인트/resume) — 의도적 미채택(부채 아님)", False,
          lambda r: "dbos" in _src(r, "omd_server/core.py").lower(),
          "의도적 미채택 = 설계 결정(SERVER_SPEC §183 / CONCURRENCY §805), 미완성 부채 아님. "
          "크래시 복구는 DB-backed FSM(SQLite 원자 tx) + _recover(재기동 git-진실 조정) + "
          "crash-safe merge_token + 리더 takeover 로 이미 구현·테스트됨(test_git_splitphase_stateful"
          "/test_stateful_persistent/test_p6_multiproc_ha/test_p4_barrier_restart, 10 pass). "
          "장기 크래시-내성이 정말 필요해질 때만 DBOS Transact 를 optional 로 얹는다."),
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


if __name__ == "__main__":
    raise SystemExit(gate(out=sys.stdout))
