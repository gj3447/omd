"""재귀 도그푸드 — OMD 가 *자기 자신의 instrumentation 작업*(PROM + OOPTDD)의 병렬-dev 를 조율.

이 세션이 만든 산출물 = 병렬-dev 작업 항목. 각 항목의 write-set(파일 글롭)을 OMD 의 *실* Coordinator 가
궤도(orbit)로 claim 한다 → 서로소(입체)면 동시 grant(SINGULON: Δ분열=0, 머지충돌 0), 겹치면 직렬화
(orbit_pending). 배치 계산은 ooptdd-loop/omd_bridge.parallel_batches 와 동일 로직(omd_server.sets_overlap).

증명되는 메타-명제: *병렬-dev substrate(OMD)가 자기를 instrument 하는 작업을 분열 없이 병렬 조율할 수 있다.*

실행: .venv/bin/python scripts/dogfood_parallel_dev.py   (omd_server 만 필요 — CI-safe, FS 접근 없음)
"""
from __future__ import annotations

import json
import os
import tempfile

from omd_server.core import Coordinator
from omd_server.disjoint import sets_overlap
from omd_server.events import Emitter

# ── 이 세션 산출물 = 병렬-dev 작업 항목 (실제 파일, write-set 서로소=입체) ──────────────
_PROM = "lakatotree/tests/test_omd_parallel_{}.py"
WORK_ITEMS: dict[str, list[str]] = {
    # PROM 하네스 13 guard + programme (lakatotree) — 전부 다른 파일
    **{f"prom-{p}": [_PROM.format(p)] for p in (
        "mutex", "writeset", "2pc", "phantom", "fencing", "reclaim", "leader",
        "deadlock", "starvation", "barrier", "semaphore", "flag", "idempotency")},
    "prom-programme": ["lakatotree/examples/omd_parallel_20260627_programme.py"],
    # OOPTDD instrument (omd)
    "ooptdd-spec": ["omd/spec/omd_concurrency_ooptdd.yaml"],
    "ooptdd-driver": ["omd/omd_server/ooptdd_scenarios.py"],
    "ooptdd-test": ["omd/tests/test_ooptdd_scenarios.py"],
}

# ── OOPTDD 5 requirements 의 write-set = Longinus source (전부 core.py) → 같은 파일 = 직렬 ──
REQ_WRITESETS: dict[str, list[str]] = {
    "REQ-ORBIT-GRANT": ["omd/omd_server/core.py"],
    "REQ-FLAG-LATCH": ["omd/omd_server/core.py"],
    "REQ-DEADLOCK-REJECT": ["omd/omd_server/core.py"],
    "REQ-BARRIER-DECLARE": ["omd/omd_server/core.py"],
    "REQ-FENCE-MONOTONE": ["omd/omd_server/core.py"],
}


def _conflict(a, b) -> bool:
    if not a or not b:
        return True
    return sets_overlap(a, b)


def parallel_batches(ws: dict[str, list[str]]) -> list[list[str]]:
    """omd_bridge.parallel_batches 와 동일 greedy 입체 배치 — 같은 배치는 write-set 전부 서로소."""
    batches: list[list[str]] = []
    for rid in ws:
        for b in batches:
            if all(not _conflict(ws[rid], ws[o]) for o in b):
                b.append(rid)
                break
        else:
            batches.append([rid])
    return batches


class _Collector:
    def __init__(self):
        self.evs = []

    def ship(self, envs):
        self.evs.extend(envs)


def drive_real_coordinator(work: dict[str, list[str]]) -> dict:
    """실 Coordinator 로 각 작업 항목을 claim — 입체면 전부 HELD(orbit_granted), 0 충돌."""
    d = tempfile.mkdtemp(prefix="omd-dogfood-")
    col = _Collector()
    omd = Coordinator(db_path=os.path.join(d, "omd.db"), events=Emitter(col), agent_ttl=None)
    for key, globs in work.items():
        omd.claim(key, globs, mode="write")
    granted = sum(1 for e in col.evs if e["event"] == "orbit_granted")
    pending = sum(1 for e in col.evs if e["event"] == "orbit_pending")
    fences = sorted(e["fence"] for e in col.evs if e["event"] == "orbit_granted")
    # 합성 overlap: 두 작업이 같은 서브트리(omd/spec/**) → 둘째는 직렬(pending)
    omd.claim("overlap-A", ["omd/spec/**"], mode="write")
    omd.claim("overlap-B", ["omd/spec/omd_concurrency_ooptdd.yaml"], mode="write")
    ov_pending = sum(1 for e in col.evs if e["event"] == "orbit_pending" and e["cid"] == "overlap-B")
    return {
        "claimed": len(work), "granted": granted, "pending": pending,
        "unique_fences": len(set(fences)) == granted, "fences": fences,
        "synthetic_overlap_serialized": ov_pending >= 1,
    }


def receipt() -> dict:
    work_batches = parallel_batches(WORK_ITEMS)
    req_batches = parallel_batches(REQ_WRITESETS)
    drive = drive_real_coordinator(WORK_ITEMS)
    return {
        "thesis": "OMD coordinates the parallel-dev of its own instrumentation (Δ분열=0)",
        "work_items": len(WORK_ITEMS),
        "work_parallel_batches": len(work_batches),
        "work_max_parallel": max((len(b) for b in work_batches), default=0),
        "work_all_disjoint": len(work_batches) == 1,
        "req_items": len(REQ_WRITESETS),
        "req_parallel_batches": len(req_batches),
        "req_max_parallel": max((len(b) for b in req_batches), default=0),
        "req_serial_same_file": len(req_batches) == len(REQ_WRITESETS),
        "coordinator_drive": drive,
    }


if __name__ == "__main__":
    r = receipt()
    print(json.dumps(r, ensure_ascii=False, indent=2))
    d = r["coordinator_drive"]
    print(f"\nSINGULON: {r['work_items']} 작업 항목 → {r['work_max_parallel']} 병렬(입체 1배치) · "
          f"실 Coordinator {d['granted']}/{d['claimed']} grant, {d['pending']} 충돌, "
          f"합성 overlap 직렬={d['synthetic_overlap_serialized']}")
    print(f"OOPTDD 5 req(전부 core.py) → {r['req_max_parallel']} 병렬({r['req_parallel_batches']}배치) "
          f"= 같은 파일은 OMD 가 정직히 직렬화")
