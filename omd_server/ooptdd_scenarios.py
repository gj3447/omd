"""OOPTDD in_process 드라이버 — 실 Coordinator 를 구동해 동시성 보장의 LTDD 이벤트를 backend 로 방출.

ooptdd-loop runner(in_process)가 `run(backend, cid)` 를 호출한다. 모든 시나리오를 *하나의 상관키(cid)*
아래로 구동해, spec 의 게이트가 그 cid 로 이벤트 도착을 검증할 수 있게 한다(LTDD 양성 trace 도착 = 자기보고
아님, 외부 store 도착). OMD 동사별 상관키: claim=agent_id, depend=task_id, flag_set=agent_id, barrier=name.

generator≠verifier(ooptdd 원칙 6): 이 모듈은 *방출 시나리오*만 — 판정은 ooptdd 게이트가 store 에서.
"""
from __future__ import annotations

import os
import tempfile

from omd_server.core import Coordinator
from omd_server.events import Emitter


def run(backend, cid: str) -> dict:
    """실 OMD Coordinator 를 backend-부착 Emitter 로 구동, cid 상관키로 동시성 이벤트 방출."""
    d = tempfile.mkdtemp(prefix="omd-ooptdd-")
    omd = Coordinator(db_path=os.path.join(d, "omd.db"), events=Emitter(backend), agent_ttl=None)
    driven: list[str] = []

    # 1) 상호배제/orbit happy path — orbit_requested + orbit_granted (cid=agent_id)
    omd.claim(cid, [f"ooptdd/{cid}/a.py"], mode="write")
    driven += ["orbit_requested", "orbit_granted"]

    # 2) D3 LATCH 플래그(내구 사실) — flag_set (cid=agent_id)
    omd.flag_set(f"ooptdd:{cid}:done", "done", agent_id=cid, flag_type="LATCH")
    driven += ["flag_set"]

    # 3) D7/P0-10 교착자유 — 의존 사이클이 depend_rejected 로 거부 (cid=task_id)
    other = f"{cid}-b"
    omd.declare(cid, writes=[f"ooptdd/{cid}/**"])
    omd.declare(other, writes=[f"ooptdd/{other}/**"])
    omd.depend(other, cid)        # other after cid (ok)
    omd.depend(cid, other)        # cid after other → 사이클 → depend_rejected(cid=task_id=cid)
    driven += ["depend_rejected"]

    # 4) D5 응결 배리어 선언 — barrier_declared (cid=name)
    omd.barrier_declare(cid, [cid], kind="connect", policy="break")
    driven += ["barrier_declared"]

    return {"cid": cid, "driven": driven}
