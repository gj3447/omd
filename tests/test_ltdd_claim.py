"""LTDD(ooptdd) 게이트 — claim의 관측가능 트레이스가 store에 실제 도착했는지 적극 검증.

ooptdd 원칙 6(generator≠verifier): claim의 리턴값("HELD")은 *자기보고*일 뿐이다.
verifier는 외부 store를 읽어 이벤트의 **도착을 적극 단언**한다. backend가 조용히 드랍하면
자기보고는 여전히 HELD지만 게이트는 RED — 일반 unit test가 못 보는 silent loss를 잡는다.
"""

import os

import pytest

from omd_server import Coordinator, Emitter

GATES = os.path.join(os.path.dirname(__file__), os.pardir, "gates")
CID = "omd-claim-demo"   # gates/claim.yaml 의 cid 와 동일


def test_claim_trace_arrives_green(tmp_path):
    pytest.importorskip("ooptdd")
    from ooptdd.backends import MemoryBackend, memory as mem
    from ooptdd.gate import evaluate, evidence_tier, load_gate

    mem.reset()
    backend = MemoryBackend()
    omd = Coordinator(db_path=str(tmp_path / "omd.db"), events=Emitter(backend))

    r = omd.claim(CID, ["src/a/**"], "write")
    assert r["state"] == "HELD" and r["fence"] == 1          # 자기보고(거짓일 수 있음)
    omd.flush_admission_outbox()

    res = evaluate(backend, load_gate(os.path.join(GATES, "claim.yaml")))
    assert res["ok"], res                                     # 진실: 이벤트가 도착했다
    assert res["scope"]["asserts_anything"]                   # 비-vacuous
    assert "value-pinned" in res["scope"]["by_strength"]      # fence 핀 = 존재-only 아님
    assert evidence_tier(res) == "arrived"                    # 사다리: 실제 증거 관측
    omd.close()


def test_claim_gate_red_on_silent_loss(tmp_path):
    """backend가 조용히 드랍 → 자기보고는 HELD인데 게이트 RED (silent ingest loss 적발)."""
    pytest.importorskip("ooptdd")
    from ooptdd.backends import MemoryBackend, memory as mem
    from ooptdd.gate import evaluate, load_gate

    mem.reset()
    backend = MemoryBackend(drop=True)
    omd = Coordinator(db_path=str(tmp_path / "omd.db"), events=Emitter(backend))

    r = omd.claim(CID, ["src/a/**"], "write")
    assert r["state"] == "HELD"                                # 자기보고는 여전히 "성공"
    omd.flush_admission_outbox()
    res = evaluate(backend, load_gate(os.path.join(GATES, "claim.yaml")))
    assert not res["ok"]                                       # store는 비어있다 → RED
    omd.close()
