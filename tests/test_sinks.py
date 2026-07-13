"""이벤트 sink 계약 가드 (Q6 observability). OMD 는 구조화 이벤트를 emit 만 하고(ooptdd
generator≠verifier), durable 기록/shipping 은 sink 가 맡는다. JsonlSink=append-only 로컬
audit trail(OpenObserve/vector 가 tail 해서 ship — OMD↔sink decouple). MultiSink=fan-out
fail-soft. sink 실패는 coordination 을 절대 안 막는다(fail-soft)."""
import json
import os
import tempfile

from omd_server.core import Coordinator
from omd_server.events import Emitter
from omd_server.sinks import JsonlSink, MultiSink


def _tmp(name="ev.jsonl"):
    return os.path.join(tempfile.mkdtemp(prefix="omd-sink-"), name)


def _lines(path):
    with open(path, encoding="utf-8") as f:
        return [json.loads(ln) for ln in f if ln.strip()]


def test_jsonl_sink_writes_one_json_line_per_envelope():
    p = _tmp()
    s = JsonlSink(p)
    s.ship([{"event": "a", "cid": "1"}, {"event": "b", "cid": "2"}])
    rows = _lines(p)
    assert [r["event"] for r in rows] == ["a", "b"]


def test_jsonl_sink_appends_across_calls():
    p = _tmp()
    s = JsonlSink(p)
    s.ship([{"event": "a"}])
    s.ship([{"event": "b"}])
    assert len(_lines(p)) == 2                       # append (덮어쓰기 아님)


def test_coordinator_verb_events_land_in_sink():
    # 실 Coordinator 의 동사가 구조화 이벤트를 sink 로 흘린다(자기보고 아님, 외부 도착).
    p = _tmp()
    db = os.path.join(tempfile.mkdtemp(prefix="omd-sink-"), "omd.db")
    omd = Coordinator(db_path=db, agent_ttl=None, events=Emitter(JsonlSink(p)))
    omd.claim("agA", ["a/**"])                       # → orbit_granted
    events = {r["event"] for r in _lines(p)}
    assert "orbit_granted" in events


def test_multisink_fans_out():
    p1, p2 = _tmp("a.jsonl"), _tmp("b.jsonl")
    m = MultiSink([JsonlSink(p1), JsonlSink(p2)])
    m.ship([{"event": "x"}])
    assert _lines(p1)[0]["event"] == "x"
    assert _lines(p2)[0]["event"] == "x"


def test_multisink_is_fail_soft_when_one_sink_raises():
    class _Broken:
        def ship(self, envs):
            raise RuntimeError("sink down")

    p = _tmp()
    m = MultiSink([_Broken(), JsonlSink(p)])
    m.ship([{"event": "y"}])                          # 예외 전파 안 함 — 나머지 sink 는 산다
    assert _lines(p)[0]["event"] == "y"
