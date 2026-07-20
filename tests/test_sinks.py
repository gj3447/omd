"""이벤트 sink 계약 가드 (Q6 observability). OMD 는 구조화 이벤트를 emit 만 하고(ooptdd
generator≠verifier), durable 기록/shipping 은 sink 가 맡는다. JsonlSink=append-only 로컬
audit trail(OpenObserve/vector 가 tail 해서 ship — OMD↔sink decouple). MultiSink=fan-out
fail-soft for legacy telemetry; ``ship_strict`` reports every durable outbox
failure so delivery remains retryable without rolling back coordination."""
import json
import os
import tempfile
import time

import pytest

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
    deadline = time.monotonic() + 2.0
    events = set()
    while time.monotonic() < deadline:
        events = {r["event"] for r in _lines(p)}
        if "orbit_granted" in events:
            break
        time.sleep(0.01)
    assert "orbit_granted" in events
    omd.close()


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


def test_emitter_is_fail_soft_with_a_single_broken_sink():
    """MultiSink wrapper 없이 단일 sink 만 주입해도 운행 동사를 막지 않는다."""
    class _Broken:
        def ship(self, envs):
            raise RuntimeError("sink down")

    Emitter(_Broken()).emit("orbit_granted", "agA", fence=1)


def test_multisink_strict_rejects_empty_and_nested_failure():
    class _Broken:
        def ship(self, envs):
            raise RuntimeError("sink down")

    with pytest.raises(RuntimeError, match="no configured sinks"):
        MultiSink([]).ship_strict([{"event": "x"}])
    with pytest.raises(RuntimeError, match="notification sink"):
        MultiSink([MultiSink([_Broken()])]).ship_strict([{"event": "x"}])


def test_multisink_strict_attempts_remaining_sinks_after_baseexception():
    class _Exited:
        def ship(self, envs):
            del envs
            raise SystemExit("worker exited")

    received = []

    class _Healthy:
        def ship(self, envs):
            received.extend(envs)

    with pytest.raises(RuntimeError, match="notification sink"):
        MultiSink([_Exited(), _Healthy()]).ship_strict([{"event": "x"}])
    assert received == [{"event": "x"}]
