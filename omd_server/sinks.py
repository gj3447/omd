"""이벤트 sink — 구조화 이벤트의 durable 기록/전달 seam (Q6 observability).

OMD 의 각 동사는 리턴값(자기보고)이 아니라 외부 store 에 도착한 구조화 이벤트로 검증·관측된다
(events.py / ooptdd 원칙 6: generator≠verifier). Emitter 는 방출 seam, sink 는 도착 seam.

- JsonlSink: append-only JSONL 로컬 audit trail. OpenObserve/vector/Fluent-bit 등이 이 파일을
  tail 해서 원격 관측 스택으로 ship 한다 → OMD 는 emit 만, shipping 은 남의 몫(decouple). 이게
  "connect 우회를 왜 했나"(P1) 류 사후추적의 1급 근거.
- MultiSink: 여러 sink 로 fan-out. per-sink fail-soft(하나 죽어도 나머지 산다).

INV: sink 실패는 coordination 을 절대 안 막는다(fail-soft) — 관측이 운행을 볼모로 잡지 않는다.
"""
from __future__ import annotations

import json
import threading


class JsonlSink:
    """구조화 이벤트를 append-only JSONL 로 durable 기록. thread-safe(단일 lock — 백그라운드
    sweep 스레드/여러 동사 호출이 같은 파일에 써도 줄이 안 섞인다). 각 envelope = 한 줄(정렬키로
    안정 diff). ship 은 한 번의 write 로 원자적 append(줄 찢김 최소화)."""

    def __init__(self, path: str, *, service: str = "omd"):
        self.path = path
        self.service = service
        self._lock = threading.Lock()

    def ship(self, envelopes) -> None:
        if not envelopes:
            return
        blob = "".join(
            json.dumps(e, ensure_ascii=False, sort_keys=True) + "\n" for e in envelopes)
        with self._lock:
            with open(self.path, "a", encoding="utf-8") as f:
                f.write(blob)


class MultiSink:
    """여러 sink 로 fan-out. 한 sink 가 던져도 나머지로 계속(fail-soft) — 관측 백엔드 하나가
    죽어도 다른 백엔드/로컬 로그는 산다. INV: coordination 을 절대 안 막는다."""

    def __init__(self, sinks):
        self.sinks = list(sinks)

    def ship(self, envelopes) -> None:
        for s in self.sinks:
            try:
                s.ship(envelopes)
            except Exception:  # noqa: BLE001 — 관측이 운행을 볼모로 잡지 않는다(fail-soft)
                pass
