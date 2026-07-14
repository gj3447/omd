"""구조화 이벤트 방출 — ooptdd(LTDD) 정합.

OMD의 각 동사는 "리턴값(자기보고)"이 아니라 **외부 store에 도착한 구조화 이벤트**로 검증된다
(ooptdd 원칙 6: generator≠verifier). 이 emitter는 그 방출 seam이다 — backend가 없으면 완전 no-op.

이벤트 envelope = ooptdd 규약(flat dict): `metadata`(cid/correlation_id/cycle_id/service) + `event` + attrs.
correlation(cid)은 동사별로 의미있는 상관키(claim류=agent, connect류=task)를 쓴다.

주의(ooptdd METHODOLOGY 원칙 7 — log-free zone): **µs급 동시성 레이스(SINGULON 원자성/fence)는
이 트레이스로 검증하지 않는다.** 그건 직접 동시성 불변식 테스트(territory check)의 몫이고,
트레이스 게이트는 *관측가능한 동작 시퀀스*(claim이 orbit_granted를 fence와 함께 냈는가 등)만 본다.
"""

from __future__ import annotations


class Emitter:
    """backend로 구조화 이벤트를 ship. backend=None이면 no-op(프로덕션 기본은 주입).

    관측 backend 오류는 항상 fail-soft 처리해 coordination 결과에 개입시키지 않는다.
    """

    def __init__(self, backend=None, *, service: str = "omd"):
        self.backend = backend
        self.service = service

    def emit(self, event: str, cid: str, **attrs) -> None:
        if self.backend is None:
            return
        env = {
            "cid": cid, "correlation_id": cid, "cycle_id": cid,
            "service": self.service, "event": event, **attrs,
        }
        try:
            self.backend.ship([env])
        except Exception:  # noqa: BLE001 — observability must never block coordination
            pass


#: 주입 안 됐을 때의 무비용 기본값.
NOOP = Emitter(None)
