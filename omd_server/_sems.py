"""D4 세마포어(permit=lease, 가용=max−count(ACTIVE)) — Coordinator mixin (core.py 에서 분리, 2026-07-13 apt-cleanup Q7).

슬롯 제한 동시성 — permit 을 lease 로(죽으면 자동복구, 누수 0). Coordinator(FlagMixin, SemMixin, BarrierMixin, ...) 로 결합돼 self.<store/_cs/_emit
/_check_alive/_idem/...> 는 런타임 resolution 으로 base 에서 온다. 순수 이동(behavior 불변,
290 pytest 회귀가드).
"""
from __future__ import annotations

import time

from . import fsm

class SemMixin:
    # ---- D4 세마포어: permit=lease, 가용 = max − count(ACTIVE) (§D4, §D7, §G) ----
    def _grant_permit(self, agent_id, sem_id, ttl):
        """ACTIVE permit lease 를 발급(임계구역 안). fence 부여(소유+fenced lease). 가용 검사는
        호출자(acquire/_promote_sem_waiters)가 먼저 한다 — 여기선 발급만."""
        fence = self.store.next_fence()
        pid = self.store.add_orbit(
            task_id=None, agent_id=agent_id, pathspec=[], mode="write",
            state="HELD", fence=fence,
            expires_at=time.time() + (ttl if ttl is not None else (self.agent_ttl or 90.0)),
            reason=f"sem_permit:{sem_id}", kind="sem_permit", resource_key=sem_id)
        self._emit("sem_acquired", agent_id, permit_id=pid, sem=sem_id, fence=fence)
        return {"ok": True, "state": "ACQUIRED", "permit_id": pid, "sem": sem_id,
                "fence": fence}

    def _has_earlier_waiter(self, sem_id, priority, agent_id) -> bool:
        """no-overtaking(§D7): 이 (priority, agent) 보다 **먼저 줄선** 다른 대기자가 있나.
        있으면 가용 슬롯이 있어도 양보(작은 acquire 스트림이 head 대기자를 영구 기아시키는 것 방지)."""
        for w in self.store.waiting_sem_waiters(sem_id):  # 우선순위 DESC → enqueued_seq ASC
            if w["agent_id"] == agent_id:
                continue
            if w["priority"] > priority:
                return True
            if w["priority"] == priority:
                return True  # 같은 우선순위면 먼저 줄선(정렬상 앞) 자가 우선 — 양보
        return False

    def _promote_sem_waiters(self, sem_id):
        """가용 슬롯이 생기면 줄선 순서(우선순위 DESC → FIFO)대로 head 대기자에게 permit 부여
        (§D7 no-overtaking). 임계구역 안에서만. 멱등 reuse 도 존중(이미 보유한 대기자는 그대로)."""
        sem = self.store.get_semaphore(sem_id)
        if sem is None:
            return
        for w in self.store.waiting_sem_waiters(sem_id):
            if self.store.count_active_permits(sem_id) >= sem["max_permits"]:
                break  # 슬롯 소진 — 나머지는 계속 대기
            existing = self.store.active_permit_for(sem_id, w["agent_id"])
            if existing is not None:
                # 이미 어떤 경로로 permit 을 받음 — 멱등 reuse, 대기자만 satisfied 처리.
                self.store.set_sem_waiter(w["waiter_id"], state="GRANTED",
                                          permit_id=existing["orbit_id"])
                continue
            g = self._grant_permit(w["agent_id"], sem_id, w["ttl"])
            self.store.set_sem_waiter(w["waiter_id"], state="GRANTED",
                                      permit_id=g["permit_id"])

    def sem_declare(self, sem_id, max_permits):
        """세마포어 선언/등록(멱등). max_permits 변경 시 갱신 — 슬롯이 늘면 대기자 promote."""
        if max_permits < 1:
            return {"ok": False, "reason": "max_permits must be >= 1", "sem": sem_id}
        with self._cs():
            self.store.add_semaphore(sem_id, max_permits)
            self._emit("sem_declared", sem_id, sem=sem_id, max_permits=max_permits)
            self._promote_sem_waiters(sem_id)  # max 증가 시 새 슬롯을 줄선 대기자에게
            return {"ok": True, "sem": sem_id, "max_permits": max_permits}

    def acquire(self, agent_id, sem_id, *, ttl=300.0, no_wait=False, priority=0,
                request_id=None, bail_epoch=None):
        """세마포어 permit 획득(§D4). 가용 = max − count(ACTIVE permit)(저장정수 아님 → 누수 0).
          - 멱등 reuse: 이미 ACTIVE permit 을 쥐고 있으면 그대로 반환(재발급 안 함, MCP 재시도 안전).
          - 가용 ∧ no-overtaking(§D7: 먼저 줄선 자 없음)이면 즉시 부여(ACQUIRED).
          - 아니면: no_wait=True → FAIL(즉시 실패), no_wait=False → WAITING(waiter_id 발급, poll).
        임계구역(D1)에서 check-then-grant 가 원자 → 초과배정 불가(두 acquirer 가 동시에 N-1 보고
        둘 다 N+1번째 부여하는 레이스 차단). 보유자 사망 → reclaim/sweep 이 permit EXPIRE → 슬롯 복구."""
        with self._cs():
            args = [agent_id, sem_id, ttl, no_wait, priority]
            with self._idem(request_id, agent_id, "acquire", args) as cache:
                if cache.hit:
                    return cache.value
                self._sweep_inline()  # 죽은 보유자 permit 만료 반영 → 가용 최신화
                dead = self._check_alive(agent_id, bail_epoch)
                if dead:
                    return cache.set(dead)
                sem = self.store.get_semaphore(sem_id)
                if sem is None:
                    return cache.set({"ok": False, "reason": "no such semaphore",
                                      "sem": sem_id})
                self.store.upsert_agent(agent_id)
                # 멱등 reuse — 이미 쥔 permit 이 있으면 그대로(재발급 금지). 재시도/중복 acquire 안전.
                existing = self.store.active_permit_for(sem_id, agent_id)
                if existing is not None:
                    self._emit("sem_reuse", agent_id, permit_id=existing["orbit_id"],
                               sem=sem_id)
                    return cache.set({"ok": True, "state": "ACQUIRED",
                                      "permit_id": existing["orbit_id"], "sem": sem_id,
                                      "fence": existing["fence"], "reuse": True})
                avail = sem["max_permits"] - self.store.count_active_permits(sem_id)
                if avail >= 1 and not self._has_earlier_waiter(sem_id, priority, agent_id):
                    return cache.set(self._grant_permit(agent_id, sem_id, ttl))
                # 가용 없음(또는 먼저 줄선 자 있음).
                if no_wait:
                    self._emit("sem_acquire_failed", agent_id, sem=sem_id, reason="no_permits")
                    # FAIL 은 캐시 금지(§3.C) — 슬롯이 나면 재시도가 성공해야.
                    return cache.set({"ok": False, "state": "FAIL", "sem": sem_id,
                                      "reason": "no permits available", "avail": max(avail, 0)})
                # 대기 등록(register→poll, 비블로킹). timeout = ttl 만큼 대기(영구 hang 없음).
                seq = self.store.next_seq()
                deadline = time.time() + (ttl if ttl is not None else (self.agent_ttl or 90.0))
                wid = self.store.add_sem_waiter(sem_id, agent_id, ttl, priority, seq, deadline)
                self._emit("sem_wait_registered", agent_id, sem=sem_id, waiter_id=wid)
                # WAITING 은 캐시 금지(상태가 곧 바뀜) — 비성공으로 _is_success 가 거른다.
                return cache.set({"ok": True, "state": "WAITING", "waiter_id": wid,
                                  "sem": sem_id, "deadline": deadline})

    def acquire_poll(self, waiter_id):
        """세마포어 대기 폴(저렴·멱등, register→poll). GRANTED(permit 부여됨)/TIMEOUT/CANCELLED/
        WAITING 중 하나. poll 내부 sweep 이 죽은 보유자 permit 을 거둬 슬롯을 복구하고, head 면
        부여받는다(no-overtaking)."""
        with self._cs():
            w = self.store.get_sem_waiter(waiter_id)
            if w is None:
                return {"ok": False, "reason": "no such waiter", "waiter_id": waiter_id}
            if w["state"] == "GRANTED":
                p = self.store.get_orbit(w["permit_id"]) if w["permit_id"] else None
                return {"ok": True, "state": "ACQUIRED", "permit_id": w["permit_id"],
                        "sem": w["sem_id"], "fence": p["fence"] if p else None}
            if w["state"] != "WAITING":
                return {"ok": w["state"] != "CANCELLED", "state": w["state"],
                        "sem": w["sem_id"]}
            self._sweep_inline()           # 죽은 보유자 permit 만료 → 슬롯 복구 + head promote
            self._promote_sem_waiters(w["sem_id"])
            w = self.store.get_sem_waiter(waiter_id)
            if w["state"] == "GRANTED":
                p = self.store.get_orbit(w["permit_id"]) if w["permit_id"] else None
                return {"ok": True, "state": "ACQUIRED", "permit_id": w["permit_id"],
                        "sem": w["sem_id"], "fence": p["fence"] if p else None}
            if time.time() >= w["deadline"]:
                self.store.set_sem_waiter(waiter_id, state="TIMEOUT")
                self._emit("sem_wait_timeout", w["agent_id"], sem=w["sem_id"])
                return {"ok": False, "state": "TIMEOUT", "sem": w["sem_id"]}
            return {"ok": True, "state": "WAITING", "waiter_id": waiter_id,
                    "sem": w["sem_id"]}

    def sem_release(self, permit_id, agent_id, fence, *, request_id=None, bail_epoch=None):
        """permit 반납(§D4). 소유+fence 일치해야 — 이중해제·재부여후해제 방지(§D6). 이미
        RELEASED/EXPIRED 면 멱등 OK(MCP 재시도 안전). 반납 즉시 슬롯이 나면 줄선 대기자 promote."""
        with self._cs():
            with self._idem(request_id, agent_id, "sem_release",
                            [permit_id, fence]) as cache:
                if cache.hit:
                    return cache.value
                dead = self._check_alive(agent_id, bail_epoch)
                if dead:
                    return cache.set(dead)
                p = self.store.get_orbit(permit_id)
                if not p or p["kind"] != "sem_permit":
                    return cache.set({"ok": False, "reason": "no such permit"})
                if p["state"] in ("RELEASED", "EXPIRED"):
                    return cache.set({"ok": True, "noop": True, "state": p["state"]})
                if p["state"] != "HELD":
                    return cache.set({"ok": False, "reason": f"not HELD: {p['state']}"})
                bad = self._check_owner(p, agent_id, fence)  # owner∧fence (P0-3 유형)
                if bad:
                    return cache.set(bad)
                self.store.set_orbit(permit_id,
                                     state=fsm.advance("orbit", "HELD", "release"),
                                     released_at=time.time())
                self._emit("sem_released", agent_id, permit_id=permit_id,
                           sem=p["resource_key"])
                self._promote_sem_waiters(p["resource_key"])  # 빈 슬롯을 줄선 순서로(§D7)
                return cache.set({"ok": True, "sem": p["resource_key"]})

    def sem_status(self, sem_id):
        """세마포어 현황(가용/활성/대기). 디버그·관측용(read-only)."""
        with self._cs():
            self._sweep_inline()
            sem = self.store.get_semaphore(sem_id)
            if sem is None:
                return {"ok": False, "reason": "no such semaphore", "sem": sem_id}
            active = self.store.count_active_permits(sem_id)
            waiting = len(self.store.waiting_sem_waiters(sem_id))
            return {"ok": True, "sem": sem_id, "max_permits": sem["max_permits"],
                    "active": active, "available": max(sem["max_permits"] - active, 0),
                    "waiting": waiting}

