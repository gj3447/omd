"""D3 플래그(EPHEMERAL lease / LATCH 영속·단조) — Coordinator mixin (core.py 에서 분리, 2026-07-13 apt-cleanup Q7).

producer↔consumer 신호 — 플래그로 사실/lease 를 표현. Coordinator(FlagMixin, SemMixin, BarrierMixin, ...) 로 결합돼 self.<store/_cs/_emit
/_check_alive/_idem/...> 는 런타임 resolution 으로 base 에서 온다. 순수 이동(behavior 불변,
290 pytest 회귀가드).
"""
from __future__ import annotations

import time

from . import fsm
from ._const import LATCH_RANK

class FlagMixin:
    # ---- D3 플래그: EPHEMERAL(=lease) vs LATCH(영속·단조) (§D3, §1.2, §3.H) ----
    def _flag_satisfied(self, frow, want) -> bool:
        """플래그가 want 를 만족하나(LIVE 여야). 정확 값일치 OR 단조 랭크 도달 —
        want='done' 은 'merged'(상위 랭크)로도 만족된다(merged ⊃ done, §3.H 의존해제)."""
        if frow is None or frow["status"] != "LIVE":
            return False
        if frow["value"] == want:
            return True
        want_rank = LATCH_RANK.get(want, 0)
        return want_rank > 0 and frow["rank"] >= want_rank

    def _flag_broken(self, frow) -> bool:
        return frow is not None and frow["status"] == "BROKEN"

    def _wake_flag_waiters(self, key):
        """key 의 WAITING 대기자 중 만족/BROKEN 된 것을 깨운다(상태 전이만 — poll 이 읽음).
        register→poll 패턴이라 서버가 블로킹하지 않는다. epoch 가 아니라 현재 상태로 판정."""
        frow = self.store.get_flag_row(key)
        for w in self.store.waiters_for_key(key, "WAITING"):
            if self._flag_broken(frow):
                self.store.set_flag_waiter(w["waiter_id"], state="BROKEN",
                                           wake_reason="producer_dead")
            elif self._flag_satisfied(frow, w["want_value"]):
                self.store.set_flag_waiter(w["waiter_id"], state="SATISFIED",
                                           wake_reason="satisfied")

    def _break_ephemeral_flags_for_lease(self, lease_id, *, reason="producer_dead"):
        """받쳐주는 flag_ephemeral lease 가 거둬질 때(reclaim/만료) 그 EPHEMERAL 플래그를
        BROKEN 으로(자동 clear) + epoch +1 + 대기자 PRODUCER_DEAD 기상. 영구 hang 차단(§1.2)."""
        for f in self.store.ephemeral_flags_for_lease(lease_id):
            self.store.set_flag_status(f["key"], status="BROKEN", epoch=f["epoch"] + 1)
            self._emit("flag_broken", f["owner_agent"] or f["key"], key=f["key"],
                       reason=reason)
            self._wake_flag_waiters(f["key"])

    def flag_set(self, key, value, agent_id=None, *, flag_type=None, ttl=None,
                 fence=None, request_id=None, bail_epoch=None):
        """플래그 set — 두 종류(§D3):
          LATCH(기본): 영속·단조 사실. done(1)<merged(2). 하향 set 거부('un-finish 불가'),
            동값 재발행은 멱등 no-op. 소유 개념 없음(connect 의 'merged' latch 등). epoch 보존.
          EPHEMERAL: 소유 신호(build_running 등). 소유 agent + lease(orbits.kind='flag_ephemeral',
            owned+TTL). 보유자 사망 → reclaim/sweep 이 BROKEN(대기자 PRODUCER_DEAD). 같은 owner
            만 재set 가능(owner CAS); BROKEN/CLEARED 면 새로 세움. set/clear 마다 epoch +1.
        §D6: 회수된 좀비의 flag_set 차단(bail_epoch). EPHEMERAL 의 owner CAS 도 §D6 표의 보강.
        §D9: request_id 멱등(성공만 캐시)."""
        flag_type = (flag_type or "LATCH").upper()
        with self._cs():
            with self._idem(request_id, agent_id, "flag_set",
                            [key, value, flag_type]) as cache:
                if cache.hit:
                    return cache.value
                dead = self._check_alive(agent_id, bail_epoch)
                if dead:
                    return cache.set(dead)
                frow = self.store.get_flag_row(key)
                if flag_type == "EPHEMERAL":
                    return cache.set(self._flag_set_ephemeral(key, value, agent_id,
                                                              frow, ttl))
                return cache.set(self._flag_set_latch(key, value, agent_id, frow))

    def _flag_set_latch(self, key, value, agent_id, frow):
        """LATCH set — 단조 강제(done<merged). 하향=에러, 동값=멱등 no-op, 상향/신규=set."""
        new_rank = LATCH_RANK.get(value, 0)
        if frow is not None and frow["flag_type"] == "EPHEMERAL":
            return {"ok": False, "reason": "flag is EPHEMERAL, not LATCH", "key": key}
        if frow is not None:
            cur_rank = frow["rank"]
            if frow["status"] == "LIVE" and frow["value"] == value:
                return {"ok": True, "noop": True, "key": key, "value": value,
                        "flag_type": "LATCH", "epoch": frow["epoch"]}
            # 둘 다 랭크가 있고 하향이면 거부(단조 — un-finish 불가).
            if new_rank > 0 and cur_rank > 0 and new_rank < cur_rank:
                self._emit("flag_set_rejected", agent_id or key, key=key,
                           reason="monotonic_downgrade", current=frow["value"], to=value)
                return {"ok": False, "reason": "monotonic downgrade rejected",
                        "key": key, "current": frow["value"], "current_rank": cur_rank,
                        "to": value, "to_rank": new_rank}
            epoch = frow["epoch"] + 1
        else:
            epoch = 0
        rank = max(new_rank, frow["rank"] if frow else 0) if new_rank else (frow["rank"] if frow else 0)
        self.store.upsert_flag(key, value=value, set_by=agent_id, flag_type="LATCH",
                               rank=rank, status="LIVE", epoch=epoch)
        self._emit("flag_set", agent_id or key, key=key, value=value, flag_type="LATCH")
        self._wake_flag_waiters(key)
        return {"ok": True, "key": key, "value": value, "flag_type": "LATCH",
                "epoch": epoch, "rank": rank}

    def _flag_set_ephemeral(self, key, value, agent_id, frow, ttl):
        """EPHEMERAL set — 소유+TTL lease 로 받친다. 보유자 사망 시 reclaim/sweep 이 자동 BROKEN."""
        if agent_id is None:
            return {"ok": False, "reason": "EPHEMERAL flag requires owner agent", "key": key}
        # owner 를 agent 로 등록 — bail/zombie-reclaim 이 이 agent 의 flag_ephemeral lease 를
        # 찾아 거두려면 agents 행이 있어야 한다(안 그러면 reclaim 이 noop → 플래그 영구 잔존).
        self.store.upsert_agent(agent_id)
        if frow is not None and frow["flag_type"] == "LATCH":
            return {"ok": False, "reason": "flag is LATCH, not EPHEMERAL", "key": key}
        # owner CAS(§D6 보강): LIVE 면 같은 owner 만 재set. 타 agent 는 거부.
        if frow is not None and frow["status"] == "LIVE" \
                and frow["owner_agent"] not in (agent_id, None):
            return {"ok": False, "reason": "not flag owner", "key": key,
                    "owner": frow["owner_agent"]}
        # 받쳐주는 lease: LIVE 면 재사용(같은 owner), 아니면 새로 발급.
        lease_id = frow["lease_id"] if (frow and frow["status"] == "LIVE") else None
        if lease_id is None:
            fence = self.store.next_fence()
            lease_id = self.store.add_orbit(
                task_id=None, agent_id=agent_id, pathspec=[], mode="write",
                state="HELD", fence=fence,
                expires_at=time.time() + (ttl if ttl is not None else (self.agent_ttl or 90.0)),
                reason=f"flag_ephemeral:{key}", kind="flag_ephemeral",
                resource_key=key)
        elif ttl is not None:
            self.store.set_orbit(lease_id, expires_at=time.time() + ttl)
        epoch = (frow["epoch"] + 1) if frow else 0
        self.store.upsert_flag(key, value=value, set_by=agent_id, flag_type="EPHEMERAL",
                               rank=0, status="LIVE", owner_agent=agent_id,
                               lease_id=lease_id, epoch=epoch)
        self._emit("flag_set", agent_id, key=key, value=value, flag_type="EPHEMERAL",
                   lease_id=lease_id)
        self._wake_flag_waiters(key)
        return {"ok": True, "key": key, "value": value, "flag_type": "EPHEMERAL",
                "epoch": epoch, "lease_id": lease_id}

    def flag_clear(self, key, agent_id=None, *, request_id=None, bail_epoch=None):
        """EPHEMERAL 플래그를 자발적으로 clear(작업 끝, 정상 해제). owner 만. 받쳐주는 lease 해제 +
        status→CLEARED + epoch +1 + 대기자 기상(want 가 다른 값이면 계속 대기, 끊기진 않음 —
        CLEARED 는 BROKEN 과 달리 '사실이 더 이상 참 아님'이지 'producer 사망'이 아니다).
        LATCH 는 clear 불가(단조사실은 영속)."""
        with self._cs():
            with self._idem(request_id, agent_id, "flag_clear", [key]) as cache:
                if cache.hit:
                    return cache.value
                dead = self._check_alive(agent_id, bail_epoch)
                if dead:
                    return cache.set(dead)
                frow = self.store.get_flag_row(key)
                if frow is None:
                    return cache.set({"ok": True, "noop": True, "key": key})
                if frow["flag_type"] != "EPHEMERAL":
                    return cache.set({"ok": False, "reason": "LATCH flag cannot be cleared",
                                      "key": key})
                if frow["status"] != "LIVE":
                    return cache.set({"ok": True, "noop": True, "key": key,
                                      "status": frow["status"]})
                if agent_id is not None and frow["owner_agent"] not in (agent_id, None):
                    return cache.set({"ok": False, "reason": "not flag owner", "key": key,
                                      "owner": frow["owner_agent"]})
                if frow["lease_id"]:
                    lo = self.store.get_orbit(frow["lease_id"])
                    if lo and lo["state"] == "HELD":
                        self.store.set_orbit(frow["lease_id"],
                                             state=fsm.advance("orbit", "HELD", "release"),
                                             released_at=time.time())
                self.store.set_flag_status(key, status="CLEARED", epoch=frow["epoch"] + 1)
                self._emit("flag_cleared", agent_id or key, key=key)
                self._wake_flag_waiters(key)
                return cache.set({"ok": True, "key": key, "status": "CLEARED"})

    def flag_get(self, key):
        frow = self.store.get_flag_row(key)
        if frow is None:
            return {"key": key, "value": None}
        return {"key": key, "value": frow["value"], "flag_type": frow["flag_type"],
                "status": frow["status"], "epoch": frow["epoch"], "rank": frow["rank"],
                "owner": frow["owner_agent"]}

    def flag_wait(self, key, want, timeout, agent_id=None):
        """대기 등록(§D3, §1.2). **timeout 필수**(None 거부 — 영구 hang 방지). 서버는
        블로킹하지 않는다(register→poll): 단일 스레드를 막으면 구름 전체가 직렬화됨.
        이미 만족이면 즉시 SATISFIED, producer 사망(BROKEN)이면 즉시 BROKEN(PRODUCER_DEAD),
        아니면 waiter_id 발급(클라가 flag_wait_poll 재호출). observed_epoch 로 ABA/유령기상 안전."""
        if timeout is None:
            return {"ok": False, "reason": "timeout required (no indefinite wait)", "key": key}
        with self._cs():
            frow = self.store.get_flag_row(key)
            if self._flag_broken(frow):
                return {"state": "BROKEN", "reason": "producer_dead", "key": key}
            if self._flag_satisfied(frow, want):
                return {"state": "SATISFIED", "key": key, "value": frow["value"]}
            observed_epoch = frow["epoch"] if frow else -1
            deadline = time.time() + timeout
            wid = self.store.add_flag_waiter(agent_id, key, want, observed_epoch, deadline)
            self._emit("flag_wait_registered", agent_id or key, key=key, want=want,
                       waiter_id=wid)
            return {"state": "WAITING", "waiter_id": wid, "key": key, "want": want,
                    "deadline": deadline}

    def flag_wait_poll(self, waiter_id):
        """대기 폴(저렴·멱등, §D3). 재검사는 value 가 아니라 **epoch** 로 — set→clear→set 의
        ABA 나 유령기상에 안전. 만족/BROKEN(producer_dead)/TIMEOUT/WAITING 중 하나를 회신.
        클라는 SATISFIED/TIMEOUT/BROKEN 전부 처리해야(BROKEN 을 성공이나 hang 으로 오인 금지)."""
        with self._cs():
            w = self.store.get_flag_waiter(waiter_id)
            if w is None:
                return {"ok": False, "reason": "no such waiter", "waiter_id": waiter_id}
            if w["state"] != "WAITING":
                # reclaim/sweep/다른 poll 이 이미 전이시킴(SATISFIED/BROKEN). 종단 회신.
                if w["state"] == "BROKEN":
                    return {"state": "BROKEN", "reason": w["wake_reason"] or "producer_dead",
                            "key": w["key"]}
                return {"state": w["state"], "key": w["key"]}
            self._sweep_inline()  # 만료된 flag_ephemeral lease 를 BROKEN 으로 반영(producer 사망)
            frow = self.store.get_flag_row(w["key"])
            if self._flag_broken(frow):
                self.store.set_flag_waiter(waiter_id, state="BROKEN",
                                           wake_reason="producer_dead")
                return {"state": "BROKEN", "reason": "producer_dead", "key": w["key"]}
            if self._flag_satisfied(frow, w["want_value"]):
                self.store.set_flag_waiter(waiter_id, state="SATISFIED",
                                           wake_reason="satisfied")
                return {"state": "SATISFIED", "key": w["key"], "value": frow["value"]}
            if time.time() >= w["deadline"]:
                self.store.set_flag_waiter(waiter_id, state="TIMEOUT", wake_reason="timeout")
                self._emit("flag_wait_timeout", w["agent_id"] or w["key"], key=w["key"])
                return {"state": "TIMEOUT", "key": w["key"]}
            return {"state": "WAITING", "waiter_id": waiter_id, "key": w["key"]}

