"""D5 배리어(세대-스탬프 응결 랑데부 + BROKEN 종단) — Coordinator mixin (core.py 에서 분리, 2026-07-13 apt-cleanup Q7).

N-task 응결 rendezvous — 참가자 사망/타임아웃은 BROKEN 으로 전원 기상. Coordinator(FlagMixin, SemMixin, BarrierMixin, ...) 로 결합돼 self.<store/_cs/_emit
/_check_alive/_idem/...> 는 런타임 resolution 으로 base 에서 온다. 순수 이동(behavior 불변,
290 pytest 회귀가드).
"""
from __future__ import annotations

import json
import time

from . import fsm
from ._const import MERGE_PIN_GRACE_S, WRITE_MODES

class BarrierMixin:
    # ---- D5 배리어: 세대-스탬프 응결 랑데부 + BROKEN 종단 (§D5, §1.2, §3.D) ----
    def _task_dependents(self, task_id) -> list[str]:
        """이 task 를 deps(after)로 가진 다른 task 들 — shrink 가 안전한지(의존자 없음) 판정용."""
        out = []
        for t in self.store.all_tasks():
            if t["task_id"] == task_id:
                continue
            if task_id in json.loads(t["deps"] or "[]"):
                out.append(t["task_id"])
        return out

    def _party_write_fence(self, task_id):
        """이 task 의 HELD write-orbit 들의 capture fence(arrive 시점 기록 / trip 재검증 기준).
        write-orbit 이 하나도 HELD 가 아니면 None(=참가 자격 없음/사망)."""
        writes = [o for o in self.store.orbits_for_task(task_id)
                  if o["mode"] in WRITE_MODES and o["state"] == "HELD"]
        if not writes:
            return None
        return max((o["fence"] for o in writes if o["fence"] is not None), default=None)

    def _party_alive(self, party) -> bool:
        """참가 task 가 아직 응결 가능한 살아있는 상태인가(도착 전/후 사망 판정, §D5).
        참가자 생존 = lease(write-orbit) 생존(소유물=lease 라는 §0 모델). 죽음 =
          (a) task 가 없어졌거나 ABORTED/POISONED(reclaim 이 abort→requeue 했으면 새 PENDING
              행이지만 agent_id=None — lease 도 함께 해제됨. 상한 초과면 POISONED 영구 terminal), 또는
          (b) write-orbit 이 HELD 가 아님(lease 만료/해제 = 보유자 사망/탈출, MERGED 제외), 또는
          (c) 도착했었는데 현재 write fence 가 도착 시점과 달라짐(ABA — 만료 후 타인에게 재부여)."""
        t = self.store.get_task(party["task_id"])
        if t is None or t["state"] in ("ABORTED", "POISONED"):
            return False
        if t["state"] == "MERGED":
            return True  # 이미 응결 — trip 멱등(plan 에서 제외됨)
        cur = self._party_write_fence(party["task_id"])
        if cur is None:
            return False  # HELD write-orbit 없음 = lease 가 거둬짐 = 참가자 사망/탈출
        if party["arrived"] and party["arrive_fence"] is not None \
                and t["state"] != "CONNECTING" and cur != party["arrive_fence"]:
            return False  # 도착 후 lease 가 만료/재부여됨(ABA) → stale 도착 = 사망
        return True

    def _break_barrier(self, barrier, reason):
        """배리어를 BROKEN 으로(도착해 있던 전원이 기상). 다음 세대 재무장은 declare 가 한다.
        영구 hang 불가 — 깨진 순간 모든 참가자가 BROKEN 으로 관측한다(§1.2/§D5)."""
        if barrier["state"] in ("BROKEN", "TRIPPED", "CONSUMED"):
            return
        self.store.set_barrier(barrier["barrier_id"],
                               state=fsm.advance("barrier", barrier["state"], "break_"),
                               break_reason=reason)
        self._emit("barrier_broken", barrier["name"], barrier=barrier["name"],
                   generation=barrier["generation"], reason=reason)

    def _live_parties(self, barrier):
        """(live, dead) 참가 분류 — reclaim/lease 만료가 멤버를 죽였는지 본다(멤버십=task 집합)."""
        parties = self.store.barrier_parties(barrier["barrier_id"], barrier["generation"])
        live, dead = [], []
        for p in parties:
            (live if self._party_alive(p) else dead).append(p)
        return live, dead

    def _barrier_eval(self, barrier_id, *, can_trip=False):
        """배리어 평가(도착·sweep·reclaim 모두 호출, 임계구역 안). 죽은 참가자/타임아웃을
        break/shrink 로 처리한다. `can_trip=True`(arrive 만)일 때, 남은 expected 전원이 도착했으면
        fill(ARMED→TRIPPING)한 뒤 trip 할 task 목록(응결 plan)을 돌려준다 — 실제 merge(Phase B
        락밖)는 arrive 가 돌린다. sweep/reclaim 은 `can_trip=False`(break/shrink 만; fill 하면
        TRIPPING 이 driver 없이 고아가 됨). 트립 plan 이 없으면 None. **공개 connect() 를 부르지
        않는다**(그 sweep 가 검증한 궤도를 재진입 만료시킴, §D5) — trip 은 _barrier_connect_one 으로."""
        b = self.store.get_barrier(barrier_id)
        if b is None or b["state"] not in ("ARMED",):
            return None
        live, dead = self._live_parties(b)
        now = time.time()
        # 1) 죽은 참가자 → policy. break=전원 깸 / shrink=죽은 멤버 제거(단 그 멤버 의존자 없을때만).
        if dead:
            if b["policy"] == "shrink" and all(not self._task_dependents(d["task_id"])
                                               for d in dead):
                for d in dead:
                    self.store.del_barrier_party(barrier_id, b["generation"], d["task_id"])
                    self._emit("barrier_shrink", b["name"], barrier=b["name"],
                               dropped=d["task_id"])
                self.store.set_barrier(barrier_id, parties=len(live))
                b = self.store.get_barrier(barrier_id)
                live, dead = self._live_parties(b)
            else:
                self._break_barrier(b, reason="participant_dead")
                return None
        # 2) 타임아웃: deadline 지났는데 미도착 있으면 break(영구 hang 방지).
        if b["deadline_at"] is not None and now >= b["deadline_at"]:
            if any(not p["arrived"] for p in live):
                self._break_barrier(b, reason="timeout")
                return None
        # 3) 전원 도착? → fill 후 트립 plan 반환(arrive 만 — driver 가 있을 때).
        if can_trip and live and all(p["arrived"] for p in live):
            self.store.set_barrier(barrier_id,
                                   state=fsm.advance("barrier", "ARMED", "fill"))
            self._emit("barrier_tripping", b["name"], barrier=b["name"],
                       generation=b["generation"], parties=len(live))
            # 결정적 순서(task_id) — 도착 fence 와 함께. 이미 MERGED 면 plan 제외(멱등).
            return [{"task_id": p["task_id"], "expected_fence": p["arrive_fence"]}
                    for p in sorted(live, key=lambda x: x["task_id"])
                    if (self.store.get_task(p["task_id"]) or {}).get("state") != "MERGED"]
        return None

    def barrier_declare(self, name, task_ids, *, kind="connect", policy="break",
                        timeout=None):
        """응결 랑데부 배리어 선언/재무장(§D5). 멤버십 = task 집합(reclaim 으로 task 가 requeue
        되면 N 재계산). 같은 이름을 다시 declare 하면 **다음 세대**로 재무장(이전 세대가 종단
        BROKEN/CONSUMED 일 때) — generation 스탬프가 ABA/유령 도착을 막는다. policy:
          'break'  = 참가자 사망/타임아웃 시 전원 깸(BrokenBarrier 시맨틱, 기본).
          'shrink' = 죽은 멤버를 빼고 진행(단 그 멤버에 의존하는 task 가 없을 때만)."""
        if not task_ids:
            return {"ok": False, "reason": "barrier needs >=1 task", "name": name}
        if policy not in ("break", "shrink"):
            return {"ok": False, "reason": "policy must be break|shrink", "name": name}
        with self._cs():
            prev = self.store.barrier_by_name(name)
            if prev is not None and prev["state"] in ("ARMED", "TRIPPING"):
                return {"ok": False, "reason": "barrier already active",
                        "name": name, "state": prev["state"],
                        "generation": prev["generation"]}
            gen = (prev["generation"] + 1) if prev is not None else 0
            bid = "bar-" + name + "-" + str(gen)
            deadline = (time.time() + timeout) if timeout is not None else None
            self.store.add_barrier(barrier_id=bid, name=name, kind=kind,
                                   parties=len(set(task_ids)), generation=gen,
                                   state="ARMED", policy=policy, deadline_at=deadline)
            for tid in set(task_ids):
                t = self.store.get_task(tid)
                self.store.add_barrier_party(bid, gen, tid,
                                             t["agent_id"] if t else None)
            self._emit("barrier_declared", name, barrier=name, generation=gen,
                       parties=len(set(task_ids)), policy=policy)
            return {"ok": True, "name": name, "barrier_id": bid, "generation": gen,
                    "parties": len(set(task_ids)), "state": "ARMED", "policy": policy}

    def barrier_arrive(self, name, agent_id, task_id, *, fence=None, request_id=None,
                       bail_epoch=None):
        """참가자 도착(§D5). task 가 응결 준비됨(write-orbit HELD)을 표시하고 arrive_fence 를
        기록. 전원 도착하면 배리어가 trip(전 task 를 결정적 순서로 응결=merge)하고 TRIPPED.
        한 명이라도 사망/타임아웃이면 BROKEN(전원 기상). merge(Phase B)는 락 밖에서 돈다."""
        # Phase A(락): 도착 기록 + eval → 트립 plan. merge 는 락 밖(아래).
        plan = None
        with self._cs():
            with self._idem(request_id, agent_id, "barrier_arrive",
                            [name, task_id, fence]) as cache:
                if cache.hit:
                    return cache.value
                self._sweep_inline()
                dead = self._check_alive(agent_id, bail_epoch)
                if dead:
                    return cache.set(dead)
                b = self.store.barrier_by_name(name)
                if b is None:
                    return cache.set({"ok": False, "reason": "no such barrier",
                                      "name": name})
                if b["state"] == "BROKEN":
                    return {"ok": False, "state": "BROKEN", "name": name,
                            "reason": b["break_reason"] or "broken"}  # 캐시 금지(재무장 가능)
                if b["state"] in ("TRIPPED", "CONSUMED"):
                    return cache.set({"ok": True, "state": b["state"], "name": name,
                                      "noop": True})
                party = self.store.get_barrier_party(b["barrier_id"], b["generation"],
                                                     task_id)
                if party is None:
                    return cache.set({"ok": False, "reason": "task not a barrier member",
                                      "name": name, "task": task_id})
                cap = self._party_write_fence(task_id)
                if cap is None:
                    return cache.set({"ok": False, "reason": "no HELD write orbit for task",
                                      "name": name, "task": task_id})
                if fence is not None and fence != cap:
                    return {"ok": False, "fenced_out": True,
                            "reason": "stale fence", "current": cap, "yours": fence}
                self.store.set_barrier_party(b["barrier_id"], b["generation"], task_id,
                                             arrived=1, arrive_fence=cap, agent_id=agent_id)
                self._emit("barrier_arrived", name, barrier=name, task=task_id,
                           generation=b["generation"], fence=cap)
                plan = self._barrier_eval(b["barrier_id"], can_trip=True)
                if plan is None:
                    # 아직 미달(대기) 또는 BROKEN. 현재 상태 회신(register→poll 패턴).
                    nb = self.store.get_barrier(b["barrier_id"])
                    if nb["state"] == "BROKEN":
                        return {"ok": False, "state": "BROKEN", "name": name,
                                "reason": nb["break_reason"]}  # 캐시 금지
                    parts = self.store.barrier_parties(b["barrier_id"], b["generation"])
                    return cache.set({"ok": True, "state": nb["state"], "name": name,
                                      "arrived": sum(p["arrived"] for p in parts),
                                      "parties": len(parts)})
                bid, gen = b["barrier_id"], b["generation"]
        # Phase B/C(락밖 merge + 원자 commit): 트립 plan 의 각 task 를 결정적 순서로 응결.
        return self._barrier_trip(bid, gen, name, plan, request_id, agent_id)

    def _barrier_trip(self, barrier_id, generation, name, plan, request_id, agent_id):
        """트립 실행(§D5): plan 의 각 task 를 _barrier_connect_one 으로 응결(공개 connect 아님).
        하나라도 실패(fence stale/merge 충돌)면 배리어 BROKEN(policy break — 전원 깸) + 이미
        응결된 것은 그대로(전진), 나머지는 멈춤. 전부 성공하면 TRIPPED."""
        merged = []
        for step in plan:
            res = self._barrier_connect_one(step["task_id"], step["expected_fence"])
            if not res.get("ok"):
                with self._cs():
                    b = self.store.get_barrier(barrier_id)
                    if b and b["state"] in ("TRIPPING", "ARMED"):
                        self._break_barrier(b, reason=f"trip_failed:{res.get('reason')}")
                return {"ok": False, "state": "BROKEN", "name": name,
                        "reason": f"trip failed on {step['task_id']}: {res.get('reason')}",
                        "merged": merged}
            merged.append(step["task_id"])
        # 전 task 응결됨 → TRIPPED.
        out = None
        with self._cs():
            b = self.store.get_barrier(barrier_id)
            if b and b["state"] == "TRIPPING":
                self.store.set_barrier(barrier_id,
                                       state=fsm.advance("barrier", "TRIPPING", "trip"))
                self._emit("barrier_tripped", name, barrier=name, generation=generation,
                           merged=merged)
            nb = self.store.get_barrier(barrier_id)
            out = {"ok": True, "state": nb["state"], "name": name, "merged": merged,
                   "generation": generation}
            if request_id is not None and self._is_success(out):
                self.store.begin_idem(request_id, agent_id, "barrier_arrive",
                                      self._arg_hash("barrier_arrive", [name, None, None]))
                self.store.finish_idem(request_id, out)
        return out

    def _barrier_connect_one(self, task_id, expected_fence):
        """응결 trip 프리미티브(§D5) — 공개 connect() 를 재호출하지 않는다(그 Phase A 의
        _sweep_inline 이 방금 검증한 궤도를 재진입 만료시킴). 대신 sweep 없는 Phase A'(fence==
        expected_fence 재검증) + 공유 Phase B(락밖 merge) + Phase C 를 직접 돌린다."""
        check_budget = self.integration_check_timeout if self.integration_check else 0.0
        deadline = time.time() + max(self.merge_timeout, 5.0) + check_budget + 10.0
        while True:
            a = self._barrier_connect_phase_a(task_id, expected_fence)
            if not a["ok"]:
                if a.get("retry") and time.time() < deadline:
                    time.sleep(0.01)
                    continue
                return a
            if a.get("noop"):
                return a
            token_id, intent = a["token_id"], a["intent"]
            merge_sha, err = self._connect_phase_b(intent)
            return self._connect_phase_c(task_id, token_id, intent, merge_sha, err)

    def _barrier_connect_phase_a(self, task_id, expected_fence):
        """Phase A'(임계구역, **sweep 없음**): write-orbit 이 HELD ∧ fence==expected_fence 재검증
        (ABA 차단) + write-set 감사 + merge_token 획득 + →CONNECTING + pin + intent 영속.
        _connect_phase_a 와 동일하되 _sweep_inline 을 부르지 않는다(§D5 핵심)."""
        with self._cs():
            t = self.store.get_task(task_id)
            if t is None:
                return {"ok": False, "reason": "no such task"}
            if t["state"] == "MERGED":
                return {"ok": True, "noop": True, "task_id": task_id, "state": "MERGED",
                        "merge_sha": t["merge_sha"]}
            writes = [o for o in self.store.orbits_for_task(task_id)
                      if o["mode"] in WRITE_MODES]
            if not writes:
                return {"ok": False, "reason": "no write orbit for task"}
            stale = [o["orbit_id"] for o in writes if o["state"] != "HELD"]
            if not stale and expected_fence is not None:
                cur = max((o["fence"] for o in writes if o["fence"] is not None),
                          default=None)
                if cur != expected_fence:
                    stale = [o["orbit_id"] for o in writes]
            if stale:
                return {"ok": False, "fenced_out": True,
                        "reason": "stale fence: lease changed since arrival",
                        "stale": stale}
            write_globs = self._claimed_write_globs(task_id, writes)
            # GAP-2: 배리어-트립 merge 도 권위 게이트 — VIOLATION 뿐 아니라 AUDIT_ERROR(감사 불가)도
            # fail-closed 거부한다(검증 못 한 write-set 을 응결시키지 않는다).
            audit = self._writeset_audit(task_id, t["branch"], write_globs)
            if audit.blocks:
                out = {"ok": False, "reason": audit.reason,
                       "offending": list(audit.offending), "task_id": task_id}
                if audit.error is not None:
                    out["audit_error"] = audit.error
                return out
            owner = t["agent_id"] or f"barrier:{task_id}"
            token_id = self._acquire_merge_token_locked(owner)
            if token_id is None:
                return {"ok": False, "retry": True, "reason": "merge in progress"}
            s = t["state"]
            if s == "IN_ORBIT":
                s = fsm.advance("task", s, "finish")
            if s == "DONE":
                s = fsm.advance("task", s, "connect")
            elif s != "CONNECTING":
                self._release_merge_token_locked(token_id)
                return {"ok": False, "reason": f"task not connectable: {s}"}
            cap_fence = max((o["fence"] for o in writes if o["fence"] is not None),
                            default=None)
            branch_tip = None
            integration_base = None
            if self.git and t["branch"]:
                branch_tip = self.git.branch_tip(t["branch"])
                integration_base = self.git.branch_tip(self.integration_branch)
            self.store.set_task(task_id, state=s, connect_fence=cap_fence,
                                connect_intent_at=time.time(), branch_tip_sha=branch_tip,
                                integration_base_sha=integration_base)
            check_budget = self.integration_check_timeout if self.integration_check else 0.0
            mdeadline = (time.time() + max(self.merge_timeout, 5.0)
                         + check_budget + MERGE_PIN_GRACE_S)
            for o in writes:
                self.store.set_orbit(o["orbit_id"], merging=1, merge_deadline=mdeadline)
            self._emit("connect_started", task_id, token_id=token_id, fence=cap_fence,
                       via="barrier")
            intent = {"task_id": task_id, "branch": t["branch"], "worktree": t["worktree"],
                      "writes": [o["orbit_id"] for o in writes],
                      "integration_base_sha": integration_base}
            return {"ok": True, "token_id": token_id, "intent": intent}

    def barrier_abort(self, name, agent_id=None, *, request_id=None, bail_epoch=None):
        """배리어를 강제로 깬다(§D5, Python Barrier.abort 시맨틱) — 도착해 있던 전원이 BROKEN
        으로 기상. 미달 상태에서 한 참가자가 진행 불가를 깨달았을 때(영구 hang 방지)."""
        with self._cs():
            with self._idem(request_id, agent_id, "barrier_abort", [name]) as cache:
                if cache.hit:
                    return cache.value
                dead = self._check_alive(agent_id, bail_epoch)
                if dead:
                    return cache.set(dead)
                b = self.store.barrier_by_name(name)
                if b is None:
                    return cache.set({"ok": False, "reason": "no such barrier",
                                      "name": name})
                if b["state"] in ("BROKEN", "TRIPPED", "CONSUMED"):
                    return cache.set({"ok": True, "state": b["state"], "name": name,
                                      "noop": True})
                self._break_barrier(b, reason="aborted")
                return cache.set({"ok": True, "state": "BROKEN", "name": name})

    def barrier_consume(self, name, agent_id=None, *, request_id=None, bail_epoch=None):
        """TRIPPED 배리어의 결과를 수거(§D5 CONSUMED 종단, 증분11) — 멤버별 merge_sha 동봉.
        TRIPPED 에서만 유효(ARMED/TRIPPING=아직 결과 없음, BROKEN=수거할 성공 없음);
        CONSUMED 재호출은 멱등 noop(결과 재동봉). 수거는 관측이 아니라 소비의 표식 —
        같은 세대를 두 번 소비하는 파이프라인 버그를 FSM 이 잡아준다."""
        with self._cs():
            with self._idem(request_id, agent_id, "barrier_consume", [name]) as cache:
                if cache.hit:
                    return cache.value
                dead = self._check_alive(agent_id, bail_epoch)
                if dead:
                    return cache.set(dead)
                b = self.store.barrier_by_name(name)
                if b is None:
                    return cache.set({"ok": False, "reason": "no such barrier",
                                      "name": name})
                if b["state"] not in ("TRIPPED", "CONSUMED"):
                    return cache.set({"ok": False, "state": b["state"], "name": name,
                                      "reason": f"not TRIPPED: {b['state']} — 수거할 결과 없음"})
                parts = self.store.barrier_parties(b["barrier_id"], b["generation"])
                results = [{"task_id": p["task_id"],
                            "merge_sha": (self.store.get_task(p["task_id"]) or {}).get("merge_sha")}
                           for p in parts]
                if b["state"] == "TRIPPED":
                    self.store.set_barrier(b["barrier_id"],
                                           state=fsm.advance("barrier", "TRIPPED", "consume"))
                    self._emit("barrier_consumed", name, barrier=name,
                               generation=b["generation"])
                    return cache.set({"ok": True, "state": "CONSUMED", "name": name,
                                      "generation": b["generation"], "results": results})
                return cache.set({"ok": True, "state": "CONSUMED", "name": name, "noop": True,
                                  "generation": b["generation"], "results": results})

    def barrier_status(self, name):
        """배리어 현황(상태/세대/도착/참가). 관측용 — 내부 sweep 으로 사망/타임아웃 반영."""
        with self._cs():
            self._sweep_inline()
            b = self.store.barrier_by_name(name)
            if b is None:
                return {"ok": False, "reason": "no such barrier", "name": name}
            self._barrier_eval(b["barrier_id"])  # 사망/타임아웃을 BROKEN 으로 반영(미달이면 plan=None)
            b = self.store.get_barrier(b["barrier_id"])
            parts = self.store.barrier_parties(b["barrier_id"], b["generation"])
            return {"ok": True, "name": name, "state": b["state"],
                    "generation": b["generation"], "policy": b["policy"],
                    "parties": len(parts),
                    "arrived": sum(p["arrived"] for p in parts),
                    "break_reason": b["break_reason"]}
