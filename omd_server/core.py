"""OMD Coordinator — 군단장 코어 로직.

SINGULON 불변식을 2지점에서 강제:
  ① claim/next     : write-set이 활성 HELD 궤도와 서로소(입체)일 때만 grant/배정 (사전)
  ② connect(merge) : 작업 중 lease가 만료/해제됐으면 응결 거부 (fencing, merge 게이트)
=> CLOUD CONNECT 시 충돌(분열)이 구조적으로 0.

**동시성 임계구역(D1, CONCURRENCY.md §D1).** 모든 변이 동사는 `with self._cs():`
( = 프로세스내 단일 writer 직렬화 RLock + `store.tx()`(BEGIN IMMEDIATE/WAL) )
한 트랜잭션 안에서 일어난다. check-then-act(claim의 충돌검사→grant, fence 발급)가 원자적이라
동시 호출에도 SINGULON이 깨지지 않는다(P0-1 TOCTOU·P0-2 fence중복 닫힘). `tx()`는 재진입 가능하여
한 동사가 sweep/_promote_pending을 같은 트랜잭션으로 호출한다.

**관측가능성(LTDD).** 각 동사는 구조화 이벤트(events.Emitter)를 방출해 외부 store에서 도착-검증된다.
단 µs 동시성 레이스 자체는 트레이스가 아니라 직접 불변식 테스트로 본다(METHODOLOGY 원칙 7).

증분3(CONCURRENCY §D1/§3.B/§D8/§D11): connect는 이제 **split-phase**다.
  Phase A(락+tx): write-orbit 재검증(P0-4 fence==captured) + repo-wide merge_token 획득 +
                  task→CONNECTING + 궤도 pin(merging=1) + intent 영속 + 커밋.
  Phase B(락 밖): 전용 통합 worktree에서 `checkout integration_branch` + `merge --no-ff`
                  (subprocess 타임아웃, §E). 충돌/타임아웃이면 abort.
  Phase C(락+tx): merge_sha 먼저 기록(P0-6) → task→MERGED → write-orbit 해제 + merge_token 반납
                  + unpin + promote. Phase B 실패면 CONNECTING→DONE rollback(재시도가능) + 토큰반납.
재기동 시 `_recover()`(§D8)가 CONNECTING task를 git 진실(trailer-probe)과 조정하고 dangling
merge_token을 abort한다.
"""

from __future__ import annotations

import hashlib
import json
import os
import threading
import time
from contextlib import contextmanager

from . import fsm
from .disjoint import path_in_globs, sets_overlap
from .events import NOOP
from .gitio import GitError, GitRepo, GitTimeout
from .store import Store

# Phase B(락밖 merge) 서브프로세스 타임아웃(§E — 무한 hang 방지). pin은 이보다 길게 잡아
# 타임아웃→abort→rollback이 완료될 시간을 준다.
MERGE_TIMEOUT_S = 120.0
MERGE_PIN_GRACE_S = 60.0


class _IdemSlot:
    """멱등 래퍼의 슬롯. hit=캐시 적중(본문 skip), value=동사 본문이 set한 응답."""
    __slots__ = ("hit", "value")

    def __init__(self):
        self.hit = False
        self.value = None

    def set(self, value):
        self.value = value
        return value


class Coordinator:
    def __init__(self, db_path: str = ":memory:", repo: str | None = None,
                 worktrees_dir: str | None = None, agent_ttl: float | None = 90.0,
                 events=None, integration_branch: str | None = None,
                 merge_timeout: float | None = None):
        self.store = Store(db_path)
        # heartbeat 만료 시 좀비 회수. 기본 ON(P0-7) — None=비활성. 끄면 죽은 물방울의
        # 궤도/작업이 영구 고아가 된다(사용자 핵심 우려). 권장 90s, renew는 TTL/3 주기.
        self.agent_ttl = agent_ttl
        self.events = events or NOOP
        self._lock = threading.RLock()  # 프로세스내 단일 writer(actor 대용) — D1
        self.merge_timeout = merge_timeout if merge_timeout is not None else MERGE_TIMEOUT_S
        self.git = GitRepo(repo) if repo else None
        self.integration_branch = integration_branch
        self.integration_worktree = None
        self.merge_resource = "cloud:default"   # repo-wide merge_token 키(§D11)
        if self.git:
            self.worktrees_dir = worktrees_dir or (self.git.root.rstrip("/") + "-omd-worktrees")
            os.makedirs(self.worktrees_dir, exist_ok=True)
            # 통합 브랜치: 명시 안 하면 레포 현재 브랜치(보통 main) — 사용자 HEAD가 아니라
            # 전용 worktree에서만 변이된다(§D11).
            if self.integration_branch is None:
                self.integration_branch = self.git.current_branch()
            self.integration_worktree = self.git.root.rstrip("/") + "-omd-integration"
        # 재기동 복구(§D8, 멱등) — git↔DB 조정 + dangling merge_token abort.
        self._recover()

    # ---- 임계구역 / 이벤트 ----
    @contextmanager
    def _cs(self):
        """단일 writer 직렬화(RLock) + 원자 트랜잭션(BEGIN IMMEDIATE). 재진입 안전."""
        with self._lock:
            with self.store.tx():
                yield

    def _emit(self, event, cid, **attrs):
        self.events.emit(event, cid, **attrs)

    # ---- 내부 (모두 임계구역 안에서 호출됨) ----
    def _conflicts(self, pathspec, mode) -> list[str]:
        """pathspec/mode가 충돌하는 활성 HELD 궤도 id들. read↔read는 공존."""
        out = []
        for o in self.store.held_orbits():
            if mode == "read" and o["mode"] == "read":
                continue
            if sets_overlap(pathspec, json.loads(o["pathspec"])):
                out.append(o["orbit_id"])
        return out

    def _promote_pending(self):
        for o in self.store.pending_orbits():  # 우선순위 DESC → FIFO
            if not self._conflicts(json.loads(o["pathspec"]), o["mode"]):
                fence = self.store.next_fence()
                self.store.set_orbit(
                    o["orbit_id"],
                    state=fsm.advance("orbit", "PENDING", "grant"),
                    expires_at=time.time() + 600, fence=fence)
                self._emit("orbit_granted", o["agent_id"], orbit_id=o["orbit_id"],
                           fence=fence, mode=o["mode"], promoted=True)

    def _wait_for(self) -> dict:
        """wait-for 그래프: PENDING 요청 agent → 그 경로를 쥔 HELD agent."""
        held = self.store.held_orbits()
        edges: dict = {}
        for p in self.store.pending_orbits():
            req, md = p["agent_id"], p["mode"]
            ps = json.loads(p["pathspec"])
            for o in held:
                if md == "read" and o["mode"] == "read":
                    continue
                if o["agent_id"] != req and sets_overlap(ps, json.loads(o["pathspec"])):
                    edges.setdefault(req, set()).add(o["agent_id"])
        return edges

    def _cycle_with(self, node) -> bool:
        """node가 wait-for 그래프에서 자기 자신으로 되돌아오는 사이클에 있나(데드락)."""
        edges = self._wait_for()

        def dfs(n, path):
            for m in edges.get(n, ()):
                if m == node:
                    return True
                if m not in path and dfs(m, path | {m}):
                    return True
            return False

        return dfs(node, {node})

    # ---- task 의존 DAG 사이클 게이트 (§D7, P0-10) ----
    def _dep_graph(self, extra_edges=None) -> dict:
        """task→deps 의존 그래프(엣지 t→d = 'd가 t보다 먼저'). DB의 모든 task `deps` +
        선택적 `extra_edges`(예: 추가하려는 후보 엣지)를 합친다. 임계구역 안에서만 호출."""
        g: dict = {}
        for t in self.store.all_tasks():
            g.setdefault(t["task_id"], set())
            for d in json.loads(t["deps"] or "[]"):
                g.setdefault(t["task_id"], set()).add(d)
        for (src, dst) in (extra_edges or []):
            g.setdefault(src, set()).add(dst)
        return g

    def _find_cycle(self, graph) -> list[str] | None:
        """방향 그래프에 사이클이 있으면 그 사이클 경로(노드 리스트)를, 없으면 None.
        DFS 색칠(WHITE/GRAY/BLACK) — GRAY 노드로 되돌아가는 back-edge가 사이클."""
        WHITE, GRAY, BLACK = 0, 1, 2
        color = {n: WHITE for n in graph}
        stack: list[str] = []

        def visit(n):
            color[n] = GRAY
            stack.append(n)
            for m in graph.get(n, ()):
                c = color.get(m, WHITE)
                if c == GRAY:
                    # back-edge → 사이클. stack[m..] + m 닫힘.
                    return stack[stack.index(m):] + [m]
                if c == WHITE:
                    cyc = visit(m)
                    if cyc:
                        return cyc
            color[n] = BLACK
            stack.pop()
            return None

        for n in list(graph):
            if color.get(n, WHITE) == WHITE:
                cyc = visit(n)
                if cyc:
                    return cyc
        return None

    def _would_cycle(self, task_id, deps) -> list[str] | None:
        """task_id 가 `deps`(after-목록)를 가질 때 의존 그래프에 사이클이 생기면 그 경로,
        아니면 None. self-dep(task_id ∈ deps)는 길이-1 사이클로 잡힌다. 후보 task가 아직
        DB에 없어도(declare 직전) 후보 엣지로 가상 추가해 전역 재검(Kahn/DFS 동치)."""
        extra = [(task_id, d) for d in (deps or [])]
        g = self._dep_graph(extra_edges=extra)
        g.setdefault(task_id, set())
        return self._find_cycle(g)

    def _sweep_inline(self):
        """임계구역 안에서 도는 sweep 본체(만료 회수 + 좀비 회수 + promote). tx 자기관리 안 함."""
        if self.agent_ttl:
            self._reclaim_zombies_inline()
        now = time.time()
        for o in self.store.due_orbits(now):
            self.store.set_orbit(o["orbit_id"],
                                 state=fsm.advance("orbit", "HELD", "expire"))
            self._emit("orbit_expired", o["agent_id"], orbit_id=o["orbit_id"])
        self._promote_pending()

    def _reclaim_zombies_inline(self):
        """heartbeat 끊긴 물방울(involuntary) — 단일 회수 루틴으로 위임."""
        if not self.agent_ttl:
            return []
        cutoff = time.time() - self.agent_ttl
        out = []
        for a in self.store.stale_agents(cutoff):
            self._reclaim_agent_inline(a["agent_id"], voluntary=False)
            out.append(a["agent_id"])
        return out

    def _reclaim_agent_inline(self, agent_id, *, voluntary):
        """긴급탈출(voluntary `bail`) / 좀비회수(involuntary) **단일 루틴** (D2).
        이 agent가 쥔 모든 궤도(HELD/PENDING)를 해제하고, 진행중 작업(CLAIMED/IN_ORBIT/CONNECTING)을
        requeue하고, worktree+브랜치를 정리하고, agent를 RETIRE한다 → 어떤 보유물도 고아가 안 된다.
        멱등 — 도중 죽어도 sweeper가 같은 루틴으로 마저 정리(이중해제·누락 없음)."""
        ag = self.store.get_agent(agent_id)
        if ag is None or ag["state"] == "RETIRED":
            return {"agent": agent_id, "noop": True}
        self.store.set_agent_state(agent_id, "BAILING" if voluntary else "ZOMBIE")
        # §D6: bail_epoch bump — 회수 전 epoch를 든 GC-pause 좀비가 살아나도 모든 변이가
        # stale bail_epoch로 FENCED_OUT (부활 방지). state 리셋(heartbeat)으로 못 우회.
        self.store.bump_bail_epoch(agent_id)
        freed, requeued = [], []
        # 죽은 보유자의 merge_token: dangling merge를 abort 후 토큰 반납(§D11/§E).
        for mt in self.store.merge_tokens_owned_by(agent_id, ("HELD",)):
            self._abort_dangling_merge(mt)
            self.store.set_orbit(mt["orbit_id"],
                                 state=fsm.advance("orbit", "HELD", "expire"),
                                 released_at=time.time())
            self._emit("merge_token_reclaimed", agent_id, orbit_id=mt["orbit_id"])
        for o in self.store.orbits_owned_by_agent(agent_id, ("HELD", "PENDING")):
            trig = "expire" if o["state"] == "HELD" else "deny"  # HELD→EXPIRED, PENDING→DENIED
            # merging pin은 회수와 함께 해제(§E pin은 유계 — 보유자 사망도 한 경계).
            self.store.set_orbit(o["orbit_id"], state=fsm.advance("orbit", o["state"], trig),
                                 merging=0)
            freed.append(o["orbit_id"])
            self._emit("orbit_released", agent_id, orbit_id=o["orbit_id"],
                       reason="bail" if voluntary else "reclaim")
        for t in self.store.tasks_for_agent(agent_id):
            if t["state"] in ("CLAIMED", "IN_ORBIT", "CONNECTING"):  # CONNECTING 포함(P0-9)
                s = fsm.advance("task", t["state"], "abort")
                s = fsm.advance("task", s, "requeue")  # ABORTED→PENDING
                self.store.set_task(t["task_id"], state=s, agent_id=None)
                requeued.append(t["task_id"])
                if self.git and t["worktree"]:
                    self.git.remove_worktree(t["worktree"])
                    if t["branch"]:
                        self.git.delete_branch(t["branch"])  # P0-8: 안 지우면 다음 start() 막힘
        self.store.set_agent_state(agent_id, "RETIRED")
        self._emit("agent_reclaimed", agent_id, voluntary=voluntary,
                   orbits=len(freed), tasks=len(requeued))
        self._promote_pending()
        return {"agent": agent_id, "voluntary": voluntary, "orbits": freed, "tasks": requeued}

    def _check_owner(self, o, agent_id, fence):
        """소유+fence 가드(D6). 통과면 None, 아니면 거부 dict. 오추방된 좀비/타 agent 차단."""
        if o["agent_id"] != agent_id:
            return {"ok": False, "reason": "not owner", "owner": o["agent_id"]}
        if o["fence"] != fence:
            return {"ok": False, "reason": "stale fence", "fenced_out": True,
                    "current": o["fence"], "yours": fence}
        return None

    def _check_task_write_fence(self, task_id, agent_id, fence):
        """finish/commit/connect의 D6 가드(opt-in): caller가 (agent,fence)를 주면
        task.owner==agent ∧ 모든 write-orbit HELD ∧ fence==f 여야 한다. 통과면 None,
        아니면 fenced_out 거부 dict. 작업 중 lease가 만료/재부여(ABA)됐으면 여기서 잡힌다.
        (agent/fence 둘 다 None이면 검사 skip — 증분2까지의 무인자 호출 하위호환.)"""
        if agent_id is None and fence is None:
            return None
        t = self.store.get_task(task_id)
        if t is None:
            return {"ok": False, "reason": "no such task"}
        if agent_id is not None and t["agent_id"] not in (agent_id, None):
            return {"ok": False, "reason": "not owner", "owner": t["agent_id"],
                    "fenced_out": True}
        writes = [o for o in self.store.orbits_for_task(task_id) if o["mode"] == "write"]
        stale = [o["orbit_id"] for o in writes if o["state"] != "HELD"]
        if not stale:
            for o in writes:
                if agent_id is not None and o["agent_id"] != agent_id:
                    stale.append(o["orbit_id"])
                elif fence is not None and o["fence"] != fence:
                    stale.append(o["orbit_id"])
        if stale:
            return {"ok": False, "fenced_out": True,
                    "reason": "stale fence: write lease expired/released during work",
                    "stale": stale}
        return None

    # ---- bail_epoch 생존 가드 (§D6 잔여, 좀비 GC-pause 부활 방지) ----
    def _check_alive(self, agent_id, bail_epoch):
        """좀비 부활 차단(§D6). 통과면 None, 아니면 fenced_out 거부 dict.
        (a) agent가 회수/탈출 중(RETIRED/ZOMBIE/BAILING)이면 차단 — 죽은 자는 변이 못 함.
        (b) caller가 bail_epoch를 줬는데 현재값과 다르면 차단 — GC-pause로 멈췄던 좀비가 회수
            (epoch bump) 뒤 깨어나 옛 epoch로 변이하려는 것. heartbeat의 state 리셋(WORKING)으로는
            못 우회한다(epoch는 단조·보존). agent_id/bail_epoch 둘 다 None이면 검사 skip(하위호환)."""
        if agent_id is None:
            return None
        ag = self.store.get_agent(agent_id)
        if ag is None:
            return None  # 미등록 — 다른 게이트가 처리(예: 신규 claim은 여기서 upsert).
        if ag["state"] in ("RETIRED", "ZOMBIE", "BAILING"):
            return {"ok": False, "reason": "agent reclaimed", "fenced_out": True,
                    "agent_state": ag["state"]}
        if bail_epoch is not None and ag["bail_epoch"] != bail_epoch:
            return {"ok": False, "reason": "stale bail_epoch", "fenced_out": True,
                    "current": ag["bail_epoch"], "yours": bail_epoch}
        return None

    # ---- 멱등성 (§D9, at-least-once MCP exactly-once 효과) ----
    @staticmethod
    def _arg_hash(verb, args) -> str:
        return hashlib.sha256(
            (verb + "|" + json.dumps(args, sort_keys=True, default=str)).encode()).hexdigest()

    @staticmethod
    def _is_success(res) -> bool:
        """성공 종단인가 — 성공만 캐시(§3.C). 거부(ok:false)·fenced_out·deadlock·재시도(retry)는
        캐시 금지: 세상이 바뀌면 같은 request_id 재시도가 성공할 수 있어야 한다."""
        if not isinstance(res, dict):
            return res is not None
        if res.get("ok") is False:
            return False
        if res.get("fenced_out") or res.get("deadlock") or res.get("retry"):
            return False
        if res.get("state") in ("DENIED",):
            return False
        return True

    @contextmanager
    def _idem(self, request_id, agent_id, verb, args):
        """변이 동사 멱등 래퍼(임계구역 안). request_id가 None이면 패스스루.
        DONE이면 캐시 응답을 yield(본문 skip 신호=cached). 아니면 INFLIGHT 등록 후 본문 실행,
        성공 종단만 DONE 캐시, 비성공은 clear(재시도 가능). 호출 패턴:
            with self._idem(rid, ag, 'claim', args) as cache:
                if cache.hit: return cache.value
                res = <본문>; cache.set(res); return res
        """
        cache = _IdemSlot()
        if request_id is None:
            yield cache
            return
        prior = self.store.get_idem(request_id)
        if prior is not None and prior["status"] == "DONE":
            cache.hit = True
            cache.value = json.loads(prior["response"])
            cache.value = dict(cache.value, replayed=True) if isinstance(cache.value, dict) else cache.value
            yield cache
            return
        self.store.begin_idem(request_id, agent_id, verb, self._arg_hash(verb, args))
        yield cache
        if cache.value is not None and self._is_success(cache.value):
            self.store.finish_idem(request_id, cache.value)
        else:
            self.store.clear_idem(request_id)

    # ---- merge_token / 통합 worktree (§D11) ----
    def _trailer(self, task_id) -> str:
        """응결 머지 커밋에 박는 고유 trailer — git=병합의 진실(§D8) trailer-probe 키."""
        return f"OMD-Connect: {task_id}"

    def _ensure_integration_wt(self):
        """전용 통합 worktree를 보장(멱등). 사용자 HEAD(root)는 절대 안 건드림(§D11). 락 밖 호출 OK."""
        if not self.git:
            return None
        self.git.ensure_integration_worktree(self.integration_worktree, self.integration_branch)
        return self.integration_worktree

    def _abort_dangling_merge(self, mt):
        """merge_token 보유자가 죽으며 남긴 통합 worktree의 진행중 머지를 중단(§D11)."""
        if not self.git or not self.integration_worktree:
            return
        if os.path.isdir(self.integration_worktree):
            self.git.abort_merge(self.integration_worktree)

    def _acquire_merge_token_locked(self, agent_id):
        """repo-wide merge_token(Semaphore max=1, §D11) 획득. 가용(다른 HELD 토큰 없음)이면 부여,
        아니면 None. 임계구역(_cs) 안에서만 호출 — 초과부여 레이스 차단."""
        if self.store.held_merge_token(self.merge_resource) is not None:
            return None  # 이미 누가 응결 중 — 직렬화(한 번에 하나만)
        fence = self.store.next_fence()
        tok = self.store.add_orbit(
            task_id=None, agent_id=agent_id, pathspec=[], mode="write",
            state="HELD", fence=fence, expires_at=None, reason="merge_token",
            kind="merge_token", resource_key=self.merge_resource)
        self.store.set_orbit(tok, merge_started_mono=time.monotonic())
        return tok

    def _release_merge_token_locked(self, token_id):
        tok = self.store.get_orbit(token_id)
        if tok and tok["state"] == "HELD":
            self.store.set_orbit(token_id,
                                 state=fsm.advance("orbit", "HELD", "release"),
                                 released_at=time.time())

    # ---- 재기동 복구 (§D8, P0-6) ----
    def _recover(self):
        """재기동 시 git↔DB 조정(멱등). CONNECTING(또는 connect_intent 있는) task를 git 진실과
        맞춘다: 통합 브랜치에 trailer가 있으면 전진수정(→MERGED+해제+worktree 제거), 없으면
        rollback(→DONE, connect 재호출 가능). dangling merge_token은 abort 후 반납."""
        with self._cs():
            wt = None
            if self.git:
                try:
                    wt = self._ensure_integration_wt()
                except GitError:
                    wt = None
            for t in self.store.tasks_by_state(["CONNECTING"]):
                merged_sha = None
                if self.git and wt:
                    merged_sha = self.git.branch_in_integration(
                        wt, self.integration_branch, self._trailer(t["task_id"]))
                if merged_sha:
                    # git 진실: 이미 응결됨 → 전진수정(P0-6: merge_sha 기록 후 해제).
                    self.store.set_task(t["task_id"], merge_sha=merged_sha,
                                        merged_at=time.time())
                    self._release_task_write_orbits(t["task_id"])
                    self.store.set_task(t["task_id"],
                                        state=fsm.advance("task", "CONNECTING", "merged"))
                    self.store.set_flag(t["task_id"], "merged")
                    if self.git and t["worktree"]:
                        self.git.remove_worktree(t["worktree"])
                    self._emit("connect_recovered", t["task_id"], merge_sha=merged_sha,
                               outcome="merged")
                else:
                    # git상 미머지 → rollback(재시도가능). 궤도 unpin(merging=0).
                    for o in self.store.pinned_orbits_for_task(t["task_id"]):
                        self.store.set_orbit(o["orbit_id"], merging=0, merge_deadline=None)
                    self.store.set_task(t["task_id"],
                                        state=fsm.advance("task", "CONNECTING", "rollback"))
                    self._emit("connect_recovered", t["task_id"], outcome="rollback")
            # dangling merge_token: 재기동 시점에 HELD인 토큰은 정의상 dangling이다 —
            # merge_token은 connect Phase B 동안만 잠깐 보유되고, 그 Phase는 프로세스에 묶여
            # 재기동을 가로질러 살아있을 수 없다(§D11). 위에서 모든 CONNECTING task를 이미
            # git 진실과 조정했으므로(MERGED/DONE), 남은 토큰은 전부 abort+반납해 누수를 막는다.
            for mt in self.store.all_held_merge_tokens():
                self._abort_dangling_merge(mt)
                self.store.set_orbit(mt["orbit_id"],
                                     state=fsm.advance("orbit", "HELD", "expire"),
                                     released_at=time.time())
                self._emit("merge_token_reclaimed", mt["agent_id"],
                           orbit_id=mt["orbit_id"], reason="recover")
            self._promote_pending()

    # ---- write-set 파일시스템 감사 (§D10, P0-11 = "최대 구멍") ----
    def _claimed_write_globs(self, task_id, writes) -> list[str]:
        """task의 HELD write-orbit pathspec들의 합집합(claimed write-set). `writes`는 Phase A가
        이미 모은 write-orbit row 리스트 — 거기서 glob을 펼친다."""
        globs: list[str] = []
        for o in writes:
            for g in json.loads(o["pathspec"]):
                globs.append(g)
        return globs

    def _writeset_audit(self, task_id, branch, write_globs) -> list[str]:
        """branch가 통합 base 대비 건드린 파일 중 **claimed write-set 밖** 경로들(있으면 위반).
        §D10 option 2(저비용 pre-connect 감사): `git diff --name-only base...branch`의 모든
        경로가 claimed write-globs 에 정확히 덮여야 한다. 안 덮인 경로 = 분열 위험 = 거부 대상.
        repo 미바인딩이거나 branch 없으면 감사 불가 → 빈 리스트(감사 skip, 보수적으로 통과)."""
        if not self.git or not branch:
            return []
        try:
            changed = self.git.changed_paths(branch, self.integration_branch)
        except GitError:
            return []   # diff 실패(브랜치 없음 등) — 다른 게이트가 처리. 감사 위반은 아님.
        # path_in_globs = 정확매칭(soundness: 덮인다를 절대 거짓-양성으로 안 냄). 안 덮이면 위반.
        return [p for p in changed if not path_in_globs(p, write_globs)]

    def _release_task_write_orbits(self, task_id):
        """task의 HELD write-orbit 전부 해제 + unpin(merge_sha 기록 *후* 호출 — P0-6 순서)."""
        for o in self.store.orbits_for_task(task_id):
            if o["mode"] == "write" and o["state"] == "HELD":
                self.store.set_orbit(o["orbit_id"],
                                     state=fsm.advance("orbit", "HELD", "release"),
                                     released_at=time.time(), merging=0, merge_deadline=None)

    # ---- 공개 API (= MCP 툴 / CLI 동사) ----
    def declare(self, task_id, *, name="", writes=None, reads=None, deps=None, priority=0):
        with self._cs():
            # P0-10/§D7: deps가 의존 DAG에 사이클을 만들면 거부(그래프 불변) — 안 그러면
            # 상호의존(A after B, B after A)이 둘 다 영구 BLOCKED. self-dep 도 잡힌다.
            if deps:
                cyc = self._would_cycle(task_id, deps)
                if cyc:
                    self._emit("declare_rejected", task_id, reason="dep_cycle", cycle=cyc)
                    return {"ok": False, "reason": "dep_cycle", "cycle": cyc,
                            "task_id": task_id}
            self.store.add_task(task_id=task_id, name=name, writes=writes or [],
                                reads=reads or [], deps=deps or [], state="PENDING",
                                priority=priority)
        return {"ok": True, "task_id": task_id, "state": "PENDING"}

    def depend(self, task_id, after):
        """task_id 에 의존 엣지(`task_id` after `after`)를 추가 — 단, 사이클을 만들면 **거부**
        (그래프 불변, P0-10/§D7). self-dep 도 거부. check-then-add 가 임계구역 안에서 원자."""
        with self._cs():
            t = self.store.get_task(task_id)
            if t is None:
                return {"ok": False, "reason": "no such task", "task_id": task_id}
            existing = json.loads(t["deps"] or "[]")
            if after in existing:
                return {"ok": True, "noop": True, "task_id": task_id, "after": after,
                        "deps": existing}
            cyc = self._would_cycle(task_id, existing + [after])
            if cyc:
                # 그래프 변경 없음 — 거부만.
                self._emit("depend_rejected", task_id, after=after, reason="dep_cycle",
                           cycle=cyc)
                return {"ok": False, "reason": "dep_cycle", "cycle": cyc,
                        "task_id": task_id, "after": after}
            new_deps = existing + [after]
            self.store.set_task_deps(task_id, new_deps)
            self._emit("depend_added", task_id, after=after)
            return {"ok": True, "task_id": task_id, "after": after, "deps": new_deps}

    def _intent_key(self, agent_id, pathspec, mode, task_id) -> str:
        """claim 자연 멱등 키(§D9): hash(agent, sorted(paths), mode, task)."""
        return self._arg_hash("claim",
                              [agent_id, sorted(pathspec), mode, task_id])

    def claim(self, agent_id, pathspec, mode="write", *, ttl=600.0, task_id=None,
              reason="", priority=0, request_id=None, bail_epoch=None):
        if isinstance(pathspec, str):
            pathspec = [pathspec]
        with self._cs():
            args = [agent_id, sorted(pathspec), mode, task_id, ttl, priority]
            with self._idem(request_id, agent_id, "claim", args) as cache:
                if cache.hit:
                    return cache.value
                self._sweep_inline()
                # §D6: 회수/탈출된 좀비는 새 궤도조차 못 잡음(부활 차단).
                dead = self._check_alive(agent_id, bail_epoch)
                if dead:
                    return cache.set(dead)
                self.store.upsert_agent(agent_id)
                self._emit("orbit_requested", agent_id, mode=mode, paths=pathspec, task=task_id)
                # §D9 의미적 멱등: dedup 우회돼도(다른 request_id·없음) 같은 의도면 기존 궤도 반환.
                # §3.C 교차: 단 **현재 caller가 그 궤도의 소유자**여야 살아있는 HELD를 돌려준다 —
                # 회수돼 타인에게 재부여된 lease를 우회로 넘기지 않음(fencing 무장 방지).
                ikey = self._intent_key(agent_id, pathspec, mode, task_id)
                dup = self.store.orbit_by_intent(ikey)
                if dup is not None and dup["agent_id"] == agent_id:
                    self._emit("orbit_dedup", agent_id, orbit_id=dup["orbit_id"],
                               state=dup["state"])
                    out = {"orbit_id": dup["orbit_id"], "state": dup["state"],
                           "fence": dup["fence"], "conflicts": [], "dedup": True}
                    return cache.set(out)
                conf = self._conflicts(pathspec, mode)
                if conf:
                    oid = self.store.add_orbit(task_id=task_id, agent_id=agent_id,
                                               pathspec=pathspec, mode=mode, state="PENDING",
                                               reason=reason, priority=priority,
                                               intent_key=ikey)
                    if self._cycle_with(agent_id):  # 대기 시 데드락 사이클이면 거부
                        self.store.set_orbit(oid, state=fsm.advance("orbit", "PENDING", "deny"))
                        self._emit("orbit_denied", agent_id, orbit_id=oid, deadlock=True)
                        # DENIED는 캐시 금지(§3.C) — 세상이 바뀌면 재시도가 성공할 수 있어야.
                        return cache.set({"orbit_id": oid, "state": "DENIED",
                                          "deadlock": True, "conflicts": conf})
                    self._emit("orbit_pending", agent_id, orbit_id=oid, conflicts=len(conf))
                    return cache.set({"orbit_id": oid, "state": "PENDING", "conflicts": conf})
                fence = self.store.next_fence()
                oid = self.store.add_orbit(task_id=task_id, agent_id=agent_id, pathspec=pathspec,
                                           mode=mode, state="HELD", fence=fence,
                                           expires_at=time.time() + ttl, reason=reason,
                                           priority=priority, intent_key=ikey)
                self._emit("orbit_granted", agent_id, orbit_id=oid, fence=fence, mode=mode)
                be = self.store.get_agent(agent_id)
                return cache.set({"orbit_id": oid, "state": "HELD", "fence": fence,
                                  "conflicts": [],
                                  "bail_epoch": be["bail_epoch"] if be else 0})

    def renew(self, orbit_id, agent_id, fence, ttl=600.0, *, request_id=None,
              bail_epoch=None):
        """궤도 lease 갱신(keepalive). 소유+fence 일치해야 — 오추방된 좀비는 FENCED_OUT."""
        with self._cs():
            with self._idem(request_id, agent_id, "renew",
                            [orbit_id, fence, ttl]) as cache:
                if cache.hit:
                    return cache.value
                dead = self._check_alive(agent_id, bail_epoch)
                if dead:
                    return cache.set(dead)
                o = self.store.get_orbit(orbit_id)
                if not o:
                    return cache.set({"ok": False, "reason": "no such orbit"})
                if o["state"] != "HELD":
                    return cache.set({"ok": False, "reason": f"not HELD: {o['state']}",
                                      "fenced_out": True})
                bad = self._check_owner(o, agent_id, fence)
                if bad:
                    return cache.set(bad)
                self.store.set_orbit(orbit_id, state=fsm.advance("orbit", "HELD", "renew"),
                                     expires_at=time.time() + ttl)
                self._emit("orbit_renewed", agent_id, orbit_id=orbit_id)
                return cache.set({"ok": True, "expires_in": ttl})

    def release(self, orbit_id, agent_id, fence, *, request_id=None, bail_epoch=None):
        """궤도 lease 반납. 소유+fence 일치해야(P0-3) — 아무나 남의 궤도 해제 불가.
        이미 RELEASED/EXPIRED면 멱등 OK(MCP 재시도 안전). §3.C: dedup 재생이 *재부여된* lease를
        풀지 않게 owner/fence 가드가 감싼다(release는 소유+fence 통과 후에만 작용)."""
        with self._cs():
            with self._idem(request_id, agent_id, "release",
                            [orbit_id, fence]) as cache:
                if cache.hit:
                    return cache.value
                dead = self._check_alive(agent_id, bail_epoch)
                if dead:
                    return cache.set(dead)
                o = self.store.get_orbit(orbit_id)
                if not o:
                    return cache.set({"ok": False, "reason": "no such orbit"})
                if o["state"] in ("RELEASED", "EXPIRED", "DENIED"):
                    return cache.set({"ok": True, "noop": True, "state": o["state"]})
                if o["state"] != "HELD":
                    return cache.set({"ok": False, "reason": f"not HELD: {o['state']}"})
                bad = self._check_owner(o, agent_id, fence)
                if bad:
                    return cache.set(bad)
                self.store.set_orbit(orbit_id, state=fsm.advance("orbit", "HELD", "release"),
                                     released_at=time.time())
                self._emit("orbit_released", agent_id, orbit_id=orbit_id)
                self._promote_pending()
                return cache.set({"ok": True})

    def bail(self, agent_id, *, request_id=None):
        """물방울 긴급 탈출(자발). 보유 궤도 전부 해제 + 작업 requeue + worktree/브랜치 정리.
        멱등 — 비자발 좀비회수와 **단일 루틴**을 공유(둘 사이 누락/이중해제 없음). bail_epoch 검사는
        없음: 죽으려는 자의 탈출은 항상 허용돼야(자기 회수). request_id로 응답도 멱등."""
        with self._cs():
            with self._idem(request_id, agent_id, "bail", [agent_id]) as cache:
                if cache.hit:
                    return cache.value
                return cache.set(self._reclaim_agent_inline(agent_id, voluntary=True))

    def heartbeat(self, agent_id):
        """물방울 생존 신호. §D6 표: 이미 회수(RETIRED)된 좀비에겐 `{fenced_out:true}` 회신 →
        좀비가 다음 heartbeat에서 자기 죽음을 안다(advisory). 살아있으면 현재 bail_epoch를 회신해
        물방울이 이후 변이에 실어 보내면 회수 후 부활을 서버가 거부할 수 있다(§D6)."""
        with self._cs():
            ag = self.store.get_agent(agent_id)
            if ag is not None and ag["state"] == "RETIRED":
                # 회수된 좀비 — heartbeat로 부활시키지 않고 죽음을 통지(fence 복종 규율).
                return {"ok": False, "fenced_out": True, "reason": "agent reclaimed",
                        "bail_epoch": ag["bail_epoch"]}
            self.store.upsert_agent(agent_id)
            return {"ok": True, "bail_epoch": self.store.get_agent(agent_id)["bail_epoch"]}

    def reclaim_zombies(self):
        """heartbeat 끊긴 물방울 회수: HELD 궤도 만료 + 작업 requeue + worktree 정리."""
        if not self.agent_ttl:
            return {"reclaimed": []}
        with self._cs():
            return {"reclaimed": self._reclaim_zombies_inline()}

    def sweep(self):
        with self._cs():
            before = {o["orbit_id"] for o in self.store.held_orbits()}
            self._sweep_inline()
            after = {o["orbit_id"] for o in self.store.held_orbits()}
            return {"expired": sorted(before - after)}

    def next_task(self, agent_id):
        """deps 충족 + write-set이 활성 HELD와 서로소인 작업 1개 → READY로 올려 반환."""
        with self._cs():
            self._sweep_inline()
            held = self.store.held_orbits()
            held_specs = [(json.loads(o["pathspec"]), o["mode"]) for o in held]
            for t in self.store.tasks_by_state(["PENDING", "READY", "BLOCKED"]):
                deps = json.loads(t["deps"])
                if not all((self.store.get_task(d) or {}).get("state") == "MERGED" for d in deps):
                    continue
                writes = json.loads(t["writes"])
                if any(sets_overlap(writes, spec) for spec, _ in held_specs):
                    continue
                if t["state"] != "READY":
                    self.store.set_task(t["task_id"],
                                        state=fsm.advance("task", t["state"], "ready"))
                self._emit("task_ready", agent_id, task=t["task_id"])
                return self.store.get_task(t["task_id"])
            return None

    def start(self, task_id, agent_id, *, request_id=None, bail_epoch=None):
        """READY task에 agent 배정 → IN_ORBIT. repo 바인딩 시 물방울 worktree 발사.
        §D9 의미적 멱등: 이미 이 agent로 시작된(IN_ORBIT/이후 + worktree 존재) task 재시도는
        worktree를 재생성하지 않고 기존 것을 반환한다 — `worktree add -b`가 기존 브랜치에서
        실패(GitError+중복행)하던 버그 차단."""
        with self._cs():
            with self._idem(request_id, agent_id, "start", [task_id]) as cache:
                if cache.hit:
                    return cache.value
                dead = self._check_alive(agent_id, bail_epoch)
                if dead:
                    return cache.set(dead)
                t = self.store.get_task(task_id)
                # 이미 시작됨(같은 agent) — worktree 재생성 금지(자연 멱등).
                if t["state"] in ("IN_ORBIT", "DONE", "CONNECTING", "MERGED") \
                        and t["agent_id"] == agent_id:
                    self._emit("task_start_dedup", agent_id, task=task_id)
                    return cache.set({"task_id": task_id, "state": t["state"],
                                      "worktree": t["worktree"], "branch": t["branch"],
                                      "dedup": True})
                s = t["state"]
                if s == "READY":
                    s = fsm.advance("task", s, "claim")
                s = fsm.advance("task", s, "start")  # CLAIMED→IN_ORBIT
                self.store.upsert_agent(agent_id)
                worktree = branch = None
                if self.git:
                    branch = f"omd/{task_id}"
                    worktree = os.path.join(self.worktrees_dir, task_id)
                    self.git.add_worktree(branch, worktree)
                self.store.set_task(task_id, state=s, agent_id=agent_id,
                                    worktree=worktree if self.git else ...,
                                    branch=branch if self.git else ...)
                self._emit("task_started", agent_id, task=task_id, worktree=worktree)
                return cache.set({"task_id": task_id, "state": s, "worktree": worktree,
                                  "branch": branch})

    def commit(self, task_id, msg, agent_id=None, fence=None, *, request_id=None,
               bail_epoch=None):
        """물방울 worktree의 변경을 커밋(repo 바인딩 시). 커밋 후 write-set 감사(§D10/P0-11)를
        **자문(advisory)** 으로 돌려 궤도 밖 경로를 조기 노출한다(`offending` 동봉). 단 connect
        게이트가 *권위* 강제 지점이므로 여기선 커밋을 되돌리지 않는다 — 물방울이 일찍 알아채게.
        §D6: caller가 (agent,fence)를 주면 owner∧write-orbit HELD∧fence==f 재검증(opt-in) —
        오추방된 좀비가 남의 worktree를 커밋하지 못하게."""
        if not self.git:
            return {"ok": False, "reason": "no repo bound"}
        with self._cs():
            with self._idem(request_id, agent_id, "commit",
                            [task_id, msg, fence]) as cache:
                if cache.hit:
                    return cache.value
                dead = self._check_alive(agent_id, bail_epoch)
                if dead:
                    return cache.set(dead)
                bad = self._check_task_write_fence(task_id, agent_id, fence)
                if bad:
                    self._emit("commit_rejected", task_id, reason=bad["reason"])
                    return cache.set(bad)
                t = self.store.get_task(task_id)
                sha = self.git.commit_all(t["worktree"], msg)
                self._emit("task_committed", t["agent_id"], task=task_id, sha=sha)
                writes = [o for o in self.store.orbits_for_task(task_id)
                          if o["mode"] == "write" and o["state"] == "HELD"]
                write_globs = self._claimed_write_globs(task_id, writes)
                offending = self._writeset_audit(task_id, t["branch"], write_globs)
                res = {"ok": True, "sha": sha}
                if offending:
                    # 자문 경고 — connect에서 거부될 것임. 물방울은 지금 바로잡아야 한다.
                    self._emit("commit_writeset_warning", task_id, offending=offending)
                    res["writeset_violation"] = True
                    res["offending"] = offending
                return cache.set(res)

    def finish(self, task_id, agent_id=None, fence=None, *, request_id=None,
               bail_epoch=None):
        """작업 완료 표시(IN_ORBIT→DONE, `done` latch). §D6: caller가 (agent,fence)를 주면
        owner∧write-orbit HELD∧fence==f 재검증(opt-in) — 오추방된 좀비가 남의 task를 finish해
        분열을 부르지 못하게. 무인자 호출은 증분2까지 동작 유지(하위호환)."""
        with self._cs():
            with self._idem(request_id, agent_id, "finish", [task_id, fence]) as cache:
                if cache.hit:
                    return cache.value
                dead = self._check_alive(agent_id, bail_epoch)
                if dead:
                    return cache.set(dead)
                bad = self._check_task_write_fence(task_id, agent_id, fence)
                if bad:
                    self._emit("finish_rejected", task_id, reason=bad["reason"])
                    return cache.set(bad)
                t = self.store.get_task(task_id)
                # 의미적 멱등: 이미 DONE(또는 이후)이면 finish 재시도는 no-op.
                if t["state"] in ("DONE", "CONNECTING", "MERGED"):
                    return cache.set({"task_id": task_id, "state": t["state"], "noop": True})
                self.store.set_task(task_id, state=fsm.advance("task", t["state"], "finish"))
                self.store.set_flag(task_id, "done", set_by=t["agent_id"])
                self._emit("task_finished", t["agent_id"], task=task_id)
                return cache.set({"task_id": task_id, "state": "DONE"})

    # ---- CLOUD CONNECT — split-phase A–B–C (§3.B/§D8/§D11) ----
    def connect(self, task_id, agent_id=None, fence=None, *, request_id=None,
                bail_epoch=None):
        """CLOUD CONNECT(응결=merge). **split-phase** — git merge가 락(_cs) **밖**에서 돈다:
          A(락): write-orbit 재검증(P0-4 HELD∧fence==captured) + merge_token 획득 + →CONNECTING
                 + 궤도 pin(merging=1) + intent 영속 + 커밋.
          B(락밖): 전용 통합 worktree에서 merge --no-ff(타임아웃, §E). 충돌/타임아웃이면 abort.
          C(락): merge_sha 먼저 기록(P0-6) → →MERGED + write-orbit 해제 + merge_token 반납 + unpin.
        fencing: 작업 중 lease 만료/해제면 거부(stale fence). merge_token으로 동시 connect를 직렬화.
        §D9: request_id 멱등(성공만 캐시). split-phase라 _idem 트랜잭션을 Phase B에 걸칠 수 없어
        캐시 확인/기록을 짧은 _cs() 두 곳으로 나눈다. 의미적 멱등(already-MERGED)은 fencing 위(§3.C):
        connect는 owner/fence 통과 후에만 머지하므로 dedup 재생이 재부여 lease를 풀지 않는다."""
        # §D9 dedup 캐시 적중(성공 종단만 저장됨) → 재머지 없이 캐시 응답.
        if request_id is not None:
            with self._cs():
                prior = self.store.get_idem(request_id)
                if prior is not None and prior["status"] == "DONE":
                    out = json.loads(prior["response"])
                    return dict(out, replayed=True) if isinstance(out, dict) else out

        # 멱등(P0-9/D9): 이미 응결된 task는 재머지 없이 즉시 MERGED 회신.
        t0 = self.store.get_task(task_id)
        if t0 and t0["state"] == "MERGED":
            return {"ok": True, "task_id": task_id, "state": "MERGED",
                    "merge_sha": t0["merge_sha"], "noop": True}

        deadline = time.time() + max(self.merge_timeout, 5.0) + 10.0
        while True:
            a = self._connect_phase_a(task_id, agent_id, fence, bail_epoch)
            if not a["ok"]:
                if a.get("retry") and time.time() < deadline:
                    time.sleep(0.01)   # merge_token 경합 — 다른 connect 응결중. 곧 재시도.
                    continue
                return a               # 거부(fenced_out 등)는 캐시 안 함(§3.C)
            if a.get("noop"):          # 이미 MERGED (멱등)
                return a
            # ----- Phase B: 락 밖(no _cs, no live tx) git merge -----
            token_id, intent = a["token_id"], a["intent"]
            merge_sha, err = self._connect_phase_b(intent)
            # ----- Phase C: 락 안 — merge_sha 먼저 기록 후 해제(P0-6) -----
            res = self._connect_phase_c(task_id, token_id, intent, merge_sha, err)
            # §D9: 성공 종단만 캐시(merge conflict/timeout=retryable → 캐시 금지).
            if request_id is not None and self._is_success(res):
                with self._cs():
                    self.store.begin_idem(request_id, agent_id, "connect",
                                          self._arg_hash("connect", [task_id, fence]))
                    self.store.finish_idem(request_id, res)
            return res

    def _connect_phase_a(self, task_id, agent_id, fence, bail_epoch=None):
        """Phase A(임계구역): fence 재검증(P0-4) + merge_token 획득 + intent 영속 + pin + →CONNECTING."""
        with self._cs():
            self._sweep_inline()
            # §D6: 회수/탈출된(또는 stale bail_epoch) 좀비의 connect는 부활 차단으로 거부.
            dead = self._check_alive(agent_id, bail_epoch)
            if dead:
                self._emit("connect_rejected", task_id, reason=dead["reason"])
                return dead
            t = self.store.get_task(task_id)
            if t is None:
                return {"ok": False, "reason": "no such task"}
            if t["state"] == "MERGED":
                return {"ok": True, "noop": True, "task_id": task_id, "state": "MERGED",
                        "merge_sha": t["merge_sha"]}
            writes = [o for o in self.store.orbits_for_task(task_id) if o["mode"] == "write"]
            if not writes:
                return {"ok": False, "reason": "no write orbit for task"}
            # P0-4: 모든 write-orbit이 HELD여야(만료/해제면 stale). + 호출자가 (agent,fence)를
            # 줬으면 owner∧fence==captured 까지 — ABA(만료 후 재부여)를 fence 동일성으로 잡는다.
            stale = [o["orbit_id"] for o in writes if o["state"] != "HELD"]
            if not stale and (agent_id is not None or fence is not None):
                for o in writes:
                    if agent_id is not None and o["agent_id"] != agent_id:
                        stale.append(o["orbit_id"])
                    elif fence is not None and o["fence"] != fence:
                        stale.append(o["orbit_id"])
            if stale:
                self._emit("connect_rejected", task_id, reason="stale_fence", stale=stale)
                return {"ok": False, "fenced_out": True,
                        "reason": "stale fence: lease expired/released during work",
                        "stale": stale}
            # P0-11/§D10 — write-set 파일시스템 강제("최대 구멍"). 브랜치가 claimed write-set
            # **밖**의 파일을 건드렸으면 거부(merge 안 함, 토큰 안 잡음, 상태 불변). 이것으로
            # SINGULON 토대 (c)가 성립: 선언상 서로소 write-set이 *실제* write-set이 된다.
            write_globs = self._claimed_write_globs(task_id, writes)
            offending = self._writeset_audit(task_id, t["branch"], write_globs)
            if offending:
                self._emit("connect_rejected", task_id, reason="writeset_violation",
                           offending=offending)
                return {"ok": False, "reason": "writeset_violation", "offending": offending,
                        "claimed": write_globs, "task_id": task_id}
            # merge_token(repo-wide Semaphore max=1) — 가용 아니면 retry(다른 connect 응결중).
            owner = agent_id or t["agent_id"] or f"connect:{task_id}"
            token_id = self._acquire_merge_token_locked(owner)
            if token_id is None:
                return {"ok": False, "retry": True, "reason": "merge in progress (token held)"}
            # task → CONNECTING (+ intent 영속: connect_fence/branch_tip_sha/connect_intent_at)
            s = t["state"]
            if s == "IN_ORBIT":
                s = fsm.advance("task", s, "finish")
            if s == "DONE":
                s = fsm.advance("task", s, "connect")  # DONE→CONNECTING
            elif s != "CONNECTING":
                # 비정상 상태에서 connect → 토큰 반납하고 거부.
                self._release_merge_token_locked(token_id)
                return {"ok": False, "reason": f"task not connectable: {s}"}
            cap_fence = max((o["fence"] for o in writes if o["fence"] is not None), default=None)
            branch_tip = None
            if self.git and t["branch"]:
                branch_tip = self.git.branch_tip(t["branch"])
            self.store.set_task(task_id, state=s, connect_fence=cap_fence,
                                connect_intent_at=time.time(), branch_tip_sha=branch_tip)
            # 궤도 pin(merging=1) — sweep/reclaim이 응결중 궤도를 건드리지 않게(§E, 유계).
            deadline = time.time() + max(self.merge_timeout, 5.0) + MERGE_PIN_GRACE_S
            for o in writes:
                self.store.set_orbit(o["orbit_id"], merging=1, merge_deadline=deadline)
            self._emit("connect_started", task_id, token_id=token_id, fence=cap_fence)
            intent = {"task_id": task_id, "branch": t["branch"], "worktree": t["worktree"],
                      "writes": [o["orbit_id"] for o in writes]}
            return {"ok": True, "token_id": token_id, "intent": intent}

    def _connect_phase_b(self, intent):
        """Phase B(**락 밖** — live tx 없음): 전용 통합 worktree에서 merge --no-ff(타임아웃, §E).
        절대 _cs()/store.tx()를 잡지 않는다 — 다른 코디네이터 변이가 이 동안 interleave 가능."""
        if not self.git:
            return None, None   # repo 미바인딩 — DB-only 응결(테스트/드라이런)
        task_id, branch = intent["task_id"], intent["branch"]
        try:
            wt = self._ensure_integration_wt()
            msg = f"CLOUD CONNECT {task_id}\n\n{self._trailer(task_id)}"
            sha = self.git.merge_into(wt, self.integration_branch, branch, msg,
                                      timeout=self.merge_timeout)
            return sha, None
        except (GitError, GitTimeout) as e:
            return None, e

    def _connect_phase_c(self, task_id, token_id, intent, merge_sha, err):
        """Phase C(임계구역): Phase B 결과를 원자 반영. 성공이면 merge_sha 먼저 기록(P0-6) 후
        해제; 실패면 CONNECTING→DONE rollback(재시도가능). 어느 쪽이든 merge_token 반납 + unpin."""
        with self._cs():
            if err is not None:
                # Phase B 실패 → rollback(재시도가능). 궤도 unpin + 토큰 반납.
                for o in self.store.pinned_orbits_for_task(task_id):
                    self.store.set_orbit(o["orbit_id"], merging=0, merge_deadline=None)
                self.store.set_task(task_id, state=fsm.advance("task", "CONNECTING", "rollback"),
                                    connect_intent_at=None)
                self._release_merge_token_locked(token_id)
                self._promote_pending()
                reason = "merge timeout" if isinstance(err, GitTimeout) else "merge conflict"
                self._emit("connect_aborted", task_id, reason=str(err))
                return {"ok": False, "reason": f"{reason}: {err}", "task_id": task_id,
                        "state": "DONE", "retryable": True}
            # 성공: P0-6 순서 — merge_sha 먼저 기록 → MERGED → write-orbit 해제(+unpin).
            self.store.set_task(task_id, merge_sha=merge_sha, merged_at=time.time())
            self.store.set_task(task_id, state=fsm.advance("task", "CONNECTING", "merged"))
            self._release_task_write_orbits(task_id)
            self._release_merge_token_locked(token_id)
            if self.git and intent.get("worktree"):
                self.git.remove_worktree(intent["worktree"])
            self.store.set_flag(task_id, "merged")
            self._emit("connect_merged", task_id, merge_sha=merge_sha)
            self._promote_pending()
            return {"ok": True, "task_id": task_id, "state": "MERGED", "merge_sha": merge_sha}

    def flag_set(self, key, value, agent_id=None, *, request_id=None, bail_epoch=None):
        """LATCH 플래그 set(현 단순형 — D3 EPHEMERAL/owner/epoch CAS는 차기 증분). §D6: 회수된
        좀비의 flag_set은 차단(bail_epoch). §D9: request_id 멱등(성공만 캐시)."""
        with self._cs():
            with self._idem(request_id, agent_id, "flag_set",
                            [key, value]) as cache:
                if cache.hit:
                    return cache.value
                dead = self._check_alive(agent_id, bail_epoch)
                if dead:
                    return cache.set(dead)
                self.store.set_flag(key, value, set_by=agent_id)
                self._emit("flag_set", agent_id or key, key=key, value=value)
                return cache.set({"ok": True})

    def flag_get(self, key):
        return {"key": key, "value": self.store.get_flag(key)}

    def status(self):
        self.sweep()
        return self.store.snapshot()
