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

import json
import os
import threading
import time
from contextlib import contextmanager

from . import fsm
from .disjoint import sets_overlap
from .events import NOOP
from .gitio import GitError, GitRepo, GitTimeout
from .store import Store

# Phase B(락밖 merge) 서브프로세스 타임아웃(§E — 무한 hang 방지). pin은 이보다 길게 잡아
# 타임아웃→abort→rollback이 완료될 시간을 준다.
MERGE_TIMEOUT_S = 120.0
MERGE_PIN_GRACE_S = 60.0


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
            self.store.add_task(task_id=task_id, name=name, writes=writes or [],
                                reads=reads or [], deps=deps or [], state="PENDING",
                                priority=priority)
        return {"task_id": task_id, "state": "PENDING"}

    def claim(self, agent_id, pathspec, mode="write", *, ttl=600.0, task_id=None,
              reason="", priority=0):
        if isinstance(pathspec, str):
            pathspec = [pathspec]
        with self._cs():
            self._sweep_inline()
            self.store.upsert_agent(agent_id)
            self._emit("orbit_requested", agent_id, mode=mode, paths=pathspec, task=task_id)
            conf = self._conflicts(pathspec, mode)
            if conf:
                oid = self.store.add_orbit(task_id=task_id, agent_id=agent_id,
                                           pathspec=pathspec, mode=mode, state="PENDING",
                                           reason=reason, priority=priority)
                if self._cycle_with(agent_id):  # 대기 시 데드락 사이클이면 거부
                    self.store.set_orbit(oid, state=fsm.advance("orbit", "PENDING", "deny"))
                    self._emit("orbit_denied", agent_id, orbit_id=oid, deadlock=True)
                    return {"orbit_id": oid, "state": "DENIED", "deadlock": True,
                            "conflicts": conf}
                self._emit("orbit_pending", agent_id, orbit_id=oid, conflicts=len(conf))
                return {"orbit_id": oid, "state": "PENDING", "conflicts": conf}
            fence = self.store.next_fence()
            oid = self.store.add_orbit(task_id=task_id, agent_id=agent_id, pathspec=pathspec,
                                       mode=mode, state="HELD", fence=fence,
                                       expires_at=time.time() + ttl, reason=reason,
                                       priority=priority)
            self._emit("orbit_granted", agent_id, orbit_id=oid, fence=fence, mode=mode)
            return {"orbit_id": oid, "state": "HELD", "fence": fence, "conflicts": []}

    def renew(self, orbit_id, agent_id, fence, ttl=600.0):
        """궤도 lease 갱신(keepalive). 소유+fence 일치해야 — 오추방된 좀비는 FENCED_OUT."""
        with self._cs():
            o = self.store.get_orbit(orbit_id)
            if not o:
                return {"ok": False, "reason": "no such orbit"}
            if o["state"] != "HELD":
                return {"ok": False, "reason": f"not HELD: {o['state']}", "fenced_out": True}
            bad = self._check_owner(o, agent_id, fence)
            if bad:
                return bad
            self.store.set_orbit(orbit_id, state=fsm.advance("orbit", "HELD", "renew"),
                                 expires_at=time.time() + ttl)
            self._emit("orbit_renewed", agent_id, orbit_id=orbit_id)
            return {"ok": True, "expires_in": ttl}

    def release(self, orbit_id, agent_id, fence):
        """궤도 lease 반납. 소유+fence 일치해야(P0-3) — 아무나 남의 궤도 해제 불가.
        이미 RELEASED/EXPIRED면 멱등 OK(MCP 재시도 안전)."""
        with self._cs():
            o = self.store.get_orbit(orbit_id)
            if not o:
                return {"ok": False, "reason": "no such orbit"}
            if o["state"] in ("RELEASED", "EXPIRED", "DENIED"):
                return {"ok": True, "noop": True, "state": o["state"]}
            if o["state"] != "HELD":
                return {"ok": False, "reason": f"not HELD: {o['state']}"}
            bad = self._check_owner(o, agent_id, fence)
            if bad:
                return bad
            self.store.set_orbit(orbit_id, state=fsm.advance("orbit", "HELD", "release"),
                                 released_at=time.time())
            self._emit("orbit_released", agent_id, orbit_id=orbit_id)
            self._promote_pending()
            return {"ok": True}

    def bail(self, agent_id):
        """물방울 긴급 탈출(자발). 보유 궤도 전부 해제 + 작업 requeue + worktree/브랜치 정리.
        멱등 — 비자발 좀비회수와 **단일 루틴**을 공유(둘 사이 누락/이중해제 없음)."""
        with self._cs():
            return self._reclaim_agent_inline(agent_id, voluntary=True)

    def heartbeat(self, agent_id):
        with self._cs():
            self.store.upsert_agent(agent_id)
        return {"ok": True}

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

    def start(self, task_id, agent_id):
        """READY task에 agent 배정 → IN_ORBIT. repo 바인딩 시 물방울 worktree 발사."""
        with self._cs():
            t = self.store.get_task(task_id)
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
            return {"task_id": task_id, "state": s, "worktree": worktree, "branch": branch}

    def commit(self, task_id, msg):
        """물방울 worktree의 변경을 커밋(repo 바인딩 시)."""
        if not self.git:
            return {"ok": False, "reason": "no repo bound"}
        with self._cs():
            t = self.store.get_task(task_id)
            sha = self.git.commit_all(t["worktree"], msg)
            self._emit("task_committed", t["agent_id"], task=task_id, sha=sha)
            return {"ok": True, "sha": sha}

    def finish(self, task_id):
        with self._cs():
            t = self.store.get_task(task_id)
            self.store.set_task(task_id, state=fsm.advance("task", t["state"], "finish"))
            self.store.set_flag(task_id, "done", set_by=t["agent_id"])
            self._emit("task_finished", t["agent_id"], task=task_id)
            return {"task_id": task_id, "state": "DONE"}

    # ---- CLOUD CONNECT — split-phase A–B–C (§3.B/§D8/§D11) ----
    def connect(self, task_id, agent_id=None, fence=None):
        """CLOUD CONNECT(응결=merge). **split-phase** — git merge가 락(_cs) **밖**에서 돈다:
          A(락): write-orbit 재검증(P0-4 HELD∧fence==captured) + merge_token 획득 + →CONNECTING
                 + 궤도 pin(merging=1) + intent 영속 + 커밋.
          B(락밖): 전용 통합 worktree에서 merge --no-ff(타임아웃, §E). 충돌/타임아웃이면 abort.
          C(락): merge_sha 먼저 기록(P0-6) → →MERGED + write-orbit 해제 + merge_token 반납 + unpin.
        fencing: 작업 중 lease 만료/해제면 거부(stale fence). merge_token으로 동시 connect를 직렬화."""
        # 멱등(P0-9/D9): 이미 응결된 task는 재머지 없이 즉시 MERGED 회신.
        t0 = self.store.get_task(task_id)
        if t0 and t0["state"] == "MERGED":
            return {"ok": True, "task_id": task_id, "state": "MERGED",
                    "merge_sha": t0["merge_sha"], "noop": True}

        deadline = time.time() + max(self.merge_timeout, 5.0) + 10.0
        while True:
            a = self._connect_phase_a(task_id, agent_id, fence)
            if not a["ok"]:
                if a.get("retry") and time.time() < deadline:
                    time.sleep(0.01)   # merge_token 경합 — 다른 connect 응결중. 곧 재시도.
                    continue
                return a
            if a.get("noop"):          # 이미 MERGED (멱등)
                return a
            # ----- Phase B: 락 밖(no _cs, no live tx) git merge -----
            token_id, intent = a["token_id"], a["intent"]
            merge_sha, err = self._connect_phase_b(intent)
            # ----- Phase C: 락 안 — merge_sha 먼저 기록 후 해제(P0-6) -----
            return self._connect_phase_c(task_id, token_id, intent, merge_sha, err)

    def _connect_phase_a(self, task_id, agent_id, fence):
        """Phase A(임계구역): fence 재검증(P0-4) + merge_token 획득 + intent 영속 + pin + →CONNECTING."""
        with self._cs():
            self._sweep_inline()
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

    def flag_set(self, key, value, agent_id=None):
        with self._cs():
            self.store.set_flag(key, value, set_by=agent_id)
            self._emit("flag_set", agent_id or key, key=key, value=value)
        return {"ok": True}

    def flag_get(self, key):
        return {"key": key, "value": self.store.get_flag(key)}

    def status(self):
        self.sweep()
        return self.store.snapshot()
