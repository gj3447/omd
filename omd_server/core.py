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

TODO(다음 증분, CONCURRENCY §D1/§3.B): start/connect의 git 서브프로세스는 현재 임계구역(lock+tx)
**안에서** 돈다. split-phase(A:락→B:락밖 git+merge_token→C:락)로 빼야 멀티프로세스 stall이 없다.
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
from .gitio import GitRepo
from .store import Store


class Coordinator:
    def __init__(self, db_path: str = ":memory:", repo: str | None = None,
                 worktrees_dir: str | None = None, agent_ttl: float | None = 90.0,
                 events=None):
        self.store = Store(db_path)
        # heartbeat 만료 시 좀비 회수. 기본 ON(P0-7) — None=비활성. 끄면 죽은 물방울의
        # 궤도/작업이 영구 고아가 된다(사용자 핵심 우려). 권장 90s, renew는 TTL/3 주기.
        self.agent_ttl = agent_ttl
        self.events = events or NOOP
        self._lock = threading.RLock()  # 프로세스내 단일 writer(actor 대용) — D1
        self.git = GitRepo(repo) if repo else None
        if self.git:
            self.worktrees_dir = worktrees_dir or (self.git.root.rstrip("/") + "-omd-worktrees")
            os.makedirs(self.worktrees_dir, exist_ok=True)
            self.integration_branch = self.git.current_branch()  # P0-5: merge 착지 브랜치 고정
            self._recover()  # P0-6: 재기동 시 CONNECTING 고착 task를 git 기준 재조정

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
        for o in self.store.orbits_owned_by_agent(agent_id, ("HELD", "PENDING")):
            trig = "expire" if o["state"] == "HELD" else "deny"  # HELD→EXPIRED, PENDING→DENIED
            self.store.set_orbit(o["orbit_id"], state=fsm.advance("orbit", o["state"], trig))
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

    # ---- 공개 API (= MCP 툴 / CLI 동사) ----
    def _would_cycle(self, task_id, deps) -> bool:
        """task_id→deps를 더하면 task 의존그래프에 task_id를 지나는 사이클이 생기나 (P0-10).
        생기면 상호의존 task들이 영원히 BLOCKED(어느 쪽도 deps-MERGED 못 됨)된다."""
        graph = {t["task_id"]: json.loads(t["deps"]) for t in self.store.all_tasks()}
        graph[task_id] = list(deps)

        def dfs(n, path):
            for m in graph.get(n, []):
                if m == task_id:
                    return True
                if m not in path and dfs(m, path | {m}):
                    return True
            return False

        return dfs(task_id, {task_id})

    def declare(self, task_id, *, name="", writes=None, reads=None, deps=None, priority=0):
        deps = deps or []
        with self._cs():
            if deps and self._would_cycle(task_id, deps):
                self._emit("task_declare_rejected", task_id, reason="dep_cycle", deps=deps)
                raise ValueError(f"dependency cycle: {task_id} deps {deps} would form a cycle")
            self.store.add_task(task_id=task_id, name=name, writes=writes or [],
                                reads=reads or [], deps=deps, state="PENDING",
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
            # P0-4: 작업 시작 시점의 lease fence를 task에 고정(captured). connect는 이 fence와
            # 일치하는 HELD 궤도에서만 merge한다 → 도중 lease가 만료/회수/재부여(ABA)되면 거부.
            cf = next((o["fence"] for o in self.store.orbits_for_task(task_id)
                       if o["mode"] == "write" and o["state"] == "HELD"
                       and o["agent_id"] == agent_id), None)
            self.store.set_task(task_id, state=s, agent_id=agent_id,
                                worktree=worktree if self.git else ...,
                                branch=branch if self.git else ...,
                                captured_fence=cf if cf is not None else ...)
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

    def _recover(self):
        """P0-6/§D8 — 재기동 시 CONNECTING에 고착된 task를 git 기준으로 재조정.
        merge가 통합 브랜치에 실제 착지 → finalize(MERGED+궤도해제+worktree정리);
        안 착지 → abort→requeue(PENDING)+정리=재시도 가능. 이중쓰기 크래시의 영구고착/고아 해소."""
        if not self.git:
            return
        with self._cs():
            for t in self.store.tasks_by_state(["CONNECTING"]):
                tid = t["task_id"]
                if self.git.connect_landed(tid):
                    self.store.set_task(tid, state=fsm.advance("task", "CONNECTING", "merged"))
                    for o in self.store.orbits_for_task(tid):
                        if o["mode"] == "write" and o["state"] == "HELD":
                            self.store.set_orbit(o["orbit_id"],
                                                 state=fsm.advance("orbit", "HELD", "release"),
                                                 released_at=time.time())
                    if t.get("worktree"):
                        self.git.remove_worktree(t["worktree"])
                    self.store.set_flag(tid, "merged")
                    self._emit("recovered_merged", tid, task=tid)
                else:
                    s = fsm.advance("task", "CONNECTING", "abort")
                    self.store.set_task(tid, state=fsm.advance("task", s, "requeue"),
                                        agent_id=None, worktree=None, branch=None,
                                        captured_fence=None)
                    for o in self.store.orbits_for_task(tid):
                        if o["state"] == "HELD":
                            self.store.set_orbit(o["orbit_id"],
                                                 state=fsm.advance("orbit", "HELD", "release"),
                                                 released_at=time.time())
                        elif o["state"] == "PENDING":
                            self.store.set_orbit(o["orbit_id"],
                                                 state=fsm.advance("orbit", "PENDING", "deny"))
                    if t.get("worktree"):
                        self.git.remove_worktree(t["worktree"])
                    if t.get("branch"):
                        self.git.delete_branch(t["branch"])
                    self._emit("recovered_requeued", tid, task=tid)
            self._promote_pending()

    def connect(self, task_id):
        """CLOUD CONNECT(응결=merge). fencing: 작업 중 lease 만료/해제면 거부."""
        with self._cs():
            self._sweep_inline()
            orbs = self.store.orbits_for_task(task_id)
            writes = [o for o in orbs if o["mode"] == "write"]
            if not writes:
                return {"ok": False, "reason": "no write orbit for task"}
            # P0-4: HELD 만으로 부족 — 시작 시 고정한 fence(captured)와 일치까지 요구.
            # lease가 도중 만료→재부여(ABA)되면 state는 HELD로 돌아와도 fence가 달라 거부.
            cf = self.store.get_task(task_id).get("captured_fence")
            stale = [o["orbit_id"] for o in writes
                     if o["state"] != "HELD" or (cf is not None and o["fence"] != cf)]
            if stale:
                self._emit("connect_rejected", task_id, reason="stale_fence", stale=stale)
                return {"ok": False,
                        "reason": "stale fence: lease expired/released/regranted during work",
                        "stale": stale}
            t = self.store.get_task(task_id)
            # P0-11/§D10 — write-set FS 강제: 브랜치가 실제로 바꾼 파일이 모두 선언
            # write-set(궤도 glob) 안에 있는지 감사. 밖이면 '선언상 입체'가 실제로 겹쳐
            # 분열을 낼 수 있으므로(SINGULON 토대) merge 거부 — 통합 브랜치 불변.
            wsglobs = json.loads(t["writes"]) if t.get("writes") else []
            if self.git and t.get("branch") and wsglobs:
                try:
                    changed = self.git.diff_names(t["branch"], self.git.current_branch())
                except Exception:
                    changed = []
                violations = [f for f in changed if not sets_overlap([f], wsglobs)]
                if violations:
                    self._emit("connect_rejected", task_id, reason="write_set_violation",
                               files=violations[:20])
                    return {"ok": False,
                            "reason": "write-set violation: files outside declared orbit",
                            "violations": violations[:20], "task_id": task_id}
            s = t["state"]
            if s == "IN_ORBIT":
                s = fsm.advance("task", s, "finish")
            s = fsm.advance("task", s, "connect")  # DONE→CONNECTING
            self.store.set_task(task_id, state=s)
            merge_sha = None
            if self.git:
                from .gitio import GitError
                try:
                    merge_sha = self.git.merge(t["branch"], f"CLOUD CONNECT {task_id}",
                                               base=getattr(self, "integration_branch", None))
                except GitError as e:
                    self.store.set_task(task_id, state=fsm.advance("task", "CONNECTING", "abort"))
                    self._emit("connect_aborted", task_id, reason=str(e))
                    return {"ok": False, "reason": str(e), "task_id": task_id, "state": "ABORTED"}
            self.store.set_task(task_id, state=fsm.advance("task", "CONNECTING", "merged"))
            for o in writes:
                self.store.set_orbit(o["orbit_id"],
                                     state=fsm.advance("orbit", "HELD", "release"),
                                     released_at=time.time())
            if self.git:
                self.git.remove_worktree(t["worktree"])
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
