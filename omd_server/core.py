"""OMD Coordinator — 군단장 코어 로직.

SINGULON 불변식을 2지점에서 강제:
  ① claim/next     : write-set이 활성 HELD 궤도와 서로소(입체)일 때만 grant/배정 (사전)
  ② connect(merge) : 작업 중 lease가 만료/해제됐으면 응결 거부 (fencing, merge 게이트)
=> CLOUD CONNECT 시 충돌(분열)이 구조적으로 0.
"""

from __future__ import annotations

import json
import time

import os

from . import fsm
from .disjoint import sets_overlap
from .gitio import GitRepo
from .store import Store


class Coordinator:
    def __init__(self, db_path: str = ":memory:", repo: str | None = None,
                 worktrees_dir: str | None = None, agent_ttl: float | None = None):
        self.store = Store(db_path)
        self.agent_ttl = agent_ttl  # heartbeat 만료 시 좀비 회수 (None=비활성)
        self.git = GitRepo(repo) if repo else None
        if self.git:
            self.worktrees_dir = worktrees_dir or (self.git.root.rstrip("/") + "-omd-worktrees")
            os.makedirs(self.worktrees_dir, exist_ok=True)

    # ---- 내부 ----
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
        for o in self.store.pending_orbits():
            if not self._conflicts(json.loads(o["pathspec"]), o["mode"]):
                fence = self.store.next_fence()
                self.store.set_orbit(
                    o["orbit_id"],
                    state=fsm.advance("orbit", "PENDING", "grant"),
                    expires_at=time.time() + 600, fence=fence)

    # ---- 공개 API (= MCP 툴 / CLI 동사) ----
    def declare(self, task_id, *, name="", writes=None, reads=None, deps=None, priority=0):
        self.store.add_task(task_id=task_id, name=name, writes=writes or [],
                            reads=reads or [], deps=deps or [], state="PENDING",
                            priority=priority)
        return {"task_id": task_id, "state": "PENDING"}

    def claim(self, agent_id, pathspec, mode="write", *, ttl=600.0, task_id=None, reason=""):
        self.sweep()
        self.store.upsert_agent(agent_id)
        if isinstance(pathspec, str):
            pathspec = [pathspec]
        conf = self._conflicts(pathspec, mode)
        if conf:
            oid = self.store.add_orbit(task_id=task_id, agent_id=agent_id,
                                       pathspec=pathspec, mode=mode, state="PENDING",
                                       reason=reason)
            return {"orbit_id": oid, "state": "PENDING", "conflicts": conf}
        fence = self.store.next_fence()
        oid = self.store.add_orbit(task_id=task_id, agent_id=agent_id, pathspec=pathspec,
                                   mode=mode, state="HELD", fence=fence,
                                   expires_at=time.time() + ttl, reason=reason)
        return {"orbit_id": oid, "state": "HELD", "fence": fence, "conflicts": []}

    def renew(self, orbit_id, ttl=600.0):
        o = self.store.get_orbit(orbit_id)
        if not o or o["state"] != "HELD":
            return {"ok": False, "reason": f"orbit not HELD: {o and o['state']}"}
        self.store.set_orbit(orbit_id, state=fsm.advance("orbit", "HELD", "renew"),
                             expires_at=time.time() + ttl)
        return {"ok": True, "expires_in": ttl}

    def release(self, orbit_id):
        o = self.store.get_orbit(orbit_id)
        if not o or o["state"] != "HELD":
            return {"ok": False, "reason": "not HELD"}
        self.store.set_orbit(orbit_id, state=fsm.advance("orbit", "HELD", "release"),
                             released_at=time.time())
        self._promote_pending()
        return {"ok": True}

    def heartbeat(self, agent_id):
        self.store.upsert_agent(agent_id)
        return {"ok": True}

    def reclaim_zombies(self):
        """heartbeat 끊긴 물방울 회수: HELD 궤도 만료 + 작업 requeue + worktree 정리."""
        if not self.agent_ttl:
            return {"reclaimed": []}
        cutoff = time.time() - self.agent_ttl
        out = []
        for a in self.store.stale_agents(cutoff):
            aid = a["agent_id"]
            for o in self.store.orbits_held_by_agent(aid):
                self.store.set_orbit(o["orbit_id"],
                                     state=fsm.advance("orbit", "HELD", "expire"))
            for t in self.store.tasks_for_agent(aid):
                if t["state"] in ("CLAIMED", "IN_ORBIT"):
                    s = fsm.advance("task", t["state"], "abort")
                    s = fsm.advance("task", s, "requeue")  # ABORTED→PENDING
                    self.store.set_task(t["task_id"], state=s, agent_id=None)
                    if self.git and t["worktree"]:
                        self.git.remove_worktree(t["worktree"])
            self.store.set_agent_state(aid, "RETIRED")
            out.append(aid)
        return {"reclaimed": out}

    def sweep(self):
        if self.agent_ttl:
            self.reclaim_zombies()
        now = time.time()
        expired = []
        for o in self.store.due_orbits(now):
            self.store.set_orbit(o["orbit_id"],
                                 state=fsm.advance("orbit", "HELD", "expire"))
            expired.append(o["orbit_id"])
        self._promote_pending()
        return {"expired": expired}

    def next_task(self, agent_id):
        """deps 충족 + write-set이 활성 HELD와 서로소인 작업 1개 → READY로 올려 반환."""
        self.sweep()
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
            return self.store.get_task(t["task_id"])
        return None

    def start(self, task_id, agent_id):
        """READY task에 agent 배정 → IN_ORBIT. repo 바인딩 시 물방울 worktree 발사."""
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
        return {"task_id": task_id, "state": s, "worktree": worktree, "branch": branch}

    def commit(self, task_id, msg):
        """물방울 worktree의 변경을 커밋(repo 바인딩 시)."""
        if not self.git:
            return {"ok": False, "reason": "no repo bound"}
        t = self.store.get_task(task_id)
        sha = self.git.commit_all(t["worktree"], msg)
        return {"ok": True, "sha": sha}

    def finish(self, task_id):
        t = self.store.get_task(task_id)
        self.store.set_task(task_id, state=fsm.advance("task", t["state"], "finish"))
        self.store.set_flag(task_id, "done", set_by=t["agent_id"])
        return {"task_id": task_id, "state": "DONE"}

    def connect(self, task_id):
        """CLOUD CONNECT(응결=merge). fencing: 작업 중 lease 만료/해제면 거부."""
        self.sweep()
        orbs = self.store.orbits_for_task(task_id)
        writes = [o for o in orbs if o["mode"] == "write"]
        if not writes:
            return {"ok": False, "reason": "no write orbit for task"}
        stale = [o["orbit_id"] for o in writes if o["state"] != "HELD"]
        if stale:
            return {"ok": False, "reason": "stale fence: lease expired/released during work",
                    "stale": stale}
        t = self.store.get_task(task_id)
        s = t["state"]
        if s == "IN_ORBIT":
            s = fsm.advance("task", s, "finish")
        s = fsm.advance("task", s, "connect")  # DONE→CONNECTING
        self.store.set_task(task_id, state=s)
        merge_sha = None
        if self.git:
            from .gitio import GitError
            try:
                merge_sha = self.git.merge(t["branch"], f"CLOUD CONNECT {task_id}")
            except GitError as e:
                self.store.set_task(task_id, state=fsm.advance("task", "CONNECTING", "abort"))
                return {"ok": False, "reason": str(e), "task_id": task_id, "state": "ABORTED"}
        self.store.set_task(task_id, state=fsm.advance("task", "CONNECTING", "merged"))
        for o in writes:
            self.store.set_orbit(o["orbit_id"],
                                 state=fsm.advance("orbit", "HELD", "release"),
                                 released_at=time.time())
        if self.git:
            self.git.remove_worktree(t["worktree"])
        self.store.set_flag(task_id, "merged")
        self._promote_pending()
        return {"ok": True, "task_id": task_id, "state": "MERGED", "merge_sha": merge_sha}

    def flag_set(self, key, value, agent_id=None):
        self.store.set_flag(key, value, set_by=agent_id)
        return {"ok": True}

    def flag_get(self, key):
        return {"key": key, "value": self.store.get_flag(key)}

    def status(self):
        self.sweep()
        return self.store.snapshot()
