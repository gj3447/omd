"""State-space harness for the OMD lease/fence core.

The hand-written unit tests cover named regressions. This test explores longer
operation sequences against the real Coordinator and checks invariants after
every transition.
"""

from __future__ import annotations

import json
from itertools import combinations

import pytest

hypothesis = pytest.importorskip("hypothesis")
from hypothesis import strategies as st
from hypothesis import settings
from hypothesis.stateful import RuleBasedStateMachine, invariant, rule

from omd_server import Coordinator
from omd_server.disjoint import sets_overlap


AGENTS = ("ag-a", "ag-b", "ag-c")
TASKS = ("task-a", "task-b", "task-c")
PATHS = (("src/a/**",), ("src/b/**",), ("src/shared/**",))
REQUEST_IDS = ("a", "b", "c", None)


class OmdLeaseMachine(RuleBasedStateMachine):
    def __init__(self):
        super().__init__()
        self.omd = Coordinator(allow_memory_db=True, agent_ttl=None)
        self.claims: list[dict] = []
        for task_id, paths in zip(TASKS, PATHS):
            self.omd.declare(task_id, writes=list(paths))

    def _task(self, index: int) -> str:
        return TASKS[index % len(TASKS)]

    def _agent(self, index: int) -> str:
        return AGENTS[index % len(AGENTS)]

    def _paths(self, index: int) -> list[str]:
        return list(PATHS[index % len(PATHS)])

    def _request_id(self, verb: str, index: int) -> str | None:
        suffix = REQUEST_IDS[index % len(REQUEST_IDS)]
        return None if suffix is None else f"{verb}-{suffix}"

    def _live_claims(self) -> list[dict]:
        return [
            c for c in self.claims
            if c.get("state") == "HELD"
            and self.omd.store.get_orbit(c["orbit_id"]) is not None
            and self.omd.store.get_orbit(c["orbit_id"])["state"] == "HELD"
        ]

    @rule(agent_index=st.integers(0, 5),
          task_index=st.integers(0, 5),
          path_index=st.integers(0, 5),
          request_index=st.integers(0, 7))
    def claim_write(self, agent_index: int, task_index: int, path_index: int,
                    request_index: int):
        task_id = self._task(task_index)
        request_id = self._request_id("claim", request_index)
        res = self.omd.claim(
            self._agent(agent_index),
            self._paths(path_index),
            "write",
            task_id=task_id,
            request_id=request_id,
        )
        if res.get("state") == "HELD":
            self.claims.append({
                "orbit_id": res["orbit_id"],
                "agent_id": self._agent(agent_index),
                "task_id": task_id,
                "fence": res["fence"],
                "state": "HELD",
            })

    @rule(claim_index=st.integers(0, 20),
          request_index=st.integers(0, 7))
    def release_claim(self, claim_index: int, request_index: int):
        live = self._live_claims()
        if not live:
            return
        claim = live[claim_index % len(live)]
        request_id = self._request_id("release", request_index)
        self.omd.release(
            claim["orbit_id"],
            claim["agent_id"],
            claim["fence"],
            request_id=request_id,
        )

    @rule(claim_index=st.integers(0, 20))
    def renew_claim(self, claim_index: int):
        live = self._live_claims()
        if not live:
            return
        claim = live[claim_index % len(live)]
        self.omd.renew(claim["orbit_id"], claim["agent_id"], claim["fence"], ttl=600)

    @rule(claim_index=st.integers(0, 20))
    def stale_release_attempt(self, claim_index: int):
        live = self._live_claims()
        if not live:
            return
        claim = live[claim_index % len(live)]
        self.omd.release(claim["orbit_id"], claim["agent_id"], claim["fence"] + 999)

    @rule(agent_index=st.integers(0, 5))
    def bail_agent(self, agent_index: int):
        self.omd.bail(self._agent(agent_index))

    @rule(agent_index=st.integers(0, 5))
    def heartbeat_agent(self, agent_index: int):
        self.omd.heartbeat(self._agent(agent_index))

    @rule(agent_index=st.integers(0, 5))
    def pick_next_task(self, agent_index: int):
        self.omd.next_task(self._agent(agent_index))

    @rule(task_index=st.integers(0, 5),
          agent_index=st.integers(0, 5),
          request_index=st.integers(0, 7))
    def start_task(self, task_index: int, agent_index: int, request_index: int):
        task_id = self._task(task_index)
        task = self.omd.store.get_task(task_id)
        if task and task["state"] in ("PENDING", "BLOCKED"):
            self.omd.next_task(self._agent(agent_index))
            task = self.omd.store.get_task(task_id)
        if task and task["state"] == "READY":
            request_id = self._request_id("start", request_index)
            self.omd.start(task_id, self._agent(agent_index), request_id=request_id)

    @rule(task_index=st.integers(0, 5),
          request_index=st.integers(0, 7))
    def finish_task(self, task_index: int, request_index: int):
        task_id = self._task(task_index)
        task = self.omd.store.get_task(task_id)
        if task and task["state"] == "IN_ORBIT":
            request_id = self._request_id("finish", request_index)
            self.omd.finish(task_id, request_id=request_id)

    @rule(task_index=st.integers(0, 5),
          request_index=st.integers(0, 7))
    def connect_task(self, task_index: int, request_index: int):
        task_id = self._task(task_index)
        task = self.omd.store.get_task(task_id)
        if task and task["state"] == "DONE":
            request_id = self._request_id("connect", request_index)
            self.omd.connect(task_id, request_id=request_id)

    @rule()
    def sweep(self):
        self.omd.sweep()

    @invariant()
    def no_overlapping_held_write_orbits(self):
        held = self.omd.store.held_orbits()
        for left, right in combinations(held, 2):
            if left["mode"] == right["mode"] == "read":
                continue
            assert not sets_overlap(
                json.loads(left["pathspec"]),
                json.loads(right["pathspec"]),
            ), (left, right)

    @invariant()
    def live_fences_are_unique(self):
        fences = [
            orbit["fence"] for orbit in self.omd.store.held_orbits()
            if orbit["fence"] is not None
        ]
        assert len(fences) == len(set(fences)), fences

    @invariant()
    def merged_tasks_have_no_held_write_orbit(self):
        for task in self.omd.store.all_tasks():
            if task["state"] != "MERGED":
                continue
            held_writes = [
                orbit for orbit in self.omd.store.orbits_for_task(task["task_id"])
                if orbit["state"] == "HELD" and orbit["mode"] == "write"
            ]
            assert held_writes == []

    @invariant()
    def retired_agents_hold_no_orbits(self):
        for agent_id in AGENTS:
            agent = self.omd.store.get_agent(agent_id)
            if agent is None or agent["state"] != "RETIRED":
                continue
            held = [
                orbit for orbit in self.omd.store.held_orbits()
                if orbit["agent_id"] == agent_id
            ]
            assert held == []


TestOmdLeaseMachine = OmdLeaseMachine.TestCase
TestOmdLeaseMachine.settings = settings(max_examples=60, stateful_step_count=35)
