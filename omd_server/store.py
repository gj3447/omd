"""SQLite 영속화 — orbit lease + task + flag + 단조 fence 카운터.

deep-research 추천대로 SQLite expires_ts + sweeper로 시작(Agent Mail 검증 방식).
state는 컬럼으로 별도 영속(pytransitions pickle 스냅샷 의존 금지).
"""

from __future__ import annotations

import json
import sqlite3
import time
import uuid

_SCHEMA = """
CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT);
CREATE TABLE IF NOT EXISTS orbits (
  orbit_id TEXT PRIMARY KEY, task_id TEXT, agent_id TEXT,
  pathspec TEXT NOT NULL, mode TEXT NOT NULL, state TEXT NOT NULL,
  fence INTEGER, expires_at REAL, created_at REAL, released_at REAL, reason TEXT,
  priority INTEGER DEFAULT 0
);
CREATE INDEX IF NOT EXISTS idx_orbits_state ON orbits(state);
CREATE INDEX IF NOT EXISTS idx_orbits_task ON orbits(task_id);
CREATE TABLE IF NOT EXISTS tasks (
  task_id TEXT PRIMARY KEY, name TEXT, writes TEXT, reads TEXT, deps TEXT,
  state TEXT NOT NULL, agent_id TEXT, priority INTEGER, created_at REAL,
  worktree TEXT, branch TEXT
);
CREATE TABLE IF NOT EXISTS agents (
  agent_id TEXT PRIMARY KEY, name TEXT, state TEXT, last_heartbeat REAL
);
CREATE TABLE IF NOT EXISTS flags (
  key TEXT PRIMARY KEY, value TEXT, set_by TEXT, set_at REAL
);
"""


def _row(c) -> dict | None:
    r = c.fetchone()
    return dict(r) if r else None


def _rows(c) -> list[dict]:
    return [dict(r) for r in c.fetchall()]


class Store:
    def __init__(self, db_path: str = ":memory:"):
        self.db = sqlite3.connect(db_path)
        self.db.row_factory = sqlite3.Row
        self.db.executescript(_SCHEMA)
        self.db.commit()

    # --- fence: 단조 증가 토큰 ---
    def next_fence(self) -> int:
        cur = self.db.execute("SELECT value FROM meta WHERE key='fence'")
        row = cur.fetchone()
        n = (int(row["value"]) if row else 0) + 1
        self.db.execute("INSERT INTO meta(key,value) VALUES('fence',?) "
                        "ON CONFLICT(key) DO UPDATE SET value=?", (str(n), str(n)))
        self.db.commit()
        return n

    # --- orbits ---
    def add_orbit(self, *, task_id, agent_id, pathspec, mode, state,
                  fence=None, expires_at=None, reason="", priority=0) -> str:
        oid = "orb-" + uuid.uuid4().hex[:12]
        self.db.execute(
            "INSERT INTO orbits(orbit_id,task_id,agent_id,pathspec,mode,state,"
            "fence,expires_at,created_at,reason,priority) VALUES(?,?,?,?,?,?,?,?,?,?,?)",
            (oid, task_id, agent_id, json.dumps(pathspec), mode, state,
             fence, expires_at, time.time(), reason, priority))
        self.db.commit()
        return oid

    def get_orbit(self, oid) -> dict | None:
        return _row(self.db.execute("SELECT * FROM orbits WHERE orbit_id=?", (oid,)))

    def set_orbit(self, oid, *, state, expires_at=..., released_at=..., fence=...):
        sets, args = ["state=?"], [state]
        if expires_at is not ...:
            sets.append("expires_at=?"); args.append(expires_at)
        if released_at is not ...:
            sets.append("released_at=?"); args.append(released_at)
        if fence is not ...:
            sets.append("fence=?"); args.append(fence)
        args.append(oid)
        self.db.execute(f"UPDATE orbits SET {','.join(sets)} WHERE orbit_id=?", args)
        self.db.commit()

    def held_orbits(self) -> list[dict]:
        return _rows(self.db.execute("SELECT * FROM orbits WHERE state='HELD'"))

    def pending_orbits(self) -> list[dict]:
        # 우선순위 DESC → 같으면 FIFO(created_at ASC). 기아 방지 기본.
        return _rows(self.db.execute(
            "SELECT * FROM orbits WHERE state='PENDING' ORDER BY priority DESC, created_at ASC"))

    def orbits_for_task(self, task_id) -> list[dict]:
        return _rows(self.db.execute("SELECT * FROM orbits WHERE task_id=?", (task_id,)))

    def due_orbits(self, now) -> list[dict]:
        return _rows(self.db.execute(
            "SELECT * FROM orbits WHERE state='HELD' AND expires_at IS NOT NULL "
            "AND expires_at<=?", (now,)))

    # --- tasks ---
    def add_task(self, *, task_id, name, writes, reads, deps, state, priority):
        self.db.execute(
            "INSERT INTO tasks(task_id,name,writes,reads,deps,state,priority,created_at)"
            " VALUES(?,?,?,?,?,?,?,?) ON CONFLICT(task_id) DO UPDATE SET "
            "name=excluded.name,writes=excluded.writes,reads=excluded.reads,"
            "deps=excluded.deps,priority=excluded.priority",
            (task_id, name, json.dumps(writes), json.dumps(reads),
             json.dumps(deps), state, priority, time.time()))
        self.db.commit()

    def get_task(self, task_id) -> dict | None:
        return _row(self.db.execute("SELECT * FROM tasks WHERE task_id=?", (task_id,)))

    def set_task(self, task_id, *, state=..., agent_id=..., worktree=..., branch=...):
        sets, args = [], []
        for col, val in (("state", state), ("agent_id", agent_id),
                         ("worktree", worktree), ("branch", branch)):
            if val is not ...:
                sets.append(f"{col}=?"); args.append(val)
        if not sets:
            return
        args.append(task_id)
        self.db.execute(f"UPDATE tasks SET {','.join(sets)} WHERE task_id=?", args)
        self.db.commit()

    def tasks_by_state(self, states) -> list[dict]:
        q = ",".join("?" * len(states))
        return _rows(self.db.execute(
            f"SELECT * FROM tasks WHERE state IN ({q}) ORDER BY priority DESC, created_at",
            list(states)))

    # --- flags ---
    def set_flag(self, key, value, set_by=None):
        self.db.execute(
            "INSERT INTO flags(key,value,set_by,set_at) VALUES(?,?,?,?) "
            "ON CONFLICT(key) DO UPDATE SET value=?,set_by=?,set_at=?",
            (key, value, set_by, time.time(), value, set_by, time.time()))
        self.db.commit()

    def get_flag(self, key) -> str | None:
        r = _row(self.db.execute("SELECT value FROM flags WHERE key=?", (key,)))
        return r["value"] if r else None

    # --- agents (물방울 heartbeat / 좀비 회수) ---
    def upsert_agent(self, agent_id, name=None, state="WORKING", now=None):
        now = now if now is not None else time.time()
        self.db.execute(
            "INSERT INTO agents(agent_id,name,state,last_heartbeat) VALUES(?,?,?,?) "
            "ON CONFLICT(agent_id) DO UPDATE SET state=excluded.state,"
            "last_heartbeat=excluded.last_heartbeat,name=COALESCE(excluded.name,agents.name)",
            (agent_id, name, state, now))
        self.db.commit()

    def set_agent_state(self, agent_id, state):
        self.db.execute("UPDATE agents SET state=? WHERE agent_id=?", (state, agent_id))
        self.db.commit()

    def stale_agents(self, cutoff) -> list[dict]:
        return _rows(self.db.execute(
            "SELECT * FROM agents WHERE state!='RETIRED' AND last_heartbeat<?", (cutoff,)))

    def orbits_held_by_agent(self, agent_id) -> list[dict]:
        return _rows(self.db.execute(
            "SELECT * FROM orbits WHERE agent_id=? AND state='HELD'", (agent_id,)))

    def tasks_for_agent(self, agent_id) -> list[dict]:
        return _rows(self.db.execute("SELECT * FROM tasks WHERE agent_id=?", (agent_id,)))

    def snapshot(self) -> dict:
        return {
            "orbits": _rows(self.db.execute("SELECT orbit_id,task_id,mode,state,fence,expires_at FROM orbits")),
            "tasks": _rows(self.db.execute("SELECT task_id,name,state FROM tasks")),
            "flags": _rows(self.db.execute("SELECT key,value FROM flags")),
        }
