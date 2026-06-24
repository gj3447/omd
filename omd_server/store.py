"""SQLite 영속화 — orbit lease + task + flag + 단조 fence 카운터.

deep-research 추천대로 SQLite expires_ts + sweeper로 시작(Agent Mail 검증 방식).
state는 컬럼으로 별도 영속(pytransitions pickle 스냅샷 의존 금지).

**동시성(D1, CONCURRENCY.md §D1).** 연결은 autocommit(`isolation_level=None`) + WAL로 열고,
모든 변이는 `with store.tx():`(= `BEGIN IMMEDIATE … COMMIT/ROLLBACK`) 한 트랜잭션 안에서 일어난다.
이로써 check-then-act(예: claim의 충돌검사→grant)가 원자적이 되어 SINGULON TOCTOU(P0-1)가 닫히고,
fence 발급(P0-2)이 단조·유일해진다. `tx()`는 깊이 카운터로 **재진입 가능**(중첩 호출은 새 BEGIN을 안 연다) —
Coordinator가 한 동사 안에서 sweep/_promote_pending을 같은 트랜잭션으로 호출할 수 있게 한다.
"""

from __future__ import annotations

import json
import sqlite3
import time
import uuid
from contextlib import contextmanager

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
-- 발급된 fence는 단조·전역 유일(P0-2). 코드 회귀로 중복을 만들면 IntegrityError로 fail-closed.
CREATE UNIQUE INDEX IF NOT EXISTS uq_orbits_fence ON orbits(fence) WHERE fence IS NOT NULL;
CREATE TABLE IF NOT EXISTS tasks (
  task_id TEXT PRIMARY KEY, name TEXT, writes TEXT, reads TEXT, deps TEXT,
  state TEXT NOT NULL, agent_id TEXT, priority INTEGER, created_at REAL,
  worktree TEXT, branch TEXT, captured_fence INTEGER
);
CREATE TABLE IF NOT EXISTS agents (
  agent_id TEXT PRIMARY KEY, name TEXT, state TEXT, last_heartbeat REAL
);
CREATE TABLE IF NOT EXISTS flags (
  key TEXT PRIMARY KEY, value TEXT, set_by TEXT, set_at REAL
);
-- 단조 fence 카운터 시드(0). next_fence는 이 행을 in-statement로 +1 한다(읽고-쓰기 갭 없음).
INSERT OR IGNORE INTO meta(key,value) VALUES('fence','0');
"""


def _row(c) -> dict | None:
    r = c.fetchone()
    return dict(r) if r else None


def _rows(c) -> list[dict]:
    return [dict(r) for r in c.fetchall()]


class Store:
    def __init__(self, db_path: str = ":memory:"):
        # autocommit 모드: BEGIN을 우리가 명시 발행(tx()). 기본("") 모드는 DML 전 암묵 BEGIN을
        # 끼워넣어 BEGIN IMMEDIATE를 무력화하므로 반드시 None.
        self.db = sqlite3.connect(db_path, isolation_level=None, check_same_thread=False)
        self.db.row_factory = sqlite3.Row
        # WAL: 동시 reader가 단일 writer를 안 막음 + 멀티프로세스 안전(BEGIN IMMEDIATE 백스톱).
        # busy_timeout: writer 경합 시 즉시 SQLITE_BUSY 대신 블록-재시도. (CONCURRENCY §D1)
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.execute("PRAGMA busy_timeout=5000")
        self.db.execute("PRAGMA foreign_keys=ON")
        self.db.execute("PRAGMA synchronous=NORMAL")
        self.db.executescript(_SCHEMA)
        # 기존 DB 마이그레이션(컬럼 추가는 멱등치 않으므로 가드). 신규 DB는 _SCHEMA가 이미 포함.
        for tbl, col, decl in [("tasks", "captured_fence", "INTEGER")]:
            try:
                self.db.execute(f"ALTER TABLE {tbl} ADD COLUMN {col} {decl}")
            except sqlite3.OperationalError:
                pass  # 이미 존재
        self._txn_depth = 0

    # --- 트랜잭션 경계(재진입 가능) ---
    @contextmanager
    def tx(self):
        """BEGIN IMMEDIATE … COMMIT(정상) / ROLLBACK(예외). 깊이 카운터로 재진입 안전:
        중첩 호출은 새 BEGIN을 열지 않고, 최외곽에서만 COMMIT/ROLLBACK 한다.
        예외가 최외곽까지 전파되면 전체를 롤백(부분쓰기 없음); 중간에서 잡혀 정상 종료하면 COMMIT."""
        if self._txn_depth == 0:
            self.db.execute("BEGIN IMMEDIATE")
        self._txn_depth += 1
        try:
            yield
        except BaseException:
            self._txn_depth -= 1
            if self._txn_depth == 0:
                self.db.execute("ROLLBACK")
            raise
        else:
            self._txn_depth -= 1
            if self._txn_depth == 0:
                self.db.execute("COMMIT")

    # --- fence: 단조 증가·유일 토큰 (P0-2: 단일문 +1, 읽고-쓰기 갭 제거) ---
    def next_fence(self) -> int:
        self.db.execute("UPDATE meta SET value=CAST(value AS INTEGER)+1 WHERE key='fence'")
        return int(self.db.execute("SELECT value FROM meta WHERE key='fence'").fetchone()["value"])

    def current_fence(self) -> int:
        r = self.db.execute("SELECT value FROM meta WHERE key='fence'").fetchone()
        return int(r["value"]) if r else 0

    # --- orbits ---
    def add_orbit(self, *, task_id, agent_id, pathspec, mode, state,
                  fence=None, expires_at=None, reason="", priority=0) -> str:
        oid = "orb-" + uuid.uuid4().hex[:12]
        self.db.execute(
            "INSERT INTO orbits(orbit_id,task_id,agent_id,pathspec,mode,state,"
            "fence,expires_at,created_at,reason,priority) VALUES(?,?,?,?,?,?,?,?,?,?,?)",
            (oid, task_id, agent_id, json.dumps(pathspec), mode, state,
             fence, expires_at, time.time(), reason, priority))
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

    def get_task(self, task_id) -> dict | None:
        return _row(self.db.execute("SELECT * FROM tasks WHERE task_id=?", (task_id,)))

    def set_task(self, task_id, *, state=..., agent_id=..., worktree=..., branch=...,
                 captured_fence=...):
        sets, args = [], []
        for col, val in (("state", state), ("agent_id", agent_id),
                         ("worktree", worktree), ("branch", branch),
                         ("captured_fence", captured_fence)):
            if val is not ...:
                sets.append(f"{col}=?"); args.append(val)
        if not sets:
            return
        args.append(task_id)
        self.db.execute(f"UPDATE tasks SET {','.join(sets)} WHERE task_id=?", args)

    def all_tasks(self) -> list[dict]:
        return _rows(self.db.execute("SELECT * FROM tasks"))

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

    def get_agent(self, agent_id) -> dict | None:
        return _row(self.db.execute("SELECT * FROM agents WHERE agent_id=?", (agent_id,)))

    def set_agent_state(self, agent_id, state):
        self.db.execute("UPDATE agents SET state=? WHERE agent_id=?", (state, agent_id))

    def stale_agents(self, cutoff) -> list[dict]:
        return _rows(self.db.execute(
            "SELECT * FROM agents WHERE state!='RETIRED' AND last_heartbeat<?", (cutoff,)))

    def orbits_held_by_agent(self, agent_id) -> list[dict]:
        return _rows(self.db.execute(
            "SELECT * FROM orbits WHERE agent_id=? AND state='HELD'", (agent_id,)))

    def orbits_owned_by_agent(self, agent_id, states=("HELD", "PENDING")) -> list[dict]:
        q = ",".join("?" * len(states))
        return _rows(self.db.execute(
            f"SELECT * FROM orbits WHERE agent_id=? AND state IN ({q})",
            (agent_id, *states)))

    def tasks_for_agent(self, agent_id) -> list[dict]:
        return _rows(self.db.execute("SELECT * FROM tasks WHERE agent_id=?", (agent_id,)))

    def snapshot(self) -> dict:
        return {
            "orbits": _rows(self.db.execute("SELECT orbit_id,task_id,mode,state,fence,expires_at FROM orbits")),
            "tasks": _rows(self.db.execute("SELECT task_id,name,state FROM tasks")),
            "flags": _rows(self.db.execute("SELECT key,value FROM flags")),
        }
