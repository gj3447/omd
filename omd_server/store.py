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
  priority INTEGER DEFAULT 0,
  -- 증분3(§4.1 LEASE 통합): orbit|merge_token. merge_token=repo-wide Semaphore(max=1, §D11).
  kind TEXT NOT NULL DEFAULT 'orbit',
  resource_key TEXT,                  -- merge_token이면 통합 레포 키(cloud_id 등)
  merging INTEGER NOT NULL DEFAULT 0,  -- 1=connect Phase B 진행중 pin(sweep/reclaim skip, §E)
  merge_deadline REAL,                 -- pin 유계(§E): 이 시각 넘으면 abort 대상
  merge_started_mono REAL,             -- merge_token crash-safe(§D11): dangling merge abort 판정
  intent_key TEXT                      -- 증분5(§D9): claim 자연 멱등 — hash(agent,paths,mode,task)
);
CREATE INDEX IF NOT EXISTS idx_orbits_state ON orbits(state);
CREATE INDEX IF NOT EXISTS idx_orbits_task ON orbits(task_id);
CREATE INDEX IF NOT EXISTS idx_orbits_intent ON orbits(intent_key, state);
-- 발급된 fence는 단조·전역 유일(P0-2). 코드 회귀로 중복을 만들면 IntegrityError로 fail-closed.
CREATE UNIQUE INDEX IF NOT EXISTS uq_orbits_fence ON orbits(fence) WHERE fence IS NOT NULL;
CREATE TABLE IF NOT EXISTS tasks (
  task_id TEXT PRIMARY KEY, name TEXT, writes TEXT, reads TEXT, deps TEXT,
  state TEXT NOT NULL, agent_id TEXT, priority INTEGER, created_at REAL,
  worktree TEXT, branch TEXT,
  -- 증분3(§4.1, §D8): split-phase connect intent + git 진실 조정용
  connect_fence INTEGER,        -- Phase A에서 capture한 write-orbit fence(P0-4 재검증 기준)
  connect_intent_at REAL,       -- intent 영속 타임스탬프(복구가 CONNECTING 식별)
  branch_tip_sha TEXT,          -- merge 직전 task 브랜치 tip(복구 trailer-probe 보조)
  merge_sha TEXT,               -- 응결된 merge 커밋(MERGED 증거, P0-6: release 전에 기록)
  merged_at REAL
);
CREATE TABLE IF NOT EXISTS agents (
  agent_id TEXT PRIMARY KEY, name TEXT, state TEXT, last_heartbeat REAL,
  -- 증분5(§D6): 좀비 GC-pause 부활 방지. reclaim 이 단조 증가시키고, 변이는 caller가 든
  -- bail_epoch가 현재값과 일치하는지 본다. 재생성(같은 id 재upsert)해도 epoch는 보존 → 낡은
  -- epoch를 든 좀비는 FENCED_OUT. heartbeat 의 state 리셋(WORKING)으로는 못 우회한다.
  bail_epoch INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS flags (
  key TEXT PRIMARY KEY, value TEXT, set_by TEXT, set_at REAL,
  -- 증분6(§D3): EPHEMERAL(=lease, 소유+TTL+heartbeat, 죽으면 자동 clear/BROKEN, reclaim 대상)
  -- vs LATCH(영속·단조 done(1)<merged(2), 소유분리, 회수 대상 아님, 하향 에러).
  flag_type TEXT NOT NULL DEFAULT 'LATCH',
  -- ABA/유령기상 방어: set/clear/break 마다 +1. flag_wait 가 value 가 아니라 epoch 로 재검사.
  epoch INTEGER NOT NULL DEFAULT 0,
  -- 단조 LATCH 랭크: done=1 < merged=2 (0=랭크 없음). 하향 set 은 거부.
  rank INTEGER NOT NULL DEFAULT 0,
  -- LIVE | CLEARED | BROKEN. EPHEMERAL 보유자 사망 → BROKEN(대기자 PRODUCER_DEAD 기상).
  status TEXT NOT NULL DEFAULT 'LIVE',
  -- EPHEMERAL 일 때만: 소유 agent + 받쳐주는 lease(orbits.kind='flag_ephemeral') id.
  owner_agent TEXT, lease_id TEXT
);
-- 증분6(§D3): flag_wait register→poll(서버 비블로킹). timeout 필수. observed_epoch 로 재검사
-- (ABA/유령기상 안전). producer 사망 시 BROKEN→poll 이 PRODUCER_DEAD 로 기상(영구 hang 없음).
CREATE TABLE IF NOT EXISTS flag_waiters (
  waiter_id TEXT PRIMARY KEY, agent_id TEXT, key TEXT, want_value TEXT,
  observed_epoch INTEGER, deadline REAL, state TEXT NOT NULL, wake_reason TEXT,
  created_at REAL
);
CREATE INDEX IF NOT EXISTS idx_flag_waiters_key ON flag_waiters(key, state);
-- 증분5(§D9): request_id 멱등 테이블. INFLIGHT(진행중)→DONE(성공종단만 캐시). DENIED/stale-fence
-- 같은 비성공은 캐시 안 함(세상이 바뀌면 재시도 가능해야 — §3.C). at-least-once MCP 재시도가
-- 두 번째 효과를 일으키지 않게 한다(claim 누수·이중 merge·이중 release 차단).
CREATE TABLE IF NOT EXISTS idempotency (
  request_id TEXT PRIMARY KEY, agent_id TEXT, verb TEXT, arg_hash TEXT,
  status TEXT NOT NULL, response TEXT, created_at REAL, completed_at REAL
);
-- 단조 fence 카운터 시드(0). next_fence는 이 행을 in-statement로 +1 한다(읽고-쓰기 갭 없음).
INSERT OR IGNORE INTO meta(key,value) VALUES('fence','0');
"""

# 기존 DB(증분1·2 스키마)에도 증분3 컬럼을 멱등 추가 — fresh-DB는 위 CREATE로 이미 가짐.
_MIGRATIONS = [
    ("orbits", "kind", "TEXT NOT NULL DEFAULT 'orbit'"),
    ("orbits", "resource_key", "TEXT"),
    ("orbits", "merging", "INTEGER NOT NULL DEFAULT 0"),
    ("orbits", "merge_deadline", "REAL"),
    ("orbits", "merge_started_mono", "REAL"),
    ("tasks", "connect_fence", "INTEGER"),
    ("tasks", "connect_intent_at", "REAL"),
    ("tasks", "branch_tip_sha", "TEXT"),
    ("tasks", "merge_sha", "TEXT"),
    ("tasks", "merged_at", "REAL"),
    # 증분5(§D6/§D9)
    ("agents", "bail_epoch", "INTEGER NOT NULL DEFAULT 0"),
    ("orbits", "intent_key", "TEXT"),
    # 증분6(§D3 flags): EPHEMERAL/LATCH 분리 + wait register→poll.
    ("flags", "flag_type", "TEXT NOT NULL DEFAULT 'LATCH'"),
    ("flags", "epoch", "INTEGER NOT NULL DEFAULT 0"),
    ("flags", "rank", "INTEGER NOT NULL DEFAULT 0"),
    ("flags", "status", "TEXT NOT NULL DEFAULT 'LIVE'"),
    ("flags", "owner_agent", "TEXT"),
    ("flags", "lease_id", "TEXT"),
]


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
        self._migrate()
        self._txn_depth = 0

    def _migrate(self):
        """멱등 컬럼 추가(증분3) — 기존 DB도 안전하게 신규 컬럼을 얻는다(fresh-DB 친화)."""
        for table, col, decl in _MIGRATIONS:
            cols = {r["name"] for r in self.db.execute(f"PRAGMA table_info({table})")}
            if col not in cols:
                self.db.execute(f"ALTER TABLE {table} ADD COLUMN {col} {decl}")

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
                  fence=None, expires_at=None, reason="", priority=0,
                  kind="orbit", resource_key=None, intent_key=None) -> str:
        oid = "orb-" + uuid.uuid4().hex[:12]
        self.db.execute(
            "INSERT INTO orbits(orbit_id,task_id,agent_id,pathspec,mode,state,"
            "fence,expires_at,created_at,reason,priority,kind,resource_key,intent_key) "
            "VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (oid, task_id, agent_id, json.dumps(pathspec), mode, state,
             fence, expires_at, time.time(), reason, priority, kind, resource_key,
             intent_key))
        return oid

    def orbit_by_intent(self, intent_key) -> dict | None:
        """증분5(§D9): 같은 intent_key의 살아있는 궤도(HELD/PENDING) — claim 자연 멱등.
        같은 (agent,paths,mode,task) 재시도가 새 궤도(누수 lease)를 만드는 대신 기존 것을 반환."""
        return _row(self.db.execute(
            "SELECT * FROM orbits WHERE intent_key=? AND state IN ('HELD','PENDING') "
            "ORDER BY created_at ASC LIMIT 1", (intent_key,)))

    def get_orbit(self, oid) -> dict | None:
        return _row(self.db.execute("SELECT * FROM orbits WHERE orbit_id=?", (oid,)))

    def set_orbit(self, oid, *, state=..., expires_at=..., released_at=..., fence=...,
                  merging=..., merge_deadline=..., merge_started_mono=...):
        sets, args = [], []
        for col, val in (("state", state), ("expires_at", expires_at),
                         ("released_at", released_at), ("fence", fence),
                         ("merging", merging), ("merge_deadline", merge_deadline),
                         ("merge_started_mono", merge_started_mono)):
            if val is not ...:
                sets.append(f"{col}=?"); args.append(val)
        if not sets:
            return
        args.append(oid)
        self.db.execute(f"UPDATE orbits SET {','.join(sets)} WHERE orbit_id=?", args)

    def held_orbits(self) -> list[dict]:
        # 입체 검사 대상 = 일반 궤도(orbit)만. merge_token 은 경로궤도가 아니므로 제외.
        return _rows(self.db.execute(
            "SELECT * FROM orbits WHERE state='HELD' AND kind='orbit'"))

    def pending_orbits(self) -> list[dict]:
        # 우선순위 DESC → 같으면 FIFO(created_at ASC). 기아 방지 기본. merge_token 제외.
        return _rows(self.db.execute(
            "SELECT * FROM orbits WHERE state='PENDING' AND kind='orbit' "
            "ORDER BY priority DESC, created_at ASC"))

    def orbits_for_task(self, task_id) -> list[dict]:
        return _rows(self.db.execute(
            "SELECT * FROM orbits WHERE task_id=? AND kind='orbit'", (task_id,)))

    def due_orbits(self, now) -> list[dict]:
        # merging=1(connect Phase B pin) 궤도는 만료 sweep에서 skip(§E). merge_token 도 제외.
        return _rows(self.db.execute(
            "SELECT * FROM orbits WHERE state='HELD' AND kind='orbit' AND merging=0 "
            "AND expires_at IS NOT NULL AND expires_at<=?", (now,)))

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
                 connect_fence=..., connect_intent_at=..., branch_tip_sha=...,
                 merge_sha=..., merged_at=...):
        sets, args = [], []
        for col, val in (("state", state), ("agent_id", agent_id),
                         ("worktree", worktree), ("branch", branch),
                         ("connect_fence", connect_fence),
                         ("connect_intent_at", connect_intent_at),
                         ("branch_tip_sha", branch_tip_sha),
                         ("merge_sha", merge_sha), ("merged_at", merged_at)):
            if val is not ...:
                sets.append(f"{col}=?"); args.append(val)
        if not sets:
            return
        args.append(task_id)
        self.db.execute(f"UPDATE tasks SET {','.join(sets)} WHERE task_id=?", args)

    def all_tasks(self) -> list[dict]:
        """모든 task — 의존 DAG 사이클 검사(P0-10)용 전역 그래프 빌드."""
        return _rows(self.db.execute("SELECT * FROM tasks"))

    def set_task_deps(self, task_id, deps):
        """task의 deps(JSON 배열)를 교체 — depend() 가 사이클-안전 검증 후 호출(P0-10)."""
        self.db.execute("UPDATE tasks SET deps=? WHERE task_id=?",
                        (json.dumps(deps), task_id))

    def tasks_by_state(self, states) -> list[dict]:
        q = ",".join("?" * len(states))
        return _rows(self.db.execute(
            f"SELECT * FROM tasks WHERE state IN ({q}) ORDER BY priority DESC, created_at",
            list(states)))

    # --- flags (D3: LATCH 단조사실 + EPHEMERAL 소유신호) ---
    def set_flag(self, key, value, set_by=None):
        """단순 LATCH set(하위호환 경로). 증분6 의 전체 메타(type/rank/epoch/status)는
        get_flag_row/upsert_flag 가 다룬다. 기존 호출부(connect 의 'merged' latch 등)는
        이 경로로도 동작 — flag_type 디폴트 LATCH, epoch 는 보존(set 마다 안 올림)."""
        self.db.execute(
            "INSERT INTO flags(key,value,set_by,set_at) VALUES(?,?,?,?) "
            "ON CONFLICT(key) DO UPDATE SET value=?,set_by=?,set_at=?",
            (key, value, set_by, time.time(), value, set_by, time.time()))

    def get_flag(self, key) -> str | None:
        r = _row(self.db.execute("SELECT value FROM flags WHERE key=?", (key,)))
        return r["value"] if r else None

    def get_flag_row(self, key) -> dict | None:
        """증분6: 플래그 전체 메타(type/rank/epoch/status/owner/lease) — D3 CAS·만족판정용."""
        return _row(self.db.execute("SELECT * FROM flags WHERE key=?", (key,)))

    def upsert_flag(self, key, *, value, set_by=None, flag_type="LATCH", rank=0,
                    status="LIVE", owner_agent=None, lease_id=None, epoch):
        """증분6: 플래그 전체 메타를 set(epoch 명시 — set/clear/break 마다 +1 해서 넘김).
        EPHEMERAL/LATCH 분기·단조검사·소유검사는 core 가 미리 하고 여기선 영속만 한다."""
        self.db.execute(
            "INSERT INTO flags(key,value,set_by,set_at,flag_type,epoch,rank,status,"
            "owner_agent,lease_id) VALUES(?,?,?,?,?,?,?,?,?,?) "
            "ON CONFLICT(key) DO UPDATE SET value=?,set_by=?,set_at=?,flag_type=?,"
            "epoch=?,rank=?,status=?,owner_agent=?,lease_id=?",
            (key, value, set_by, time.time(), flag_type, epoch, rank, status,
             owner_agent, lease_id,
             value, set_by, time.time(), flag_type, epoch, rank, status,
             owner_agent, lease_id))

    def set_flag_status(self, key, *, status, epoch, value=..., owner_agent=...,
                        lease_id=...):
        """증분6: 플래그 상태 전이(LIVE→CLEARED/BROKEN) + epoch +1. 보유자 사망 시 reclaim 이
        EPHEMERAL 플래그를 BROKEN 으로(대기자 PRODUCER_DEAD 기상)."""
        sets, args = ["status=?", "epoch=?"], [status, epoch]
        for col, val in (("value", value), ("owner_agent", owner_agent),
                         ("lease_id", lease_id)):
            if val is not ...:
                sets.append(f"{col}=?"); args.append(val)
        args.append(key)
        self.db.execute(f"UPDATE flags SET {','.join(sets)} WHERE key=?", args)

    def ephemeral_flags_for_lease(self, lease_id) -> list[dict]:
        """증분6: 주어진 lease(orbits.kind='flag_ephemeral')에 묶인 EPHEMERAL 플래그들 —
        reclaim 이 그 lease 를 거두며 플래그를 BROKEN 으로 만든다(자동 clear)."""
        return _rows(self.db.execute(
            "SELECT * FROM flags WHERE flag_type='EPHEMERAL' AND lease_id=? "
            "AND status='LIVE'", (lease_id,)))

    # --- flag_waiters (D3: register→poll, 비블로킹 wait) ---
    def add_flag_waiter(self, agent_id, key, want_value, observed_epoch, deadline) -> str:
        wid = "fw-" + uuid.uuid4().hex[:12]
        self.db.execute(
            "INSERT INTO flag_waiters(waiter_id,agent_id,key,want_value,observed_epoch,"
            "deadline,state,created_at) VALUES(?,?,?,?,?,?, 'WAITING', ?)",
            (wid, agent_id, key, want_value, observed_epoch, deadline, time.time()))
        return wid

    def get_flag_waiter(self, waiter_id) -> dict | None:
        return _row(self.db.execute(
            "SELECT * FROM flag_waiters WHERE waiter_id=?", (waiter_id,)))

    def set_flag_waiter(self, waiter_id, *, state, wake_reason=...):
        sets, args = ["state=?"], [state]
        if wake_reason is not ...:
            sets.append("wake_reason=?"); args.append(wake_reason)
        args.append(waiter_id)
        self.db.execute(
            f"UPDATE flag_waiters SET {','.join(sets)} WHERE waiter_id=?", args)

    def waiters_for_key(self, key, state="WAITING") -> list[dict]:
        return _rows(self.db.execute(
            "SELECT * FROM flag_waiters WHERE key=? AND state=?", (key, state)))

    # --- idempotency (D9: at-least-once MCP exactly-once 효과) ---
    def get_idem(self, request_id) -> dict | None:
        return _row(self.db.execute(
            "SELECT * FROM idempotency WHERE request_id=?", (request_id,)))

    def begin_idem(self, request_id, agent_id, verb, arg_hash):
        """request_id를 INFLIGHT로 등록. 이미 있으면 무시(OR IGNORE) — 호출부가 get_idem으로 분기."""
        self.db.execute(
            "INSERT OR IGNORE INTO idempotency(request_id,agent_id,verb,arg_hash,status,created_at)"
            " VALUES(?,?,?,?, 'INFLIGHT', ?)",
            (request_id, agent_id, verb, arg_hash, time.time()))

    def finish_idem(self, request_id, response):
        """성공 종단만 캐시(DONE). 비성공은 clear_idem로 지운다(재시도 가능해야 — §3.C)."""
        self.db.execute(
            "UPDATE idempotency SET status='DONE', response=?, completed_at=? WHERE request_id=?",
            (json.dumps(response), time.time(), request_id))

    def clear_idem(self, request_id):
        """비성공(DENIED/stale-fence/fenced_out) — INFLIGHT 흔적 제거 → 세상이 바뀌면 재시도 가능."""
        self.db.execute("DELETE FROM idempotency WHERE request_id=?", (request_id,))

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

    def bump_bail_epoch(self, agent_id):
        """증분5(§D6): 좀비 회수 시 단조 증가 — 회수 전 epoch를 든 GC-pause 좀비를 부활 차단."""
        self.db.execute(
            "UPDATE agents SET bail_epoch=bail_epoch+1 WHERE agent_id=?", (agent_id,))

    def stale_agents(self, cutoff) -> list[dict]:
        return _rows(self.db.execute(
            "SELECT * FROM agents WHERE state!='RETIRED' AND last_heartbeat<?", (cutoff,)))

    def orbits_held_by_agent(self, agent_id) -> list[dict]:
        return _rows(self.db.execute(
            "SELECT * FROM orbits WHERE agent_id=? AND state='HELD' AND kind='orbit'",
            (agent_id,)))

    def orbits_owned_by_agent(self, agent_id, states=("HELD", "PENDING")) -> list[dict]:
        # 경로 궤도(orbit)만 — merge_token은 reclaim에서 별도 abort 경로(merge_tokens_owned_by).
        q = ",".join("?" * len(states))
        return _rows(self.db.execute(
            f"SELECT * FROM orbits WHERE agent_id=? AND kind='orbit' AND state IN ({q})",
            (agent_id, *states)))

    # --- merge_token (repo-wide Semaphore max=1, §D11) ---
    def held_merge_token(self, resource_key) -> dict | None:
        """현재 HELD 상태인 통합 레포 merge_token(있으면). capacity 1 → 최대 한 행."""
        return _row(self.db.execute(
            "SELECT * FROM orbits WHERE kind='merge_token' AND state='HELD' "
            "AND resource_key=?", (resource_key,)))

    def all_held_merge_tokens(self) -> list[dict]:
        return _rows(self.db.execute(
            "SELECT * FROM orbits WHERE kind='merge_token' AND state='HELD'"))

    def merge_tokens_owned_by(self, agent_id, states=("HELD",)) -> list[dict]:
        q = ",".join("?" * len(states))
        return _rows(self.db.execute(
            f"SELECT * FROM orbits WHERE agent_id=? AND kind='merge_token' AND state IN ({q})",
            (agent_id, *states)))

    # --- flag_ephemeral lease (D3: EPHEMERAL 플래그를 받쳐주는 owned+TTL lease) ---
    def flag_leases_owned_by(self, agent_id, states=("HELD",)) -> list[dict]:
        """증분6(§D3): 이 agent 가 쥔 flag_ephemeral lease 들 — reclaim 이 거두며 받쳐주는
        EPHEMERAL 플래그를 BROKEN 으로 만든다(자동 clear, 대기자 PRODUCER_DEAD 기상)."""
        q = ",".join("?" * len(states))
        return _rows(self.db.execute(
            f"SELECT * FROM orbits WHERE agent_id=? AND kind='flag_ephemeral' "
            f"AND state IN ({q})", (agent_id, *states)))

    def due_flag_leases(self, now) -> list[dict]:
        """증분6(§D3): TTL 만료된 flag_ephemeral lease(보유자가 renew/heartbeat 안 함) —
        sweep 이 거둬 EPHEMERAL 플래그를 BROKEN 으로(보유자 GC-pause/사망 = lease 만료)."""
        return _rows(self.db.execute(
            "SELECT * FROM orbits WHERE state='HELD' AND kind='flag_ephemeral' "
            "AND expires_at IS NOT NULL AND expires_at<=?", (now,)))

    def pinned_orbits_for_task(self, task_id) -> list[dict]:
        return _rows(self.db.execute(
            "SELECT * FROM orbits WHERE task_id=? AND kind='orbit' AND merging=1", (task_id,)))

    def tasks_for_agent(self, agent_id) -> list[dict]:
        return _rows(self.db.execute("SELECT * FROM tasks WHERE agent_id=?", (agent_id,)))

    def snapshot(self) -> dict:
        return {
            "orbits": _rows(self.db.execute("SELECT orbit_id,task_id,mode,state,fence,expires_at FROM orbits")),
            "tasks": _rows(self.db.execute("SELECT task_id,name,state FROM tasks")),
            "flags": _rows(self.db.execute("SELECT key,value FROM flags")),
        }
