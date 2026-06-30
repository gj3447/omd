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
  intent_key TEXT,                     -- 증분5(§D9): claim 자연 멱등 — hash(agent,paths,mode,task)
  -- 증분9(§D12 read-set 코히런스): read-orbit 이 어느 통합 generation 위에서 분기했는지(read 시점
  -- integration_gen). 응결이 이 read-궤도와 겹치는 경로를 통합에 추가/변경하면(read_gen < 현 gen
  -- 이면서 겹침) consumer 는 옛 base 위에 빌드 중 → stale=1 로 표시 → connect 전 rebase/재독 강제.
  read_gen INTEGER,
  stale INTEGER NOT NULL DEFAULT 0     -- 1=read-궤도가 낡음(겹치는 응결이 일어남). connect 차단.
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
  merged_at REAL,
  -- 증분9(§D12): consumer 가 자기 read-set 을 마지막으로 통합과 동기화한 generation. claim(read)
  -- /read_refresh 시 현 integration_gen 으로 박힌다. connect 때 이 gen 이후의 merge 가 이 task 의
  -- 선언 reads 와 겹치면 = 유령 읽기(옛 base 위 빌드) → connect 거부. read-궤도 release 후에도
  -- 유지되므로(궤도 생명과 분리) read↔write 배타성을 안 깨고 코히런스를 추적한다.
  read_synced_gen INTEGER
);
-- 증분9(§D12): 응결 로그 — gen 마다 통합에 추가/변경된 write-globs. consumer connect 가
-- read_synced_gen 이후 merge 들 중 자기 reads 와 겹치는 게 있는지 본다(유령 읽기 판정).
CREATE TABLE IF NOT EXISTS merge_log (
  gen INTEGER PRIMARY KEY, task_id TEXT, globs TEXT NOT NULL, merged_at REAL
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
-- 증분7(§D4): 세마포어 레지스트리(lease 아님 — 설정). permit 자체는 orbits(kind='sem_permit',
-- resource_key=sem_id)로, 가용 = max_permits − count(ACTIVE permit)(저장 정수 아님 → 누수 0).
CREATE TABLE IF NOT EXISTS semaphores (
  sem_id TEXT PRIMARY KEY, max_permits INTEGER NOT NULL, created_at REAL
);
-- 증분7(§D4): 세마포어 대기자(register→poll, 서버 비블로킹). no-overtaking(§D7): 가용 슬롯이
-- 생겨도 자기보다 먼저 줄선(우선순위↑ 또는 enqueued_seq↓) 대기자가 있으면 양보(기아 방지).
-- 보유자/대기자 사망 시 reclaim 이 거두므로 영구 hang 없음.
CREATE TABLE IF NOT EXISTS sem_waiters (
  waiter_id TEXT PRIMARY KEY, sem_id TEXT NOT NULL, agent_id TEXT,
  ttl REAL, priority INTEGER DEFAULT 0, enqueued_seq INTEGER,
  deadline REAL, state TEXT NOT NULL, permit_id TEXT, created_at REAL
);
CREATE INDEX IF NOT EXISTS idx_sem_waiters_sem ON sem_waiters(sem_id, state);
-- 증분5(§D9): request_id 멱등 테이블. INFLIGHT(진행중)→DONE(성공종단만 캐시). DENIED/stale-fence
-- 같은 비성공은 캐시 안 함(세상이 바뀌면 재시도 가능해야 — §3.C). at-least-once MCP 재시도가
-- 두 번째 효과를 일으키지 않게 한다(claim 누수·이중 merge·이중 release 차단).
CREATE TABLE IF NOT EXISTS idempotency (
  request_id TEXT PRIMARY KEY, agent_id TEXT, verb TEXT, arg_hash TEXT,
  status TEXT NOT NULL, response TEXT, created_at REAL, completed_at REAL
);
-- 증분8(§D5): 응결 랑데부 배리어. 세대(generation) 스탬프 + BROKEN 종단. 멤버십은 agent 수가
-- 아니라 **task 집합**(reclaim 으로 task 가 requeue 되면 N 재계산). 참가자 사망(도착 전/후)·
-- 타임아웃 → break → 도착해 있던 전원이 BROKEN 으로 기상(영구 hang 0).
CREATE TABLE IF NOT EXISTS barriers (
  barrier_id TEXT PRIMARY KEY, name TEXT, kind TEXT, parties INTEGER,
  generation INTEGER NOT NULL DEFAULT 0, state TEXT NOT NULL, break_reason TEXT,
  policy TEXT NOT NULL DEFAULT 'break', deadline_at REAL, created_at REAL,
  UNIQUE(name, generation)
);
-- 각 (배리어,세대)의 참가 task. arrived=도착 여부, arrive_fence=도착 시점 write-orbit fence
-- (응결 trip 직전 재검증 기준 — ABA 차단). owner stale=참가자 사망 판정.
CREATE TABLE IF NOT EXISTS barrier_parties (
  barrier_id TEXT, generation INTEGER, task_id TEXT, agent_id TEXT,
  arrived INTEGER NOT NULL DEFAULT 0, arrive_fence INTEGER,
  PRIMARY KEY(barrier_id, generation, task_id)
);
CREATE INDEX IF NOT EXISTS idx_barrier_parties_task ON barrier_parties(task_id);
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
    # 증분9(§D12): read-set 코히런스. read-orbit 의 분기 generation + stale 플래그 +
    # task 의 read-set 동기화 gen(궤도 생명과 분리된 코히런스 추적).
    ("orbits", "read_gen", "INTEGER"),
    ("orbits", "stale", "INTEGER NOT NULL DEFAULT 0"),
    ("tasks", "read_synced_gen", "INTEGER"),
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

    def next_seq(self) -> int:
        """증분7(§D7): 단조 전역 enqueue 티켓(FIFO no-overtaking 순서용). fence 와 분리 —
        fence 는 lease 신원, seq 는 큐 도착순서. 단일문 +1(읽고-쓰기 갭 없음)."""
        self.db.execute(
            "INSERT INTO meta(key,value) VALUES('seq','0') ON CONFLICT(key) DO UPDATE "
            "SET value=CAST(value AS INTEGER)+1")
        return int(self.db.execute(
            "SELECT value FROM meta WHERE key='seq'").fetchone()["value"])

    # --- meta (일반 KV: 증분9 D12 integration_gen / D14 leader_lease) ---
    def get_meta(self, key, default=None):
        r = self.db.execute("SELECT value FROM meta WHERE key=?", (key,)).fetchone()
        return r["value"] if r else default

    def set_meta(self, key, value):
        self.db.execute(
            "INSERT INTO meta(key,value) VALUES(?,?) ON CONFLICT(key) DO UPDATE "
            "SET value=excluded.value", (key, str(value)))

    def integration_gen(self) -> int:
        """현 통합 generation(§D12). 응결 1건마다 +1. read-orbit 은 분기 시점 이 값을 박는다."""
        r = self.db.execute("SELECT value FROM meta WHERE key='integration_gen'").fetchone()
        return int(r["value"]) if r else 0

    def bump_integration_gen(self) -> int:
        """응결(merge)이 통합 브랜치를 전진시킬 때 +1(단일문, 읽고-쓰기 갭 없음). §D12."""
        self.db.execute(
            "INSERT INTO meta(key,value) VALUES('integration_gen','1') ON CONFLICT(key) "
            "DO UPDATE SET value=CAST(value AS INTEGER)+1")
        return self.integration_gen()

    def append_merge_log(self, gen, task_id, globs) -> None:
        """gen 에 응결된 통합 write-globs 를 기록(§D12). consumer connect 가 read_synced_gen
        이후 이 로그를 훑어 자기 reads 와 겹치는 응결을 찾는다(궤도 생명과 분리된 코히런스)."""
        self.db.execute(
            "INSERT INTO merge_log(gen,task_id,globs,merged_at) VALUES(?,?,?,?) "
            "ON CONFLICT(gen) DO UPDATE SET task_id=excluded.task_id,globs=excluded.globs",
            (gen, task_id, json.dumps(globs), time.time()))

    def merges_since(self, gen) -> list[dict]:
        """gen *초과*(>gen) 의 모든 응결 로그(오름차순). consumer 가 자기 read_synced_gen 이후
        통합에 무엇이 들어왔는지 본다."""
        return _rows(self.db.execute(
            "SELECT * FROM merge_log WHERE gen>? ORDER BY gen ASC", (gen,)))

    # --- D14 leader-lease (코디네이터 singleton/HA 입장) ---
    def get_leader(self) -> dict | None:
        """현 리더 lease(JSON: coordinator_id/epoch/last_heartbeat/started_at) 또는 None."""
        raw = self.get_meta("leader_lease")
        if raw is None:
            return None
        try:
            return json.loads(raw)
        except (TypeError, ValueError):
            return None

    def cas_leader(self, expect_epoch, new_lease) -> bool:
        """leader_lease 를 CAS 로 교체(현 epoch == expect_epoch 일 때만). _cs(BEGIN IMMEDIATE)
        안에서 호출되므로 단일 writer 직렬화 + 멀티프로세스간 row-lock 양쪽으로 원자.
        expect_epoch=None 은 '리더 부재(또는 행 없음)' 를 기대. 반환=교체 성공 여부."""
        cur = self.get_leader()
        cur_epoch = cur["epoch"] if cur else None
        if cur_epoch != expect_epoch:
            return False
        self.set_meta("leader_lease", json.dumps(new_lease))
        return True

    def write_leader(self, lease) -> None:
        """리더 lease 무조건 기록(heartbeat 갱신 등 — 이미 소유 검증된 경로에서만)."""
        self.set_meta("leader_lease", json.dumps(lease))

    def live_read_orbits(self) -> list[dict]:
        """살아있는(HELD) read-궤도 — §D12 stale 표시 대상. merge_token/permit 등 제외(kind='orbit')."""
        return _rows(self.db.execute(
            "SELECT * FROM orbits WHERE state='HELD' AND kind='orbit' AND mode='read'"))

    def stale_read_orbits_for_task(self, task_id) -> list[dict]:
        """task 의 HELD read-궤도 중 stale 로 표시된 것(§D12) — connect 차단 판정용."""
        return _rows(self.db.execute(
            "SELECT * FROM orbits WHERE task_id=? AND kind='orbit' AND mode='read' "
            "AND state='HELD' AND stale=1", (task_id,)))

    # --- orbits ---
    def add_orbit(self, *, task_id, agent_id, pathspec, mode, state,
                  fence=None, expires_at=None, reason="", priority=0,
                  kind="orbit", resource_key=None, intent_key=None,
                  read_gen=None) -> str:
        oid = "orb-" + uuid.uuid4().hex[:12]
        self.db.execute(
            "INSERT INTO orbits(orbit_id,task_id,agent_id,pathspec,mode,state,"
            "fence,expires_at,created_at,reason,priority,kind,resource_key,intent_key,"
            "read_gen) "
            "VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (oid, task_id, agent_id, json.dumps(pathspec), mode, state,
             fence, expires_at, time.time(), reason, priority, kind, resource_key,
             intent_key, read_gen))
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
                  merging=..., merge_deadline=..., merge_started_mono=...,
                  read_gen=..., stale=...):
        sets, args = [], []
        for col, val in (("state", state), ("expires_at", expires_at),
                         ("released_at", released_at), ("fence", fence),
                         ("merging", merging), ("merge_deadline", merge_deadline),
                         ("merge_started_mono", merge_started_mono),
                         ("read_gen", read_gen), ("stale", stale)):
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
                 merge_sha=..., merged_at=..., read_synced_gen=...):
        sets, args = [], []
        for col, val in (("state", state), ("agent_id", agent_id),
                         ("worktree", worktree), ("branch", branch),
                         ("connect_fence", connect_fence),
                         ("connect_intent_at", connect_intent_at),
                         ("branch_tip_sha", branch_tip_sha),
                         ("merge_sha", merge_sha), ("merged_at", merged_at),
                         ("read_synced_gen", read_synced_gen)):
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

    def gc_idem(self, cutoff: float) -> int:
        """§D9 멱등 캐시 GC: cutoff 이전에 완료된 DONE 행 삭제(무한누적 차단).
        INFLIGHT(진행중)은 completed_at IS NULL 이라 제외(진행중 멱등 윈도우 보존).
        status='DONE' 명시로 의도 고정. 반환=삭제 행 수."""
        cur = self.db.execute(
            "DELETE FROM idempotency WHERE status='DONE' AND completed_at IS NOT NULL"
            " AND completed_at < ?", (cutoff,))
        return cur.rowcount

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

    # --- semaphores (D4: permit=lease, 가용 = max − count(ACTIVE)) ---
    def add_semaphore(self, sem_id, max_permits):
        """세마포어 레지스트리 등록(멱등). max_permits 변경은 ON CONFLICT 으로 갱신."""
        self.db.execute(
            "INSERT INTO semaphores(sem_id,max_permits,created_at) VALUES(?,?,?) "
            "ON CONFLICT(sem_id) DO UPDATE SET max_permits=excluded.max_permits",
            (sem_id, max_permits, time.time()))

    def get_semaphore(self, sem_id) -> dict | None:
        return _row(self.db.execute(
            "SELECT * FROM semaphores WHERE sem_id=?", (sem_id,)))

    def count_active_permits(self, sem_id) -> int:
        """증분7(§D4): 가용 계산의 핵심 — 활성(HELD) permit 수. 저장 정수가 아니라 lease
        count 라서, 보유자가 죽어 permit 이 EXPIRED/RELEASED 되면 자동으로 가용이 복구된다(누수 0)."""
        r = self.db.execute(
            "SELECT COUNT(*) AS n FROM orbits WHERE kind='sem_permit' AND state='HELD' "
            "AND resource_key=?", (sem_id,)).fetchone()
        return int(r["n"])

    def active_permit_for(self, sem_id, agent_id) -> dict | None:
        """이 agent 가 이 세마포어에 이미 쥔 ACTIVE permit(있으면) — 멱등 reuse 용(재발급 안 함)."""
        return _row(self.db.execute(
            "SELECT * FROM orbits WHERE kind='sem_permit' AND state='HELD' "
            "AND resource_key=? AND agent_id=? LIMIT 1", (sem_id, agent_id)))

    def active_permits(self, sem_id) -> list[dict]:
        return _rows(self.db.execute(
            "SELECT * FROM orbits WHERE kind='sem_permit' AND state='HELD' "
            "AND resource_key=?", (sem_id,)))

    def sem_permits_owned_by(self, agent_id, states=("HELD",)) -> list[dict]:
        """증분7(§D4): 이 agent 가 쥔 sem_permit lease 들 — reclaim 이 거두며 슬롯 복구(누수 0)."""
        q = ",".join("?" * len(states))
        return _rows(self.db.execute(
            f"SELECT * FROM orbits WHERE agent_id=? AND kind='sem_permit' "
            f"AND state IN ({q})", (agent_id, *states)))

    def due_sem_permits(self, now) -> list[dict]:
        """증분7(§D4): TTL 만료된 sem_permit(보유자가 renew/heartbeat 안 함=GC-pause/사망) —
        sweep 이 거둬 EXPIRED → 슬롯 복구."""
        return _rows(self.db.execute(
            "SELECT * FROM orbits WHERE state='HELD' AND kind='sem_permit' "
            "AND expires_at IS NOT NULL AND expires_at<=?", (now,)))

    # --- sem_waiters (D4: register→poll, no-overtaking §D7) ---
    def add_sem_waiter(self, sem_id, agent_id, ttl, priority, enqueued_seq, deadline) -> str:
        wid = "sw-" + uuid.uuid4().hex[:12]
        self.db.execute(
            "INSERT INTO sem_waiters(waiter_id,sem_id,agent_id,ttl,priority,enqueued_seq,"
            "deadline,state,created_at) VALUES(?,?,?,?,?,?,?, 'WAITING', ?)",
            (wid, sem_id, agent_id, ttl, priority, enqueued_seq, deadline, time.time()))
        return wid

    def get_sem_waiter(self, waiter_id) -> dict | None:
        return _row(self.db.execute(
            "SELECT * FROM sem_waiters WHERE waiter_id=?", (waiter_id,)))

    def set_sem_waiter(self, waiter_id, *, state, permit_id=...):
        sets, args = ["state=?"], [state]
        if permit_id is not ...:
            sets.append("permit_id=?"); args.append(permit_id)
        args.append(waiter_id)
        self.db.execute(
            f"UPDATE sem_waiters SET {','.join(sets)} WHERE waiter_id=?", args)

    def waiting_sem_waiters(self, sem_id) -> list[dict]:
        """우선순위 DESC → FIFO(enqueued_seq ASC). head = 다음에 부여받을 자(no-overtaking)."""
        return _rows(self.db.execute(
            "SELECT * FROM sem_waiters WHERE sem_id=? AND state='WAITING' "
            "ORDER BY priority DESC, enqueued_seq ASC", (sem_id,)))

    def sem_waiters_for_agent(self, agent_id, sem_id) -> list[dict]:
        return _rows(self.db.execute(
            "SELECT * FROM sem_waiters WHERE agent_id=? AND sem_id=? AND state='WAITING'",
            (agent_id, sem_id)))

    # --- barriers (D5: 세대-스탬프 응결 랑데부, 멤버십=task 집합) ---
    def add_barrier(self, *, barrier_id, name, kind, parties, generation, state,
                    policy, deadline_at) -> None:
        self.db.execute(
            "INSERT INTO barriers(barrier_id,name,kind,parties,generation,state,policy,"
            "deadline_at,created_at) VALUES(?,?,?,?,?,?,?,?,?)",
            (barrier_id, name, kind, parties, generation, state, policy, deadline_at,
             time.time()))

    def get_barrier(self, barrier_id) -> dict | None:
        return _row(self.db.execute(
            "SELECT * FROM barriers WHERE barrier_id=?", (barrier_id,)))

    def barrier_by_name(self, name) -> dict | None:
        """이름으로 최신 세대 배리어 — arrive/abort 가 이름으로 찾는다. 한 이름은 세대마다
        한 행(UNIQUE(name,generation)); 최신 세대가 활성 인스턴스."""
        return _row(self.db.execute(
            "SELECT * FROM barriers WHERE name=? ORDER BY generation DESC LIMIT 1", (name,)))

    def set_barrier(self, barrier_id, *, state=..., break_reason=..., parties=...,
                    deadline_at=...):
        sets, args = [], []
        for col, val in (("state", state), ("break_reason", break_reason),
                         ("parties", parties), ("deadline_at", deadline_at)):
            if val is not ...:
                sets.append(f"{col}=?"); args.append(val)
        if not sets:
            return
        args.append(barrier_id)
        self.db.execute(f"UPDATE barriers SET {','.join(sets)} WHERE barrier_id=?", args)

    def add_barrier_party(self, barrier_id, generation, task_id, agent_id=None):
        self.db.execute(
            "INSERT OR IGNORE INTO barrier_parties(barrier_id,generation,task_id,agent_id,"
            "arrived,arrive_fence) VALUES(?,?,?,?,0,NULL)",
            (barrier_id, generation, task_id, agent_id))

    def barrier_parties(self, barrier_id, generation) -> list[dict]:
        return _rows(self.db.execute(
            "SELECT * FROM barrier_parties WHERE barrier_id=? AND generation=? "
            "ORDER BY task_id", (barrier_id, generation)))

    def get_barrier_party(self, barrier_id, generation, task_id) -> dict | None:
        return _row(self.db.execute(
            "SELECT * FROM barrier_parties WHERE barrier_id=? AND generation=? AND task_id=?",
            (barrier_id, generation, task_id)))

    def set_barrier_party(self, barrier_id, generation, task_id, *, arrived=...,
                          arrive_fence=..., agent_id=...):
        sets, args = [], []
        for col, val in (("arrived", arrived), ("arrive_fence", arrive_fence),
                         ("agent_id", agent_id)):
            if val is not ...:
                sets.append(f"{col}=?"); args.append(val)
        if not sets:
            return
        args.extend([barrier_id, generation, task_id])
        self.db.execute(
            f"UPDATE barrier_parties SET {','.join(sets)} WHERE barrier_id=? "
            f"AND generation=? AND task_id=?", args)

    def del_barrier_party(self, barrier_id, generation, task_id):
        self.db.execute(
            "DELETE FROM barrier_parties WHERE barrier_id=? AND generation=? AND task_id=?",
            (barrier_id, generation, task_id))

    def barriers_with_task(self, task_id, generation_match=True) -> list[dict]:
        """이 task 를 (현재 세대) 멤버로 가진 비종단 배리어들 — reclaim 이 task 를 requeue 할 때
        영향받는 배리어를 찾는다(멤버십=task 집합, N 재계산/break/shrink)."""
        rows = _rows(self.db.execute(
            "SELECT DISTINCT b.* FROM barriers b JOIN barrier_parties p "
            "ON b.barrier_id=p.barrier_id AND b.generation=p.generation "
            "WHERE p.task_id=? AND b.state IN ('ARMED','TRIPPING')", (task_id,)))
        return rows

    def all_barriers(self, states=None) -> list[dict]:
        if states:
            q = ",".join("?" * len(states))
            return _rows(self.db.execute(
                f"SELECT * FROM barriers WHERE state IN ({q})", list(states)))
        return _rows(self.db.execute("SELECT * FROM barriers"))

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
