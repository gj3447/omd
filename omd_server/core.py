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
import uuid
from contextlib import contextmanager

from . import fsm
from .disjoint import path_in_globs, sets_overlap
from .events import NOOP
from .gitio import GitError, GitNothingToCommit, GitRepo, GitTimeout
from .store import Store

# Phase B(락밖 merge) 서브프로세스 타임아웃(§E — 무한 hang 방지). pin은 이보다 길게 잡아
# 타임아웃→abort→rollback이 완료될 시간을 준다.
MERGE_TIMEOUT_S = 120.0
MERGE_PIN_GRACE_S = 60.0

# P2 shared 레인: write-동급 궤도 mode. "shared" = hot 공유파일 전용 — 같은 경로에 shared↔shared
# 동시 HELD 를 허용하고(직렬화 마찰 제거) 응결은 git 3-way 에 맡긴다. 진짜 충돌(같은 hunk)은
# connect 에서 shared_conflict(정상사건·retryable)로 표면화. write-set 감사/fence/해제 경로에선
# write 와 동급으로 취급돼 disjoint(write) 궤도의 배타 의미론은 불변.
WRITE_MODES = ("write", "shared")

# D3 단조 LATCH 랭크(§D3): done(1) < merged(2). 하향 set 은 거부, 동값 재발행은 멱등 no-op.
# 0 = 랭크 없는 일반 LATCH(임의 값, 단조검사 안 함). 의존 해제는 =merged 에 건다(§3.H).
LATCH_RANK = {"done": 1, "merged": 2}

# D14 리더-lease(코디네이터 singleton). 기동 시 리더 lease 를 획득(또는 살아있는 리더 감지 시
# 거부). last_heartbeat 가 이 TTL 을 넘으면 죽은 리더로 보고 takeover 가능(fence=epoch +1 로
# 옛 리더의 잔여 변이는 stale leader_epoch 로 차단). 권장: leader heartbeat 주기 = TTL/3.
LEADER_TTL_S = 30.0


class CoordinatorConflict(RuntimeError):
    """D14: 같은 DB 에 살아있는 다른 코디네이터(리더 lease 보유)가 있어 기동을 거부한다.
    in-process actor 직렬화는 프로세스당이라, 한 DB 에 코디네이터 둘 = writer 둘 = SINGULON
    무효. 단일 인스턴스 전용을 *명시적으로 강제*(§D14)."""


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
                 merge_timeout: float | None = None, *,
                 coordinator_id: str | None = None, leader_ttl: float = LEADER_TTL_S,
                 allow_memory_db: bool = False,
                 enforce_single_coordinator: bool = True,
                 auto_push: str | None = None,
                 idem_ttl: float | None = 3600.0,
                 strict_writeset: bool = False):
        # §D14: `:memory:` 디폴트 금지 — 재기동마다 모든 fence/leader_epoch 가 0 으로 리셋되어
        # 낡은 토큰/잔여 merge 와 충돌(고스트 writer). 영속 DB 필수. 단위테스트는 allow_memory_db=
        # True 로 명시 opt-in(프로세스 1개, 재기동 없음 — fence 리셋 위험 없음).
        if db_path == ":memory:" and not allow_memory_db:
            raise ValueError(
                "OMD requires a persistent DB path (got ':memory:'). An in-memory DB "
                "resets fence/leader epoch to 0 on every restart, colliding with stale "
                "tokens. Pass a file path, or allow_memory_db=True for single-process tests.")
        self.store = Store(db_path)
        self.coordinator_id = coordinator_id or f"coord-{uuid.uuid4().hex[:12]}"
        self.leader_ttl = leader_ttl
        self.enforce_single_coordinator = enforce_single_coordinator
        self.leader_epoch = None  # 리더 lease 획득 후 채워짐(현 리더 세대)
        # heartbeat 만료 시 좀비 회수. 기본 ON(P0-7) — None=비활성. 끄면 죽은 물방울의
        # 궤도/작업이 영구 고아가 된다(사용자 핵심 우려). 권장 90s, renew는 TTL/3 주기.
        self.agent_ttl = agent_ttl
        self.events = events or NOOP
        self._lock = threading.RLock()  # 프로세스내 단일 writer(actor 대용) — D1
        # §D14: 리더 lease 획득 — 살아있는 다른 코디네이터가 있으면 CoordinatorConflict 거부.
        # 이 호출 *전*엔 어떤 변이도(특히 _recover 의 git↔DB 조정) 하면 안 된다(writer 둘 방지).
        # _lock/events/store 가 필요하므로 그것들 뒤에 둔다.
        if self.enforce_single_coordinator:
            self._acquire_leadership()
        self.merge_timeout = merge_timeout if merge_timeout is not None else MERGE_TIMEOUT_S
        self.git = GitRepo(repo) if repo else None
        # 연결(connect=merge) 직후 통합 브랜치를 이 remote 로 push — 로컬 누적 divergence 방지
        # (operator "커밋하면 바로 sync"의 OMD 내장판). None=off(기본·기존동작). env OMD_AUTO_PUSH 폴백.
        # push 실패는 fail-soft(merge 는 로컬 반영됨) — connect 성공 유지.
        self.auto_push = auto_push if auto_push is not None else (os.environ.get("OMD_AUTO_PUSH") or None)
        # §D9 멱등 캐시 GC TTL(초). 기본 1h — 어떤 현실적 MCP 재시도 윈도우보다 길어 replay 안전.
        # None=GC 안 함(기존동작). _sweep_inline 이 idem_ttl 지난 DONE 행 정리(무한누적 차단).
        self.idem_ttl = idem_ttl
        # P5 strict-writeset: True 면 commit-time 에 write-set 위반 즉시 거부+soft-reset(빠른 fail-loud).
        # 기본 off(connect-time enforce 유지=하위호환). env OMD_STRICT_WRITESET 폴백(정확 truthy 파싱).
        self.strict_writeset = bool(strict_writeset) or (
            (os.environ.get("OMD_STRICT_WRITESET") or "").strip().lower() in ("1", "true", "yes", "on"))
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
    def _cs(self, *, leader_guard=True):
        """단일 writer 직렬화(RLock) + 원자 트랜잭션(BEGIN IMMEDIATE). 재진입 안전.
        §D14: 트랜잭션을 연 직후 leader-fence 검사 — 다른 코디네이터가 takeover 했으면(우리가
        좀비 리더) CoordinatorConflict 로 거부해 writer 둘이 한 DB 를 변이하는 것을 막는다.
        leader_guard=False 는 리더십 *획득 중*(아직 epoch 미설정)에만 쓴다."""
        with self._lock:
            with self.store.tx():
                if (self.enforce_single_coordinator and leader_guard
                        and self.leader_epoch is not None):
                    self._assert_leader()
                yield

    def _emit(self, event, cid, **attrs):
        self.events.emit(event, cid, **attrs)

    # ---- D14 코디네이터 singleton / HA 입장 (§D14) ----
    def _acquire_leadership(self):
        """기동 시 리더 lease 획득. 살아있는 다른 코디네이터(heartbeat 가 TTL 안)가 있으면
        CoordinatorConflict 로 거부 — 한 DB 에 코디네이터 둘(=writer 둘)을 막는다.
        죽은(=heartbeat 가 TTL 초과한) 리더는 takeover(epoch +1 로 fence — 옛 리더가 GC-pause
        뒤 깨어나 변이하려 해도 stale leader_epoch 로 차단). CAS 는 _cs(BEGIN IMMEDIATE) 안에서
        돌아 동시 기동 둘 중 하나만 성공한다(멀티프로세스 row-lock + 단일 writer)."""
        with self._cs():
            now = time.time()
            cur = self.store.get_leader()
            if cur is not None:
                # liveness 는 **incumbent 가 선언한 TTL** 로 판정(레코드에 저장된 ttl) — 신규자
                # 자기 TTL 로 보면 안 됨. 레코드에 ttl 없으면(구버전) 신규자 TTL 로 폴백.
                inc_ttl = cur.get("ttl", self.leader_ttl)
                alive = (now - cur.get("last_heartbeat", 0)) <= inc_ttl
                if alive and cur.get("coordinator_id") != self.coordinator_id:
                    # 살아있는 다른 리더 — 거부(단일 인스턴스 강제).
                    self._emit("leader_conflict", self.coordinator_id,
                               incumbent=cur.get("coordinator_id"),
                               last_heartbeat=cur.get("last_heartbeat"))
                    raise CoordinatorConflict(
                        f"another live coordinator holds the leader lease "
                        f"(incumbent={cur.get('coordinator_id')}, "
                        f"epoch={cur.get('epoch')}); refusing to start a second "
                        f"coordinator on the same DB (§D14 single-instance).")
            prev_epoch = cur["epoch"] if cur else None
            new_epoch = (cur["epoch"] + 1) if cur else 1
            lease = {"coordinator_id": self.coordinator_id, "epoch": new_epoch,
                     "started_at": now, "last_heartbeat": now, "ttl": self.leader_ttl}
            if not self.store.cas_leader(prev_epoch, lease):
                # 다른 코디네이터가 우리 검사~CAS 사이에 끼어들어 lease 를 가져감 — 거부.
                raise CoordinatorConflict(
                    "leader lease was taken concurrently during startup (§D14).")
            self.leader_epoch = new_epoch
            self._emit("leader_acquired", self.coordinator_id, epoch=new_epoch,
                       took_over_from=(cur.get("coordinator_id") if cur else None),
                       took_over=(cur is not None))

    def _assert_leader(self):
        """현 프로세스가 여전히 리더인지 확인(다른 코디네이터가 takeover 했으면 fence-out).
        리더 lease 가 우리 epoch/id 가 아니면 우리는 좀비 리더 — 어떤 변이도 하면 안 된다.
        _cs() 안에서 변이 직전 호출(write-fence)."""
        cur = self.store.get_leader()
        if (cur is None or cur.get("coordinator_id") != self.coordinator_id
                or cur.get("epoch") != self.leader_epoch):
            self._emit("leader_fenced_out", self.coordinator_id,
                       my_epoch=self.leader_epoch,
                       current=(cur.get("epoch") if cur else None))
            raise CoordinatorConflict(
                f"coordinator {self.coordinator_id} (epoch={self.leader_epoch}) is no "
                f"longer leader — another coordinator took over. Refusing to mutate.")

    def coordinator_heartbeat(self) -> dict:
        """리더 lease keepalive(권장 주기 = leader_ttl/3). 먼저 우리가 여전히 리더인지 확인
        (takeover 됐으면 거부) → last_heartbeat 갱신. 이걸 멈추면(프로세스 사망/hang) TTL 후
        다른 코디네이터가 takeover 할 수 있다(영구 점유 불가)."""
        with self._cs():
            self._assert_leader()
            cur = self.store.get_leader()
            cur["last_heartbeat"] = time.time()
            self.store.write_leader(cur)
            return {"ok": True, "coordinator_id": self.coordinator_id,
                    "epoch": self.leader_epoch}

    def resign(self) -> dict:
        """자발적 리더십 반납(graceful shutdown). lease 를 비워(epoch 유지) 다음 코디네이터가
        TTL 대기 없이 즉시 takeover. 우리가 리더가 아니면 no-op."""
        with self._cs():
            cur = self.store.get_leader()
            if cur is None or cur.get("coordinator_id") != self.coordinator_id:
                return {"ok": True, "noop": True}
            # last_heartbeat=0 으로 만들어 즉시 만료 처리(epoch 는 보존 → 다음 리더가 +1).
            cur["last_heartbeat"] = 0
            self.store.write_leader(cur)
            self.leader_epoch = None
            self._emit("leader_resigned", self.coordinator_id)
            return {"ok": True, "coordinator_id": self.coordinator_id}

    # ---- 내부 (모두 임계구역 안에서 호출됨) ----
    def _conflicts(self, pathspec, mode) -> list[str]:
        """pathspec/mode가 충돌하는 활성 HELD 궤도 id들. read↔read 공존; shared↔shared 공존
        (P2 hot 공유파일 레인 — 응결은 3-way, 배타 write/read 와 겹치면 여전히 충돌)."""
        out = []
        for o in self.store.held_orbits():
            if mode == "read" and o["mode"] == "read":
                continue
            if mode == "shared" and o["mode"] == "shared":
                continue
            if sets_overlap(pathspec, json.loads(o["pathspec"])):
                out.append(o["orbit_id"])
        return out

    def _promote_pending(self):
        for o in self.store.pending_orbits():  # 우선순위 DESC → FIFO
            if not self._conflicts(json.loads(o["pathspec"]), o["mode"]):
                fence = self.store.next_fence()
                # §D12: PENDING read-궤도가 뒤늦게 grant 될 때도 현 통합 gen 을 박는다.
                rg = self.store.integration_gen() if o["mode"] == "read" else ...
                self.store.set_orbit(
                    o["orbit_id"],
                    state=fsm.advance("orbit", "PENDING", "grant"),
                    expires_at=time.time() + 600, fence=fence, read_gen=rg)
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
        # D3(§1.2): TTL 만료된 flag_ephemeral lease — 보유자가 renew 안 함(GC-pause/사망) →
        # 받쳐주던 EPHEMERAL 플래그를 BROKEN(자동 clear) + 대기자 PRODUCER_DEAD 기상. 영구 hang 0.
        for fl in self.store.due_flag_leases(now):
            self._break_ephemeral_flags_for_lease(fl["orbit_id"], reason="producer_dead")
            self.store.set_orbit(fl["orbit_id"],
                                 state=fsm.advance("orbit", "HELD", "expire"))
            self._emit("flag_lease_expired", fl["agent_id"], orbit_id=fl["orbit_id"])
        # D4(§1.2): TTL 만료된 sem_permit — 보유자가 renew/heartbeat 안 함(GC-pause/사망) →
        # permit EXPIRE → 가용 = max − count(ACTIVE) 가 구조적으로 복구(누수 0). 정수 카운터를
        # 쓰면 죽을 때마다 새서 결국 0(영구 정지)인 고전 버그를 permit=lease 로 원천 차단.
        expired_sems = set()
        for p in self.store.due_sem_permits(now):
            self.store.set_orbit(p["orbit_id"],
                                 state=fsm.advance("orbit", "HELD", "expire"),
                                 released_at=now)
            self._emit("sem_permit_expired", p["agent_id"], orbit_id=p["orbit_id"],
                       sem=p["resource_key"])
            expired_sems.add(p["resource_key"])
        for sem_id in expired_sems:
            self._promote_sem_waiters(sem_id)  # 복구된 슬롯을 줄선 순서대로(no-overtaking) 부여
        # D5(§D5/§1.2): ARMED 배리어의 사망(write-lease 만료=참가자 GC-pause/죽음)·타임아웃을
        # break/shrink 로 반영 — 누군가 sweep 하면(poll/status/arrive) 반영되므로 영구 hang 0.
        # 트립 plan(전원 도착)은 sweep 에서 실행하지 않는다(merge 는 락 밖이라 arrive 가 돌린다).
        for b in self.store.all_barriers(states=("ARMED",)):
            self._barrier_eval(b["barrier_id"])
        self._promote_pending()
        # §D9 멱등 캐시 GC: idem_ttl 지난 DONE 행 정리(무한누적 차단). INFLIGHT(진행중)은
        # completed_at NULL 로 보존. now 는 위에서 이미 정의됨(시각 일관).
        if self.idem_ttl:
            self.store.gc_idem(now - self.idem_ttl)

    def _reclaim_zombies_inline(self):
        """heartbeat 끊긴 물방울(involuntary) — 단일 회수 루틴으로 위임.
        F2: 생존창은 per-agent(liveness_ttl 선언, 미선언=agent_ttl) — 판정은 store 쿼리가 원자."""
        if not self.agent_ttl:
            return []
        out = []
        for a in self.store.stale_agents(time.time(), self.agent_ttl):
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
        # 죽은 보유자의 flag_ephemeral lease: 받쳐주던 EPHEMERAL 플래그를 BROKEN(자동 clear)
        # + lease EXPIRE + 대기자 PRODUCER_DEAD 기상(§1.2 — "작업중 플래그 영구 잔존" 해소).
        for fl in self.store.flag_leases_owned_by(agent_id, ("HELD",)):
            self._break_ephemeral_flags_for_lease(fl["orbit_id"], reason="producer_dead")
            self.store.set_orbit(fl["orbit_id"],
                                 state=fsm.advance("orbit", "HELD", "expire"),
                                 released_at=time.time())
            self._emit("flag_lease_reclaimed", agent_id, orbit_id=fl["orbit_id"])
        # 죽은 보유자의 sem_permit: EXPIRE → 가용 슬롯 복구(누수 0, §D4). 복구된 세마포어의
        # 대기자를 줄선 순서로 부여(no-overtaking, §D7) — 영구 hang/기아 없음.
        reclaimed_sems = set()
        for p in self.store.sem_permits_owned_by(agent_id, ("HELD",)):
            self.store.set_orbit(p["orbit_id"],
                                 state=fsm.advance("orbit", "HELD", "expire"),
                                 released_at=time.time())
            self._emit("sem_permit_reclaimed", agent_id, orbit_id=p["orbit_id"],
                       sem=p["resource_key"])
            reclaimed_sems.add(p["resource_key"])
        # 이 agent 가 어딘가 줄서 있었으면(아직 permit 못 받음) 그 대기 등록을 취소.
        for w in self.store.db.execute(
                "SELECT * FROM sem_waiters WHERE agent_id=? AND state='WAITING'",
                (agent_id,)).fetchall():
            self.store.set_sem_waiter(w["waiter_id"], state="CANCELLED")
        for o in self.store.orbits_owned_by_agent(agent_id, ("HELD", "PENDING")):
            trig = "expire" if o["state"] == "HELD" else "deny"  # HELD→EXPIRED, PENDING→DENIED
            # merging pin은 회수와 함께 해제(§E pin은 유계 — 보유자 사망도 한 경계).
            self.store.set_orbit(o["orbit_id"], state=fsm.advance("orbit", o["state"], trig),
                                 merging=0)
            # §D12: 회수되는 read-궤도의 stale 신호 플래그도 청산(LIVE 누수 방지). 보유자가
            # 죽었으므로 connect 차단은 어차피 부활차단(bail_epoch)이 맡는다.
            if o["mode"] == "read":
                self._clear_read_stale_signal(o["orbit_id"])
            freed.append(o["orbit_id"])
            self._emit("orbit_released", agent_id, orbit_id=o["orbit_id"],
                       reason="bail" if voluntary else "reclaim")
        # §D5/§3.D: 이 agent 의 **모든** task 를 멤버로 둔 활성 배리어를 재평가 대상에 모은다.
        # 이 agent 의 write-orbit 이 위에서 이미 해제됐으므로(lease 사망), 그 task 가 requeue
        # 되든(IN_ORBIT 등) 안 되든(이미 DONE) 배리어 입장에선 참가자 사망이다 → break/shrink.
        affected_barriers = set()
        for t in self.store.tasks_for_agent(agent_id):
            for b in self.store.barriers_with_task(t["task_id"]):
                affected_barriers.add(b["barrier_id"])
            if t["state"] in ("CLAIMED", "IN_ORBIT", "CONNECTING"):  # CONNECTING 포함(P0-9)
                s = fsm.advance("task", t["state"], "abort")
                s = fsm.advance("task", s, "requeue")  # ABORTED→PENDING
                self.store.set_task(t["task_id"], state=s, agent_id=None)
                requeued.append(t["task_id"])
                if self.git and t["worktree"]:
                    self.git.remove_worktree(t["worktree"])
                    if t["branch"]:
                        self.git.delete_branch(t["branch"])  # P0-8: 안 지우면 다음 start() 막힘
        # 죽은 참가자가 든 배리어 재평가(can_trip=False — reclaim 중엔 merge 안 함; 이 경로는
        # 사망이라 절대 fill 되지 않고 break/shrink 만 일어난다). 영구 hang 0(전원 BROKEN 기상).
        for bid in affected_barriers:
            self._barrier_eval(bid)
        self.store.set_agent_state(agent_id, "RETIRED")
        self._emit("agent_reclaimed", agent_id, voluntary=voluntary,
                   orbits=len(freed), tasks=len(requeued))
        for sem_id in reclaimed_sems:
            self._promote_sem_waiters(sem_id)  # 복구된 슬롯을 줄선 순서로 부여(§D7)
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
        writes = [o for o in self.store.orbits_for_task(task_id)
                  if o["mode"] in WRITE_MODES]
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
            # §3.D 배리어-bound 단위복구(증분11) — task-단위 조정이 끝난 *뒤* 배리어를 단위로
            # 조정한다(위에서 CONNECTING 이 전부 MERGED/DONE 으로 수렴했으므로 여기의 멤버
            # 상태 = git 진실).
            self._barrier_recover()
            self._promote_pending()

    def _barrier_recover(self):
        """§3.D: TRIPPING 중 크래시한 배리어를 *단위*로 조정(임계구역 안, _recover 말미).
        전 멤버 MERGED = 트립이 사실상 완료 → TRIPPED 전진수정. 일부만 MERGED = 반쪽 트립 →
        BROKEN(coordinator_crash_partial_trip) fail-loud — "BROKEN 신호 없이 반쪽 MERGED" 함정
        폐쇄. MERGED 는 단조 사실이라 되돌리지 않고(§D5 deviation 1과 동일 계약), 미응결
        task 는 task-단위 복구가 이미 재시도 가능 상태로 되돌려 놓았다. ARMED/종단은 불가침."""
        for b in self.store.all_barriers(states=["TRIPPING"]):
            parts = self.store.barrier_parties(b["barrier_id"], b["generation"])
            tasks = [self.store.get_task(p["task_id"]) for p in parts]
            if parts and all(t is not None and t["state"] == "MERGED" for t in tasks):
                self.store.set_barrier(b["barrier_id"],
                                       state=fsm.advance("barrier", "TRIPPING", "trip"))
                self._emit("barrier_recovered", b["name"], barrier=b["name"],
                           generation=b["generation"], outcome="tripped")
            else:
                self._break_barrier(b, reason="coordinator_crash_partial_trip")
                self._emit("barrier_recovered", b["name"], barrier=b["name"],
                           generation=b["generation"], outcome="broken")

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
            if o["mode"] in WRITE_MODES and o["state"] == "HELD":
                self.store.set_orbit(o["orbit_id"],
                                     state=fsm.advance("orbit", "HELD", "release"),
                                     released_at=time.time(), merging=0, merge_deadline=None)

    # ---- D12 read-set 코히런스 (§D12, 유령 읽기) ----
    def _merged_write_globs(self, task_id) -> list[str]:
        """방금 응결된 task 가 통합 브랜치에 추가/변경한 경로 글로브. 권위 소스는 그 task 의
        claimed write-set(이미 P0-11 감사로 *실제* write-set == 선언 write-set 이 강제됨).
        repo 가 있으면 실제 changed_paths(구체 경로)도 합쳐 더 정밀히 — 둘 다 overlap 판정에 쓴다."""
        globs = []
        t = self.store.get_task(task_id)
        if t:
            try:
                globs.extend(json.loads(t["writes"] or "[]"))
                globs.extend(json.loads(t["shared"] or "[]"))
            except (TypeError, ValueError):
                pass
        # write-orbit pathspec(해제 직전에 부르므로 아직 잡을 수 있을 때 합집합) 도 포함.
        for o in self.store.orbits_for_task(task_id):
            if o["mode"] in WRITE_MODES:
                globs.extend(json.loads(o["pathspec"]))
        return list(dict.fromkeys(globs))  # 중복 제거(순서 보존)

    def _mark_stale_reads(self, merged_task_id, new_gen, merged_globs=None):
        """응결(merge)이 통합을 new_gen 으로 전진시켰다. 이 응결이 추가/변경한 경로와 **겹치는**
        live HELD read-궤도(자기보다 옛 gen 에서 분기) 를 stale=1 로 표시 → 그 consumer 는
        connect 전 rebase/재독 강제(§D12). 신호는 D3 EPHEMERAL 플래그/이벤트로(consumer 가 안다).
        주: read↔write 배타성 때문에 *live* read-궤도가 겹치는 일은 드물다(연속점유 시) — 주된
        코히런스 게이트는 connect 의 merge_log 검사다. 이건 그 보조(즉시 신호)다."""
        if merged_globs is None:
            merged_globs = self._merged_write_globs(merged_task_id)
        if not merged_globs:
            return []
        affected = []
        for r in self.store.live_read_orbits():
            if r["task_id"] == merged_task_id:
                continue  # 자기 자신의 read 는 무관
            if r["stale"]:
                continue  # 이미 표시됨(멱등)
            # read 가 분기한 gen 이 이번 응결 *이전*이어야 유령(이후면 이미 본 것). None=보수적 표시.
            rg = r["read_gen"]
            if rg is not None and rg >= new_gen:
                continue
            if sets_overlap(json.loads(r["pathspec"]), merged_globs):
                self.store.set_orbit(r["orbit_id"], stale=1)
                self._emit("read_stale", r["agent_id"], orbit_id=r["orbit_id"],
                           task=r["task_id"], by_task=merged_task_id, gen=new_gen)
                # D3 이벤트/플래그 신호: consumer 가 자기 read-coherence 키로 flag_wait 관측 가능.
                # epoch 는 보존·증가(이전 refresh 가 CLEARED 로 만든 뒤 재-stale 도 단조 전진) →
                # 옛 epoch 로 register 한 대기자가 깨어난다(§D3 register→poll).
                key = self._read_stale_key(r["orbit_id"])
                prev = self.store.get_flag_row(key)
                epoch = (prev["epoch"] + 1) if prev else 0
                self.store.upsert_flag(
                    key, value=str(new_gen), set_by=merged_task_id,
                    flag_type="LATCH", rank=0, status="LIVE", epoch=epoch)
                self._wake_flag_waiters(key)
                affected.append(r["orbit_id"])
        return affected

    def _ghost_reads(self, task) -> list[str]:
        """task 의 선언 reads 와 겹치는, read_synced_gen *이후*의 응결 write-globs(유령 읽기).
        read 를 한 적 없는(read_synced_gen=None) task 는 코히런스 대상 아님 → 빈 리스트.
        궤도를 release 해도 read_synced_gen 이 task 에 남으므로 read↔write 배타성을 안 깨고
        consumer connect 시점에 정확히 판정한다(§D12)."""
        if task is None:
            return []
        synced = task["read_synced_gen"]
        if synced is None:
            return []  # 이 task 는 read claim 을 한 적 없음 — 코히런스 무관
        try:
            reads = json.loads(task["reads"] or "[]")
        except (TypeError, ValueError):
            reads = []
        if not reads:
            return []
        ghost = []
        for m in self.store.merges_since(synced):
            if m["task_id"] == task["task_id"]:
                continue  # 자기 자신의 응결은 무관
            try:
                mglobs = json.loads(m["globs"])
            except (TypeError, ValueError):
                mglobs = []
            if sets_overlap(reads, mglobs):
                # 겹치는 응결의 globs 중 reads 와 실제로 교차하는 것만 보고(진단성).
                ghost.extend(g for g in mglobs if sets_overlap(reads, [g]))
        return list(dict.fromkeys(ghost))  # 중복 제거(순서 보존)

    @staticmethod
    def _read_stale_key(orbit_id) -> str:
        """consumer 가 자기 read-궤도의 stale 신호를 flag_wait 로 관측하는 D3 플래그 키(§D12)."""
        return f"read_stale:{orbit_id}"

    def _clear_read_stale_signal(self, orbit_id):
        """read-궤도가 refresh/회수되면 그 stale 신호 플래그를 CLEARED + epoch +1(대기자 기상).
        flag 가 LIVE 로 영구 잔존(누수)하지 않게 한다."""
        key = self._read_stale_key(orbit_id)
        f = self.store.get_flag_row(key)
        if f is not None and f["status"] == "LIVE":
            self.store.set_flag_status(key, status="CLEARED", epoch=f["epoch"] + 1)
            self._wake_flag_waiters(key)

    def read_refresh(self, task_id, agent_id, fence, *, request_id=None, bail_epoch=None):
        """consumer 가 rebase/재독을 마쳤다고 선언 → task 의 read-set 동기화 gen 을 현 통합 gen
        으로 재앵커(+ 살아있는 read-궤도가 있으면 stale 해제·재앵커) (§D12). **task 중심**: read 를
        읽고 release 한 뒤(read↔write 배타라 producer 가 그 영역을 쓰려면 read 가 비어야 함)에도
        코히런스가 task 에 남으므로, 이 동사가 그 task 차원 동기화를 갱신한다.
        소유+fence 가드 — connect 와 동일하게 caller 가 그 task 의 write-orbit (agent,fence)를
        쥐고 있어야(남의 task 를 못 재앵커). 물방울 계약: connect 가 read_stale 로 거부되면
        worktree 를 통합 최신으로 rebase 한 뒤 이 동사로 청산하고 다시 connect 한다."""
        with self._cs():
            with self._idem(request_id, agent_id, "read_refresh",
                            [task_id, fence]) as cache:
                if cache.hit:
                    return cache.value
                dead = self._check_alive(agent_id, bail_epoch)
                if dead:
                    return cache.set(dead)
                t = self.store.get_task(task_id)
                if t is None:
                    return cache.set({"ok": False, "reason": "no such task"})
                # 소유+fence: caller 가 이 task 의 HELD write-orbit 을 (agent,fence)로 쥐어야.
                writes = [o for o in self.store.orbits_for_task(task_id)
                          if o["mode"] in WRITE_MODES]
                live = [o for o in writes if o["state"] == "HELD"]
                if not live:
                    return cache.set({"ok": False, "reason": "no held write orbit for task",
                                      "fenced_out": True})
                if agent_id is not None and any(o["agent_id"] != agent_id for o in live):
                    return cache.set({"ok": False, "reason": "not owner",
                                      "fenced_out": True})
                if fence is not None and all(o["fence"] != fence for o in live):
                    return cache.set({"ok": False, "reason": "stale fence",
                                      "fenced_out": True})
                gen = self.store.integration_gen()
                self.store.set_task(task_id, read_synced_gen=gen)
                # 살아있는 read-궤도가 있으면(release_read=False 경로) 그것도 재앵커+stale 해제.
                for o in self.store.orbits_for_task(task_id):
                    if o["mode"] == "read" and o["state"] == "HELD":
                        self.store.set_orbit(o["orbit_id"], read_gen=gen, stale=0)
                        self._clear_read_stale_signal(o["orbit_id"])
                self._emit("read_refreshed", agent_id, task=task_id, gen=gen)
                return cache.set({"ok": True, "task_id": task_id, "read_gen": gen,
                                  "stale": False})

    # ---- 공개 API (= MCP 툴 / CLI 동사) ----
    def declare(self, task_id, *, name="", writes=None, reads=None, deps=None, priority=0,
                shared=None):
        """shared(P2 레인): hot 공유파일 glob — 배타 writes 와 달리 다른 task 의 shared 궤도와
        겹쳐도 next_task/claim 을 막지 않는다(응결은 3-way, 충돌 시 shared_conflict retryable)."""
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
                                priority=priority, shared=shared or [])
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
                # §D12: read-궤도는 분기한 통합 generation 을 박는다 — 이후 겹치는 응결이
                # 이보다 새 gen 을 만들면 stale 로 표시돼 consumer 가 옛 base 위에 빌드하는 것을 막는다.
                read_gen = self.store.integration_gen() if mode == "read" else None
                oid = self.store.add_orbit(task_id=task_id, agent_id=agent_id, pathspec=pathspec,
                                           mode=mode, state="HELD", fence=fence,
                                           expires_at=time.time() + ttl, reason=reason,
                                           priority=priority, intent_key=ikey,
                                           read_gen=read_gen)
                # §D12: read claim 은 그 task 의 read-set 동기화 gen 을 박는다(궤도 생명과 분리 —
                # 궤도를 release 한 뒤에도 consumer 의 connect 가 코히런스를 검사하도록). 여러 read
                # 를 claim 하면 가장 옛 gen(보수적)로 고정한다.
                if mode == "read" and task_id is not None:
                    t = self.store.get_task(task_id)
                    if t is not None:
                        prev = t["read_synced_gen"]
                        if prev is None or read_gen < prev:
                            self.store.set_task(task_id, read_synced_gen=read_gen)
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
                self.store.upsert_agent(agent_id)   # F2: 활동=생존신호(mutating verb 가 liveness touch)
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

    def heartbeat(self, agent_id, *, ttl=None):
        """물방울 생존 신호. §D6 표: 이미 회수(RETIRED)된 좀비에겐 `{fenced_out:true}` 회신 →
        좀비가 다음 heartbeat에서 자기 죽음을 안다(advisory). 살아있으면 현재 bail_epoch를 회신해
        물방울이 이후 변이에 실어 보내면 회수 후 부활을 서버가 거부할 수 있다(§D6).

        F2(채택마찰 2026-07-02): `ttl=` 로 *자기 페이스를 선언* — 이 agent 의 per-agent 생존창
        (liveness_ttl). 인터랙티브 세션(verb 간 침묵 수십 분)이 claim 직후 한 번 선언하면 좀비
        회수가 그 창을 존중한다. 미선언 agent 는 기본 agent_ttl(기계 물방울 crash-fast §D2 불변)."""
        with self._cs():
            ag = self.store.get_agent(agent_id)
            if ag is not None and ag["state"] == "RETIRED":
                # 회수된 좀비 — heartbeat로 부활시키지 않고 죽음을 통지(fence 복종 규율).
                return {"ok": False, "fenced_out": True, "reason": "agent reclaimed",
                        "bail_epoch": ag["bail_epoch"]}
            self.store.upsert_agent(agent_id)
            if ttl is not None:
                self.store.set_agent_liveness_ttl(agent_id, float(ttl) if ttl else None)
            # D3(§1.2 / D2 §): heartbeat 한 번이 이 agent 의 모든 hb_bound flag_ephemeral lease 를
            # 갱신 — 건강한 producer 가 renew 깜빡해 자기 신호 플래그가 BROKEN 되는 일 방지.
            renewed = 0
            for fl in self.store.flag_leases_owned_by(agent_id, ("HELD",)):
                if fl["expires_at"] is not None:
                    base = fl["expires_at"] - fl["created_at"] if fl["created_at"] else None
                    ttl = base if (base and base > 0) else (self.agent_ttl or 90.0)
                    self.store.set_orbit(fl["orbit_id"], expires_at=time.time() + ttl)
                    renewed += 1
            # D4(§1.2/G): 건강한 보유자의 sem_permit 도 heartbeat 로 연장 — renew 깜빡으로 슬롯을
            # 잃지 않게(궤도/permit 의 비대칭 만료가 빌드 슬롯 이중배정을 부르는 §G 를 함께 닫음).
            permits_renewed = 0
            for p in self.store.sem_permits_owned_by(agent_id, ("HELD",)):
                if p["expires_at"] is not None:
                    base = p["expires_at"] - p["created_at"] if p["created_at"] else None
                    ttl = base if (base and base > 0) else (self.agent_ttl or 90.0)
                    self.store.set_orbit(p["orbit_id"], expires_at=time.time() + ttl)
                    permits_renewed += 1
            ag = self.store.get_agent(agent_id)
            return {"ok": True, "bail_epoch": ag["bail_epoch"],
                    "flag_leases_renewed": renewed, "sem_permits_renewed": permits_renewed}

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
        """deps 충족 + write-set이 활성 HELD와 서로소인 작업 1개 → READY로 올려 반환.
        P2 레인: 선언 shared glob 은 shared HELD 궤도와의 겹침은 허용(공존) — 배타(write/read)
        HELD 와 겹치면 여전히 대기."""
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
                shared = json.loads(t["shared"] or "[]") if "shared" in t.keys() else []
                if any(sets_overlap(shared, spec)
                       for spec, m in held_specs if m != "shared"):
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
                if agent_id:
                    self.store.upsert_agent(agent_id)   # F2: 활동=생존신호
                bad = self._check_task_write_fence(task_id, agent_id, fence)
                if bad:
                    self._emit("commit_rejected", task_id, reason=bad["reason"])
                    return cache.set(bad)
                t = self.store.get_task(task_id)
                writes = [o for o in self.store.orbits_for_task(task_id)
                          if o["mode"] in WRITE_MODES and o["state"] == "HELD"]
                write_globs = self._claimed_write_globs(task_id, writes)
                if self.strict_writeset:
                    # P5 strict: 궤도-밖 경로를 **commit 전에** staged 에서 제외 → 위반이 history
                    # 진입 못 함(no wedge). 밖-경로는 working tree 에 보존(uncommitted) + 라우드
                    # 리포트. in-orbit 변경이 하나도 없으면 ok:False(nothing_in_orbit). git add -A
                    # 로 재staged 되어도 매 commit 마다 일관 제외 → livelock 0(기본 off=advisory).
                    self.git.stage_all(t["worktree"])
                    excluded = [p for p in self.git.staged_paths(t["worktree"])
                                if not path_in_globs(p, write_globs)]
                    if excluded:
                        self.git.unstage(t["worktree"], excluded)
                        self._emit("commit_excluded_out_of_orbit", task_id, excluded=excluded)
                    try:
                        sha = self.git.commit_staged(t["worktree"], msg)
                    except GitNothingToCommit:
                        return cache.set({"ok": False, "reason": "nothing_in_orbit",
                                          "excluded": excluded, "claimed": write_globs,
                                          "task_id": task_id})
                    self._emit("task_committed", t["agent_id"], task=task_id, sha=sha)
                    res = {"ok": True, "sha": sha}
                    if excluded:
                        res["excluded_out_of_orbit"] = excluded
                    return cache.set(res)
                # ---- advisory(기본) 경로 — 기존 동작 불변(commit 후 자문 감사, connect가 권위 거부) ----
                sha = self.git.commit_all(t["worktree"], msg)
                self._emit("task_committed", t["agent_id"], task=task_id, sha=sha)
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
                if agent_id:
                    self.store.upsert_agent(agent_id)   # F2: 활동=생존신호
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

    def cancel(self, task_id, *, reason="", request_id=None):
        """F4(채택마찰 2026-07-02): **미시작** 태스크의 종결 verb — lease-only 흐름(declare+claim,
        start 미경유)의 태스크가 PENDING 으로 영구 잔류하던 갭 봉합. FSM 의 기존 `abort` 전이
        (source="*") 재사용이라 상태기계/TLA 모델 무변경 — PENDING/READY/BLOCKED → ABORTED(종결,
        requeue 로 재개 가능). 시작된 태스크(IN_ORBIT 이후)는 거부 — 진행중 작업의 무단 증발 금지,
        finish/bail 경유. 멱등: 이미 ABORTED 면 {ok, already}. 미존재는 fail-loud(캐시 안 함)."""
        with self._cs():
            with self._idem(request_id, task_id, "cancel", [task_id]) as cache:
                if cache.hit:
                    return cache.value
                t = self.store.get_task(task_id)
                if t is None:
                    return {"ok": False, "reason": "no such task"}       # 캐시 금지 — 이후 declare 가능
                if t["state"] == "ABORTED":
                    return cache.set({"ok": True, "already": True, "state": "ABORTED"})
                if t["state"] not in ("PENDING", "READY", "BLOCKED"):
                    return {"ok": False,                                  # 캐시 금지 — finish 후 재시도 무해
                            "reason": f"cancel 은 미시작 태스크 전용(state={t['state']}) — "
                                      f"시작된 작업은 finish/bail 경유"}
                s = fsm.advance("task", t["state"], "abort")
                self.store.set_task(task_id, state=s)
                self._emit("task_cancelled", task_id, task=task_id, reason=reason)
                return cache.set({"ok": True, "state": s, "reason": reason})

    # ---- CLOUD CONNECT — split-phase A–B–C (§3.B/§D8/§D11) ----
    def connect(self, task_id, agent_id=None, fence=None, push=None, *, request_id=None,
                bail_epoch=None):
        """CLOUD CONNECT(응결=merge). **split-phase** — git merge가 락(_cs) **밖**에서 돈다:
        push: per-call remote override(없으면 self.auto_push 상속). merge 직후 통합브랜치 push.
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
        with self._cs():
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
            merge_sha, err = self._connect_phase_b(intent, push=push)
            # ----- Phase C: 락 안 — merge_sha 먼저 기록 후 해제(P0-6) -----
            res = self._connect_phase_c(task_id, token_id, intent, merge_sha, err)
            # §D9: 성공 종단만 캐시(merge conflict/timeout=retryable → 캐시 금지).
            if request_id is not None and self._is_success(res):
                with self._cs():
                    self.store.begin_idem(request_id, agent_id, "connect",
                                          self._arg_hash("connect", [task_id, fence]))
                    self.store.finish_idem(request_id, res)
            return res

    def complete_task(self, task_id, msg=None, agent_id=None, fence=None, push=None,
                      *, request_id=None, bail_epoch=None):
        """P5 — happy-path 원샷: (선택)commit → finish → connect(+push). verb 망각-스트랜드
        (finish 빼면 IN_ORBIT 기아·connect 빼면 미통합) 방지. INV: ok:True 는 **오직 최종
        task state == MERGED** 일 때뿐 — 어느 단계 거부든 {ok:False, stage:'commit'|'finish'|
        'connect', ...원본거부...}로 fail-loud 전파(거부 은폐 금지). 하위 verb 엔 request_id
        suffix(:commit/:finish/:connect)로 idem PK 분리."""
        def _rid(s):
            return f"{request_id}:{s}" if request_id else None
        committed = False
        if msg is not None:
            try:
                cr = self.commit(task_id, msg, agent_id, fence,
                                 request_id=_rid("commit"), bail_epoch=bail_epoch)
            except GitNothingToCommit:
                cr = {"ok": True, "noop": True}   # 변경 없음(구조적 판별) — commit skip
            except (GitError, GitTimeout) as e:
                return {"ok": False, "stage": "commit", "error": str(e)}   # 진짜 실패는 은폐 안 함
            if cr.get("ok") is False:
                return {**cr, "ok": False, "stage": "commit"}
            committed = not cr.get("noop")
        fr = self.finish(task_id, agent_id, fence,
                         request_id=_rid("finish"), bail_epoch=bail_epoch)
        if fr.get("ok") is False:   # finish 성공은 'ok' 키 없음(state=DONE); 거부만 ok=False
            return {**fr, "ok": False, "stage": "finish"}
        cn = self.connect(task_id, agent_id, fence, push=push,
                          request_id=_rid("connect"), bail_epoch=bail_epoch)
        st = self.store.get_task(task_id)
        final = st["state"] if st else None
        if cn.get("ok") and final == "MERGED":   # INV: ok ⟺ MERGED(store 권위 확인)
            return {**cn, "ok": True, "stage": "connect", "state": "MERGED",
                    "committed": committed}
        return {**cn, "ok": False, "stage": "connect", "state": final}

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
            writes = [o for o in self.store.orbits_for_task(task_id)
                      if o["mode"] in WRITE_MODES]
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
            # §D12 read-set 코히런스 — 유령 읽기 차단. consumer 가 자기 read-set 을 동기화한
            # gen(read_synced_gen) *이후* 통합에 들어온 응결 중 자기 선언 reads 와 겹치는 게
            # 있으면 = 옛 base 위에 *조용히* 빌드(머지는 성공하되 로직이 틀림) → connect 거부.
            # 토큰 잡기 **전**에 검사(거부 시 토큰 쥐었다 반납하는 낭비/경합 회피).
            # 물방울 계약: rebase/재독 → read_refresh() 로 청산 후 재시도.
            ghost = self._ghost_reads(t)
            stale_orbits = [o["orbit_id"] for o in
                            self.store.stale_read_orbits_for_task(task_id)]
            if ghost or stale_orbits:
                self._emit("connect_rejected", task_id, reason="read_stale",
                           ghost_globs=ghost, stale_reads=stale_orbits)
                return {"ok": False, "reason": "read_stale", "task_id": task_id,
                        "ghost_globs": ghost, "stale_reads": stale_orbits,
                        "hint": "rebase onto integration tip, then read_refresh() your "
                                "read orbit(s) before retrying connect"}
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

    def _connect_phase_b(self, intent, push=None):
        """Phase B(**락 밖** — live tx 없음): 전용 통합 worktree에서 merge --no-ff(타임아웃, §E).
        절대 _cs()/store.tx()를 잡지 않는다 — 다른 코디네이터 변이가 이 동안 interleave 가능.
        push: per-call remote override(complete_task 등). None 이면 self.auto_push 상속."""
        if not self.git:
            return None, None   # repo 미바인딩 — DB-only 응결(테스트/드라이런)
        task_id, branch = intent["task_id"], intent["branch"]
        try:
            wt = self._ensure_integration_wt()
            msg = f"CLOUD CONNECT {task_id}\n\n{self._trailer(task_id)}"
            sha = self.git.merge_into(wt, self.integration_branch, branch, msg,
                                      timeout=self.merge_timeout)
            # 연결=merge 직후 remote sync(operator "커밋하면 바로 sync"의 OMD 내장판).
            # opt-in(push override > self.auto_push). fail-soft: push 실패해도 merge 는 로컬
            # 반영됨이라 connect 는 성공 유지(다음 connect/수동 push 가 따라잡음). 강제 push 안 함.
            remote = push if push is not None else self.auto_push
            if remote:
                try:
                    self.git.push_integration(wt, self.integration_branch, remote,
                                              timeout=self.merge_timeout)
                    self._emit("connect_pushed", task_id, remote=remote, merge_sha=sha)
                except (GitError, GitTimeout) as pe:
                    self._emit("connect_push_failed", task_id, remote=remote, error=str(pe))
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
                out = {"ok": False, "task_id": task_id, "state": "DONE", "retryable": True}
                # P2 shared 레인: shared 궤도를 쥔 task 의 merge conflict 는 불변식 버그가
                # 아니라 **정상사건**(같은 hunk 동시편집) — 경보 대신 rebase 복구 힌트(P3).
                # 배타(write-only) task 의 conflict 는 기존 '구조적 불가=경보' 의미론 유지.
                shared = any(o["mode"] == "shared"
                             for o in self.store.orbits_for_task(task_id))
                if reason == "merge conflict" and shared:
                    reason = "shared_conflict"
                    out["hint"] = ("shared-lane 3-way conflict (정상사건) — worktree 브랜치를 "
                                   "통합 tip 위로 rebase 해 충돌을 해소하고 connect 를 재시도")
                    self._emit("connect_shared_conflict", task_id, error=str(err))
                else:
                    self._emit("connect_aborted", task_id, reason=str(err))
                out["reason"] = f"{reason}: {err}"
                return out
            # 성공: P0-6 순서 — merge_sha 먼저 기록 → MERGED → write-orbit 해제(+unpin).
            self.store.set_task(task_id, merge_sha=merge_sha, merged_at=time.time())
            self.store.set_task(task_id, state=fsm.advance("task", "CONNECTING", "merged"))
            # §D12: 통합 generation 전진 + 겹치는 live read-궤도 stale 표시(write-orbit 해제
            # **전** — pathspec 이 아직 잡힐 때 글로브를 모은다).
            new_gen = self.store.bump_integration_gen()
            merged_globs = self._merged_write_globs(task_id)
            # merge_log: 이 gen 에 통합으로 들어간 write-globs 기록 — consumer 가 release 한 뒤에도
            # connect 에서 자기 read-set 코히런스를 검사할 수 있게(궤도 생명과 분리, §D12).
            self.store.append_merge_log(new_gen, task_id, merged_globs)
            stale_reads = self._mark_stale_reads(task_id, new_gen, merged_globs)
            self._release_task_write_orbits(task_id)
            self._release_merge_token_locked(token_id)
            if self.git and intent.get("worktree"):
                self.git.remove_worktree(intent["worktree"])
            self.store.set_flag(task_id, "merged")
            self._emit("connect_merged", task_id, merge_sha=merge_sha,
                       gen=new_gen, stale_reads=len(stale_reads))
            self._promote_pending()
            return {"ok": True, "task_id": task_id, "state": "MERGED", "merge_sha": merge_sha,
                    "gen": new_gen, "stale_reads": stale_reads}

    # ---- D3 플래그: EPHEMERAL(=lease) vs LATCH(영속·단조) (§D3, §1.2, §3.H) ----
    def _flag_satisfied(self, frow, want) -> bool:
        """플래그가 want 를 만족하나(LIVE 여야). 정확 값일치 OR 단조 랭크 도달 —
        want='done' 은 'merged'(상위 랭크)로도 만족된다(merged ⊃ done, §3.H 의존해제)."""
        if frow is None or frow["status"] != "LIVE":
            return False
        if frow["value"] == want:
            return True
        want_rank = LATCH_RANK.get(want, 0)
        return want_rank > 0 and frow["rank"] >= want_rank

    def _flag_broken(self, frow) -> bool:
        return frow is not None and frow["status"] == "BROKEN"

    def _wake_flag_waiters(self, key):
        """key 의 WAITING 대기자 중 만족/BROKEN 된 것을 깨운다(상태 전이만 — poll 이 읽음).
        register→poll 패턴이라 서버가 블로킹하지 않는다. epoch 가 아니라 현재 상태로 판정."""
        frow = self.store.get_flag_row(key)
        for w in self.store.waiters_for_key(key, "WAITING"):
            if self._flag_broken(frow):
                self.store.set_flag_waiter(w["waiter_id"], state="BROKEN",
                                           wake_reason="producer_dead")
            elif self._flag_satisfied(frow, w["want_value"]):
                self.store.set_flag_waiter(w["waiter_id"], state="SATISFIED",
                                           wake_reason="satisfied")

    def _break_ephemeral_flags_for_lease(self, lease_id, *, reason="producer_dead"):
        """받쳐주는 flag_ephemeral lease 가 거둬질 때(reclaim/만료) 그 EPHEMERAL 플래그를
        BROKEN 으로(자동 clear) + epoch +1 + 대기자 PRODUCER_DEAD 기상. 영구 hang 차단(§1.2)."""
        for f in self.store.ephemeral_flags_for_lease(lease_id):
            self.store.set_flag_status(f["key"], status="BROKEN", epoch=f["epoch"] + 1)
            self._emit("flag_broken", f["owner_agent"] or f["key"], key=f["key"],
                       reason=reason)
            self._wake_flag_waiters(f["key"])

    def flag_set(self, key, value, agent_id=None, *, flag_type=None, ttl=None,
                 fence=None, request_id=None, bail_epoch=None):
        """플래그 set — 두 종류(§D3):
          LATCH(기본): 영속·단조 사실. done(1)<merged(2). 하향 set 거부('un-finish 불가'),
            동값 재발행은 멱등 no-op. 소유 개념 없음(connect 의 'merged' latch 등). epoch 보존.
          EPHEMERAL: 소유 신호(build_running 등). 소유 agent + lease(orbits.kind='flag_ephemeral',
            owned+TTL). 보유자 사망 → reclaim/sweep 이 BROKEN(대기자 PRODUCER_DEAD). 같은 owner
            만 재set 가능(owner CAS); BROKEN/CLEARED 면 새로 세움. set/clear 마다 epoch +1.
        §D6: 회수된 좀비의 flag_set 차단(bail_epoch). EPHEMERAL 의 owner CAS 도 §D6 표의 보강.
        §D9: request_id 멱등(성공만 캐시)."""
        flag_type = (flag_type or "LATCH").upper()
        with self._cs():
            with self._idem(request_id, agent_id, "flag_set",
                            [key, value, flag_type]) as cache:
                if cache.hit:
                    return cache.value
                dead = self._check_alive(agent_id, bail_epoch)
                if dead:
                    return cache.set(dead)
                frow = self.store.get_flag_row(key)
                if flag_type == "EPHEMERAL":
                    return cache.set(self._flag_set_ephemeral(key, value, agent_id,
                                                              frow, ttl))
                return cache.set(self._flag_set_latch(key, value, agent_id, frow))

    def _flag_set_latch(self, key, value, agent_id, frow):
        """LATCH set — 단조 강제(done<merged). 하향=에러, 동값=멱등 no-op, 상향/신규=set."""
        new_rank = LATCH_RANK.get(value, 0)
        if frow is not None and frow["flag_type"] == "EPHEMERAL":
            return {"ok": False, "reason": "flag is EPHEMERAL, not LATCH", "key": key}
        if frow is not None:
            cur_rank = frow["rank"]
            if frow["status"] == "LIVE" and frow["value"] == value:
                return {"ok": True, "noop": True, "key": key, "value": value,
                        "flag_type": "LATCH", "epoch": frow["epoch"]}
            # 둘 다 랭크가 있고 하향이면 거부(단조 — un-finish 불가).
            if new_rank > 0 and cur_rank > 0 and new_rank < cur_rank:
                self._emit("flag_set_rejected", agent_id or key, key=key,
                           reason="monotonic_downgrade", current=frow["value"], to=value)
                return {"ok": False, "reason": "monotonic downgrade rejected",
                        "key": key, "current": frow["value"], "current_rank": cur_rank,
                        "to": value, "to_rank": new_rank}
            epoch = frow["epoch"] + 1
        else:
            epoch = 0
        rank = max(new_rank, frow["rank"] if frow else 0) if new_rank else (frow["rank"] if frow else 0)
        self.store.upsert_flag(key, value=value, set_by=agent_id, flag_type="LATCH",
                               rank=rank, status="LIVE", epoch=epoch)
        self._emit("flag_set", agent_id or key, key=key, value=value, flag_type="LATCH")
        self._wake_flag_waiters(key)
        return {"ok": True, "key": key, "value": value, "flag_type": "LATCH",
                "epoch": epoch, "rank": rank}

    def _flag_set_ephemeral(self, key, value, agent_id, frow, ttl):
        """EPHEMERAL set — 소유+TTL lease 로 받친다. 보유자 사망 시 reclaim/sweep 이 자동 BROKEN."""
        if agent_id is None:
            return {"ok": False, "reason": "EPHEMERAL flag requires owner agent", "key": key}
        # owner 를 agent 로 등록 — bail/zombie-reclaim 이 이 agent 의 flag_ephemeral lease 를
        # 찾아 거두려면 agents 행이 있어야 한다(안 그러면 reclaim 이 noop → 플래그 영구 잔존).
        self.store.upsert_agent(agent_id)
        if frow is not None and frow["flag_type"] == "LATCH":
            return {"ok": False, "reason": "flag is LATCH, not EPHEMERAL", "key": key}
        # owner CAS(§D6 보강): LIVE 면 같은 owner 만 재set. 타 agent 는 거부.
        if frow is not None and frow["status"] == "LIVE" \
                and frow["owner_agent"] not in (agent_id, None):
            return {"ok": False, "reason": "not flag owner", "key": key,
                    "owner": frow["owner_agent"]}
        # 받쳐주는 lease: LIVE 면 재사용(같은 owner), 아니면 새로 발급.
        lease_id = frow["lease_id"] if (frow and frow["status"] == "LIVE") else None
        if lease_id is None:
            fence = self.store.next_fence()
            lease_id = self.store.add_orbit(
                task_id=None, agent_id=agent_id, pathspec=[], mode="write",
                state="HELD", fence=fence,
                expires_at=time.time() + (ttl if ttl is not None else (self.agent_ttl or 90.0)),
                reason=f"flag_ephemeral:{key}", kind="flag_ephemeral",
                resource_key=key)
        elif ttl is not None:
            self.store.set_orbit(lease_id, expires_at=time.time() + ttl)
        epoch = (frow["epoch"] + 1) if frow else 0
        self.store.upsert_flag(key, value=value, set_by=agent_id, flag_type="EPHEMERAL",
                               rank=0, status="LIVE", owner_agent=agent_id,
                               lease_id=lease_id, epoch=epoch)
        self._emit("flag_set", agent_id, key=key, value=value, flag_type="EPHEMERAL",
                   lease_id=lease_id)
        self._wake_flag_waiters(key)
        return {"ok": True, "key": key, "value": value, "flag_type": "EPHEMERAL",
                "epoch": epoch, "lease_id": lease_id}

    def flag_clear(self, key, agent_id=None, *, request_id=None, bail_epoch=None):
        """EPHEMERAL 플래그를 자발적으로 clear(작업 끝, 정상 해제). owner 만. 받쳐주는 lease 해제 +
        status→CLEARED + epoch +1 + 대기자 기상(want 가 다른 값이면 계속 대기, 끊기진 않음 —
        CLEARED 는 BROKEN 과 달리 '사실이 더 이상 참 아님'이지 'producer 사망'이 아니다).
        LATCH 는 clear 불가(단조사실은 영속)."""
        with self._cs():
            with self._idem(request_id, agent_id, "flag_clear", [key]) as cache:
                if cache.hit:
                    return cache.value
                dead = self._check_alive(agent_id, bail_epoch)
                if dead:
                    return cache.set(dead)
                frow = self.store.get_flag_row(key)
                if frow is None:
                    return cache.set({"ok": True, "noop": True, "key": key})
                if frow["flag_type"] != "EPHEMERAL":
                    return cache.set({"ok": False, "reason": "LATCH flag cannot be cleared",
                                      "key": key})
                if frow["status"] != "LIVE":
                    return cache.set({"ok": True, "noop": True, "key": key,
                                      "status": frow["status"]})
                if agent_id is not None and frow["owner_agent"] not in (agent_id, None):
                    return cache.set({"ok": False, "reason": "not flag owner", "key": key,
                                      "owner": frow["owner_agent"]})
                if frow["lease_id"]:
                    lo = self.store.get_orbit(frow["lease_id"])
                    if lo and lo["state"] == "HELD":
                        self.store.set_orbit(frow["lease_id"],
                                             state=fsm.advance("orbit", "HELD", "release"),
                                             released_at=time.time())
                self.store.set_flag_status(key, status="CLEARED", epoch=frow["epoch"] + 1)
                self._emit("flag_cleared", agent_id or key, key=key)
                self._wake_flag_waiters(key)
                return cache.set({"ok": True, "key": key, "status": "CLEARED"})

    def flag_get(self, key):
        frow = self.store.get_flag_row(key)
        if frow is None:
            return {"key": key, "value": None}
        return {"key": key, "value": frow["value"], "flag_type": frow["flag_type"],
                "status": frow["status"], "epoch": frow["epoch"], "rank": frow["rank"],
                "owner": frow["owner_agent"]}

    def flag_wait(self, key, want, timeout, agent_id=None):
        """대기 등록(§D3, §1.2). **timeout 필수**(None 거부 — 영구 hang 방지). 서버는
        블로킹하지 않는다(register→poll): 단일 스레드를 막으면 구름 전체가 직렬화됨.
        이미 만족이면 즉시 SATISFIED, producer 사망(BROKEN)이면 즉시 BROKEN(PRODUCER_DEAD),
        아니면 waiter_id 발급(클라가 flag_wait_poll 재호출). observed_epoch 로 ABA/유령기상 안전."""
        if timeout is None:
            return {"ok": False, "reason": "timeout required (no indefinite wait)", "key": key}
        with self._cs():
            frow = self.store.get_flag_row(key)
            if self._flag_broken(frow):
                return {"state": "BROKEN", "reason": "producer_dead", "key": key}
            if self._flag_satisfied(frow, want):
                return {"state": "SATISFIED", "key": key, "value": frow["value"]}
            observed_epoch = frow["epoch"] if frow else -1
            deadline = time.time() + timeout
            wid = self.store.add_flag_waiter(agent_id, key, want, observed_epoch, deadline)
            self._emit("flag_wait_registered", agent_id or key, key=key, want=want,
                       waiter_id=wid)
            return {"state": "WAITING", "waiter_id": wid, "key": key, "want": want,
                    "deadline": deadline}

    def flag_wait_poll(self, waiter_id):
        """대기 폴(저렴·멱등, §D3). 재검사는 value 가 아니라 **epoch** 로 — set→clear→set 의
        ABA 나 유령기상에 안전. 만족/BROKEN(producer_dead)/TIMEOUT/WAITING 중 하나를 회신.
        클라는 SATISFIED/TIMEOUT/BROKEN 전부 처리해야(BROKEN 을 성공이나 hang 으로 오인 금지)."""
        with self._cs():
            w = self.store.get_flag_waiter(waiter_id)
            if w is None:
                return {"ok": False, "reason": "no such waiter", "waiter_id": waiter_id}
            if w["state"] != "WAITING":
                # reclaim/sweep/다른 poll 이 이미 전이시킴(SATISFIED/BROKEN). 종단 회신.
                if w["state"] == "BROKEN":
                    return {"state": "BROKEN", "reason": w["wake_reason"] or "producer_dead",
                            "key": w["key"]}
                return {"state": w["state"], "key": w["key"]}
            self._sweep_inline()  # 만료된 flag_ephemeral lease 를 BROKEN 으로 반영(producer 사망)
            frow = self.store.get_flag_row(w["key"])
            if self._flag_broken(frow):
                self.store.set_flag_waiter(waiter_id, state="BROKEN",
                                           wake_reason="producer_dead")
                return {"state": "BROKEN", "reason": "producer_dead", "key": w["key"]}
            if self._flag_satisfied(frow, w["want_value"]):
                self.store.set_flag_waiter(waiter_id, state="SATISFIED",
                                           wake_reason="satisfied")
                return {"state": "SATISFIED", "key": w["key"], "value": frow["value"]}
            if time.time() >= w["deadline"]:
                self.store.set_flag_waiter(waiter_id, state="TIMEOUT", wake_reason="timeout")
                self._emit("flag_wait_timeout", w["agent_id"] or w["key"], key=w["key"])
                return {"state": "TIMEOUT", "key": w["key"]}
            return {"state": "WAITING", "waiter_id": waiter_id, "key": w["key"]}

    # ---- D4 세마포어: permit=lease, 가용 = max − count(ACTIVE) (§D4, §D7, §G) ----
    def _grant_permit(self, agent_id, sem_id, ttl):
        """ACTIVE permit lease 를 발급(임계구역 안). fence 부여(소유+fenced lease). 가용 검사는
        호출자(acquire/_promote_sem_waiters)가 먼저 한다 — 여기선 발급만."""
        fence = self.store.next_fence()
        pid = self.store.add_orbit(
            task_id=None, agent_id=agent_id, pathspec=[], mode="write",
            state="HELD", fence=fence,
            expires_at=time.time() + (ttl if ttl is not None else (self.agent_ttl or 90.0)),
            reason=f"sem_permit:{sem_id}", kind="sem_permit", resource_key=sem_id)
        self._emit("sem_acquired", agent_id, permit_id=pid, sem=sem_id, fence=fence)
        return {"ok": True, "state": "ACQUIRED", "permit_id": pid, "sem": sem_id,
                "fence": fence}

    def _has_earlier_waiter(self, sem_id, priority, agent_id) -> bool:
        """no-overtaking(§D7): 이 (priority, agent) 보다 **먼저 줄선** 다른 대기자가 있나.
        있으면 가용 슬롯이 있어도 양보(작은 acquire 스트림이 head 대기자를 영구 기아시키는 것 방지)."""
        for w in self.store.waiting_sem_waiters(sem_id):  # 우선순위 DESC → enqueued_seq ASC
            if w["agent_id"] == agent_id:
                continue
            if w["priority"] > priority:
                return True
            if w["priority"] == priority:
                return True  # 같은 우선순위면 먼저 줄선(정렬상 앞) 자가 우선 — 양보
        return False

    def _promote_sem_waiters(self, sem_id):
        """가용 슬롯이 생기면 줄선 순서(우선순위 DESC → FIFO)대로 head 대기자에게 permit 부여
        (§D7 no-overtaking). 임계구역 안에서만. 멱등 reuse 도 존중(이미 보유한 대기자는 그대로)."""
        sem = self.store.get_semaphore(sem_id)
        if sem is None:
            return
        for w in self.store.waiting_sem_waiters(sem_id):
            if self.store.count_active_permits(sem_id) >= sem["max_permits"]:
                break  # 슬롯 소진 — 나머지는 계속 대기
            existing = self.store.active_permit_for(sem_id, w["agent_id"])
            if existing is not None:
                # 이미 어떤 경로로 permit 을 받음 — 멱등 reuse, 대기자만 satisfied 처리.
                self.store.set_sem_waiter(w["waiter_id"], state="GRANTED",
                                          permit_id=existing["orbit_id"])
                continue
            g = self._grant_permit(w["agent_id"], sem_id, w["ttl"])
            self.store.set_sem_waiter(w["waiter_id"], state="GRANTED",
                                      permit_id=g["permit_id"])

    def sem_declare(self, sem_id, max_permits):
        """세마포어 선언/등록(멱등). max_permits 변경 시 갱신 — 슬롯이 늘면 대기자 promote."""
        if max_permits < 1:
            return {"ok": False, "reason": "max_permits must be >= 1", "sem": sem_id}
        with self._cs():
            self.store.add_semaphore(sem_id, max_permits)
            self._emit("sem_declared", sem_id, sem=sem_id, max_permits=max_permits)
            self._promote_sem_waiters(sem_id)  # max 증가 시 새 슬롯을 줄선 대기자에게
            return {"ok": True, "sem": sem_id, "max_permits": max_permits}

    def acquire(self, agent_id, sem_id, *, ttl=300.0, no_wait=False, priority=0,
                request_id=None, bail_epoch=None):
        """세마포어 permit 획득(§D4). 가용 = max − count(ACTIVE permit)(저장정수 아님 → 누수 0).
          - 멱등 reuse: 이미 ACTIVE permit 을 쥐고 있으면 그대로 반환(재발급 안 함, MCP 재시도 안전).
          - 가용 ∧ no-overtaking(§D7: 먼저 줄선 자 없음)이면 즉시 부여(ACQUIRED).
          - 아니면: no_wait=True → FAIL(즉시 실패), no_wait=False → WAITING(waiter_id 발급, poll).
        임계구역(D1)에서 check-then-grant 가 원자 → 초과배정 불가(두 acquirer 가 동시에 N-1 보고
        둘 다 N+1번째 부여하는 레이스 차단). 보유자 사망 → reclaim/sweep 이 permit EXPIRE → 슬롯 복구."""
        with self._cs():
            args = [agent_id, sem_id, ttl, no_wait, priority]
            with self._idem(request_id, agent_id, "acquire", args) as cache:
                if cache.hit:
                    return cache.value
                self._sweep_inline()  # 죽은 보유자 permit 만료 반영 → 가용 최신화
                dead = self._check_alive(agent_id, bail_epoch)
                if dead:
                    return cache.set(dead)
                sem = self.store.get_semaphore(sem_id)
                if sem is None:
                    return cache.set({"ok": False, "reason": "no such semaphore",
                                      "sem": sem_id})
                self.store.upsert_agent(agent_id)
                # 멱등 reuse — 이미 쥔 permit 이 있으면 그대로(재발급 금지). 재시도/중복 acquire 안전.
                existing = self.store.active_permit_for(sem_id, agent_id)
                if existing is not None:
                    self._emit("sem_reuse", agent_id, permit_id=existing["orbit_id"],
                               sem=sem_id)
                    return cache.set({"ok": True, "state": "ACQUIRED",
                                      "permit_id": existing["orbit_id"], "sem": sem_id,
                                      "fence": existing["fence"], "reuse": True})
                avail = sem["max_permits"] - self.store.count_active_permits(sem_id)
                if avail >= 1 and not self._has_earlier_waiter(sem_id, priority, agent_id):
                    return cache.set(self._grant_permit(agent_id, sem_id, ttl))
                # 가용 없음(또는 먼저 줄선 자 있음).
                if no_wait:
                    self._emit("sem_acquire_failed", agent_id, sem=sem_id, reason="no_permits")
                    # FAIL 은 캐시 금지(§3.C) — 슬롯이 나면 재시도가 성공해야.
                    return cache.set({"ok": False, "state": "FAIL", "sem": sem_id,
                                      "reason": "no permits available", "avail": max(avail, 0)})
                # 대기 등록(register→poll, 비블로킹). timeout = ttl 만큼 대기(영구 hang 없음).
                seq = self.store.next_seq()
                deadline = time.time() + (ttl if ttl is not None else (self.agent_ttl or 90.0))
                wid = self.store.add_sem_waiter(sem_id, agent_id, ttl, priority, seq, deadline)
                self._emit("sem_wait_registered", agent_id, sem=sem_id, waiter_id=wid)
                # WAITING 은 캐시 금지(상태가 곧 바뀜) — 비성공으로 _is_success 가 거른다.
                return cache.set({"ok": True, "state": "WAITING", "waiter_id": wid,
                                  "sem": sem_id, "deadline": deadline})

    def acquire_poll(self, waiter_id):
        """세마포어 대기 폴(저렴·멱등, register→poll). GRANTED(permit 부여됨)/TIMEOUT/CANCELLED/
        WAITING 중 하나. poll 내부 sweep 이 죽은 보유자 permit 을 거둬 슬롯을 복구하고, head 면
        부여받는다(no-overtaking)."""
        with self._cs():
            w = self.store.get_sem_waiter(waiter_id)
            if w is None:
                return {"ok": False, "reason": "no such waiter", "waiter_id": waiter_id}
            if w["state"] == "GRANTED":
                p = self.store.get_orbit(w["permit_id"]) if w["permit_id"] else None
                return {"ok": True, "state": "ACQUIRED", "permit_id": w["permit_id"],
                        "sem": w["sem_id"], "fence": p["fence"] if p else None}
            if w["state"] != "WAITING":
                return {"ok": w["state"] != "CANCELLED", "state": w["state"],
                        "sem": w["sem_id"]}
            self._sweep_inline()           # 죽은 보유자 permit 만료 → 슬롯 복구 + head promote
            self._promote_sem_waiters(w["sem_id"])
            w = self.store.get_sem_waiter(waiter_id)
            if w["state"] == "GRANTED":
                p = self.store.get_orbit(w["permit_id"]) if w["permit_id"] else None
                return {"ok": True, "state": "ACQUIRED", "permit_id": w["permit_id"],
                        "sem": w["sem_id"], "fence": p["fence"] if p else None}
            if time.time() >= w["deadline"]:
                self.store.set_sem_waiter(waiter_id, state="TIMEOUT")
                self._emit("sem_wait_timeout", w["agent_id"], sem=w["sem_id"])
                return {"ok": False, "state": "TIMEOUT", "sem": w["sem_id"]}
            return {"ok": True, "state": "WAITING", "waiter_id": waiter_id,
                    "sem": w["sem_id"]}

    def sem_release(self, permit_id, agent_id, fence, *, request_id=None, bail_epoch=None):
        """permit 반납(§D4). 소유+fence 일치해야 — 이중해제·재부여후해제 방지(§D6). 이미
        RELEASED/EXPIRED 면 멱등 OK(MCP 재시도 안전). 반납 즉시 슬롯이 나면 줄선 대기자 promote."""
        with self._cs():
            with self._idem(request_id, agent_id, "sem_release",
                            [permit_id, fence]) as cache:
                if cache.hit:
                    return cache.value
                dead = self._check_alive(agent_id, bail_epoch)
                if dead:
                    return cache.set(dead)
                p = self.store.get_orbit(permit_id)
                if not p or p["kind"] != "sem_permit":
                    return cache.set({"ok": False, "reason": "no such permit"})
                if p["state"] in ("RELEASED", "EXPIRED"):
                    return cache.set({"ok": True, "noop": True, "state": p["state"]})
                if p["state"] != "HELD":
                    return cache.set({"ok": False, "reason": f"not HELD: {p['state']}"})
                bad = self._check_owner(p, agent_id, fence)  # owner∧fence (P0-3 유형)
                if bad:
                    return cache.set(bad)
                self.store.set_orbit(permit_id,
                                     state=fsm.advance("orbit", "HELD", "release"),
                                     released_at=time.time())
                self._emit("sem_released", agent_id, permit_id=permit_id,
                           sem=p["resource_key"])
                self._promote_sem_waiters(p["resource_key"])  # 빈 슬롯을 줄선 순서로(§D7)
                return cache.set({"ok": True, "sem": p["resource_key"]})

    def sem_status(self, sem_id):
        """세마포어 현황(가용/활성/대기). 디버그·관측용(read-only)."""
        with self._cs():
            self._sweep_inline()
            sem = self.store.get_semaphore(sem_id)
            if sem is None:
                return {"ok": False, "reason": "no such semaphore", "sem": sem_id}
            active = self.store.count_active_permits(sem_id)
            waiting = len(self.store.waiting_sem_waiters(sem_id))
            return {"ok": True, "sem": sem_id, "max_permits": sem["max_permits"],
                    "active": active, "available": max(sem["max_permits"] - active, 0),
                    "waiting": waiting}

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
          (a) task 가 없어졌거나 ABORTED(reclaim 이 abort→requeue 했으면 새 PENDING 행이지만
              agent_id=None — lease 도 함께 해제됨), 또는
          (b) write-orbit 이 HELD 가 아님(lease 만료/해제 = 보유자 사망/탈출, MERGED 제외), 또는
          (c) 도착했었는데 현재 write fence 가 도착 시점과 달라짐(ABA — 만료 후 타인에게 재부여)."""
        t = self.store.get_task(party["task_id"])
        if t is None or t["state"] == "ABORTED":
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
        deadline = time.time() + max(self.merge_timeout, 5.0) + 10.0
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
            offending = self._writeset_audit(task_id, t["branch"], write_globs)
            if offending:
                return {"ok": False, "reason": "writeset_violation",
                        "offending": offending, "task_id": task_id}
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
            if self.git and t["branch"]:
                branch_tip = self.git.branch_tip(t["branch"])
            self.store.set_task(task_id, state=s, connect_fence=cap_fence,
                                connect_intent_at=time.time(), branch_tip_sha=branch_tip)
            mdeadline = time.time() + max(self.merge_timeout, 5.0) + MERGE_PIN_GRACE_S
            for o in writes:
                self.store.set_orbit(o["orbit_id"], merging=1, merge_deadline=mdeadline)
            self._emit("connect_started", task_id, token_id=token_id, fence=cap_fence,
                       via="barrier")
            intent = {"task_id": task_id, "branch": t["branch"], "worktree": t["worktree"],
                      "writes": [o["orbit_id"] for o in writes]}
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

    def status(self):
        self.sweep()
        return self.store.snapshot()
