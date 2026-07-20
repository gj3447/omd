"""OMD MCP 서버 — FastMCP 툴 스키마 (MCP 툴 = CLI 동사, 1:1).

`pip install -e .[server]` 후 `python -m omd_server.server [db_path]` 로 기동.
fastmcp 미설치 시 import만 가드 (core/cli/tests는 fastmcp 없이 동작).

툴 표면:
  about()                                   OMD 가 뭔지/뭐가 아닌지 + 표준 운행 루프 (오리엔테이션)
  claim(agent, paths, mode, ttl, task)      궤도 lease 획득 (입체 검사 → HELD or PENDING)
  rollover_claim(prior_orbit_id, generation)  명시적 비정책 종단 N→N+1
  release(orbit_id) / renew(orbit_id, ttl) / cancel_wait(orbit_id, generation)
  declare(task, name, writes, reads, deps)  write-set(궤도) 선언
  next(agent)                               서로소(입체) READY 작업 추천
  start(task, agent) / finish(task)
  connect(task)                             CLOUD CONNECT(응결=merge) + fencing 집행
  flag_set(key, value) / flag_get(key)
  sweep() / status()
"""

from __future__ import annotations

import math
import os

from .admission_config import (
    parse_admission_aging_quantum as _parse_admission_aging_quantum,
    parse_admission_max_age_boost as _parse_admission_max_age_boost,
    parse_admission_queue_capacity as _parse_admission_queue_capacity,
)
from .core import Coordinator
from .events import Emitter
from .sinks import JsonlSink

# 첫 접점(MCP `initialize`)에서 클라이언트/에이전트에 그대로 노출되는 자기소개.
# 비어 있으면 에이전트가 OMD 를 'object-model/스키마/계약 정의물'로 오독한다
# (호스트 프로젝트의 정의·계약 패러다임으로 빈칸을 채움). 그 오독을 구조적으로 차단한다.
OMD_INSTRUCTIONS = """\
OMD (Orbital Motion Droplet / 입체운행물방울) — a runtime COORDINATOR for running
N coding agents in PARALLEL on ONE git repository, with merge conflicts prevented
*in advance* by server-authoritative disjoint write-set leases + git-worktree
isolation, then merged back via CLOUD CONNECT.

WHAT IT IS *NOT*: OMD is NOT an object model, NOT a data schema, NOT an acceptance
contract, and NOT an artifact you must define/author/adopt per project before using
it. There is nothing to define first. If you were asked to "use/apply OMD on project
X", that means "coordinate parallel dev on repo X with these tools" — it does NOT
mean "author an OMD schema/contract and gate it through OOPTDD".

DRIVER LOOP (per task; MCP verbs == CLI verbs):
  declare(task, writes=[...], deps=[...])   # 1. register disjoint write-sets (orbits)
  next(agent)                               # 2. get a safe disjoint READY task
  start(task, agent)                        # 3. launch the agent's git worktree
  claim(agent, paths, task=...)             # 4. lease the write-set (HELD / PENDING)
  ...agent edits files only in its worktree...
  commit(task, msg); finish(task)           # 5. commit + mark DONE
  connect(task)                             # 6. CLOUD CONNECT = real git merge (fenced)

SYNC PRIMITIVES: barrier_* (rendezvous before merge), flag_* (signals),
sem_*/acquire (semaphores), heartbeat/sweep/bail (liveness & emergency escape).

Call about() any time to re-read this orientation. Full design: README.md / CONCEPT.md.
"""

try:
    from fastmcp import FastMCP
    from fastmcp.server.lifespan import lifespan
except ImportError:  # 서버 extra 미설치
    FastMCP = None


DEFAULT_SERVER_SWEEP_INTERVAL = 1.0


def _parse_server_sweep_interval(raw: str | None) -> float | None:
    """MCP default is autonomous; exact zero is the operator opt-out."""
    if raw is None or not raw.strip():
        return DEFAULT_SERVER_SWEEP_INTERVAL
    try:
        interval = float(raw)
    except ValueError as exc:
        raise ValueError(
            "OMD_SWEEP_INTERVAL must be 0 or a positive finite number"
        ) from exc
    if not math.isfinite(interval) or interval < 0:
        raise ValueError("OMD_SWEEP_INTERVAL must be 0 or a positive finite number")
    return interval or None


def _coordinator_lifespan(omd: Coordinator, *, sweep_interval: float | None = None):
    @lifespan
    async def coordinator_lifespan(server):
        try:
            # Constructor-time recovery is synchronous, but no background
            # writer/notifier effect starts until lifespan owns its shutdown.
            omd.start_background_workers(
                sweep_interval=sweep_interval,
                # Even with OMD_SWEEP_INTERVAL=0 an enforced server must retain
                # its lease through synchronous close/effect joins.
                heartbeat=omd.enforce_single_coordinator,
            )
            yield {"omd": omd}
        finally:
            # Stop and join the periodic writer before handing leadership off.
            # close() fails loudly rather than resigning with a live old writer.
            omd.close()
            if omd.enforce_single_coordinator:
                omd.resign()

    return coordinator_lifespan


def build_server(db_path: str = "omd.db"):
    if FastMCP is None:
        raise RuntimeError("fastmcp 미설치: pip install -e .[server]")
    # Codex starts stdio MCP servers per client/session. A process-wide singleton
    # leader lease makes concurrent MCP clients fail before initialize; SQLite
    # BEGIN IMMEDIATE still serializes cross-process mutations for this surface.
    # §D3/D4 백그라운드 sweep: MCP는 기본 1초. 빈 값/미설정도 default, 정확한 0만 opt-out,
    # 양의 finite 값은 override다. 파싱은 Coordinator/DB 생성 전에 fail loud한다.
    _swp = _parse_server_sweep_interval(os.environ.get("OMD_SWEEP_INTERVAL"))
    _capacity = _parse_admission_queue_capacity(
        os.environ.get("OMD_ADMISSION_QUEUE_CAPACITY")
    )
    _aging_quantum = _parse_admission_aging_quantum(
        os.environ.get("OMD_ADMISSION_AGING_QUANTUM_SECONDS")
    )
    _max_age_boost = _parse_admission_max_age_boost(
        os.environ.get("OMD_ADMISSION_MAX_AGE_BOOST")
    )
    # Q6 observability: env OMD_EVENT_LOG=<path> 주면 모든 OMD 동사 이벤트를 append-only JSONL
    # 로 durable 기록(OpenObserve/vector 가 tail→ship). 미설정=NOOP(기존동작). fail-soft.
    _evlog = os.environ.get("OMD_EVENT_LOG")
    _events = Emitter(JsonlSink(_evlog)) if _evlog else None
    omd = Coordinator(
        db_path,
        enforce_single_coordinator=False,
        # build_server() may synchronously initialize/recover durable state, but
        # must not start a background writer/effect before FastMCP lifespan.
        sweep_interval=None,
        autostart_background_workers=False,
        events=_events,
        admission_queue_capacity=_capacity,
        admission_aging_quantum=_aging_quantum,
        admission_max_age_boost=_max_age_boost,
    )
    mcp = FastMCP(
        "omd",
        instructions=OMD_INSTRUCTIONS,
        lifespan=_coordinator_lifespan(omd, sweep_interval=_swp),
    )

    @mcp.tool()
    def about() -> dict:
        """OMD 가 무엇이고(병렬 코딩 에이전트 코디네이터) 무엇이 아닌지(정의/스키마/계약이 아님),
        그리고 표준 운행 루프(declare→next→start→claim→commit→finish→connect)를 돌려준다.
        OMD 적용을 시작하기 전에/헷갈릴 때 이걸 먼저 호출할 것."""
        return {
            "name": "OMD — Orbital Motion Droplet / 입체운행물방울",
            "is": "병렬 코딩 에이전트 코디네이터 (1 git repo 에서 N 에이전트를 서로소 "
                  "write-set 으로 충돌 없이 병렬 운행 → CLOUD CONNECT 로 merge)",
            "is_not": [
                "object model 아님", "data schema 아님", "acceptance contract 아님",
                "프로젝트마다 먼저 정의/채택해야 하는 정의물 아님 — 시작 전 작성할 게 없다",
            ],
            "driver_loop": [
                "declare(task, writes=[...], deps=[...])",
                "next(agent)", "start(task, agent)",
                "claim(agent, paths, task=...)",
                "commit(task, msg)", "finish(task)", "connect(task)",
            ],
            "happy_path_oneshot": {
                "note": "7-verb 시퀀스 대신 이 2개면 충분(망각-스트랜드 방지). 채택 자동화: "
                        "부팅 시 begin() 한 번 → 자동 격리, 끝날 때 complete_task() 한 번.",
                "begin(agent, task, writes)": "declare→deps게이트→claim→promote→start 원샷 "
                                              "onboarding(worktree 자동 격리). 충돌 시 fail-loud.",
                "complete_task(task, msg)": "commit→finish→connect(+push) 원샷 마감.",
                "task_conditions(task)": "task 의 직교 condition(deps/held/heartbeat/merge_ready) "
                                         "+ phase 관측(read-only).",
            },
            "sync_primitives": ["barrier_*", "flag_*", "sem_*/acquire",
                                "heartbeat/sweep/bail"],
            "docs": ["README.md", "CONCEPT.md", "SERVER_SPEC.md", "CONCURRENCY.md"],
        }

    @mcp.tool()
    def claim(agent: str, paths: list[str], mode: str = "write", ttl: float = 600.0,
              task: str | None = None, priority: int = 0,
              request_id: str | None = None, bail_epoch: int | None = None) -> dict:
        """궤도(write-set) lease 획득. 입체면 HELD, 충돌이면 PENDING(+우선순위), 데드락이면 DENIED.
        request_id로 멱등(재시도가 누수 lease를 안 만듦), bail_epoch로 회수된 좀비 차단(§D6)."""
        return omd.claim(agent, paths, mode, ttl=ttl, task_id=task, priority=priority,
                         request_id=request_id, bail_epoch=bail_epoch)

    @mcp.tool()
    def rollover_claim(prior_orbit_id: str, agent: str, expected_generation: int,
                       bail_epoch: int, request_id: str) -> dict:
        """RELEASED/EXPIRED/CANCELLED/TIMED_OUT claim을 명시적으로 N+1 세대로 재입장한다.
        request_id는 원 claim ID와 다른 rollover 작업 ID다. 보존 기간 안의 exact retry는
        같은 결과를 재생하며, 이후에도 predecessor fence가 중복 세대 생성을 막는다."""
        return omd.rollover_claim(
            prior_orbit_id,
            agent,
            expected_generation,
            bail_epoch=bail_epoch,
            request_id=request_id,
        )

    @mcp.tool()
    def release(orbit_id: str, agent: str, fence: int,
                request_id: str | None = None, bail_epoch: int | None = None) -> dict:
        """궤도 lease 반납. 소유+fence 일치해야(아무나 남의 궤도 해제 불가)."""
        return omd.release(orbit_id, agent, fence, request_id=request_id, bail_epoch=bail_epoch)

    @mcp.tool()
    def cancel_wait(orbit_id: str, agent: str, request_generation: int, bail_epoch: int,
                    request_id: str | None = None) -> dict:
        """PENDING admission wait를 현재 owner+generation+bail_epoch로 인증해 취소한다.
        request_id는 원래 claim ID와 분리된 취소 작업 ID이며, 성공 재시도는 exactly-once로 재생된다."""
        return omd.cancel_wait(
            orbit_id,
            agent,
            request_generation,
            bail_epoch=bail_epoch,
            request_id=request_id,
        )

    @mcp.tool()
    def cancel(task: str, reason: str = "", request_id: str | None = None) -> dict:
        """미시작 태스크(PENDING/READY/BLOCKED) 종결 — lease-only 흐름(declare+claim, start 미경유)
        의 태스크를 PENDING 잔류 없이 닫는다(→ABORTED, requeue 로 재개 가능). 시작된 태스크는
        거부(finish/bail 경유). 멱등(F4, 채택마찰 2026-07-02)."""
        return omd.cancel(task, reason=reason, request_id=request_id)

    @mcp.tool()
    def renew(orbit_id: str, agent: str, fence: int, ttl: float = 600.0,
              request_id: str | None = None, bail_epoch: int | None = None) -> dict:
        """궤도 lease 갱신(keepalive, TTL/3 주기). 소유+fence 불일치=FENCED_OUT."""
        return omd.renew(orbit_id, agent, fence, ttl, request_id=request_id,
                         bail_epoch=bail_epoch)

    @mcp.tool()
    def read_refresh(task: str, agent: str, fence: int,
                     request_id: str | None = None, bail_epoch: int | None = None) -> dict:
        """§D12: rebase/재독 후 task 의 read-set 을 현 통합 gen 으로 재앵커(유령 읽기 청산).
        connect 가 read_stale 로 거부되면 worktree 를 통합 최신으로 rebase 한 뒤 이걸 호출.
        소유+fence 가드(그 task 의 write-orbit 을 쥔 caller 만)."""
        return omd.read_refresh(task, agent, fence, request_id=request_id,
                                bail_epoch=bail_epoch)

    @mcp.tool()
    def bail(agent: str, request_id: str | None = None) -> dict:
        """물방울 긴급 탈출(자발). 보유 궤도 전부 해제 + 작업 requeue + worktree/브랜치 정리(멱등)."""
        return omd.bail(agent, request_id=request_id)

    @mcp.tool()
    def declare(task: str, name: str = "", writes: list[str] | None = None,
                reads: list[str] | None = None, deps: list[str] | None = None,
                priority: int = 0, shared: list[str] | None = None) -> dict:
        """작업의 write-set(궤도)/read-set/의존을 선언. shared = hot 공유파일 glob(P2 레인):
        배타 writes 와 달리 다른 task 의 shared 와 겹쳐도 병렬 진행 — claim 도 mode='shared' 로,
        응결은 git 3-way(진짜 충돌 시 shared_conflict retryable + rebase 힌트)."""
        return omd.declare(task, name=name, writes=writes, reads=reads,
                           deps=deps, priority=priority, shared=shared)

    @mcp.tool()
    def depend(task: str, after: str) -> dict:
        """작업 의존 엣지 추가(`task` after `after`). 의존 DAG에 사이클을 만들면 거부
        (`{ok:false, reason:'dep_cycle', cycle:[...]}`, 그래프 불변). self-dep 도 거부(P0-10)."""
        return omd.depend(task, after)

    @mcp.tool()
    def next(agent: str) -> dict | None:
        """지금 안전하게 운행 가능한 서로소(입체) 작업 추천 → READY."""
        return omd.next_task(agent)

    @mcp.tool()
    def task_conditions(task: str) -> dict | None:
        """task 의 K8s식 직교 condition(deps_satisfied/held/heartbeat_fresh/merge_ready) + phase
        rollup 관측. 순수 read — lifecycle 전이 없음(fsm state 가 authoritative). 미존재 task=None."""
        return omd.task_conditions(task)

    @mcp.tool()
    def start(task: str, agent: str, request_id: str | None = None,
              bail_epoch: int | None = None) -> dict:
        return omd.start(task, agent, request_id=request_id, bail_epoch=bail_epoch)

    @mcp.tool()
    def begin(task: str, agent: str, writes: list[str], reads: list[str] | None = None,
              shared: list[str] | None = None, deps: list[str] | None = None,
              priority: int = 0, name: str = "", ttl: float = 600.0,
              liveness_ttl: float | None = None,
              request_id: str | None = None, bail_epoch: int | None = None) -> dict:
        """원샷 onboarding(declare→deps게이트→claim→promote→start). "그냥 begin 하면 OMD 안에서
        격리" — 7-verb 앞단을 한 호출로. complete_task 의 start-side dual. fail-loud stage 전파."""
        return omd.begin(task, agent, writes, reads=reads, shared=shared, deps=deps,
                         priority=priority, name=name, ttl=ttl,
                         liveness_ttl=liveness_ttl,
                         request_id=request_id, bail_epoch=bail_epoch)

    @mcp.tool()
    def commit(task: str, msg: str, agent: str | None = None, fence: int | None = None,
               request_id: str | None = None, bail_epoch: int | None = None) -> dict:
        """물방울 worktree 커밋 + write-set 자문 감사. agent/fence를 주면 owner∧write-orbit
        HELD∧fence==f 재검증(§D6) — 오추방 좀비의 커밋 차단."""
        return omd.commit(task, msg, agent, fence, request_id=request_id, bail_epoch=bail_epoch)

    @mcp.tool()
    def finish(task: str, agent: str | None = None, fence: int | None = None,
               request_id: str | None = None, bail_epoch: int | None = None) -> dict:
        """작업 완료(IN_ORBIT→DONE). agent/fence를 주면 owner∧write-orbit HELD∧fence==f
        재검증(§D6) — 오추방 좀비의 finish 차단."""
        return omd.finish(task, agent, fence, request_id=request_id, bail_epoch=bail_epoch)

    @mcp.tool()
    def connect(task: str, agent: str | None = None, fence: int | None = None,
                push: str | None = None,
                request_id: str | None = None, bail_epoch: int | None = None) -> dict:
        """CLOUD CONNECT(응결=merge, split-phase). agent/fence를 주면 write-orbit fence==captured
        까지 재검증(P0-4). 작업 중 lease 만료/ABA면 fencing으로 거부. merge_token으로 직렬화.
        push=remote 면 merge 직후 통합브랜치를 그 remote 로 push(없으면 self.auto_push 상속)."""
        return omd.connect(task, agent, fence, push=push,
                           request_id=request_id, bail_epoch=bail_epoch)

    @mcp.tool()
    def complete_task(task: str, msg: str | None = None, agent: str | None = None,
                      fence: int | None = None, push: str | None = None,
                      request_id: str | None = None, bail_epoch: int | None = None) -> dict:
        """P5 원샷(happy-path): (선택)commit → finish → connect(+push) 한 번에. verb 망각-스트랜드
        (finish 빼면 IN_ORBIT 기아·connect 빼면 미통합) 방지. ok:True 는 오직 최종 state==MERGED;
        어느 단계 거부든 {ok:False, stage:'commit'|'finish'|'connect', ...}로 fail-loud 전파."""
        return omd.complete_task(task, msg, agent, fence, push=push,
                                 request_id=request_id, bail_epoch=bail_epoch)

    @mcp.tool()
    def flag_set(key: str, value: str, agent: str | None = None,
                 flag_type: str = "LATCH", ttl: float | None = None,
                 request_id: str | None = None, bail_epoch: int | None = None) -> dict:
        """플래그 set(§D3). flag_type='LATCH'(영속·단조 done<merged) | 'EPHEMERAL'(소유 신호=
        owned+TTL lease, 보유자 사망 시 자동 BROKEN). EPHEMERAL 은 agent 필수 + owner CAS."""
        return omd.flag_set(key, value, agent, flag_type=flag_type, ttl=ttl,
                            request_id=request_id, bail_epoch=bail_epoch)

    @mcp.tool()
    def flag_clear(key: str, agent: str | None = None,
                   request_id: str | None = None, bail_epoch: int | None = None) -> dict:
        """EPHEMERAL 플래그 자발 clear(작업 끝). owner 만. LATCH 는 clear 불가(단조사실 영속)."""
        return omd.flag_clear(key, agent, request_id=request_id, bail_epoch=bail_epoch)

    @mcp.tool()
    def flag_get(key: str) -> dict:
        return omd.flag_get(key)

    @mcp.tool()
    def flag_wait(key: str, want: str, timeout: float, agent: str | None = None) -> dict:
        """플래그 대기 등록(register→poll, 서버 비블로킹, §D3). timeout 필수(영구 hang 방지).
        즉시 SATISFIED/BROKEN(producer_dead) 또는 waiter_id 발급 → flag_wait_poll 재호출.
        의존 해제는 =done 이 아니라 =merged 에 건다(§3.H)."""
        return omd.flag_wait(key, want, timeout, agent)

    @mcp.tool()
    def flag_wait_poll(waiter_id: str) -> dict:
        """대기 폴(저렴·멱등). SATISFIED/TIMEOUT/BROKEN(producer_dead)/WAITING. epoch 재검사로
        ABA/유령기상 안전. BROKEN 을 성공이나 hang 으로 오인하지 말 것."""
        return omd.flag_wait_poll(waiter_id)

    @mcp.tool()
    def sem_declare(sem: str, max_permits: int) -> dict:
        """세마포어 선언/등록(§D4, 멱등). max_permits 변경 시 갱신(슬롯 늘면 대기자 promote)."""
        return omd.sem_declare(sem, max_permits)

    @mcp.tool()
    def acquire(agent: str, sem: str, ttl: float = 300.0, no_wait: bool = False,
                priority: int = 0, request_id: str | None = None,
                bail_epoch: int | None = None) -> dict:
        """세마포어 permit 획득(§D4). 가용=max−count(ACTIVE)(누수 0). 멱등 reuse(이미 보유시
        재발급 안 함). no_wait=False 면 WAITING(waiter_id → acquire_poll), no-overtaking(§D7)."""
        return omd.acquire(agent, sem, ttl=ttl, no_wait=no_wait, priority=priority,
                           request_id=request_id, bail_epoch=bail_epoch)

    @mcp.tool()
    def acquire_poll(waiter_id: str) -> dict:
        """세마포어 대기 폴(register→poll, 비블로킹). ACQUIRED/TIMEOUT/CANCELLED/WAITING."""
        return omd.acquire_poll(waiter_id)

    @mcp.tool()
    def sem_release(permit_id: str, agent: str, fence: int,
                    request_id: str | None = None, bail_epoch: int | None = None) -> dict:
        """permit 반납. 소유+fence 일치해야(이중해제·재부여후해제 방지, §D6). 슬롯 나면 대기자 promote."""
        return omd.sem_release(permit_id, agent, fence, request_id=request_id,
                               bail_epoch=bail_epoch)

    @mcp.tool()
    def sem_status(sem: str) -> dict:
        """세마포어 현황(가용/활성/대기) — 관측용."""
        return omd.sem_status(sem)

    @mcp.tool()
    def barrier_declare(name: str, task_ids: list[str], kind: str = "connect",
                        policy: str = "break", timeout: float | None = None) -> dict:
        """응결 랑데부 배리어 선언/재무장(§D5). 멤버십=task 집합(reclaim 으로 requeue 되면 N 재계산).
        전원 도착 → 결정적 순서로 응결(merge) → TRIPPED. 참가자 사망/타임아웃 → BROKEN(전원 기상).
        policy='break'(전원 깸) | 'shrink'(죽은 멤버 빼고 진행, 의존자 없을 때만)."""
        return omd.barrier_declare(name, task_ids, kind=kind, policy=policy, timeout=timeout)

    @mcp.tool()
    def barrier_arrive(name: str, agent: str, task: str, fence: int | None = None,
                       request_id: str | None = None, bail_epoch: int | None = None) -> dict:
        """참가자 도착(§D5). task 가 응결 준비됨(write-orbit HELD)을 표시. 전원 도착하면 trip
        (전 task 응결 후 TRIPPED). 사망/타임아웃이면 BROKEN. fence 를 주면 arrive 시점 재검증."""
        return omd.barrier_arrive(name, agent, task, fence=fence, request_id=request_id,
                                  bail_epoch=bail_epoch)

    @mcp.tool()
    def barrier_abort(name: str, agent: str | None = None,
                      request_id: str | None = None, bail_epoch: int | None = None) -> dict:
        """배리어 강제 break(§D5, Barrier.abort 시맨틱) — 도착해 있던 전원 BROKEN 기상(영구 hang 방지)."""
        return omd.barrier_abort(name, agent, request_id=request_id, bail_epoch=bail_epoch)

    @mcp.tool()
    def barrier_status(name: str) -> dict:
        """배리어 현황(상태/세대/도착/참가) — 관측용. 내부 sweep 으로 사망/타임아웃 반영."""
        return omd.barrier_status(name)

    @mcp.tool()
    def barrier_consume(name: str, agent: str | None = None,
                        request_id: str | None = None, bail_epoch: int | None = None) -> dict:
        """TRIPPED 배리어 결과 수거(→CONSUMED 종단, 증분11) — 멤버별 merge_sha 동봉.
        TRIPPED 에서만 유효; CONSUMED 재호출은 멱등 noop(결과 재동봉)."""
        return omd.barrier_consume(name, agent, request_id=request_id, bail_epoch=bail_epoch)

    @mcp.tool()
    def heartbeat(agent: str, ttl: float | None = None) -> dict:
        """물방울 생존 신호. 끊기면(생존창 초과) 좀비 회수로 궤도/작업 반환.
        ttl= 로 *자기 페이스 선언*(per-agent 생존창) — 인터랙티브 세션은 claim 직후 한 번
        heartbeat(agent, ttl=3600) 식으로 선언할 것(미선언=기계 물방울 crash-fast 기본)."""
        return omd.heartbeat(agent, ttl=ttl)

    @mcp.tool()
    def sweep() -> dict:
        return omd.sweep()

    @mcp.tool()
    def status() -> dict:
        return omd.status()

    return mcp


if __name__ == "__main__":
    import sys
    build_server(sys.argv[1] if len(sys.argv) > 1 else "omd.db").run(
        show_banner=False,
        log_level="WARNING",
    )
