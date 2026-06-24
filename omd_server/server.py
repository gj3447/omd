"""OMD MCP 서버 — FastMCP 툴 스키마 (MCP 툴 = CLI 동사, 1:1).

`pip install -e .[server]` 후 `python -m omd_server.server [db_path]` 로 기동.
fastmcp 미설치 시 import만 가드 (core/cli/tests는 fastmcp 없이 동작).

툴 표면:
  claim(agent, paths, mode, ttl, task)      궤도 lease 획득 (입체 검사 → HELD or PENDING)
  release(orbit_id) / renew(orbit_id, ttl)
  declare(task, name, writes, reads, deps)  write-set(궤도) 선언
  next(agent)                               서로소(입체) READY 작업 추천
  start(task, agent) / finish(task)
  connect(task)                             CLOUD CONNECT(응결=merge) + fencing 집행
  flag_set(key, value) / flag_get(key)
  sweep() / status()
"""

from __future__ import annotations

from .core import Coordinator

try:
    from fastmcp import FastMCP
except ImportError:  # 서버 extra 미설치
    FastMCP = None


def build_server(db_path: str = "omd.db"):
    if FastMCP is None:
        raise RuntimeError("fastmcp 미설치: pip install -e .[server]")
    mcp = FastMCP("omd")
    omd = Coordinator(db_path)

    @mcp.tool()
    def claim(agent: str, paths: list[str], mode: str = "write", ttl: float = 600.0,
              task: str | None = None, priority: int = 0,
              request_id: str | None = None, bail_epoch: int | None = None) -> dict:
        """궤도(write-set) lease 획득. 입체면 HELD, 충돌이면 PENDING(+우선순위), 데드락이면 DENIED.
        request_id로 멱등(재시도가 누수 lease를 안 만듦), bail_epoch로 회수된 좀비 차단(§D6)."""
        return omd.claim(agent, paths, mode, ttl=ttl, task_id=task, priority=priority,
                         request_id=request_id, bail_epoch=bail_epoch)

    @mcp.tool()
    def release(orbit_id: str, agent: str, fence: int,
                request_id: str | None = None, bail_epoch: int | None = None) -> dict:
        """궤도 lease 반납. 소유+fence 일치해야(아무나 남의 궤도 해제 불가)."""
        return omd.release(orbit_id, agent, fence, request_id=request_id, bail_epoch=bail_epoch)

    @mcp.tool()
    def renew(orbit_id: str, agent: str, fence: int, ttl: float = 600.0,
              request_id: str | None = None, bail_epoch: int | None = None) -> dict:
        """궤도 lease 갱신(keepalive, TTL/3 주기). 소유+fence 불일치=FENCED_OUT."""
        return omd.renew(orbit_id, agent, fence, ttl, request_id=request_id,
                         bail_epoch=bail_epoch)

    @mcp.tool()
    def bail(agent: str, request_id: str | None = None) -> dict:
        """물방울 긴급 탈출(자발). 보유 궤도 전부 해제 + 작업 requeue + worktree/브랜치 정리(멱등)."""
        return omd.bail(agent, request_id=request_id)

    @mcp.tool()
    def declare(task: str, name: str = "", writes: list[str] | None = None,
                reads: list[str] | None = None, deps: list[str] | None = None,
                priority: int = 0) -> dict:
        """작업의 write-set(궤도)/read-set/의존을 선언."""
        return omd.declare(task, name=name, writes=writes, reads=reads,
                           deps=deps, priority=priority)

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
    def start(task: str, agent: str, request_id: str | None = None,
              bail_epoch: int | None = None) -> dict:
        return omd.start(task, agent, request_id=request_id, bail_epoch=bail_epoch)

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
                request_id: str | None = None, bail_epoch: int | None = None) -> dict:
        """CLOUD CONNECT(응결=merge, split-phase). agent/fence를 주면 write-orbit fence==captured
        까지 재검증(P0-4). 작업 중 lease 만료/ABA면 fencing으로 거부. merge_token으로 직렬화."""
        return omd.connect(task, agent, fence, request_id=request_id, bail_epoch=bail_epoch)

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
    def heartbeat(agent: str) -> dict:
        """물방울 생존 신호. 끊기면(agent_ttl 초과) 좀비 회수로 궤도/작업 반환."""
        return omd.heartbeat(agent)

    @mcp.tool()
    def sweep() -> dict:
        return omd.sweep()

    @mcp.tool()
    def status() -> dict:
        return omd.status()

    return mcp


if __name__ == "__main__":
    import sys
    build_server(sys.argv[1] if len(sys.argv) > 1 else "omd.db").run()
