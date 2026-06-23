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
              task: str | None = None, priority: int = 0) -> dict:
        """궤도(write-set) lease 획득. 입체면 HELD, 충돌이면 PENDING(+우선순위), 데드락이면 DENIED."""
        return omd.claim(agent, paths, mode, ttl=ttl, task_id=task, priority=priority)

    @mcp.tool()
    def release(orbit_id: str) -> dict:
        return omd.release(orbit_id)

    @mcp.tool()
    def renew(orbit_id: str, ttl: float = 600.0) -> dict:
        return omd.renew(orbit_id, ttl)

    @mcp.tool()
    def declare(task: str, name: str = "", writes: list[str] | None = None,
                reads: list[str] | None = None, deps: list[str] | None = None,
                priority: int = 0) -> dict:
        """작업의 write-set(궤도)/read-set/의존을 선언."""
        return omd.declare(task, name=name, writes=writes, reads=reads,
                           deps=deps, priority=priority)

    @mcp.tool()
    def next(agent: str) -> dict | None:
        """지금 안전하게 운행 가능한 서로소(입체) 작업 추천 → READY."""
        return omd.next_task(agent)

    @mcp.tool()
    def start(task: str, agent: str) -> dict:
        return omd.start(task, agent)

    @mcp.tool()
    def finish(task: str) -> dict:
        return omd.finish(task)

    @mcp.tool()
    def connect(task: str) -> dict:
        """CLOUD CONNECT(응결=merge). 작업 중 lease 만료면 fencing으로 거부."""
        return omd.connect(task)

    @mcp.tool()
    def flag_set(key: str, value: str, agent: str | None = None) -> dict:
        return omd.flag_set(key, value, agent)

    @mcp.tool()
    def flag_get(key: str) -> dict:
        return omd.flag_get(key)

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
