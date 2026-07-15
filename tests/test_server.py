"""FastMCP 서버 빌드 스모크 — fastmcp 설치 시 툴 스키마가 유효하게 구성되는지."""

import sys
import sqlite3
from datetime import timedelta
from types import SimpleNamespace

import pytest


def test_leader_heartbeat_loop_is_noop_without_singleton_enforcement():
    anyio = pytest.importorskip("anyio")
    pytest.importorskip("fastmcp")
    from omd_server.server import _leader_heartbeat_loop

    class _NonEnforcedCoordinator:
        enforce_single_coordinator = False

        def coordinator_heartbeat(self):
            raise AssertionError("non-enforced coordinator has no leader lease")

    anyio.run(_leader_heartbeat_loop, _NonEnforcedCoordinator())


def test_leader_heartbeat_loop_stops_when_leadership_is_lost(monkeypatch):
    anyio = pytest.importorskip("anyio")
    pytest.importorskip("fastmcp")
    from omd_server.core import CoordinatorConflict
    from omd_server import server as server_module

    calls = []

    async def no_wait(_interval):
        return None

    def fenced_heartbeat():
        calls.append("heartbeat")
        raise CoordinatorConflict("lost leadership")

    monkeypatch.setattr(server_module.anyio, "sleep", no_wait)
    omd = SimpleNamespace(
        enforce_single_coordinator=True,
        leader_ttl=30.0,
        coordinator_heartbeat=fenced_heartbeat,
    )
    anyio.run(server_module._leader_heartbeat_loop, omd)
    assert calls == ["heartbeat"]


def test_leader_heartbeat_loop_survives_transient_error_then_stops_on_conflict(monkeypatch):
    """GAP-3: CoordinatorConflict 외의 예외(일시적 오류)가 루프를 *조용히 죽이지* 않는다 —
    로그+백오프 후 재시도로 생존하고, 성공하면 연속-실패 카운터를 리셋한다. sweep 데몬 가드 미러."""
    anyio = pytest.importorskip("anyio")
    pytest.importorskip("fastmcp")
    from omd_server.core import CoordinatorConflict
    from omd_server import server as server_module

    calls = []
    # 2회 일시적 오류(생존해야) → 1회 성공(카운터 리셋) → 2회 일시적 오류 → takeover 로 typed 종결.
    script = [RuntimeError("db busy"), RuntimeError("db busy"), None,
              RuntimeError("db busy"), RuntimeError("db busy"),
              CoordinatorConflict("lost leadership")]

    async def no_wait(_interval):
        return None

    def scripted_heartbeat():
        calls.append("hb")
        step = script[len(calls) - 1]
        if step is not None:
            raise step

    monkeypatch.setattr(server_module.anyio, "sleep", no_wait)
    omd = SimpleNamespace(
        enforce_single_coordinator=True,
        leader_ttl=30.0,
        coordinator_heartbeat=scripted_heartbeat,
    )
    # max=3 인데도 카운터 리셋 덕에 3 연속에 도달 못 함 → 전 스크립트 소진(takeover 에서 종결).
    async def _drive():
        await server_module._leader_heartbeat_loop(omd, max_consecutive_failures=3)

    anyio.run(_drive)
    assert len(calls) == len(script)   # 일시 오류 4회 생존 + 성공 리셋 후 conflict 에서 종결


def test_leader_heartbeat_loop_exits_bounded_after_repeated_failures(monkeypatch):
    """GAP-3: 연속 실패가 상한을 넘으면 typed 로 탈출 — 무한 에러 루프 금지(유계 stop)."""
    anyio = pytest.importorskip("anyio")
    pytest.importorskip("fastmcp")
    from omd_server import server as server_module

    calls = []

    async def no_wait(_interval):
        return None

    def always_fails():
        calls.append("hb")
        raise RuntimeError("permanent programming error")

    monkeypatch.setattr(server_module.anyio, "sleep", no_wait)
    omd = SimpleNamespace(
        enforce_single_coordinator=True,
        leader_ttl=30.0,
        coordinator_heartbeat=always_fails,
    )

    async def _drive():
        await server_module._leader_heartbeat_loop(omd, max_consecutive_failures=3)

    anyio.run(_drive)
    # 정확히 상한 횟수만 시도하고 종료(무한루프 아님).
    assert len(calls) == 3


def test_coordinator_lifespan_resigns_only_when_singleton_enforced():
    anyio = pytest.importorskip("anyio")
    pytest.importorskip("fastmcp")
    from omd_server.server import _coordinator_lifespan

    class _Coordinator:
        leader_ttl = 30.0

        def __init__(self, enforced):
            self.enforce_single_coordinator = enforced
            self.resign_calls = 0

        def coordinator_heartbeat(self):
            return {"ok": True}

        def resign(self):
            self.resign_calls += 1
            return {"ok": True}

    async def enter_and_exit(omd):
        async with _coordinator_lifespan(omd)(None) as state:
            assert state == {"omd": omd}

    non_enforced = _Coordinator(False)
    anyio.run(enter_and_exit, non_enforced)
    assert non_enforced.resign_calls == 0

    enforced = _Coordinator(True)
    anyio.run(enter_and_exit, enforced)
    assert enforced.resign_calls == 1


def test_begin_tool_exposes_and_forwards_liveness_ttl(tmp_path, monkeypatch):
    anyio = pytest.importorskip("anyio")
    pytest.importorskip("fastmcp")
    from omd_server.core import Coordinator
    from omd_server.server import build_server

    mcp = build_server(str(tmp_path / "s.db"))
    observed = {}

    def fake_begin(self, task, agent, writes, **kwargs):
        observed.update(task=task, agent=agent, writes=writes, **kwargs)
        return {"ok": True}

    monkeypatch.setattr(Coordinator, "begin", fake_begin)

    async def inspect_and_call():
        tool = next(t for t in await mcp.list_tools() if t.name == "begin")
        assert "liveness_ttl" in tool.parameters["properties"]
        result = tool.fn(task="T", agent="ag", writes=["src/**"], liveness_ttl=90.0)
        assert result == {"ok": True}

    anyio.run(inspect_and_call)
    assert observed["liveness_ttl"] == 90.0


def test_server_builds(tmp_path):
    pytest.importorskip("fastmcp")
    from omd_server.server import build_server
    mcp = build_server(str(tmp_path / "s.db"))
    assert mcp is not None


def test_server_stdio_initializes_and_lists_tools(tmp_path):
    pytest.importorskip("fastmcp")
    pytest.importorskip("mcp")
    anyio = pytest.importorskip("anyio")

    from mcp.client.session import ClientSession
    from mcp.client.stdio import StdioServerParameters, stdio_client

    async def run_smoke():
        params = StdioServerParameters(
            command=sys.executable,
            args=["-m", "omd_server.server", str(tmp_path / "s.db")],
            cwd=str(tmp_path),
        )
        async with stdio_client(params) as (read_stream, write_stream):
            async with ClientSession(
                read_stream,
                write_stream,
                read_timeout_seconds=timedelta(seconds=5),
            ) as session:
                init = await session.initialize()
                assert init.serverInfo.name == "omd"
                tools = await session.list_tools()
                names = {tool.name for tool in tools.tools}
                assert {"claim", "release", "status"} <= names

    anyio.run(run_smoke)


def test_server_self_describes_as_parallel_coordinator(tmp_path):
    """첫 접점(MCP initialize)에서 OMD 가 '병렬-dev 코디네이터'임을, 그리고
    '정의해야 할 스키마/계약'이 *아님*을 평문으로 자기소개해야 한다.

    회귀 방지: instructions 가 비면 에이전트가 OMD 를 'object-model/계약 정의물'로
    오독한다(이 레포의 정의/계약 패러다임으로 빈칸을 채움). 그 오독이 이 테스트의 대상."""
    pytest.importorskip("fastmcp")
    pytest.importorskip("mcp")
    anyio = pytest.importorskip("anyio")

    from mcp.client.session import ClientSession
    from mcp.client.stdio import StdioServerParameters, stdio_client

    async def run_smoke():
        params = StdioServerParameters(
            command=sys.executable,
            args=["-m", "omd_server.server", str(tmp_path / "s.db")],
            cwd=str(tmp_path),
        )
        async with stdio_client(params) as (read_stream, write_stream):
            async with ClientSession(
                read_stream,
                write_stream,
                read_timeout_seconds=timedelta(seconds=5),
            ) as session:
                init = await session.initialize()
                instr = (init.instructions or "").lower()
                # 무엇인지: 병렬 코딩 에이전트 코디네이터
                assert "parallel" in instr
                assert "coordinat" in instr
                # 무엇이 *아닌지*: 정의/채택할 스키마·계약·object model 이 아님
                assert "not" in instr
                assert ("schema" in instr or "contract" in instr or "object model" in instr)
                # 자기-오리엔테이션 툴 노출
                tools = await session.list_tools()
                assert "about" in {tool.name for tool in tools.tools}

    anyio.run(run_smoke)


def test_about_tool_returns_orientation(tmp_path):
    pytest.importorskip("fastmcp")
    from omd_server.server import build_server, OMD_INSTRUCTIONS

    # instructions 가 핵심 disambiguation 을 담는지 (서버 빌드만으로 검증)
    text = OMD_INSTRUCTIONS.lower()
    assert "parallel" in text and "coordinat" in text
    assert "not" in text and ("object model" in text or "schema" in text or "contract" in text)
    mcp = build_server(str(tmp_path / "s.db"))
    assert mcp is not None


def test_server_stdio_does_not_take_singleton_leader(tmp_path):
    pytest.importorskip("fastmcp")
    pytest.importorskip("mcp")
    anyio = pytest.importorskip("anyio")

    from mcp.client.session import ClientSession
    from mcp.client.stdio import StdioServerParameters, stdio_client

    db = tmp_path / "s.db"

    async def run_smoke():
        params = StdioServerParameters(
            command=sys.executable,
            args=["-m", "omd_server.server", str(db)],
            cwd=str(tmp_path),
        )
        async with stdio_client(params) as (read_stream, write_stream):
            async with ClientSession(
                read_stream,
                write_stream,
                read_timeout_seconds=timedelta(seconds=5),
            ) as session:
                init = await session.initialize()
                assert init.serverInfo.name == "omd"

    anyio.run(run_smoke)

    con = sqlite3.connect(db)
    raw = con.execute("SELECT value FROM meta WHERE key='leader_lease'").fetchone()
    assert raw is None


def test_server_stdio_allows_concurrent_clients_on_same_db(tmp_path):
    pytest.importorskip("fastmcp")
    pytest.importorskip("mcp")
    anyio = pytest.importorskip("anyio")

    from mcp.client.session import ClientSession
    from mcp.client.stdio import StdioServerParameters, stdio_client

    db = tmp_path / "s.db"

    async def connect_and_list(delay: float):
        await anyio.sleep(delay)
        params = StdioServerParameters(
            command=sys.executable,
            args=["-m", "omd_server.server", str(db)],
            cwd=str(tmp_path),
        )
        async with stdio_client(params) as (read_stream, write_stream):
            async with ClientSession(
                read_stream,
                write_stream,
                read_timeout_seconds=timedelta(seconds=5),
            ) as session:
                init = await session.initialize()
                tools = await session.list_tools()
                assert init.serverInfo.name == "omd"
                assert {"claim", "release", "status"} <= {tool.name for tool in tools.tools}
                await anyio.sleep(0.2)

    async def run_smoke():
        async with anyio.create_task_group() as tg:
            tg.start_soon(connect_and_list, 0.0)
            tg.start_soon(connect_and_list, 0.05)

    anyio.run(run_smoke)
