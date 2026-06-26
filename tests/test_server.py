"""FastMCP 서버 빌드 스모크 — fastmcp 설치 시 툴 스키마가 유효하게 구성되는지."""

import sys
from datetime import timedelta

import pytest


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
