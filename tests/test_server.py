"""FastMCP 서버 빌드 스모크 — fastmcp 설치 시 툴 스키마가 유효하게 구성되는지."""

import pytest


def test_server_builds(tmp_path):
    pytest.importorskip("fastmcp")
    from omd_server.server import build_server
    mcp = build_server(str(tmp_path / "s.db"))
    assert mcp is not None
