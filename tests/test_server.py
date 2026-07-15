"""FastMCP 서버 빌드 스모크 — fastmcp 설치 시 툴 스키마가 유효하게 구성되는지."""

import json
import sys
import sqlite3
import time
from datetime import timedelta

import pytest


def test_coordinator_lifespan_resigns_only_when_singleton_enforced():
    anyio = pytest.importorskip("anyio")
    pytest.importorskip("fastmcp")
    from omd_server.server import _coordinator_lifespan

    class _Coordinator:
        leader_ttl = 30.0

        def __init__(self, enforced):
            self.enforce_single_coordinator = enforced
            self.resign_calls = 0
            self.calls = []

        def coordinator_heartbeat(self):
            return {"ok": True}

        def resign(self):
            self.calls.append("resign")
            self.resign_calls += 1
            return {"ok": True}

        def start_background_workers(self, *, sweep_interval, heartbeat):
            self.calls.append(
                ("start_background_workers", sweep_interval, heartbeat)
            )
            return {"ok": True}

        def close(self):
            self.calls.append("close")

    async def enter_and_exit(omd):
        async with _coordinator_lifespan(omd)(None) as state:
            assert state == {"omd": omd}

    non_enforced = _Coordinator(False)
    anyio.run(enter_and_exit, non_enforced)
    assert non_enforced.resign_calls == 0
    assert non_enforced.calls == [
        ("start_background_workers", None, False),
        "close",
    ]

    enforced = _Coordinator(True)
    anyio.run(enter_and_exit, enforced)
    assert enforced.resign_calls == 1
    assert enforced.calls[0] == ("start_background_workers", None, True)
    assert enforced.calls[-2:] == ["close", "resign"]

    swept = _Coordinator(True)

    async def enter_swept():
        async with _coordinator_lifespan(swept, sweep_interval=0.25)(None):
            assert swept.calls == [
                ("start_background_workers", 0.25, True)
            ]

    anyio.run(enter_swept)
    assert swept.calls[-2:] == ["close", "resign"]


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        (None, 1.0),
        ("", 1.0),
        ("  ", 1.0),
        ("0", None),
        ("0.0", None),
        ("0.25", 0.25),
        ("5", 5.0),
    ],
)
def test_server_sweep_interval_contract(raw, expected):
    from omd_server.server import _parse_server_sweep_interval

    assert _parse_server_sweep_interval(raw) == expected


def test_build_server_defers_background_workers_until_lifespan(tmp_path, monkeypatch):
    pytest.importorskip("fastmcp")
    from omd_server import server as server_module

    created = []

    class SpyCoordinator:
        def __init__(self, *args, **kwargs):
            created.append((args, kwargs))

    class FakeMCP:
        def __init__(self, *args, **kwargs):
            self.lifespan = kwargs["lifespan"]

        def tool(self):
            return lambda function: function

    monkeypatch.setattr(server_module, "Coordinator", SpyCoordinator)
    monkeypatch.setattr(server_module, "FastMCP", FakeMCP)
    server_module.build_server(str(tmp_path / "deferred.db"))
    assert len(created) == 1
    assert created[0][1]["sweep_interval"] is None
    assert created[0][1]["autostart_background_workers"] is False


def test_build_server_defers_pending_outbox_effect_until_lifespan(
    tmp_path, monkeypatch
):
    anyio = pytest.importorskip("anyio")
    pytest.importorskip("fastmcp")
    from omd_server import server as server_module
    from omd_server.core import Coordinator

    db = tmp_path / "deferred-outbox.db"
    event_log = tmp_path / "events.jsonl"
    seed = Coordinator(
        str(db),
        agent_ttl=None,
        enforce_single_coordinator=False,
        sweep_interval=None,
        autostart_background_workers=False,
    )
    seed.claim("agent", ["src/**"], request_id="deferred-notification")
    seed.close()

    class FakeMCP:
        def __init__(self, *args, **kwargs):
            self.lifespan = kwargs["lifespan"]

        def tool(self):
            return lambda function: function

    def outbox_state():
        with sqlite3.connect(db) as conn:
            return conn.execute(
                "SELECT state FROM admission_outbox WHERE request_id=?",
                ("deferred-notification",),
            ).fetchone()[0]

    monkeypatch.setenv("OMD_EVENT_LOG", str(event_log))
    monkeypatch.setattr(server_module, "FastMCP", FakeMCP)
    mcp = server_module.build_server(str(db))
    time.sleep(0.10)
    assert outbox_state() == "PENDING"
    if event_log.exists():
        assert "admission_notification/v1" not in event_log.read_text()

    async def enter_lifespan():
        async with mcp.lifespan(None):
            with anyio.fail_after(2.0):
                while outbox_state() != "DELIVERED":
                    await anyio.sleep(0.01)

    anyio.run(enter_lifespan)
    assert outbox_state() == "DELIVERED"
    events = [json.loads(line) for line in event_log.read_text().splitlines()]
    assert any(
        event.get("notification_schema") == "admission_notification/v1"
        for event in events
    )


@pytest.mark.parametrize("raw", ["-1", "nan", "inf", "-inf", "not-a-number"])
def test_invalid_server_sweep_interval_fails_before_db_creation(
    tmp_path, monkeypatch, raw
):
    pytest.importorskip("fastmcp")
    from omd_server.server import build_server

    db = tmp_path / "invalid-sweep.db"
    monkeypatch.setenv("OMD_SWEEP_INTERVAL", raw)
    with pytest.raises(ValueError, match="OMD_SWEEP_INTERVAL"):
        build_server(str(db))
    assert not db.exists()


@pytest.mark.parametrize(
    ("raw", "expected"),
    [(None, 1024), ("", 1024), ("  ", 1024), ("0", 0), ("1", 1), ("4096", 4096)],
)
def test_server_admission_queue_capacity_contract(raw, expected):
    from omd_server.server import _parse_admission_queue_capacity

    assert _parse_admission_queue_capacity(raw) == expected


@pytest.mark.parametrize("raw", ["-1", "1.5", "nan", "inf", "not-a-number"])
def test_invalid_server_queue_capacity_fails_before_db_creation(
    tmp_path, monkeypatch, raw
):
    pytest.importorskip("fastmcp")
    from omd_server.server import build_server

    db = tmp_path / "invalid-capacity.db"
    monkeypatch.setenv("OMD_ADMISSION_QUEUE_CAPACITY", raw)
    with pytest.raises(ValueError, match="OMD_ADMISSION_QUEUE_CAPACITY"):
        build_server(str(db))
    assert not db.exists()


@pytest.mark.parametrize(
    ("raw", "expected"),
    [(None, 60.0), ("", 60.0), ("  ", 60.0), ("0.5", 0.5), ("120", 120.0)],
)
def test_server_admission_aging_quantum_contract(raw, expected):
    from omd_server.server import _parse_admission_aging_quantum

    assert _parse_admission_aging_quantum(raw) == expected


@pytest.mark.parametrize("raw", ["0", "-1", "nan", "inf", "not-a-number"])
def test_invalid_server_aging_quantum_fails_before_db_creation(
    tmp_path, monkeypatch, raw
):
    pytest.importorskip("fastmcp")
    from omd_server.server import build_server

    db = tmp_path / "invalid-aging-quantum.db"
    monkeypatch.setenv("OMD_ADMISSION_AGING_QUANTUM_SECONDS", raw)
    with pytest.raises(ValueError, match="OMD_ADMISSION_AGING_QUANTUM_SECONDS"):
        build_server(str(db))
    assert not db.exists()


@pytest.mark.parametrize(
    ("raw", "expected"),
    [(None, 10), ("", 10), ("  ", 10), ("0", 0), ("25", 25)],
)
def test_server_admission_max_age_boost_contract(raw, expected):
    from omd_server.server import _parse_admission_max_age_boost

    assert _parse_admission_max_age_boost(raw) == expected


@pytest.mark.parametrize(
    "raw", ["-1", "1.5", "nan", "inf", "not-a-number", str(1 << 63)]
)
def test_invalid_server_max_age_boost_fails_before_db_creation(
    tmp_path, monkeypatch, raw
):
    pytest.importorskip("fastmcp")
    from omd_server.server import build_server

    db = tmp_path / "invalid-max-age-boost.db"
    monkeypatch.setenv("OMD_ADMISSION_MAX_AGE_BOOST", raw)
    with pytest.raises(ValueError, match="OMD_ADMISSION_MAX_AGE_BOOST"):
        build_server(str(db))
    assert not db.exists()


def test_server_lifespan_sweep_delivers_idle_wait_deadline(tmp_path):
    anyio = pytest.importorskip("anyio")
    pytest.importorskip("fastmcp")
    from omd_server.core import Coordinator
    from omd_server.server import _coordinator_lifespan

    omd = Coordinator(
        str(tmp_path / "idle-deadline.db"),
        agent_ttl=None,
        admission_wait_timeout=0.04,
        enforce_single_coordinator=False,
        sweep_interval=None,
    )
    omd.claim("holder", ["a/**"])
    waiting = omd.claim("waiter", ["a/**"], request_id="idle-wait")

    async def wait_for_timeout():
        async with _coordinator_lifespan(omd, sweep_interval=0.01)(None):
            with anyio.fail_after(2.0):
                while omd.store.get_orbit(waiting["orbit_id"])["state"] == "PENDING":
                    await anyio.sleep(0.01)

    anyio.run(wait_for_timeout)
    row = omd.store.get_orbit(waiting["orbit_id"])
    assert row["state"] == "DENIED"
    assert row["decision_type"] == "WAIT_TIMEOUT"
    assert omd._sweep_thread is None


def test_server_lifespan_keeps_enforced_lease_when_sweep_is_opted_out(tmp_path):
    anyio = pytest.importorskip("anyio")
    pytest.importorskip("fastmcp")
    from omd_server.core import Coordinator, CoordinatorConflict
    from omd_server.server import _coordinator_lifespan

    db = str(tmp_path / "heartbeat-only-lifespan.db")
    omd = Coordinator(
        db,
        agent_ttl=None,
        leader_ttl=0.06,
        sweep_interval=None,
        autostart_background_workers=False,
    )

    async def hold_lifespan():
        async with _coordinator_lifespan(omd, sweep_interval=None)(None):
            await anyio.sleep(0.10)
            with pytest.raises(
                CoordinatorConflict, match="another live coordinator"
            ):
                Coordinator(
                    db,
                    agent_ttl=None,
                    coordinator_id="lifespan-takeover-probe",
                    leader_ttl=0.06,
                    sweep_interval=None,
                )

    anyio.run(hold_lifespan)
    with Coordinator(
        db,
        agent_ttl=None,
        coordinator_id="lifespan-next-owner",
        sweep_interval=None,
    ) as reopened:
        assert reopened.leader_epoch == 2


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


def test_cancel_wait_tool_exposes_and_forwards_authority_tuple(tmp_path, monkeypatch):
    anyio = pytest.importorskip("anyio")
    pytest.importorskip("fastmcp")
    from omd_server.core import Coordinator
    from omd_server.server import build_server

    mcp = build_server(str(tmp_path / "cancel-wait.db"))
    observed = {}

    def fake_cancel_wait(self, orbit_id, agent_id, request_generation, **kwargs):
        observed.update(
            orbit_id=orbit_id,
            agent_id=agent_id,
            request_generation=request_generation,
            **kwargs,
        )
        return {"ok": True, "state": "CANCELLED"}

    monkeypatch.setattr(Coordinator, "cancel_wait", fake_cancel_wait)

    async def inspect_and_call():
        tool = next(t for t in await mcp.list_tools() if t.name == "cancel_wait")
        assert {"orbit_id", "agent", "request_generation", "bail_epoch", "request_id"} <= set(
            tool.parameters["properties"]
        )
        assert {"orbit_id", "agent", "request_generation", "bail_epoch"} <= set(
            tool.parameters["required"]
        )
        result = tool.fn(
            orbit_id="orb-1",
            agent="worker",
            request_generation=4,
            bail_epoch=2,
            request_id="cancel-op",
        )
        assert result == {"ok": True, "state": "CANCELLED"}

    anyio.run(inspect_and_call)
    assert observed == {
        "orbit_id": "orb-1",
        "agent_id": "worker",
        "request_generation": 4,
        "bail_epoch": 2,
        "request_id": "cancel-op",
    }


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
                assert {"claim", "release", "cancel_wait", "status"} <= names

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
    # Fresh-schema migration briefly takes a fenced leader generation even in
    # stdio mode, then immediately resigns before serving concurrent clients.
    assert raw is not None
    assert json.loads(raw[0])["last_heartbeat"] == 0


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
