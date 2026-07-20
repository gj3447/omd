"""W1 — 서버 lifecycle watchdog (부모 사망 + idle timeout).

2026-07-19 실사고 수리: 죽은 클라이언트가 남긴 orphan omd_server 들이 SQLite lock
경합으로 신규 호출을 무한 hang 시켰다. 판정 로직은 clock/getppid 주입으로 결정론
단위 테스트, 자기종료는 실프로세스(중간 부모 사망) 통합 테스트로 증명한다.
"""

from __future__ import annotations

import os
import subprocess
import sys
import textwrap
import threading
import time
from pathlib import Path

import pytest

from omd_server.server import (
    DEFAULT_WATCHDOG_POLL,
    LifecycleWatchdog,
    _parse_idle_timeout,
    _parse_watchdog_enabled,
    _parse_watchdog_poll,
    _watchdog_loop,
    _watchdog_shutdown,
)

_ROOT = str(Path(__file__).resolve().parents[1])


# ---------------------------------------------------------------- env parsing

@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        (None, True),
        ("", True),
        ("  ", True),
        ("1", True),
        ("yes", True),
        ("0", False),
        (" 0 ", False),
    ],
)
def test_watchdog_enabled_contract(raw, expected):
    """기본 ON — 정확한 "0" 만 opt-out."""
    assert _parse_watchdog_enabled(raw) is expected


@pytest.mark.parametrize(
    ("raw", "expected"),
    [(None, DEFAULT_WATCHDOG_POLL), ("", DEFAULT_WATCHDOG_POLL),
     ("  ", DEFAULT_WATCHDOG_POLL), ("0.2", 0.2), ("10", 10.0)],
)
def test_watchdog_poll_contract(raw, expected):
    assert _parse_watchdog_poll(raw) == expected


@pytest.mark.parametrize("raw", ["0", "-1", "nan", "inf", "not-a-number"])
def test_invalid_watchdog_poll_rejected(raw):
    with pytest.raises(ValueError, match="OMD_WATCHDOG_POLL"):
        _parse_watchdog_poll(raw)


@pytest.mark.parametrize(
    ("raw", "expected"),
    [(None, 0.0), ("", 0.0), ("  ", 0.0), ("0", 0.0), ("0.5", 0.5), ("300", 300.0)],
)
def test_idle_timeout_contract(raw, expected):
    assert _parse_idle_timeout(raw) == expected


@pytest.mark.parametrize("raw", ["-1", "nan", "inf", "-inf", "not-a-number"])
def test_invalid_idle_timeout_rejected(raw):
    with pytest.raises(ValueError, match="OMD_IDLE_TIMEOUT"):
        _parse_idle_timeout(raw)


@pytest.mark.parametrize(
    ("env", "raw"),
    [("OMD_WATCHDOG_POLL", "-1"), ("OMD_IDLE_TIMEOUT", "not-a-number")],
)
def test_invalid_watchdog_env_fails_before_db_creation(tmp_path, monkeypatch, env, raw):
    pytest.importorskip("fastmcp")
    from omd_server.server import build_server

    db = tmp_path / "invalid-watchdog-env.db"
    monkeypatch.setenv(env, raw)
    with pytest.raises(ValueError, match=env):
        build_server(str(db))
    assert not db.exists()


# ---------------------------------------------------------- decision logic

class _FakeClock:
    def __init__(self, start=100.0):
        self.now = start

    def __call__(self):
        return self.now


def test_check_none_while_parent_alive_and_not_idle():
    clock = _FakeClock()
    wd = LifecycleWatchdog(idle_timeout=30.0, clock=clock, getppid=lambda: 42)
    assert wd.check() is None
    clock.now += 29.9
    assert wd.check() is None


def test_check_parent_death_on_ppid_change():
    ppid = {"value": 42}
    wd = LifecycleWatchdog(clock=_FakeClock(), getppid=lambda: ppid["value"])
    assert wd.check() is None
    ppid["value"] = 4242   # 클라이언트 사망 → subreaper 로 reparent
    assert wd.check() == "parent_death"


def test_check_parent_death_when_reparented_to_init():
    ppid = {"value": 42}
    wd = LifecycleWatchdog(clock=_FakeClock(), getppid=lambda: ppid["value"])
    ppid["value"] = 1      # 클라이언트 사망 → init/launchd 로 reparent
    assert wd.check() == "parent_death"


def test_check_parent_death_when_already_orphaned_at_start():
    # 스포너가 watchdog 기록 전에 죽은 race — initial ppid==1 도 즉시 트리거.
    wd = LifecycleWatchdog(clock=_FakeClock(), getppid=lambda: 1)
    assert wd.check() == "parent_death"


def test_check_idle_timeout_fires_and_touch_resets():
    clock = _FakeClock()
    wd = LifecycleWatchdog(idle_timeout=30.0, clock=clock, getppid=lambda: 42)
    clock.now += 30.0
    assert wd.check() == "idle_timeout"
    wd.touch()
    assert wd.check() is None
    clock.now += 30.0
    assert wd.check() == "idle_timeout"


def test_idle_timeout_zero_never_fires():
    clock = _FakeClock()
    wd = LifecycleWatchdog(idle_timeout=0.0, clock=clock, getppid=lambda: 42)
    clock.now += 1e9
    assert wd.check() is None


def test_armed_reflects_either_axis():
    kw = {"clock": _FakeClock(), "getppid": lambda: 42}
    assert LifecycleWatchdog(**kw).armed is True
    assert LifecycleWatchdog(parent_death=False, **kw).armed is False
    assert LifecycleWatchdog(parent_death=False, idle_timeout=5.0, **kw).armed is True


def test_watchdog_loop_invokes_shutdown_once_then_returns():
    class _Scripted:
        def __init__(self, results):
            self.results = list(results)

        def check(self):
            return self.results.pop(0)

    reasons = []
    _watchdog_loop(_Scripted([None, None, "parent_death"]),
                   threading.Event(), 0.001, reasons.append)
    assert reasons == ["parent_death"]


def test_watchdog_loop_stops_cleanly_on_stop_event():
    stop = threading.Event()
    stop.set()
    _watchdog_loop(None, stop, 0.001, pytest.fail)   # check() 호출 없이 즉시 반환


# ------------------------------------------------------------- shutdown path

def _coordinator(tmp_path, name):
    from omd_server.core import Coordinator

    return Coordinator(
        str(tmp_path / name),
        agent_ttl=None,
        enforce_single_coordinator=False,
        sweep_interval=None,
        autostart_background_workers=False,
    )


def test_watchdog_shutdown_reuses_close_path_and_exits_zero(tmp_path):
    omd = _coordinator(tmp_path, "wd-shutdown.db")
    codes = []
    _watchdog_shutdown(omd, "parent_death", effect_grace=0.0, _exit=codes.append)
    assert codes == [0]
    omd.close()   # close 는 idempotent — lifespan finally 와의 이중 진입도 안전


def test_watchdog_shutdown_waits_for_inflight_connect_effect(tmp_path):
    """_connect_effect(=fenced merge 스코프) 가 살아있는 동안엔 종료를 미룬다."""
    omd = _coordinator(tmp_path, "wd-inflight.db")
    entered, release = threading.Event(), threading.Event()

    def hold():
        with omd._connect_effect(blocking=True) as acquired:
            assert acquired
            entered.set()
            release.wait(10.0)

    holder = threading.Thread(target=hold, daemon=True)
    holder.start()
    assert entered.wait(5.0)

    codes, waits = [], []

    def _sleep(seconds):
        # 폴링 진입 = in-flight 감지 증거. 여기서 merge 를 "끝내" 주면
        # shutdown 이 락을 획득한 뒤에야 종료해야 한다.
        waits.append(seconds)
        release.set()
        holder.join(5.0)

    _watchdog_shutdown(omd, "idle_timeout", effect_grace=10.0,
                       _exit=codes.append, sleep=_sleep)
    assert waits, "in-flight effect 를 폴링으로 기다리지 않았다"
    assert codes == [0]
    assert not holder.is_alive()


def test_watchdog_shutdown_is_bounded_when_effect_outlives_grace(tmp_path):
    """grace 초과 merge 는 기다림을 포기하고 종료한다 — durable CONNECTING +
    kernel-release flock + 재기동 rollback(tests/test_m1_connect_effect_process.py)
    이 안전 근거."""
    omd = _coordinator(tmp_path, "wd-grace.db")
    entered, release = threading.Event(), threading.Event()

    def hold():
        with omd._connect_effect(blocking=True) as acquired:
            assert acquired
            entered.set()
            release.wait(10.0)

    holder = threading.Thread(target=hold, daemon=True)
    holder.start()
    assert entered.wait(5.0)
    try:
        codes = []
        _watchdog_shutdown(omd, "parent_death", effect_grace=0.0,
                           _exit=codes.append)
        assert codes == [0]
        assert holder.is_alive(), "grace=0 인데 effect 종료를 기다렸다"
    finally:
        release.set()
        holder.join(5.0)


# ------------------------------------------------------- middleware / lifespan

def test_activity_middleware_touches_before_and_after_tool_call():
    anyio = pytest.importorskip("anyio")
    pytest.importorskip("fastmcp")
    from omd_server.server import _ActivityMiddleware

    touches = []
    mw = _ActivityMiddleware(lambda: touches.append(1))

    async def call_next(context):
        assert len(touches) == 1   # 호출 전 touch
        return "result"

    assert anyio.run(mw.on_call_tool, None, call_next) == "result"
    assert len(touches) == 2       # 긴 tool 실행이 idle 로 오산되지 않게 종료 후도 touch

    async def failing(context):
        raise RuntimeError("boom")

    with pytest.raises(RuntimeError, match="boom"):
        anyio.run(mw.on_call_tool, None, failing)
    assert len(touches) == 4       # 실패 경로에서도 양쪽 touch


def _watchdog_thread_alive():
    return any(
        t.name == "omd-lifecycle-watchdog" and t.is_alive()
        for t in threading.enumerate()
    )


def test_lifespan_starts_and_stops_watchdog_thread(tmp_path):
    anyio = pytest.importorskip("anyio")
    pytest.importorskip("fastmcp")
    from omd_server.server import _coordinator_lifespan

    omd = _coordinator(tmp_path, "wd-lifespan.db")
    wd = LifecycleWatchdog(getppid=lambda: 42)   # 안정 ppid — 트리거 없음

    async def enter_and_exit():
        async with _coordinator_lifespan(omd, watchdog=wd, watchdog_poll=5.0)(None):
            assert _watchdog_thread_alive()

    anyio.run(enter_and_exit)
    assert not _watchdog_thread_alive()


def test_lifespan_without_watchdog_starts_no_thread(tmp_path):
    anyio = pytest.importorskip("anyio")
    pytest.importorskip("fastmcp")
    from omd_server.server import _coordinator_lifespan

    omd = _coordinator(tmp_path, "wd-off.db")

    async def enter_and_exit():
        async with _coordinator_lifespan(omd, watchdog=None)(None):
            assert not _watchdog_thread_alive()

    anyio.run(enter_and_exit)


def test_build_server_disarms_watchdog_only_when_both_axes_off(tmp_path, monkeypatch):
    """OMD_WATCHDOG=0 + idle off ⇒ lifespan 에 watchdog 없음(스레드 0)."""
    anyio = pytest.importorskip("anyio")
    pytest.importorskip("fastmcp")
    from omd_server import server as server_module

    class FakeMCP:
        def __init__(self, *args, **kwargs):
            self.lifespan = kwargs["lifespan"]

        def tool(self):
            return lambda function: function

    monkeypatch.setattr(server_module, "FastMCP", FakeMCP)
    monkeypatch.setenv("OMD_WATCHDOG", "0")
    mcp = server_module.build_server(str(tmp_path / "wd-disarmed.db"))

    async def enter_and_exit():
        async with mcp.lifespan(None):
            assert not _watchdog_thread_alive()

    anyio.run(enter_and_exit)


# ----------------------------------------------------- real-process integration

_INTERMEDIATE_PARENT = r"""
    import os
    import subprocess
    import sys
    import time
    from pathlib import Path

    root, db, pidfile, errlog = sys.argv[1:5]
    # 2026-07-19 orphan 사고의 fd-보존 조건 재현: 파이프 write end 를 서버 자신에게
    # 상속시켜(pass_fds) 부모가 죽어도 서버 stdin 이 EOF 되지 않게 한다.
    # 따라서 이 서버를 끝낼 수 있는 것은 stdin EOF 가 아니라 watchdog 뿐이다.
    r, w = os.pipe()
    env = dict(os.environ, OMD_WATCHDOG_POLL="0.2")
    with open(errlog, "w") as err:
        proc = subprocess.Popen(
            [sys.executable, "-m", "omd_server.server", db],
            cwd=root, stdin=r, stdout=subprocess.DEVNULL, stderr=err,
            pass_fds=(w,), env=env,
        )
    Path(pidfile).write_text(str(proc.pid))
    # 서버가 initial ppid(=이 프로세스)를 기록하고 부팅을 마칠 때까지 산 채로 대기 —
    # build_server 는 ppid 기록 *후* DB 를 만들므로 DB 파일 출현이 그 증거다.
    # 그래야 사망이 '기동 중 고아'가 아니라 '운행 중 reparent' 경로를 밟는다.
    deadline = time.monotonic() + 20.0
    while not Path(db).exists():
        if proc.poll() is not None:
            raise SystemExit(f"server died during boot rc={proc.returncode}")
        if time.monotonic() >= deadline:
            raise SystemExit("server did not create its db in time")
        time.sleep(0.05)
    # 중간 부모는 여기서 즉시 죽는다 → 서버는 init/launchd 로 reparent.
"""


def _pid_alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    return True


@pytest.mark.skipif(os.name != "posix", reason="requires POSIX reparenting semantics")
def test_orphaned_server_self_terminates_within_deadline(tmp_path):
    """중간 부모가 서버를 spawn 하고 죽으면 서버가 ~15초 내 watchdog 으로 자기종료."""
    pytest.importorskip("fastmcp")
    script = tmp_path / "intermediate_parent.py"
    script.write_text(textwrap.dedent(_INTERMEDIATE_PARENT))
    pidfile = tmp_path / "server.pid"
    errlog = tmp_path / "server.stderr"

    result = subprocess.run(
        [sys.executable, str(script), _ROOT, str(tmp_path / "orphan.db"),
         str(pidfile), str(errlog)],
        cwd=_ROOT, capture_output=True, text=True, timeout=30,
    )
    assert result.returncode == 0, (
        f"intermediate parent failed: stdout={result.stdout!r} stderr={result.stderr!r}"
    )
    pid = int(pidfile.read_text().strip())
    try:
        deadline = time.monotonic() + 15.0
        while _pid_alive(pid):
            if time.monotonic() >= deadline:
                pytest.fail(
                    "orphaned server still alive after 15s; "
                    f"stderr={errlog.read_text()[-2000:]!r}"
                )
            time.sleep(0.1)
    finally:
        if _pid_alive(pid):
            os.kill(pid, 9)
    # 자기종료가 watchdog graceful 경로(close→exit)였음을 positive readback.
    assert "watchdog exit (parent_death)" in errlog.read_text()
