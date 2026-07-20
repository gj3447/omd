"""W2 — coord DB terminal 행 GC 변환-불변식.

retention 경과 terminal task/orbit 은 archive 로 *이동*(supersession, 삭제 아님) /
live task 가 deps 로 참조하는 terminal dep 은 잔류 / 0=off / 멱등 / counts 정확 /
기존 sweep(만료 회수→promote) 비회귀.
"""
import time

import pytest

from omd_server import Coordinator
from omd_server.store import ORBIT_TERMINAL_STATES, TASK_TERMINAL_STATES


def _mk(**kw):
    kw.setdefault("allow_memory_db", True)
    return Coordinator(**kw)


def _backdate_task(store, task_id, terminal_at):
    store.db.execute(
        "UPDATE tasks SET terminal_at=? WHERE task_id=?", (terminal_at, task_id))


def _backdate_orbit(store, orbit_id, terminal_at):
    store.db.execute(
        "UPDATE orbits SET terminal_at=? WHERE orbit_id=?", (terminal_at, orbit_id))


def _count(store, table):
    return store.db.execute(f"SELECT COUNT(*) AS n FROM {table}").fetchone()["n"]


def test_fsm_terminal_states_ground_truth():
    """추정 금지 — fsm.py 전이표에서 terminal 집합을 실확인(나가는 전이가 live 로
    복귀하는 상태는 DONE/CONNECTING 등 워크플로 상태뿐)."""
    from omd_server import fsm
    orbit_sources = set()
    for t in fsm.ORBIT_TRANSITIONS:
        src = t["source"]
        orbit_sources.update([src] if isinstance(src, str) else src)
    # orbit terminal = 나가는 전이가 전혀 없는 상태
    assert set(ORBIT_TERMINAL_STATES) == set(fsm.ORBIT_STATES) - orbit_sources
    # task terminal(휴면 종단) 은 fsm 상태 집합의 부분집합이며 진행중 상태를 안 품는다
    assert set(TASK_TERMINAL_STATES) <= set(fsm.TASK_STATES)
    assert not set(TASK_TERMINAL_STATES) & {
        "PENDING", "BLOCKED", "READY", "CLAIMED", "IN_ORBIT", "DONE", "CONNECTING"}


def test_terminal_rows_moved_after_retention():
    """happy: retention 경과 terminal task/orbit 은 archive 로 이동(원본 컬럼 보존)."""
    omd = _mk(terminal_retention=3600.0)
    omd.declare("t1", writes=["a/**"])
    omd.cancel("t1")                                      # PENDING→ABORTED (휴면 종단)
    c = omd.claim("agA", ["b/**"], "write")
    omd.release(c["orbit_id"], "agA", c["fence"])         # HELD→RELEASED
    omd.sweep()                                           # 첫 sweep = lazy-stamp
    assert omd.store.get_task("t1") is not None           # 방금 종단 → 아직 잔류
    assert omd.store.get_orbit(c["orbit_id"]) is not None
    old = time.time() - 7200                              # retention 2배 과거
    _backdate_task(omd.store, "t1", old)
    _backdate_orbit(omd.store, c["orbit_id"], old)
    omd.sweep()
    assert omd.store.get_task("t1") is None               # live 에서 사라짐
    assert omd.store.get_orbit(c["orbit_id"]) is None
    ta = omd.store.db.execute(
        "SELECT * FROM tasks_archive WHERE task_id='t1'").fetchone()
    oa = omd.store.db.execute(
        "SELECT * FROM orbits_archive WHERE orbit_id=?", (c["orbit_id"],)).fetchone()
    assert ta is not None and ta["state"] == "ABORTED"    # 원본 컬럼 보존(이동)
    assert ta["writes"] == '["a/**"]' and ta["archived_at"] is not None
    assert oa is not None and oa["state"] == "RELEASED"
    assert oa["fence"] == c["fence"] and oa["archived_at"] is not None


def test_dep_referenced_terminal_task_survives():
    """정합성 절대조건: 살아있는 task 가 deps 로 참조하는 terminal(MERGED) dep 은
    retention 이 지나도 아카이브 금지 — deps 판정(state=='MERGED' 읽기)이 깨지면 안 됨."""
    omd = _mk(terminal_retention=3600.0)
    omd.declare("dep1", writes=["a/**"])
    old = time.time() - 7200
    omd.store.db.execute(
        "UPDATE tasks SET state='MERGED', merged_at=? WHERE task_id='dep1'", (old,))
    omd.declare("t2", writes=["b/**"], deps=["dep1"])
    omd.sweep()   # dep1 의 terminal_at=merged_at(과거) 스탬프 — 그래도 live 참조라 잔류
    omd.sweep()
    assert omd.store.get_task("dep1") is not None
    # deps 판정 비회귀: dep1(MERGED) 덕에 t2 가 READY 로 뽑힌다
    nt = omd.next_task("agX")
    assert nt is not None and nt["task_id"] == "t2"
    # t2 도 종결되면 dep1 은 더 이상 live 참조가 아님 → 둘 다 이동 가능
    omd.cancel("t2")
    omd.sweep()                                           # t2 lazy-stamp
    _backdate_task(omd.store, "t2", old)
    omd.sweep()
    assert omd.store.get_task("dep1") is None
    assert omd.store.get_task("t2") is None
    assert _count(omd.store, "tasks_archive") == 2


def test_live_task_orbit_not_archived():
    """정합성: 살아있는 task 소속의 terminal orbit 은 배제, task 종결 후엔 이동."""
    omd = _mk(terminal_retention=3600.0)
    omd.declare("t1", writes=["a/**"])
    c = omd.claim("agA", ["a/**"], "write", task_id="t1")
    omd.release(c["orbit_id"], "agA", c["fence"])         # orbit 은 RELEASED, task 는 live
    old = time.time() - 7200
    _backdate_orbit(omd.store, c["orbit_id"], old)
    omd.sweep()
    assert omd.store.get_orbit(c["orbit_id"]) is not None  # live task 참조 → 잔류
    omd.cancel("t1")                                       # task 종결
    omd.sweep()                                            # task lazy-stamp
    _backdate_task(omd.store, "t1", old)
    omd.sweep()
    assert omd.store.get_task("t1") is None
    assert omd.store.get_orbit(c["orbit_id"]) is None
    assert _count(omd.store, "orbits_archive") == 1


def test_zero_retention_is_off():
    """0=off: 아무리 오래된 terminal 행도 안 움직인다(스탬프도 안 박음)."""
    omd = _mk(terminal_retention=0)
    assert omd.terminal_retention == 0.0
    omd.declare("t1", writes=["a/**"])
    omd.cancel("t1")
    _backdate_task(omd.store, "t1", time.time() - 10 * 604800)
    omd.sweep()
    assert omd.store.get_task("t1") is not None
    assert _count(omd.store, "tasks_archive") == 0


def test_default_retention_seven_days_keeps_fresh_rows(monkeypatch):
    """기본 604800(7일): 테스트/세션 내 fresh terminal 행은 절대 안 움직인다."""
    monkeypatch.delenv("OMD_TERMINAL_RETENTION", raising=False)
    omd = _mk()
    assert omd.terminal_retention == 604800.0
    omd.declare("t1", writes=["a/**"])
    omd.cancel("t1")
    omd.sweep()
    omd.sweep()
    assert omd.store.get_task("t1") is not None
    assert _count(omd.store, "tasks_archive") == 0


def test_env_fallback(monkeypatch):
    """env OMD_TERMINAL_RETENTION 폴백(인자 미지정 시) + 잘못된 값 fail-loud."""
    monkeypatch.setenv("OMD_TERMINAL_RETENTION", "120")
    assert _mk().terminal_retention == 120.0
    monkeypatch.setenv("OMD_TERMINAL_RETENTION", "0")
    assert _mk().terminal_retention == 0.0
    # 명시 인자가 env 보다 우선
    assert _mk(terminal_retention=60).terminal_retention == 60.0
    monkeypatch.setenv("OMD_TERMINAL_RETENTION", "nope")
    with pytest.raises(ValueError):
        _mk()
    monkeypatch.delenv("OMD_TERMINAL_RETENTION", raising=False)
    with pytest.raises(ValueError):
        _mk(terminal_retention=-1)


def test_idempotent_double_run_and_counts():
    """멱등 + counts 정확: gc_terminal 반환이 이동 행 수와 일치, 재실행은 0/무해."""
    omd = _mk(terminal_retention=3600.0)
    now = time.time()
    for tid in ("t1", "t2"):
        omd.declare(tid, writes=[f"{tid}/**"])
        omd.cancel(tid)
    c = omd.claim("agA", ["b/**"], "write")
    omd.release(c["orbit_id"], "agA", c["fence"])
    # backdate 는 모든 verb(내장 inline sweep 포함) 뒤에 — 아니면 verb 의 sweep 이 먼저 이동시킴
    for tid in ("t1", "t2"):
        _backdate_task(omd.store, tid, now - 7200)
    _backdate_orbit(omd.store, c["orbit_id"], now - 7200)
    r1 = omd.store.gc_terminal(now, 3600.0)
    assert r1 == {"tasks": 2, "orbits": 1}
    r2 = omd.store.gc_terminal(now, 3600.0)               # 두 번 돌려도 안전
    assert r2 == {"tasks": 0, "orbits": 0}
    assert _count(omd.store, "tasks_archive") == 2        # 중복 이동 없음
    assert _count(omd.store, "orbits_archive") == 1
    assert _count(omd.store, "tasks") == 0


def test_existing_sweep_behavior_not_regressed():
    """비회귀: terminal GC 켠 채로도 만료 lease 회수→대기 promote 가 그대로 동작하고,
    방금 EXPIRED 된 궤도는 (retention 미경과) 아카이브되지 않는다."""
    omd = _mk(terminal_retention=3600.0)
    held = omd.claim("agentA", ["src/a/**"], "write", ttl=0.05)
    waiting = omd.claim("agentC", ["src/a/**"], "write")
    assert waiting["state"] == "PENDING"
    time.sleep(0.08)
    omd.sweep()
    assert omd.store.get_orbit(held["orbit_id"])["state"] == "EXPIRED"
    assert omd.store.get_orbit(waiting["orbit_id"])["state"] == "HELD"
    assert _count(omd.store, "orbits_archive") == 0
