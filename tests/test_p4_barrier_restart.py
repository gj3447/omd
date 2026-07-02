"""P4 — §3.D 배리어-bound 재기동 단위복구 + TRIPPED→CONSUMED 수거 동사 (증분11).

CONCURRENCY §D5 deviation 3/4 가 자백한 부채를 닫는다:
  INV-P4-BR1 (전진수정): TRIPPING 중 크래시했는데 git 진실상 **전 멤버가 이미 MERGED** 면
      재기동 복구가 배리어를 TRIPPED 로 전진수정한다(반쪽 신호 없음).
  INV-P4-BR2 (부분트립 fail-loud): 일부만 MERGED 인 채 크래시면 재기동 복구가 배리어를
      **BROKEN(reason=coordinator_crash_partial_trip)** 으로 — "BROKEN 신호 없이 반쪽 MERGED"
      함정(§3.D)이 닫힌다. 이미 MERGED 인 task 는 단조 사실로 유지(비가역), 미응결 task 는
      task-단위 복구가 재시도 가능 상태로 되돌린다.
  INV-P4-BR3 (무해): 건강한 ARMED 배리어는 재기동이 건드리지 않는다.
  INV-P4-C1  (수거): barrier_consume 이 TRIPPED→CONSUMED + 멤버별 merge_sha 수거.
      비-TRIPPED(ARMED/TRIPPING/BROKEN) 거부, CONSUMED 재호출은 멱등 noop.
"""
import subprocess
from pathlib import Path

import pytest

from omd_server import Coordinator

COORD = "restart-sim-p4"


def _git(args, cwd):
    subprocess.run(["git", *args], cwd=str(cwd), check=True, capture_output=True, text=True)


def _init_repo(root: Path):
    root.mkdir()
    _git(["init", "-b", "main"], root)
    _git(["config", "user.name", "t"], root)
    _git(["config", "user.email", "t@t"], root)
    (root / "README.md").write_text("base\n")
    _git(["add", "-A"], root)
    _git(["commit", "-m", "base"], root)
    _git(["checkout", "-b", "dev"], root)


def _mk(tmp_path, **kw):
    repo = tmp_path / "repo"
    if not repo.exists():
        _init_repo(repo)
    return Coordinator(db_path=str(tmp_path / "o.db"), repo=str(repo),
                       worktrees_dir=str(tmp_path / "wt"), integration_branch="main",
                       coordinator_id=COORD, **kw)


def _develop(omd, task, sub):
    """task 를 자기 worktree 에서 완전 개발(claim→start→write→commit→finish). fence 반환."""
    omd.declare(task, writes=[f"{sub}/**"])
    omd.next_task(f"ag{task}")
    r = omd.claim(f"ag{task}", [f"{sub}/**"], task_id=task)
    s = omd.start(task, f"ag{task}")
    d = Path(s["worktree"]) / sub
    d.mkdir(parents=True)
    (d / "f.py").write_text(f"{task} = 1\n")
    assert omd.commit(task, f"feat {task}")["ok"] is True
    omd.finish(task)
    return r["fence"]


def _arm_two(omd, name="rc"):
    fa = _develop(omd, "A", "a")
    fb = _develop(omd, "B", "b")
    assert omd.barrier_declare(name, ["A", "B"], timeout=600.0)["ok"] is True
    return fa, fb


class _Crash(RuntimeError):
    pass


# ---------------------------------------------------------------------------
# INV-P4-BR1 — 전 멤버 MERGED 후 크래시 → 재기동이 TRIPPED 로 전진수정
# ---------------------------------------------------------------------------


def test_restart_forward_completes_fully_merged_trip(tmp_path, monkeypatch):
    omd = _mk(tmp_path)
    fa, fb = _arm_two(omd)

    # 크래시 주입: 트립이 전 task 를 응결한 뒤 배리어를 TRIPPED 로 표기하기 *직전* 사망.
    real = omd.store.set_barrier

    def dying(barrier_id, **kw):
        if kw.get("state") == "TRIPPED":
            raise _Crash("process died before marking TRIPPED")
        return real(barrier_id, **kw)

    monkeypatch.setattr(omd.store, "set_barrier", dying)
    omd.barrier_arrive("rc", "agA", "A", fence=fa)
    with pytest.raises(_Crash):
        omd.barrier_arrive("rc", "agB", "B", fence=fb)          # 마지막 도착 → trip → 사망

    assert omd.store.get_task("A")["state"] == "MERGED"
    assert omd.store.get_task("B")["state"] == "MERGED"
    assert omd.store.barrier_by_name("rc")["state"] == "TRIPPING", "크래시 잔해"

    omd2 = _mk(tmp_path)                                         # 재기동(같은 db/coordinator_id)
    st = omd2.barrier_status("rc")
    assert st["state"] == "TRIPPED", (
        f"전 멤버 MERGED — 재기동 복구는 배리어를 TRIPPED 로 전진수정해야: {st}")


# ---------------------------------------------------------------------------
# INV-P4-BR2 — 부분트립 크래시 → 재기동이 BROKEN 으로 fail-loud (§3.D 함정 폐쇄)
# ---------------------------------------------------------------------------


def test_restart_breaks_partially_tripped_barrier_fail_loud(tmp_path, monkeypatch):
    omd = _mk(tmp_path)
    fa, fb = _arm_two(omd)

    # 크래시 주입: plan 의 1번째 task 응결 후, 2번째 응결 *직전* 사망.
    real = omd._barrier_connect_one
    calls = {"n": 0}

    def dying(task_id, expected_fence):
        calls["n"] += 1
        if calls["n"] >= 2:
            raise _Crash("process died mid-plan")
        return real(task_id, expected_fence)

    monkeypatch.setattr(omd, "_barrier_connect_one", dying)
    omd.barrier_arrive("rc", "agA", "A", fence=fa)
    with pytest.raises(_Crash):
        omd.barrier_arrive("rc", "agB", "B", fence=fb)

    merged = [t for t in ("A", "B") if omd.store.get_task(t)["state"] == "MERGED"]
    assert len(merged) == 1, f"정확히 한 task 만 응결된 반쪽 상태여야: {merged}"
    assert omd.store.barrier_by_name("rc")["state"] == "TRIPPING"

    omd2 = _mk(tmp_path)                                         # 재기동
    st = omd2.barrier_status("rc")
    assert st["state"] == "BROKEN", (
        f"반쪽 트립은 침묵 금지 — BROKEN 으로 fail-loud 해야(§3.D): {st}")
    assert "crash" in (st["break_reason"] or ""), st
    # 이미 MERGED 는 단조 사실로 유지, 미응결은 재시도 가능(MERGED 아님).
    assert omd2.store.get_task(merged[0])["state"] == "MERGED"
    other = ({"A", "B"} - set(merged)).pop()
    assert omd2.store.get_task(other)["state"] != "MERGED"


# ---------------------------------------------------------------------------
# INV-P4-BR3 — 건강한 ARMED 배리어는 재기동 무해
# ---------------------------------------------------------------------------


def test_restart_leaves_healthy_armed_barrier_untouched(tmp_path):
    omd = _mk(tmp_path)
    fa, _fb = _arm_two(omd)
    omd.barrier_arrive("rc", "agA", "A", fence=fa)               # 부분 도착(대기중)

    omd2 = _mk(tmp_path)                                         # 재기동
    st = omd2.barrier_status("rc")
    assert st["state"] == "ARMED" and st["arrived"] == 1 and st["parties"] == 2, st


# ---------------------------------------------------------------------------
# INV-P4-C1 — barrier_consume: TRIPPED→CONSUMED 수거 동사
# ---------------------------------------------------------------------------


def test_consume_collects_merge_shas_and_is_idempotent(tmp_path):
    omd = _mk(tmp_path)
    fa, fb = _arm_two(omd)
    omd.barrier_arrive("rc", "agA", "A", fence=fa)
    r = omd.barrier_arrive("rc", "agB", "B", fence=fb)
    assert r["state"] == "TRIPPED", r

    c = omd.barrier_consume("rc", "agA")
    assert c["ok"] is True and c["state"] == "CONSUMED", c
    shas = {x["task_id"]: x["merge_sha"] for x in c["results"]}
    assert set(shas) == {"A", "B"} and all(shas.values()), (
        f"멤버별 merge_sha 를 수거해야: {c}")
    assert shas["A"] == omd.store.get_task("A")["merge_sha"]
    assert omd.barrier_status("rc")["state"] == "CONSUMED"

    c2 = omd.barrier_consume("rc", "agA")                        # 멱등 noop
    assert c2["ok"] is True and c2.get("noop") is True and c2["state"] == "CONSUMED"


def test_consume_rejected_unless_tripped(tmp_path):
    omd = _mk(tmp_path)
    fa, _fb = _arm_two(omd)
    r = omd.barrier_consume("rc", "agA")                         # ARMED — 수거할 결과 없음
    assert r["ok"] is False and "TRIPPED" in r["reason"], r

    omd.barrier_abort("rc", "agA")                               # → BROKEN
    r = omd.barrier_consume("rc", "agA")
    assert r["ok"] is False and r.get("state") == "BROKEN", r

    r = omd.barrier_consume("ghost", "agA")                      # 미지 배리어
    assert r["ok"] is False and "no such barrier" in r["reason"], r
