"""증분8 — D5 크래시 안전 배리어: 세대-스탬프 응결 랑데부 + BROKEN 종단.

§D5/§1.2/§3.D 의 핵심:
  - 배리어 = ARMED → TRIPPING → TRIPPED → CONSUMED  ⊕  (any) → BROKEN.
  - 멤버십은 agent 수가 아니라 **task 집합**(reclaim 으로 task 가 requeue 되면 N 재계산).
  - 참가자 사망(도착 전/후 모두)·타임아웃 → break → 도착해 있던 전원이 BROKEN 으로 기상
    (Java BrokenBarrierException / Python Barrier.abort 시맨틱). **영구 hang 불가.**
  - 응결 배리어는 내부 _barrier_connect_one(task, expected_fence)로 trip — 공개 connect() 를
    재호출하지 않는다(그 Phase A 의 _sweep_inline 이 방금 검증한 궤도를 재진입 만료시킴).
  - policy: break(전원 깸) | shrink(죽은 멤버 빼고 진행, 단 그 멤버 의존자 없을 때만).

정상경로 + 크래시/사망/오추방 실패경로 + fence/owner 거부를 직접 확인한다.
"""

import os
import subprocess
import threading
import time
from pathlib import Path

import pytest

from omd_server import Coordinator, Emitter

GATES = os.path.join(os.path.dirname(__file__), os.pardir, "gates")


# ---------- 헬퍼 ----------
def _ready(omd, task, sub):
    """DB-only: task 를 DONE 까지(claim→start→finish). write-orbit fence 반환."""
    omd.declare(task, writes=[f"{sub}/**"])
    omd.next_task(f"ag{task}")
    r = omd.claim(f"ag{task}", [f"{sub}/**"], task_id=task)
    omd.start(task, f"ag{task}")
    omd.finish(task)
    return r["fence"]


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


def _develop(omd, task, sub, fname, content):
    """git: task 를 자기 worktree 에서 완전 개발(claim→start→write→commit→finish)."""
    omd.declare(task, writes=[f"{sub}/**"])
    omd.next_task(f"ag{task}")
    r = omd.claim(f"ag{task}", [f"{sub}/**"], task_id=task)
    s = omd.start(task, f"ag{task}")
    (Path(s["worktree"]) / sub).mkdir(parents=True)
    (Path(s["worktree"]) / sub / fname).write_text(content)
    omd.commit(task, f"feat: {sub}/{fname}")
    omd.finish(task)
    return r["fence"]


# ========== 정상 경로 ==========
def test_declare_arm(tmp_path):
    omd = Coordinator(db_path=str(tmp_path / "omd.db"))
    _ready(omd, "A", "a"); _ready(omd, "B", "b")
    r = omd.barrier_declare("rv", ["A", "B"])
    assert r["ok"] and r["state"] == "ARMED" and r["parties"] == 2 and r["generation"] == 0


def test_declare_needs_tasks(tmp_path):
    omd = Coordinator(db_path=str(tmp_path / "omd.db"))
    assert omd.barrier_declare("rv", [])["ok"] is False


def test_arrive_unknown_barrier(tmp_path):
    omd = Coordinator(db_path=str(tmp_path / "omd.db"))
    _ready(omd, "A", "a")
    assert omd.barrier_arrive("nope", "agA", "A")["ok"] is False


def test_arrive_nonmember_rejected(tmp_path):
    omd = Coordinator(db_path=str(tmp_path / "omd.db"))
    _ready(omd, "A", "a"); _ready(omd, "B", "b"); _ready(omd, "C", "c")
    omd.barrier_declare("rv", ["A", "B"])
    r = omd.barrier_arrive("rv", "agC", "C")
    assert r["ok"] is False and "not a barrier member" in r["reason"]


def test_partial_arrival_waits(tmp_path):
    """일부만 도착하면 ARMED 유지(트립 안 함). register→poll 패턴."""
    omd = Coordinator(db_path=str(tmp_path / "omd.db"))
    _ready(omd, "A", "a"); _ready(omd, "B", "b")
    omd.barrier_declare("rv", ["A", "B"])
    r = omd.barrier_arrive("rv", "agA", "A")
    assert r["ok"] and r["state"] == "ARMED" and r["arrived"] == 1
    assert omd.store.get_task("A")["state"] == "DONE"  # 아직 응결 안 됨


def test_all_arrive_trips_and_merges_dbonly(tmp_path):
    """전원 도착 → trip → 전 task 응결(MERGED) → 배리어 TRIPPED."""
    omd = Coordinator(db_path=str(tmp_path / "omd.db"))
    _ready(omd, "A", "a"); _ready(omd, "B", "b")
    omd.barrier_declare("rv", ["A", "B"])
    omd.barrier_arrive("rv", "agA", "A")
    r = omd.barrier_arrive("rv", "agB", "B")
    assert r["ok"] and r["state"] == "TRIPPED" and set(r["merged"]) == {"A", "B"}
    assert omd.store.get_task("A")["state"] == "MERGED"
    assert omd.store.get_task("B")["state"] == "MERGED"


def test_all_arrive_trips_and_merges_git(tmp_path):
    """git 백엔드: 전원 도착 → 결정적 순서로 실제 merge → 통합 브랜치에 양쪽 파일 + clean index."""
    repo = tmp_path / "repo"
    _init_repo(repo)
    omd = Coordinator(db_path=str(tmp_path / "omd.db"), repo=str(repo),
                      worktrees_dir=str(tmp_path / "wt"), integration_branch="main")
    _develop(omd, "A", "a", "x.py", "x = 1\n")
    _develop(omd, "B", "b", "y.py", "y = 2\n")
    omd.barrier_declare("rv", ["A", "B"])
    omd.barrier_arrive("rv", "agA", "A")
    r = omd.barrier_arrive("rv", "agB", "B")
    assert r["state"] == "TRIPPED", r
    integ = Path(omd.integration_worktree)
    assert (integ / "a" / "x.py").exists() and (integ / "b" / "y.py").exists()
    st = subprocess.run(["git", "status", "--porcelain"], cwd=str(integ),
                        capture_output=True, text=True).stdout.strip()
    assert st == "", f"통합 worktree 더러움: {st!r}"
    assert omd.store.all_held_merge_tokens() == []  # 토큰 누수 0
    log = subprocess.run(["git", "log", "--oneline", "main"], cwd=str(repo),
                        capture_output=True, text=True).stdout
    assert "CLOUD CONNECT A" in log and "CLOUD CONNECT B" in log


def test_arrive_idempotent_request_id(tmp_path):
    """§D9: 같은 request_id 재시도(트립 후)는 두 번째 트립을 안 만든다(캐시 응답)."""
    omd = Coordinator(db_path=str(tmp_path / "omd.db"))
    _ready(omd, "A", "a"); _ready(omd, "B", "b")
    omd.barrier_declare("rv", ["A", "B"])
    omd.barrier_arrive("rv", "agA", "A")
    r1 = omd.barrier_arrive("rv", "agB", "B", request_id="req-1")
    r2 = omd.barrier_arrive("rv", "agB", "B", request_id="req-1")
    assert r1["state"] == "TRIPPED"
    assert r2.get("replayed") and r2["state"] == "TRIPPED"


# ========== fence/owner 거부 (§D6) ==========
def test_arrive_stale_fence_rejected(tmp_path):
    """arrive 가 stale fence 를 주면 fenced_out — 도착 표시 안 됨."""
    omd = Coordinator(db_path=str(tmp_path / "omd.db"))
    f = _ready(omd, "A", "a"); _ready(omd, "B", "b")
    omd.barrier_declare("rv", ["A", "B"])
    r = omd.barrier_arrive("rv", "agA", "A", fence=f + 999)
    assert r["ok"] is False and r.get("fenced_out")
    # 도착 표시 안 됨
    p = omd.store.get_barrier_party("bar-rv-0", 0, "A")
    assert p["arrived"] == 0


def test_arrive_with_released_lease_breaks(tmp_path):
    """write-orbit 이 HELD 가 아니면(lease 거둬짐=보유자 사망) 그 참가자는 죽은 것 → arrive 의
    내부 eval 이 BROKEN(participant_dead). 죽은 참가자가 끼어 트립되는 일 없음(영구 hang 0)."""
    omd = Coordinator(db_path=str(tmp_path / "omd.db"))
    _ready(omd, "A", "a"); _ready(omd, "B", "b")
    omd.barrier_declare("rv", ["A", "B"])
    # A 의 write-orbit 을 강제로 해제(lease 거둬짐 모사 — A 가 죽음)
    o = [x for x in omd.store.orbits_for_task("A") if x["mode"] == "write"][0]
    omd.store.set_orbit(o["orbit_id"], state="RELEASED")
    r = omd.barrier_arrive("rv", "agB", "B")    # 살아있는 B 가 도착 시도
    assert r["ok"] is False and r["state"] == "BROKEN"
    assert omd.barrier_status("rv")["break_reason"] == "participant_dead"


def test_reclaimed_zombie_cannot_arrive(tmp_path):
    """오추방/회수된 좀비는 arrive 못 함(bail_epoch/state 가드, §D6)."""
    omd = Coordinator(db_path=str(tmp_path / "omd.db"))
    _ready(omd, "A", "a"); _ready(omd, "B", "b")
    omd.barrier_declare("rv", ["A", "B"])
    omd.bail("agA")  # agA RETIRED
    r = omd.barrier_arrive("rv", "agA", "A")
    assert r.get("fenced_out"), r


# ========== 크래시/사망/오추방 실패경로 ==========
def test_participant_death_before_arrival_breaks(tmp_path):
    """참가자가 도착 **전** 죽으면(bail) → 배리어 BROKEN(participant_dead). 도착해 있던 전원 기상."""
    omd = Coordinator(db_path=str(tmp_path / "omd.db"))
    _ready(omd, "A", "a"); _ready(omd, "B", "b")
    omd.barrier_declare("rv", ["A", "B"])
    omd.barrier_arrive("rv", "agA", "A")        # A 도착, B 미도착
    omd.bail("agB")                              # B 가 도착 전 사망
    s = omd.barrier_status("rv")
    assert s["state"] == "BROKEN" and s["break_reason"] == "participant_dead"
    # 도착해 있던 A 도 다음 arrive/poll 에서 BROKEN 으로 기상(영구 hang 없음).
    assert omd.barrier_arrive("rv", "agA", "A")["state"] == "BROKEN"


def test_participant_death_after_arrival_breaks(tmp_path):
    """참가자가 도착 **후** 죽으면(lease 만료=GC-pause/사망) → BROKEN. 도착=arrived 였어도 깬다."""
    omd = Coordinator(db_path=str(tmp_path / "omd.db"))
    _ready(omd, "A", "a"); _ready(omd, "B", "b"); _ready(omd, "C", "c")
    omd.barrier_declare("rv", ["A", "B", "C"])
    omd.barrier_arrive("rv", "agA", "A")
    omd.barrier_arrive("rv", "agB", "B")        # A,B 도착, C 미도착
    omd.bail("agA")                              # 도착했던 A 가 사망
    s = omd.barrier_status("rv")
    assert s["state"] == "BROKEN" and s["break_reason"] == "participant_dead"


def test_zombie_reclaim_breaks_barrier(tmp_path):
    """비자발(kill -9 모사): heartbeat 끊긴 좀비 회수 → task requeue → 배리어 BROKEN. bail 과 수렴."""
    omd = Coordinator(db_path=str(tmp_path / "omd.db"), agent_ttl=0.03)
    _ready(omd, "A", "a"); _ready(omd, "B", "b")
    omd.barrier_declare("rv", ["A", "B"])
    omd.barrier_arrive("rv", "agA", "A")
    time.sleep(0.05)
    omd.reclaim_zombies()    # agA, agB 둘 다 heartbeat 끊김 → 회수
    assert omd.barrier_status("rv")["state"] == "BROKEN"


def test_timeout_breaks_barrier(tmp_path):
    """타임아웃: deadline 지났는데 미도착이 있으면 BROKEN(timeout) — 영구 hang 방지."""
    omd = Coordinator(db_path=str(tmp_path / "omd.db"))
    _ready(omd, "A", "a"); _ready(omd, "B", "b")
    omd.barrier_declare("rv", ["A", "B"], timeout=0.02)
    omd.barrier_arrive("rv", "agA", "A")    # B 영영 안 옴
    time.sleep(0.05)
    s = omd.barrier_status("rv")             # status 내부 sweep+eval 이 타임아웃 반영
    assert s["state"] == "BROKEN" and s["break_reason"] == "timeout"


def test_abort_breaks_barrier(tmp_path):
    """barrier_abort: 한 참가자가 진행 불가를 깨달아 강제로 깸 → 전원 BROKEN(Barrier.abort)."""
    omd = Coordinator(db_path=str(tmp_path / "omd.db"))
    _ready(omd, "A", "a"); _ready(omd, "B", "b")
    omd.barrier_declare("rv", ["A", "B"])
    omd.barrier_arrive("rv", "agA", "A")
    r = omd.barrier_abort("rv", "agA")
    assert r["ok"] and r["state"] == "BROKEN"
    assert omd.barrier_arrive("rv", "agB", "B")["state"] == "BROKEN"  # 늦게 온 B 도 BROKEN


def test_abort_idempotent(tmp_path):
    omd = Coordinator(db_path=str(tmp_path / "omd.db"))
    _ready(omd, "A", "a")
    omd.barrier_declare("rv", ["A"])
    omd.barrier_abort("rv")
    assert omd.barrier_abort("rv").get("noop")


def test_abort_during_trip_stops_remaining_connect_effects(tmp_path):
    """Abort wins before the next guarded effect: later plan entries never merge."""
    omd = Coordinator(db_path=str(tmp_path / "omd.db"))
    fa = _ready(omd, "A", "a")
    fb = _ready(omd, "B", "b")
    omd.barrier_declare("rv", ["A", "B"])
    omd.barrier_arrive("rv", "agA", "A", fence=fa)
    real_connect_one = omd._barrier_connect_one
    second_entered = threading.Event()
    proceed = threading.Event()
    calls = {"n": 0}
    result = {}

    def block_before_second(task_id, expected_fence, **trip_guard):
        calls["n"] += 1
        if calls["n"] == 2:
            second_entered.set()
            assert proceed.wait(5)
        return real_connect_one(task_id, expected_fence, **trip_guard)

    omd._barrier_connect_one = block_before_second

    def trip():
        result.update(omd.barrier_arrive(
            "rv", "agB", "B", fence=fb, request_id="abort-mid-trip"
        ))

    thread = threading.Thread(target=trip)
    thread.start()
    assert second_entered.wait(5)
    assert omd.store.get_task("A")["state"] == "MERGED"
    try:
        aborted = omd.barrier_abort("rv", "agA")
        assert aborted["ok"] is True and aborted["state"] == "BROKEN"
    finally:
        proceed.set()
    thread.join(timeout=10)
    assert not thread.is_alive()

    assert result["ok"] is False and result["state"] == "BROKEN", result
    assert omd.store.get_task("A")["state"] == "MERGED"
    assert omd.store.get_task("B")["state"] == "DONE"
    assert omd.store.get_idem("abort-mid-trip") is None
    replay = omd.barrier_arrive(
        "rv", "agB", "B", fence=fb, request_id="abort-mid-trip"
    )
    assert replay["ok"] is False and replay["state"] == "BROKEN"
    assert omd.store.get_idem("abort-mid-trip") is None


def test_abort_after_last_effect_never_reports_or_caches_success(tmp_path):
    """Abort between the last effect and final commit makes the trip response fail."""
    omd = Coordinator(db_path=str(tmp_path / "omd.db"))
    fence = _ready(omd, "A", "a")
    omd.barrier_declare("rv", ["A"])
    real_connect_one = omd._barrier_connect_one
    effect_done = threading.Event()
    proceed = threading.Event()
    result = {}

    def block_after_effect(task_id, expected_fence, **trip_guard):
        response = real_connect_one(task_id, expected_fence, **trip_guard)
        effect_done.set()
        assert proceed.wait(5)
        return response

    omd._barrier_connect_one = block_after_effect

    def trip():
        result.update(omd.barrier_arrive(
            "rv", "agA", "A", fence=fence, request_id="abort-after-effect"
        ))

    thread = threading.Thread(target=trip)
    thread.start()
    assert effect_done.wait(5)
    assert omd.store.get_task("A")["state"] == "MERGED"
    try:
        aborted = omd.barrier_abort("rv", "agA")
        assert aborted["ok"] is True and aborted["state"] == "BROKEN"
    finally:
        proceed.set()
    thread.join(timeout=10)
    assert not thread.is_alive()

    assert result["ok"] is False and result["state"] == "BROKEN", result
    assert result["merged"] == ["A"]
    assert omd.store.barrier_by_name("rv")["state"] == "BROKEN"
    assert omd.store.get_idem("abort-after-effect") is None


# ========== policy: shrink ==========
def test_shrink_drops_dead_member_no_dependents(tmp_path):
    """policy='shrink': 죽은 멤버에 의존하는 task 가 없으면 그 멤버를 빼고 N 재계산 후 진행."""
    omd = Coordinator(db_path=str(tmp_path / "omd.db"))
    _ready(omd, "A", "a"); _ready(omd, "B", "b")
    omd.barrier_declare("rv", ["A", "B"], policy="shrink")
    omd.barrier_arrive("rv", "agA", "A")
    omd.bail("agB")    # B 죽음 — 의존자 없음 → shrink(B 제거)
    s = omd.barrier_status("rv")
    # B 가 빠지고 A 만 남아 전원(A) 도착 상태 → status 의 eval 은 can_trip=False 라 fill 안 함;
    # 살아남아 ARMED 유지(BROKEN 아님). 남은 멤버 = 1.
    assert s["state"] == "ARMED" and s["parties"] == 1 and s["break_reason"] is None
    # 이제 A 가 다시 arrive(멱등 재도착)하면 전원 도착 → trip.
    r = omd.barrier_arrive("rv", "agA", "A")
    assert r["state"] == "TRIPPED" and r["merged"] == ["A"]


def test_shrink_blocked_by_dependent_breaks(tmp_path):
    """policy='shrink' 라도 죽은 멤버에 **의존하는 task 가 있으면** shrink 금지 → break(전원 깸).
    의존자를 두고 멤버를 빼면 그 의존자가 미응결 base 위에 빌드하게 되어 위험."""
    omd = Coordinator(db_path=str(tmp_path / "omd.db"))
    _ready(omd, "A", "a"); _ready(omd, "B", "b")
    omd.declare("C", writes=["c/**"], deps=["B"])   # C 가 B 에 의존
    omd.barrier_declare("rv", ["A", "B"], policy="shrink")
    omd.barrier_arrive("rv", "agA", "A")
    omd.bail("agB")    # B 죽음 — 의존자 C 있음 → shrink 불가 → break
    s = omd.barrier_status("rv")
    assert s["state"] == "BROKEN" and s["break_reason"] == "participant_dead"


# ========== 세대(generation) 재무장 ==========
def test_rearm_next_generation_after_broken(tmp_path):
    """BROKEN 된 배리어를 같은 이름으로 다시 declare → 다음 세대(generation+1)로 재무장.
    세대 스탬프가 옛 세대의 유령 도착을 막는다."""
    omd = Coordinator(db_path=str(tmp_path / "omd.db"))
    _ready(omd, "A", "a"); _ready(omd, "B", "b")
    omd.barrier_declare("rv", ["A", "B"])
    omd.barrier_abort("rv")
    assert omd.barrier_status("rv")["state"] == "BROKEN"
    r = omd.barrier_declare("rv", ["A", "B"])    # 재무장
    assert r["ok"] and r["generation"] == 1 and r["state"] == "ARMED"


def test_redeclare_active_rejected(tmp_path):
    """아직 활성(ARMED) 배리어를 같은 이름으로 재declare 하면 거부(이중 무장 방지)."""
    omd = Coordinator(db_path=str(tmp_path / "omd.db"))
    _ready(omd, "A", "a")
    omd.barrier_declare("rv", ["A"])
    r = omd.barrier_declare("rv", ["A"])
    assert r["ok"] is False and "already active" in r["reason"]


# ========== connect_one 이 검증한 궤도를 재진입 만료시키지 않음 (§D5 핵심) ==========
def test_trip_phase_a_does_not_sweep(tmp_path):
    """§D5 핵심(검증기 적발): 응결 trip 은 공개 connect() 를 재호출하지 않는다 — 공개 connect 의
    Phase A 는 _sweep_inline 을 부르고, 그 sweep 가 방금 배리어가 검증한(만료 임박) 궤도를 트립
    직전 재진입 만료시켜 fenced_out 으로 깰 수 있다. 그래서 _barrier_connect_one 의 Phase A'(=
    _barrier_connect_phase_a)는 sweep 을 부르지 않는다.

    트립이 도는 동안 _sweep_inline 이 한 번도 안 불리는지 직접 센다 — 트립이 공개 connect 를
    썼다면(또는 Phase A' 가 sweep 을 부른다면) 카운트가 늘어 RED."""
    omd = Coordinator(db_path=str(tmp_path / "omd.db"))
    _ready(omd, "A", "a"); _ready(omd, "B", "b")
    omd.barrier_declare("rv", ["A", "B"])
    omd.barrier_arrive("rv", "agA", "A")
    # B 의 arrive 가 트립을 돈다. arrive 의 Phase A 는 sweep 1회(정상) 부르지만, 그 *후* 락밖
    # 트립 phase 들은 sweep 을 안 부른다. 트립 구간만 계측한다.
    sweeps = {"n": 0}
    real_sweep = omd._sweep_inline

    def counting_sweep():
        sweeps["n"] += 1
        return real_sweep()

    # 트립은 _barrier_trip 안에서 일어난다 — 그 진입 시점부터 sweep 을 계측.
    real_trip = omd._barrier_trip

    def trip_wrapper(*a, **k):
        omd._sweep_inline = counting_sweep
        try:
            return real_trip(*a, **k)
        finally:
            omd._sweep_inline = real_sweep

    omd._barrier_trip = trip_wrapper
    r = omd.barrier_arrive("rv", "agB", "B")
    assert r["state"] == "TRIPPED", r
    assert omd.store.get_task("A")["state"] == "MERGED"
    assert sweeps["n"] == 0, f"트립 중 sweep 이 {sweeps['n']}회 불림(검증한 궤도 재진입 만료 위험)"


# ========== LTDD 트레이스 게이트 ==========
def test_barrier_break_trace_arrives(tmp_path):
    """LTDD(증분8): barrier_declared → 참가자 사망 → barrier_broken(participant_dead) 트레이스가
    순서대로 store 에 도착(gates/barrier.yaml). 관측가능 동작 = 전원 BROKEN 기상의 외부 증거."""
    pytest.importorskip("ooptdd")
    from ooptdd.backends import MemoryBackend, memory as mem
    from ooptdd.gate import evaluate, evidence_tier, load_gate

    mem.reset()
    backend = MemoryBackend()
    omd = Coordinator(db_path=str(tmp_path / "omd.db"), events=Emitter(backend))
    _ready(omd, "A", "a"); _ready(omd, "B", "b")
    omd.barrier_declare("rendezvous", ["A", "B"])
    omd.barrier_arrive("rendezvous", "agA", "A")
    omd.bail("agB")   # 참가자 사망 → barrier_broken(participant_dead)

    res = evaluate(backend, load_gate(os.path.join(GATES, "barrier.yaml")))
    assert res["ok"], res
    assert evidence_tier(res) == "arrived"
