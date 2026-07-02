"""채택마찰 F2/F3/F4 가드 — 2026-07-02 lakatotree 병렬-dev 실전에서 실측된 마찰의 봉합 계약.

실측(현장 영수증): 인터랙티브 Claude 세션이 claim(ttl=3600) 후 편집(verb 간 침묵 수십 분) →
agent_ttl 기본 90s 에 좀비 판정 → RETIRED+fenced_out, sweep 이 TTL 남은 HELD orbit 회수.
또 lease-only 흐름(declare+claim, start 미경유)의 태스크는 종결 verb 가 없어 PENDING 영구 잔류.

봉합 계약(이 파일이 박제):
  F2/F3 — **페이스는 선언한다**: lease 는 liveness 계약이 아니다(죽은 agent 의 긴 lease 를
          agent_ttl 로 빨리 회수하는 §D2 crash-fast 는 불변 — 기존 6개 좀비 테스트가 박제).
          대신 인터랙티브 세션은 `heartbeat(agent, ttl=)` 로 per-agent 생존창을 *명시 선언*하고,
          그 창 안에서는 verb 간 침묵 수십 분에도 회수되지 않는다.
          + 활동=생존신호: agent 를 나르는 mutating verb(release/commit/finish)가 liveness 를 touch.
  F4  — lease-only 태스크의 종결 verb: cancel(task, reason) = 미시작 상태(PENDING/READY/BLOCKED)
          전용 종결(FSM 기존 abort 전이 재사용 — TLA 모델 무변경). 시작된 태스크는 finish/bail 경유.
"""
import time

from omd_server.core import Coordinator


def _core(tmp_path, agent_ttl=0.15):
    return Coordinator(db_path=str(tmp_path / "omd.db"), repo=None, agent_ttl=agent_ttl)


# ── F2/F3 defect 축: 선언한 페이스 창 안에서는 침묵해도 회수되지 않는다 ──────────────────
def test_interactive_pace_agent_survives_declared_lease_window(tmp_path):
    """claim 후 heartbeat(ttl=)로 페이스 선언 → agent_ttl 을 훌쩍 넘는 침묵에도 orbit 보존 +
    release ok (2026-07-02 실전: 30분 편집 중 RETIRED+lease 회수 재현의 봉합)."""
    core = _core(tmp_path, agent_ttl=0.15)
    out = core.claim("interactive-agent", ["repo/file.py"], task_id=None, ttl=30.0)
    assert out["state"] == "HELD"
    core.heartbeat("interactive-agent", ttl=30.0)   # 페이스 선언: 내 verb 간격은 최대 30s
    time.sleep(0.3)                      # 기본 agent_ttl(0.15s) 훌쩍 넘는 침묵 — 선언창(30s) 안
    swept = core.sweep()
    assert out["orbit_id"] not in swept["expired"], \
        "sweep 이 선언 페이스 창 안의 HELD orbit 을 좀비회수(F3 재현)"
    rel = core.release(out["orbit_id"], "interactive-agent", out["fence"])
    assert rel.get("ok", False) is True, f"선언 창 안 release 가 fenced_out(F2 재현): {rel}"


def test_crash_fast_reclaim_preserved_for_undeclared_agents(tmp_path):
    """과잉보호 아님(반대 오라클): 페이스 미선언 agent 는 긴 lease 를 쥐고 있어도 기존
    crash-fast(agent_ttl) 그대로 회수된다 — 죽은 물방울의 긴 lease 를 빨리 되찾는 §D2 불변."""
    core = _core(tmp_path, agent_ttl=0.15)
    core.claim("machine-agent", ["repo/other.py"], task_id=None, ttl=600.0)   # 긴 lease, 무선언
    time.sleep(0.3)                      # heartbeat stale — 선언 없음
    core.sweep()
    hb = core.heartbeat("machine-agent")
    assert hb.get("fenced_out") is True, "미선언 침묵 agent 가 회수되지 않음(crash-fast 퇴행)"


def test_mutating_verbs_touch_agent_liveness(tmp_path):
    """활동=생존신호: release(및 agent 를 나르는 mutating verb)가 last_heartbeat 를 갱신한다 —
    verb 를 부르는 동안엔 명시 heartbeat 없이도 살아있다."""
    core = _core(tmp_path, agent_ttl=5.0)
    out = core.claim("busy-agent", ["repo/a.py"], task_id=None, ttl=30.0)
    a0 = core.store.get_agent("busy-agent")["last_heartbeat"]
    time.sleep(0.05)
    core.release(out["orbit_id"], "busy-agent", out["fence"])
    a1 = core.store.get_agent("busy-agent")["last_heartbeat"]
    assert a1 > a0, "release 가 liveness 를 touch 하지 않음(활동≠생존신호)"


# ── F4: lease-only 태스크의 종결 verb ────────────────────────────────────────────────────
def test_lease_only_task_can_be_cancelled(tmp_path):
    """declare(+lease 작업) 후 start 미경유 태스크: finish 는 여전히 FSM 거부(정확),
    cancel 은 PENDING→ABORTED 로 종결하고 next() 추천에서 사라진다."""
    core = _core(tmp_path, agent_ttl=None)
    core.declare("lease-only-task", writes=["repo/x.py"])
    try:
        core.finish("lease-only-task")
        raise AssertionError("PENDING 태스크 finish 가 통과(FSM 가드 소실)")
    except Exception:
        pass                                            # 기존 정확 거동: 미시작 finish 불가
    out = core.cancel("lease-only-task", reason="lease-only 작업 완료 — 커밋은 pathspec 으로 별도")
    assert out.get("ok") is True and out.get("state") == "ABORTED", out
    assert core.next_task("anyone") is None, "cancel 된 태스크가 next() 추천에 잔류"


def test_cancel_refuses_started_tasks(tmp_path):
    """cancel 은 미시작 전용 — IN_ORBIT 이후는 finish/bail 경유(무단 종결로 진행중 작업 증발 금지)."""
    core = _core(tmp_path, agent_ttl=None)
    core.declare("started-task", writes=["repo/y.py"])
    t = core.next_task("worker")
    assert t is not None and t["task_id"] == "started-task"
    core.start("started-task", "worker")                # READY→CLAIMED→IN_ORBIT (repo=None 이면 워크트리 스킵)
    out = core.cancel("started-task", reason="x")
    assert out.get("ok") is False, "시작된 태스크가 cancel 로 증발(무단 종결)"


def test_cancel_is_idempotent_and_missing_task_fails_loud(tmp_path):
    core = _core(tmp_path, agent_ttl=None)
    core.declare("t1", writes=["repo/z.py"])
    assert core.cancel("t1", reason="r")["ok"] is True
    again = core.cancel("t1", reason="r")               # 이미 ABORTED — 멱등(ok, 재종결 아님)
    assert again.get("ok") is True and again.get("already") is True, again
    missing = core.cancel("no-such-task", reason="r")
    assert missing.get("ok") is False, "미존재 태스크 cancel 이 무음 성공(fail-loud 위반)"
