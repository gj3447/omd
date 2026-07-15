"""증분5 — D9 멱등성 (at-least-once MCP → exactly-once 효과).

MCP 는 at-least-once: 서버는 성공했는데 응답이 유실되면 클라가 재시도한다. 멱등 없이는
  claim 재시도 → 두 번째 HELD 궤도(누수 lease)
  start 재시도 → worktree add -b 가 기존 브랜치에서 GitError + 중복행
  connect 재시도 → 이중 merge / 이중 release
멱등 = request_id 캐시(INFLIGHT/DONE, 성공 종단만) + 의미적 멱등(intent_key/기존worktree/already-merged).
§3.C: 성공만 캐시(DENIED/stale-fence 는 재시도 가능해야) + dedup 재생을 owner/fence 가 감싼다.
"""

import threading
import time
from pathlib import Path

import pytest

from omd_server import Coordinator


# ---------- request_id 캐시: 같은 효과 한 번만 ----------
def test_claim_retry_same_request_id_no_leak(tmp_path):
    """같은 request_id 로 claim 재시도 → 캐시 적중, 같은 궤도 반환(누수 lease 0)."""
    omd = Coordinator(db_path=str(tmp_path / "omd.db"))
    r1 = omd.claim("agA", ["a/**"], "write", request_id="req-1")
    r2 = omd.claim("agA", ["a/**"], "write", request_id="req-1")
    assert r1["orbit_id"] == r2["orbit_id"]
    assert r2.get("replayed"), r2
    held = [o for o in omd.store.held_orbits()]
    assert len(held) == 1, f"누수 lease: {held}"


@pytest.mark.parametrize(
    "mutated",
    [
        {"agent_id": "agB"},
        {"pathspec": ["b/**"]},
        {"mode": "read"},
        {"bail_epoch": 1},
    ],
)
def test_completed_request_id_rejects_mutated_claim_envelope(tmp_path, mutated):
    """DONE replay is exact-envelope only; identity/intent mutation is a conflict."""
    omd = Coordinator(db_path=str(tmp_path / "omd.db"))
    first = omd.claim(
        "agA", ["a/**"], "write", request_id="same-id", bail_epoch=0
    )
    args = {
        "agent_id": "agA",
        "pathspec": ["a/**"],
        "mode": "write",
        "request_id": "same-id",
        "bail_epoch": 0,
    }
    args.update(mutated)
    conflict = omd.claim(**args)
    assert conflict["ok"] is False
    assert conflict["reason"] == "idempotency_conflict"
    assert omd.store.get_orbit(first["orbit_id"])["state"] == "HELD"
    assert len(omd.store.held_orbits()) == 1


def test_request_id_cannot_cross_verbs(tmp_path):
    omd = Coordinator(db_path=str(tmp_path / "omd.db"))
    held = omd.claim("agA", ["a/**"], request_id="cross-verb")
    conflict = omd.release(
        held["orbit_id"], "agA", held["fence"], request_id="cross-verb"
    )
    assert conflict["ok"] is False
    assert conflict["reason"] == "idempotency_conflict"
    assert omd.store.get_orbit(held["orbit_id"])["state"] == "HELD"


def test_pending_request_identity_replays_exactly_and_rejects_mutation(tmp_path):
    """PENDING is retryable but still owns its live request identity."""
    omd = Coordinator(db_path=str(tmp_path / "omd.db"))
    omd.claim("holder", ["a/**"])
    first = omd.claim(
        "waiter", ["a/**"], priority=3, request_id="pending-id", bail_epoch=0
    )
    replay = omd.claim(
        "waiter", ["a/**"], priority=3, request_id="pending-id", bail_epoch=0
    )
    assert replay["dedup"] is True
    assert replay["orbit_id"] == first["orbit_id"]
    assert replay["queue_seq"] == first["queue_seq"]

    conflict = omd.claim(
        "waiter", ["a/**"], priority=4, request_id="pending-id", bail_epoch=0
    )
    assert conflict["ok"] is False
    assert conflict["reason"] == "idempotency_conflict"
    assert len(omd.store.pending_orbits()) == 1


def test_pending_request_rejects_reason_mutation(tmp_path):
    omd = Coordinator(db_path=str(tmp_path / "omd.db"))
    omd.claim("holder", ["a/**"])
    first = omd.claim(
        "waiter", ["a/**"], reason="first", request_id="pending-reason"
    )
    conflict = omd.claim(
        "waiter", ["a/**"], reason="changed", request_id="pending-reason"
    )
    assert first["state"] == "PENDING"
    assert conflict["ok"] is False
    assert conflict["reason"] == "idempotency_conflict"
    assert len(omd.store.pending_orbits()) == 1


def test_pending_request_id_owns_global_live_namespace(tmp_path):
    omd = Coordinator(db_path=str(tmp_path / "omd.db"))
    holder = omd.claim("holder", ["a/**"])
    waiting = omd.claim(
        "waiter", ["a/**"], request_id="pending-global-id"
    )
    conflict = omd.release(
        holder["orbit_id"], "holder", holder["fence"],
        request_id="pending-global-id",
    )
    assert conflict["ok"] is False
    assert conflict["reason"] == "idempotency_conflict"
    assert omd.store.get_orbit(waiting["orbit_id"])["state"] == "PENDING"
    assert omd.store.get_orbit(holder["orbit_id"])["state"] == "HELD"


def test_explicit_request_id_cannot_alias_natural_intent(tmp_path):
    omd = Coordinator(db_path=str(tmp_path / "omd.db"))
    omd.claim("holder", ["a/**"])
    original = omd.claim("waiter", ["a/**"], request_id="original-id")
    alias = omd.claim("waiter", ["a/**"], request_id="alias-id")
    assert original["state"] == "PENDING"
    assert alias["ok"] is False
    assert alias["reason"] == "idempotency_conflict"
    assert alias["original_request_id"] == "original-id"
    assert omd.store.latest_orbit_by_request("alias-id") is None


def test_promoted_then_released_request_cannot_repeat_generation_zero(tmp_path):
    omd = Coordinator(db_path=str(tmp_path / "omd.db"), agent_ttl=None)
    holder = omd.claim("holder", ["a/**"])
    waiting = omd.claim("waiter", ["a/**"], request_id="lifecycle-id")
    omd.release(holder["orbit_id"], "holder", holder["fence"])
    promoted = omd.store.get_orbit(waiting["orbit_id"])
    assert promoted["state"] == "HELD"
    omd.release(promoted["orbit_id"], "waiter", promoted["fence"])
    seq_before = omd.store.current_seq()
    fence_before = omd.store.current_fence()

    replay = omd.claim("waiter", ["a/**"], request_id="lifecycle-id")
    rows = omd.store.db.execute(
        "SELECT orbit_id,request_generation,state FROM orbits WHERE request_id=?",
        ("lifecycle-id",),
    ).fetchall()
    assert replay["dedup"] is True
    assert replay["orbit_id"] == waiting["orbit_id"]
    assert replay["state"] == "RELEASED"
    assert [(row["request_generation"], row["state"]) for row in rows] == [
        (0, "RELEASED")
    ]
    assert omd.store.current_seq() == seq_before
    assert omd.store.current_fence() == fence_before

    conflict = omd.claim("waiter", ["b/**"], request_id="lifecycle-id")
    assert conflict["ok"] is False
    assert conflict["reason"] == "idempotency_conflict"


def test_timed_out_request_replays_terminal_generation_zero(tmp_path):
    omd = Coordinator(db_path=str(tmp_path / "omd.db"), agent_ttl=None)
    holder = omd.claim("holder", ["a/**"])
    waiting = omd.claim("waiter", ["a/**"], request_id="timeout-terminal")
    with omd.store.tx():
        omd.store.set_orbit(waiting["orbit_id"], wait_deadline=time.time() - 1)
    omd.sweep()
    timed_out = omd.store.get_orbit(waiting["orbit_id"])
    assert timed_out["state"] == "DENIED"
    assert timed_out["decision_type"] == "WAIT_TIMEOUT"

    omd.release(holder["orbit_id"], "holder", holder["fence"])
    seq_before = omd.store.current_seq()
    fence_before = omd.store.current_fence()
    replay = omd.claim("waiter", ["a/**"], request_id="timeout-terminal")
    rows = omd.store.db.execute(
        "SELECT orbit_id,request_generation,state,decision_type FROM orbits "
        "WHERE request_id=?",
        ("timeout-terminal",),
    ).fetchall()

    assert replay["dedup"] is True
    assert replay["orbit_id"] == waiting["orbit_id"]
    assert replay["state"] == "DENIED"
    assert replay["request_generation"] == 0
    assert [tuple(row) for row in rows] == [
        (waiting["orbit_id"], 0, "DENIED", "WAIT_TIMEOUT")
    ]
    assert omd.store.current_seq() == seq_before
    assert omd.store.current_fence() == fence_before


def test_claim_semantic_dedup_without_request_id(tmp_path):
    """request_id 없어도(또는 달라도) 같은 의도(agent,paths,mode,task)면 기존 궤도 반환 — intent_key."""
    omd = Coordinator(db_path=str(tmp_path / "omd.db"))
    r1 = omd.claim("agA", ["a/**"], "write", task_id="T")
    r2 = omd.claim("agA", ["a/**"], "write", task_id="T")   # request_id 없음 — 의미적 멱등
    assert r1["orbit_id"] == r2["orbit_id"] and r2.get("dedup")
    assert len([o for o in omd.store.held_orbits()]) == 1


def test_denied_not_cached_retryable(tmp_path):
    """§3.C: DENIED(데드락) 는 캐시 금지 — 세상이 바뀌면 같은 request_id 재시도가 성공해야."""
    omd = Coordinator(db_path=str(tmp_path / "omd.db"))
    # 데드락 사이클을 만든다: agA HELD a, agB HELD b, 그 위에서 교차 대기.
    a = omd.claim("agA", ["a/**"], "write")
    b = omd.claim("agB", ["b/**"], "write")
    omd.claim("agA", ["b/**"], "write")                      # agA 가 b 대기
    denied = omd.claim("agB", ["a/**"], "write", request_id="req-x")  # agB 가 a 대기 → 사이클
    assert denied["state"] == "DENIED" and denied.get("deadlock"), denied
    assert denied["request_generation"] == 0
    # DENIED 가 캐시 안 됐는지: idempotency 행이 없어야(재시도 가능).
    assert omd.store.get_idem("req-x") is None
    # 교착 해소(agA release) 후 같은 request_id 재시도 → 이번엔 진행(캐시된 DENIED 재생 아님).
    omd.release(a["orbit_id"], "agA", a["fence"])
    omd.release(b["orbit_id"], "agB", b["fence"])
    retry = omd.claim("agB", ["a/**"], "write", request_id="req-x")
    assert retry["state"] in ("HELD", "PENDING"), retry
    assert retry["request_generation"] == 1


def test_fenced_out_not_cached(tmp_path):
    """§3.C: stale-fence(fenced_out) 거부는 캐시 금지 — fence 가 맞춰지면 재시도 가능해야."""
    omd = Coordinator(db_path=str(tmp_path / "omd.db"))
    r = omd.claim("agA", ["a/**"], "write")
    bad = omd.release(r["orbit_id"], "agA", r["fence"] + 99, request_id="rel-1")
    assert bad.get("fenced_out") and omd.store.get_idem("rel-1") is None
    # 올바른 fence 로 같은 request_id 재시도 → 성공.
    ok = omd.release(r["orbit_id"], "agA", r["fence"], request_id="rel-1")
    assert ok["ok"]


# ---------- start 의미적 멱등 (worktree 재생성 금지) ----------
def _init_repo(root: Path):
    import subprocess
    def g(*a):
        subprocess.run(["git", *a], cwd=str(root), check=True, capture_output=True, text=True)
    root.mkdir()
    g("init", "-b", "main"); g("config", "user.name", "t"); g("config", "user.email", "t@t")
    (root / "README.md").write_text("base\n")
    g("add", "-A"); g("commit", "-m", "base"); g("checkout", "-b", "dev")


def test_start_retry_does_not_recreate_worktree(tmp_path):
    """start 재시도 → worktree add -b 를 다시 안 부른다(기존 브랜치면 GitError 였음). dedup 회신."""
    repo = tmp_path / "repo"; _init_repo(repo)
    omd = Coordinator(db_path=str(tmp_path / "omd.db"), repo=str(repo),
                      worktrees_dir=str(tmp_path / "wt"), integration_branch="main")
    omd.declare("A", writes=["a/**"]); omd.next_task("agA")
    omd.claim("agA", ["a/**"], "write", task_id="A")
    s1 = omd.start("A", "agA")
    s2 = omd.start("A", "agA")                               # 재시도(request_id 없이도 자연 멱등)
    assert s1["worktree"] == s2["worktree"] and s2.get("dedup")


def test_start_retry_request_id_cached(tmp_path):
    repo = tmp_path / "repo"; _init_repo(repo)
    omd = Coordinator(db_path=str(tmp_path / "omd.db"), repo=str(repo),
                      worktrees_dir=str(tmp_path / "wt"), integration_branch="main")
    omd.declare("A", writes=["a/**"]); omd.next_task("agA")
    omd.claim("agA", ["a/**"], "write", task_id="A")
    s1 = omd.start("A", "agA", request_id="st-1")
    s2 = omd.start("A", "agA", request_id="st-1")
    assert s1["worktree"] == s2["worktree"] and s2.get("replayed")


# ---------- connect 멱등 (이중 merge 금지) ----------
def _develop(omd, task, subdir, fname, content):
    omd.declare(task, writes=[f"{subdir}/**"]); omd.next_task(f"ag{task}")
    omd.claim(f"ag{task}", [f"{subdir}/**"], task_id=task)
    s = omd.start(task, f"ag{task}")
    (Path(s["worktree"]) / subdir).mkdir(parents=True)
    (Path(s["worktree"]) / subdir / fname).write_text(content)
    omd.commit(task, f"feat: {subdir}/{fname}")
    omd.finish(task)


def test_connect_retry_no_double_merge(tmp_path):
    """connect 재시도(같은 request_id) → 재머지 없이 캐시된 MERGED 회신. 통합에 머지커밋 1개."""
    repo = tmp_path / "repo"; _init_repo(repo)
    omd = Coordinator(db_path=str(tmp_path / "omd.db"), repo=str(repo),
                      worktrees_dir=str(tmp_path / "wt"), integration_branch="main")
    _develop(omd, "A", "a", "x.py", "x = 1\n")
    r1 = omd.connect("A", request_id="cn-1")
    assert r1["state"] == "MERGED"
    r2 = omd.connect("A", request_id="cn-1")
    assert r2["state"] == "MERGED" and r2.get("replayed"), r2
    import subprocess
    log = subprocess.run(["git", "log", "--oneline", "main"], cwd=str(repo),
                         capture_output=True, text=True).stdout
    assert log.count("CLOUD CONNECT A") == 1, log


def test_post_merged_noop_binds_request_envelope(tmp_path):
    repo = tmp_path / "repo"; _init_repo(repo)
    omd = Coordinator(db_path=str(tmp_path / "omd.db"), repo=str(repo),
                      worktrees_dir=str(tmp_path / "wt"), integration_branch="main")
    _develop(omd, "A", "a", "x.py", "x = 1\n")
    assert omd.connect("A")["state"] == "MERGED"

    noop = omd.connect("A", request_id="post-merged")
    assert noop["state"] == "MERGED" and noop["noop"]
    assert omd.store.get_idem("post-merged")["status"] == "DONE"
    conflict = omd.connect("A", push="different", request_id="post-merged")
    assert conflict["ok"] is False
    assert conflict["reason"] == "idempotency_conflict"


def test_connect_reserves_request_envelope_across_unlocked_phase_b(tmp_path):
    repo = tmp_path / "repo"; _init_repo(repo)
    omd = Coordinator(db_path=str(tmp_path / "omd.db"), repo=str(repo),
                      worktrees_dir=str(tmp_path / "wt"), integration_branch="main")
    _develop(omd, "A", "a", "x.py", "x = 1\n")
    entered = threading.Event()
    proceed = threading.Event()
    real_phase_b = omd._connect_phase_b
    result = {}

    def blocked_phase_b(intent, push=None):
        entered.set()
        assert proceed.wait(5)
        return real_phase_b(intent, push=push)

    omd._connect_phase_b = blocked_phase_b

    def first_connect():
        result.update(omd.connect("A", request_id="split-id"))

    thread = threading.Thread(target=first_connect)
    thread.start()
    assert entered.wait(5)
    exact = omd.connect("A", request_id="split-id")
    assert exact["ok"] is False and exact["reason"] == "request_inflight"
    conflict = omd.connect("A", push="different", request_id="split-id")
    assert conflict["ok"] is False
    assert conflict["reason"] == "idempotency_conflict"
    proceed.set()
    thread.join(timeout=10)
    assert not thread.is_alive()
    assert result["state"] == "MERGED"
    replay = omd.connect("A", request_id="split-id")
    assert replay["state"] == "MERGED" and replay["replayed"]


def test_barrier_trip_reserves_request_envelope_across_split_phase(tmp_path):
    omd = Coordinator(db_path=str(tmp_path / "omd.db"))

    def ready(task, sub):
        omd.declare(task, writes=[f"{sub}/**"])
        omd.next_task(f"ag{task}")
        claim = omd.claim(f"ag{task}", [f"{sub}/**"], task_id=task)
        omd.start(task, f"ag{task}")
        omd.finish(task)
        return claim["fence"]

    ready("A", "a")
    fence_b = ready("B", "b")
    omd.barrier_declare("rv", ["A", "B"])
    omd.barrier_arrive("rv", "agA", "A")
    entered = threading.Event()
    proceed = threading.Event()
    real_connect_one = omd._barrier_connect_one
    result = {}

    def blocked_connect_one(task_id, expected_fence):
        if not entered.is_set():
            entered.set()
            assert proceed.wait(5)
        return real_connect_one(task_id, expected_fence)

    omd._barrier_connect_one = blocked_connect_one

    def trip():
        result.update(
            omd.barrier_arrive("rv", "agB", "B", request_id="barrier-split")
        )

    thread = threading.Thread(target=trip)
    thread.start()
    assert entered.wait(5)
    exact = omd.barrier_arrive(
        "rv", "agB", "B", request_id="barrier-split"
    )
    assert exact["ok"] is False and exact["reason"] == "request_inflight"
    conflict = omd.barrier_arrive(
        "rv", "agB", "B", fence=fence_b, request_id="barrier-split"
    )
    assert conflict["ok"] is False
    assert conflict["reason"] == "idempotency_conflict"
    proceed.set()
    thread.join(timeout=10)
    assert not thread.is_alive()
    assert result["state"] == "TRIPPED"
    replay = omd.barrier_arrive(
        "rv", "agB", "B", request_id="barrier-split"
    )
    assert replay["state"] == "TRIPPED" and replay["replayed"]


def test_connect_already_merged_semantic_idempotent(tmp_path):
    """request_id 없어도 이미 MERGED 인 task connect 는 noop(재머지 없음) — 의미적 멱등."""
    repo = tmp_path / "repo"; _init_repo(repo)
    omd = Coordinator(db_path=str(tmp_path / "omd.db"), repo=str(repo),
                      worktrees_dir=str(tmp_path / "wt"), integration_branch="main")
    _develop(omd, "A", "a", "x.py", "x = 1\n")
    assert omd.connect("A")["state"] == "MERGED"
    again = omd.connect("A")
    assert again["state"] == "MERGED" and again.get("noop")


def test_connect_conflict_not_cached_retryable(tmp_path):
    """§3.C: merge conflict(retryable rollback) 는 캐시 금지 — 재시도 가능해야."""
    repo = tmp_path / "repo"; _init_repo(repo)
    omd = Coordinator(db_path=str(tmp_path / "omd.db"), repo=str(repo),
                      worktrees_dir=str(tmp_path / "wt"), integration_branch="main")
    # 통합에 같은 파일을 먼저 응결시켜 충돌을 강제: B 가 a/x.py 를 다른 내용으로.
    _develop(omd, "B", "a", "x.py", "y = 9\n")
    assert omd.connect("B")["state"] == "MERGED"
    # A 도 a/x.py 를 건드려 충돌. 같은 경로라 입체검사상 B와 겹치지만 B가 이미 release 했으니 claim 가능.
    _develop(omd, "A", "a", "x.py", "z = 1\n")
    res = omd.connect("A", request_id="cn-A")
    assert res["ok"] is False and res.get("retryable"), res
    assert omd.store.get_idem("cn-A") is None      # 캐시 안 됨(재시도 가능)


# ---------- §3.C 교차: dedup 이 재부여 lease 를 안 푼다 ----------
def test_dedup_release_does_not_unlock_reassigned_lease(tmp_path):
    """§3.C 핵심: agA 의 release 재시도(request_id)가 *재부여된* lease(같은 orbit_id 가 다른
    소유자/fence)를 풀면 안 된다. 성공만 캐시되므로 첫 release 가 성공했으면 캐시 재생은
    멱등(같은 응답)일 뿐, owner/fence 가드가 새 보유자 lease 를 절대 건드리지 않는다."""
    omd = Coordinator(db_path=str(tmp_path / "omd.db"))
    r = omd.claim("agA", ["a/**"], "write")
    # agA 가 release(성공) — 캐시됨.
    ok = omd.release(r["orbit_id"], "agA", r["fence"], request_id="rel-z")
    assert ok["ok"]
    # 같은 경로를 agB 가 재획득(새 fence, 새 orbit). 옛 release 재생이 이걸 풀면 분열.
    r2 = omd.claim("agB", ["a/**"], "write")
    assert r2["state"] == "HELD"
    # agA 가 release 재시도(같은 request_id) → 캐시된(옛 orbit 의) 응답만 재생, agB 궤도 무손상.
    replay = omd.release(r["orbit_id"], "agA", r["fence"], request_id="rel-z")
    assert replay["ok"]
    assert omd.store.get_orbit(r2["orbit_id"])["state"] == "HELD", "agB lease 가 풀림(§3.C 위반)"
