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
    # DENIED 가 캐시 안 됐는지: idempotency 행이 없어야(재시도 가능).
    assert omd.store.get_idem("req-x") is None
    # 교착 해소(agA release) 후 같은 request_id 재시도 → 이번엔 진행(캐시된 DENIED 재생 아님).
    omd.release(a["orbit_id"], "agA", a["fence"])
    omd.release(b["orbit_id"], "agB", b["fence"])
    retry = omd.claim("agB", ["a/**"], "write", request_id="req-x")
    assert retry["state"] in ("HELD", "PENDING"), retry


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
