"""P3-O3 — 충돌의 resolve-task 자동 승격 (증분17, jj first-class conflicts 반영).

증분13(O1/O2)은 충돌 응답에 진단(conflict_files/culprits/hint)을 동봉하고 rerere로 동일충돌을
재사용했지만, "경보 이후가 비어있다"의 잔여 O3 — *충돌을 큐에 들어가는 정상 작업으로* — 은
미구현이었다. 충돌이 나면 에이전트가 out-of-band로 rebase/재연결해야 했다(큐 밖 작업).

  INV-P3-O3-1 (승격): merge conflict(배타/shared 무관) 응답이 `resolve_task_id`를 동봉하고,
      그 id의 task가 큐에 PENDING/READY로 존재한다 — `resolve_for=원task`, `resolve_conflict_files`
      에 충돌 경로. resolve-task 자체는 궤도를 claim하지 않는다(원 task가 아직 write-orbit 보유중 —
      같은 파일 claim은 자기충돌). 순수 큐 마커.
  INV-P3-O3-2 (멱등): 같은 원 task의 재연결이 다시 충돌해도 resolve-task는 **정확히 1개**
      (deterministic id `resolve::{task}` + dedup). 두 번째 resolve_task_id == 첫 번째.
  INV-P3-O3-3 (gate = 음성 오라클): 충돌 없이 성공한 connect는 resolve-task를 **만들지 않는다**
      (승격은 충돌-게이트, 무조건 아님). 이 음성이 깨지면(무조건 승격) 테스트가 RED.
"""
import subprocess
from pathlib import Path

from omd_server import Coordinator


def _git(args, cwd, check=True):
    return subprocess.run(["git", *args], cwd=str(cwd), check=check,
                          capture_output=True, text=True)


def _init(root):
    root.mkdir()
    _git(["init", "-b", "main"], root)
    _git(["config", "user.name", "t"], root)
    _git(["config", "user.email", "t@t"], root)
    (root / "constants").mkdir()
    (root / "constants" / "env.py").write_text("X = 1\n")
    _git(["add", "-A"], root)
    _git(["commit", "-m", "base"], root)
    _git(["checkout", "-b", "dev"], root)


def _mk(tmp_path):
    repo = tmp_path / "repo"; _init(repo)
    omd = Coordinator(db_path=str(tmp_path / "o.db"), repo=str(repo),
                      worktrees_dir=str(tmp_path / "wt"), integration_branch="main")
    return omd, repo, repo.parent / f"{repo.name}-omd-integration"


def _develop(omd, task, agent, val, *, mode="write", shared=False):
    kw = {"shared": ["constants/**"]} if shared else {"writes": ["constants/**"]}
    omd.declare(task, **kw)
    omd.next_task(agent)
    omd.claim(agent, ["constants/**"], "shared" if shared else "write", task_id=task)
    wt = Path(omd.start(task, agent)["worktree"])
    env = wt / "constants" / "env.py"
    env.write_text(env.read_text().replace("X = 1", val))
    assert omd.commit(task, f"{task}: {val}")["ok"] is True
    omd.finish(task)
    return wt


def _bypass_commit_on_main(integ, content="X = 999\n"):
    env = integ / "constants" / "env.py"
    env.write_text(content)
    _git(["add", "-A"], integ)
    _git(["-c", "user.name=human", "-c", "user.email=h@h",
          "commit", "-m", "bypass: hotfix on main"], integ)
    return _git(["rev-parse", "HEAD"], integ).stdout.strip()


def _resolve_rows(omd, original):
    return omd.store.db.execute(
        "SELECT task_id, resolve_for, state, resolve_conflict_files "
        "FROM tasks WHERE resolve_for=?", (original,)).fetchall()


# ── INV-P3-O3-1 : 배타충돌 → resolve-task 승격 ──────────────────────────────
def test_exclusive_conflict_promotes_resolve_task(tmp_path):
    omd, _repo, integ = _mk(tmp_path)
    _develop(omd, "T", "agT", "X = 222")
    _bypass_commit_on_main(integ)

    r = omd.connect("T")
    assert r["ok"] is False and r["reason"].startswith("merge conflict")
    rid = r.get("resolve_task_id")
    assert rid == "resolve::T", f"resolve_task_id 동봉해야: {r}"

    rt = omd.store.get_task(rid)
    assert rt is not None, "resolve-task가 큐에 존재해야"
    assert rt["resolve_for"] == "T"
    assert rt["state"] in ("PENDING", "READY"), rt["state"]
    import json
    assert json.loads(rt["resolve_conflict_files"]) == ["constants/env.py"], rt
    # 순수 큐 마커 — 궤도 claim 안 함(원 task가 아직 constants/** 보유중)
    assert json.loads(rt["writes"] or "[]") == []


# ── INV-P3-O3-2 : 멱등 (재충돌해도 resolve-task 1개) ────────────────────────
def test_resolve_task_promotion_is_idempotent(tmp_path):
    omd, _repo, integ = _mk(tmp_path)
    _develop(omd, "T", "agT", "X = 222")
    _bypass_commit_on_main(integ)

    r1 = omd.connect("T")
    r2 = omd.connect("T")  # T는 여전히 DONE + constants/** 보유 → 다시 충돌
    assert r1["resolve_task_id"] == r2["resolve_task_id"] == "resolve::T"
    rows = _resolve_rows(omd, "T")
    assert len(rows) == 1, f"resolve-task는 정확히 1개여야(멱등): {rows}"


# ── INV-P3-O3-3 : shared 충돌도 승격 ────────────────────────────────────────
def test_shared_conflict_promotes_resolve_task(tmp_path):
    omd, _repo, _integ = _mk(tmp_path)
    _develop(omd, "A", "agA", "X = 111", shared=True)
    _develop(omd, "B", "agB", "X = 333", shared=True)
    assert omd.connect("A")["ok"] is True
    r = omd.connect("B")
    assert r["reason"].startswith("shared_conflict")
    assert r.get("resolve_task_id") == "resolve::B"
    assert omd.store.get_task("resolve::B")["resolve_for"] == "B"


# ── INV-P3-O3-4 : 음성 오라클 — 성공 connect는 승격 안 함 ──────────────────
def test_successful_connect_creates_no_resolve_task(tmp_path):
    omd, _repo, _integ = _mk(tmp_path)
    _develop(omd, "T", "agT", "X = 222")
    r = omd.connect("T")           # 우회 없음 → 깨끗한 3-way merge 성공
    assert r["ok"] is True, r
    assert "resolve_task_id" not in r, "성공 connect는 resolve-task 만들면 안 됨(충돌-게이트)"
    assert _resolve_rows(omd, "T") == [], "성공엔 resolve-task 0개"
