"""GAP-2 — write-set 파일시스템 감사 fail-CLOSED (§D10/P0-11 soundness 구멍 봉합).

이전: `_writeset_audit` 이 repo 미바인딩/브랜치 부재/`git diff` 실패를 전부 `[]`(=위반없음=PASS)로
삼켰다. bound repo 에서 diff 가 실패하면 감사가 *조용히 통과* → 검증 못 한 write-set 이 응결(merge)
되어 분열 위험(soundness hole).

이제: typed WritesetAudit 4-way.
  - repo 미바인딩 → SKIPPED_NO_REPO (실 merge 없음 = DB-only 응결; 감사 대상 없음 → 비차단, idiom 예외).
  - repo 바인딩됐는데 branch 부재/`git diff` 실패 → AUDIT_ERROR (**차단**: 검증 못 한 걸 통과 안 시킴).
  - 궤도 밖 경로 → VIOLATION (차단, 기존 동작).
  - 깨끗 → CLEAN (통과).
서버는 죽지 않고 typed verdict 로 connect/merge 를 거부한다(raise-and-die 아님).
"""

import subprocess
from pathlib import Path

from omd_server import Coordinator
from omd_server.core import WritesetAudit, WritesetVerdict
from omd_server.gitio import GitError


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


def _mk(root: Path):
    return Coordinator(db_path=str(root / "omd.db"), repo=str(root / "repo"),
                       worktrees_dir=str(root / "wt"), integration_branch="main")


def _write(wt, rel, content="x = 1\n"):
    p = Path(wt) / rel
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(content)


def _drive_to_done(omd):
    """A(a/** claim, a/x.py 쓰기) → commit → finish. connect 직전(DONE)까지 몰고 온다."""
    omd.declare("A", writes=["a/**"])
    omd.next_task("agA")
    omd.claim("agA", ["a/**"], task_id="A")
    s = omd.start("A", "agA")
    _write(s["worktree"], "a/x.py")
    omd.commit("A", "feat: a/x")
    omd.finish("A")
    return omd


# ---------- 유닛: typed verdict 의미 ----------
def test_writeset_audit_verdict_semantics():
    assert WritesetAudit(WritesetVerdict.CLEAN).blocks is False
    assert WritesetAudit(WritesetVerdict.SKIPPED_NO_REPO).blocks is False
    v = WritesetAudit(WritesetVerdict.VIOLATION, offending=("b/x.py",))
    assert v.blocks is True and v.reason == "writeset_violation"
    e = WritesetAudit(WritesetVerdict.AUDIT_ERROR, error="git diff failed: boom")
    assert e.blocks is True and e.reason == "writeset_audit_error"


# ---------- 1) 가드 발동: git diff 실패 → AUDIT_ERROR 로 connect 차단(fail-closed) ----------
def test_git_diff_error_blocks_connect_fail_closed(tmp_path, monkeypatch):
    _init_repo(tmp_path / "repo")
    omd = _drive_to_done(_mk(tmp_path))

    def _boom(*a, **k):
        raise GitError("simulated diff failure")

    monkeypatch.setattr(omd.git, "changed_paths", _boom)   # 감사가 write-set 을 관측 불가
    res = omd.connect("A")
    # 조용히 PASS 하지 않고 typed 거부(서버 살아있음 — 예외로 죽지 않음).
    assert res["ok"] is False and res["reason"] == "writeset_audit_error", res
    assert "audit_error" in res
    # merge 안 일어남 — 통합 브랜치 불변, task 미MERGED, 토큰 누수 0.
    assert omd.store.get_task("A")["state"] != "MERGED"
    log = subprocess.run(["git", "log", "--oneline", "main"], cwd=str(tmp_path / "repo"),
                         capture_output=True, text=True).stdout
    assert "CLOUD CONNECT A" not in log
    assert omd.store.all_held_merge_tokens() == []


# ---------- 2) 가드 발동: repo 바인딩됐는데 branch 부재 → AUDIT_ERROR ----------
def test_missing_branch_with_bound_repo_is_audit_error(tmp_path):
    _init_repo(tmp_path / "repo")
    omd = _mk(tmp_path)
    audit = omd._writeset_audit("A", None, ["a/**"])
    assert audit.verdict is WritesetVerdict.AUDIT_ERROR and audit.blocks is True


# ---------- 3) 정상 경로 불변: 깨끗한 감사 → CLEAN → connect MERGED ----------
def test_clean_audit_allows_connect(tmp_path):
    _init_repo(tmp_path / "repo")
    omd = _drive_to_done(_mk(tmp_path))
    res = omd.connect("A")
    assert res["ok"] and res["state"] == "MERGED", res


# ---------- 4) VIOLATION 은 여전히 writeset_violation(하위호환) ----------
def test_out_of_orbit_still_reports_violation(tmp_path):
    _init_repo(tmp_path / "repo")
    omd = _mk(tmp_path)
    omd.declare("A", writes=["a/**"])
    omd.next_task("agA")
    omd.claim("agA", ["a/**"], task_id="A")
    s = omd.start("A", "agA")
    _write(s["worktree"], "a/x.py")
    _write(s["worktree"], "b/foo.py")            # 궤도 밖
    omd.commit("A", "feat: a/x + b/foo")
    omd.finish("A")
    res = omd.connect("A")
    assert res["ok"] is False and res["reason"] == "writeset_violation", res
    assert res["offending"] == ["b/foo.py"], res


# ---------- 5) idiom 예외: repo 미바인딩(DB-only) → SKIPPED_NO_REPO(비차단) ----------
def test_unbound_repo_audit_is_skipped_not_blocking(tmp_path):
    omd = Coordinator(db_path=str(tmp_path / "omd.db"))   # repo 미바인딩
    audit = omd._writeset_audit("T", None, ["a/**"])
    assert audit.verdict is WritesetVerdict.SKIPPED_NO_REPO and audit.blocks is False
    # DB-only 응결 흐름은 여전히 동작(감사가 막지 않음).
    omd.declare("T", writes=["a/**"])
    omd.next_task("ag")
    omd.claim("ag", ["a/**"], task_id="T")
    omd.start("T", "ag")
    omd.finish("T")
    res = omd.connect("T")
    assert res["ok"] and res["state"] == "MERGED", res


# ---------- 6) commit 은 advisory: 감사 불가를 라우드 표기하되 커밋은 막지 않음 ----------
def test_commit_surfaces_audit_error_but_does_not_block(tmp_path, monkeypatch):
    _init_repo(tmp_path / "repo")
    omd = _mk(tmp_path)
    omd.declare("A", writes=["a/**"])
    omd.next_task("agA")
    omd.claim("agA", ["a/**"], task_id="A")
    s = omd.start("A", "agA")
    _write(s["worktree"], "a/x.py")

    def _boom(*a, **k):
        raise GitError("simulated diff failure")

    monkeypatch.setattr(omd.git, "changed_paths", _boom)
    r = omd.commit("A", "feat: a/x")
    # 커밋 자체는 성공(advisory 지점) — 단 감사 불가를 조용히 삼키지 않고 표기.
    assert r["ok"] is True and "sha" in r, r
    assert r.get("writeset_audit_error") is not None, r
    assert "writeset_violation" not in r, r
