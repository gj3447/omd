"""증분4 — P0-11 / §D10: write-set 파일시스템 강제(SINGULON 토대 (c), "최대 구멍").

전(前): 궤도는 순수 advisory — `git add -A`가 worktree 전체를 커밋하므로 `a/**`를 claim한
물방울이 `b/foo.py`를 고쳐도 응결되어 분열을 일으키면서 다른 모든 검사를 통과한다.

이제 connect 게이트(Phase A)가 `git diff --name-only base...branch`의 모든 경로가 claimed
write-set glob 에 덮이는지 감사한다. 궤도 밖 경로가 있으면 `writeset_violation` 으로 거부하고
**merge 하지 않으며 통합 브랜치는 불변**.

검증축:
 1) 궤도 밖 쓰기(a/** claim, b/ 작성) → connect 거부 + 통합 불변(미머지).
 2) 궤도 안 쓰기만 → connect 정상 MERGED.
 3) 다중 write-orbit 합집합 커버리지(a/** ∪ c/**).
 4) 궤도 밖으로의 rename → 거부.
 5) (이빨) 감사 우회 시 궤도 밖 쓰기가 머지됨(§D10 분열) → 테스트가 RED.
"""

import subprocess
from pathlib import Path

import pytest

from omd_server import Coordinator
from omd_server.disjoint import path_in_globs, path_matches_glob
from omd_server.gitio import GitError, GitIntegrationPreconditionError


def _git(args, cwd):
    subprocess.run(["git", *args], cwd=str(cwd), check=True, capture_output=True, text=True)


def _init_repo(root: Path):
    """root=사용자 HEAD(dev), main=OMD 전용 통합 브랜치(§D11)."""
    root.mkdir()
    _git(["init", "-b", "main"], root)
    _git(["config", "user.name", "t"], root)
    _git(["config", "user.email", "t@t"], root)
    (root / "README.md").write_text("base\n")
    _git(["add", "-A"], root)
    _git(["commit", "-m", "base"], root)
    _git(["checkout", "-b", "dev"], root)


def _mk(omd):
    return Coordinator(db_path=str(omd / "omd.db"), repo=str(omd / "repo"),
                       worktrees_dir=str(omd / "wt"), integration_branch="main")


def _write(wt, rel, content="x = 1\n"):
    p = Path(wt) / rel
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(content)


# ---------- 순수 매처 단위 검증(soundness) ----------
def test_path_matcher_is_precise_not_false_positive():
    assert path_matches_glob("a/**", "a/x.py") is True
    assert path_matches_glob("a/**", "b/foo.py") is False
    assert path_matches_glob("a/*.py", "a/sub/x.py") is False
    # char-class: globs_overlap 은 보수적으로 True 지만, 감사 매처는 정확해야(거짓-덮임 금지).
    assert path_matches_glob("a/[xy].py", "a/z.py") is False
    assert path_in_globs("a/x.py", ["b/**", "a/**"]) is True
    assert path_in_globs("c/x.py", ["b/**", "a/**"]) is False


# ---------- 1) 궤도 밖 쓰기 → connect 거부 + 통합 불변 ----------
def test_out_of_bounds_write_is_rejected(tmp_path):
    _init_repo(tmp_path / "repo")
    omd = _mk(tmp_path)
    omd.declare("A", writes=["a/**"])
    omd.next_task("agA")
    omd.claim("agA", ["a/**"], task_id="A")
    s = omd.start("A", "agA")
    _write(s["worktree"], "a/x.py")          # 궤도 안 (OK)
    _write(s["worktree"], "b/foo.py")        # 궤도 밖 (위반!)
    omd.commit("A", "feat: a/x + (불법) b/foo")
    omd.finish("A")

    res = omd.connect("A")
    assert res["ok"] is False and res["reason"] == "writeset_violation", res
    assert res["offending"] == ["b/foo.py"], res
    # 통합 브랜치 불변 — merge 안 일어남(Phase A 거부, Phase B git 미실행).
    integ = Path(omd.integration_worktree)
    assert not (integ / "a" / "x.py").exists()
    assert not (integ / "b" / "foo.py").exists()
    log = subprocess.run(["git", "log", "--oneline", "main"], cwd=str(tmp_path / "repo"),
                         capture_output=True, text=True).stdout
    assert "CLOUD CONNECT A" not in log
    assert omd.store.get_task("A")["state"] != "MERGED"
    # merge_token 누수 0 (거부 시 토큰 안 잡았어야).
    assert omd.store.all_held_merge_tokens() == []


def test_connect_audit_git_failure_is_fail_closed_before_authority_mutation(
    tmp_path, monkeypatch
):
    """A Git read failure is not evidence that the candidate is in-bounds."""
    _init_repo(tmp_path / "repo")
    omd = _mk(tmp_path)
    omd.declare("A", writes=["a/**"])
    omd.next_task("agA")
    claimed = omd.claim("agA", ["a/**"], task_id="A")
    started = omd.start("A", "agA")
    _write(started["worktree"], "a/x.py")
    assert omd.commit("A", "feat: a/x")["ok"] is True
    omd.finish("A")
    task_before = omd.store.get_task("A")
    orbit_before = omd.store.get_orbit(claimed["orbit_id"])

    def unavailable(*_args, **_kwargs):
        raise GitError("diff authority unavailable")

    monkeypatch.setattr(omd.git, "changed_paths", unavailable)
    result = omd.connect("A")

    assert result["ok"] is False
    assert result["reason"] == "writeset_audit_error"
    assert "audit_error" in result
    assert omd.store.get_task("A") == task_before
    assert omd.store.get_orbit(claimed["orbit_id"]) == orbit_before
    assert omd.store.all_held_merge_tokens() == []


def test_barrier_audit_git_failure_is_fail_closed_before_authority_mutation(
    tmp_path, monkeypatch
):
    _init_repo(tmp_path / "repo")
    omd = _mk(tmp_path)
    omd.declare("A", writes=["a/**"])
    omd.next_task("agA")
    claimed = omd.claim("agA", ["a/**"], task_id="A")
    started = omd.start("A", "agA")
    _write(started["worktree"], "a/x.py")
    omd.commit("A", "feat: a/x")
    omd.finish("A")
    task_before = omd.store.get_task("A")
    orbit_before = omd.store.get_orbit(claimed["orbit_id"])

    def unavailable(*_args, **_kwargs):
        raise GitError("barrier diff authority unavailable")

    monkeypatch.setattr(omd.git, "changed_paths", unavailable)
    result = omd._barrier_connect_phase_a("A", claimed["fence"])

    assert result["ok"] is False
    assert result["reason"] == "writeset_audit_error"
    assert "audit_error" in result
    assert omd.store.get_task("A") == task_before
    assert omd.store.get_orbit(claimed["orbit_id"]) == orbit_before
    assert omd.store.all_held_merge_tokens() == []


def test_normal_connect_merges_only_the_audited_candidate_sha(tmp_path):
    """A late out-of-band branch commit cannot ride an already-audited intent."""
    _init_repo(tmp_path / "repo")
    omd = _mk(tmp_path)
    omd.declare("A", writes=["a/**"])
    omd.next_task("agA")
    omd.claim("agA", ["a/**"], task_id="A")
    started = omd.start("A", "agA")
    _write(started["worktree"], "a/x.py")
    omd.commit("A", "feat: audited a/x")
    omd.finish("A")

    phase_a = omd._connect_phase_a("A", None, None)
    assert phase_a["ok"] is True
    intent = phase_a["intent"]
    audited_tip = intent["branch_tip_sha"]

    _write(started["worktree"], "b/late.py", "LATE = True\n")
    _git(["add", "-A"], started["worktree"])
    _git(["commit", "-m", "late out-of-orbit commit"], started["worktree"])
    assert omd.git.branch_tip("omd/A") != audited_tip

    merge_sha, error = omd._connect_phase_b(intent)
    assert error is None
    result = omd._connect_phase_c(
        "A", phase_a["token_id"], intent, merge_sha, error
    )

    assert result["ok"] is True and result["state"] == "MERGED"
    integration = Path(omd.integration_worktree)
    assert (integration / "a" / "x.py").exists()
    assert not (integration / "b" / "late.py").exists()
    omd.git.assert_ancestor(audited_tip, result["merge_sha"], cwd=integration)


def test_barrier_connect_merges_only_the_audited_candidate_sha(tmp_path):
    """Barrier Phase A' carries the same immutable candidate authority."""
    _init_repo(tmp_path / "repo")
    omd = _mk(tmp_path)
    omd.declare("A", writes=["a/**"])
    omd.next_task("agA")
    claimed = omd.claim("agA", ["a/**"], task_id="A")
    started = omd.start("A", "agA")
    _write(started["worktree"], "a/x.py")
    omd.commit("A", "feat: audited barrier candidate")
    omd.finish("A")

    phase_a = omd._barrier_connect_phase_a("A", claimed["fence"])
    assert phase_a["ok"] is True
    intent = phase_a["intent"]
    audited_tip = intent["branch_tip_sha"]

    _write(started["worktree"], "b/barrier-late.py", "LATE = True\n")
    _git(["add", "-A"], started["worktree"])
    _git(["commit", "-m", "late barrier branch advance"], started["worktree"])
    assert omd.git.branch_tip("omd/A") != audited_tip

    merge_sha, error = omd._connect_phase_b(intent)
    assert error is None
    result = omd._connect_phase_c(
        "A", phase_a["token_id"], intent, merge_sha, error
    )

    assert result["ok"] is True and result["state"] == "MERGED"
    integration = Path(omd.integration_worktree)
    assert (integration / "a" / "x.py").exists()
    assert not (integration / "b" / "barrier-late.py").exists()
    omd.git.assert_ancestor(audited_tip, result["merge_sha"], cwd=integration)


def test_public_commit_is_rejected_after_connect_candidate_is_captured(tmp_path):
    _init_repo(tmp_path / "repo")
    omd = _mk(tmp_path)
    omd.declare("A", writes=["a/**"])
    omd.next_task("agA")
    omd.claim("agA", ["a/**"], task_id="A")
    started = omd.start("A", "agA")
    _write(started["worktree"], "a/x.py")
    omd.commit("A", "feat: candidate")
    omd.finish("A")
    phase_a = omd._connect_phase_a("A", None, None)
    assert phase_a["ok"] is True
    tip_before = omd.git.branch_tip("omd/A")

    _write(started["worktree"], "a/late.py")
    result = omd.commit("A", "must not advance captured candidate")

    assert result["ok"] is False
    assert result["reason"] == "connect_in_progress"
    assert omd.git.branch_tip("omd/A") == tip_before


def test_integration_base_drift_releases_attempt_for_retry(tmp_path):
    """An unrelated integration advance is a no-effect retry, not a stuck attempt."""
    _init_repo(tmp_path / "repo")
    omd = _mk(tmp_path)
    omd.declare("A", writes=["a/**"])
    omd.next_task("agA")
    claimed = omd.claim("agA", ["a/**"], task_id="A")
    started = omd.start("A", "agA")
    _write(started["worktree"], "a/x.py")
    omd.commit("A", "feat: candidate")
    omd.finish("A")
    phase_a = omd._connect_phase_a("A", "agA", claimed["fence"])
    assert phase_a["ok"] is True

    integration = omd._ensure_integration_wt()
    foreign_tip = omd.git.commit_empty_integration(
        integration, "independent integration advance"
    )
    merge_sha, error = omd._connect_phase_b(phase_a["intent"])
    assert merge_sha is None
    assert isinstance(error, GitIntegrationPreconditionError)

    result = omd._connect_phase_c(
        "A", phase_a["token_id"], phase_a["intent"], merge_sha, error
    )

    assert result["ok"] is False
    assert result["state"] == "DONE" and result["retryable"] is True
    assert result["reason"] == "integration_precondition_failed"
    assert omd.git.branch_tip("main") == foreign_tip
    assert omd.store.all_held_merge_tokens() == []
    orbit = omd.store.get_orbit(claimed["orbit_id"])
    assert orbit["state"] == "HELD" and orbit["merging"] == 0


def test_barrier_integration_base_drift_releases_attempt_for_retry(tmp_path):
    _init_repo(tmp_path / "repo")
    omd = _mk(tmp_path)
    omd.declare("A", writes=["a/**"])
    omd.next_task("agA")
    claimed = omd.claim("agA", ["a/**"], task_id="A")
    started = omd.start("A", "agA")
    _write(started["worktree"], "a/x.py")
    omd.commit("A", "feat: barrier candidate")
    omd.finish("A")
    phase_a = omd._barrier_connect_phase_a("A", claimed["fence"])
    assert phase_a["ok"] is True

    integration = omd._ensure_integration_wt()
    foreign_tip = omd.git.commit_empty_integration(
        integration, "independent barrier integration advance"
    )
    merge_sha, error = omd._connect_phase_b(phase_a["intent"])
    assert merge_sha is None
    assert isinstance(error, GitIntegrationPreconditionError)

    result = omd._connect_phase_c(
        "A", phase_a["token_id"], phase_a["intent"], merge_sha, error
    )

    assert result["state"] == "DONE" and result["retryable"] is True
    assert result["reason"] == "integration_precondition_failed"
    assert omd.git.branch_tip("main") == foreign_tip
    assert omd.store.all_held_merge_tokens() == []
    orbit = omd.store.get_orbit(claimed["orbit_id"])
    assert orbit["state"] == "HELD" and orbit["merging"] == 0


# ---------- 2) 궤도 안 쓰기만 → MERGED ----------
def test_within_bounds_write_merges(tmp_path):
    _init_repo(tmp_path / "repo")
    omd = _mk(tmp_path)
    omd.declare("A", writes=["a/**"])
    omd.next_task("agA")
    omd.claim("agA", ["a/**"], task_id="A")
    s = omd.start("A", "agA")
    _write(s["worktree"], "a/x.py")
    _write(s["worktree"], "a/sub/deep.py")   # 서브트리 안 — 여전히 덮임
    r = omd.commit("A", "feat: a/*")
    assert not r.get("writeset_violation"), r
    omd.finish("A")
    res = omd.connect("A")
    assert res["ok"] and res["state"] == "MERGED", res
    integ = Path(omd.integration_worktree)
    assert (integ / "a" / "x.py").exists() and (integ / "a" / "sub" / "deep.py").exists()


# ---------- 3) 다중 write-orbit 합집합 커버리지 ----------
def test_multiple_write_orbits_union_coverage(tmp_path):
    _init_repo(tmp_path / "repo")
    omd = _mk(tmp_path)
    omd.declare("A", writes=["a/**", "c/**"])
    omd.next_task("agA")
    # 두 개의 write-orbit (서로 다른 glob) — 합집합이 claimed write-set.
    omd.claim("agA", ["a/**"], task_id="A")
    omd.claim("agA", ["c/**"], task_id="A")
    s = omd.start("A", "agA")
    _write(s["worktree"], "a/x.py")
    _write(s["worktree"], "c/y.py")          # 둘째 궤도가 덮음
    omd.commit("A", "feat: a/x + c/y")
    omd.finish("A")
    res = omd.connect("A")
    assert res["ok"] and res["state"] == "MERGED", res
    integ = Path(omd.integration_worktree)
    assert (integ / "a" / "x.py").exists() and (integ / "c" / "y.py").exists()


def test_multiple_orbits_one_path_outside_union(tmp_path):
    """다중 궤도라도 합집합 밖 경로(d/)는 거부."""
    _init_repo(tmp_path / "repo")
    omd = _mk(tmp_path)
    omd.declare("A", writes=["a/**", "c/**"])
    omd.next_task("agA")
    omd.claim("agA", ["a/**"], task_id="A")
    omd.claim("agA", ["c/**"], task_id="A")
    s = omd.start("A", "agA")
    _write(s["worktree"], "a/x.py")
    _write(s["worktree"], "d/out.py")        # a/** 도 c/** 도 안 덮음
    omd.commit("A", "feat: a/x + (불법) d/out")
    omd.finish("A")
    res = omd.connect("A")
    assert res["ok"] is False and res["reason"] == "writeset_violation", res
    assert res["offending"] == ["d/out.py"], res


# ---------- 4) 궤도 밖으로의 rename → 거부 ----------
def test_rename_out_of_bounds_is_rejected(tmp_path):
    """a/old.py → b/new.py rename: --no-renames diff 가 삭제된 a/old.py(궤도 안)와 새
    b/new.py(궤도 밖) 둘 다 낸다 → 궤도 밖 b/new.py 가 거부 사유."""
    _init_repo(tmp_path / "repo")
    omd = _mk(tmp_path)
    omd.declare("A", writes=["a/**"])
    omd.next_task("agA")
    omd.claim("agA", ["a/**"], task_id="A")
    s = omd.start("A", "agA")
    wt = s["worktree"]
    _write(wt, "a/old.py", "x = 1\n")
    omd.commit("A", "seed a/old")
    # rename a/old.py -> b/new.py (궤도 밖으로 이동). git mv 는 목적지 디렉토리가 있어야 함.
    Path(wt, "b").mkdir(exist_ok=True)
    _git(["mv", "a/old.py", "b/new.py"], wt)
    omd.commit("A", "rename a/old -> b/new (불법: 궤도 밖)")
    omd.finish("A")
    res = omd.connect("A")
    assert res["ok"] is False and res["reason"] == "writeset_violation", res
    assert "b/new.py" in res["offending"], res
    assert not (Path(omd.integration_worktree) / "b" / "new.py").exists()


# ---------- 5) 이빨: 감사 우회 시 궤도 밖 쓰기가 머지됨(분열) ----------
def test_mutation_disabling_audit_lets_split_through(tmp_path, monkeypatch):
    """변이검증: write-set 감사를 무력화(_writeset_audit→항상 위반없음)하면 궤도 밖 쓰기가
    응결되어 §D10이 경고하는 분열이 일어난다 — 그래서 #1 테스트가 RED가 됨을 실증.
    (여기선 우회 후 *머지가 통과함*을 직접 보여 게이트의 이빨을 확인.)"""
    _init_repo(tmp_path / "repo")
    omd = _mk(tmp_path)
    from omd_server.core import WritesetAudit, WritesetVerdict
    # 감사 무력화 = 항상 CLEAN(궤도 밖 경로 0) 판정을 강제 → 궤도 밖 쓰기가 통과해버림.
    monkeypatch.setattr(omd, "_writeset_audit",
                        lambda *a, **k: WritesetAudit(WritesetVerdict.CLEAN))
    omd.declare("A", writes=["a/**"])
    omd.next_task("agA")
    omd.claim("agA", ["a/**"], task_id="A")
    s = omd.start("A", "agA")
    _write(s["worktree"], "a/x.py")
    _write(s["worktree"], "b/foo.py")        # 궤도 밖 — 감사 꺼졌으므로 통과해버림
    omd.commit("A", "feat: a/x + b/foo (감사 무력화)")
    omd.finish("A")
    res = omd.connect("A")
    # 감사가 꺼지면 분열이 머지된다(통합에 궤도 밖 b/foo.py 가 들어옴).
    assert res["ok"] and res["state"] == "MERGED", res
    assert (Path(omd.integration_worktree) / "b" / "foo.py").exists(), \
        "감사 무력화인데 궤도 밖 쓰기가 안 머지됨 → 테스트가 분열을 못 잡는다(가짜 green 위험)"
