"""P1 — OMD 우회 감지 게이트 테스트. happy + 우회 4종 + first-parent 구분 + git실패 fail-loud + hook."""
import subprocess
from pathlib import Path

from omd_server.bypass_audit import Kind, bypass_audit, classify, gate
from omd_server.bypass_hook import generate_pre_push_hook, install_pre_push_hook


def _git(args, cwd):
    subprocess.run(["git", *args], cwd=str(cwd), check=True, capture_output=True, text=True)


def _out(args, cwd):
    return subprocess.run(["git", *args], cwd=str(cwd), capture_output=True, text=True).stdout.strip()


def _init(root):
    root.mkdir()
    _git(["init", "-b", "main"], root)
    _git(["config", "user.name", "t"], root)
    _git(["config", "user.email", "t@t"], root)
    (root / "base.txt").write_text("base\n")
    _git(["add", "-A"], root)
    _git(["commit", "-m", "base"], root)


def _omd_connect(root, task, fname):
    """OMD 정상 응결 시뮬: feature 브랜치 → --no-ff 머지 + OMD-Connect trailer + 작성자=omd
    (gitio._IDENT 와 동일 — 진짜 OMD 머지의 작성자)."""
    _git(["checkout", "-b", f"omd/{task}", "main"], root)
    (root / fname).write_text("x\n")
    _git(["add", "-A"], root)
    _git(["commit", "-m", f"feat {task} (drop-let, no trailer)"], root)
    _git(["checkout", "main"], root)
    _git(["-c", "user.name=omd", "-c", "user.email=omd@acme", "merge", "--no-ff",
          "-m", f"CLOUD CONNECT {task}\n\nOMD-Connect: {task}", f"omd/{task}"], root)


def _bypass_direct(root, fname):
    _git(["checkout", "main"], root)
    (root / fname).write_text("y\n")
    _git(["add", "-A"], root)
    _git(["commit", "-m", "직접커밋(OMD 우회)"], root)


def test_all_omd_connect_is_clean_GO(tmp_path):
    r = tmp_path / "repo"; _init(r)
    since = _out(["rev-parse", "HEAD"], r)
    _omd_connect(r, "T1", "a.txt"); _omd_connect(r, "T2", "b.txt")
    rep = bypass_audit(str(r), "main", since)
    assert rep.clean and len(rep.omd_connect) == 2 and rep.adoption_ratio == 1.0
    assert gate(str(r), "main", since) == 0


def test_direct_commit_is_bypass_NO_GO(tmp_path):
    r = tmp_path / "repo"; _init(r)
    since = _out(["rev-parse", "HEAD"], r)
    _omd_connect(r, "T1", "a.txt"); _bypass_direct(r, "hack.txt")
    rep = bypass_audit(str(r), "main", since)
    assert not rep.clean and any(k is Kind.DIRECT_COMMIT for _, k in rep.bypass)
    assert gate(str(r), "main", since) == 1


def test_foreign_merge_is_bypass(tmp_path):
    """git pull 류 = trailer 없는 머지."""
    r = tmp_path / "repo"; _init(r)
    since = _out(["rev-parse", "HEAD"], r)
    _git(["checkout", "-b", "other", "main"], r)
    (r / "o.txt").write_text("o\n"); _git(["add", "-A"], r); _git(["commit", "-m", "other"], r)
    _git(["checkout", "main"], r)
    _git(["merge", "--no-ff", "-m", "Merge other (no trailer)", "other"], r)
    rep = bypass_audit(str(r), "main", since)
    assert any(k is Kind.FOREIGN_MERGE for _, k in rep.bypass)
    assert gate(str(r), "main", since) == 1


def test_forged_trailer_non_merge_is_bypass(tmp_path):
    """non-merge 에 OMD-Connect trailer 위조 = 우회(gitio 는 --no-ff 머지만 박음)."""
    r = tmp_path / "repo"; _init(r)
    since = _out(["rev-parse", "HEAD"], r)
    _git(["checkout", "main"], r)
    (r / "f.txt").write_text("f\n"); _git(["add", "-A"], r)
    _git(["commit", "-m", "fake\n\nOMD-Connect: T99"], r)
    rep = bypass_audit(str(r), "main", since)
    assert any(k is Kind.FORGED_TRAILER for _, k in rep.bypass)
    assert gate(str(r), "main", since) == 1


def test_first_parent_vs_all_distinction(tmp_path):
    """드롭릿 feature 커밋(trailer 없음)은 머지에 흡수돼 first-parent 에서 제외 → 오탐 0."""
    r = tmp_path / "repo"; _init(r)
    since = _out(["rev-parse", "HEAD"], r)
    _omd_connect(r, "T1", "a.txt")   # feat T1(trailer 없음) + merge(trailer)
    rep = bypass_audit(str(r), "main", since)
    assert rep.clean, [(c.subject, k) for c, k in rep.bypass]


def test_warn_only_allows_despite_bypass(tmp_path):
    """warn_only: 우회 있어도 경고만 하고 GO(0) — 채택 0% 브랜치 안전 적용용."""
    r = tmp_path / "repo"; _init(r)
    since = _out(["rev-parse", "HEAD"], r)
    _bypass_direct(r, "hack.txt")
    assert gate(str(r), "main", since) == 1                      # enforce → NO_GO
    assert gate(str(r), "main", since, warn_only=True) == 0      # warn → GO(경고만)


def test_git_failure_is_fail_loud(tmp_path):
    """잘못된 ref → 빈 리포트로 삼키지 않고 NO_GO(2). silent skip 금지."""
    r = tmp_path / "repo"; _init(r)
    assert gate(str(r), "nonexistent-branch-xyz", "HEAD") == 2


def test_adoption_ratio(tmp_path):
    r = tmp_path / "repo"; _init(r)
    since = _out(["rev-parse", "HEAD"], r)
    _omd_connect(r, "T1", "a.txt"); _omd_connect(r, "T2", "b.txt")
    _omd_connect(r, "T3", "c.txt"); _bypass_direct(r, "h.txt")
    rep = bypass_audit(str(r), "main", since)
    assert abs(rep.adoption_ratio - 0.75) < 1e-9   # 3 omd / (3+1)


def test_pre_push_hook_generates_and_installs(tmp_path):
    r = tmp_path / "repo"; _init(r)
    # 격리: repo-local hooksPath = .git/hooks (글로벌 core.hooksPath 간섭/덮어쓰기 차단)
    _git(["config", "core.hooksPath", ".git/hooks"], r)
    h = generate_pre_push_hook("main", "HEAD~1", python="python3")
    assert "refs/heads/main" in h and "bypass_hook" in h
    p = install_pre_push_hook(str(r), "main", "")
    # ⚠️ 반드시 repo-local .git/hooks 에 설치(글로벌 hooksPath 덮어쓰기 footgun 회귀가드)
    assert Path(p).exists() and Path(p).name == "pre-push"
    assert (Path(r) / ".git" / "hooks" / "pre-push").resolve() == Path(p).resolve()


def test_pre_push_hook_rejects_bypass_push(tmp_path):
    """functional: 설치된 hook 이 우회 커밋 push 는 거부하고 OMD-connect push 는 허용."""
    import sys
    r = tmp_path / "repo"; _init(r)
    _git(["config", "core.hooksPath", ".git/hooks"], r)   # 격리(글로벌 간섭 차단)
    since = _out(["rev-parse", "HEAD"], r)
    install_pre_push_hook(str(r), "main", since, python=sys.executable)
    remote = tmp_path / "remote.git"; remote.mkdir()
    _git(["init", "--bare", "-b", "main"], remote)
    _git(["remote", "add", "origin", str(remote)], r)
    _omd_connect(r, "T1", "a.txt")
    _git(["push", "origin", "main"], r)   # OMD-connect → 통과
    _bypass_direct(r, "hack.txt")         # 직접커밋(우회)
    res = subprocess.run(["git", "push", "origin", "main"], cwd=str(r),
                         capture_output=True, text=True)
    assert res.returncode != 0   # hook 이 우회 push 거부(fail-loud)


def test_forged_merge_wrong_author_is_bypass(tmp_path):
    """수동 위조: 누가 직접 `git merge --no-ff -m '..OMD-Connect: X'`(작성자≠omd)로 가짜 응결을
    만들어 우회. 작성자 신원 검사로 FORGED_MERGE(bypass) 분류 — false-green 차단."""
    r = tmp_path / "repo"; _init(r)
    since = _out(["rev-parse", "HEAD"], r)
    _git(["checkout", "-b", "side", "main"], r)
    (r / "evil.txt").write_text("evil\n"); _git(["add", "-A"], r); _git(["commit", "-m", "evil"], r)
    _git(["checkout", "main"], r)
    # 작성자 omd 를 *안* 씀(기본 user.name=t) → 위조 머지
    _git(["merge", "--no-ff", "-m", "CLOUD CONNECT faketask\n\nOMD-Connect: faketask", "side"], r)
    rep = bypass_audit(str(r), "main", since)
    assert any(k is Kind.FORGED_MERGE for _, k in rep.bypass), [(c.subject, k) for c, k in rep.bypass]
    assert gate(str(r), "main", since) == 1   # 위조 머지도 NO_GO


def test_classify_unit():
    from omd_server.bypass_audit import Commit
    assert classify(Commit("s", (), (), "omd", "root")) is Kind.ROOT
    assert classify(Commit("s", ("p1", "p2"), ("T1",), "omd", "m")) is Kind.OMD_CONNECT
    assert classify(Commit("s", ("p1",), (), "omd", "c")) is Kind.DIRECT_COMMIT
    assert classify(Commit("s", ("p1", "p2"), (), "omd", "m")) is Kind.FOREIGN_MERGE
    assert classify(Commit("s", ("p1",), ("T1",), "omd", "c")) is Kind.FORGED_TRAILER
    # 머지+trailer 지만 작성자≠omd = 위조 머지(bypass)
    assert classify(Commit("s", ("p1", "p2"), ("T1",), "attacker", "m")) is Kind.FORGED_MERGE
