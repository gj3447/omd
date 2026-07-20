"""omd-merge 단독 CLI — checked-merge green-gate를 coord DB 없이 집행한다."""

from __future__ import annotations

import json
import shlex
import subprocess
import sys
from pathlib import Path

from omd_server import merge_cli


def _git(args: list[str], cwd: Path, *, check: bool = True) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["git", *args], cwd=str(cwd), check=check, capture_output=True, text=True,
    )


def _fixture(tmp_path: Path) -> tuple[Path, str]:
    """main이 체크아웃된 repo + feature 브랜치(linked worktree에서 1커밋)."""
    repo = tmp_path / "repo"
    repo.mkdir()
    _git(["init", "-b", "main"], repo)
    _git(["config", "user.name", "test"], repo)
    _git(["config", "user.email", "test@example.com"], repo)
    (repo / "base.txt").write_text("base\n")
    _git(["add", "-A"], repo)
    _git(["commit", "-m", "base"], repo)
    base = _git(["rev-parse", "HEAD"], repo).stdout.strip()

    feature_wt = tmp_path / "feature-wt"
    _git(["worktree", "add", "-b", "feature", str(feature_wt), "main"], repo)
    (feature_wt / "feature.txt").write_text("feature\n")
    _git(["add", "-A"], feature_wt)
    _git(["commit", "-m", "feature"], feature_wt)
    return repo, base


def _verify_cmd(tmp_path: Path, body: str, name: str) -> str:
    """shlex.split 가능한 verify 명령 문자열(스크립트 파일 경유 — 인용부호 지옥 회피)."""
    script = tmp_path / name
    script.write_text(body)
    return f"{shlex.quote(sys.executable)} {shlex.quote(str(script))}"


def _assert_restored(repo: Path, original_head: str) -> None:
    assert _git(["rev-parse", "HEAD"], repo).stdout.strip() == original_head
    assert _git(["rev-parse", "-q", "--verify", "MERGE_HEAD"], repo,
                check=False).returncode != 0
    assert _git(["status", "--porcelain", "--untracked-files=no"],
                repo).stdout.strip() == ""


def test_green_verify_merges_and_advances_head(tmp_path: Path) -> None:
    repo, base = _fixture(tmp_path)
    verify = _verify_cmd(
        tmp_path,
        "from pathlib import Path\n"
        "assert Path('feature.txt').read_text() == 'feature\\n'\n",
        "green.py",
    )

    rc = merge_cli.main([str(repo), "--source", "feature", "--verify-cmd", verify])

    assert rc == 0
    head = _git(["rev-parse", "HEAD"], repo).stdout.strip()
    assert head != base
    assert _git(["rev-parse", "main"], repo).stdout.strip() == head
    # --no-ff merge commit: 부모 2개
    assert _git(["rev-list", "--parents", "-n", "1", head], repo).stdout.count(" ") == 2
    assert (repo / "feature.txt").read_text() == "feature\n"


def test_red_verify_aborts_restores_head_and_exits_2(tmp_path: Path) -> None:
    repo, base = _fixture(tmp_path)
    verify = _verify_cmd(tmp_path, "import sys\nsys.exit(7)\n", "red.py")

    rc = merge_cli.main([str(repo), "--source", "feature", "--verify-cmd", verify])

    assert rc == 2
    _assert_restored(repo, base)
    assert not (repo / "feature.txt").exists()


def test_json_output_shape_green_and_red(tmp_path: Path, capsys) -> None:
    repo, base = _fixture(tmp_path)
    green = _verify_cmd(tmp_path, "", "green.py")

    rc = merge_cli.main([str(repo), "--source", "feature",
                         "--verify-cmd", green, "--json"])
    out = json.loads(capsys.readouterr().out)
    assert rc == 0
    assert out["status"] == "merged"
    assert out["target"] == "main"
    assert out["source"] == "feature"
    assert out["original_head"] == base
    assert out["merged_sha"] == _git(["rev-parse", "HEAD"], repo).stdout.strip()
    assert out["up_to_date"] is False

    # 같은 소스 재응결 시도 → red verify로 abort JSON 형태 검증 (새 feature 커밋 추가)
    feature_wt = tmp_path / "feature-wt"
    (feature_wt / "more.txt").write_text("more\n")
    _git(["add", "-A"], feature_wt)
    _git(["commit", "-m", "more"], feature_wt)
    merged_head = _git(["rev-parse", "HEAD"], repo).stdout.strip()
    red = _verify_cmd(tmp_path, "import sys\nprint('boom')\nsys.exit(3)\n", "red.py")

    rc = merge_cli.main([str(repo), "--source", "feature",
                         "--verify-cmd", red, "--json"])
    out = json.loads(capsys.readouterr().out)
    assert rc == 2
    assert out["status"] == "verify_aborted"
    assert out["verify_returncode"] == 3
    assert "boom" in out["verify_stdout"]
    assert out["restored_head"] == merged_head
    assert out["rollback_proven"] is True
    _assert_restored(repo, merged_head)


def test_conflict_aborts_and_exits_3(tmp_path: Path) -> None:
    repo, base = _fixture(tmp_path)
    # 양쪽에서 base.txt를 다르게 수정 → 충돌
    (repo / "base.txt").write_text("main side\n")
    _git(["add", "-A"], repo)
    _git(["commit", "-m", "main change"], repo)
    main_head = _git(["rev-parse", "HEAD"], repo).stdout.strip()
    feature_wt = tmp_path / "feature-wt"
    (feature_wt / "base.txt").write_text("feature side\n")
    _git(["add", "-A"], feature_wt)
    _git(["commit", "-m", "feature change"], feature_wt)
    verify = _verify_cmd(tmp_path, "", "green.py")

    rc = merge_cli.main([str(repo), "--source", "feature", "--verify-cmd", verify])

    assert rc == 3
    _assert_restored(repo, main_head)


def test_missing_verify_cmd_is_usage_error_exit_1(tmp_path: Path) -> None:
    repo, _base = _fixture(tmp_path)
    rc = merge_cli.main([str(repo), "--source", "feature"])
    assert rc == 1


def test_no_verify_bypasses_gate_explicitly(tmp_path: Path) -> None:
    repo, base = _fixture(tmp_path)
    rc = merge_cli.main([str(repo), "--source", "feature", "--no-verify"])
    assert rc == 0
    assert _git(["rev-parse", "HEAD"], repo).stdout.strip() != base
    assert (repo / "feature.txt").read_text() == "feature\n"


def test_dirty_tree_precondition_exits_1(tmp_path: Path) -> None:
    repo, base = _fixture(tmp_path)
    (repo / "base.txt").write_text("uncommitted\n")
    verify = _verify_cmd(tmp_path, "", "green.py")
    rc = merge_cli.main([str(repo), "--source", "feature", "--verify-cmd", verify])
    assert rc == 1
    # 사용자 변경은 보존돼야 한다(gate는 dirty tree를 건드리지 않고 거부만)
    assert (repo / "base.txt").read_text() == "uncommitted\n"
    assert _git(["rev-parse", "HEAD"], repo).stdout.strip() == base
