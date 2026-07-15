"""GitRepo pre-commit integration gate — green만 main에 응결한다."""

from __future__ import annotations

import subprocess
import sys
import time
from pathlib import Path

import pytest

from omd_server.gitio import (
    GitIntegrationCheckFailed,
    GitIntegrationCheckTimeout,
    GitIntegrationMutation,
    GitIntegrationPreconditionError,
    GitRepo,
    GitRollbackError,
)


def _git(args: list[str], cwd: Path, *, check: bool = True) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["git", *args], cwd=str(cwd), check=check, capture_output=True, text=True,
    )


def _fixture(tmp_path: Path) -> tuple[GitRepo, Path, Path, str]:
    """사용자 root는 dev, main은 전용 integration worktree에만 checkout한다."""
    repo = tmp_path / "repo"
    repo.mkdir()
    _git(["init", "-b", "main"], repo)
    _git(["config", "user.name", "test"], repo)
    _git(["config", "user.email", "test@example.com"], repo)
    (repo / "base.txt").write_text("base\n")
    _git(["add", "-A"], repo)
    _git(["commit", "-m", "base"], repo)
    base = _git(["rev-parse", "HEAD"], repo).stdout.strip()
    _git(["checkout", "-b", "dev"], repo)

    feature_wt = tmp_path / "feature-wt"
    _git(["worktree", "add", "-b", "feature", str(feature_wt), "main"], repo)
    (feature_wt / "feature.txt").write_text("feature\n")
    _git(["add", "-A"], feature_wt)
    _git(["commit", "-m", "feature"], feature_wt)

    git = GitRepo(str(repo))
    integration_wt = tmp_path / "integration-wt"
    git.ensure_integration_worktree(str(integration_wt), "main")
    return git, repo, integration_wt, base


def _assert_rolled_back(integration_wt: Path, original_head: str) -> None:
    assert _git(["rev-parse", "HEAD"], integration_wt).stdout.strip() == original_head
    assert _git(["rev-parse", "-q", "--verify", "MERGE_HEAD"], integration_wt,
                check=False).returncode != 0
    assert _git(["status", "--porcelain", "--untracked-files=no"],
                integration_wt).stdout.strip() == ""


def test_green_check_commits_candidate_merge_only_after_check(tmp_path: Path) -> None:
    git, repo, integration_wt, base = _fixture(tmp_path)
    argv = [
        sys.executable,
        "-c",
        "from pathlib import Path; assert Path('feature.txt').read_text() == 'feature\\n'",
    ]

    sha = git.merge_into(
        str(integration_wt), "main", "feature", "checked merge",
        check_argv=argv, check_timeout=2,
    )

    assert sha != base
    assert _git(["rev-parse", "main"], repo).stdout.strip() == sha
    assert _git(["rev-list", "--parents", "-n", "1", sha], repo).stdout.count(" ") == 2
    assert (integration_wt / "feature.txt").read_text() == "feature\n"
    assert _git(["rev-parse", "--abbrev-ref", "HEAD"], repo).stdout.strip() == "dev"


@pytest.mark.parametrize("checked", [False, True])
def test_merge_publication_cas_preserves_external_ref_winner(
    tmp_path: Path, monkeypatch, checked: bool,
) -> None:
    git, repo, integration_wt, base = _fixture(tmp_path)
    external_wt = tmp_path / "external-wt"
    _git(["worktree", "add", "-b", "external", str(external_wt), "main"], repo)
    (external_wt / "external.txt").write_text("external winner\n")
    _git(["add", "external.txt"], external_wt)
    _git(["commit", "-m", "external winner"], external_wt)
    external_sha = _git(["rev-parse", "HEAD"], external_wt).stdout.strip()
    original_git = git._git
    injected = False

    def race_before_cas(*args, **kwargs):
        nonlocal injected
        if not injected and args[:2] == ("update-ref", "refs/heads/main"):
            injected = True
            _git(["update-ref", "refs/heads/main", external_sha, base], repo)
        return original_git(*args, **kwargs)

    monkeypatch.setattr(git, "_git", race_before_cas)
    options = (
        {"check_argv": [sys.executable, "-c", "pass"], "check_timeout": 2}
        if checked else {}
    )
    with pytest.raises(GitIntegrationPreconditionError, match="publication"):
        git.merge_into(
            str(integration_wt), "main", "feature", "must lose CAS",
            expected_base=base, **options,
        )

    assert injected
    assert _git(["rev-parse", "main"], repo).stdout.strip() == external_sha
    assert (integration_wt / "external.txt").read_text() == "external winner\n"
    assert not (integration_wt / "feature.txt").exists()
    assert _git(
        ["rev-parse", "-q", "--verify", "MERGE_HEAD"], integration_wt,
        check=False,
    ).returncode != 0
    assert _git(
        ["status", "--porcelain", "--untracked-files=no"], integration_wt
    ).stdout.strip() == ""


def test_red_check_exposes_bounded_output_and_does_not_commit(tmp_path: Path) -> None:
    git, _repo, integration_wt, base = _fixture(tmp_path)
    argv = [
        sys.executable,
        "-c",
        "import sys; print('O' * 1000); print('E' * 1000, file=sys.stderr); sys.exit(7)",
    ]

    with pytest.raises(GitIntegrationCheckFailed) as caught:
        git.merge_into(
            str(integration_wt), "main", "feature", "must not land",
            check_argv=argv, check_timeout=2, check_output_limit=80,
        )

    error = caught.value
    assert error.returncode == 7
    assert len(error.stdout.encode()) <= 80 and "truncated" in error.stdout
    assert len(error.stderr.encode()) <= 80 and "truncated" in error.stderr
    _assert_rolled_back(integration_wt, base)


def test_red_check_rollback_never_clobbers_external_ref_winner(tmp_path: Path) -> None:
    git, repo, integration_wt, base = _fixture(tmp_path)
    external_wt = tmp_path / "external-red-wt"
    _git(["worktree", "add", "-b", "external-red", str(external_wt), "main"], repo)
    (external_wt / "external.txt").write_text("external red winner\n")
    _git(["add", "external.txt"], external_wt)
    _git(["commit", "-m", "external red winner"], external_wt)
    external_sha = _git(["rev-parse", "HEAD"], external_wt).stdout.strip()
    argv = [
        sys.executable, "-c",
        (
            "import subprocess, sys; "
            f"subprocess.run(['git','update-ref','refs/heads/main',"
            f"'{external_sha}','{base}'], check=True); sys.exit(7)"
        ),
    ]

    with pytest.raises(GitRollbackError) as caught:
        git.merge_into(
            str(integration_wt), "main", "feature", "red ref race",
            check_argv=argv, check_timeout=2, expected_base=base,
        )

    assert isinstance(caught.value.cause, GitIntegrationCheckFailed)
    assert _git(["rev-parse", "main"], repo).stdout.strip() == external_sha
    assert (integration_wt / "external.txt").read_text() == "external red winner\n"
    assert not (integration_wt / "feature.txt").exists()
    assert _git(
        ["rev-parse", "-q", "--verify", "MERGE_HEAD"], integration_wt,
        check=False,
    ).returncode != 0


def test_timed_out_check_is_killed_and_candidate_is_aborted(tmp_path: Path) -> None:
    git, _repo, integration_wt, base = _fixture(tmp_path)
    argv = [
        sys.executable,
        "-c",
        "import time; print('started', flush=True); time.sleep(10)",
    ]

    with pytest.raises(GitIntegrationCheckTimeout) as caught:
        git.merge_into(
            str(integration_wt), "main", "feature", "timeout",
            check_argv=argv, check_timeout=0.05,
        )

    assert "started" in caught.value.stdout
    _assert_rolled_back(integration_wt, base)


def test_timeout_covers_descendants_that_inherit_output_pipes(tmp_path: Path) -> None:
    git, _repo, integration_wt, base = _fixture(tmp_path)
    argv = [
        sys.executable,
        "-c",
        (
            "import subprocess, sys; "
            "subprocess.Popen([sys.executable, '-c', "
            "'import time; time.sleep(30)']); "
            "print('spawned', flush=True)"
        ),
    ]

    started = time.monotonic()
    with pytest.raises(GitIntegrationCheckTimeout) as caught:
        git.merge_into(
            str(integration_wt), "main", "feature", "descendant timeout",
            check_argv=argv, check_timeout=0.1,
        )

    assert time.monotonic() - started < 2.0
    assert "spawned" in caught.value.stdout
    _assert_rolled_back(integration_wt, base)


def test_shell_metacharacters_are_passed_as_one_literal_argument(tmp_path: Path) -> None:
    git, _repo, integration_wt, _base = _fixture(tmp_path)
    captured = tmp_path / "captured.txt"
    injected = tmp_path / "SHOULD_NOT_EXIST"
    payload = f"; touch {injected}"
    argv = [
        sys.executable,
        "-c",
        "from pathlib import Path; import sys; Path(sys.argv[1]).write_text(sys.argv[2])",
        str(captured),
        payload,
    ]

    git.merge_into(
        str(integration_wt), "main", "feature", "literal argv",
        check_argv=argv, check_timeout=2,
    )

    assert captured.read_text() == payload
    assert not injected.exists(), "operator argv must never be interpreted by a shell"


@pytest.mark.parametrize("bad_timeout", [float("nan"), float("inf"), float("-inf")])
def test_check_timeout_must_be_finite(tmp_path: Path, bad_timeout: float) -> None:
    git = GitRepo(str(tmp_path))
    with pytest.raises(ValueError, match="positive and finite"):
        git.merge_into(
            str(tmp_path / "missing"), "main", "feature", "invalid timeout",
            check_argv=[sys.executable, "-c", "pass"], check_timeout=bad_timeout,
        )


@pytest.mark.parametrize("stage", [False, True])
def test_green_check_cannot_mutate_tracked_worktree_or_index(
    tmp_path: Path, stage: bool,
) -> None:
    git, _repo, integration_wt, base = _fixture(tmp_path)
    code = (
        "from pathlib import Path; import subprocess; "
        "Path('base.txt').write_text('mutated\\n'); "
        + ("subprocess.run(['git', 'add', 'base.txt'], check=True)" if stage else "")
    )

    if stage:
        with pytest.raises(GitIntegrationMutation) as caught:
            git.merge_into(
                str(integration_wt), "main", "feature", "mutating checker",
                check_argv=[sys.executable, "-c", code], check_timeout=2,
            )
        assert "index" in " ".join(caught.value.mutations)
        _assert_rolled_back(integration_wt, base)
    else:
        # git merge --abort는 merge 시작 뒤 생긴 unstaged 편집을 덮어쓰지 않는다. OMD도
        # reset --hard로 증거를 지우지 않고, 복구 불증명을 typed fail-stop으로 올린다.
        with pytest.raises(GitRollbackError) as caught:
            git.merge_into(
                str(integration_wt), "main", "feature", "mutating checker",
                check_argv=[sys.executable, "-c", code], check_timeout=2,
            )
        assert isinstance(caught.value.cause, GitIntegrationMutation)
        assert "tracked tree is not clean" in " ".join(caught.value.problems)
        assert _git(["rev-parse", "HEAD"], integration_wt).stdout.strip() == base


@pytest.mark.parametrize("metadata", ["MERGE_HEAD", "MERGE_MSG"])
def test_green_check_cannot_mutate_merge_metadata(
    tmp_path: Path, metadata: str,
) -> None:
    git, repo, integration_wt, base = _fixture(tmp_path)
    replacement = base if metadata == "MERGE_HEAD" else "forged message\n"
    code = (
        "from pathlib import Path; import subprocess; "
        f"raw=subprocess.check_output(['git','rev-parse','--git-path',"
        f"'{metadata}'], text=True).strip(); "
        f"Path(raw).write_text({replacement!r})"
    )

    with pytest.raises(GitIntegrationMutation) as caught:
        git.merge_into(
            str(integration_wt), "main", "feature", "trusted merge message",
            check_argv=[sys.executable, "-c", code], check_timeout=2,
        )

    assert metadata in " ".join(caught.value.mutations)
    assert _git(["rev-parse", "main"], repo).stdout.strip() == base
    _assert_rolled_back(integration_wt, base)


def test_green_check_cannot_retarget_symbolic_head(tmp_path: Path) -> None:
    git, repo, integration_wt, base = _fixture(tmp_path)
    code = (
        "import subprocess; "
        "subprocess.run(['git','branch','checker-escape','HEAD'],check=True); "
        "subprocess.run(['git','symbolic-ref','HEAD',"
        "'refs/heads/checker-escape'],check=True)"
    )

    with pytest.raises(GitRollbackError) as caught:
        git.merge_into(
            str(integration_wt), "main", "feature", "trusted merge message",
            check_argv=[sys.executable, "-c", code], check_timeout=2,
        )

    assert isinstance(caught.value.cause, GitIntegrationMutation)
    assert "symbolic HEAD" in " ".join(caught.value.cause.mutations)
    assert "symbolic HEAD" in " ".join(caught.value.problems)
    assert _git(["rev-parse", "main"], repo).stdout.strip() == base
