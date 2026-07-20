"""overlap_baserate 존속재판 오라클 테스트 — 합성 git repo 로 검출/미검출/스키마 고정.

합성 repo: subprocess git + GIT_AUTHOR_EMAIL / GIT_COMMITTER_* env 로 저자 제어,
GIT_AUTHOR_DATE / GIT_COMMITTER_DATE 로 타임스탬프 제어.
"""
import json
import os
import subprocess
import sys
import time

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scripts"))

from overlap_baserate import analyze_repo, main  # noqa: E402

NOW = int(time.time())


def _git(repo, *args, env=None):
    e = os.environ.copy()
    if env:
        e.update(env)
    subprocess.run(["git", "-C", str(repo), *args], check=True, capture_output=True, env=e)


def _commit(repo, filename, content, email, ts):
    """filename 에 content append 후 email/ts 로 커밋."""
    path = repo / filename
    with open(path, "a") as f:
        f.write(content + "\n")
    env = {
        "GIT_AUTHOR_NAME": email.split("@")[0],
        "GIT_AUTHOR_EMAIL": email,
        "GIT_AUTHOR_DATE": f"{ts} +0000",
        "GIT_COMMITTER_NAME": email.split("@")[0],
        "GIT_COMMITTER_EMAIL": email,
        "GIT_COMMITTER_DATE": f"{ts} +0000",
    }
    _git(repo, "add", filename, env=env)
    _git(repo, "commit", "-m", f"edit {filename} by {email}", env=env)


@pytest.fixture()
def synth_repo(tmp_path):
    repo = tmp_path / "synth"
    repo.mkdir()
    _git(repo, "init", "-q")
    return repo


def test_two_authors_same_file_within_window_detected(synth_repo):
    """① window(72h) 내 서로 다른 두 저자의 동일파일 수정 → overlap 검출."""
    _commit(synth_repo, "shared.py", "a", "alice@example.com", NOW - 3600 * 10)
    _commit(synth_repo, "shared.py", "b", "bob@example.com", NOW - 3600 * 5)
    r = analyze_repo(str(synth_repo), window_hours=72, since="30 days ago")
    assert r["commits"] == 2
    assert r["authors"] == 2
    assert r["files_seen"] == 1
    assert r["files_multi_author"] == 1
    assert r["files_overlap_within_window"] == 1
    assert r["overlap_ratio"] == 1.0
    assert r["top_overlap_files"] == [{"file": "shared.py", "pair_count": 1}]


def test_two_authors_same_file_outside_window_not_detected(synth_repo):
    """② 두 저자 동일파일이지만 간격 100h > window 72h → multi_author=1, overlap=0."""
    _commit(synth_repo, "shared.py", "a", "alice@example.com", NOW - 3600 * 110)
    _commit(synth_repo, "shared.py", "b", "bob@example.com", NOW - 3600 * 10)
    r = analyze_repo(str(synth_repo), window_hours=72, since="30 days ago")
    assert r["files_multi_author"] == 1
    assert r["files_overlap_within_window"] == 0
    assert r["overlap_ratio"] == 0.0
    assert r["top_overlap_files"] == []


def test_single_author_only_zero_overlap(synth_repo):
    """③ 단일저자가 같은 파일을 아무리 자주 고쳐도 overlap 0."""
    for i in range(4):
        _commit(synth_repo, "solo.py", f"edit{i}", "alice@example.com", NOW - 3600 * (i + 1))
    r = analyze_repo(str(synth_repo), window_hours=72, since="30 days ago")
    assert r["commits"] == 4
    assert r["authors"] == 1
    assert r["files_seen"] == 1
    assert r["files_multi_author"] == 0
    assert r["files_overlap_within_window"] == 0
    assert r["overlap_ratio"] == 0.0


def test_json_schema(synth_repo, capsys):
    """④ --json 출력 스키마: results 리스트 + repo별 필수 키 + caveat/limitation 문구."""
    _commit(synth_repo, "shared.py", "a", "alice@example.com", NOW - 3600 * 10)
    _commit(synth_repo, "shared.py", "b", "bob@example.com", NOW - 3600 * 5)
    rc = main([str(synth_repo), "--window-hours", "72", "--since", "30 days ago", "--json"])
    assert rc == 0
    payload = json.loads(capsys.readouterr().out)
    assert isinstance(payload["results"], list) and len(payload["results"]) == 1
    r = payload["results"][0]
    required = {
        "repo", "since", "window_hours", "commits", "authors", "files_seen",
        "files_multi_author", "files_overlap_within_window", "overlap_ratio",
        "top_overlap_files", "limitations", "identity_caveat",
    }
    assert required <= set(r.keys())
    assert r["window_hours"] == 72.0
    assert isinstance(r["top_overlap_files"], list)
    assert all({"file", "pair_count"} <= set(e.keys()) for e in r["top_overlap_files"])
    assert "rename" in r["limitations"][0]
    assert "identity" in r["identity_caveat"]


def test_human_output_mentions_caveats(synth_repo, capsys):
    """기본(사람) 출력에도 rename 한계 + identity caveat 문구 포함."""
    _commit(synth_repo, "a.py", "x", "alice@example.com", NOW - 3600)
    rc = main([str(synth_repo), "--since", "30 days ago"])
    assert rc == 0
    out = capsys.readouterr().out
    assert "rename tracking is OFF" in out
    assert "identity caveat" in out
    assert "overlap_ratio" in out
