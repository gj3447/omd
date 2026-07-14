"""hot_files 감지→행동 루프(P2 Q8). suggest_shared_for_writes = task 의 배타 write-set 중
hot 파일만 골라 shared *재분할 후보*로 돌린다. caller는 원래 writes를 서로소로 다시 나눠야 한다.
필터링(내 신규 로직)은 unit 으로, 집계는 실물 git repo 로 검증."""
import subprocess
from pathlib import Path

from omd_server import hot_files
from omd_server.hot_files import HotFile, HotReport, suggest_shared_for_writes


def _git(args, cwd):
    subprocess.run(["git", *args], cwd=str(cwd), check=True, capture_output=True, text=True)


def _repo(root: Path):
    root.mkdir()
    _git(["init", "-b", "main"], root)
    _git(["config", "user.name", "t"], root)
    _git(["config", "user.email", "t@t"], root)
    return root


# ---- unit: write-set 필터링(전역 나열이 아니라 타깃 추천) ----
def test_suggest_filters_to_writeset_only(monkeypatch):
    rep = HotReport(since_ref=None, threshold=3, hot=[
        HotFile("constants/env.py", 9, 3),      # 내 write-set 안 (hot)
        HotFile("other/unrelated.py", 8, 2),    # 내 write-set 밖 → 제외
    ])
    monkeypatch.setattr(hot_files, "hot_file_audit", lambda *a, **k: rep)
    got = suggest_shared_for_writes("/x", writes=["constants/**"])
    assert got == ["constants/env.py"]           # writes 밖 hot 파일은 빠진다


def test_suggest_empty_when_no_hot_in_writeset(monkeypatch):
    rep = HotReport(since_ref=None, threshold=3, hot=[HotFile("a/b.py", 5, 2)])
    monkeypatch.setattr(hot_files, "hot_file_audit", lambda *a, **k: rep)
    assert suggest_shared_for_writes("/x", writes=["z/**"]) == []


# ---- integration: 실물 git 히스토리에서 hot 집계 + 타깃 추천 ----
def test_suggest_on_real_repo_picks_hot_writeset_file(tmp_path):
    repo = _repo(tmp_path / "r")
    hot = repo / "constants" / "env.py"
    hot.parent.mkdir()
    hot.write_text("V=init\n")                    # init 전에 생성 → 이후 -am 이 수정 커밋
    cold = repo / "app" / "main.py"
    cold.parent.mkdir()
    cold.write_text("x\n")
    _git(["add", "-A"], repo)
    _git(["commit", "-m", "init"], repo)
    for i in range(4):                            # env.py 를 4커밋(=hot, threshold 3) 동안 수정
        hot.write_text(f"V={i}\n")
        _git(["commit", "-am", f"touch env {i}"], repo)
    got = suggest_shared_for_writes(str(repo), writes=["constants/**"], threshold=3)
    assert "constants/env.py" in got
    # cold 파일은 writes(constants/**) 밖이자 hot 도 아님 → 없음
    assert all("app/" not in g for g in got)
