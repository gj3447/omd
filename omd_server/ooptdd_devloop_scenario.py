"""OOPTDD in_process 드라이버 — *전체 병렬-dev 라이프사이클*을 실 git 으로 구동, LTDD 이벤트 방출.

ooptdd-loop runner 가 run(backend, cid) 호출. agent=task=cid 로 단일 작업의 풀 라이프사이클
(claim→start→edit-worktree→commit→finish→connect=실 git merge)을 구동 → 모든 이벤트가 그 cid 상관키로
backend 에 도착. spec/omd_devloop_ooptdd.yaml 의 게이트가 이 도착을 검증(LTDD 양성 trace).
"""
from __future__ import annotations

import subprocess
import tempfile
from pathlib import Path

from omd_server.core import Coordinator
from omd_server.events import Emitter


def _git(args, cwd):
    subprocess.run(["git", *args], cwd=str(cwd), check=True, capture_output=True, text=True)


def run(backend, cid: str) -> dict:
    tmp = Path(tempfile.mkdtemp(prefix="omd-devloop-"))
    repo = tmp / "repo"
    repo.mkdir(parents=True)
    _git(["init", "-b", "main"], repo)
    _git(["config", "user.name", "omd"], repo)
    _git(["config", "user.email", "omd@omd"], repo)
    (repo / "README.md").write_text("base\n")
    _git(["add", "-A"], repo)
    _git(["commit", "-m", "base"], repo)
    _git(["checkout", "-b", "dev"], repo)

    omd = Coordinator(db_path=str(tmp / "omd.db"), repo=str(repo),
                      worktrees_dir=str(tmp / "wt"), integration_branch="main",
                      events=Emitter(backend), agent_ttl=None)

    # agent=task=cid → 모든 동사 이벤트가 cid 상관키로 (claim/start/commit/finish=agent, connect=task)
    mod = cid
    omd.declare(mod, writes=[f"{mod}/**"])
    omd.next_task(cid)
    omd.claim(cid, [f"{mod}/**"], task_id=mod)            # orbit_granted (cid)
    s = omd.start(mod, cid)                               # task_started (cid)
    d = Path(s["worktree"]) / mod
    d.mkdir(parents=True, exist_ok=True)
    (d / "impl.py").write_text(f"# {mod}\nVALUE = 1\n")
    omd.commit(mod, f"feat({mod}): impl")                # task_committed (cid)
    omd.finish(mod)                                      # task_finished (cid)
    omd.connect(mod)                                     # connect_started→connect_merged (cid) = 실 git merge

    omd.flush_admission_outbox()
    omd.close()
    return {"cid": cid, "lifecycle": ["orbit_granted", "task_started", "task_committed",
                                      "task_finished", "connect_started", "connect_merged"]}
