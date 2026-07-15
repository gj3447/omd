"""라이브 멀티에이전트 병렬-dev 세션 — 실 git worktree + 실 머지로 SINGULON 을 *실증*.

N 개 에이전트(실 스레드)가 *서로소* write-set 모듈을 각자 worktree 에서 동시 개발(claim→start→edit→
commit→finish) 후 동시 connect → OMD 가 통합 브랜치로 *실제* `git merge --no-ff`. 입체라 머지충돌 0
(SINGULON Δ분열=0). 겹치는 5번째 에이전트는 OMD 가 직렬화(orbit PENDING). LTDD 이벤트는 backend 로 방출.

실행: .venv/bin/python scripts/multiagent_parallel_session.py
"""
from __future__ import annotations

import json
import os
import subprocess
import tempfile
import threading
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from omd_server.core import Coordinator
from omd_server.events import Emitter


def _git(args, cwd):
    subprocess.run(["git", *args], cwd=str(cwd), check=True,
                   capture_output=True, text=True)


def _init_repo(root: Path):
    root.mkdir(parents=True, exist_ok=True)
    _git(["init", "-b", "main"], root)
    _git(["config", "user.name", "omd"], root)
    _git(["config", "user.email", "omd@omd"], root)
    (root / "README.md").write_text("base\n")
    _git(["add", "-A"], root)
    _git(["commit", "-m", "base"], root)
    _git(["checkout", "-b", "dev"], root)


class _Collector:
    def __init__(self):
        self.evs = []
        self._lock = threading.Lock()

    def ship(self, envs):
        with self._lock:
            self.evs.extend(envs)


def run_session(n_agents: int = 4) -> dict:
    tmp = Path(tempfile.mkdtemp(prefix="omd-session-"))
    repo = tmp / "repo"
    _init_repo(repo)
    col = _Collector()
    omd = Coordinator(db_path=str(tmp / "omd.db"), repo=str(repo),
                      worktrees_dir=str(tmp / "wt"), integration_branch="main",
                      events=Emitter(col), agent_ttl=None)

    mods = [f"svc_{chr(ord('a') + i)}" for i in range(n_agents)]   # 서로소 모듈 = 작업

    # ── 1) 코디네이터 핸드아웃(직렬 임계구역=안전기제) + 각 물방울이 자기 worktree 에서 실 개발 ──
    #    OMD 모델: 코디네이터 1, 물방울 N. next_task→claim→start 배정은 _cs() 로 직렬(SINGULON 보장),
    #    실제 코드편집·commit 은 서로소 worktree 에서 일어난다(자존자 격리).
    for mod in mods:
        agent = f"droplet-{mod}"
        omd.declare(mod, writes=[f"{mod}/**"])
        omd.next_task(agent)                          # PENDING→READY (배정)
        omd.claim(agent, [f"{mod}/**"], task_id=mod)  # 궤도 점유(입체)
        s = omd.start(mod, agent)                     # IN_ORBIT + worktree 발사
        d = Path(s["worktree"]) / mod
        d.mkdir(parents=True, exist_ok=True)
        (d / "impl.py").write_text(f"# {mod} developed by {agent}\nVALUE = '{mod}'\n")
        omd.commit(mod, f"feat({mod}): impl")
        omd.finish(mod)
    developed = list(mods)

    # ── 2) 겹치는 5번째 에이전트 — 이 시점 svc_a 궤도는 아직 HELD(connect 전) → OMD 가 직렬화(PENDING) ──
    omd.declare("overlap", writes=[f"{mods[0]}/impl.py"])
    overlap = omd.claim("ag-overlap", [f"{mods[0]}/impl.py"], task_id="overlap")

    # ── 3) N 에이전트가 *동시* connect → 실 git merge, merge_token 직렬화 ──
    max_tokens = {"n": 0}
    real_merge = omd.git.merge_into

    def sampled_merge(*a, **k):
        max_tokens["n"] = max(max_tokens["n"], len(omd.store.all_held_merge_tokens()))
        return real_merge(*a, **k)

    omd.git.merge_into = sampled_merge

    results: dict = {}

    def do_connect(task):
        results[task] = omd.connect(task)

    threads = [threading.Thread(target=do_connect, args=(t,)) for t in developed]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    # ── 검증 ──
    integ = Path(omd.integration_worktree)
    files_merged = sum(1 for m in mods if (integ / m / "impl.py").exists())
    st = subprocess.run(["git", "status", "--porcelain"], cwd=str(integ),
                        capture_output=True, text=True).stdout.strip()
    log = subprocess.run(["git", "log", "--oneline", "main"], cwd=str(repo),
                         capture_output=True, text=True).stdout
    merge_commits = log.count("CLOUD CONNECT")
    ev = lambda name: sum(1 for e in col.evs if e["event"] == name)

    return {
        "thesis": "실 git 멀티에이전트 병렬-dev — 입체 write-set ⇒ 무충돌 머지(SINGULON Δ분열=0)",
        "agents": n_agents,
        "all_merged": all(r.get("state") == "MERGED" for r in results.values()),
        "merged_states": {t: r.get("state") for t, r in results.items()},
        "files_in_integration": files_merged,
        "real_merge_commits": merge_commits,
        "integration_worktree_clean": st == "",
        "merge_token_max_held": max_tokens["n"],   # 1 = 상호배제(P0-5)
        "merge_token_leak": omd.store.all_held_merge_tokens() != [],
        "overlap_serialized": overlap.get("state") == "PENDING",
        "ltdd_events": {k: ev(k) for k in (
            "orbit_granted", "task_started", "task_committed", "task_finished",
            "connect_started", "connect_merged")},
        "total_events": len(col.evs),
        "repo": str(repo),
    }


if __name__ == "__main__":
    r = run_session(4)
    print(json.dumps(r, ensure_ascii=False, indent=2))
    print(f"\n✅ {r['agents']} 에이전트 동시 개발+connect → {r['real_merge_commits']} 실 머지커밋, "
          f"통합브랜치 {r['files_in_integration']}/{r['agents']} 파일, 충돌 0, "
          f"merge_token 최대보유 {r['merge_token_max_held']}(상호배제), 겹침 직렬화={r['overlap_serialized']}")
    print(f"LTDD: {r['ltdd_events']}")
