"""증분3 — split-phase connect (§3.B/§D8/§D11): P0-4·P0-5·P0-6.

검증 5축:
 1) merge_token 상호배제: 서로소 두 task가 동시 connect → 둘 다 MERGED, 통합 브랜치에 두 파일,
    index 무손상. connect는 merge_token으로 직렬화(한 번에 하나만 보유).
 2) git이 락 밖: Phase B(merge)는 _cs()/live tx를 안 잡는다(다른 변이가 interleave 가능).
 3) P0-4 stale fence: 작업 중 fence가 bump(만료+재부여)되면 connect FENCED_OUT, merge 없음.
 4) _recover(): CONNECTING task를 git 진실과 조정(이미 통합=MERGED, 아니면 rollback→DONE);
    dangling merge_token abort.
 5) LTDD 게이트(connect_started→connect_merged 도착).
"""

import os
import subprocess
import threading
import time
from pathlib import Path

import pytest

from omd_server import Coordinator, Emitter

GATES = os.path.join(os.path.dirname(__file__), os.pardir, "gates")


def _git(args, cwd):
    subprocess.run(["git", *args], cwd=str(cwd), check=True, capture_output=True, text=True)


def _init_repo(root: Path):
    """root=사용자 HEAD(dev), main=OMD 전용 통합 브랜치(§D11). 통합 worktree만 main을 잡는다."""
    root.mkdir()
    _git(["init", "-b", "main"], root)
    _git(["config", "user.name", "t"], root)
    _git(["config", "user.email", "t@t"], root)
    (root / "README.md").write_text("base\n")
    _git(["add", "-A"], root)
    _git(["commit", "-m", "base"], root)
    _git(["checkout", "-b", "dev"], root)


def _develop(omd, task, subdir, fname, content):
    """task를 자기 worktree에서 완전 개발(claim→start→write→commit→finish)."""
    omd.declare(task, writes=[f"{subdir}/**"])
    omd.next_task(f"ag{task}")
    r = omd.claim(f"ag{task}", [f"{subdir}/**"], task_id=task)
    s = omd.start(task, f"ag{task}")
    (Path(s["worktree"]) / subdir).mkdir(parents=True)
    (Path(s["worktree"]) / subdir / fname).write_text(content)
    omd.commit(task, f"feat: {subdir}/{fname}")
    omd.finish(task)
    return r


# ---------- 1) merge_token 상호배제 ----------
def test_merge_token_serializes_concurrent_connect(tmp_path):
    """서로소 두 task가 동시 connect → 둘 다 MERGED, 통합 브랜치에 두 파일, index 무손상.
    merge_token이 한 번에 하나만 보유됨(상호배제) — 동시 머지의 index 오염(P0-5) 차단."""
    repo = tmp_path / "repo"
    _init_repo(repo)
    omd = Coordinator(db_path=str(tmp_path / "omd.db"), repo=str(repo),
                      worktrees_dir=str(tmp_path / "wt"), integration_branch="main")
    _develop(omd, "A", "a", "x.py", "x = 1\n")
    _develop(omd, "B", "b", "y.py", "y = 2\n")

    # merge_token이 동시에 1개 초과로 HELD인 적이 없는지 — 느린 merge 동안 표본.
    max_tokens = {"n": 0}
    real_merge = omd.git.merge_into

    def slow_merge(*args, **kwargs):
        held = len(omd.store.all_held_merge_tokens())
        max_tokens["n"] = max(max_tokens["n"], held)
        time.sleep(0.15)                      # 머지 창을 넓혀 동시성 노출
        held2 = len(omd.store.all_held_merge_tokens())
        max_tokens["n"] = max(max_tokens["n"], held2)
        return real_merge(*args, **kwargs)

    omd.git.merge_into = slow_merge

    out = {}

    def go(task):
        out[task] = omd.connect(task)

    t1 = threading.Thread(target=go, args=("A",))
    t2 = threading.Thread(target=go, args=("B",))
    t1.start(); t2.start(); t1.join(); t2.join()

    assert out["A"]["ok"] and out["A"]["state"] == "MERGED", out["A"]
    assert out["B"]["ok"] and out["B"]["state"] == "MERGED", out["B"]
    assert max_tokens["n"] == 1, f"merge_token 상호배제 위반: {max_tokens['n']} held at once"

    integ = Path(omd.integration_worktree)
    assert (integ / "a" / "x.py").exists() and (integ / "b" / "y.py").exists()
    # index 무손상: 통합 worktree가 clean(미해결 머지·충돌 없음)
    st = subprocess.run(["git", "status", "--porcelain"], cwd=str(integ),
                        capture_output=True, text=True).stdout.strip()
    assert st == "", f"통합 worktree 더러움(index 오염?): {st!r}"
    # 두 merge_token 모두 반납됨(누수 0)
    assert omd.store.all_held_merge_tokens() == []
    log = subprocess.run(["git", "log", "--oneline", "main"], cwd=str(repo),
                        capture_output=True, text=True).stdout
    assert "CLOUD CONNECT A" in log and "CLOUD CONNECT B" in log


# ---------- 2) git이 락 밖에서 돈다 ----------
def test_phase_b_runs_without_lock_or_live_tx(tmp_path):
    """Phase B(merge)는 _cs()/live tx를 안 잡는다 — 머지 중 다른 변이가 interleave 한다."""
    repo = tmp_path / "repo"
    _init_repo(repo)
    omd = Coordinator(db_path=str(tmp_path / "omd.db"), repo=str(repo),
                      worktrees_dir=str(tmp_path / "wt"), integration_branch="main")
    _develop(omd, "A", "a", "x.py", "x = 1\n")

    observed = {"txn_depth": None, "interleaved": False}
    real_merge = omd.git.merge_into

    def merge_hook(*args, **kwargs):
        # Phase B 시점: live 트랜잭션이 없어야(=0). 락도 안 쥐어 다른 스레드가 변이 가능.
        observed["txn_depth"] = omd.store._txn_depth
        done = threading.Event()

        def other_mutation():
            omd.claim("agOther", ["unrelated/**"], "write")   # 별 task — 락 잡혀있다면 블록
            observed["interleaved"] = True
            done.set()

        threading.Thread(target=other_mutation).start()
        assert done.wait(2.0), "Phase B 동안 다른 변이가 블록됨 → git이 락 안에서 돈다"
        return real_merge(*args, **kwargs)

    omd.git.merge_into = merge_hook
    res = omd.connect("A")
    assert res["ok"] and res["state"] == "MERGED"
    assert observed["txn_depth"] == 0, "Phase B가 live tx를 들고 있다(_cs 밖이어야)"
    assert observed["interleaved"], "Phase B 동안 다른 변이가 interleave 못 함"


# ---------- 3) P0-4 stale fence ----------
def test_connect_fenced_out_on_bumped_fence(tmp_path):
    """작업 중 lease가 만료→재부여되어 fence가 bump되면 connect는 captured fence와 불일치로
    FENCED_OUT — merge 안 일어남(ABA를 fence 동일성으로 잡음, P0-4)."""
    repo = tmp_path / "repo"
    _init_repo(repo)
    omd = Coordinator(db_path=str(tmp_path / "omd.db"), repo=str(repo),
                      worktrees_dir=str(tmp_path / "wt"), integration_branch="main")
    omd.declare("A", writes=["a/**"])
    omd.next_task("agA")
    r = omd.claim("agA", ["a/**"], "write", ttl=0.05, task_id="A")
    captured_fence = r["fence"]
    s = omd.start("A", "agA")
    (Path(s["worktree"]) / "a").mkdir(parents=True)
    (Path(s["worktree"]) / "a" / "x.py").write_text("x = 1\n")
    omd.commit("A", "feat: a/x")
    omd.finish("A")

    # lease 만료 → 다른 물방울이 같은 경로 재획득(fence bump). 이제 captured_fence는 낡음.
    time.sleep(0.08)
    omd.sweep()
    r2 = omd.claim("agB", ["a/**"], "write")
    assert r2["state"] == "HELD" and r2["fence"] != captured_fence

    res = omd.connect("A", "agA", captured_fence)
    assert res["ok"] is False and res.get("fenced_out"), res
    # merge 안 됨 — 통합 worktree에 a/ 없음
    assert not (Path(omd.integration_worktree) / "a" / "x.py").exists()


def test_connect_fenced_out_on_aba_fence_while_held(tmp_path):
    """순수 ABA(P0-4): write-orbit가 여전히 HELD지만 fence가 captured와 다르면(=만료 후
    재부여로 토큰이 바뀜) connect는 FENCED_OUT — 'state==HELD'만 보는 게 아니라 fence 동일성을
    본다. (state!=HELD 베이직 검사로는 안 잡히는, fence-equality 가드의 이빨 확인.)"""
    repo = tmp_path / "repo"
    _init_repo(repo)
    omd = Coordinator(db_path=str(tmp_path / "omd.db"), repo=str(repo),
                      worktrees_dir=str(tmp_path / "wt"), integration_branch="main")
    omd.declare("A", writes=["a/**"])
    omd.next_task("agA")
    r = omd.claim("agA", ["a/**"], "write", ttl=600, task_id="A")
    captured_fence = r["fence"]
    s = omd.start("A", "agA")
    (Path(s["worktree"]) / "a").mkdir(parents=True)
    (Path(s["worktree"]) / "a" / "x.py").write_text("x = 1\n")
    omd.commit("A", "feat: a/x")
    omd.finish("A")

    # ABA: 궤도는 HELD인 채로 fence만 바뀜(만료→재부여가 같은 행을 재발급한 것처럼).
    # captured_fence는 이제 낡았다 — basic state 검사는 통과하지만 fence-equality가 잡아야 한다.
    with omd.store.tx():
        omd.store.set_orbit(r["orbit_id"], state="HELD", fence=captured_fence + 100)

    res = omd.connect("A", "agA", captured_fence)
    assert res["ok"] is False and res.get("fenced_out"), res
    assert not (Path(omd.integration_worktree) / "a" / "x.py").exists()  # merge 없음


def test_connect_fence_ok_when_fence_matches(tmp_path):
    """대조: captured fence가 현재 write-orbit fence와 일치하면 정상 응결."""
    repo = tmp_path / "repo"
    _init_repo(repo)
    omd = Coordinator(db_path=str(tmp_path / "omd.db"), repo=str(repo),
                      worktrees_dir=str(tmp_path / "wt"), integration_branch="main")
    omd.declare("A", writes=["a/**"])
    omd.next_task("agA")
    r = omd.claim("agA", ["a/**"], "write", ttl=600, task_id="A")
    s = omd.start("A", "agA")
    (Path(s["worktree"]) / "a").mkdir(parents=True)
    (Path(s["worktree"]) / "a" / "x.py").write_text("x = 1\n")
    omd.commit("A", "feat: a/x")
    omd.finish("A")
    res = omd.connect("A", "agA", r["fence"])
    assert res["ok"] and res["state"] == "MERGED", res


# ---------- 4) _recover() ----------
def test_recover_connecting_already_in_integration_to_merged(tmp_path):
    """재기동: 통합 브랜치에 이미 trailer가 있는 CONNECTING task → 전진수정(MERGED+해제)."""
    repo = tmp_path / "repo"
    _init_repo(repo)
    db = str(tmp_path / "omd.db")
    omd = Coordinator(db_path=db, repo=str(repo), coordinator_id="restart-sim",
                      worktrees_dir=str(tmp_path / "wt"), integration_branch="main")
    _develop(omd, "A", "a", "x.py", "x = 1\n")

    # Phase B까지 실제 머지하되, Phase C(merge_sha 기록/MERGED)는 못 하고 크래시한 상황을 모사:
    a = omd._connect_phase_a("A", None, None)
    assert a["ok"], a
    sha, err = omd._connect_phase_b(a["intent"])
    assert err is None and sha
    # ← 여기서 코디네이터 크래시. task는 CONNECTING, merge_token HELD, 궤도 pin 인 채로 영속.
    assert omd.store.get_task("A")["state"] == "CONNECTING"
    assert omd.store.all_held_merge_tokens()                     # dangling token

    # 재기동: 같은 DB+repo로 새 코디네이터 → _recover()가 git 진실과 조정.
    omd2 = Coordinator(db_path=db, repo=str(repo), coordinator_id="restart-sim",
                       worktrees_dir=str(tmp_path / "wt"), integration_branch="main")
    t = omd2.store.get_task("A")
    assert t["state"] == "MERGED", t                             # 전진수정(git=진실)
    assert t["merge_sha"]                                         # P0-6: merge_sha 기록됨
    assert omd2.store.orbits_held_by_agent("agA") == []          # write-orbit 해제됨
    assert omd2.store.all_held_merge_tokens() == []              # dangling token abort+반납


def test_recover_connecting_not_merged_rolls_back_to_done(tmp_path):
    """재기동: 통합 브랜치에 trailer가 없는 CONNECTING task → rollback(DONE, connect 재호출 가능)."""
    repo = tmp_path / "repo"
    _init_repo(repo)
    db = str(tmp_path / "omd.db")
    omd = Coordinator(db_path=db, repo=str(repo), coordinator_id="restart-sim",
                      worktrees_dir=str(tmp_path / "wt"), integration_branch="main")
    _develop(omd, "A", "a", "x.py", "x = 1\n")

    # Phase A만 하고(머지 전) 크래시: CONNECTING + token HELD + pin, 단 통합엔 미머지.
    a = omd._connect_phase_a("A", None, None)
    assert a["ok"]
    assert omd.store.get_task("A")["state"] == "CONNECTING"

    omd2 = Coordinator(db_path=db, repo=str(repo), coordinator_id="restart-sim",
                       worktrees_dir=str(tmp_path / "wt"), integration_branch="main")
    t = omd2.store.get_task("A")
    assert t["state"] == "DONE", t                               # rollback(재시도가능)
    assert omd2.store.all_held_merge_tokens() == []              # dangling token 반납
    # 궤도 unpin — 재호출 connect가 정상 응결
    assert omd2.store.pinned_orbits_for_task("A") == []
    res = omd2.connect("A")
    assert res["ok"] and res["state"] == "MERGED", res
    assert (Path(omd2.integration_worktree) / "a" / "x.py").exists()


def test_recover_trailer_probe_no_prefix_false_match(tmp_path):
    """trailer-probe는 줄 단위 정확매칭 — 이미 머지된 'AB'가 미머지 'A'의 응결로 오탐되지 않게.
    (A의 trailer 'OMD-Connect: A'가 'OMD-Connect: AB'에 prefix-매칭하면 A를 잘못 MERGED 처리.)"""
    repo = tmp_path / "repo"
    _init_repo(repo)
    db = str(tmp_path / "omd.db")
    omd = Coordinator(db_path=db, repo=str(repo), coordinator_id="restart-sim",
                      worktrees_dir=str(tmp_path / "wt"), integration_branch="main")
    # AB는 완전 응결, A는 Phase A만(미머지)인 채로 크래시.
    _develop(omd, "AB", "ab", "z.py", "z = 0\n")
    assert omd.connect("AB")["state"] == "MERGED"
    _develop(omd, "A", "a", "x.py", "x = 1\n")
    a = omd._connect_phase_a("A", None, None)
    assert a["ok"] and omd.store.get_task("A")["state"] == "CONNECTING"

    omd2 = Coordinator(db_path=db, repo=str(repo), coordinator_id="restart-sim",
                       worktrees_dir=str(tmp_path / "wt"), integration_branch="main")
    # A는 통합에 없음 → rollback(DONE), MERGED 오탐 아님.
    assert omd2.store.get_task("A")["state"] == "DONE", omd2.store.get_task("A")
    assert omd2.store.get_task("AB")["state"] == "MERGED"


def test_recover_is_idempotent(tmp_path):
    """_recover()는 멱등 — 정상 종료한 DB로 두 번째 기동해도 손상 없음."""
    repo = tmp_path / "repo"
    _init_repo(repo)
    db = str(tmp_path / "omd.db")
    omd = Coordinator(db_path=db, repo=str(repo), coordinator_id="restart-sim",
                      worktrees_dir=str(tmp_path / "wt"), integration_branch="main")
    _develop(omd, "A", "a", "x.py", "x = 1\n")
    assert omd.connect("A")["state"] == "MERGED"
    # 두 번 더 기동 — MERGED는 그대로, 새 토큰/롤백 없음.
    for _ in range(2):
        omd = Coordinator(db_path=db, repo=str(repo), coordinator_id="restart-sim",
                          worktrees_dir=str(tmp_path / "wt"), integration_branch="main")
        assert omd.store.get_task("A")["state"] == "MERGED"
        assert omd.store.all_held_merge_tokens() == []


# ---------- 5) LTDD 게이트 ----------
def test_connect_trace_arrives(tmp_path):
    """LTDD: split-phase connect의 관측가능 트레이스(connect_started → connect_merged)가 도착."""
    pytest.importorskip("ooptdd")
    from ooptdd.backends import MemoryBackend, memory as mem
    from ooptdd.gate import evaluate, evidence_tier, load_gate

    repo = tmp_path / "repo"
    _init_repo(repo)
    mem.reset()
    backend = MemoryBackend()
    omd = Coordinator(db_path=str(tmp_path / "omd.db"), repo=str(repo),
                      worktrees_dir=str(tmp_path / "wt"), integration_branch="main",
                      events=Emitter(backend))
    _develop(omd, "omd-connect-demo", "a", "x.py", "x = 1\n")
    assert omd.connect("omd-connect-demo")["state"] == "MERGED"

    res = evaluate(backend, load_gate(os.path.join(GATES, "connect.yaml")))
    assert res["ok"], res
    assert evidence_tier(res) == "arrived"
