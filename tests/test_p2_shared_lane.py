"""P2 — shared(hot 공유파일) 3-way 레인. FEEDBACK P2 + 현장실측(consumer-b adoption 0%·hot 30파일) 응답.

INV-P2-LANE:
  ① mode="shared" 궤도는 같은 경로에 **동시 HELD 공존**(shared↔shared) — 직렬화 마찰 제거.
  ② shared↔write / shared↔read 는 여전히 충돌(배타 궤도의 의미 보존).
  ③ shared 경로 편집은 write-set 감사(commit/connect §D10)에서 in-orbit 취급.
  ④ 서로 다른 hunk 편집은 CLOUD CONNECT 의 git 3-way 가 자동 응결(둘 다 MERGED, 두 편집 공존).
  ⑤ 같은 hunk 진짜 충돌은 **경보가 아니라 정상사건** — reason="shared_conflict", retryable,
     rebase 힌트 동봉(P3 부분 해소). disjoint(write) 충돌의 '불변식 버그=경보' 의미론은 불변.
"""
import subprocess
from pathlib import Path

from omd_server import Coordinator


def _git(args, cwd):
    subprocess.run(["git", *args], cwd=str(cwd), check=True, capture_output=True, text=True)


def _init(root):
    root.mkdir()
    _git(["init", "-b", "main"], root)
    _git(["config", "user.name", "t"], root)
    _git(["config", "user.email", "t@t"], root)
    # hot 공유파일 시뮬레이션: 멀리 떨어진 두 섹션(3-way 가 안전하게 합칠 수 있는 모양).
    (root / "constants").mkdir()
    (root / "constants" / "env.py").write_text(
        "# section A\nA = 1\n" + "\n" * 20 + "# section B\nB = 1\n")
    _git(["add", "-A"], root)
    _git(["commit", "-m", "base"], root)
    _git(["checkout", "-b", "dev"], root)


def _mk(tmp_path):
    repo = tmp_path / "repo"; _init(repo)
    return Coordinator(db_path=str(tmp_path / "o.db"), repo=str(repo),
                       worktrees_dir=str(tmp_path / "wt"), integration_branch="main"), repo


def _drive(omd, task, agent, paths, *, mode="shared"):
    """declare→next→claim→start → worktree 경로 반환. shared task 는 declare 도 shared= 로."""
    if mode == "shared":
        omd.declare(task, shared=paths)
    else:
        omd.declare(task, writes=paths)
    omd.next_task(agent)
    r = omd.claim(agent, paths, mode, task_id=task)
    assert r["state"] == "HELD", f"{task}: {mode} claim 이 HELD 여야: {r}"
    return Path(omd.start(task, agent)["worktree"]), r


# ---------------------------------------------------------------------------
# ① shared↔shared 공존 / ② 배타 궤도 의미 보존
# ---------------------------------------------------------------------------


def test_shared_orbits_coexist_on_same_path(tmp_path):
    """같은 hot 파일에 두 에이전트가 shared 궤도를 잡으면 **둘 다 HELD**(공존) —
    disjoint-only 직렬화(둘째=PENDING)가 사라진다."""
    omd, _ = _mk(tmp_path)
    r1 = omd.claim("agA", ["constants/env.py"], "shared", task_id=None)
    r2 = omd.claim("agB", ["constants/env.py"], "shared", task_id=None)
    assert r1["state"] == "HELD"
    assert r2["state"] == "HELD", f"shared↔shared 는 공존해야: {r2}"
    assert r2["conflicts"] == []


def test_shared_vs_exclusive_write_still_conflicts(tmp_path):
    """shared 가 HELD 인 경로에 배타 write claim → PENDING(배타 의미 보존). 역방향도 동일."""
    omd, _ = _mk(tmp_path)
    assert omd.claim("agA", ["constants/env.py"], "shared")["state"] == "HELD"
    r = omd.claim("agB", ["constants/env.py"], "write")
    assert r["state"] == "PENDING", f"shared↔write 는 충돌해야: {r}"

    # 역방향: write HELD 위에 shared 요청 → PENDING
    omd3 = Coordinator(db_path=str(tmp_path / "o3.db"))
    assert omd3.claim("agC", ["a/**"], "write")["state"] == "HELD"
    assert omd3.claim("agD", ["a/**"], "shared")["state"] == "PENDING"


# ---------------------------------------------------------------------------
# ③+④ 3-way 자동 응결 — 서로 다른 hunk 편집 둘 다 MERGED
# ---------------------------------------------------------------------------


def test_shared_nonoverlapping_hunks_automerge_both_tasks(tmp_path):
    """두 task 가 같은 hot 파일의 **다른 섹션**을 shared 궤도로 편집 → 순차 CONNECT 에서
    git 3-way 가 자동 병합. 둘 다 MERGED, 통합엔 두 편집이 공존. writeset 감사도 통과(③)."""
    omd, repo = _mk(tmp_path)

    wtA, _ = _drive(omd, "A", "agA", ["constants/env.py"])
    wtB, _ = _drive(omd, "B", "agB", ["constants/env.py"])

    # A = 위 섹션, B = 아래 섹션 (base 에서 서로 다른 hunk)
    envA = (wtA / "constants" / "env.py")
    envA.write_text(envA.read_text().replace("A = 1", "A = 2  # by A"))
    envB = (wtB / "constants" / "env.py")
    envB.write_text(envB.read_text().replace("B = 1", "B = 2  # by B"))

    assert omd.commit("A", "feat: section A")["ok"] is True     # ③ 감사 통과
    assert omd.commit("B", "feat: section B")["ok"] is True
    omd.finish("A"); omd.finish("B")

    ra = omd.connect("A")
    assert ra["ok"] is True and ra["state"] == "MERGED"
    rb = omd.connect("B")                                        # ④ 3-way 자동 병합
    assert rb["ok"] is True and rb["state"] == "MERGED", f"3-way automerge 여야: {rb}"

    merged = subprocess.run(["git", "show", "main:constants/env.py"], cwd=str(repo),
                            capture_output=True, text=True).stdout
    assert "A = 2" in merged and "B = 2" in merged, "두 편집이 통합에 공존해야"


# ---------------------------------------------------------------------------
# ⑤ 진짜 충돌 = 정상사건(shared_conflict, retryable) — 경보 아님
# ---------------------------------------------------------------------------


def test_shared_true_conflict_is_retryable_shared_conflict_not_alarm(tmp_path):
    """두 task 가 같은 라인을 편집 → 둘째 CONNECT 는 reason='shared_conflict' + retryable
    + rebase 힌트. task 는 DONE 롤백(재시도 가능) — CONNECTING 좌초/경보 아님."""
    omd, _ = _mk(tmp_path)

    wtA, _ = _drive(omd, "A", "agA", ["constants/env.py"])
    wtB, _ = _drive(omd, "B", "agB", ["constants/env.py"])

    envA = (wtA / "constants" / "env.py")
    envA.write_text(envA.read_text().replace("A = 1", "A = 111"))
    envB = (wtB / "constants" / "env.py")
    envB.write_text(envB.read_text().replace("A = 1", "A = 222"))   # 같은 라인!

    assert omd.commit("A", "A=111")["ok"] is True
    assert omd.commit("B", "A=222")["ok"] is True
    omd.finish("A"); omd.finish("B")

    assert omd.connect("A")["ok"] is True
    rb = omd.connect("B")
    assert rb["ok"] is False
    assert rb["reason"].startswith("shared_conflict"), (
        f"shared 궤도의 충돌은 정상사건 reason=shared_conflict 여야(경보 금지): {rb}")
    assert rb.get("retryable") is True
    assert "rebase" in rb.get("hint", ""), "물방울에게 rebase 복구 힌트를 줘야"
    assert omd.store.get_task("B")["state"] == "DONE", "CONNECTING 좌초 금지 — DONE 롤백"


def test_exclusive_write_conflict_semantics_unchanged(tmp_path):
    """음성 컨트롤: shared 궤도가 **없는** task 의 merge conflict 는 기존 의미론
    (reason='merge conflict…', 불변식 위반=경보 계열) 그대로 — shared_conflict 로 오분류 금지.
    disjoint write 아래에선 OMD 경유 충돌이 구조적으로 불가하므로, 실사고(P1)처럼
    **out-of-band 직접커밋**으로 통합 브랜치를 갈라 충돌을 강제한다."""
    omd, repo = _mk(tmp_path)

    wtB, _ = _drive(omd, "B", "agB", ["constants/env.py"], mode="write")
    envB = (wtB / "constants" / "env.py")
    envB.write_text(envB.read_text().replace("A = 1", "A = 222"))
    assert omd.commit("B", "A=222")["ok"] is True
    omd.finish("B")

    # out-of-band: 누군가 OMD 를 우회해 main 의 같은 라인을 직접커밋(= P1 실사고 재현).
    # main 은 OMD 통합 worktree(<repo>-omd-integration)에 상주 — 우회자는 거기에 직접 쓴다.
    integ = repo.parent / f"{repo.name}-omd-integration"
    env_main = integ / "constants" / "env.py"
    env_main.write_text(env_main.read_text().replace("A = 1", "A = 999"))
    _git(["add", "-A"], integ)
    _git(["commit", "-m", "bypass: direct commit"], integ)

    rb = omd.connect("B")
    assert rb["ok"] is False
    assert rb["reason"].startswith("merge conflict"), (
        f"배타 write 충돌은 기존 경보 의미론 유지: {rb}")
    assert not rb["reason"].startswith("shared_conflict")
