"""P3 — 배타충돌 복구 UX: 진단 동봉(O1) + rerere 레인(O2) (증분13).

배타(write) 레인 충돌의 유일 발생경로는 out-of-band 우회(P1) — '충돌=경보' 의미론은 유지하되,
경보 *이후*를 채운다(선행문헌: Zuul merge-conflict reporter / git rerere / jj first-class conflicts).

  INV-P3-O1 (진단 동봉): merge conflict 응답이 ①conflict_files(충돌 경로) ②culprits(통합측
      원인 커밋 — bypass_audit 분류로 우회 여부·작성자까지 지목) ③hint(rebase 복구 레시피)를
      동봉한다. shared_conflict 도 conflict_files/culprits 를 받는다(의미론 분리는 불변).
  INV-P3-O2 (rerere 레인): repo 바인딩 시 rerere.enabled+autoUpdate 활성. 동일충돌 재시도에서
      기록된 해소가 자동 재적용돼 전부 해소되면 connect 가 **성공**한다(trailer 보존 —
      재기동 복구 probe 호환).
"""
import subprocess
from pathlib import Path

from omd_server import Coordinator


def _git(args, cwd, check=True):
    return subprocess.run(["git", *args], cwd=str(cwd), check=check,
                          capture_output=True, text=True)


def _init(root):
    root.mkdir()
    _git(["init", "-b", "main"], root)
    _git(["config", "user.name", "t"], root)
    _git(["config", "user.email", "t@t"], root)
    (root / "constants").mkdir()
    (root / "constants" / "env.py").write_text("X = 1\n")
    _git(["add", "-A"], root)
    _git(["commit", "-m", "base"], root)
    _git(["checkout", "-b", "dev"], root)


def _mk(tmp_path):
    repo = tmp_path / "repo"; _init(repo)
    omd = Coordinator(db_path=str(tmp_path / "o.db"), repo=str(repo),
                      worktrees_dir=str(tmp_path / "wt"), integration_branch="main")
    return omd, repo, repo.parent / f"{repo.name}-omd-integration"


def _develop_conflicting_task(omd):
    """task T: 옛 base(X=1)에서 X=222 로 편집·commit·finish (아직 connect 안 함)."""
    omd.declare("T", writes=["constants/**"])
    omd.next_task("agT")
    omd.claim("agT", ["constants/**"], task_id="T")
    wt = Path(omd.start("T", "agT")["worktree"])
    env = wt / "constants" / "env.py"
    env.write_text(env.read_text().replace("X = 1", "X = 222"))
    assert omd.commit("T", "T: X=222")["ok"] is True
    omd.finish("T")
    return wt


def _bypass_commit_on_main(integ, content="X = 999\n"):
    """out-of-band 우회: 통합(main) 에 직접커밋(작성자 human, trailer 없음) — P1 실사고 재현."""
    env = integ / "constants" / "env.py"
    env.write_text(content)
    _git(["add", "-A"], integ)
    _git(["-c", "user.name=human", "-c", "user.email=h@h",
          "commit", "-m", "bypass: hotfix on main"], integ)
    return _git(["rev-parse", "HEAD"], integ).stdout.strip()


# ---------------------------------------------------------------------------
# INV-P3-O1 — 진단 동봉
# ---------------------------------------------------------------------------


def test_bypass_conflict_response_carries_diagnosis(tmp_path):
    """우회 유발 배타충돌: 응답에 충돌파일 + 원인커밋(direct_commit 분류·작성자) + rebase 힌트."""
    omd, _repo, integ = _mk(tmp_path)
    _develop_conflicting_task(omd)
    bypass_sha = _bypass_commit_on_main(integ)

    r = omd.connect("T")
    assert r["ok"] is False and r["reason"].startswith("merge conflict")
    assert r["conflict_files"] == ["constants/env.py"], r
    culprits = {c["sha"]: c for c in r["culprits"]}
    assert bypass_sha in culprits, f"통합측 원인 커밋을 지목해야: {r['culprits']}"
    assert culprits[bypass_sha]["kind"] == "direct_commit", "우회 분류(bypass_audit)까지"
    assert culprits[bypass_sha]["author"] == "human"
    assert "rebase" in r.get("hint", ""), "복구 레시피(rebase) 동봉"
    assert r.get("retryable") is True and omd.store.get_task("T")["state"] == "DONE"


def test_shared_conflict_also_carries_files_and_culprits(tmp_path):
    """shared_conflict 의미론(증분10)은 불변 + conflict_files/culprits 동봉."""
    omd, _repo, _integ = _mk(tmp_path)
    for t, ag, val in (("A", "agA", "X = 111"), ("B", "agB", "X = 333")):
        omd.declare(t, shared=["constants/**"])
        omd.next_task(ag)
        omd.claim(ag, ["constants/**"], "shared", task_id=t)
        wt = Path(omd.start(t, ag)["worktree"])
        env = wt / "constants" / "env.py"
        env.write_text(env.read_text().replace("X = 1", val))
        assert omd.commit(t, f"{t}")["ok"] is True
        omd.finish(t)
    assert omd.connect("A")["ok"] is True
    r = omd.connect("B")
    assert r["ok"] is False and r["reason"].startswith("shared_conflict")
    assert r["conflict_files"] == ["constants/env.py"], r
    shas = {c["sha"] for c in r["culprits"]}
    assert omd.store.get_task("A")["merge_sha"] in shas, "통합측 원인 = A 의 응결 머지"
    assert "rebase" in r.get("hint", "")


# ---------------------------------------------------------------------------
# INV-P3-O2 — rerere 레인
# ---------------------------------------------------------------------------


def test_rerere_enabled_on_repo_bind(tmp_path):
    """repo 바인딩 시 rerere.enabled + rerere.autoUpdate 가 켜진다(멱등)."""
    _omd, repo, _integ = _mk(tmp_path)
    assert _git(["config", "rerere.enabled"], repo).stdout.strip() == "true"
    assert _git(["config", "rerere.autoUpdate"], repo).stdout.strip() == "true"


def test_identical_conflict_auto_reresolved_and_connect_succeeds(tmp_path):
    """동일충돌 재시도: 1차 connect 충돌(preimage 기록) → 해소가 한 번 기록되면 →
    2차 connect 에서 rerere 가 자동 재해소 → 전부 해소 시 merge 를 **완성**(성공, trailer 보존)."""
    omd, _repo, integ = _mk(tmp_path)
    _develop_conflicting_task(omd)
    _bypass_commit_on_main(integ)

    r1 = omd.connect("T")                                   # 1차: 충돌(+ preimage 기록)
    assert r1["ok"] is False and r1["reason"].startswith("merge conflict")

    # 해소를 한 번 기록(물방울이 안내대로 해소하는 상황의 최소 재현): 같은 머지를 통합 wt 에서
    # 수동으로 밟아 X 합의값으로 resolve+commit → rerere 가 해소를 기록 → 머지는 되돌림.
    branch = omd.store.get_task("T")["branch"]
    m = _git(["merge", "--no-ff", "-m", "prime", branch], integ, check=False)
    assert m.returncode != 0, "동일충돌이어야 rerere 시나리오가 성립"
    (integ / "constants" / "env.py").write_text("X = 999222\n")   # 합의 해소
    _git(["add", "-A"], integ)
    _git(["-c", "user.name=t", "-c", "user.email=t@t", "commit", "--no-edit"], integ)
    _git(["reset", "--hard", "ORIG_HEAD"], integ)           # 머지 자체는 무효화(해소 기록만 유지)

    r2 = omd.connect("T")                                   # 2차: rerere 자동 재해소 → 성공
    assert r2["ok"] is True and r2["state"] == "MERGED", (
        f"기록된 해소가 재적용돼 connect 가 성공해야: {r2}")
    merged = _git(["show", "main:constants/env.py"], integ).stdout
    assert "X = 999222" in merged, "재적용된 내용이 기록된 해소와 일치"
    # trailer 보존 — 재기동 복구(trailer-probe)가 이 머지를 인식할 수 있어야 한다.
    body = _git(["log", "-1", "--format=%(trailers:key=OMD-Connect,valueonly)", "main"],
                integ).stdout.strip()
    assert body == "T", f"rerere-완성 머지도 OMD-Connect trailer 를 보존해야: {body!r}"
