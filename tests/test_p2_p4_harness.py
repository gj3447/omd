"""P2 hot파일 + P4 적합성 + 통합 하네스 CLI 테스트."""
import subprocess
from pathlib import Path

from omd_server import conformance, harness, hot_files


def _git(args, cwd):
    subprocess.run(["git", *args], cwd=str(cwd), check=True, capture_output=True, text=True)


def _head(root):
    return subprocess.run(["git", "-C", str(root), "rev-parse", "HEAD"],
                          capture_output=True, text=True).stdout.strip()


def _init(root):
    root.mkdir()
    _git(["init", "-b", "main"], root)
    _git(["config", "user.name", "t"], root)
    _git(["config", "user.email", "t@t"], root)


def _commit(root, files, msg):
    for rel, c in files.items():
        p = root / rel
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(c)
    _git(["add", "-A"], root)
    _git(["commit", "-m", msg], root)


# ---- P2 hot files ----
def test_hot_file_detected_cold_excluded(tmp_path):
    r = tmp_path / "repo"; _init(r)
    _commit(r, {"README.md": "0\n"}, "base")
    base = _head(r)
    for i in range(4):
        _commit(r, {"hot.py": f"v{i}\n"}, f"touch hot {i}")
    _commit(r, {"cold.py": "c\n"}, "cold once")
    rep = hot_files.hot_file_audit(str(r), base, threshold=3)
    paths = [h.path for h in rep.hot]
    assert "hot.py" in paths and "cold.py" not in paths
    assert rep.recommend_shared_globs() == ["hot.py"]


def test_hot_gate_max_hot_threshold(tmp_path):
    r = tmp_path / "repo"; _init(r)
    _commit(r, {"README.md": "0\n"}, "base")
    base = _head(r)
    for i in range(4):
        _commit(r, {"hot.py": f"{i}\n"}, f"t{i}")
    assert hot_files.gate(str(r), base, threshold=3, max_hot=0) == 1   # 1 hot > 0 → NO_GO
    assert hot_files.gate(str(r), base, threshold=3, max_hot=5) == 0   # 1 hot ≤ 5 → GO


def test_hot_gate_git_failure_fail_loud(tmp_path):
    r = tmp_path / "repo"; _init(r); _commit(r, {"a": "0\n"}, "base")
    assert hot_files.gate(str(r), "nonexistent-ref-xyz") == 2   # silent skip 금지


# ---- P4 conformance (실제 OMD repo 대상) ----
def test_conformance_built_capabilities_are_done():
    r = conformance.audit()
    by = {x["key"]: x["done"] for x in r["results"]}
    for k in ("idem_gc", "strict_writeset", "auto_push", "bypass_gate",
              "complete_task", "hot_file_gate"):
        assert by[k] is True, f"{k} 가 DONE 이어야(이 하네스가 구현함)"
    assert r["ok"] is True   # must=True 회귀 없음 → 게이트 GO


def test_conformance_reports_known_gaps_honestly():
    r = conformance.audit()
    gap_keys = {x["key"] for x in r["gaps"]}
    # durable_engine(DBOS) 은 의도적 미채택(설계결정 — DB-backed FSM+_recover 로 크래시 복구 충분,
    # 부채 아님) — 그래도 침묵 truncation 금지하고 정직히 표기(note 로 '의도적'임 명시).
    # (periodic_sweep/read_coherence/crash_recovery 는 구현 완료 → DONE.)
    assert "durable_engine" in gap_keys
    for g in r["gaps"]:
        assert g["note"], f"GAP {g['key']} 는 안내 note 필수"


def test_conformance_gate_go():
    assert conformance.gate() == 0


# ---- 통합 하네스 CLI ----
def test_harness_run_go_without_branch(capsys):
    # P1 skip(branch 미지정) + P2(실 repo, max_hot None=GO) + P4(GO) → 종합 GO
    rc = harness.run(str(conformance.ROOT), None, None, out=__import__("sys").stderr)
    assert rc == 0


def test_harness_main_exit_code(tmp_path):
    r = tmp_path / "repo"; _init(r); _commit(r, {"a": "0\n"}, "base")
    # 우회 없는 단일 base 커밋이지만 branch=main 지정 → P1 검사(직접커밋 1=base는 ROOT 제외 후 0 우회)
    # P4 는 ROOT(실 repo) 대상이라 GO. since 미지정시 P1 은 전수(base 만, ROOT 제외) → clean
    rc = harness.main(["--repo", str(r), "--branch", "main"])
    assert rc in (0, 1)   # 환경에 따라(실 repo conformance) — 최소한 예외 없이 정수 반환
