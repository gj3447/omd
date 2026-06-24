"""P0-11/§D10 — write-set FS 강제. 선언 궤도 밖 파일을 건드린 브랜치는 connect 거부."""
import subprocess
from pathlib import Path
from omd_server import Coordinator


def _git(a, cwd): subprocess.run(["git", *a], cwd=str(cwd), check=True, capture_output=True, text=True)


def _init(root: Path):
    root.mkdir()
    _git(["init", "-b", "main"], root); _git(["config", "user.name", "t"], root)
    _git(["config", "user.email", "t@t"], root)
    (root / "README.md").write_text("base\n"); _git(["add", "-A"], root); _git(["commit", "-m", "base"], root)


def _flow(omd, tid, writes, files):
    omd.declare(tid, writes=writes); omd.next_task(f"ag{tid}")
    omd.claim(f"ag{tid}", writes, task_id=tid); s = omd.start(tid, f"ag{tid}")
    for rel, txt in files.items():
        p = Path(s["worktree"]) / rel; p.parent.mkdir(parents=True, exist_ok=True); p.write_text(txt)
    omd.commit(tid, f"feat {tid}"); omd.finish(tid)
    return omd.connect(tid)


def test_writeset_violation_rejected(tmp_path):
    repo = tmp_path / "repo"; _init(repo)
    omd = Coordinator(db_path=str(tmp_path / "o.db"), repo=str(repo), worktrees_dir=str(tmp_path / "wt"))
    # A declares a/** but ALSO writes b/evil.py (outside its orbit) → connect must reject
    r = _flow(omd, "A", ["a/**"], {"a/x.py": "x=1\n", "b/evil.py": "boom\n"})
    assert r["ok"] is False and "write-set" in r["reason"], r
    assert any("b/evil.py" in v for v in r["violations"]), r
    assert not (repo / "b" / "evil.py").exists()   # merge 거부 → 통합 브랜치 불변
    assert not (repo / "a" / "x.py").exists()


def test_writeset_clean_merges(tmp_path):
    repo = tmp_path / "repo"; _init(repo)
    omd = Coordinator(db_path=str(tmp_path / "o.db"), repo=str(repo), worktrees_dir=str(tmp_path / "wt"))
    r = _flow(omd, "A", ["a/**"], {"a/x.py": "x=1\n", "a/sub/z.py": "z=3\n"})
    assert r["ok"] is True and r["state"] == "MERGED", r
    assert (repo / "a" / "x.py").exists() and (repo / "a" / "sub" / "z.py").exists()
