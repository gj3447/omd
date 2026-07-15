"""PROM16 R3 — real-workload write-set overlap/conflict-rate pilot."""

from __future__ import annotations

import json
import hashlib
import sqlite3
import subprocess
from pathlib import Path

import pytest

from omd_server.overlap_pilot import (
    PilotDataError,
    ScopeRule,
    load_attempts,
    main,
    run_pilot,
)


def _seed_db(path: Path, *, tasks: list[tuple], orbits: list[tuple]) -> None:
    with sqlite3.connect(path) as db:
        db.executescript(
            """
            CREATE TABLE tasks (
              task_id TEXT PRIMARY KEY,
              branch_tip_sha TEXT,
              merge_sha TEXT,
              merged_at REAL
            );
            CREATE TABLE orbits (
              orbit_id TEXT PRIMARY KEY,
              task_id TEXT,
              agent_id TEXT,
              pathspec TEXT NOT NULL,
              mode TEXT NOT NULL,
              state TEXT NOT NULL,
              kind TEXT NOT NULL DEFAULT 'orbit',
              expires_at REAL,
              created_at REAL,
              released_at REAL
            );
            """
        )
        db.executemany(
            "INSERT INTO tasks(task_id, branch_tip_sha, merge_sha, merged_at) "
            "VALUES (?, ?, ?, ?)",
            tasks,
        )
        db.executemany(
            "INSERT INTO orbits(orbit_id, task_id, agent_id, pathspec, mode, state, "
            "expires_at, created_at, released_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            orbits,
        )


def _orbit(
    orbit_id: str,
    task: str,
    agent: str,
    paths: list[str],
    start: float,
    end: float,
    *,
    released: float | None = None,
) -> tuple:
    return (
        orbit_id,
        task,
        agent,
        json.dumps(paths),
        "write",
        "RELEASED" if released is not None else "EXPIRED",
        end,
        start,
        released,
    )


def _git(cwd: Path, *args: str) -> str:
    return subprocess.run(
        ["git", *args], cwd=cwd, check=True, capture_output=True, text=True
    ).stdout.strip()


def _sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _commit_branch(repo: Path, base: str, branch: str, changes: dict[str, str]) -> str:
    _git(repo, "switch", "-c", branch, base)
    for name, content in changes.items():
        target = repo / name
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(content)
    _git(repo, "add", "-A")
    _git(repo, "commit", "-m", branch)
    return _git(repo, "rev-parse", "HEAD")


def test_counts_cross_agent_concurrent_write_set_overlap(tmp_path):
    db = tmp_path / "coord.db"
    _seed_db(
        db,
        tasks=[
            ("lt-a", None, None, None),
            ("lt-b", None, None, None),
            ("lt-c", None, None, None),
        ],
        orbits=[
            _orbit("oa", "lt-a", "alice", ["lakatotree/src/**"], 0, 10),
            _orbit("ob", "lt-b", "bob", ["lakatotree/src/api.py"], 2, 8),
            _orbit("oc", "lt-c", "carol", ["lakatotree/docs/**"], 3, 9),
        ],
    )

    report = run_pilot(db, [ScopeRule("lakatotree", r"^lt-")]).to_dict()
    scope = report["scopes"][0]

    assert scope["task_agent_groups"] == 3
    assert scope["candidate_nominal_window_pairs"] == 3
    assert scope["declared_overlap_pairs"] == 1
    assert scope["declared_overlap_pair_fraction"] == 1 / 3
    assert scope["candidate_unique_task_pairs"] == 3
    assert scope["candidate_graph_groups"] == 3
    assert scope["candidate_graph_components"] == 1
    assert scope["candidate_window_basis_counts"] == {
        "RELEASED_ONLY": 0,
        "EXPIRY_PROXY_ONLY": 3,
        "MIXED_RELEASED_AND_EXPIRY_PROXY": 0,
    }
    assert scope["declared_overlap_unique_task_pairs"] == 1
    assert scope["declared_overlap_graph_groups"] == 2
    assert scope["declared_overlap_graph_components"] == 1
    assert scope["declared_overlap_window_basis_counts"] == {
        "RELEASED_ONLY": 0,
        "EXPIRY_PROXY_ONLY": 1,
        "MIXED_RELEASED_AND_EXPIRY_PROXY": 0,
    }
    assert scope["oracle_coverage_status"] == "INCOMPLETE"
    assert scope["counterfactual_pairwise_tip_conflict_observed"] is False
    assert scope["field_endpoint_status"] == "NOT_ASSESSED"
    assert scope["descriptive_only"] is True
    assert scope["statistical_inference_permitted"] is False
    evidence = scope["pair_evidence"][0]
    assert evidence["tasks"] == ["lt-a", "lt-b"]
    assert evidence["nominal_window_basis"] == "EXPIRY_PROXY_ONLY"
    assert evidence["temporally_overlapping_orbits"][0] == {
        "orbit_ids": ["oa", "ob"],
        "snapshot_states": ["EXPIRED", "EXPIRED"],
        "end_sources": ["EXPIRES_AT_PROXY", "EXPIRES_AT_PROXY"],
        "started_at": [0.0, 2.0],
        "ended_at": [10.0, 8.0],
    }


def test_multiple_orbits_for_one_task_are_aggregated_not_double_counted(tmp_path):
    db = tmp_path / "coord.db"
    _seed_db(
        db,
        tasks=[("lt-a", None, None, None), ("lt-b", None, None, None)],
        orbits=[
            _orbit("oa1", "lt-a", "alice", ["lakatotree/src/a.py"], 0, 10),
            _orbit("oa2", "lt-a", "alice", ["lakatotree/src/b.py"], 1, 9),
            _orbit("ob", "lt-b", "bob", ["lakatotree/src/a.py"], 2, 8),
        ],
    )

    scope = run_pilot(db, [ScopeRule("lt", r"^lt-")]).to_dict()["scopes"][0]

    assert scope["task_agent_groups"] == 2
    assert scope["orbit_rows"] == 3
    assert scope["candidate_nominal_window_pairs"] == 1
    assert scope["declared_overlap_pairs"] == 1


def test_task_envelope_does_not_invent_path_overlap_across_separate_orbit_windows(
    tmp_path,
):
    """A task-level min/max envelope must not smear an early path into a later lease."""
    db = tmp_path / "coord.db"
    _seed_db(
        db,
        tasks=[("lt-a", None, None, None), ("lt-b", None, None, None)],
        orbits=[
            _orbit("oa-early", "lt-a", "alice", ["repo/src/x.py"], 0, 2),
            _orbit("oa-late", "lt-a", "alice", ["repo/src/y.py"], 10, 12),
            _orbit("ob-late", "lt-b", "bob", ["repo/src/x.py"], 10, 12),
        ],
    )

    scope = run_pilot(db, [ScopeRule("lt", r"^lt-")]).to_dict()["scopes"][0]

    assert scope["candidate_nominal_window_pairs"] == 1
    assert scope["declared_overlap_pairs"] == 0
    assert scope["declared_overlap_status"] == "ZERO"
    assert scope["oracle_coverage_status"] == "INCOMPLETE"


def test_explicit_path_roots_canonicalize_different_worktree_clones(tmp_path):
    db = tmp_path / "coord.db"
    _seed_db(
        db,
        tasks=[("lt-a", None, None, None), ("lt-b", None, None, None)],
        orbits=[
            _orbit(
                "oa",
                "lt-a",
                "alice",
                ["/work/clone-a/lakatotree/src/"],
                0,
                10,
            ),
            _orbit(
                "ob",
                "lt-b",
                "bob",
                ["/work/clone-b/lakatotree/src/x.py"],
                1,
                9,
            ),
        ],
    )

    without_roots = run_pilot(db, [ScopeRule("lt", r"^lt-")]).to_dict()["scopes"][0]
    with_roots = run_pilot(
        db,
        [ScopeRule("lt", r"^lt-")],
        path_roots={"lt": ["/work/clone-a/lakatotree", "/work/clone-b/lakatotree"]},
    ).to_dict()["scopes"][0]

    assert without_roots["declared_overlap_pairs"] == 0
    assert with_roots["declared_overlap_pairs"] == 1
    assert with_roots["path_roots"] == [
        "/work/clone-a/lakatotree",
        "/work/clone-b/lakatotree",
    ]


def test_released_at_closes_window_before_lease_expiry(tmp_path):
    db = tmp_path / "coord.db"
    _seed_db(
        db,
        tasks=[("lt-a", None, None, None), ("lt-b", None, None, None)],
        orbits=[
            _orbit("oa", "lt-a", "alice", ["lakatotree/**"], 0, 100, released=2),
            _orbit("ob", "lt-b", "bob", ["lakatotree/**"], 3, 10),
        ],
    )

    scope = run_pilot(db, [ScopeRule("lt", r"^lt-")]).to_dict()["scopes"][0]

    assert scope["candidate_nominal_window_pairs"] == 0
    assert scope["declared_overlap_pair_fraction"] is None
    assert scope["oracle_coverage_status"] == "NO_CANDIDATE_WINDOWS"


def test_ambiguous_and_unclassified_attempts_are_excluded_fail_loud(tmp_path):
    db = tmp_path / "coord.db"
    _seed_db(
        db,
        tasks=[("shared-a", None, None, None), ("unknown", None, None, None)],
        orbits=[
            _orbit("oa", "shared-a", "alice", ["repo/**"], 0, 10),
            _orbit("ob", "unknown", "bob", ["elsewhere/**"], 0, 10),
        ],
    )

    report = run_pilot(
        db,
        [ScopeRule("one", r"shared"), ScopeRule("two", r"shared")],
    ).to_dict()

    assert report["ambiguous_task_agent_groups"] == ["shared-a@alice"]
    assert report["unclassified_task_agent_groups"] == ["unknown@bob"]
    assert all(scope["task_agent_groups"] == 0 for scope in report["scopes"])


def test_git_merge_tree_oracle_sees_conflict_and_clean_pair_without_repo_writes(
    tmp_path,
):
    repo = tmp_path / "repo"
    repo.mkdir()
    _git(repo, "init", "-b", "main")
    _git(repo, "config", "user.name", "pilot")
    _git(repo, "config", "user.email", "pilot@example.invalid")
    (repo / "shared.txt").write_text("base\n")
    _git(repo, "add", "-A")
    _git(repo, "commit", "-m", "base")
    base = _git(repo, "rev-parse", "HEAD")

    conflict_a = _commit_branch(repo, base, "conflict-a", {"shared.txt": "A\n"})
    conflict_b = _commit_branch(repo, base, "conflict-b", {"shared.txt": "B\n"})
    clean_a = _commit_branch(repo, base, "clean-a", {"left.txt": "left\n"})
    clean_b = _commit_branch(repo, base, "clean-b", {"right.txt": "right\n"})
    before = _git(repo, "count-objects", "-v")

    db = tmp_path / "coord.db"
    _seed_db(
        db,
        tasks=[
            ("demo-conflict-a", conflict_a, None, None),
            ("demo-conflict-b", conflict_b, None, None),
            ("demo-clean-a", clean_a, None, None),
            ("demo-clean-b", clean_b, None, None),
        ],
        orbits=[
            _orbit("o1", "demo-conflict-a", "a", ["**"], 0, 10),
            _orbit("o2", "demo-conflict-b", "b", ["**"], 1, 9),
            _orbit("o3", "demo-clean-a", "c", ["**"], 20, 30),
            _orbit("o4", "demo-clean-b", "d", ["**"], 21, 29),
        ],
    )

    scope = run_pilot(
        db,
        [ScopeRule("demo", r"^demo-")],
        git_repos={"demo": repo},
    ).to_dict()["scopes"][0]

    assert scope["declared_overlap_pairs"] == 2
    assert scope["oracle_eligible_pairs"] == 2
    assert scope["pairwise_tip_conflict_pairs"] == 1
    assert scope["pairwise_tip_clean_pairs"] == 1
    assert scope["pairwise_tip_conflict_pair_fraction"] == 0.5
    assert scope["oracle_coverage"] == 1.0
    assert scope["oracle_coverage_status"] == "COMPLETE"
    assert scope["counterfactual_pairwise_tip_conflict_observed"] is True
    assert scope["field_endpoint_status"] == "NOT_ASSESSED"
    assert _git(repo, "count-objects", "-v") == before


def test_git_oracle_does_not_execute_repo_custom_merge_driver(tmp_path):
    repo = tmp_path / "repo"
    repo.mkdir()
    _git(repo, "init", "-b", "main")
    _git(repo, "config", "user.name", "pilot")
    _git(repo, "config", "user.email", "pilot@example.invalid")
    marker = tmp_path / "driver-was-executed"
    _git(repo, "config", "merge.evil.driver", f"touch {marker}")
    (repo / ".gitattributes").write_text("*.txt merge=evil\n")
    (repo / "shared.txt").write_text("base\n")
    _git(repo, "add", "-A")
    _git(repo, "commit", "-m", "base")
    base = _git(repo, "rev-parse", "HEAD")
    left = _commit_branch(repo, base, "left", {"shared.txt": "left\n"})
    right = _commit_branch(repo, base, "right", {"shared.txt": "right\n"})

    db = tmp_path / "coord.db"
    _seed_db(
        db,
        tasks=[("demo-left", left, None, None), ("demo-right", right, None, None)],
        orbits=[
            _orbit("o1", "demo-left", "a", ["**"], 0, 10),
            _orbit("o2", "demo-right", "b", ["**"], 1, 9),
        ],
    )

    scope = run_pilot(
        db,
        [ScopeRule("demo", r"^demo-")],
        git_repos={"demo": repo},
    ).to_dict()["scopes"][0]

    assert not marker.exists(), "untrusted repository merge driver executed"
    assert scope["pairwise_tip_conflict_pairs"] == 1
    assert scope["git_oracle_errors"] == 0


def test_git_conflict_oracle_covers_declared_disjoint_candidate_pair(tmp_path):
    repo = tmp_path / "repo"
    repo.mkdir()
    _git(repo, "init", "-b", "main")
    _git(repo, "config", "user.name", "pilot")
    _git(repo, "config", "user.email", "pilot@example.invalid")
    (repo / "shared.txt").write_text("base\n")
    _git(repo, "add", "-A")
    _git(repo, "commit", "-m", "base")
    base = _git(repo, "rev-parse", "HEAD")
    left = _commit_branch(repo, base, "left", {"shared.txt": "left\n"})
    right = _commit_branch(repo, base, "right", {"shared.txt": "right\n"})

    db = tmp_path / "coord.db"
    _seed_db(
        db,
        tasks=[("demo-left", left, None, None), ("demo-right", right, None, None)],
        orbits=[
            _orbit("o1", "demo-left", "a", ["declared/a.py"], 0, 10),
            _orbit("o2", "demo-right", "b", ["declared/b.py"], 1, 9),
        ],
    )

    scope = run_pilot(
        db,
        [ScopeRule("demo", r"^demo-")],
        git_repos={"demo": repo},
    ).to_dict()["scopes"][0]

    assert scope["declared_overlap_pairs"] == 0
    assert scope["pairwise_tip_conflict_pairs"] == 1
    assert scope["oracle_coverage"] == 1.0
    assert scope["counterfactual_pairwise_tip_conflict_observed"] is True
    assert scope["field_endpoint_status"] == "NOT_ASSESSED"


def test_auxiliary_orbit_kinds_are_not_workload_attempts(tmp_path):
    db = tmp_path / "coord.db"
    _seed_db(
        db,
        tasks=[("lt-a", None, None, None)],
        orbits=[_orbit("oa", "lt-a", "alice", ["repo/**"], 0, 10)],
    )
    with sqlite3.connect(db) as conn:
        conn.execute(
            "INSERT INTO orbits(orbit_id, task_id, agent_id, pathspec, mode, state, "
            "kind, expires_at, created_at, released_at) "
            "VALUES ('token', NULL, 'system', '[]', 'write', 'HELD', "
            "'merge_token', 10, 0, NULL)"
        )

    report = run_pilot(db, [ScopeRule("lt", r"^lt-")]).to_dict()

    assert report["source"]["task_agent_groups"] == 1
    assert report["source"]["orbit_rows"] == 1


def test_task_tip_is_withheld_after_multi_agent_requeue(tmp_path):
    db = tmp_path / "coord.db"
    _seed_db(
        db,
        tasks=[
            ("lt-requeued", "a" * 40, None, None),
            ("lt-peer", "b" * 40, None, None),
        ],
        orbits=[
            _orbit("old", "lt-requeued", "old-agent", ["repo/**"], 0, 10),
            _orbit("peer", "lt-peer", "peer", ["repo/**"], 1, 9),
            _orbit("new", "lt-requeued", "new-agent", ["repo/**"], 20, 30),
        ],
    )

    attempts = load_attempts(db)
    requeued = [attempt for attempt in attempts if attempt.task_id == "lt-requeued"]
    assert {attempt.branch_tip_sha for attempt in requeued} == {None}
    assert {attempt.branch_tip_provenance for attempt in requeued} == {
        "WITHHELD_MULTI_AGENT_TASK"
    }

    scope = run_pilot(db, [ScopeRule("lt", r"^lt-")]).to_dict()["scopes"][0]

    assert scope["candidate_nominal_window_pairs"] == 1
    assert scope["oracle_eligible_pairs"] == 0
    assert scope["git_provenance_missing_pairs"] == 1


def test_cutoff_withholds_mutable_task_tip_without_pre_cutoff_merge(tmp_path):
    db = tmp_path / "coord.db"
    _seed_db(
        db,
        tasks=[("lt-live", "a" * 40, None, None)],
        orbits=[_orbit("oa", "lt-live", "alice", ["repo/**"], 0, 10)],
    )

    [attempt] = load_attempts(db, created_before=5)

    assert attempt.branch_tip_sha is None
    assert attempt.branch_tip_provenance == "WITHHELD_CUTOFF_UNBOUND"


def test_pre_cutoff_merged_single_agent_tip_is_provenance_eligible(tmp_path):
    db = tmp_path / "coord.db"
    _seed_db(
        db,
        tasks=[("lt-merged", "a" * 40, "b" * 40, 4)],
        orbits=[_orbit("oa", "lt-merged", "alice", ["repo/**"], 0, 3)],
    )

    [attempt] = load_attempts(db, created_before=5)

    assert attempt.branch_tip_sha == "a" * 40
    assert attempt.branch_tip_provenance == (
        "AVAILABLE_MERGED_BEFORE_CUTOFF_SINGLE_NOMINAL_EPOCH"
    )


def test_overlapping_released_orbits_form_one_tip_eligible_nominal_epoch(tmp_path):
    db = tmp_path / "coord.db"
    _seed_db(
        db,
        tasks=[("lt-one-epoch", "a" * 40, None, None)],
        orbits=[
            _orbit("o1", "lt-one-epoch", "alice", ["repo/a.py"], 0, 10, released=10),
            _orbit("o2", "lt-one-epoch", "alice", ["repo/b.py"], 1, 9, released=9),
        ],
    )

    [attempt] = load_attempts(db)

    assert attempt.acquisition_epochs == 1
    assert attempt.branch_tip_sha == "a" * 40
    assert attempt.branch_tip_provenance == (
        "AVAILABLE_SINGLE_AGENT_SINGLE_NOMINAL_EPOCH_SNAPSHOT"
    )


def test_touching_same_agent_windows_withhold_task_level_tip(tmp_path):
    db = tmp_path / "coord.db"
    _seed_db(
        db,
        tasks=[("lt-two-epochs", "a" * 40, None, None)],
        orbits=[
            _orbit("o1", "lt-two-epochs", "alice", ["repo/a.py"], 0, 10, released=10),
            _orbit("o2", "lt-two-epochs", "alice", ["repo/b.py"], 10, 20, released=20),
        ],
    )

    [attempt] = load_attempts(db)

    assert attempt.acquisition_epochs == 2
    assert attempt.branch_tip_sha is None
    assert attempt.branch_tip_provenance == ("WITHHELD_AMBIGUOUS_MULTI_ORBIT_HISTORY")


def test_expiry_proxy_cannot_bridge_multiple_rows_into_tip_provenance(tmp_path):
    db = tmp_path / "coord.db"
    _seed_db(
        db,
        tasks=[("lt-proxy-bridge", "a" * 40, None, None)],
        orbits=[
            _orbit("o1", "lt-proxy-bridge", "alice", ["repo/a.py"], 0, 20),
            _orbit("o2", "lt-proxy-bridge", "alice", ["repo/b.py"], 10, 30),
        ],
    )

    [attempt] = load_attempts(db)

    assert attempt.acquisition_epochs == 1
    assert attempt.branch_tip_sha is None
    assert attempt.branch_tip_provenance == ("WITHHELD_AMBIGUOUS_MULTI_ORBIT_HISTORY")


def test_non_finite_merged_at_is_rejected(tmp_path):
    db = tmp_path / "coord.db"
    _seed_db(
        db,
        tasks=[("lt-bad-time", "a" * 40, None, float("-inf"))],
        orbits=[_orbit("oa", "lt-bad-time", "alice", ["repo/**"], 0, 10)],
    )

    with pytest.raises(PilotDataError, match="merged_at must be finite"):
        load_attempts(db, created_before=5)


def test_live_wal_source_files_are_byte_unchanged(tmp_path):
    db_path = tmp_path / "coord.db"
    db = sqlite3.connect(db_path)
    try:
        db.execute("PRAGMA journal_mode=WAL")
        db.execute("PRAGMA wal_autocheckpoint=0")
        db.executescript(
            """
            CREATE TABLE tasks (
              task_id TEXT PRIMARY KEY, branch_tip_sha TEXT, merge_sha TEXT,
              merged_at REAL
            );
                CREATE TABLE orbits (
                  orbit_id TEXT PRIMARY KEY, task_id TEXT, agent_id TEXT,
                  pathspec TEXT NOT NULL, mode TEXT NOT NULL, state TEXT NOT NULL,
                  kind TEXT NOT NULL DEFAULT 'orbit',
                  expires_at REAL, created_at REAL, released_at REAL
            );
            """
        )
        db.execute("INSERT INTO tasks VALUES ('lt-a', NULL, NULL, NULL)")
        db.execute(
            "INSERT INTO orbits(orbit_id, task_id, agent_id, pathspec, mode, state, "
            "expires_at, created_at, released_at) VALUES "
            "('oa', 'lt-a', 'alice', '[\"repo/**\"]', 'write', 'HELD', 10, 0, NULL)"
        )
        db.commit()
        files = [db_path, Path(f"{db_path}-wal"), Path(f"{db_path}-shm")]
        before = {path.name: _sha(path) for path in files if path.exists()}

        run_pilot(db_path, [ScopeRule("lt", r"^lt-")])

        after = {path.name: _sha(path) for path in files if path.exists()}
        assert after == before
    finally:
        db.close()


def test_measurement_digest_binds_scope_and_path_root_configuration(tmp_path):
    db = tmp_path / "coord.db"
    _seed_db(
        db,
        tasks=[("lt-a", None, None, None)],
        orbits=[_orbit("oa", "lt-a", "alice", ["/clone/repo/x.py"], 0, 10)],
    )

    one = run_pilot(db, [ScopeRule("lt", r"^lt-")]).to_dict()
    two = run_pilot(
        db,
        [ScopeRule("renamed", r"^lt-")],
        path_roots={"renamed": ["/clone/repo"]},
    ).to_dict()

    assert (
        one["source"]["canonical_input_sha256"]
        == two["source"]["canonical_input_sha256"]
    )
    assert one["measurement_sha256"] != two["measurement_sha256"]


def test_measurement_digest_binds_entire_report_and_overlap_dependency(tmp_path):
    db = tmp_path / "coord.db"
    _seed_db(
        db,
        tasks=[("lt-a", None, None, None)],
        orbits=[_orbit("oa", "lt-a", "alice", ["repo/**"], 0, 10)],
    )

    report = run_pilot(db, [ScopeRule("lt", r"^lt-")]).to_dict()
    expected = report.pop("measurement_sha256")
    canonical = json.dumps(
        report, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    ).encode()

    assert hashlib.sha256(canonical).hexdigest() == expected
    assert set(report["measurement_config"]["implementation_files_sha256"]) == {
        "omd_server/disjoint.py",
        "omd_server/overlap_pilot.py",
    }


def test_scope_rule_spec_requires_name_and_regex():
    assert ScopeRule.from_spec(r"lt=^lt-") == ScopeRule("lt", r"^lt-")

    for invalid in ("", "lt", "=x", "lt="):
        try:
            ScopeRule.from_spec(invalid)
        except ValueError:
            pass
        else:  # pragma: no cover - assertion branch only
            raise AssertionError(f"invalid scope spec accepted: {invalid!r}")


def test_scope_anchor_matches_each_field_line(tmp_path):
    db = tmp_path / "coord.db"
    _seed_db(
        db,
        tasks=[("unrelated", None, None, None)],
        orbits=[_orbit("oa", "unrelated", "alice", ["repo/src/x.py"], 0, 10)],
    )

    scope = run_pilot(db, [ScopeRule("path", r"^repo/")]).to_dict()["scopes"][0]

    assert scope["task_agent_groups"] == 1


def test_candidate_pair_limit_fails_closed(tmp_path):
    db = tmp_path / "coord.db"
    _seed_db(
        db,
        tasks=[(f"lt-{name}", None, None, None) for name in ("a", "b", "c")],
        orbits=[
            _orbit("oa", "lt-a", "alice", ["repo/**"], 0, 10),
            _orbit("ob", "lt-b", "bob", ["repo/**"], 0, 10),
            _orbit("oc", "lt-c", "carol", ["repo/**"], 0, 10),
        ],
    )

    with pytest.raises(PilotDataError, match="candidate pair limit 2 exceeded"):
        run_pilot(
            db,
            [ScopeRule("lt", r"^lt-")],
            max_candidate_pairs=2,
        )


def test_pair_comparison_limit_bounds_non_overlapping_cohort(tmp_path):
    db = tmp_path / "coord.db"
    _seed_db(
        db,
        tasks=[(f"lt-{name}", None, None, None) for name in ("a", "b", "c")],
        orbits=[
            _orbit("oa", "lt-a", "alice", ["repo/**"], 0, 1),
            _orbit("ob", "lt-b", "bob", ["repo/**"], 2, 3),
            _orbit("oc", "lt-c", "carol", ["repo/**"], 4, 5),
        ],
    )

    with pytest.raises(PilotDataError, match="pair comparison limit 2 exceeded"):
        run_pilot(
            db,
            [ScopeRule("lt", r"^lt-")],
            max_candidate_pairs=0,
            max_pair_comparisons=2,
        )


def test_orbit_pair_comparison_limit_fails_closed(tmp_path):
    db = tmp_path / "coord.db"
    _seed_db(
        db,
        tasks=[("lt-a", None, None, None), ("lt-b", None, None, None)],
        orbits=[
            _orbit("a1", "lt-a", "alice", ["repo/a"], 0, 10),
            _orbit("a2", "lt-a", "alice", ["repo/b"], 0, 10),
            _orbit("b1", "lt-b", "bob", ["repo/c"], 0, 10),
            _orbit("b2", "lt-b", "bob", ["repo/d"], 0, 10),
        ],
    )

    with pytest.raises(PilotDataError, match="orbit pair comparison limit 3 exceeded"):
        run_pilot(
            db,
            [ScopeRule("lt", r"^lt-")],
            max_orbit_pair_comparisons=3,
        )


def test_path_selector_pair_and_per_orbit_limits_fail_closed(tmp_path):
    db = tmp_path / "coord.db"
    _seed_db(
        db,
        tasks=[("lt-a", None, None, None), ("lt-b", None, None, None)],
        orbits=[
            _orbit("oa", "lt-a", "alice", ["repo/a", "repo/b"], 0, 10),
            _orbit("ob", "lt-b", "bob", ["repo/c", "repo/d"], 0, 10),
        ],
    )

    with pytest.raises(PilotDataError, match="path pair comparison limit 3 exceeded"):
        run_pilot(
            db,
            [ScopeRule("lt", r"^lt-")],
            max_path_pair_comparisons=3,
        )
    with pytest.raises(PilotDataError, match="pathspec exceeds 1 selectors"):
        run_pilot(
            db,
            [ScopeRule("lt", r"^lt-")],
            max_paths_per_orbit=1,
        )


def test_require_complete_oracle_cli_has_distinct_incomplete_exit(tmp_path, capsys):
    db = tmp_path / "coord.db"
    _seed_db(
        db,
        tasks=[("lt-a", None, None, None), ("lt-b", None, None, None)],
        orbits=[
            _orbit("oa", "lt-a", "alice", ["repo/**"], 0, 10),
            _orbit("ob", "lt-b", "bob", ["repo/**"], 1, 9),
        ],
    )

    result = main(
        [
            "--db",
            str(db),
            "--scope",
            r"lt=^lt-",
            "--compact",
            "--require-complete-oracle",
        ]
    )

    assert result == 3
    assert (
        json.loads(capsys.readouterr().out)["scopes"][0]["oracle_coverage_status"]
        == "INCOMPLETE"
    )


def test_malformed_sqlite_cli_returns_data_error_without_traceback(tmp_path, capsys):
    db = tmp_path / "not-sqlite.db"
    db.write_bytes(b"not sqlite")

    result = main(["--db", str(db), "--scope", r"lt=^lt-"])

    captured = capsys.readouterr()
    assert result == 2
    assert "omd-overlap-pilot:" in captured.err
    assert "Traceback" not in captured.err


def test_cutoff_and_control_task_exclusion_limit_row_membership(tmp_path):
    db = tmp_path / "coord.db"
    _seed_db(
        db,
        tasks=[
            ("lt-before", None, None, None),
            ("lt-after", None, None, None),
            ("lt-control", None, None, None),
        ],
        orbits=[
            _orbit("o1", "lt-before", "a", ["repo/**"], 1, 4),
            _orbit("o2", "lt-after", "b", ["repo/**"], 10, 14),
            _orbit("o3", "lt-control", "c", ["repo/**"], 2, 3),
        ],
    )

    report = run_pilot(
        db,
        [ScopeRule("lt", r"^lt-")],
        created_before=5,
        exclude_task_ids=["lt-control"],
    ).to_dict()

    assert report["source"]["task_agent_groups"] == 1
    assert report["source"]["filters"] == {
        "created_before_exclusive": 5,
        "excluded_task_ids": ["lt-control"],
    }
    assert report["scopes"][0]["task_agent_groups"] == 1
