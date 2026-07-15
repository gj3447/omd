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


def _upgrade_to_native_provenance_schema(path: Path) -> None:
    """Add the R3 provenance tables/columns without synthesizing row history."""

    with sqlite3.connect(path) as db:
        for column, sql_type in (
            ("fence", "INTEGER"),
            ("attempt_id", "TEXT"),
            ("requested_at", "REAL"),
            ("granted_at", "REAL"),
            ("requested_ttl", "REAL"),
            ("terminal_at", "REAL"),
            ("terminal_effective_at", "REAL"),
            ("reclaimed_at", "REAL"),
            ("terminal_reason", "TEXT"),
        ):
            db.execute(f"ALTER TABLE orbits ADD COLUMN {column} {sql_type}")
        db.execute("ALTER TABLE tasks ADD COLUMN attempt_id TEXT")
        db.execute("ALTER TABLE tasks ADD COLUMN connect_attempt_id TEXT")
        db.executescript(
            """
            CREATE TABLE task_attempts (
              attempt_id TEXT PRIMARY KEY,
              task_id TEXT NOT NULL,
              attempt_ordinal INTEGER NOT NULL,
              agent_id TEXT NOT NULL,
              repo_id TEXT,
              repo_root TEXT,
              integration_branch TEXT,
              writes TEXT NOT NULL,
              shared TEXT NOT NULL,
              opened_at REAL NOT NULL,
              opened_by TEXT NOT NULL,
              started_at REAL,
              finished_at REAL,
              finish_source TEXT,
              finished_by TEXT,
              worktree_base_sha TEXT,
              branch TEXT,
              terminal_at REAL,
              terminal_state TEXT,
              terminal_reason TEXT,
              actor_trust TEXT NOT NULL DEFAULT 'SELF_ASSERTED'
            );
            CREATE TABLE connect_attempts (
              connect_attempt_id TEXT PRIMARY KEY,
              attempt_id TEXT NOT NULL,
              task_id TEXT NOT NULL,
              connect_seq INTEGER NOT NULL,
              token_id TEXT NOT NULL,
              orbit_ids TEXT NOT NULL,
              orbit_fences TEXT NOT NULL,
              coordinator_epoch INTEGER,
              trigger_kind TEXT NOT NULL DEFAULT 'DIRECT',
              barrier_id TEXT,
              barrier_generation INTEGER,
              started_at REAL NOT NULL,
              branch_tip_sha TEXT,
              integration_base_sha TEXT,
              candidate_tree_sha TEXT,
              candidate_commit_sha TEXT,
              candidate_prepared_at REAL,
              terminal_at REAL,
              outcome TEXT,
              outcome_code TEXT,
              merge_sha TEXT,
              merge_gen INTEGER,
              resolution_source TEXT,
              detail TEXT
            );
            """
        )


def _insert_native_attempt(
    db: sqlite3.Connection,
    *,
    attempt_id: str,
    task_id: str,
    ordinal: int,
    agent_id: str,
    opened_at: float,
    started_at: float,
    terminal_at: float,
    terminal_state: str,
) -> None:
    db.execute(
        """
        INSERT INTO task_attempts(
          attempt_id,task_id,attempt_ordinal,agent_id,repo_id,repo_root,
          integration_branch,writes,shared,opened_at,opened_by,started_at,
          finished_at,finish_source,finished_by,worktree_base_sha,branch,
          terminal_at,terminal_state,terminal_reason,actor_trust
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """,
        (
            attempt_id,
            task_id,
            ordinal,
            agent_id,
            "repo:test",
            "/immutable/repo",
            "main",
            '["repo/**"]',
            "[]",
            opened_at,
            "CLAIM",
            started_at,
            started_at + 1,
            "EXPLICIT",
            agent_id,
            "0" * 40,
            f"omd/{attempt_id}",
            terminal_at,
            terminal_state,
            terminal_state.lower(),
            "SELF_ASSERTED",
        ),
    )


def _insert_native_orbit(
    db: sqlite3.Connection,
    *,
    orbit_id: str,
    attempt_id: str,
    task_id: str,
    agent_id: str,
    requested_at: float,
    granted_at: float,
    terminal_at: float,
    terminal_effective_at: float,
    fence: int,
) -> None:
    db.execute(
        """
        INSERT INTO orbits(
          orbit_id,task_id,agent_id,pathspec,mode,state,kind,expires_at,
          created_at,released_at,fence,attempt_id,requested_at,granted_at,
          requested_ttl,terminal_at,terminal_effective_at,reclaimed_at,
          terminal_reason
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """,
        (
            orbit_id,
            task_id,
            agent_id,
            '["repo/**"]',
            "write",
            "RELEASED",
            "orbit",
            terminal_effective_at + 100,
            requested_at - 1000,
            terminal_at,
            fence,
            attempt_id,
            requested_at,
            granted_at,
            600,
            terminal_at,
            terminal_effective_at,
            None,
            "connect_merged",
        ),
    )


def _insert_connect_try(
    db: sqlite3.Connection,
    *,
    connect_id: str,
    attempt_id: str,
    task_id: str,
    seq: int,
    orbit_id: str,
    fence: int,
    tip: str,
    started_at: float,
    terminal_at: float,
    outcome: str,
    merge_gen: int | None = None,
    outcome_code: str | None = None,
    integration_base_sha: str = "b" * 40,
    merge_sha: str | None = None,
    candidate_tree_sha: str | None = None,
    candidate_commit_sha: str | None = None,
    candidate_prepared_at: float | None = None,
) -> None:
    outcome_code = outcome if outcome_code is None else outcome_code
    if merge_sha is None and outcome == "MERGED":
        merge_sha = "c" * 40
    if (
        outcome == "MERGED"
        and outcome_code not in {
            "ALREADY_INTEGRATED",
            "RECOVERED_ALREADY_INTEGRATED",
        }
        and candidate_tree_sha is None
        and candidate_commit_sha is None
        and candidate_prepared_at is None
    ):
        candidate_tree_sha = "d" * 40
        candidate_commit_sha = merge_sha
        candidate_prepared_at = started_at + (terminal_at - started_at) / 2
    db.execute(
        """
        INSERT INTO connect_attempts(
          connect_attempt_id,attempt_id,task_id,connect_seq,token_id,orbit_ids,
          orbit_fences,coordinator_epoch,trigger_kind,barrier_id,
          barrier_generation,started_at,branch_tip_sha,integration_base_sha,
          candidate_tree_sha,candidate_commit_sha,candidate_prepared_at,
          terminal_at,outcome,outcome_code,merge_sha,merge_gen,
          resolution_source,detail
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """,
        (
            connect_id,
            attempt_id,
            task_id,
            seq,
            f"token-{connect_id}",
            json.dumps([orbit_id]),
            json.dumps({orbit_id: fence}),
            1,
            "DIRECT",
            None,
            None,
            started_at,
            tip,
            integration_base_sha,
            candidate_tree_sha,
            candidate_commit_sha,
            candidate_prepared_at,
            terminal_at,
            outcome,
            outcome_code,
            merge_sha,
            merge_gen,
            "LIVE",
            f"detail-{connect_id}",
        ),
    )


def _seed_single_native_connect(
    path: Path,
    *,
    outcome: str = "MERGED",
    outcome_code: str | None = None,
    merge_gen: int | None = 1,
    integration_base_sha: str = "b" * 40,
    merge_sha: str | None = None,
    candidate_tree_sha: str | None = None,
    candidate_commit_sha: str | None = None,
    candidate_prepared_at: float | None = None,
) -> None:
    _seed_db(path, tasks=[("lt-one", None, None, None)], orbits=[])
    _upgrade_to_native_provenance_schema(path)
    with sqlite3.connect(path) as db:
        _insert_native_attempt(
            db,
            attempt_id="att-one",
            task_id="lt-one",
            ordinal=1,
            agent_id="alice",
            opened_at=10,
            started_at=15,
            terminal_at=40,
            terminal_state="MERGED" if outcome == "MERGED" else "RECLAIMED",
        )
        _insert_native_orbit(
            db,
            orbit_id="orb-one",
            attempt_id="att-one",
            task_id="lt-one",
            agent_id="alice",
            requested_at=11,
            granted_at=15,
            terminal_at=40,
            terminal_effective_at=39,
            fence=1,
        )
        _insert_connect_try(
            db,
            connect_id="con-one",
            attempt_id="att-one",
            task_id="lt-one",
            seq=1,
            orbit_id="orb-one",
            fence=1,
            tip="a" * 40,
            started_at=20,
            terminal_at=30,
            outcome=outcome,
            outcome_code=outcome_code,
            merge_gen=merge_gen if outcome == "MERGED" else None,
            integration_base_sha=integration_base_sha,
            merge_sha=merge_sha,
            candidate_tree_sha=candidate_tree_sha,
            candidate_commit_sha=candidate_commit_sha,
            candidate_prepared_at=candidate_prepared_at,
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


def test_empty_migrated_provenance_tables_remain_legacy_v2(tmp_path):
    db = tmp_path / "coord.db"
    _seed_db(
        db,
        tasks=[("lt-a", None, None, None)],
        orbits=[_orbit("oa", "lt-a", "alice", ["repo/**"], 0, 10)],
    )
    _upgrade_to_native_provenance_schema(db)

    report = run_pilot(db, [ScopeRule("lt", r"^lt-")]).to_dict()

    assert report["schema"] == "omd-base-overlap-pilot/v2"
    assert "provenance_mode" not in report["source"]
    assert report["source"]["task_agent_groups"] == 1


def test_native_v3_uses_attempt_windows_first_connect_and_full_retry_history(tmp_path):
    db_path = tmp_path / "coord.db"
    _seed_db(
        db_path,
        tasks=[
            ("lt-requeue", "d" * 40, "e" * 40, 999),
            ("lt-peer", "f" * 40, None, None),
            ("lt-legacy", None, None, None),
        ],
        orbits=[
            _orbit("legacy", "lt-legacy", "legacy", ["repo/**"], 22, 34),
        ],
    )
    _upgrade_to_native_provenance_schema(db_path)
    with sqlite3.connect(db_path) as db:
        _insert_native_attempt(
            db,
            attempt_id="att-old",
            task_id="lt-requeue",
            ordinal=1,
            agent_id="alice",
            opened_at=1,
            started_at=5,
            terminal_at=20,
            terminal_state="RECLAIMED",
        )
        _insert_native_attempt(
            db,
            attempt_id="att-new",
            task_id="lt-requeue",
            ordinal=2,
            agent_id="alice",
            opened_at=21,
            started_at=25,
            terminal_at=41,
            terminal_state="MERGED",
        )
        _insert_native_attempt(
            db,
            attempt_id="att-peer",
            task_id="lt-peer",
            ordinal=1,
            agent_id="bob",
            opened_at=23,
            started_at=24,
            terminal_at=36,
            terminal_state="MERGED",
        )
        _insert_native_orbit(
            db,
            orbit_id="orb-old",
            attempt_id="att-old",
            task_id="lt-requeue",
            agent_id="alice",
            requested_at=2,
            granted_at=2,
            terminal_at=20,
            terminal_effective_at=10,
            fence=1,
        )
        _insert_native_orbit(
            db,
            orbit_id="orb-new",
            attempt_id="att-new",
            task_id="lt-requeue",
            agent_id="alice",
            requested_at=22,
            granted_at=23,
            terminal_at=41,
            terminal_effective_at=40,
            fence=2,
        )
        _insert_native_orbit(
            db,
            orbit_id="orb-peer",
            attempt_id="att-peer",
            task_id="lt-peer",
            agent_id="bob",
            requested_at=24,
            granted_at=26,
            terminal_at=36,
            terminal_effective_at=35,
            fence=3,
        )
        _insert_connect_try(
            db,
            connect_id="con-new-1",
            attempt_id="att-new",
            task_id="lt-requeue",
            seq=1,
            orbit_id="orb-new",
            fence=2,
            tip="1" * 40,
            started_at=30,
            terminal_at=31,
            outcome="FAILED",
        )
        _insert_connect_try(
            db,
            connect_id="con-new-2",
            attempt_id="att-new",
            task_id="lt-requeue",
            seq=2,
            orbit_id="orb-new",
            fence=2,
            tip="2" * 40,
            started_at=37,
            terminal_at=38,
            outcome="MERGED",
            merge_gen=2,
        )
        _insert_connect_try(
            db,
            connect_id="con-peer-1",
            attempt_id="att-peer",
            task_id="lt-peer",
            seq=1,
            orbit_id="orb-peer",
            fence=3,
            tip="3" * 40,
            started_at=32,
            terminal_at=34,
            outcome="MERGED",
            merge_gen=1,
        )

    attempts = load_attempts(db_path)
    assert len(attempts) == 3
    assert [
        attempt.attempt_id
        for attempt in attempts
        if attempt.task_id == "lt-requeue"
    ] == ["att-old", "att-new"]
    current = next(attempt for attempt in attempts if attempt.attempt_id == "att-new")
    assert current.orbits[0].started_at == 25
    assert current.orbits[0].ended_at == 40
    assert current.branch_tip_sha == "1" * 40
    assert current.canonical_connect_outcome == "FAILED"
    assert [row.connect_seq for row in current.connect_attempts] == [1, 2]

    report = run_pilot(db_path, [ScopeRule("lt", r"^lt-")]).to_dict()
    source = report["source"]
    scope = report["scopes"][0]
    assert report["schema"] == "omd-base-overlap-pilot/v3"
    assert source["provenance_mode"] == "NATIVE_V3"
    assert source["legacy_workload_orbit_rows_excluded"] == 1
    assert source["legacy_write_capable_orbit_rows_excluded"] == 1
    assert source["attempt_rows"] == 3
    assert [row["merge_gen"] for row in source["merge_order"]] == [1, 2]
    assert scope["task_agent_groups"] == 3
    assert scope["candidate_nominal_window_pairs"] == 1
    assert scope["candidate_window_basis_counts"] == {
        "NATIVE_TERMINAL_EFFECTIVE_AT": 1
    }
    assert scope["pair_evidence"][0]["attempt_ids"] == ["att-peer", "att-new"]

    original_input = source["canonical_input_sha256"]
    original_measurement = report["measurement_sha256"]
    with sqlite3.connect(db_path) as db:
        db.execute(
            "UPDATE tasks SET branch_tip_sha=?,merge_sha=?,merged_at=?",
            ("9" * 40, "8" * 40, -1),
        )
    projection_changed = run_pilot(
        db_path, [ScopeRule("lt", r"^lt-")]
    ).to_dict()
    assert projection_changed["source"]["canonical_input_sha256"] == original_input
    assert projection_changed["measurement_sha256"] == original_measurement

    with sqlite3.connect(db_path) as db:
        db.execute(
            "UPDATE connect_attempts SET detail='retry-history-changed' "
            "WHERE connect_attempt_id='con-new-2'"
        )
    retry_changed = run_pilot(db_path, [ScopeRule("lt", r"^lt-")]).to_dict()
    assert retry_changed["source"]["canonical_input_sha256"] != original_input


@pytest.mark.parametrize(
    ("assignment", "params", "message"),
    [
        (
            "candidate_tree_sha=NULL",
            (),
            "candidate attestation must be complete",
        ),
        (
            "candidate_prepared_at=?",
            (float("inf"),),
            "candidate_prepared_at must be finite",
        ),
        (
            "candidate_prepared_at=19",
            (),
            "candidate_prepared_at precedes started_at",
        ),
        (
            "candidate_prepared_at=31",
            (),
            "candidate_prepared_at follows terminal_at",
        ),
        (
            "candidate_commit_sha='eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee'",
            (),
            "merge_sha does not match the attested candidate commit",
        ),
        (
            "candidate_tree_sha=NULL,candidate_commit_sha=NULL,"
            "candidate_prepared_at=NULL",
            (),
            "MERGED requires an exact candidate commit attestation",
        ),
        (
            "candidate_tree_sha=NULL,candidate_commit_sha=NULL,"
            "candidate_prepared_at=NULL,outcome_code='ALREADY_INTEGRATED'",
            (),
            "MERGED requires an exact candidate commit attestation",
        ),
    ],
)
def test_native_v3_rejects_invalid_candidate_attestation(
    tmp_path, assignment, params, message,
):
    db_path = tmp_path / "coord.db"
    _seed_single_native_connect(db_path)
    with sqlite3.connect(db_path) as db:
        db.execute(
            f"UPDATE connect_attempts SET {assignment} "
            "WHERE connect_attempt_id='con-one'",
            params,
        )

    with pytest.raises(PilotDataError, match=message):
        load_attempts(db_path)


@pytest.mark.parametrize(
    "outcome_code",
    ["ALREADY_INTEGRATED", "RECOVERED_ALREADY_INTEGRATED"],
)
def test_native_v3_accepts_explicit_already_integrated_noop_without_candidate(
    tmp_path, outcome_code,
):
    db_path = tmp_path / "coord.db"
    base = "b" * 40
    _seed_single_native_connect(
        db_path,
        outcome_code=outcome_code,
        integration_base_sha=base,
        merge_sha=base,
    )

    attempt = load_attempts(db_path)[0]
    connect = attempt.connect_attempts[0]
    assert connect.outcome == "MERGED"
    assert connect.outcome_code == outcome_code
    assert connect.merge_sha == base
    assert connect.candidate_tree_sha is None
    assert connect.candidate_commit_sha is None
    assert connect.candidate_prepared_at is None


def test_native_v3_retains_failed_candidate_and_binds_it_into_digest(tmp_path):
    db_path = tmp_path / "coord.db"
    _seed_single_native_connect(
        db_path,
        outcome="FAILED",
        merge_gen=None,
        candidate_tree_sha="d" * 40,
        candidate_commit_sha="e" * 40,
        candidate_prepared_at=25,
    )

    attempt = load_attempts(db_path)[0]
    connect = attempt.connect_attempts[0]
    assert connect.outcome == "FAILED"
    assert connect.candidate_tree_sha == "d" * 40
    assert connect.candidate_commit_sha == "e" * 40
    assert connect.candidate_prepared_at == 25
    original = run_pilot(
        db_path, [ScopeRule("lt", r"^lt-")]
    ).to_dict()["source"]["canonical_input_sha256"]

    with sqlite3.connect(db_path) as db:
        db.execute(
            "UPDATE connect_attempts SET candidate_tree_sha=? "
            "WHERE connect_attempt_id='con-one'",
            ("f" * 40,),
        )
    changed = run_pilot(
        db_path, [ScopeRule("lt", r"^lt-")]
    ).to_dict()["source"]["canonical_input_sha256"]
    assert changed != original


def test_any_partial_native_marker_forbids_legacy_fallback(tmp_path):
    db = tmp_path / "coord.db"
    _seed_db(
        db,
        tasks=[("lt-a", None, None, None)],
        orbits=[_orbit("oa", "lt-a", "alice", ["repo/**"], 0, 10)],
    )
    _upgrade_to_native_provenance_schema(db)
    with sqlite3.connect(db) as conn:
        conn.execute("UPDATE orbits SET requested_at=1 WHERE orbit_id='oa'")

    with pytest.raises(PilotDataError, match="partial native provenance"):
        run_pilot(db, [ScopeRule("lt", r"^lt-")])

    with sqlite3.connect(db) as conn:
        conn.execute("UPDATE orbits SET requested_at=NULL WHERE orbit_id='oa'")
        conn.execute("UPDATE tasks SET attempt_id='missing-attempt' WHERE task_id='lt-a'")
    with pytest.raises(PilotDataError, match="dangling native attempt_id pointer"):
        run_pilot(db, [ScopeRule("lt", r"^lt-")])


def test_synthetic_legacy_adapter_is_excluded_without_poisoning_native_v3(tmp_path):
    db_path = tmp_path / "coord.db"
    _seed_db(
        db_path,
        tasks=[
            ("lt-native", None, None, None),
            ("lt-adapter", None, None, None),
        ],
        orbits=[
            _orbit(
                "orb-legacy-null", "lt-adapter", "legacy-agent",
                ["legacy/**"], 1, 50, released=45,
            ),
        ],
    )
    _upgrade_to_native_provenance_schema(db_path)
    with sqlite3.connect(db_path) as db:
        _insert_native_attempt(
            db, attempt_id="att-native", task_id="lt-native", ordinal=1,
            agent_id="native-agent", opened_at=5, started_at=10,
            terminal_at=30, terminal_state="RECLAIMED",
        )
        _insert_native_orbit(
            db, orbit_id="orb-native", attempt_id="att-native",
            task_id="lt-native", agent_id="native-agent", requested_at=6,
            granted_at=8, terminal_at=30, terminal_effective_at=29, fence=1,
        )
        _insert_native_attempt(
            db, attempt_id="att-adapter", task_id="lt-adapter", ordinal=1,
            agent_id="legacy-agent", opened_at=2, started_at=3,
            terminal_at=45, terminal_state="MERGED",
        )
        db.execute(
            "UPDATE task_attempts SET opened_by='CLAIM_LEGACY' "
            "WHERE attempt_id='att-adapter'"
        )
        _insert_native_orbit(
            db, orbit_id="orb-adapter-bound", attempt_id="att-adapter",
            task_id="lt-adapter", agent_id="legacy-agent", requested_at=4,
            granted_at=5, terminal_at=45, terminal_effective_at=44, fence=3,
        )
        db.execute(
            "UPDATE orbits SET fence=2,terminal_at=45,terminal_effective_at=44,"
            "terminal_reason='connect_merged' WHERE orbit_id='orb-legacy-null'"
        )
        _insert_connect_try(
            db, connect_id="con-adapter", attempt_id="att-adapter",
            task_id="lt-adapter", seq=1, orbit_id="orb-adapter-bound",
            fence=3, tip="a" * 40, started_at=40, terminal_at=45,
            outcome="MERGED", merge_gen=1,
        )
        db.execute(
            "UPDATE connect_attempts SET orbit_ids=?,orbit_fences=? "
            "WHERE connect_attempt_id='con-adapter'",
            (
                json.dumps(["orb-legacy-null", "orb-adapter-bound"]),
                json.dumps({"orb-legacy-null": 2, "orb-adapter-bound": 3}),
            ),
        )

    first = run_pilot(db_path, [ScopeRule("lt", r"^lt-")]).to_dict()
    source = first["source"]
    assert source["attempt_rows"] == 1
    assert source["task_attempt_rows"] == 2
    assert source["synthetic_legacy_attempt_rows_excluded"] == 1
    assert source["synthetic_legacy_connect_attempt_rows_excluded"] == 1
    assert source["synthetic_legacy_bound_orbit_rows_excluded"] == 1
    assert source["synthetic_legacy_null_attempt_orbit_rows_excluded"] == 1
    assert source["synthetic_legacy_write_capable_orbit_rows_excluded"] == 2
    assert first["scopes"][0]["task_agent_groups"] == 1

    original_digest = source["canonical_input_sha256"]
    with sqlite3.connect(db_path) as db:
        db.execute(
            "UPDATE connect_attempts SET detail='excluded-evidence-changed' "
            "WHERE connect_attempt_id='con-adapter'"
        )
    changed = run_pilot(db_path, [ScopeRule("lt", r"^lt-")]).to_dict()
    assert changed["source"]["canonical_input_sha256"] != original_digest

    # A live/incomplete adapter is still excluded; it cannot block the native cohort.
    with sqlite3.connect(db_path) as db:
        db.execute(
            "UPDATE task_attempts SET terminal_at=NULL,terminal_state=NULL,"
            "terminal_reason=NULL WHERE attempt_id='att-adapter'"
        )
        db.execute(
            "UPDATE connect_attempts SET terminal_at=NULL,outcome=NULL,outcome_code=NULL,"
            "branch_tip_sha=NULL,integration_base_sha=NULL,merge_sha=NULL,merge_gen=NULL "
            "WHERE connect_attempt_id='con-adapter'"
        )
    assert len(load_attempts(db_path)) == 1

    # Adapter snapshots cannot smuggle another native generation's orbit.
    with sqlite3.connect(db_path) as db:
        db.execute(
            "UPDATE connect_attempts SET orbit_ids=?,orbit_fences=? "
            "WHERE connect_attempt_id='con-adapter'",
            (json.dumps(["orb-native"]), json.dumps({"orb-native": 1})),
        )
    with pytest.raises(PilotDataError, match="legacy adapter orbit identity mismatch"):
        load_attempts(db_path)

    # Unrelated partial native data remains strict/fail-closed.
    with sqlite3.connect(db_path) as db:
        db.execute(
            "UPDATE connect_attempts SET orbit_ids=?,orbit_fences=? "
            "WHERE connect_attempt_id='con-adapter'",
            (
                json.dumps(["orb-legacy-null", "orb-adapter-bound"]),
                json.dumps({"orb-legacy-null": 2, "orb-adapter-bound": 3}),
            ),
        )
        db.execute(
            "INSERT INTO orbits(orbit_id,task_id,agent_id,pathspec,mode,state,kind,"
            "expires_at,created_at,released_at,requested_at) "
            "VALUES('orb-rogue','lt-native','rogue','[\"rogue/**\"]','write',"
            "'RELEASED','orbit',10,1,9,2)"
        )
    with pytest.raises(PilotDataError, match="partial native provenance"):
        load_attempts(db_path)


def test_runtime_terminal_overlay_on_unadapted_legacy_orbit_stays_excluded(tmp_path):
    db_path = tmp_path / "coord.db"
    _seed_db(
        db_path,
        tasks=[("lt-native", None, None, None), ("lt-legacy", None, None, None)],
        orbits=[_orbit("orb-legacy", "lt-legacy", "old", ["old/**"], 1, 20)],
    )
    _upgrade_to_native_provenance_schema(db_path)
    with sqlite3.connect(db_path) as db:
        _insert_native_attempt(
            db, attempt_id="att-native", task_id="lt-native", ordinal=1,
            agent_id="new", opened_at=2, started_at=3, terminal_at=10,
            terminal_state="RECLAIMED",
        )
        _insert_native_orbit(
            db, orbit_id="orb-native", attempt_id="att-native",
            task_id="lt-native", agent_id="new", requested_at=2,
            granted_at=3, terminal_at=10, terminal_effective_at=9, fence=1,
        )
        # These are exactly the fields an upgraded runtime can add while the
        # original request identity remains unknowable.
        db.execute(
            "UPDATE orbits SET fence=2,granted_at=4,terminal_at=8,"
            "terminal_effective_at=7,terminal_reason='lease_expired' "
            "WHERE orbit_id='orb-legacy'"
        )

    report = run_pilot(db_path, [ScopeRule("lt", r"^lt-")]).to_dict()
    assert report["source"]["attempt_rows"] == 1
    assert report["source"]["legacy_workload_orbit_rows_excluded"] == 1
