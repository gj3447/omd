"""R3 attempt provenance and lifecycle fencing.

These tests pin the durable facts needed by the R3 field trace.  They also
exercise the lifecycle races that become visible once task reuse is separated
from an execution attempt.
"""

from __future__ import annotations

import math
import json
import os
import sqlite3
import subprocess
import sys
import threading
from pathlib import Path

import pytest

from omd_server import Coordinator
from omd_server.gitio import GitError, GitIntegrationPreconditionError, GitRepo
from omd_server.store import Store


def _git(args, cwd):
    return subprocess.run(
        ["git", *args], cwd=str(cwd), check=True, capture_output=True, text=True
    ).stdout.strip()


def _init_repo(root: Path):
    root.mkdir()
    _git(["init", "-b", "main"], root)
    _git(["config", "user.name", "t"], root)
    _git(["config", "user.email", "t@t"], root)
    (root / "README.md").write_text("base\n")
    _git(["add", "-A"], root)
    _git(["commit", "-m", "base"], root)
    _git(["checkout", "-b", "dev"], root)


def _develop(omd: Coordinator, task="T", agent="ag", path="src/x.py"):
    parent = str(Path(path).parent)
    omd.declare(task, writes=[f"{parent}/**"])
    omd.next_task(agent)
    claim = omd.claim(agent, [f"{parent}/**"], task_id=task)
    started = omd.start(task, agent)
    target = Path(started["worktree"]) / path
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text("x = 1\n")
    omd.commit(task, f"feat: {path}")
    omd.finish(task, agent, claim["fence"])
    return claim, started


def test_legacy_schema_migrates_before_new_indexes(tmp_path):
    """A pre-intent-key DB must reach the provenance schema atomically."""

    path = tmp_path / "legacy.db"
    db = sqlite3.connect(path)
    db.execute(
        """
        CREATE TABLE orbits (
          orbit_id TEXT PRIMARY KEY, task_id TEXT, agent_id TEXT,
          pathspec TEXT NOT NULL, mode TEXT NOT NULL, state TEXT NOT NULL,
          fence INTEGER, expires_at REAL, created_at REAL, released_at REAL,
          reason TEXT, priority INTEGER DEFAULT 0
        )
        """
    )
    db.commit()
    db.close()

    store = Store(str(path))
    orbit_cols = {
        row["name"] for row in store.db.execute("PRAGMA table_info(orbits)")
    }
    task_cols = {
        row["name"] for row in store.db.execute("PRAGMA table_info(tasks)")
    }
    assert {
        "attempt_id",
        "requested_at",
        "granted_at",
        "requested_ttl",
        "terminal_at",
        "reclaimed_at",
        "terminal_reason",
    } <= orbit_cols
    assert {"attempt_id", "connect_attempt_id"} <= task_cols
    assert store.db.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='task_attempts'"
    ).fetchone()
    assert store.db.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='connect_attempts'"
    ).fetchone()

    # Reopening is idempotent and does not synthesize provenance for legacy rows.
    store.db.close()
    reopened = Store(str(path))
    assert reopened.db.execute("PRAGMA integrity_check").fetchone()[0] == "ok"


def test_connect_migration_keeps_unobserved_fields_null(tmp_path):
    path = tmp_path / "intermediate.db"
    store = Store(str(path))
    store.add_task(task_id="T", name="", writes=["a/**"], reads=[], deps=[],
                   state="READY", priority=0)
    attempt_id = store.add_attempt(
        task_id="T", agent_id="A", writes=["a/**"], shared=[], opened_by="CLAIM"
    )
    orbit_id = store.add_orbit(
        task_id="T", agent_id="A", pathspec=["a/**"], mode="write",
        state="HELD", fence=1, attempt_id=attempt_id, requested_ttl=30,
    )
    token_id = store.add_orbit(
        task_id=None, agent_id="A", pathspec=[], mode="write", state="HELD",
        fence=2, kind="merge_token",
    )
    store.add_connect_attempt(
        attempt_id=attempt_id, task_id="T", token_id=token_id,
        orbit_ids=[orbit_id], orbit_fences={orbit_id: 1}, coordinator_epoch=1,
        branch_tip_sha="tip", integration_base_sha="base",
    )
    store.db.close()

    db = sqlite3.connect(path)
    for (name,) in db.execute("SELECT name FROM sqlite_master WHERE type='trigger'"):
        db.execute(f'DROP TRIGGER "{name}"')
    for column in ("orbit_fences", "trigger_kind", "barrier_id", "barrier_generation"):
        db.execute(f"ALTER TABLE connect_attempts DROP COLUMN {column}")
    db.commit()
    db.close()

    migrated = Store(str(path))
    row = migrated.connect_attempts_for_attempt(attempt_id)[0]
    assert row["orbit_fences"] is None
    assert row["trigger_kind"] is None
    assert row["barrier_id"] is None
    assert row["barrier_generation"] is None


def test_attempt_rows_reject_replace_and_late_lifecycle_mutation():
    store = Store()
    store.add_task(task_id="T", name="", writes=["a/**"], reads=[], deps=[],
                   state="READY", priority=0)
    attempt_id = store.add_attempt(
        task_id="T", agent_id="A", writes=["a/**"], shared=[], opened_by="CLAIM"
    )
    store.close_attempt(attempt_id, "CANCELLED", "operator")

    with pytest.raises(RuntimeError, match="terminal"):
        store.start_attempt(attempt_id, branch="late", worktree_base_sha="sha")
    assert store.finish_attempt(attempt_id, source="LATE") is False
    with pytest.raises(sqlite3.IntegrityError, match="replacement"):
        store.db.execute(
            "INSERT OR REPLACE INTO task_attempts SELECT * FROM task_attempts "
            "WHERE attempt_id=?", (attempt_id,),
        )
    with pytest.raises(sqlite3.IntegrityError, match="terminal facts"):
        store.db.execute(
            "UPDATE task_attempts SET terminal_reason='rewritten' WHERE attempt_id=?",
            (attempt_id,),
        )


def test_connect_candidate_attestation_is_complete_and_single_assignment():
    store = Store()
    store.add_task(task_id="T", name="", writes=["a/**"], reads=[], deps=[],
                   state="READY", priority=0)
    attempt_id = store.add_attempt(
        task_id="T", agent_id="A", writes=["a/**"], shared=[], opened_by="CLAIM"
    )
    orbit_id = store.add_orbit(
        task_id="T", agent_id="A", pathspec=["a/**"], mode="write",
        state="HELD", fence=1, attempt_id=attempt_id, requested_ttl=30,
    )
    token_id = store.add_orbit(
        task_id=None, agent_id="A", pathspec=[], mode="write", state="HELD",
        fence=2, kind="merge_token",
    )
    connect_id = store.add_connect_attempt(
        attempt_id=attempt_id, task_id="T", token_id=token_id,
        orbit_ids=[orbit_id], orbit_fences={orbit_id: 1}, coordinator_epoch=1,
        branch_tip_sha="b" * 40, integration_base_sha="a" * 40,
    )
    started_at = store.get_connect_attempt(connect_id)["started_at"]

    with pytest.raises(ValueError, match="finite and follow"):
        store.seal_connect_candidate(
            connect_id, tree_sha="c" * 40, commit_sha="d" * 40,
            prepared_at=math.nan,
        )
    assert store.seal_connect_candidate(
        connect_id, tree_sha="c" * 40, commit_sha="d" * 40,
        prepared_at=started_at,
    ) is True
    assert store.seal_connect_candidate(
        connect_id, tree_sha="c" * 40, commit_sha="d" * 40,
        prepared_at=started_at,
    ) is False
    with pytest.raises(RuntimeError, match="conflicts"):
        store.seal_connect_candidate(
            connect_id, tree_sha="e" * 40, commit_sha="f" * 40,
            prepared_at=started_at,
        )
    with pytest.raises(sqlite3.IntegrityError, match="single-assignment"):
        store.db.execute(
            "UPDATE connect_attempts SET candidate_commit_sha=? "
            "WHERE connect_attempt_id=?",
            ("0" * 40, connect_id),
        )
    store.finish_connect_attempt(
        connect_id, outcome="FAILED", outcome_code="TEST", detail="terminal"
    )
    with pytest.raises(RuntimeError, match="terminal"):
        store.seal_connect_candidate(
            connect_id, tree_sha="c" * 40, commit_sha="d" * 40,
            prepared_at=started_at,
        )

    partial_token_id = store.add_orbit(
        task_id=None, agent_id="A", pathspec=[], mode="write", state="HELD",
        fence=3, kind="merge_token",
    )
    partial_id = store.add_connect_attempt(
        attempt_id=attempt_id, task_id="T", token_id=partial_token_id,
        orbit_ids=[orbit_id], orbit_fences={orbit_id: 1}, coordinator_epoch=1,
        branch_tip_sha="b" * 40, integration_base_sha="a" * 40,
    )
    partial_started_at = store.get_connect_attempt(partial_id)["started_at"]
    store.db.execute("DROP TRIGGER connect_attempt_candidate_single_assignment")
    store.db.execute(
        "UPDATE connect_attempts SET candidate_tree_sha=?,candidate_commit_sha=? "
        "WHERE connect_attempt_id=?",
        ("c" * 40, "d" * 40, partial_id),
    )
    with pytest.raises(RuntimeError, match="conflicts"):
        store.seal_connect_candidate(
            partial_id, tree_sha="c" * 40, commit_sha="d" * 40,
            prepared_at=partial_started_at,
        )


@pytest.mark.parametrize("ttl", [0, -1, math.inf, -math.inf, math.nan])
def test_claim_rejects_non_finite_or_non_positive_ttl(ttl):
    omd = Coordinator(allow_memory_db=True)
    out = omd.claim("ag", ["a/**"], ttl=ttl)
    assert out == {"ok": False, "reason": "invalid_ttl", "ttl": ttl} or (
        out["ok"] is False and out["reason"] == "invalid_ttl" and math.isnan(ttl)
    )
    assert omd.store.held_orbits() == []


@pytest.mark.parametrize("ttl", [0, -1, math.inf, -math.inf, math.nan])
def test_renew_rejects_non_finite_or_non_positive_ttl(ttl):
    omd = Coordinator(allow_memory_db=True)
    claim = omd.claim("ag", ["a/**"], ttl=30)
    before = omd.store.get_orbit(claim["orbit_id"])["expires_at"]
    out = omd.renew(claim["orbit_id"], "ag", claim["fence"], ttl=ttl)
    assert out["ok"] is False and out["reason"] == "invalid_ttl"
    assert omd.store.get_orbit(claim["orbit_id"])["expires_at"] == before


def test_pending_promotion_preserves_requested_ttl_and_exact_window(monkeypatch):
    now = [100.0]
    monkeypatch.setattr("omd_server.core.time.time", lambda: now[0])
    monkeypatch.setattr("omd_server.store.time.time", lambda: now[0])

    omd = Coordinator(allow_memory_db=True, agent_ttl=None)
    blocker = omd.claim("blocker", ["a/**"], ttl=1000)
    pending = omd.claim("waiter", ["a/**"], ttl=7)
    requested = omd.store.get_orbit(pending["orbit_id"])
    assert requested["requested_at"] == 100.0
    assert requested["granted_at"] is None
    assert requested["requested_ttl"] == 7

    now[0] = 150.0
    omd.release(blocker["orbit_id"], "blocker", blocker["fence"])
    granted = omd.store.get_orbit(pending["orbit_id"])
    assert granted["state"] == "HELD"
    assert granted["granted_at"] == 150.0
    assert granted["expires_at"] == 157.0

    now[0] = 151.0
    omd.release(pending["orbit_id"], "waiter", granted["fence"])
    terminal = omd.store.get_orbit(pending["orbit_id"])
    assert terminal["terminal_at"] == 151.0
    assert terminal["terminal_reason"] == "explicit_release"
    assert terminal["reclaimed_at"] is None


def test_task_bound_read_is_native_and_cancelled_without_ttl_inflation(monkeypatch):
    now = [200.0]
    monkeypatch.setattr("omd_server.core.time.time", lambda: now[0])
    monkeypatch.setattr("omd_server.store.time.time", lambda: now[0])
    omd = Coordinator(allow_memory_db=True, agent_ttl=None)
    blocker = omd.claim("blocker", ["a/**"], ttl=100)
    omd.declare("T", writes=[], reads=["a/**"])
    omd.next_task("A")
    pending = omd.claim("A", ["a/**"], mode="read", task_id="T", ttl=7)
    row = omd.store.get_orbit(pending["orbit_id"])
    assert pending["attempt_id"] and row["requested_ttl"] == 7

    now[0] = 210.0
    omd.release(blocker["orbit_id"], "blocker", blocker["fence"])
    promoted = omd.store.get_orbit(pending["orbit_id"])
    assert promoted["granted_at"] == 210.0 and promoted["expires_at"] == 217.0
    out = omd.cancel("T")
    assert out["ok"] and omd.store.get_orbit(pending["orbit_id"])["state"] == "RELEASED"
    assert omd.store.get_attempt(pending["attempt_id"])["terminal_state"] == "CANCELLED"


def test_renew_after_logical_expiry_cannot_resurrect(monkeypatch):
    now = [10.0]
    monkeypatch.setattr("omd_server.core.time.time", lambda: now[0])
    monkeypatch.setattr("omd_server.store.time.time", lambda: now[0])
    omd = Coordinator(allow_memory_db=True, agent_ttl=None)
    claim = omd.claim("ag", ["a/**"], ttl=1)

    now[0] = 12.0
    out = omd.renew(claim["orbit_id"], "ag", claim["fence"], ttl=10)
    assert out["ok"] is False and out["fenced_out"] is True
    row = omd.store.get_orbit(claim["orbit_id"])
    assert row["state"] == "EXPIRED"
    assert row["terminal_at"] == 12.0
    assert row["reclaimed_at"] == 12.0
    assert row["terminal_reason"] == "lease_expired_before_renew"


@pytest.mark.parametrize("terminal", ["release", "expire"])
def test_write_lease_loss_invalidates_attempt_and_requeues_memory_task(
    terminal, monkeypatch
):
    now = [10.0]
    monkeypatch.setattr("omd_server.core.time.time", lambda: now[0])
    monkeypatch.setattr("omd_server.store.time.time", lambda: now[0])
    omd = Coordinator(allow_memory_db=True, agent_ttl=None)
    omd.declare("T", writes=["a/**", "b/**"])
    omd.next_task("A")
    first = omd.claim("A", ["a/**"], task_id="T", ttl=2)
    second = omd.claim("A", ["b/**"], task_id="T", ttl=20)
    omd.start("T", "A")

    if terminal == "release":
        out = omd.release(first["orbit_id"], "A", first["fence"])
        assert out["ok"] is True
        expected = "RELEASED"
    else:
        now[0] = 13.0
        out = omd.renew(first["orbit_id"], "A", first["fence"], ttl=20)
        assert out["ok"] is False and out["fenced_out"] is True
        expected = "EXPIRED"

    task = omd.store.get_task("T")
    attempt = omd.store.get_attempt(first["attempt_id"])
    assert task["state"] == "PENDING" and task["attempt_id"] is None
    assert attempt["terminal_state"] == expected
    assert omd.store.get_orbit(second["orbit_id"])["state"] == "RELEASED"


def test_release_cannot_tear_down_pinned_connect():
    omd = Coordinator(allow_memory_db=True, agent_ttl=None)
    omd.declare("T", writes=["a/**"])
    omd.next_task("A")
    claim = omd.claim("A", ["a/**"], task_id="T")
    omd.start("T", "A")
    omd.finish("T", "A", claim["fence"])
    omd._connect_phase_a("T", "A", claim["fence"])

    out = omd.release(claim["orbit_id"], "A", claim["fence"])

    assert out["ok"] is False and out["reason"] == "connect_in_progress"
    assert omd.store.get_attempt(claim["attempt_id"])["terminal_at"] is None


def test_task_attempt_identity_separates_requeue_and_filters_old_orbits(tmp_path):
    omd = Coordinator(str(tmp_path / "omd.db"), enforce_single_coordinator=False)
    omd.declare("T", writes=["a/**"])
    omd.next_task("A")
    first = omd.claim("A", ["a/**"], task_id="T")
    omd.start("T", "A")
    first_attempt = first["attempt_id"]
    omd.bail("A")

    omd.next_task("B")
    second = omd.claim("B", ["a/**"], task_id="T")
    omd.start("T", "B")
    second_attempt = second["attempt_id"]
    assert first_attempt != second_attempt
    assert omd.store.get_task("T")["attempt_id"] == second_attempt
    assert omd.store.get_attempt(first_attempt)["terminal_reason"] == "agent_bail"

    # The terminal A orbit is history, not part of B's current fence set.
    out = omd.finish("T", "B", second["fence"])
    assert out["state"] == "DONE"


def test_rolling_upgrade_adapter_connects_legacy_and_new_bound_orbits(tmp_path):
    """One adapter generation must retain both sides of a hybrid rollout."""

    repo = tmp_path / "repo"
    _init_repo(repo)
    omd = Coordinator(
        str(tmp_path / "omd.db"), repo=str(repo),
        worktrees_dir=str(tmp_path / "wt"), integration_branch="main",
        enforce_single_coordinator=False,
    )
    omd.declare("T", writes=["src/**"], shared=["shared/**"])
    omd.next_task("A")
    intent_key = omd._intent_key("A", ["src/**"], "write", "T")
    with omd._cs():
        legacy_fence = omd.store.next_fence()
        legacy_id = omd.store.add_orbit(
            task_id="T", agent_id="A", pathspec=["src/**"], mode="write",
            state="HELD", fence=legacy_fence, expires_at=10**12,
            intent_key=intent_key, attempt_id=None,
        )

    dedup = omd.claim("A", ["src/**"], task_id="T")
    assert dedup["dedup"] is True and dedup["orbit_id"] == legacy_id
    adapter_id = dedup["attempt_id"]
    assert omd.store.get_attempt(adapter_id)["opened_by"] == "CLAIM_LEGACY"
    assert omd.store.get_orbit(legacy_id)["attempt_id"] is None

    bound = omd.claim("A", ["shared/**"], mode="shared", task_id="T")
    assert bound["state"] == "HELD" and bound["attempt_id"] == adapter_id
    assert omd.store.get_orbit(bound["orbit_id"])["attempt_id"] == adapter_id
    task_fence = max(legacy_fence, bound["fence"])

    started = omd.start("T", "A")
    worktree = Path(started["worktree"])
    for relative in ("src/legacy.py", "shared/new.py"):
        target = worktree / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(f"# {relative}\n")
    committed = omd.commit("T", "hybrid rollout", "A", task_fence)
    assert committed["ok"] is True
    assert omd.finish("T", "A", task_fence)["state"] == "DONE"

    connected = omd.connect("T", "A", task_fence)
    assert connected["ok"] is True and connected["state"] == "MERGED"
    attempt = omd.store.get_attempt(adapter_id)
    connect_try = omd.store.connect_attempts_for_attempt(adapter_id)[0]
    assert attempt["terminal_state"] == "MERGED"
    assert connect_try["outcome"] == "MERGED"
    assert set(json.loads(connect_try["orbit_ids"])) == {
        legacy_id, bound["orbit_id"],
    }
    assert omd.store.get_orbit(legacy_id)["attempt_id"] is None
    assert omd.store.get_orbit(legacy_id)["state"] == "RELEASED"
    assert omd.store.get_orbit(bound["orbit_id"])["state"] == "RELEASED"
    assert not [o for o in omd.store.held_orbits() if o["kind"] == "merge_token"]


def test_pure_legacy_adapter_can_arrive_and_trip_barrier(tmp_path):
    omd = Coordinator(str(tmp_path / "omd.db"), enforce_single_coordinator=False)
    omd.declare("T", writes=["src/**"])
    omd.next_task("A")
    intent_key = omd._intent_key("A", ["src/**"], "write", "T")
    with omd._cs():
        fence = omd.store.next_fence()
        legacy_id = omd.store.add_orbit(
            task_id="T", agent_id="A", pathspec=["src/**"], mode="write",
            state="HELD", fence=fence, expires_at=10**12,
            intent_key=intent_key, attempt_id=None,
        )
    dedup = omd.claim("A", ["src/**"], task_id="T")
    adapter_id = dedup["attempt_id"]
    omd.start("T", "A")
    omd.finish("T", "A", fence)
    assert omd.barrier_declare("rv", ["T"])["state"] == "ARMED"

    out = omd.barrier_arrive("rv", "A", "T", fence=fence)

    assert out["ok"] is True and out["state"] == "TRIPPED"
    party = omd.store.get_barrier_party("bar-rv-0", 0, "T")
    assert party["arrive_attempt_id"] == adapter_id
    assert omd.store.get_task("T")["state"] == "MERGED"
    assert omd.store.get_orbit(legacy_id)["state"] == "RELEASED"
    connect_try = omd.store.connect_attempts_for_attempt(adapter_id)[0]
    assert connect_try["trigger_kind"] == "BARRIER"
    assert connect_try["barrier_id"] == "bar-rv-0"


def test_inflight_pre_r3_task_binds_adapter_before_barrier_arrival(tmp_path):
    omd = Coordinator(str(tmp_path / "omd.db"), enforce_single_coordinator=False)
    omd.declare("T", writes=["src/**"])
    omd.next_task("A")
    intent_key = omd._intent_key("A", ["src/**"], "write", "T")
    with omd._cs():
        fence = omd.store.next_fence()
        omd.store.add_orbit(
            task_id="T", agent_id="A", pathspec=["src/**"], mode="write",
            state="HELD", fence=fence, expires_at=10**12,
            intent_key=intent_key, attempt_id=None,
        )
        # Simulate the projection inherited from an old coordinator that had
        # already started the task but knew no task_attempt identity.
        omd.store.set_task("T", state="IN_ORBIT", agent_id="A", attempt_id=None)

    dedup = omd.claim("A", ["src/**"], task_id="T")
    adapter_id = dedup["attempt_id"]
    assert omd.store.get_task("T")["attempt_id"] == adapter_id
    started = omd.start("T", "A")
    assert started["dedup"] is True and started["attempt_id"] == adapter_id
    omd.finish("T", "A", fence)
    omd.barrier_declare("rv", ["T"])

    out = omd.barrier_arrive("rv", "A", "T", fence=fence)

    assert out["ok"] is True and out["state"] == "TRIPPED"
    assert omd.store.get_barrier_party(
        "bar-rv-0", 0, "T"
    )["arrive_attempt_id"] == adapter_id


def test_cancel_releases_prestart_legacy_adapter_orbit(tmp_path):
    omd = Coordinator(str(tmp_path / "omd.db"), enforce_single_coordinator=False)
    omd.declare("T", writes=["src/**"])
    omd.next_task("A")
    intent_key = omd._intent_key("A", ["src/**"], "write", "T")
    with omd._cs():
        fence = omd.store.next_fence()
        legacy_id = omd.store.add_orbit(
            task_id="T", agent_id="A", pathspec=["src/**"], mode="write",
            state="HELD", fence=fence, expires_at=10**12,
            intent_key=intent_key, attempt_id=None,
        )
    adapter_id = omd.claim("A", ["src/**"], task_id="T")["attempt_id"]

    out = omd.cancel("T", reason="rolling-upgrade cancel")

    assert out["ok"] is True and out["state"] == "ABORTED"
    assert omd.store.get_orbit(legacy_id)["state"] == "RELEASED"
    assert omd.store.get_attempt(adapter_id)["terminal_state"] == "CANCELLED"
    assert omd.claim("B", ["src/**"])["state"] == "HELD"


@pytest.mark.parametrize("lease_loss", ["release", "expire"])
def test_legacy_adapter_write_lease_loss_invalidates_generation(tmp_path, lease_loss):
    omd = Coordinator(str(tmp_path / "omd.db"), enforce_single_coordinator=False)
    omd.declare("T", writes=["src/**"])
    omd.next_task("A")
    intent_key = omd._intent_key("A", ["src/**"], "write", "T")
    with omd._cs():
        fence = omd.store.next_fence()
        legacy_id = omd.store.add_orbit(
            task_id="T", agent_id="A", pathspec=["src/**"], mode="write",
            state="HELD", fence=fence, expires_at=10**12,
            intent_key=intent_key, attempt_id=None,
        )
    adapter_id = omd.claim("A", ["src/**"], task_id="T")["attempt_id"]
    omd.start("T", "A")

    if lease_loss == "release":
        assert omd.release(legacy_id, "A", fence)["ok"] is True
        expected_orbit, expected_attempt = "RELEASED", "RELEASED"
    else:
        with omd._cs():
            omd.store.set_orbit(legacy_id, expires_at=0)
        assert legacy_id in omd.sweep()["expired"]
        expected_orbit, expected_attempt = "EXPIRED", "EXPIRED"

    assert omd.store.get_orbit(legacy_id)["state"] == expected_orbit
    assert omd.store.get_attempt(adapter_id)["terminal_state"] == expected_attempt
    task = omd.store.get_task("T")
    assert task["state"] == "PENDING" and task["attempt_id"] is None
    finished = omd.finish("T", "A", fence)
    assert finished["ok"] is False and finished["fenced_out"] is True
    assert omd.claim("B", ["src/**"])["state"] == "HELD"


def test_task_cannot_start_under_another_agents_pending_attempt():
    omd = Coordinator(allow_memory_db=True, agent_ttl=None)
    omd.declare("block", writes=["a/**"])
    blocker = omd.claim("blocker", ["a/**"], task_id="block")
    omd.declare("T", writes=["a/**"])
    omd.next_task("A")
    pending = omd.claim("A", ["a/**"], task_id="T", ttl=5)
    assert pending["state"] == "PENDING"

    out = omd.start("T", "B")
    assert out["ok"] is False and out["reason"] == "attempt_owner_mismatch"
    omd.release(blocker["orbit_id"], "blocker", blocker["fence"])
    # B's rejected start cannot steal A's still-valid demand attempt.
    assert omd.store.get_orbit(pending["orbit_id"])["state"] == "HELD"


def test_active_redeclare_is_fail_closed():
    omd = Coordinator(allow_memory_db=True)
    omd.declare("T", writes=["a/**"])
    omd.next_task("A")
    claim = omd.claim("A", ["a/**"], task_id="T")
    omd.start("T", "A")

    out = omd.declare("T", writes=["b/**"])
    assert out["ok"] is False and out["reason"] == "task_active"
    assert omd.store.get_task("T")["writes"] == '["a/**"]'
    omd.release(claim["orbit_id"], "A", claim["fence"])


def test_idempotency_key_reuse_with_different_args_is_rejected():
    omd = Coordinator(allow_memory_db=True)
    first = omd.claim("ag", ["a/**"], request_id="same")
    assert first["state"] == "HELD"
    second = omd.claim("ag", ["b/**"], request_id="same")
    assert second["ok"] is False
    assert second["reason"] == "idempotency_key_reused"
    assert len(omd.store.held_orbits()) == 1


def test_connect_idempotency_key_cannot_replay_another_task():
    omd = Coordinator(allow_memory_db=True, agent_ttl=None)
    for task, agent, area in (("T", "A", "a"), ("U", "B", "b")):
        omd.declare(task, writes=[f"{area}/**"])
        omd.next_task(agent)
        claim = omd.claim(agent, [f"{area}/**"], task_id=task)
        omd.start(task, agent)
        omd.finish(task, agent, claim["fence"])
        if task == "T":
            first = omd.connect(task, agent, claim["fence"], request_id="same-connect")
            assert first["ok"] is True
        else:
            second = omd.connect(task, agent, claim["fence"], request_id="same-connect")
            assert second["ok"] is False
            assert second["reason"] == "idempotency_key_reused"


def test_connect_attempt_records_tip_base_order_and_outcome(tmp_path):
    repo = tmp_path / "repo"
    _init_repo(repo)
    omd = Coordinator(
        str(tmp_path / "omd.db"),
        repo=str(repo),
        worktrees_dir=str(tmp_path / "wt"),
        integration_branch="main",
        repo_id="repo:test",
        enforce_single_coordinator=False,
    )
    claim, started = _develop(omd)
    attempt = omd.store.get_attempt(claim["attempt_id"])
    assert attempt["repo_id"] == "repo:test"
    assert attempt["worktree_base_sha"] == _git(["rev-parse", "dev"], repo)
    assert attempt["branch"] == started["branch"]

    result = omd.connect("T", "ag", claim["fence"])
    assert result["ok"] is True
    rows = omd.store.connect_attempts_for_attempt(claim["attempt_id"])
    assert len(rows) == 1
    row = rows[0]
    assert row["connect_attempt_id"] == result["connect_attempt_id"]
    assert row["branch_tip_sha"]
    assert row["integration_base_sha"]
    assert row["outcome"] == "MERGED"
    assert row["merge_sha"] == result["merge_sha"]
    assert row["candidate_commit_sha"] == result["merge_sha"]
    assert row["candidate_tree_sha"] == omd.git.commit_tree(
        omd.integration_worktree, result["merge_sha"]
    )
    assert row["started_at"] <= row["candidate_prepared_at"] <= row["terminal_at"]
    assert row["merge_gen"] == result["gen"] == 1
    assert row["resolution_source"] == "LIVE"
    assert omd.store.get_attempt(claim["attempt_id"])["terminal_state"] == "MERGED"


def test_already_integrated_connect_has_explicit_noop_outcome_and_no_candidate(tmp_path):
    repo = tmp_path / "repo"
    _init_repo(repo)
    omd = Coordinator(
        str(tmp_path / "omd.db"), repo=str(repo),
        worktrees_dir=str(tmp_path / "wt"), integration_branch="main",
        enforce_single_coordinator=False,
    )
    claim, started = _develop(omd)
    task_tip = _git(["rev-parse", "HEAD"], Path(started["worktree"]))
    base = _git(["rev-parse", "refs/heads/main"], repo)
    _git(["update-ref", "refs/heads/main", task_tip, base], repo)

    result = omd.connect("T", "ag", claim["fence"])

    assert result["ok"] is True and result["merge_sha"] == task_tip
    row = omd.store.get_connect_attempt(result["connect_attempt_id"])
    assert row["outcome"] == "MERGED"
    assert row["outcome_code"] == "ALREADY_INTEGRATED"
    assert row["merge_sha"] == row["integration_base_sha"] == task_tip
    assert row["candidate_tree_sha"] is None
    assert row["candidate_commit_sha"] is None
    assert row["candidate_prepared_at"] is None


def test_start_uses_captured_oid_if_raw_head_moves_before_worktree_add(
    tmp_path, monkeypatch,
):
    repo = tmp_path / "repo"
    _init_repo(repo)
    omd = Coordinator(
        str(tmp_path / "omd.db"), repo=str(repo),
        worktrees_dir=str(tmp_path / "wt"), integration_branch="main",
        enforce_single_coordinator=False,
    )
    omd.declare("T", writes=["src/**"])
    omd.next_task("A")
    claim = omd.claim("A", ["src/**"], task_id="T")
    original_revision_tip = omd.git.revision_tip
    captured = []

    def capture_then_move_head(revision="HEAD", cwd=None):
        oid = original_revision_tip(revision, cwd=cwd)
        if revision == "HEAD" and not captured:
            captured.append(oid)
            (repo / "raw-writer.txt").write_text("moved\n")
            _git(["add", "raw-writer.txt"], repo)
            _git(["commit", "-m", "raw writer moved HEAD"], repo)
        return oid

    monkeypatch.setattr(omd.git, "revision_tip", capture_then_move_head)
    started = omd.start("T", "A")
    attempt = omd.store.get_attempt(claim["attempt_id"])

    assert _git(["rev-parse", "HEAD"], repo) != captured[0]
    assert _git(["rev-parse", "HEAD"], Path(started["worktree"])) == captured[0]
    assert attempt["worktree_base_sha"] == captured[0]


def test_connect_rejects_uncommitted_worktree_without_losing_file(tmp_path):
    repo = tmp_path / "repo"
    _init_repo(repo)
    omd = Coordinator(
        str(tmp_path / "omd.db"), repo=str(repo), worktrees_dir=str(tmp_path / "wt"),
        integration_branch="main", enforce_single_coordinator=False,
    )
    omd.declare("T", writes=["src/**"])
    omd.next_task("A")
    claim = omd.claim("A", ["src/**"], task_id="T")
    started = omd.start("T", "A")
    dirty = Path(started["worktree"]) / "src" / "lost.py"
    dirty.parent.mkdir(parents=True)
    dirty.write_text("lost = False\n")
    omd.finish("T", "A", claim["fence"])

    out = omd.connect("T", "A", claim["fence"])

    assert out["ok"] is False and out["reason"] == "uncommitted_worktree_changes"
    assert dirty.exists()
    assert omd.store.get_task("T")["state"] == "DONE"


def test_phase_c_rejects_late_tip_and_claim_after_snapshot(tmp_path):
    repo = tmp_path / "repo"
    _init_repo(repo)
    omd = Coordinator(
        str(tmp_path / "omd.db"), repo=str(repo), worktrees_dir=str(tmp_path / "wt"),
        integration_branch="main", enforce_single_coordinator=False,
    )
    claim, started = _develop(omd)
    phase_a = omd._connect_phase_a("T", "ag", claim["fence"])
    late_claim = omd.claim("ag", ["other/**"], task_id="T")
    assert late_claim["ok"] is False and late_claim["reason"] == "connect_in_progress"

    late = Path(started["worktree"]) / "src" / "late.py"
    late.write_text("late = True\n")
    _git(["add", "-A"], Path(started["worktree"]))
    _git(["commit", "-m", "late commit"], Path(started["worktree"]))
    late_tip = _git(["rev-parse", "HEAD"], Path(started["worktree"]))
    merge_sha, err = omd._connect_phase_b(phase_a["intent"])
    assert merge_sha and err is None

    out = omd._connect_phase_c(
        "T", phase_a["token_id"], phase_a["intent"], merge_sha, None
    )

    assert out["ok"] is False and out["reason"] == "stale_connect_after_merge"
    ancestry = subprocess.run(
        ["git", "merge-base", "--is-ancestor", late_tip, merge_sha], cwd=repo
    )
    assert ancestry.returncode == 1


def test_branch_ref_lock_blocks_raw_git_commit(tmp_path):
    repo = tmp_path / "repo"
    _init_repo(repo)
    git = GitRepo(str(repo))
    before = _git(["rev-parse", "dev"], repo)

    with git.branch_ref_lock("dev"):
        (repo / "late.txt").write_text("late\n")
        _git(["add", "late.txt"], repo)
        commit = subprocess.run(
            ["git", "commit", "-m", "late"], cwd=repo,
            capture_output=True, text=True,
        )

    assert commit.returncode != 0
    assert _git(["rev-parse", "dev"], repo) == before
    assert (repo / "late.txt").exists()


def test_branch_ref_lock_reaps_only_crashed_omd_owner(tmp_path):
    repo = tmp_path / "repo"
    _init_repo(repo)
    source_root = Path(__file__).resolve().parents[1]
    child = (
        "import os\n"
        "from omd_server.gitio import GitRepo\n"
        f"lock = GitRepo({str(repo)!r}).branch_ref_lock('dev')\n"
        "lock.__enter__()\n"
        "os._exit(0)\n"
    )
    env = dict(os.environ)
    env["PYTHONPATH"] = str(source_root)
    subprocess.run([sys.executable, "-c", child], env=env, check=True)

    git = GitRepo(str(repo))
    with git.branch_ref_lock("dev", timeout=0.5):
        pass

    ordinary = Path(_git(["rev-parse", "--git-path", "refs/heads/dev"], repo))
    if not ordinary.is_absolute():
        ordinary = repo / ordinary
    ordinary = Path(str(ordinary.resolve()) + ".lock")
    ordinary.write_text("ordinary git lock\n")
    with pytest.raises(GitError, match="timed out sealing"):
        with git.branch_ref_lock("dev", timeout=0.02):
            pass
    assert ordinary.read_text() == "ordinary git lock\n"


def test_phase_a_seals_writeset_audit_and_captured_tip(tmp_path, monkeypatch):
    repo = tmp_path / "repo"
    _init_repo(repo)
    omd = Coordinator(
        str(tmp_path / "omd.db"), repo=str(repo), worktrees_dir=str(tmp_path / "wt"),
        integration_branch="main", enforce_single_coordinator=False,
    )
    claim, started = _develop(omd)
    original_audit = omd._writeset_audit
    raw_commit = {}

    def inject_raw_commit(*args, **kwargs):
        result = original_audit(*args, **kwargs)
        worktree = Path(started["worktree"])
        (worktree / "forbidden.txt").write_text("outside declaration\n")
        _git(["add", "forbidden.txt"], worktree)
        raw_commit["result"] = subprocess.run(
            ["git", "commit", "-m", "raw race"], cwd=worktree,
            capture_output=True, text=True,
        )
        return result

    monkeypatch.setattr(omd, "_writeset_audit", inject_raw_commit)
    out = omd.connect("T", "ag", claim["fence"])

    assert raw_commit["result"].returncode != 0
    assert out["ok"] is False and out["reason"] == "stale_connect_after_merge"
    assert not (Path(omd._ensure_integration_wt()) / "forbidden.txt").exists()
    assert (Path(started["worktree"]) / "forbidden.txt").exists()


def test_phase_a_rejects_missing_git_identity_before_token_or_attempt(tmp_path, monkeypatch):
    repo = tmp_path / "repo"
    _init_repo(repo)
    omd = Coordinator(
        str(tmp_path / "omd.db"), repo=str(repo), worktrees_dir=str(tmp_path / "wt"),
        integration_branch="main", enforce_single_coordinator=False,
    )
    claim, started = _develop(omd)
    original_branch_tip = omd.git.branch_tip

    def missing_task_tip(branch):
        return None if branch == started["branch"] else original_branch_tip(branch)

    monkeypatch.setattr(omd.git, "branch_tip", missing_task_tip)
    out = omd.connect("T", "ag", claim["fence"])

    assert out["ok"] is False and out["reason"] == "git_identity_unavailable"
    assert omd.store.get_task("T")["state"] == "DONE"
    assert omd.store.connect_attempts_for_attempt(claim["attempt_id"]) == []
    assert omd.store.all_held_merge_tokens() == []


def test_barrier_phase_a_rejects_missing_git_identity_before_token(tmp_path, monkeypatch):
    repo = tmp_path / "repo"
    _init_repo(repo)
    omd = Coordinator(
        str(tmp_path / "omd.db"), repo=str(repo), worktrees_dir=str(tmp_path / "wt"),
        integration_branch="main", enforce_single_coordinator=False,
    )
    claim, started = _develop(omd)
    original_branch_tip = omd.git.branch_tip

    def missing_integration_tip(branch):
        return None if branch == "main" else original_branch_tip(branch)

    monkeypatch.setattr(omd.git, "branch_tip", missing_integration_tip)
    out = omd._barrier_connect_phase_a(
        "T", claim["fence"], claim["attempt_id"], "ag", "bar-x", 0
    )

    assert out["ok"] is False and out["reason"] == "git_identity_unavailable"
    assert omd.store.get_task("T")["state"] == "DONE"
    assert omd.store.connect_attempts_for_attempt(claim["attempt_id"]) == []
    assert omd.store.all_held_merge_tokens() == []


def test_phase_b_never_falls_back_from_missing_pinned_tip(tmp_path):
    repo = tmp_path / "repo"
    _init_repo(repo)
    omd = Coordinator(
        str(tmp_path / "omd.db"), repo=str(repo), worktrees_dir=str(tmp_path / "wt"),
        integration_branch="main", enforce_single_coordinator=False,
    )
    claim, _ = _develop(omd)
    phase_a = omd._connect_phase_a("T", "ag", claim["fence"])
    before = omd.git.branch_tip("main")
    malformed = dict(phase_a["intent"])
    malformed["branch_tip_sha"] = None

    merge_sha, err = omd._connect_phase_b(malformed)

    assert merge_sha is None and isinstance(err, GitIntegrationPreconditionError)
    assert omd.git.branch_tip("main") == before
    out = omd._connect_phase_c(
        "T", phase_a["token_id"], phase_a["intent"], None, err
    )
    assert out["ok"] is False and out["reason"] == "integration_precondition_failed"
    assert out["retryable"] is True and out["state"] == "DONE"
    assert omd.store.all_held_merge_tokens() == []


def test_connect_sweeps_stale_owner_before_task_ref_seal(tmp_path):
    repo = tmp_path / "repo"
    _init_repo(repo)
    omd = Coordinator(
        str(tmp_path / "omd.db"), repo=str(repo), worktrees_dir=str(tmp_path / "wt"),
        integration_branch="main", enforce_single_coordinator=False,
    )
    claim, started = _develop(omd, agent="old")
    with omd.store.tx():
        omd.store.db.execute(
            "UPDATE agents SET last_heartbeat=0 WHERE agent_id='old'"
        )

    out = omd.connect("T", "old", claim["fence"])

    assert out["ok"] is False and out["fenced_out"] is True
    assert omd.store.get_task("T")["state"] == "PENDING"
    assert not Path(started["worktree"]).exists()
    assert not omd.git.branch_exists("omd/T")
    assert omd.next_task("new")["task_id"] == "T"
    replacement = omd.claim("new", ["src/**"], task_id="T")
    restarted = omd.start("T", "new")
    assert replacement["state"] == "HELD"
    assert restarted["state"] == "IN_ORBIT"


def test_phase_c_rejects_integration_ref_reset_after_merge(tmp_path):
    repo = tmp_path / "repo"
    _init_repo(repo)
    omd = Coordinator(
        str(tmp_path / "omd.db"), repo=str(repo), worktrees_dir=str(tmp_path / "wt"),
        integration_branch="main", enforce_single_coordinator=False,
    )
    claim, _ = _develop(omd)
    phase_a = omd._connect_phase_a("T", "ag", claim["fence"])
    merge_sha, err = omd._connect_phase_b(phase_a["intent"])
    assert merge_sha and err is None
    integration_wt = Path(omd._ensure_integration_wt())
    _git(
        ["reset", "--hard", phase_a["intent"]["integration_base_sha"]],
        integration_wt,
    )

    out = omd._connect_phase_c(
        "T", phase_a["token_id"], phase_a["intent"], merge_sha, None
    )

    assert out["ok"] is False and out["reason"] == "stale_connect_after_merge"
    assert omd.store.get_task("T")["state"] == "CONNECTING"
    row = omd.store.get_connect_attempt(phase_a["connect_attempt_id"])
    assert row["outcome"] == "INDETERMINATE"
    assert row["outcome_code"] == "STALE_AFTER_GIT_MERGE"


def test_late_phase_c_cannot_merge_reclaimed_attempt(tmp_path):
    omd = Coordinator(str(tmp_path / "omd.db"), enforce_single_coordinator=False)
    omd.declare("T", writes=["a/**"])
    omd.next_task("A")
    claim = omd.claim("A", ["a/**"], task_id="T")
    omd.start("T", "A")
    omd.finish("T", "A", claim["fence"])
    phase_a = omd._connect_phase_a("T", "A", claim["fence"])

    # A live Phase B is pinned: voluntary bail must not tear down its identity.
    bail = omd.bail("A")
    assert bail["ok"] is False and bail["reason"] == "connect_in_progress"

    # Simulate an out-of-band stale token/attempt replacement and deliver old C.
    with omd.store.tx():
        omd.store.set_orbit(
            phase_a["token_id"], state="EXPIRED", terminal_reason="test_reclaim",
            reclaimed=True,
        )
        omd.store.set_task("T", state="PENDING", attempt_id=None, connect_attempt_id=None)
    result = omd._connect_phase_c(
        "T", phase_a["token_id"], phase_a["intent"], "deadbeef", None
    )
    assert result["ok"] is False and result["reason"] == "stale_connect_after_merge"
    assert result["retryable"] is False
    task = omd.store.get_task("T")
    assert task["state"] == "PENDING"
    assert task["merge_sha"] is None


def test_late_failed_phase_c_cannot_clobber_new_attempt(tmp_path):
    omd = Coordinator(str(tmp_path / "omd.db"), enforce_single_coordinator=False)
    omd.declare("T", writes=["a/**"])
    omd.next_task("A")
    old = omd.claim("A", ["a/**"], task_id="T")
    omd.start("T", "A")
    omd.finish("T", "A", old["fence"])
    phase_a = omd._connect_phase_a("T", "A", old["fence"])

    with omd.store.tx():
        omd.store.set_orbit(
            phase_a["token_id"], state="EXPIRED", terminal_reason="test_reclaim",
            reclaimed=True,
        )
        omd.store.set_orbit(
            old["orbit_id"], state="EXPIRED", terminal_reason="test_reclaim",
            reclaimed=True,
        )
        omd.store.close_attempt(old["attempt_id"], "RECLAIMED", "test_reclaim")
        omd.store.set_task("T", state="PENDING", agent_id=None, attempt_id=None,
                           connect_attempt_id=None)
    omd.next_task("B")
    new = omd.claim("B", ["a/**"], task_id="T")
    omd.start("T", "B")

    result = omd._connect_phase_c(
        "T", phase_a["token_id"], phase_a["intent"], None, RuntimeError("old")
    )
    assert result["ok"] is False and result["reason"] == "stale_connect_attempt"
    task = omd.store.get_task("T")
    assert task["state"] == "IN_ORBIT"
    assert task["attempt_id"] == new["attempt_id"]


def test_cancel_closes_prestart_attempt_and_held_orbit():
    omd = Coordinator(allow_memory_db=True, agent_ttl=None)
    omd.declare("T", writes=["a/**"])
    omd.next_task("A")
    claim = omd.claim("A", ["a/**"], task_id="T")

    out = omd.cancel("T", reason="operator_cancel")

    assert out["ok"] is True and out["state"] == "ABORTED"
    orbit = omd.store.get_orbit(claim["orbit_id"])
    assert orbit["state"] == "RELEASED"
    assert orbit["terminal_reason"] == "task_cancelled"
    attempt = omd.store.get_attempt(claim["attempt_id"])
    assert attempt["terminal_state"] == "CANCELLED"
    task = omd.store.get_task("T")
    assert task["attempt_id"] is None and task["connect_attempt_id"] is None


def test_second_agent_cannot_open_competing_attempt_for_same_task():
    omd = Coordinator(allow_memory_db=True, agent_ttl=None)
    omd.declare("T", writes=["a/**", "b/**"])
    first = omd.claim("A", ["a/**"], task_id="T")

    second = omd.claim("B", ["b/**"], task_id="T")

    assert second["ok"] is False and second["reason"] == "attempt_owner_mismatch"
    assert omd.store.active_attempt("T")["attempt_id"] == first["attempt_id"]
    assert omd.store.orbits_owned_by_agent("B") == []


def test_wrong_agent_connect_cannot_synthesize_legacy_attempt_before_start():
    omd = Coordinator(allow_memory_db=True, agent_ttl=None)
    omd.declare("T", writes=["a/**"])
    omd.next_task("A")
    claim = omd.claim("A", ["a/**"], task_id="T")
    assert omd.store.get_task("T")["attempt_id"] is None

    out = omd.connect("T", "B", claim["fence"])

    assert out["ok"] is False and out["fenced_out"] is True
    assert out["reason"] == "attempt_owner_mismatch"
    assert out["owner"] == "A" and out["attempt_id"] == claim["attempt_id"]
    assert [a["attempt_id"] for a in omd.store.attempts_for_task("T")] == [
        claim["attempt_id"]
    ]
    assert omd.store.get_task("T")["attempt_id"] is None
    assert omd.store.all_held_merge_tokens() == []


def test_barrier_legacy_synthesis_rejects_wrong_active_attempt_owner():
    omd = Coordinator(allow_memory_db=True, agent_ttl=None)
    omd.declare("T", writes=["a/**"])
    omd.next_task("A")
    claim = omd.claim("A", ["a/**"], task_id="T")
    # Model a stale pre-R3 runtime projection.  Immutable provenance still says
    # A owns the only live generation, so B cannot create a barrier adapter.
    with omd._cs():
        omd.store.set_task("T", state="DONE", agent_id="B", attempt_id=None)

    out = omd._barrier_connect_phase_a(
        "T", claim["fence"], expected_agent_id="B"
    )

    assert out["ok"] is False and out["fenced_out"] is True
    assert out["reason"] == "attempt_owner_mismatch"
    assert out["owner"] == "A" and out["attempt_id"] == claim["attempt_id"]
    assert [a["attempt_id"] for a in omd.store.attempts_for_task("T")] == [
        claim["attempt_id"]
    ]
    assert omd.store.get_task("T")["attempt_id"] is None
    assert omd.store.all_held_merge_tokens() == []


def test_database_rejects_fence_change_after_phase_a(tmp_path):
    omd = Coordinator(str(tmp_path / "omd.db"), enforce_single_coordinator=False)
    omd.declare("T", writes=["a/**"])
    omd.next_task("A")
    claim = omd.claim("A", ["a/**"], task_id="T")
    omd.start("T", "A")
    omd.finish("T", "A", claim["fence"])
    phase_a = omd._connect_phase_a("T", "A", claim["fence"])
    with pytest.raises(sqlite3.IntegrityError, match="grant fence"):
        with omd.store.tx():
            omd.store.set_orbit(claim["orbit_id"], fence=claim["fence"] + 100)
    assert omd.store.get_task("T")["state"] == "CONNECTING"


def test_phase_c_uses_durable_orbit_set_not_caller_intent(tmp_path):
    omd = Coordinator(str(tmp_path / "omd.db"), enforce_single_coordinator=False)
    omd.declare("T", writes=["a/**"])
    omd.next_task("A")
    claim = omd.claim("A", ["a/**"], task_id="T")
    omd.start("T", "A")
    omd.finish("T", "A", claim["fence"])
    phase_a = omd._connect_phase_a("T", "A", claim["fence"])
    forged = dict(phase_a["intent"], writes=[])

    out = omd._connect_phase_c(
        "T", phase_a["token_id"], forged, None, RuntimeError("abort")
    )

    assert out["ok"] is False and out["reason"] == "stale_connect_attempt"


def test_barrier_arrive_rejects_non_owner():
    omd = Coordinator(allow_memory_db=True, agent_ttl=None)
    omd.declare("T", writes=["a/**"])
    omd.next_task("A")
    claim = omd.claim("A", ["a/**"], task_id="T")
    omd.start("T", "A")
    omd.finish("T", "A", claim["fence"])
    omd.barrier_declare("one", ["T"])

    out = omd.barrier_arrive("one", "evil", "T", fence=claim["fence"])

    assert out["ok"] is False and out["fenced_out"] is True
    assert omd.store.get_task("T")["state"] == "DONE"


def test_barrier_arrival_provenance_is_single_assignment():
    omd = Coordinator(allow_memory_db=True, agent_ttl=None)
    omd.declare("T", writes=["src/**"])
    omd.next_task("ag")
    claim = omd.claim("ag", ["src/**"], task_id="T")
    omd.start("T", "ag")
    omd.finish("T", "ag", claim["fence"])
    omd.declare("U", writes=["other/**"])
    omd.next_task("bg")
    other = omd.claim("bg", ["other/**"], task_id="U")
    omd.start("U", "bg")
    omd.finish("U", "bg", other["fence"])
    omd.barrier_declare("pair", ["T", "U"])
    barrier = omd.store.barrier_by_name("pair")

    # The public arrival records the immutable (owner, attempt, fence) tuple.
    assert omd.barrier_arrive(
        "pair", "ag", "T", fence=claim["fence"]
    )["ok"] is True
    party = omd.store.get_barrier_party(
        barrier["barrier_id"], barrier["generation"], "T"
    )
    omd.store.set_barrier_party(
        barrier["barrier_id"], barrier["generation"], "T",
        arrived=1, arrive_fence=party["arrive_fence"],
        arrive_attempt_id=party["arrive_attempt_id"], agent_id=party["agent_id"],
    )

    with pytest.raises(sqlite3.IntegrityError, match="single-assignment"):
        omd.store.set_barrier_party(
            barrier["barrier_id"], barrier["generation"], "T",
            arrived=1, arrive_fence=party["arrive_fence"] + 1,
            arrive_attempt_id=party["arrive_attempt_id"], agent_id=party["agent_id"],
        )
    with pytest.raises(sqlite3.IntegrityError, match="replacement"):
        omd.store.db.execute(
            "INSERT OR REPLACE INTO barrier_parties("
            "barrier_id,generation,task_id,agent_id,arrived,arrive_fence,"
            "arrive_attempt_id) VALUES(?,?,?,?,?,?,?)",
            (barrier["barrier_id"], barrier["generation"], "T",
             party["agent_id"], 1, party["arrive_fence"],
             party["arrive_attempt_id"]),
        )
    with pytest.raises(sqlite3.IntegrityError, match="0 or 1"):
        omd.store.db.execute(
            "UPDATE barrier_parties SET arrived=2 WHERE barrier_id=? "
            "AND generation=? AND task_id='U'",
            (barrier["barrier_id"], barrier["generation"]),
        )


def test_barrier_rejects_member_merged_by_direct_connect():
    omd = Coordinator(allow_memory_db=True, agent_ttl=None)
    claims = {}
    for task, agent, area in (("A", "agA", "a"), ("B", "agB", "b")):
        omd.declare(task, writes=[f"{area}/**"])
        omd.next_task(agent)
        claims[task] = omd.claim(agent, [f"{area}/**"], task_id=task)
        omd.start(task, agent)
        omd.finish(task, agent, claims[task]["fence"])
    omd.barrier_declare("pair", ["A", "B"])
    assert omd.barrier_arrive(
        "pair", "agA", "A", fence=claims["A"]["fence"]
    )["state"] == "ARMED"
    assert omd.connect("A", "agA", claims["A"]["fence"])["ok"] is True

    out = omd.barrier_arrive(
        "pair", "agB", "B", fence=claims["B"]["fence"]
    )

    assert out["ok"] is False and out["state"] == "BROKEN"
    assert "provenance mismatch" in out["reason"]
    assert omd.store.get_task("B")["state"] == "DONE"


def test_barrier_recovery_requires_barrier_bound_merge_provenance():
    omd = Coordinator(allow_memory_db=True, agent_ttl=None)
    claims = {}
    for task, agent, area in (("A", "agA", "a"), ("B", "agB", "b")):
        omd.declare(task, writes=[f"{area}/**"])
        omd.next_task(agent)
        claims[task] = omd.claim(agent, [f"{area}/**"], task_id=task)
        omd.start(task, agent)
        omd.finish(task, agent, claims[task]["fence"])
    omd.barrier_declare("pair", ["A", "B"])
    barrier = omd.store.barrier_by_name("pair")
    for task, agent in (("A", "agA"), ("B", "agB")):
        identity = omd._party_write_identity(task)
        omd.store.set_barrier_party(
            barrier["barrier_id"], barrier["generation"], task,
            arrived=1, arrive_fence=identity["fence"],
            arrive_attempt_id=identity["attempt_id"], agent_id=agent,
        )
        assert omd.connect(task, agent, claims[task]["fence"])["ok"] is True
    omd.store.set_barrier(barrier["barrier_id"], state="TRIPPING")

    omd._barrier_recover()

    assert omd.store.get_barrier(barrier["barrier_id"])["state"] == "BROKEN"


def test_recovery_rolls_back_and_closes_connect_try(tmp_path):
    repo = tmp_path / "repo"
    db = tmp_path / "omd.db"
    _init_repo(repo)
    omd = Coordinator(
        str(db), repo=str(repo), worktrees_dir=str(tmp_path / "wt"),
        integration_branch="main", enforce_single_coordinator=False,
    )
    claim, _ = _develop(omd)
    phase_a = omd._connect_phase_a("T", "ag", claim["fence"])
    connect_id = phase_a["connect_attempt_id"]
    with omd.store.tx():
        omd.store.set_task("T", integration_base_sha=None)
    omd.store.db.close()

    recovered = Coordinator(
        str(db), repo=str(repo), worktrees_dir=str(tmp_path / "wt"),
        integration_branch="main", enforce_single_coordinator=False,
    )

    assert recovered.store.get_task("T")["state"] == "DONE"
    assert recovered.store.get_task("T")["connect_attempt_id"] is None
    row = recovered.store.get_connect_attempt(connect_id)
    assert row["outcome"] == "ROLLED_BACK"
    assert row["outcome_code"] == "RECOVERED_ROLLBACK"
    assert row["resolution_source"] == "RECOVERY"
    assert recovered.store.get_attempt(claim["attempt_id"])["terminal_at"] is None


def test_candidate_attestor_failure_never_moves_integration_ref(
    tmp_path, monkeypatch,
):
    repo = tmp_path / "repo"
    _init_repo(repo)
    omd = Coordinator(
        str(tmp_path / "omd.db"), repo=str(repo),
        worktrees_dir=str(tmp_path / "wt"), integration_branch="main",
        enforce_single_coordinator=False,
    )
    claim, _ = _develop(omd)
    phase_a = omd._connect_phase_a("T", "ag", claim["fence"])
    base = phase_a["intent"]["integration_base_sha"]

    def reject_attestation(*_args, **_kwargs):
        raise GitIntegrationPreconditionError("injected attestor rejection")

    monkeypatch.setattr(omd, "_attest_connect_candidate", reject_attestation)
    merge_sha, err = omd._connect_phase_b(phase_a["intent"])

    assert merge_sha is None and isinstance(err, GitIntegrationPreconditionError)
    assert omd.git.branch_tip("main") == base
    row = omd.store.get_connect_attempt(phase_a["connect_attempt_id"])
    assert row["candidate_tree_sha"] is None
    assert row["candidate_commit_sha"] is None
    assert row["candidate_prepared_at"] is None
    assert not omd.git.has_merge_in_progress(omd.integration_worktree)


def test_recovery_rolls_back_candidate_sealed_before_failed_ref_cas(
    tmp_path, monkeypatch,
):
    repo = tmp_path / "repo"
    db = tmp_path / "omd.db"
    _init_repo(repo)
    first = Coordinator(
        str(db), repo=str(repo), worktrees_dir=str(tmp_path / "wt"),
        integration_branch="main", enforce_single_coordinator=False,
    )
    claim, _ = _develop(first)
    phase_a = first._connect_phase_a("T", "ag", claim["fence"])
    connect_id = phase_a["connect_attempt_id"]
    integration_base = phase_a["intent"]["integration_base_sha"]
    original_git = first.git._git
    failed = False

    def fail_publication(*args, **kwargs):
        nonlocal failed
        if (
            not failed
            and len(args) >= 2
            and args[0] == "update-ref"
            and args[1] == "refs/heads/main"
        ):
            failed = True
            raise GitError("injected publication CAS failure")
        return original_git(*args, **kwargs)

    monkeypatch.setattr(first.git, "_git", fail_publication)
    merge_sha, err = first._connect_phase_b(phase_a["intent"])
    assert merge_sha is None and isinstance(err, GitIntegrationPreconditionError)
    sealed = first.store.get_connect_attempt(connect_id)
    assert sealed["candidate_tree_sha"]
    assert sealed["candidate_commit_sha"]
    assert sealed["candidate_prepared_at"] is not None
    assert first.git.branch_tip("main") == integration_base
    first.store.db.close()

    recovered = Coordinator(
        str(db), repo=str(repo), worktrees_dir=str(tmp_path / "wt"),
        integration_branch="main", enforce_single_coordinator=False,
    )

    task = recovered.store.get_task("T")
    row = recovered.store.get_connect_attempt(connect_id)
    assert task["state"] == "DONE" and task["connect_attempt_id"] is None
    assert row["outcome"] == "ROLLED_BACK"
    assert row["outcome_code"] == "RECOVERED_ROLLBACK"
    assert row["candidate_commit_sha"] == sealed["candidate_commit_sha"]
    assert recovered.git.branch_tip("main") == integration_base


def test_recovery_does_not_rollback_when_integration_worktree_is_unavailable(
    tmp_path, monkeypatch,
):
    repo = tmp_path / "repo"
    db = tmp_path / "omd.db"
    _init_repo(repo)
    first = Coordinator(
        str(db), repo=str(repo), worktrees_dir=str(tmp_path / "wt"),
        integration_branch="main", enforce_single_coordinator=False,
    )
    claim, _ = _develop(first)
    phase_a = first._connect_phase_a("T", "ag", claim["fence"])
    connect_id = phase_a["connect_attempt_id"]

    def integration_worktree_unavailable(*_args, **_kwargs):
        raise GitError("injected integration worktree failure")

    monkeypatch.setattr(
        GitRepo, "ensure_integration_worktree", integration_worktree_unavailable
    )
    with pytest.raises(
        RuntimeError, match="cannot prepare integration worktree for recovery"
    ):
        Coordinator(
            str(db), repo=str(repo), worktrees_dir=str(tmp_path / "wt"),
            integration_branch="main", enforce_single_coordinator=False,
        )

    task = first.store.get_task("T")
    connect = first.store.get_connect_attempt(connect_id)
    assert task["state"] == "CONNECTING"
    assert task["connect_attempt_id"] == connect_id
    assert connect["terminal_at"] is None and connect["outcome"] is None
    assert first.store.get_attempt(claim["attempt_id"])["terminal_at"] is None
    assert first.store.all_held_merge_tokens()


def test_token_only_recovery_preserves_external_integration_ref_winner(tmp_path):
    repo = tmp_path / "repo"
    db = tmp_path / "omd.db"
    _init_repo(repo)
    first = Coordinator(
        str(db), repo=str(repo), worktrees_dir=str(tmp_path / "wt"),
        integration_branch="main", enforce_single_coordinator=False,
    )
    integration_wt = Path(first._ensure_integration_wt())
    base = _git(["rev-parse", "refs/heads/main"], repo)

    (repo / "candidate.txt").write_text("candidate\n")
    _git(["add", "candidate.txt"], repo)
    _git(["commit", "-m", "candidate"], repo)
    candidate = _git(["rev-parse", "HEAD"], repo)
    _git(["merge", "--no-ff", "--no-commit", candidate], integration_wt)
    assert _git(["rev-parse", "-q", "--verify", "MERGE_HEAD"], integration_wt)

    with first._cs():
        token_id = first._acquire_merge_token_locked("crashed-coordinator")
    assert token_id is not None

    base_tree = _git(["rev-parse", f"{base}^{{tree}}"], repo)
    winner = _git(
        ["commit-tree", base_tree, "-p", base, "-m", "external winner"], repo
    )
    _git(["update-ref", "refs/heads/main", winner, base], repo)
    assert _git(["rev-parse", "refs/heads/main"], repo) == winner

    recovered = Coordinator(
        str(db), repo=str(repo), worktrees_dir=str(tmp_path / "wt"),
        integration_branch="main", enforce_single_coordinator=False,
    )

    assert _git(["rev-parse", "refs/heads/main"], repo) == winner
    assert subprocess.run(
        ["git", "rev-parse", "-q", "--verify", "MERGE_HEAD"],
        cwd=integration_wt, capture_output=True, text=True,
    ).returncode != 0
    assert _git(["status", "--porcelain"], integration_wt) == ""
    assert recovered.store.all_held_merge_tokens() == []


def test_token_only_recovery_seals_integration_ref_before_abort(
    tmp_path, monkeypatch,
):
    repo = tmp_path / "repo"
    db = tmp_path / "omd.db"
    _init_repo(repo)
    first = Coordinator(
        str(db), repo=str(repo), worktrees_dir=str(tmp_path / "wt"),
        integration_branch="main", enforce_single_coordinator=False,
    )
    integration_wt = Path(first._ensure_integration_wt())
    base = _git(["rev-parse", "refs/heads/main"], repo)

    (repo / "candidate.txt").write_text("candidate\n")
    _git(["add", "candidate.txt"], repo)
    _git(["commit", "-m", "candidate"], repo)
    candidate = _git(["rev-parse", "HEAD"], repo)
    _git(["merge", "--no-ff", "--no-commit", candidate], integration_wt)
    assert _git(["rev-parse", "-q", "--verify", "MERGE_HEAD"], integration_wt)

    with first._cs():
        token_id = first._acquire_merge_token_locked("crashed-coordinator")
    assert token_id is not None

    raw_update_results = []
    original_abort = GitRepo.abort_merge_preserving_ref

    def abort_then_try_raw_update(self, worktree, integration_branch):
        original_abort(self, worktree, integration_branch)
        raw_update_results.append(
            subprocess.run(
                [
                    "git", "update-ref", "refs/heads/main", candidate, base,
                ],
                cwd=repo, capture_output=True, text=True,
            )
        )

    monkeypatch.setattr(
        GitRepo, "abort_merge_preserving_ref", abort_then_try_raw_update
    )
    recovered = Coordinator(
        str(db), repo=str(repo), worktrees_dir=str(tmp_path / "wt"),
        integration_branch="main", enforce_single_coordinator=False,
    )

    assert len(raw_update_results) == 1
    assert raw_update_results[0].returncode != 0
    assert _git(["rev-parse", "refs/heads/main"], repo) == base
    assert subprocess.run(
        ["git", "rev-parse", "-q", "--verify", "MERGE_HEAD"],
        cwd=integration_wt, capture_output=True, text=True,
    ).returncode != 0
    assert _git(["status", "--porcelain"], integration_wt) == ""
    assert recovered.store.all_held_merge_tokens() == []


def test_recovery_rejects_trailer_commit_with_forged_merge_parents(tmp_path):
    repo = tmp_path / "repo"
    db = tmp_path / "omd.db"
    _init_repo(repo)
    first = Coordinator(
        str(db), repo=str(repo), worktrees_dir=str(tmp_path / "wt"),
        integration_branch="main", enforce_single_coordinator=False,
    )
    claim, _ = _develop(first)
    phase_a = first._connect_phase_a("T", "ag", claim["fence"])
    intent = phase_a["intent"]
    base = intent["integration_base_sha"]
    task_tip = intent["branch_tip_sha"]
    base_tree = _git(["rev-parse", f"{base}^{{tree}}"], repo)
    forged_message = (
        "forged recovery marker\n\n"
        + first._trailer(
            "T", intent["attempt_id"], intent["connect_attempt_id"]
        )
        + "\n"
    )
    forged = subprocess.run(
        ["git", "commit-tree", base_tree, "-p", task_tip],
        cwd=repo, check=True, capture_output=True, text=True,
        input=forged_message,
    ).stdout.strip()
    _git(["update-ref", "refs/heads/main", forged, base], repo)

    with pytest.raises(RuntimeError, match="lacks durable candidate attestation"):
        Coordinator(
            str(db), repo=str(repo), worktrees_dir=str(tmp_path / "wt"),
            integration_branch="main", enforce_single_coordinator=False,
        )

    task = first.store.get_task("T")
    connect_try = first.store.get_connect_attempt(intent["connect_attempt_id"])
    assert _git(["rev-parse", "refs/heads/main"], repo) == forged
    assert task["state"] == "CONNECTING"
    assert task["connect_attempt_id"] == intent["connect_attempt_id"]
    assert connect_try["terminal_at"] is None and connect_try["outcome"] is None
    assert first.store.get_orbit(phase_a["token_id"])["state"] == "HELD"
    assert not (Path(first.integration_worktree) / "src/x.py").exists()


def test_recovery_rejects_exact_parent_trailer_commit_with_unattested_tree(tmp_path):
    repo = tmp_path / "repo"
    db = tmp_path / "omd.db"
    _init_repo(repo)
    first = Coordinator(
        str(db), repo=str(repo), worktrees_dir=str(tmp_path / "wt"),
        integration_branch="main", enforce_single_coordinator=False,
    )
    claim, _ = _develop(first)
    phase_a = first._connect_phase_a("T", "ag", claim["fence"])
    intent = phase_a["intent"]
    base = intent["integration_base_sha"]
    base_tree = _git(["rev-parse", f"{base}^{{tree}}"], repo)
    message = (
        "forged exact-parent recovery marker\n\n"
        + first._trailer(
            "T", intent["attempt_id"], intent["connect_attempt_id"]
        )
        + "\n"
    )
    forged = subprocess.run(
        [
            "git", "commit-tree", base_tree,
            "-p", base, "-p", intent["branch_tip_sha"],
        ],
        cwd=repo, check=True, capture_output=True, text=True, input=message,
    ).stdout.strip()
    _git(["update-ref", "refs/heads/main", forged, base], repo)
    first.store.db.close()

    with pytest.raises(RuntimeError, match="lacks durable candidate attestation"):
        Coordinator(
            str(db), repo=str(repo), worktrees_dir=str(tmp_path / "wt"),
            integration_branch="main", enforce_single_coordinator=False,
        )

    # The forged tree omits the task payload despite exact ordered parents and trailers.
    assert _git(["rev-parse", f"{forged}^{{tree}}"], repo) == base_tree
    assert subprocess.run(
        ["git", "cat-file", "-e", f"{forged}:src/x.py"],
        cwd=repo, capture_output=True, text=True,
    ).returncode != 0


def test_recovery_rejects_wrong_tree_commit_against_sealed_candidate(
    tmp_path, monkeypatch,
):
    repo = tmp_path / "repo"
    db = tmp_path / "omd.db"
    _init_repo(repo)
    first = Coordinator(
        str(db), repo=str(repo), worktrees_dir=str(tmp_path / "wt"),
        integration_branch="main", enforce_single_coordinator=False,
    )
    claim, _ = _develop(first)
    phase_a = first._connect_phase_a("T", "ag", claim["fence"])
    intent = phase_a["intent"]
    original_git = first.git._git
    blocked = False

    def block_candidate_publication(*args, **kwargs):
        nonlocal blocked
        if (
            not blocked
            and len(args) >= 2
            and args[0] == "update-ref"
            and args[1] == "refs/heads/main"
        ):
            blocked = True
            raise GitError("leave legitimate candidate sealed but unpublished")
        return original_git(*args, **kwargs)

    monkeypatch.setattr(first.git, "_git", block_candidate_publication)
    merge_sha, err = first._connect_phase_b(intent)
    assert merge_sha is None and isinstance(err, GitIntegrationPreconditionError)
    sealed = first.store.get_connect_attempt(intent["connect_attempt_id"])
    legitimate_candidate = sealed["candidate_commit_sha"]
    assert legitimate_candidate and sealed["candidate_tree_sha"]

    base = intent["integration_base_sha"]
    base_tree = _git(["rev-parse", f"{base}^{{tree}}"], repo)
    message = (
        "forged wrong-tree recovery marker\n\n"
        + first._trailer(
            "T", intent["attempt_id"], intent["connect_attempt_id"]
        )
        + "\n"
    )
    forged = subprocess.run(
        [
            "git", "commit-tree", base_tree,
            "-p", base, "-p", intent["branch_tip_sha"],
        ],
        cwd=repo, check=True, capture_output=True, text=True, input=message,
    ).stdout.strip()
    assert forged != legitimate_candidate
    _git(["update-ref", "refs/heads/main", forged, base], repo)
    first.store.db.close()

    with pytest.raises(RuntimeError, match="does not match durable candidate"):
        Coordinator(
            str(db), repo=str(repo), worktrees_dir=str(tmp_path / "wt"),
            integration_branch="main", enforce_single_coordinator=False,
        )


def test_git_identity_helpers_ignore_replace_refs(tmp_path):
    repo = tmp_path / "repo"
    _init_repo(repo)
    base = _git(["rev-parse", "refs/heads/main"], repo)
    (repo / "replacement-task.txt").write_text("task\n")
    _git(["add", "replacement-task.txt"], repo)
    _git(["commit", "-m", "replacement task tip"], repo)
    task_tip = _git(["rev-parse", "refs/heads/dev"], repo)
    base_tree = _git(["rev-parse", f"{base}^{{tree}}"], repo)
    actual = _git(
        ["commit-tree", base_tree, "-p", base, "-m", "actual one-parent"], repo
    )
    replacement = _git(
        [
            "commit-tree", base_tree, "-p", base, "-p", task_tip,
            "-m", "replacement two-parent",
        ],
        repo,
    )
    _git(["replace", actual, replacement], repo)
    assert tuple(_git(["show", "-s", "--format=%P", actual], repo).split()) == (
        base, task_tip,
    )

    git = GitRepo(str(repo))
    assert git.commit_parents(str(repo), actual) == (base,)
    assert git.commit_tree(str(repo), actual) == base_tree


def test_recovery_ignores_graft_that_forges_already_integrated_ancestry(tmp_path):
    repo = tmp_path / "repo"
    db = tmp_path / "omd.db"
    _init_repo(repo)
    first = Coordinator(
        str(db), repo=str(repo), worktrees_dir=str(tmp_path / "wt"),
        integration_branch="main", enforce_single_coordinator=False,
    )
    claim, _ = _develop(first)
    initial_base = _git(["rev-parse", "refs/heads/main"], repo)
    base_tree = _git(["rev-parse", f"{initial_base}^{{tree}}"], repo)
    divergent_base = _git(
        [
            "commit-tree", base_tree, "-p", initial_base,
            "-m", "integration-only sibling",
        ],
        repo,
    )
    _git(
        ["update-ref", "refs/heads/main", divergent_base, initial_base], repo
    )
    phase_a = first._connect_phase_a("T", "ag", claim["fence"])
    intent = phase_a["intent"]
    assert intent["integration_base_sha"] == divergent_base

    grafts = repo / ".git" / "info" / "grafts"
    grafts.parent.mkdir(parents=True, exist_ok=True)
    grafts.write_text(f"{divergent_base} {intent['branch_tip_sha']}\n")
    forged_ancestry = subprocess.run(
        [
            "git", "merge-base", "--is-ancestor",
            intent["branch_tip_sha"], divergent_base,
        ],
        cwd=repo, capture_output=True, text=True,
    )
    assert forged_ancestry.returncode == 0
    assert first.git.is_ancestor(
        intent["branch_tip_sha"], divergent_base, cwd=repo
    ) is False
    first.store.db.close()

    recovered = Coordinator(
        str(db), repo=str(repo), worktrees_dir=str(tmp_path / "wt"),
        integration_branch="main", enforce_single_coordinator=False,
    )

    task = recovered.store.get_task("T")
    connect = recovered.store.get_connect_attempt(intent["connect_attempt_id"])
    assert task["state"] == "DONE" and task["merge_sha"] is None
    assert connect["outcome"] == "ROLLED_BACK"
    assert connect["outcome_code"] == "RECOVERED_ROLLBACK"
    assert connect["candidate_commit_sha"] is None
    assert recovered.git.branch_tip("main") == divergent_base
    assert not (Path(recovered.integration_worktree) / "src/x.py").exists()


def test_recovery_fails_closed_when_native_attempt_pointer_is_missing(tmp_path):
    repo = tmp_path / "repo"
    db = tmp_path / "omd.db"
    _init_repo(repo)
    omd = Coordinator(
        str(db), repo=str(repo), worktrees_dir=str(tmp_path / "wt"),
        integration_branch="main", enforce_single_coordinator=False,
    )
    claim, _ = _develop(omd)
    omd._connect_phase_a("T", "ag", claim["fence"])
    with omd.store.tx():
        omd.store.set_task("T", attempt_id=None)
    omd.store.db.close()

    with pytest.raises(RuntimeError, match="incomplete connect provenance"):
        Coordinator(
            str(db), repo=str(repo), worktrees_dir=str(tmp_path / "wt"),
            integration_branch="main", enforce_single_coordinator=False,
        )


def test_worktree_cleanup_waits_for_phase_c_commit(tmp_path, monkeypatch):
    repo = tmp_path / "repo"
    db = tmp_path / "omd.db"
    _init_repo(repo)
    omd = Coordinator(
        str(db), repo=str(repo), worktrees_dir=str(tmp_path / "wt"),
        integration_branch="main", enforce_single_coordinator=False,
    )
    claim, started = _develop(omd)
    phase_a = omd._connect_phase_a("T", "ag", claim["fence"])
    merge_sha, err = omd._connect_phase_b(phase_a["intent"])
    assert merge_sha and err is None
    original_set_flag = omd.store.set_flag

    def fail_after_merge_rows(*args, **kwargs):
        raise RuntimeError("injected before SQLite commit")

    monkeypatch.setattr(omd.store, "set_flag", fail_after_merge_rows)
    with pytest.raises(RuntimeError, match="injected"):
        omd._connect_phase_c(
            "T", phase_a["token_id"], phase_a["intent"], merge_sha, None
        )

    assert omd.store.get_task("T")["state"] == "CONNECTING"
    assert Path(started["worktree"]).exists()
    monkeypatch.setattr(omd.store, "set_flag", original_set_flag)
    omd.store.db.close()

    recovered = Coordinator(
        str(db), repo=str(repo), worktrees_dir=str(tmp_path / "wt"),
        integration_branch="main", enforce_single_coordinator=False,
    )
    assert recovered.store.get_task("T")["state"] == "MERGED"
    assert not Path(started["worktree"]).exists()


def test_recovery_waits_for_live_connect_when_leader_lease_is_disabled(
    tmp_path, monkeypatch,
):
    repo = tmp_path / "repo"
    db = tmp_path / "omd.db"
    worktrees = tmp_path / "wt"
    _init_repo(repo)
    first = Coordinator(
        str(db), repo=str(repo), worktrees_dir=str(worktrees),
        integration_branch="main", enforce_single_coordinator=False,
    )
    claim, _ = _develop(first)
    entered_phase_b = threading.Event()
    release_phase_b = threading.Event()
    original_phase_b = first._connect_phase_b

    def blocked_phase_b(intent, push=None):
        entered_phase_b.set()
        assert release_phase_b.wait(5)
        return original_phase_b(intent, push=push)

    monkeypatch.setattr(first, "_connect_phase_b", blocked_phase_b)
    outcomes = {}
    connect_thread = threading.Thread(
        target=lambda: outcomes.setdefault(
            "connect", first.connect("T", "ag", claim["fence"])
        )
    )
    connect_thread.start()
    assert entered_phase_b.wait(5)

    def start_recovery():
        outcomes["recovered"] = Coordinator(
            str(db), repo=str(repo), worktrees_dir=str(worktrees),
            integration_branch="main", enforce_single_coordinator=False,
        )

    recovery_thread = threading.Thread(target=start_recovery)
    recovery_thread.start()
    recovery_thread.join(timeout=0.1)
    assert recovery_thread.is_alive(), "recovery must wait for the live connect"
    release_phase_b.set()
    connect_thread.join(timeout=10)
    recovery_thread.join(timeout=10)

    assert not connect_thread.is_alive() and not recovery_thread.is_alive()
    assert outcomes["connect"]["ok"] is True
    assert outcomes["recovered"].store.get_task("T")["state"] == "MERGED"
    assert outcomes["recovered"].store.all_held_merge_tokens() == []


def test_recovery_waits_for_whole_live_barrier_trip_when_leader_lease_is_disabled(
    tmp_path, monkeypatch,
):
    repo = tmp_path / "repo"
    db = tmp_path / "omd.db"
    worktrees = tmp_path / "wt"
    _init_repo(repo)
    first = Coordinator(
        str(db), repo=str(repo), worktrees_dir=str(worktrees),
        integration_branch="main", enforce_single_coordinator=False,
    )
    claims = {}
    for task, agent, path in (
        ("A", "agA", "a/x.py"),
        ("B", "agB", "b/x.py"),
    ):
        claims[task], _ = _develop(first, task=task, agent=agent, path=path)
    first.barrier_declare("pair", ["A", "B"])
    assert first.barrier_arrive(
        "pair", "agA", "A", fence=claims["A"]["fence"]
    )["state"] == "ARMED"

    first_member_done = threading.Event()
    release_trip = threading.Event()
    original_connect_one = first._barrier_connect_one
    member_count = 0

    def pause_after_first_member(task_id, expected_fence):
        nonlocal member_count
        result = original_connect_one(task_id, expected_fence)
        member_count += 1
        if member_count == 1:
            first_member_done.set()
            assert release_trip.wait(5)
        return result

    monkeypatch.setattr(first, "_barrier_connect_one", pause_after_first_member)
    outcomes = {}
    errors = {}

    def run_trip():
        try:
            outcomes["trip"] = first.barrier_arrive(
                "pair", "agB", "B", fence=claims["B"]["fence"]
            )
        except BaseException as exc:  # surface failures from the worker thread
            errors["trip"] = exc

    trip_thread = threading.Thread(target=run_trip)
    trip_thread.start()
    assert first_member_done.wait(5)

    def start_recovery():
        try:
            outcomes["recovered"] = Coordinator(
                str(db), repo=str(repo), worktrees_dir=str(worktrees),
                integration_branch="main", enforce_single_coordinator=False,
            )
        except BaseException as exc:  # surface failures from the worker thread
            errors["recovery"] = exc

    recovery_thread = threading.Thread(target=start_recovery)
    recovery_thread.start()
    recovery_thread.join(timeout=0.1)
    assert recovery_thread.is_alive(), "recovery must wait for the complete barrier trip"
    release_trip.set()
    trip_thread.join(timeout=10)
    recovery_thread.join(timeout=10)

    assert not trip_thread.is_alive() and not recovery_thread.is_alive()
    assert errors == {}
    assert outcomes["trip"]["ok"] is True
    assert outcomes["trip"]["state"] == "TRIPPED"
    recovered = outcomes["recovered"]
    assert recovered.store.barrier_by_name("pair")["state"] == "TRIPPED"
    assert recovered.store.get_task("A")["state"] == "MERGED"
    assert recovered.store.get_task("B")["state"] == "MERGED"


def test_abort_waits_for_whole_live_barrier_trip(tmp_path, monkeypatch):
    repo = tmp_path / "repo"
    db = tmp_path / "omd.db"
    worktrees = tmp_path / "wt"
    _init_repo(repo)
    omd = Coordinator(
        str(db), repo=str(repo), worktrees_dir=str(worktrees),
        integration_branch="main", enforce_single_coordinator=False,
    )
    claims = {}
    for task, agent, path in (
        ("A", "agA", "a/x.py"),
        ("B", "agB", "b/x.py"),
    ):
        claims[task], _ = _develop(omd, task=task, agent=agent, path=path)
    omd.barrier_declare("pair", ["A", "B"])
    assert omd.barrier_arrive(
        "pair", "agA", "A", fence=claims["A"]["fence"]
    )["state"] == "ARMED"

    first_member_done = threading.Event()
    release_trip = threading.Event()
    original_connect_one = omd._barrier_connect_one
    member_count = 0

    def pause_after_first_member(task_id, expected_fence):
        nonlocal member_count
        result = original_connect_one(task_id, expected_fence)
        member_count += 1
        if member_count == 1:
            first_member_done.set()
            assert release_trip.wait(5)
        return result

    monkeypatch.setattr(omd, "_barrier_connect_one", pause_after_first_member)
    outcomes = {}
    errors = {}

    def run_trip():
        try:
            outcomes["trip"] = omd.barrier_arrive(
                "pair", "agB", "B", fence=claims["B"]["fence"]
            )
        except BaseException as exc:
            errors["trip"] = exc

    def run_abort():
        try:
            outcomes["abort"] = omd.barrier_abort("pair", "agA")
        except BaseException as exc:
            errors["abort"] = exc

    trip_thread = threading.Thread(target=run_trip)
    trip_thread.start()
    assert first_member_done.wait(5)
    abort_thread = threading.Thread(target=run_abort)
    abort_thread.start()
    abort_thread.join(timeout=0.1)
    assert abort_thread.is_alive(), "abort must linearize after the live trip"
    release_trip.set()
    trip_thread.join(timeout=10)
    abort_thread.join(timeout=10)

    assert not trip_thread.is_alive() and not abort_thread.is_alive()
    assert errors == {}
    assert outcomes["trip"]["ok"] is True
    assert outcomes["trip"]["state"] == "TRIPPED"
    assert outcomes["abort"]["state"] == "TRIPPED"
    assert outcomes["abort"]["noop"] is True
    assert omd.store.get_task("A")["state"] == "MERGED"
    assert omd.store.get_task("B")["state"] == "MERGED"


def test_barrier_trip_stops_if_state_changes_between_members(monkeypatch):
    omd = Coordinator(allow_memory_db=True, agent_ttl=None)
    claims = {}
    for task, agent, area in (("A", "agA", "a"), ("B", "agB", "b")):
        omd.declare(task, writes=[f"{area}/**"])
        omd.next_task(agent)
        claims[task] = omd.claim(agent, [f"{area}/**"], task_id=task)
        omd.start(task, agent)
        omd.finish(task, agent, claims[task]["fence"])
    omd.barrier_declare("pair", ["A", "B"])
    assert omd.barrier_arrive(
        "pair", "agA", "A", fence=claims["A"]["fence"]
    )["state"] == "ARMED"

    original_connect_one = omd._barrier_connect_one
    member_count = 0

    def break_after_first_member(task_id, expected_fence):
        nonlocal member_count
        result = original_connect_one(task_id, expected_fence)
        member_count += 1
        if member_count == 1:
            with omd._cs():
                barrier = omd.store.barrier_by_name("pair")
                omd._break_barrier(barrier, reason="injected_abort")
        return result

    monkeypatch.setattr(omd, "_barrier_connect_one", break_after_first_member)
    out = omd.barrier_arrive(
        "pair", "agB", "B", fence=claims["B"]["fence"]
    )

    assert out["ok"] is False
    assert out["state"] == "BROKEN"
    assert out["merged"] == ["A"]
    assert omd.store.get_task("A")["state"] == "MERGED"
    assert omd.store.get_task("B")["state"] == "DONE"


def test_recovery_rejects_forged_trailer_without_candidate_attestation(tmp_path):
    repo = tmp_path / "repo"
    db = tmp_path / "omd.db"
    _init_repo(repo)
    omd = Coordinator(
        str(db), repo=str(repo), worktrees_dir=str(tmp_path / "wt"),
        integration_branch="main", enforce_single_coordinator=False,
    )
    claim, _ = _develop(omd)
    phase_a = omd._connect_phase_a("T", "ag", claim["fence"])
    integration_wt = Path(omd._ensure_integration_wt())
    message = "forged\n\n" + omd._trailer(
        "T", claim["attempt_id"], phase_a["connect_attempt_id"]
    )
    _git(["commit", "--allow-empty", "-m", message], integration_wt)
    omd.store.db.close()

    with pytest.raises(RuntimeError, match="lacks durable candidate attestation"):
        Coordinator(
            str(db), repo=str(repo), worktrees_dir=str(tmp_path / "wt"),
            integration_branch="main", enforce_single_coordinator=False,
        )


def test_recovery_forward_fixes_attempt_connect_and_merge_order(tmp_path):
    repo = tmp_path / "repo"
    db = tmp_path / "omd.db"
    _init_repo(repo)
    omd = Coordinator(
        str(db), repo=str(repo), worktrees_dir=str(tmp_path / "wt"),
        integration_branch="main", enforce_single_coordinator=False,
    )
    claim, _ = _develop(omd)
    phase_a = omd._connect_phase_a("T", "ag", claim["fence"])
    merge_sha, err = omd._connect_phase_b(phase_a["intent"])
    assert merge_sha and err is None
    connect_id = phase_a["connect_attempt_id"]
    omd.store.db.close()

    recovered = Coordinator(
        str(db), repo=str(repo), worktrees_dir=str(tmp_path / "wt"),
        integration_branch="main", enforce_single_coordinator=False,
    )

    task = recovered.store.get_task("T")
    assert task["state"] == "MERGED" and task["connect_attempt_id"] is None
    row = recovered.store.get_connect_attempt(connect_id)
    assert row["outcome"] == "MERGED"
    assert row["outcome_code"] == "RECOVERED_MERGED"
    assert row["resolution_source"] == "RECOVERY"
    assert row["merge_sha"] == merge_sha and row["merge_gen"] == 1
    assert row["candidate_commit_sha"] == merge_sha
    assert row["candidate_tree_sha"] == recovered.git.commit_tree(
        recovered.integration_worktree, merge_sha
    )
    assert recovered.store.get_attempt(claim["attempt_id"])["terminal_state"] == "MERGED"
    assert recovered.store.integration_gen() == 1
