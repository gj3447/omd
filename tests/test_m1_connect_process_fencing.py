"""M1 P0: split-connect process fencing and recovery proof authority.

The stdio MCP deployment intentionally permits more than one Coordinator over
the same SQLite database.  A newly constructed process therefore must not
interpret another process's live split effect as crash residue.  Recovery may
complete a repo-bound connect idempotency envelope only when the recorded merge
SHA is backed by the exact task trailer in the integration branch.
"""

from __future__ import annotations

import sqlite3
import subprocess
import threading
from pathlib import Path

import pytest

from omd_server import Coordinator
from omd_server.gitio import GitError, GitRepo, GitTimeout
from omd_server.store import Store


class _CrashCut(RuntimeError):
    pass


def _git(args, cwd):
    return subprocess.run(
        ["git", *args], cwd=str(cwd), check=True, capture_output=True, text=True
    )


def _init_repo(root: Path) -> str:
    root.mkdir()
    _git(["init", "-b", "main"], root)
    _git(["config", "user.name", "test"], root)
    _git(["config", "user.email", "test@example.com"], root)
    (root / "README.md").write_text("base\n")
    _git(["add", "-A"], root)
    _git(["commit", "-m", "base"], root)
    base = _git(["rev-parse", "HEAD"], root).stdout.strip()
    _git(["checkout", "-b", "dev"], root)
    return base


def _repo_coordinator(tmp_path: Path, repo: Path, coordinator_id: str) -> Coordinator:
    return Coordinator(
        db_path=str(tmp_path / "omd.db"),
        repo=str(repo),
        worktrees_dir=str(tmp_path / "worktrees"),
        integration_branch="main",
        coordinator_id=coordinator_id,
        enforce_single_coordinator=False,
        agent_ttl=None,
    )


def _ready_repo_task(omd: Coordinator) -> int:
    begun = omd.begin("T", "ag", ["change/**"], ttl=30.0)
    assert begun["ok"] is True
    worktree = Path(begun["worktree"])
    (worktree / "change").mkdir()
    (worktree / "change" / "value.py").write_text("VALUE = 2\n")
    assert omd.commit("T", "change value", "ag", begun["fence"])["ok"] is True
    assert omd.finish("T", "ag", begun["fence"])["state"] == "DONE"
    return begun["fence"]


def _ready_db_task(omd: Coordinator) -> None:
    omd.declare("T", writes=["change/**"])
    omd.next_task("ag")
    omd.claim("ag", ["change/**"], task_id="T")
    omd.start("T", "ag")
    omd.finish("T")


def _cut_after_phase_c(omd: Coordinator, request_id: str) -> None:
    """Leave authoritative MERGED plus the original connect envelope INFLIGHT."""

    def crash_before_finalization(*_args, **_kwargs):
        raise _CrashCut("response finalization was not observed")

    omd._complete_split_idem = crash_before_finalization
    with pytest.raises(_CrashCut):
        omd.connect("T", request_id=request_id)
    assert omd.store.get_task("T")["state"] == "MERGED"
    assert omd.store.get_idem(request_id)["status"] == "INFLIGHT"


def _cut_after_phase_b(omd: Coordinator, request_id: str, *, agent="ag", fence=None):
    """Leave exact Git proof plus CONNECTING/token/pin/INFLIGHT authority."""
    real_phase_c = omd._connect_phase_c

    def crash_before_phase_c(*_args, **_kwargs):
        raise _CrashCut("phase C was not observed")

    omd._connect_phase_c = crash_before_phase_c
    try:
        with pytest.raises(_CrashCut):
            omd.connect("T", agent, fence, request_id=request_id)
    finally:
        omd._connect_phase_c = real_phase_c
    task = omd.store.get_task("T")
    assert task["state"] == "CONNECTING" and task["connect_repo_bound"] == 1
    assert omd.store.get_idem(request_id)["status"] == "INFLIGHT"
    assert len(omd.store.all_held_merge_tokens()) == 1
    return task


def _close(omd: Coordinator) -> None:
    omd.close()
    omd.store.db.close()


def test_second_coordinator_does_not_recover_a_live_phase_b(tmp_path):
    """A live process owns CONNECTING, its token, pin, and INFLIGHT envelope."""
    repo = tmp_path / "repo"
    _init_repo(repo)
    first = _repo_coordinator(tmp_path, repo, "first")
    fence = _ready_repo_task(first)

    phase_b_entered = threading.Event()
    release_phase_b = threading.Event()
    real_phase_b = first._connect_phase_b
    result: dict = {}

    def blocked_phase_b(intent, push=None):
        phase_b_entered.set()
        assert release_phase_b.wait(5), "test did not release Phase B"
        return real_phase_b(intent, push=push)

    first._connect_phase_b = blocked_phase_b

    def connect():
        try:
            result["connect"] = first.connect(
                "T", "ag", fence, request_id="live-connect"
            )
        except BaseException as exc:  # surface thread failures in the test thread
            result["connect_error"] = exc

    connect_thread = threading.Thread(target=connect, name="live-connect")
    connect_thread.start()
    assert phase_b_entered.wait(5)

    before_task = first.store.get_task("T")
    before_tokens = first.store.all_held_merge_tokens()
    before_idem = first.store.get_idem("live-connect")
    before_orbit = next(
        orbit
        for orbit in first.store.orbits_for_task("T")
        if orbit["mode"] in ("write", "shared")
    )
    assert before_task["state"] == "CONNECTING"
    assert len(before_tokens) == 1
    assert before_idem["status"] == "INFLIGHT"
    assert before_orbit["merging"] == 1

    constructor_started = threading.Event()
    constructor_done = threading.Event()
    second_result: dict = {}

    def construct_second():
        constructor_started.set()
        try:
            second_result["coordinator"] = _repo_coordinator(tmp_path, repo, "second")
        except BaseException as exc:
            second_result["error"] = exc
        finally:
            constructor_done.set()

    second_thread = threading.Thread(target=construct_second, name="second-coordinator")
    second_thread.start()
    assert constructor_started.wait(2)
    # Give an unsafe constructor enough time to run recovery.  A conforming
    # implementation may either wait for the shared effect lock or skip the
    # effect as live; both preserve the four authority records below.
    constructor_done.wait(0.35)
    during_task = first.store.get_task("T")
    during_tokens = first.store.all_held_merge_tokens()
    during_idem = first.store.get_idem("live-connect")
    during_orbit = first.store.get_orbit(before_orbit["orbit_id"])

    release_phase_b.set()
    connect_thread.join(timeout=10)
    second_thread.join(timeout=10)

    assert during_task["state"] == "CONNECTING"
    assert [row["orbit_id"] for row in during_tokens] == [
        row["orbit_id"] for row in before_tokens
    ]
    assert during_idem["status"] == "INFLIGHT"
    assert during_idem["arg_hash"] == before_idem["arg_hash"]
    assert during_orbit["state"] == "HELD" and during_orbit["merging"] == 1
    assert not connect_thread.is_alive()
    assert not second_thread.is_alive()
    assert "connect_error" not in result
    assert "error" not in second_result
    assert result["connect"]["state"] == "MERGED"
    assert first.store.get_idem("live-connect")["status"] == "DONE"
    assert first.store.all_held_merge_tokens() == []

    _close(first)
    _close(second_result["coordinator"])


def test_effect_lock_covers_phase_c_to_idempotency_finalization(tmp_path):
    """Recovery cannot steal the response envelope after MERGED but before DONE."""
    repo = tmp_path / "repo"
    _init_repo(repo)
    first = _repo_coordinator(tmp_path, repo, "first")
    fence = _ready_repo_task(first)

    finalization_entered = threading.Event()
    release_finalization = threading.Event()
    real_complete = first._complete_split_idem
    result: dict = {}

    def blocked_complete(request_id, agent_id, verb, args, response):
        if verb == "connect":
            finalization_entered.set()
            assert release_finalization.wait(5), "test did not release finalization"
        return real_complete(request_id, agent_id, verb, args, response)

    first._complete_split_idem = blocked_complete

    def connect():
        try:
            result["connect"] = first.connect(
                "T", "ag", fence, request_id="finalizing-connect"
            )
        except BaseException as exc:
            result["connect_error"] = exc

    connect_thread = threading.Thread(target=connect, name="finalizing-connect")
    connect_thread.start()
    assert finalization_entered.wait(5)
    assert first.store.get_task("T")["state"] == "MERGED"
    assert first.store.get_idem("finalizing-connect")["status"] == "INFLIGHT"

    constructor_started = threading.Event()
    constructor_done = threading.Event()
    second_result: dict = {}

    def construct_second():
        constructor_started.set()
        try:
            second_result["coordinator"] = _repo_coordinator(tmp_path, repo, "second")
        except BaseException as exc:
            second_result["error"] = exc
        finally:
            constructor_done.set()

    second_thread = threading.Thread(target=construct_second, name="second-finalizer")
    second_thread.start()
    assert constructor_started.wait(2)
    constructor_done.wait(0.35)
    during = first.store.get_idem("finalizing-connect")

    release_finalization.set()
    connect_thread.join(timeout=10)
    second_thread.join(timeout=10)

    assert during["status"] == "INFLIGHT"
    assert not connect_thread.is_alive()
    assert not second_thread.is_alive()
    assert "connect_error" not in result
    assert "error" not in second_result
    assert result["connect"]["state"] == "MERGED"
    assert first.store.get_idem("finalizing-connect")["status"] == "DONE"

    _close(first)
    _close(second_result["coordinator"])


@pytest.mark.parametrize("stored_proof", ["missing", "unrelated_sha"])
def test_repo_recovery_rejects_merged_without_exact_git_proof(tmp_path, stored_proof):
    """MERGED state alone, or a SHA without T's trailer, cannot mint DONE."""
    repo = tmp_path / "repo"
    base = _init_repo(repo)
    original = _repo_coordinator(tmp_path, repo, "original")
    _ready_repo_task(original)
    request_id = f"repo-proof-{stored_proof}"
    _cut_after_phase_c(original, request_id)
    with original.store.tx():
        original.store.set_task(
            "T", merge_sha=base if stored_proof == "unrelated_sha" else None
        )
    assert original.store.get_task("T")["merge_sha"] == (
        base if stored_proof == "unrelated_sha" else None
    )
    _close(original)

    recovered = _repo_coordinator(tmp_path, repo, "recovery")

    row = recovered.store.get_idem(request_id)
    assert row["status"] == "INFLIGHT"
    retry = recovered.connect("T", request_id=request_id)
    assert retry["ok"] is False
    assert retry["reason"] == "request_inflight"
    _close(recovered)


def test_repo_recovery_accepts_recorded_sha_with_exact_task_trailer(tmp_path):
    repo = tmp_path / "repo"
    _init_repo(repo)
    original = _repo_coordinator(tmp_path, repo, "original")
    _ready_repo_task(original)
    _cut_after_phase_c(original, "repo-proof-valid")
    task = original.store.get_task("T")
    assert task["merge_sha"]
    assert original.git.branch_in_integration(
        original.integration_worktree, "main", original._trailer("T")
    ) == task["merge_sha"]
    _close(original)

    recovered = _repo_coordinator(tmp_path, repo, "recovery")

    assert recovered.store.get_idem("repo-proof-valid")["status"] == "DONE"
    replay = recovered.connect("T", request_id="repo-proof-valid")
    assert replay["state"] == "MERGED"
    assert replay["merge_sha"] == task["merge_sha"]
    assert replay["recovered"] is True and replay["replayed"] is True
    _close(recovered)


def test_db_only_recovery_accepts_merged_without_merge_sha(tmp_path):
    db_path = tmp_path / "omd.db"
    original = Coordinator(
        db_path=str(db_path), enforce_single_coordinator=False, agent_ttl=None
    )
    _ready_db_task(original)
    _cut_after_phase_c(original, "db-only-proof")
    assert original.store.get_task("T")["merge_sha"] is None
    _close(original)

    recovered = Coordinator(
        db_path=str(db_path), enforce_single_coordinator=False, agent_ttl=None
    )

    assert recovered.store.get_idem("db-only-proof")["status"] == "DONE"
    replay = recovered.connect("T", request_id="db-only-proof")
    assert replay["state"] == "MERGED"
    assert replay["merge_sha"] is None
    assert replay["recovered"] is True and replay["replayed"] is True
    _close(recovered)


def test_repo_bound_connecting_recovery_without_repo_fails_closed(tmp_path):
    repo = tmp_path / "repo"
    _init_repo(repo)
    original = _repo_coordinator(tmp_path, repo, "original")
    fence = _ready_repo_task(original)
    task = _cut_after_phase_b(
        original, "repo-required", agent="ag", fence=fence
    )
    proof_sha = original.git.branch_in_integration(
        original.integration_worktree, "main",
        original._trailer("T", task["connect_attempt_id"]), strict=True,
    )
    assert proof_sha
    _close(original)

    recovered = Coordinator(
        db_path=str(tmp_path / "omd.db"), enforce_single_coordinator=False,
        agent_ttl=None,
    )

    task = recovered.store.get_task("T")
    assert task["state"] == "CONNECTING" and task["connect_repo_bound"] == 1
    assert recovered.store.get_idem("repo-required")["status"] == "INFLIGHT"
    assert len(recovered.store.all_held_merge_tokens()) == 1
    retry = recovered.connect("T", "ag", fence, request_id="repo-required")
    assert retry["reason"] == "request_inflight"
    _close(recovered)


def test_legacy_git_intent_migration_backfills_repo_bound_before_recovery(tmp_path):
    repo = tmp_path / "repo"
    _init_repo(repo)
    original = _repo_coordinator(tmp_path, repo, "original")
    fence = _ready_repo_task(original)
    task = _cut_after_phase_b(original, "legacy-mode", agent="ag", fence=fence)
    token_id = task["connect_token_id"]
    with original.store.tx():
        # Model the actual pre-generation schema: the Git intent fields exist,
        # while every modern task/token/idempotency binding is absent.
        original.store.set_task(
            "T", connect_repo_bound=0, connect_attempt_id=None,
            connect_owner_instance=None, connect_owner_generation=0,
            connect_token_id=None, connect_request_id=None,
            connect_arg_hash=None,
        )
        original.store.set_orbit(
            token_id, operation_id=None, owner_instance=None,
            owner_generation=None,
        )
        original.store.clear_idem("legacy-mode")
    _close(original)

    # Model the actual predecessor schema: the mode column itself did not
    # exist.  Adding it may infer legacy Git authority exactly once.
    db_path = tmp_path / "omd.db"
    with sqlite3.connect(db_path) as legacy:
        legacy.execute("DELETE FROM meta WHERE key='schema_version'")
        legacy.execute("ALTER TABLE tasks DROP COLUMN connect_repo_bound")

    migrated = Store(str(db_path), initialize=True)
    task = migrated.get_task("T")
    assert task["connect_repo_bound"] == 1
    assert task["state"] == "CONNECTING"
    assert task["connect_attempt_id"] is None
    migrated.set_task("T", connect_repo_bound=0)
    migrated.db.close()

    reopened = Store(str(db_path), initialize=True)
    assert reopened.get_task("T")["connect_repo_bound"] == 0
    reopened.db.close()


def test_modern_db_only_mode_survives_repeated_reopen_with_stale_repo_fields(
    tmp_path,
):
    db_path = tmp_path / "omd.db"
    original = Coordinator(
        db_path=str(db_path), enforce_single_coordinator=False, agent_ttl=None
    )
    _ready_db_task(original)
    _cut_after_phase_c(original, "db-only-modern")
    with original.store.tx():
        original.store.set_task(
            "T", branch="stale/branch", worktree="/stale/worktree"
        )
    attempt_id = original.store.get_task("T")["connect_attempt_id"]
    assert attempt_id
    assert original.store.get_task("T")["connect_repo_bound"] == 0
    _close(original)

    for _ in range(2):
        reopened = Coordinator(
            db_path=str(db_path), enforce_single_coordinator=False,
            agent_ttl=None,
        )
        task = reopened.store.get_task("T")
        assert task["state"] == "MERGED"
        assert task["connect_attempt_id"] == attempt_id
        assert task["connect_repo_bound"] == 0
        _close(reopened)


def _modern_db_connecting_attempt(omd: Coordinator, request_id: str):
    _ready_db_task(omd)
    args = ["T", None, None, None, None]
    arg_hash = omd._arg_hash("connect", args)
    with omd._cs():
        with omd._idem(request_id, None, "connect", args) as slot:
            slot.defer()
    phase_a = omd._connect_phase_a(
        "T", None, None, request_id=request_id, request_agent=None,
        request_arg_hash=arg_hash,
    )
    assert phase_a["ok"] is True
    return phase_a


def test_modern_recovery_does_not_adopt_foreign_sole_merge_token(tmp_path):
    db_path = tmp_path / "omd.db"
    original = Coordinator(
        db_path=str(db_path), enforce_single_coordinator=False, agent_ttl=None
    )
    phase_a = _modern_db_connecting_attempt(original, "modern-token")
    token_id = phase_a["token_id"]
    with original.store.tx():
        original.store.set_task("T", connect_token_id=None)
        original.store.set_orbit(
            token_id, operation_id="foreign-operation",
            owner_instance="foreign-owner", owner_generation=99,
        )
    task_before = original.store.get_task("T")
    token_before = original.store.get_orbit(token_id)
    idem_before = original.store.get_idem("modern-token")
    _close(original)

    recovered = Coordinator(
        db_path=str(db_path), enforce_single_coordinator=False, agent_ttl=None
    )

    assert recovered.store.get_task("T") == task_before
    assert recovered.store.get_orbit(token_id) == token_before
    assert recovered.store.get_idem("modern-token") == idem_before
    assert recovered.store.get_task("T")["state"] == "CONNECTING"
    assert recovered.store.get_orbit(token_id)["state"] == "HELD"
    _close(recovered)


def test_modern_recovery_does_not_synthesize_operationless_idempotency_binding(
    tmp_path,
):
    db_path = tmp_path / "omd.db"
    original = Coordinator(
        db_path=str(db_path), enforce_single_coordinator=False, agent_ttl=None
    )
    phase_a = _modern_db_connecting_attempt(original, "modern-idem")
    token_id = phase_a["token_id"]
    with original.store.tx():
        original.store.db.execute(
            "UPDATE idempotency SET operation_id=NULL,owner_instance=NULL,"
            "owner_generation=NULL WHERE request_id=?",
            ("modern-idem",),
        )
    task_before = original.store.get_task("T")
    token_before = original.store.get_orbit(token_id)
    idem_before = original.store.get_idem("modern-idem")
    _close(original)

    recovered = Coordinator(
        db_path=str(db_path), enforce_single_coordinator=False, agent_ttl=None
    )

    assert recovered.store.get_task("T") == task_before
    assert recovered.store.get_orbit(token_id) == token_before
    assert recovered.store.get_idem("modern-idem") == idem_before
    assert recovered.store.get_idem("modern-idem")["operation_id"] is None
    _close(recovered)


def test_modern_recovery_rejects_null_owner_generation_tuple(tmp_path):
    db_path = tmp_path / "omd.db"
    original = Coordinator(
        db_path=str(db_path), enforce_single_coordinator=False, agent_ttl=None
    )
    phase_a = _modern_db_connecting_attempt(original, "null-modern-owner")
    token_id = phase_a["token_id"]
    with original.store.tx():
        original.store.set_task(
            "T", connect_owner_instance=None, connect_owner_generation=0
        )
        original.store.set_orbit(
            token_id, owner_instance=None, owner_generation=None
        )
        original.store.db.execute(
            "UPDATE idempotency SET owner_instance=NULL,owner_generation=NULL "
            "WHERE request_id=?",
            ("null-modern-owner",),
        )
    task_before = original.store.get_task("T")
    token_before = original.store.get_orbit(token_id)
    idem_before = original.store.get_idem("null-modern-owner")
    _close(original)

    recovered = Coordinator(
        db_path=str(db_path), enforce_single_coordinator=False, agent_ttl=None
    )

    assert recovered.store.get_task("T") == task_before
    assert recovered.store.get_orbit(token_id) == token_before
    assert recovered.store.get_idem("null-modern-owner") == idem_before
    assert recovered.store.get_task("T")["state"] == "CONNECTING"
    _close(recovered)


def test_legacy_recovery_rejects_partial_modern_owner_tuple(tmp_path):
    db_path = tmp_path / "omd.db"
    original = Coordinator(
        db_path=str(db_path), enforce_single_coordinator=False, agent_ttl=None
    )
    phase_a = _modern_db_connecting_attempt(original, "partial-legacy-owner")
    token_id = phase_a["token_id"]
    with original.store.tx():
        original.store.set_task(
            "T", connect_attempt_id=None, connect_owner_instance="partial-owner",
            connect_owner_generation=4, connect_request_id=None,
            connect_arg_hash=None,
        )
        original.store.set_orbit(
            token_id, operation_id=None, owner_instance=None,
            owner_generation=None,
        )
        original.store.clear_idem("partial-legacy-owner")
    task_before = original.store.get_task("T")
    token_before = original.store.get_orbit(token_id)
    _close(original)

    recovered = Coordinator(
        db_path=str(db_path), enforce_single_coordinator=False, agent_ttl=None
    )

    assert recovered.store.get_task("T") == task_before
    assert recovered.store.get_orbit(token_id) == token_before
    assert recovered.store.get_task("T")["state"] == "CONNECTING"
    _close(recovered)


@pytest.mark.parametrize("request_id", ["", 7])
def test_connect_rejects_invalid_request_id_without_mutation(tmp_path, request_id):
    omd = Coordinator(
        db_path=str(tmp_path / "omd.db"), enforce_single_coordinator=False,
        agent_ttl=None,
    )
    _ready_db_task(omd)
    task_before = omd.store.get_task("T")
    orbits_before = omd.store.orbits_for_task("T")
    fence_before = omd.store.current_fence()
    seq_before = omd.store.current_seq()

    result = omd.connect("T", request_id=request_id)

    assert result == {
        "ok": False, "state": "REJECTED", "reason": "invalid_request_id"
    }
    assert omd.store.get_task("T") == task_before
    assert omd.store.orbits_for_task("T") == orbits_before
    assert omd.store.current_fence() == fence_before
    assert omd.store.current_seq() == seq_before
    assert omd.store.all_held_merge_tokens() == []
    assert omd.store.inflight_idem() == []
    _close(omd)


def test_recovery_preserves_empty_request_landed_attempt_without_partial_takeover(
    tmp_path,
):
    repo = tmp_path / "repo"
    _init_repo(repo)
    original = _repo_coordinator(tmp_path, repo, "original")
    fence = _ready_repo_task(original)
    args = ["T", "ag", fence, None, None]
    arg_hash = original._arg_hash("connect", args)
    with original._cs():
        with original._idem("", "ag", "connect", args) as slot:
            slot.defer()
    phase_a = original._connect_phase_a(
        "T", "ag", fence, request_id="", request_agent="ag",
        request_arg_hash=arg_hash,
    )
    assert phase_a["ok"] is True
    merge_sha, error = original._connect_phase_b(phase_a["intent"])
    assert error is None and merge_sha
    token_id = phase_a["token_id"]
    task_before = original.store.get_task("T")
    token_before = original.store.get_orbit(token_id)
    idem_before = original.store.get_idem("")
    _close(original)

    recovered = _repo_coordinator(tmp_path, repo, "recovery")

    assert recovered.store.get_task("T") == task_before
    assert recovered.store.get_orbit(token_id) == token_before
    assert recovered.store.get_idem("") == idem_before
    assert recovered.store.get_task("T")["state"] == "CONNECTING"
    assert recovered.store.get_orbit(token_id)["state"] == "HELD"
    _close(recovered)


def test_recovery_rejects_exact_attempt_trailer_without_candidate_ancestry(
    tmp_path,
):
    repo = tmp_path / "repo"
    _init_repo(repo)
    original = _repo_coordinator(tmp_path, repo, "original")
    fence = _ready_repo_task(original)
    args = ["T", "ag", fence, None, None]
    arg_hash = original._arg_hash("connect", args)
    with original._cs():
        with original._idem("forged-proof", "ag", "connect", args) as slot:
            slot.defer()
    phase_a = original._connect_phase_a(
        "T", "ag", fence, request_id="forged-proof", request_agent="ag",
        request_arg_hash=arg_hash,
    )
    assert phase_a["ok"] is True
    intent = phase_a["intent"]
    wt = original._ensure_integration_wt()
    forged_sha = original.git.commit_empty_integration(
        wt,
        f"forged proof only\n\n{original._trailer('T')}\n"
        f"{original._trailer('T', intent['attempt_id'])}",
    )
    with pytest.raises(GitError):
        original.git.assert_ancestor(intent["branch_tip_sha"], forged_sha, cwd=wt)
    token_id = phase_a["token_id"]
    _close(original)

    recovered = _repo_coordinator(tmp_path, repo, "recovery")

    task = recovered.store.get_task("T")
    token = recovered.store.get_orbit(token_id)
    idem = recovered.store.get_idem("forged-proof")
    assert task["state"] == "CONNECTING"
    assert task["merge_sha"] is None
    assert token["state"] == "HELD"
    assert token["operation_id"] == task["connect_attempt_id"]
    assert recovered.store.pinned_orbits_for_task("T")
    assert idem["status"] == "INFLIGHT"
    _close(recovered)


def test_repo_bound_connecting_transient_git_probe_error_fails_closed(
    tmp_path, monkeypatch
):
    repo = tmp_path / "repo"
    _init_repo(repo)
    original = _repo_coordinator(tmp_path, repo, "original")
    fence = _ready_repo_task(original)
    _cut_after_phase_b(original, "probe-error", agent="ag", fence=fence)
    _close(original)

    def unavailable(*_args, **_kwargs):
        raise GitError("transient integration worktree failure")

    monkeypatch.setattr(GitRepo, "ensure_integration_worktree", unavailable)
    recovered = _repo_coordinator(tmp_path, repo, "recovery")

    assert recovered.store.get_task("T")["state"] == "CONNECTING"
    assert recovered.store.get_idem("probe-error")["status"] == "INFLIGHT"
    assert len(recovered.store.all_held_merge_tokens()) == 1
    _close(recovered)


def test_repo_bound_merged_inflight_without_repo_does_not_mint_done(tmp_path):
    repo = tmp_path / "repo"
    _init_repo(repo)
    original = _repo_coordinator(tmp_path, repo, "original")
    _ready_repo_task(original)
    _cut_after_phase_c(original, "merged-needs-repo")
    assert original.store.get_task("T")["connect_repo_bound"] == 1
    _close(original)

    recovered = Coordinator(
        db_path=str(tmp_path / "omd.db"), enforce_single_coordinator=False,
        agent_ttl=None,
    )

    assert recovered.store.get_task("T")["state"] == "MERGED"
    assert recovered.store.get_idem("merged-needs-repo")["status"] == "INFLIGHT"
    _close(recovered)


def test_timeout_reported_after_exact_git_commit_forward_completes(tmp_path):
    repo = tmp_path / "repo"
    _init_repo(repo)
    omd = _repo_coordinator(tmp_path, repo, "coordinator")
    fence = _ready_repo_task(omd)
    real_merge = omd.git.merge_into

    def commit_then_timeout(*args, **kwargs):
        real_merge(*args, **kwargs)
        raise GitTimeout("timeout observed after ref update")

    omd.git.merge_into = commit_then_timeout
    result = omd.connect("T", "ag", fence, request_id="late-timeout")

    assert result["ok"] is True and result["state"] == "MERGED"
    task = omd.store.get_task("T")
    assert task["merge_sha"] == result["merge_sha"]
    assert omd.store.get_idem("late-timeout")["status"] == "DONE"
    assert omd.store.all_held_merge_tokens() == []
    _close(omd)


def test_barrier_recovery_requires_repo_proof_for_every_merged_member(tmp_path):
    repo = tmp_path / "repo"
    _init_repo(repo)
    original = _repo_coordinator(tmp_path, repo, "original")
    fence = _ready_repo_task(original)
    assert original.barrier_declare("publish", ["T"])["ok"] is True
    real_connect_one = original._barrier_connect_one

    def merge_then_cut(*args, **kwargs):
        result = real_connect_one(*args, **kwargs)
        assert result["ok"] is True
        raise _CrashCut("barrier finalization was not observed")

    original._barrier_connect_one = merge_then_cut
    original._terminalize_barrier_exception = lambda *_args, **_kwargs: None
    with pytest.raises(_CrashCut):
        original.barrier_arrive(
            "publish", "ag", "T", fence=fence, request_id="barrier-cut"
        )
    assert original.store.barrier_by_name("publish")["state"] == "TRIPPING"
    assert original.store.get_task("T")["state"] == "MERGED"
    assert original.store.get_idem("barrier-cut")["status"] == "INFLIGHT"
    _close(original)

    no_repo = Coordinator(
        db_path=str(tmp_path / "omd.db"), enforce_single_coordinator=False,
        agent_ttl=None,
    )
    assert no_repo.store.barrier_by_name("publish")["state"] == "TRIPPING"
    assert no_repo.store.get_idem("barrier-cut")["status"] == "INFLIGHT"
    _close(no_repo)

    recovered = _repo_coordinator(tmp_path, repo, "recovery")
    assert recovered.store.barrier_by_name("publish")["state"] == "TRIPPED"
    assert recovered.store.get_idem("barrier-cut")["status"] == "DONE"
    replay = recovered.barrier_arrive(
        "publish", "ag", "T", fence=fence, request_id="barrier-cut"
    )
    assert replay["state"] == "TRIPPED"
    assert replay["recovered"] is True and replay["replayed"] is True
    _close(recovered)


def test_stale_finalizer_cannot_consume_successor_idempotency_generation():
    omd = Coordinator(allow_memory_db=True, agent_ttl=None)
    args = ["T", "ag", 7, None, None]
    arg_hash = omd._arg_hash("connect", args)
    with omd.store.tx():
        omd.store.begin_idem("same-request", "ag", "connect", arg_hash, args)
        assert omd.store.bind_idem_operation_exact(
            "same-request", "ag", "connect", arg_hash,
            operation_id="successor", owner_instance="new-owner",
            owner_generation=2,
        )

    stale_response = {
        "ok": True,
        "state": "MERGED",
        "_idempotency_operation": {
            "attempt_id": "old-attempt",
            "owner_instance": "old-owner",
            "owner_generation": 1,
        },
    }
    result = omd._complete_split_idem(
        "same-request", "ag", "connect", args, stale_response
    )

    assert result["fenced_out"] is True
    row = omd.store.get_idem("same-request")
    assert row["status"] == "INFLIGHT"
    assert (row["operation_id"], row["owner_instance"], row["owner_generation"]) == (
        "successor", "new-owner", 2,
    )
    omd.close()


def test_stale_phase_c_cannot_mutate_successor_attempt():
    omd = Coordinator(allow_memory_db=True, agent_ttl=None)
    _ready_db_task(omd)
    args = ["T", None, None, None, None]
    arg_hash = omd._arg_hash("connect", args)
    with omd._cs():
        with omd._idem("phase-c-request", None, "connect", args) as slot:
            slot.defer()
    phase_a = omd._connect_phase_a(
        "T", None, None, request_id="phase-c-request",
        request_agent=None, request_arg_hash=arg_hash,
    )
    assert phase_a["ok"] is True
    old_intent = phase_a["intent"]
    token_id = phase_a["token_id"]

    with omd._cs():
        omd.store.set_task(
            "T", connect_attempt_id="successor",
            connect_owner_instance="new-owner", connect_owner_generation=2,
            connect_token_id=token_id,
        )
        omd.store.set_orbit(
            token_id, operation_id="successor", owner_instance="new-owner",
            owner_generation=2,
        )
        assert omd.store.takeover_idem_operation_exact(
            "phase-c-request", None, "connect", arg_hash,
            operation_id=old_intent["attempt_id"],
            previous_owner=old_intent["owner_instance"],
            previous_generation=old_intent["owner_generation"],
            owner_instance="new-owner", owner_generation=2,
        )
    before_task = omd.store.get_task("T")
    before_token = omd.store.get_orbit(token_id)
    before_idem = omd.store.get_idem("phase-c-request")

    result = omd._connect_phase_c("T", token_id, old_intent, None, None)

    assert result["fenced_out"] is True
    assert omd.store.get_task("T") == before_task
    assert omd.store.get_orbit(token_id) == before_token
    assert omd.store.get_idem("phase-c-request") == before_idem
    omd.close()


def test_public_release_cannot_release_live_merge_token():
    """The public lease API must not expose the internal effect authority."""
    omd = Coordinator(allow_memory_db=True, agent_ttl=None)
    _ready_db_task(omd)

    phase_a = omd._connect_phase_a("T", None, None)
    assert phase_a["ok"] is True
    token_id = phase_a["token_id"]
    token_before = omd.store.get_orbit(token_id)
    task_before = omd.store.get_task("T")

    result = omd.release(
        token_id, token_before["agent_id"], token_before["fence"]
    )

    assert result["ok"] is False
    assert omd.store.get_orbit(token_id) == token_before
    assert omd.store.get_task("T") == task_before
    omd.close()


def test_repo_less_new_request_cannot_overwrite_repo_bound_connecting_attempt(
    tmp_path,
):
    """A missing legacy token must not permit a Git attempt to become DB-only."""
    repo = tmp_path / "repo"
    _init_repo(repo)
    original = _repo_coordinator(tmp_path, repo, "original")
    fence = _ready_repo_task(original)
    old_args = ["T", "ag", fence, None, None]
    old_hash = original._arg_hash("connect", old_args)
    with original._cs():
        with original._idem(
            "old-connect", "ag", "connect", old_args
        ) as slot:
            slot.defer()
    phase_a = original._connect_phase_a(
        "T", "ag", fence,
        request_id="old-connect",
        request_agent="ag",
        request_arg_hash=old_hash,
    )
    assert phase_a["ok"] is True
    old_attempt = phase_a["intent"]["attempt_id"]
    old_token = phase_a["token_id"]
    with original._cs():
        original._release_merge_token_locked(old_token)
    _close(original)

    recovered = Coordinator(
        db_path=str(tmp_path / "omd.db"),
        enforce_single_coordinator=False,
        agent_ttl=None,
    )
    before = recovered.store.get_task("T")
    assert before["state"] == "CONNECTING"
    assert before["connect_repo_bound"] == 1
    assert before["connect_attempt_id"] == old_attempt
    assert recovered.store.get_idem("old-connect")["status"] == "INFLIGHT"
    assert recovered.store.all_held_merge_tokens() == []

    result = recovered.connect(
        "T", "ag", fence, request_id="new-connect"
    )

    after = recovered.store.get_task("T")
    assert result["ok"] is False
    assert after["state"] == "CONNECTING"
    assert after["connect_repo_bound"] == 1
    assert after["connect_attempt_id"] == old_attempt
    assert after["connect_token_id"] == old_token
    assert recovered.store.get_idem("old-connect")["status"] == "INFLIGHT"
    assert recovered.store.get_idem("new-connect") is None
    assert recovered.store.all_held_merge_tokens() == []
    _close(recovered)
