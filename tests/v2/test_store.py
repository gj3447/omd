from __future__ import annotations

import sqlite3
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pytest

from omd_server.v2.errors import ErrorCode
from omd_server.v2.model import (
    Accepted,
    ClaimCommand,
    ClaimStatus,
    CommandEnvelope,
    Principal,
    Rejected,
)
from omd_server.v2.resource import (
    AccessMode,
    CaseMode,
    ClaimSpec,
    RepoPolicy,
    SelectorKind,
    canonicalize_resource,
)
from omd_server.v2.store import RevisionConflict, SQLiteCoordinationStore


DOMAIN = "symposium"
REPO = RepoPolicy(repo_id="omd", case_mode=CaseMode.SENSITIVE)


def resource(path: str):
    return canonicalize_resource(
        domain_id=DOMAIN,
        policy=REPO,
        raw_path=path,
        selector=SelectorKind.EXACT,
    )


def request(request_id: str, path: str, *, agent: str = "agent-a") -> CommandEnvelope:
    return CommandEnvelope.create(
        protocol_version=2,
        domain_id=DOMAIN,
        principal=Principal("client", agent, 1),
        request_id=request_id,
        command=ClaimCommand(
            claims=(ClaimSpec(resource(path), AccessMode.WRITE),),
            lease_ttl_ms=1_000,
            wait_timeout_ms=5_000,
        ),
    )


def make_store(path: Path, **kwargs) -> SQLiteCoordinationStore:
    store = SQLiteCoordinationStore(path, **kwargs)
    store.initialize()
    store.create_domain(domain_id=DOMAIN, repo_policies=(REPO,))
    for agent in ("agent-a", "agent-b"):
        store.register_session(
            domain_id=DOMAIN,
            client_id="client",
            agent_id=agent,
            registered_at_ms=0,
        )
    return store


def table_count(path: Path, table: str) -> int:
    with sqlite3.connect(path) as connection:
        return connection.execute(f"SELECT count(*) FROM {table}").fetchone()[0]


def test_store_requires_file_backed_wal_and_initializes_schema(tmp_path: Path) -> None:
    path = tmp_path / "coordination.db"
    store = make_store(path)

    assert store.journal_mode() == "wal"
    assert store.schema_version() == 3
    snapshot = store.read_domain(DOMAIN)
    assert snapshot.revision == 0
    assert snapshot.state.claims == {}


def test_command_commits_state_events_idempotency_and_outbox_atomically(
    tmp_path: Path,
) -> None:
    path = tmp_path / "coordination.db"
    store = make_store(path)

    receipt = store.execute(request("claim-a", "src/a.py"), now_ms=10)
    snapshot = store.read_domain(DOMAIN)

    assert receipt.revision == 1
    assert isinstance(receipt.result, Accepted)
    assert receipt.result.status is ClaimStatus.ACTIVE
    assert snapshot.revision == 1
    assert snapshot.state.claims[receipt.result.claim_id].status is ClaimStatus.ACTIVE
    assert table_count(path, "domain_events") == len(receipt.events)
    assert table_count(path, "idempotency_keys") == 1
    assert table_count(path, "outbox") == len(receipt.effects) == 2


def test_replay_reads_current_projection_without_new_revision_or_rows(
    tmp_path: Path,
) -> None:
    path = tmp_path / "coordination.db"
    store = make_store(path)
    command = request("same", "src/a.py")
    first = store.execute(command, now_ms=10)
    counts = tuple(
        table_count(path, table)
        for table in ("domain_events", "idempotency_keys", "outbox")
    )

    replay = store.execute(command, now_ms=11)

    assert replay.revision == first.revision
    assert replay.events == ()
    assert replay.effects == ()
    assert isinstance(replay.result, Accepted)
    assert replay.result.replayed is True
    assert counts == tuple(
        table_count(path, table)
        for table in ("domain_events", "idempotency_keys", "outbox")
    )


def test_idempotency_mismatch_is_fail_closed_and_does_not_mutate_store(
    tmp_path: Path,
) -> None:
    path = tmp_path / "coordination.db"
    store = make_store(path)
    store.execute(request("same", "src/a.py"), now_ms=10)

    receipt = store.execute(request("same", "src/b.py"), now_ms=11)

    assert receipt.revision == 1
    assert isinstance(receipt.result, Rejected)
    assert receipt.result.error.code is ErrorCode.IDEMPOTENCY_KEY_REUSE
    assert table_count(path, "idempotency_keys") == 1


def test_expected_revision_is_an_explicit_compare_and_swap(tmp_path: Path) -> None:
    path = tmp_path / "coordination.db"
    store = make_store(path)
    store.execute(request("first", "src/a.py"), now_ms=10, expected_revision=0)

    with pytest.raises(RevisionConflict) as caught:
        store.execute(
            request("stale", "src/b.py", agent="agent-b"),
            now_ms=11,
            expected_revision=0,
        )

    assert (caught.value.expected, caught.value.actual) == (0, 1)
    assert store.read_domain(DOMAIN).revision == 1
    assert table_count(path, "idempotency_keys") == 1


@pytest.mark.parametrize("fault_stage", ["after_idempotency", "after_domain_cas", "after_outbox"])
def test_any_failure_rolls_back_the_entire_command_transaction(
    tmp_path: Path, fault_stage: str
) -> None:
    path = tmp_path / f"{fault_stage}.db"

    def fail(stage: str) -> None:
        if stage == fault_stage:
            raise RuntimeError(f"injected:{stage}")

    store = make_store(path, fault_injector=fail)

    with pytest.raises(RuntimeError, match=f"injected:{fault_stage}"):
        store.execute(request("claim-a", "src/a.py"), now_ms=10)

    reopened = SQLiteCoordinationStore(path)
    snapshot = reopened.read_domain(DOMAIN)
    assert snapshot.revision == 0
    assert snapshot.state.claims == {}
    assert table_count(path, "domain_events") == 0
    assert table_count(path, "idempotency_keys") == 0
    assert table_count(path, "outbox") == 0


def test_two_store_instances_serialize_writers_without_lost_updates(
    tmp_path: Path,
) -> None:
    path = tmp_path / "coordination.db"
    first = make_store(path)
    second = SQLiteCoordinationStore(path)

    def run(store: SQLiteCoordinationStore, suffix: str):
        return store.execute(
            request(f"request-{suffix}", f"src/{suffix}.py", agent=f"agent-{suffix}"),
            now_ms=10,
        )

    with ThreadPoolExecutor(max_workers=2) as pool:
        receipts = tuple(
            pool.map(lambda args: run(*args), ((first, "a"), (second, "b")))
        )

    snapshot = first.read_domain(DOMAIN)
    assert sorted(receipt.revision for receipt in receipts) == [1, 2]
    assert snapshot.revision == 2
    assert len(snapshot.state.claims) == 2
    assert all(record.status is ClaimStatus.ACTIVE for record in snapshot.state.claims.values())


def test_pure_read_does_not_advance_revision_or_dispatch_outbox(tmp_path: Path) -> None:
    path = tmp_path / "coordination.db"
    store = make_store(path)
    store.execute(request("claim-a", "src/a.py"), now_ms=10)
    before = store.read_domain(DOMAIN)
    pending_before = store.pending_outbox(DOMAIN)

    after = store.read_domain(DOMAIN)
    pending_after = store.pending_outbox(DOMAIN)

    assert after == before
    assert pending_after == pending_before


def test_database_constraints_reject_invalid_outbox_state(tmp_path: Path) -> None:
    path = tmp_path / "coordination.db"
    make_store(path)

    with sqlite3.connect(path) as connection, pytest.raises(sqlite3.IntegrityError):
        connection.execute(
            """
            INSERT INTO outbox(
                domain_id, revision, seq, effect_kind, payload_json,
                status, attempts, created_at_ms
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (DOMAIN, 1, 0, "claim_changed", "{}", "invented", 0, 10),
        )
