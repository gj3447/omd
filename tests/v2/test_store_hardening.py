from __future__ import annotations

import json
import sqlite3
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from threading import Event

import pytest

from omd_server.v2.errors import ErrorCode
from omd_server.v2.model import (
    Accepted,
    ClaimCommand,
    CommandEnvelope,
    Principal,
    Rejected,
)
from omd_server.v2.resource import (
    AccessMode,
    ClaimSpec,
    RepoPolicy,
    SelectorKind,
    canonicalize_resource,
)
from omd_server.v2.store import (
    SQLiteCoordinationStore,
    SQLiteVersionError,
    StoreCorruptionError,
)


DOMAIN = "hardening"
REPO = RepoPolicy("repo")


def make_store(path: Path):
    store = SQLiteCoordinationStore(path)
    store.initialize()
    store.create_domain(domain_id=DOMAIN, repo_policies=(REPO,))
    principal = store.register_session(
        domain_id=DOMAIN,
        client_id="client",
        agent_id="agent",
        registered_at_ms=1,
    )
    return store, principal


def test_initialize_rejects_sqlite_without_strict_table_support(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(sqlite3, "sqlite_version_info", (3, 36, 0))
    store = SQLiteCoordinationStore(tmp_path / "old-sqlite.db")

    with pytest.raises(SQLiteVersionError, match="SQLite >= 3.37"):
        store.initialize()


def request(principal: Principal, request_id: str, path: str):
    resource = canonicalize_resource(
        domain_id=DOMAIN,
        policy=REPO,
        raw_path=path,
        selector=SelectorKind.EXACT,
    )
    return CommandEnvelope.create(
        protocol_version=2,
        domain_id=DOMAIN,
        principal=principal,
        request_id=request_id,
        command=ClaimCommand(
            (ClaimSpec(resource, AccessMode.WRITE),),
            lease_ttl_ms=1_000,
            wait_timeout_ms=5_000,
        ),
    )


def claim_set_request(
    principal: Principal,
    request_id: str,
    paths: tuple[str, ...],
    *,
    wait_timeout_ms: int = 5_000,
) -> CommandEnvelope:
    return CommandEnvelope.create(
        protocol_version=2,
        domain_id=DOMAIN,
        principal=principal,
        request_id=request_id,
        command=ClaimCommand(
            tuple(
                ClaimSpec(
                    canonicalize_resource(
                        domain_id=DOMAIN,
                        policy=REPO,
                        raw_path=path,
                        selector=SelectorKind.EXACT,
                    ),
                    AccessMode.WRITE,
                )
                for path in paths
            ),
            lease_ttl_ms=1_000,
            wait_timeout_ms=wait_timeout_ms,
        ),
    )
def test_session_epoch_is_monotonic_and_stale_epoch_is_rejected(
    tmp_path: Path,
) -> None:
    store, first = make_store(tmp_path / "sessions.db")
    second = store.register_session(
        domain_id=DOMAIN,
        client_id=first.client_id,
        agent_id=first.agent_id,
        registered_at_ms=2,
    )

    stale = store.execute(request(first, "stale", "a.py"), now_ms=10)
    fresh = store.execute(request(second, "fresh", "a.py"), now_ms=10)

    assert second.session_epoch == first.session_epoch + 1
    assert isinstance(stale.result, Rejected)
    assert stale.result.error.code is ErrorCode.STALE_SESSION
    assert isinstance(fresh.result, Accepted)


def test_session_rollover_atomically_fences_its_pending_claims(
    tmp_path: Path,
) -> None:
    store, holder = make_store(tmp_path / "pending-rollover.db")
    worker = store.register_session(
        domain_id=DOMAIN,
        client_id="client",
        agent_id="worker",
        registered_at_ms=1,
    )
    later = store.register_session(
        domain_id=DOMAIN,
        client_id="client",
        agent_id="later",
        registered_at_ms=1,
    )
    store.execute(request(holder, "holder", "x.py"), now_ms=10)
    pending = store.execute(
        claim_set_request(worker, "pending", ("x.py", "y.py")), now_ms=11
    )
    queued = store.execute(request(later, "queued", "y.py"), now_ms=12)
    assert isinstance(pending.result, Accepted)
    assert pending.result.status.value == "pending"
    assert isinstance(queued.result, Accepted)
    assert queued.result.status.value == "pending"

    replacement = store.register_session(
        domain_id=DOMAIN,
        client_id="client",
        agent_id="worker",
        registered_at_ms=13,
    )
    snapshot = store.read_domain(DOMAIN)

    assert replacement.session_epoch == worker.session_epoch + 1
    assert snapshot.state.claims[pending.result.claim_id].status.value == "fenced"
    assert snapshot.state.claims[queued.result.claim_id].status.value == "active"
    assert snapshot.revision == queued.revision + 1


def test_session_rollover_samples_time_after_writer_lock_and_times_out_waiter(
    tmp_path: Path,
) -> None:
    path = tmp_path / "pending-rollover-clock.db"
    store, holder = make_store(path)
    stale = store.register_session(
        domain_id=DOMAIN,
        client_id="client",
        agent_id="stale-worker",
        registered_at_ms=1,
    )
    later = store.register_session(
        domain_id=DOMAIN,
        client_id="client",
        agent_id="later-worker",
        registered_at_ms=1,
    )
    store.execute(request(holder, "holder", "x.py"), now_ms=10)
    stale_pending = store.execute(
        claim_set_request(stale, "stale-pending", ("x.py", "y.py")),
        now_ms=11,
    )
    later_pending = store.execute(
        claim_set_request(
            later, "later-pending", ("y.py",), wait_timeout_ms=50
        ),
        now_ms=12,
    )
    assert stale_pending.result.status.value == "pending"
    assert later_pending.result.status.value == "pending"

    writer = sqlite3.connect(path, isolation_level=None)
    writer.execute("PRAGMA busy_timeout = 5000")
    writer.execute("BEGIN IMMEDIATE")
    clock_called = Event()
    writer_released = Event()

    def post_lock_clock() -> int:
        clock_called.set()
        assert writer_released.is_set()
        return 100

    with ThreadPoolExecutor(max_workers=1) as pool:
        future = pool.submit(
            store.register_session,
            domain_id=DOMAIN,
            client_id=stale.client_id,
            agent_id=stale.agent_id,
            clock_ms=post_lock_clock,
        )
        sampled_while_locked = clock_called.wait(timeout=0.1)
        writer_released.set()
        writer.commit()
        replacement = future.result(timeout=5)
    writer.close()

    snapshot = store.read_domain(DOMAIN)
    assert sampled_while_locked is False
    assert replacement.session_epoch == stale.session_epoch + 1
    assert snapshot.state.claims[stale_pending.result.claim_id].status.value == "fenced"
    assert snapshot.state.claims[later_pending.result.claim_id].status.value == "timed_out"


def test_store_resolves_wall_clock_after_lock_and_never_regresses(
    tmp_path: Path,
) -> None:
    store, principal = make_store(tmp_path / "clock.db")
    first = store.execute(request(principal, "first", "a.py"), now_ms=16)
    second = store.execute(request(principal, "second", "b.py"), now_ms=15)

    assert isinstance(first.result, Accepted)
    assert isinstance(second.result, Accepted)
    assert store.read_domain(DOMAIN).state.last_now_ms == 16


def test_domain_identity_corruption_is_quarantined(tmp_path: Path) -> None:
    path = tmp_path / "domain-corrupt.db"
    store, _ = make_store(path)
    with sqlite3.connect(path) as connection:
        payload = json.loads(
            connection.execute(
                "SELECT state_json FROM domains WHERE domain_id=?", (DOMAIN,)
            ).fetchone()[0]
        )
        payload["domain_id"] = "other"
        connection.execute(
            "UPDATE domains SET state_json=? WHERE domain_id=?",
            (json.dumps(payload), DOMAIN),
        )

    with pytest.raises(StoreCorruptionError, match="domain identity mismatch"):
        store.read_domain(DOMAIN)


def test_missing_normalized_idempotency_binding_is_quarantined(
    tmp_path: Path,
) -> None:
    path = tmp_path / "idem-corrupt.db"
    store, principal = make_store(path)
    store.execute(request(principal, "claim", "a.py"), now_ms=10)
    with sqlite3.connect(path) as connection:
        connection.execute("DELETE FROM idempotency_keys")

    with pytest.raises(StoreCorruptionError, match="idempotency"):
        store.read_domain(DOMAIN)


@pytest.mark.parametrize(
    ("statement", "parameters"),
    [
        (
            "UPDATE idempotency_keys SET request_id=? WHERE domain_id=?",
            ("renamed", DOMAIN),
        ),
        (
            "UPDATE idempotency_keys SET fingerprint=? WHERE domain_id=?",
            ("0" * 64, DOMAIN),
        ),
        (
            "UPDATE idempotency_keys SET frozen_error_json=? WHERE domain_id=?",
            ('{"code":"invalid_ttl","details":[]}', DOMAIN),
        ),
    ],
)
def test_tampered_idempotency_projection_is_cross_checked_against_events(
    tmp_path: Path, statement: str, parameters: tuple[str, str]
) -> None:
    path = tmp_path / "idem-tamper.db"
    store, principal = make_store(path)
    store.execute(request(principal, "claim", "a.py"), now_ms=10)
    with sqlite3.connect(path) as connection:
        connection.execute(statement, parameters)

    with pytest.raises(StoreCorruptionError, match="event/idempotency"):
        store.read_domain(DOMAIN)
