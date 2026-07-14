"""Local-only SQLite adapter for the OMD v2 functional kernel.

Each operation opens its own connection.  Mutation commands use
``BEGIN IMMEDIATE`` and a revision compare-and-swap.  Domain events,
idempotency bindings, the aggregate projection, and outbox effects commit in
one transaction; no external effect runs here.

# KG: finding-tpa-tcw-omd-core-engine-20260714
"""

from __future__ import annotations

import json
import sqlite3
from contextlib import contextmanager
from pathlib import Path
from typing import Callable, Iterator

from .codec import (
    CODEC_VERSION,
    decode_effect,
    decode_error,
    decode_state,
    encode_effect,
    encode_error,
    encode_event,
    encode_state,
)
from .errors import InvariantViolation
from .kernel import assert_invariants, decide, evolve
from .model import (
    CommandEnvelope,
    Decision,
    DomainEvent,
    DomainState,
    IdempotencyRecord,
    IdempotencyRecorded,
)
from .resource import RepoPolicy
from .session_store import SessionRegistryMixin
from .sqlite_schema import SCHEMA, SCHEMA_VERSION
from .store_types import (
    DomainConfigurationConflict,
    DomainNotFound,
    DomainSnapshot,
    ExecutionReceipt,
    FaultInjector,
    JournalModeError,
    OutboxRecord,
    RevisionConflict,
    SchemaVersionError,
    SQLiteVersionError,
    StoreCorruptionError,
    StoreError,
)


class SQLiteCoordinationStore(SessionRegistryMixin):
    """One-file, one-host persistence for a coordination domain."""

    def __init__(
        self,
        path: str | Path,
        *,
        busy_timeout_ms: int = 5_000,
        fault_injector: FaultInjector | None = None,
    ) -> None:
        raw = str(path)
        if raw == ":memory:" or raw.startswith("file:"):
            raise ValueError("OMD v2 requires a file-backed local SQLite database")
        self.path = Path(path).expanduser().resolve()
        self.busy_timeout_ms = busy_timeout_ms
        self._fault_injector = fault_injector

    def _fault(self, stage: str) -> None:
        if self._fault_injector is not None:
            self._fault_injector(stage)

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(
            self.path,
            timeout=self.busy_timeout_ms / 1_000,
            isolation_level=None,
            check_same_thread=False,
        )
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys = ON")
        connection.execute(f"PRAGMA busy_timeout = {int(self.busy_timeout_ms)}")
        connection.execute("PRAGMA synchronous = FULL")
        return connection

    @contextmanager
    def _transaction(
        self, connection: sqlite3.Connection, *, write: bool
    ) -> Iterator[None]:
        connection.execute("BEGIN IMMEDIATE" if write else "BEGIN")
        try:
            yield
        except BaseException:
            connection.rollback()
            raise
        else:
            connection.commit()

    def initialize(self) -> None:
        if sqlite3.sqlite_version_info < (3, 37, 0):
            raise SQLiteVersionError(
                "OMD v2 requires SQLite >= 3.37 for STRICT tables; "
                f"found {sqlite3.sqlite_version}"
            )
        self.path.parent.mkdir(parents=True, exist_ok=True)
        connection = self._connect()
        try:
            mode = connection.execute("PRAGMA journal_mode = WAL").fetchone()[0]
            if str(mode).lower() != "wal":
                raise JournalModeError(f"expected WAL, got {mode}")
            connection.executescript(SCHEMA)
            connection.execute(
                "INSERT OR IGNORE INTO schema_meta(key, value) VALUES('schema_version', ?)",
                (str(SCHEMA_VERSION),),
            )
            actual = int(
                connection.execute(
                    "SELECT value FROM schema_meta WHERE key='schema_version'"
                ).fetchone()[0]
            )
            if actual != SCHEMA_VERSION:
                raise SchemaVersionError(
                    f"expected schema {SCHEMA_VERSION}, found {actual}"
                )
        finally:
            connection.close()

    def journal_mode(self) -> str:
        connection = self._connect()
        try:
            return str(connection.execute("PRAGMA journal_mode").fetchone()[0]).lower()
        finally:
            connection.close()

    def schema_version(self) -> int:
        connection = self._connect()
        try:
            row = connection.execute(
                "SELECT value FROM schema_meta WHERE key='schema_version'"
            ).fetchone()
            if row is None:
                raise SchemaVersionError("schema is not initialized")
            return int(row[0])
        finally:
            connection.close()

    def create_domain(
        self,
        *,
        domain_id: str,
        repo_policies: tuple[RepoPolicy, ...],
        created_at_ms: int = 0,
    ) -> DomainSnapshot:
        candidate = DomainState.empty(
            domain_id=domain_id, repo_policies=repo_policies
        )
        connection = self._connect()
        try:
            with self._transaction(connection, write=True):
                row = connection.execute(
                    "SELECT revision, state_json FROM domains WHERE domain_id=?",
                    (domain_id,),
                ).fetchone()
                if row is not None:
                    current = self._load_domain(connection, domain_id)
                    if current.state.repo_policies != candidate.repo_policies:
                        raise DomainConfigurationConflict(domain_id)
                    return current
                connection.execute(
                    """
                    INSERT INTO domains(
                        domain_id, revision, codec_version, state_json,
                        created_at_ms, updated_at_ms
                    ) VALUES (?, 0, ?, ?, ?, ?)
                    """,
                    (
                        domain_id,
                        CODEC_VERSION,
                        encode_state(candidate),
                        created_at_ms,
                        created_at_ms,
                    ),
                )
                return DomainSnapshot(candidate, 0)
        finally:
            connection.close()

    def _load_idempotency(
        self, connection: sqlite3.Connection, domain_id: str
    ) -> dict[tuple[str, str], IdempotencyRecord]:
        rows = connection.execute(
            """
            SELECT client_id, request_id, fingerprint, operation_id,
                   claim_id, frozen_error_json
            FROM idempotency_keys
            WHERE domain_id=?
            ORDER BY client_id, request_id
            """,
            (domain_id,),
        )
        return {
            (str(row["client_id"]), str(row["request_id"])): IdempotencyRecord(
                fingerprint=str(row["fingerprint"]),
                operation_id=str(row["operation_id"]),
                claim_id=(None if row["claim_id"] is None else str(row["claim_id"])),
                frozen_error=decode_error(row["frozen_error_json"]),
            )
            for row in rows
        }

    def _load_sessions(
        self, connection: sqlite3.Connection, domain_id: str
    ) -> dict[tuple[str, str], int]:
        rows = connection.execute(
            """
            SELECT client_id, agent_id, current_epoch
            FROM sessions
            WHERE domain_id=?
            ORDER BY client_id, agent_id
            """,
            (domain_id,),
        )
        return {
            (str(row["client_id"]), str(row["agent_id"])): int(row["current_epoch"])
            for row in rows
        }

    def _load_event_idempotency(
        self, connection: sqlite3.Connection, domain_id: str
    ) -> dict[tuple[str, str], IdempotencyRecord]:
        rows = connection.execute(
            """
            SELECT payload_json
            FROM domain_events
            WHERE domain_id=? AND event_kind='idempotency_recorded'
            ORDER BY revision, seq
            """,
            (domain_id,),
        )
        records: dict[tuple[str, str], IdempotencyRecord] = {}
        for row in rows:
            payload = json.loads(str(row["payload_json"]))
            key = tuple(str(item) for item in payload["key"])
            if len(key) != 2:
                raise StoreCorruptionError("invalid idempotency event key")
            value = payload["record"]
            record = IdempotencyRecord(
                fingerprint=str(value["fingerprint"]),
                operation_id=str(value["operation_id"]),
                claim_id=(
                    None if value["claim_id"] is None else str(value["claim_id"])
                ),
                frozen_error=decode_error(value["frozen_error"]),
            )
            typed_key = (key[0], key[1])
            if typed_key in records:
                raise StoreCorruptionError("duplicate idempotency event key")
            records[typed_key] = record
        return records

    def _load_domain(
        self, connection: sqlite3.Connection, domain_id: str
    ) -> DomainSnapshot:
        row = connection.execute(
            "SELECT revision, codec_version, state_json FROM domains WHERE domain_id=?",
            (domain_id,),
        ).fetchone()
        if row is None:
            raise DomainNotFound(domain_id)
        try:
            if int(row["codec_version"]) != CODEC_VERSION:
                raise StoreCorruptionError(
                    f"domain {domain_id!r} has codec {row['codec_version']}"
                )
            idempotency = self._load_idempotency(connection, domain_id)
            event_idempotency = self._load_event_idempotency(
                connection, domain_id
            )
            if event_idempotency != idempotency:
                raise StoreCorruptionError(
                    f"event/idempotency projection mismatch for {domain_id!r}"
                )
            sessions = self._load_sessions(connection, domain_id)
            state = decode_state(
                str(row["state_json"]),
                idempotency=idempotency,
                session_epochs=sessions,
            )
            if state.domain_id != domain_id:
                raise StoreCorruptionError(
                    f"domain identity mismatch: row={domain_id!r}, state={state.domain_id!r}"
                )
            assert_invariants(state)
        except StoreCorruptionError:
            raise
        except (InvariantViolation, KeyError, TypeError, ValueError) as exc:
            raise StoreCorruptionError(
                f"corrupt projection for domain {domain_id!r}: {exc}"
            ) from exc
        return DomainSnapshot(state, int(row["revision"]))

    def read_domain(self, domain_id: str) -> DomainSnapshot:
        connection = self._connect()
        try:
            with self._transaction(connection, write=False):
                return self._load_domain(connection, domain_id)
        finally:
            connection.close()

    def _insert_idempotency(
        self,
        connection: sqlite3.Connection,
        domain_id: str,
        revision: int,
        events: tuple[DomainEvent, ...],
    ) -> None:
        for event in events:
            if not isinstance(event, IdempotencyRecorded):
                continue
            connection.execute(
                """
                INSERT INTO idempotency_keys(
                    domain_id, client_id, request_id, fingerprint,
                    operation_id, claim_id, frozen_error_json, recorded_revision
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    domain_id,
                    event.key[0],
                    event.key[1],
                    event.record.fingerprint,
                    event.record.operation_id,
                    event.record.claim_id,
                    encode_error(event.record.frozen_error),
                    revision,
                ),
            )

    def execute(
        self,
        request: CommandEnvelope,
        *,
        now_ms: int | None = None,
        clock_ms: Callable[[], int] | None = None,
        expected_revision: int | None = None,
    ) -> ExecutionReceipt:
        if (now_ms is None) == (clock_ms is None):
            raise ValueError("provide exactly one of now_ms or clock_ms")
        connection = self._connect()
        try:
            with self._transaction(connection, write=True):
                snapshot = self._load_domain(connection, request.domain_id)
                if (
                    expected_revision is not None
                    and expected_revision != snapshot.revision
                ):
                    raise RevisionConflict(expected_revision, snapshot.revision)

                sampled_now_ms = clock_ms() if clock_ms is not None else now_ms
                if type(sampled_now_ms) is not int or sampled_now_ms < 0:
                    raise ValueError("clock must return a nonnegative integer")
                # The sample occurs after BEGIN IMMEDIATE acquired the writer
                # lock. Clamp wall-clock rollback to the last committed time so
                # lock order and logical time can never disagree.
                resolved_now_ms = max(snapshot.state.last_now_ms, sampled_now_ms)

                decision: Decision = decide(
                    snapshot.state, request, resolved_now_ms
                )
                if not decision.events:
                    return ExecutionReceipt(
                        decision.result,
                        snapshot.revision,
                        snapshot.state,
                        decision.events,
                        decision.effects,
                    )

                revision = snapshot.revision + 1
                next_state = evolve(snapshot.state, decision.events)
                assert_invariants(next_state)
                for seq, event in enumerate(decision.events):
                    kind, payload = encode_event(event)
                    connection.execute(
                        """
                        INSERT INTO domain_events(
                            domain_id, revision, seq, event_kind, payload_json
                        ) VALUES (?, ?, ?, ?, ?)
                        """,
                        (request.domain_id, revision, seq, kind, payload),
                    )
                self._insert_idempotency(
                    connection, request.domain_id, revision, decision.events
                )
                self._fault("after_idempotency")

                updated = connection.execute(
                    """
                    UPDATE domains
                    SET revision=?, state_json=?, updated_at_ms=?
                    WHERE domain_id=? AND revision=?
                    RETURNING revision
                    """,
                    (
                        revision,
                        encode_state(next_state),
                        resolved_now_ms,
                        request.domain_id,
                        snapshot.revision,
                    ),
                ).fetchone()
                if updated is None:
                    actual = connection.execute(
                        "SELECT revision FROM domains WHERE domain_id=?",
                        (request.domain_id,),
                    ).fetchone()
                    raise RevisionConflict(
                        snapshot.revision,
                        -1 if actual is None else int(actual[0]),
                    )
                self._fault("after_domain_cas")

                for seq, effect in enumerate(decision.effects):
                    kind, payload = encode_effect(effect)
                    connection.execute(
                        """
                        INSERT INTO outbox(
                            domain_id, revision, seq, effect_kind, payload_json,
                            status, attempts, created_at_ms
                        ) VALUES (?, ?, ?, ?, ?, 'pending', 0, ?)
                        """,
                        (
                            request.domain_id,
                            revision,
                            seq,
                            kind,
                            payload,
                            resolved_now_ms,
                        ),
                    )
                self._fault("after_outbox")
                receipt = ExecutionReceipt(
                    decision.result,
                    revision,
                    next_state,
                    decision.events,
                    decision.effects,
                )
            return receipt
        finally:
            connection.close()

    def pending_outbox(
        self, domain_id: str, *, limit: int = 100
    ) -> tuple[OutboxRecord, ...]:
        connection = self._connect()
        try:
            with self._transaction(connection, write=False):
                rows = connection.execute(
                    """
                    SELECT domain_id, revision, seq, effect_kind, payload_json,
                           attempts, created_at_ms
                    FROM outbox
                    WHERE domain_id=? AND status='pending'
                    ORDER BY revision, seq
                    LIMIT ?
                    """,
                    (domain_id, limit),
                ).fetchall()
                return tuple(
                    OutboxRecord(
                        domain_id=str(row["domain_id"]),
                        revision=int(row["revision"]),
                        seq=int(row["seq"]),
                        effect=decode_effect(
                            str(row["effect_kind"]), str(row["payload_json"])
                        ),
                        attempts=int(row["attempts"]),
                        created_at_ms=int(row["created_at_ms"]),
                    )
                    for row in rows
                )
        finally:
            connection.close()
