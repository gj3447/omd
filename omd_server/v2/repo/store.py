"""Durable SQLite projection and event log for repo sagas."""

from __future__ import annotations

import sqlite3
import time
from pathlib import Path

from .contracts import (
    CommitMetadata,
    MutationReservation,
    SagaRecord,
    SagaStatus,
    reservation_claims_digest,
)
from .errors import IdempotencyConflict, StoreCorruptionError, TransitionConflict
from .fence_codec import decode_fence, encode_fence
from .store_schema import SCHEMA, SCHEMA_VERSION


class SQLiteRepoSagaStore:
    def __init__(self, path: Path):
        self.path = Path(path).resolve()
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._initialize()

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(
            self.path,
            timeout=10.0,
            isolation_level=None,
        )
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys = ON")
        connection.execute("PRAGMA busy_timeout = 10000")
        connection.execute("PRAGMA synchronous = FULL")
        return connection

    def _initialize(self) -> None:
        connection = self._connect()
        try:
            mode = connection.execute("PRAGMA journal_mode = WAL").fetchone()[0]
            if str(mode).lower() != "wal":
                raise StoreCorruptionError("repo saga store requires WAL mode")
            connection.execute("PRAGMA synchronous = FULL")
            connection.executescript(SCHEMA)
            row = connection.execute(
                "SELECT value FROM repo_schema_meta WHERE key = 'schema_version'"
            ).fetchone()
            if row is None:
                connection.execute(
                    "INSERT INTO repo_schema_meta(key, value) VALUES('schema_version', ?)",
                    (str(SCHEMA_VERSION),),
                )
            elif row[0] != str(SCHEMA_VERSION):
                raise StoreCorruptionError("unsupported repo saga schema version")
        finally:
            connection.close()
        self.path.chmod(0o600)

    def create_or_replay(self, record: SagaRecord) -> SagaRecord:
        connection = self._connect()
        try:
            connection.execute("BEGIN IMMEDIATE")
            existing = connection.execute(
                """
                SELECT * FROM repo_sagas
                WHERE domain_id = ? AND client_id = ? AND request_id = ?
                """,
                (record.domain_id, record.client_id, record.request_id),
            ).fetchone()
            if existing is not None:
                replay = self._decode(existing)
                self._assert_replay(replay, record)
                connection.commit()
                return replay
            by_operation = connection.execute(
                "SELECT * FROM repo_sagas WHERE operation_id = ?",
                (record.operation_id,),
            ).fetchone()
            if by_operation is not None:
                replay = self._decode(by_operation)
                self._assert_replay(replay, record)
                connection.commit()
                return replay
            connection.execute(
                """
                INSERT INTO repo_sagas(
                    operation_id, domain_id, client_id, request_id, fingerprint,
                    repo_id, repository_identity, claim_id,
                    fence_json, fence_digest, source_oid, read_base_oid,
                    target_ref, expected_target_oid,
                    author_name, author_email, author_date,
                    committer_name, committer_email, committer_date, message,
                    status, revision, created_at_ms, updated_at_ms
                ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1, ?, ?)
                """,
                (
                    record.operation_id,
                    record.domain_id,
                    record.client_id,
                    record.request_id,
                    record.fingerprint,
                    record.repo_id,
                    record.repository_identity,
                    record.claim_id,
                    encode_fence(record.fence),
                    record.fence_digest,
                    record.source_oid,
                    record.read_base_oid,
                    record.target_ref,
                    record.expected_target_oid,
                    record.metadata.author_name,
                    record.metadata.author_email,
                    record.metadata.author_date,
                    record.metadata.committer_name,
                    record.metadata.committer_email,
                    record.metadata.committer_date,
                    record.metadata.message,
                    record.status.value,
                    record.created_at_ms,
                    record.updated_at_ms,
                ),
            )
            self._event(
                connection,
                record.operation_id,
                1,
                "intent_recorded",
                record.status,
                None,
                record.created_at_ms,
            )
            connection.commit()
            return self.load(record.operation_id)
        except Exception:
            if connection.in_transaction:
                connection.rollback()
            raise
        finally:
            connection.close()

    @staticmethod
    def _assert_replay(existing: SagaRecord, incoming: SagaRecord) -> None:
        if (
            existing.operation_id != incoming.operation_id
            or existing.fingerprint != incoming.fingerprint
        ):
            raise IdempotencyConflict(
                "request key or operation ID is already bound to different input"
            )

    def load(self, operation_id: str) -> SagaRecord:
        connection = self._connect()
        try:
            row = connection.execute(
                "SELECT * FROM repo_sagas WHERE operation_id = ?", (operation_id,)
            ).fetchone()
            if row is None:
                raise KeyError(operation_id)
            return self._decode(row)
        finally:
            connection.close()

    def find_request(
        self, domain_id: str, client_id: str, request_id: str
    ) -> SagaRecord | None:
        connection = self._connect()
        try:
            row = connection.execute(
                """
                SELECT * FROM repo_sagas
                WHERE domain_id = ? AND client_id = ? AND request_id = ?
                """,
                (domain_id, client_id, request_id),
            ).fetchone()
            return None if row is None else self._decode(row)
        finally:
            connection.close()

    def recoverable(self) -> tuple[SagaRecord, ...]:
        terminal = tuple(status.value for status in SagaStatus if status.terminal)
        placeholders = ",".join("?" for _ in terminal)
        connection = self._connect()
        try:
            rows = connection.execute(
                f"""
                SELECT * FROM repo_sagas
                WHERE status NOT IN ({placeholders})
                   OR (
                       reservation_id IS NOT NULL
                       AND settled = 0
                       AND status != ?
                   )
                ORDER BY created_at_ms, operation_id
                """,
                (*terminal, SagaStatus.IN_DOUBT.value),
            ).fetchall()
            return tuple(self._decode(row) for row in rows)
        finally:
            connection.close()

    def record_reservation(
        self, operation_id: str, reservation: MutationReservation
    ) -> SagaRecord:
        return self._transition(
            operation_id,
            expected=(SagaStatus.INTENT_DURABLE,),
            status=SagaStatus.RESERVED,
            event="authority_reserved",
            fields={
                "reservation_id": reservation.reservation_id,
                "authority_claims_digest": reservation_claims_digest(
                    reservation.claims
                ),
            },
        )

    def record_inputs_pinned(self, operation_id: str) -> SagaRecord:
        return self._transition(
            operation_id,
            expected=(SagaStatus.RESERVED,),
            status=SagaStatus.INPUTS_PINNED,
            event="inputs_pinned",
        )

    def record_candidate(
        self, operation_id: str, tree_oid: str, candidate_oid: str
    ) -> SagaRecord:
        return self._transition(
            operation_id,
            expected=(SagaStatus.INPUTS_PINNED,),
            status=SagaStatus.CANDIDATE_READY,
            event="candidate_recorded",
            fields={"tree_oid": tree_oid, "candidate_oid": candidate_oid},
        )

    def mark_publishing(self, operation_id: str) -> SagaRecord:
        return self._transition(
            operation_id,
            expected=(SagaStatus.CANDIDATE_READY,),
            status=SagaStatus.PUBLISHING,
            event="publication_started",
        )

    def record_applied(self, operation_id: str, receipt_kind: str) -> SagaRecord:
        return self._transition(
            operation_id,
            expected=(SagaStatus.PUBLISHING,),
            status=SagaStatus.APPLIED,
            event="target_applied",
            fields={"receipt_kind": receipt_kind},
        )

    def record_receipted(self, operation_id: str) -> SagaRecord:
        try:
            return self._transition(
                operation_id,
                expected=(SagaStatus.APPLIED,),
                status=SagaStatus.RECEIPTED,
                event="authority_settled",
                fields={"settled": 1},
            )
        except TransitionConflict:
            current = self.load(operation_id)
            if current.status is SagaStatus.RECEIPTED:
                return current
            raise

    def record_terminal(
        self,
        operation_id: str,
        *,
        expected: tuple[SagaStatus, ...],
        status: SagaStatus,
        error_code: str,
        detail: str,
    ) -> SagaRecord:
        if not status.terminal or status is SagaStatus.RECEIPTED:
            raise ValueError("record_terminal requires a rejection/quarantine status")
        bounded_detail = detail[:2000]
        return self._transition(
            operation_id,
            expected=expected,
            status=status,
            event="saga_terminal",
            detail=bounded_detail,
            fields={"error_code": error_code, "error_detail": bounded_detail},
        )

    def record_terminal_settled(self, operation_id: str) -> SagaRecord:
        record = self.load(operation_id)
        if not record.status.terminal or record.status is SagaStatus.RECEIPTED:
            raise TransitionConflict("only terminal rejection can be settled in place")
        if record.settled:
            return record
        try:
            return self._transition(
                operation_id,
                expected=(record.status,),
                status=record.status,
                event="authority_settled",
                fields={"settled": 1},
            )
        except TransitionConflict:
            current = self.load(operation_id)
            if current.status is record.status and current.settled:
                return current
            raise

    def _transition(
        self,
        operation_id: str,
        *,
        expected: tuple[SagaStatus, ...],
        status: SagaStatus,
        event: str,
        fields: dict[str, object] | None = None,
        detail: str | None = None,
    ) -> SagaRecord:
        allowed = {
            "reservation_id",
            "authority_claims_digest",
            "tree_oid",
            "candidate_oid",
            "receipt_kind",
            "error_code",
            "error_detail",
            "settled",
        }
        updates = fields or {}
        if not set(updates).issubset(allowed):
            raise ValueError("unsupported saga projection field")
        connection = self._connect()
        try:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute(
                "SELECT * FROM repo_sagas WHERE operation_id = ?", (operation_id,)
            ).fetchone()
            if row is None:
                raise KeyError(operation_id)
            current = self._decode(row)
            now = max(
                int(time.time_ns() // 1_000_000),
                current.created_at_ms,
                current.updated_at_ms,
            )
            if current.status not in expected:
                raise TransitionConflict(
                    f"expected {[item.value for item in expected]}, got {current.status.value}"
                )
            revision = current.revision + 1
            assignments = ["status = ?", "revision = ?", "updated_at_ms = ?"]
            values: list[object] = [status.value, revision, now]
            for key, value in updates.items():
                assignments.append(f"{key} = ?")
                values.append(value)
            values.extend((operation_id, current.revision))
            cursor = connection.execute(
                f"""
                UPDATE repo_sagas SET {', '.join(assignments)}
                WHERE operation_id = ? AND revision = ?
                """,
                values,
            )
            if cursor.rowcount != 1:
                raise TransitionConflict("saga revision changed concurrently")
            self._event(
                connection,
                operation_id,
                revision,
                event,
                status,
                detail,
                now,
            )
            connection.commit()
            return self.load(operation_id)
        except Exception:
            if connection.in_transaction:
                connection.rollback()
            raise
        finally:
            connection.close()

    @staticmethod
    def _event(
        connection: sqlite3.Connection,
        operation_id: str,
        revision: int,
        event: str,
        status: SagaStatus,
        detail: str | None,
        now: int,
    ) -> None:
        connection.execute(
            """
            INSERT INTO repo_saga_events(
                operation_id, revision, event_kind, status, detail, created_at_ms
            ) VALUES(?, ?, ?, ?, ?, ?)
            """,
            (operation_id, revision, event, status.value, detail, now),
        )

    @staticmethod
    def _decode(row: sqlite3.Row) -> SagaRecord:
        try:
            status = SagaStatus(row["status"])
            fence = decode_fence(row["fence_json"])
            if fence.vector_digest != row["fence_digest"]:
                raise ValueError("fence digest column mismatch")
            metadata = CommitMetadata(
                author_name=row["author_name"],
                author_email=row["author_email"],
                author_date=row["author_date"],
                committer_name=row["committer_name"],
                committer_email=row["committer_email"],
                committer_date=row["committer_date"],
                message=row["message"],
            )
            return SagaRecord(
                operation_id=row["operation_id"],
                domain_id=row["domain_id"],
                client_id=row["client_id"],
                request_id=row["request_id"],
                fingerprint=row["fingerprint"],
                repo_id=row["repo_id"],
                repository_identity=row["repository_identity"],
                claim_id=row["claim_id"],
                fence=fence,
                fence_digest=row["fence_digest"],
                source_oid=row["source_oid"],
                read_base_oid=row["read_base_oid"],
                target_ref=row["target_ref"],
                expected_target_oid=row["expected_target_oid"],
                metadata=metadata,
                status=status,
                revision=row["revision"],
                reservation_id=row["reservation_id"],
                authority_claims_digest=row["authority_claims_digest"],
                tree_oid=row["tree_oid"],
                candidate_oid=row["candidate_oid"],
                receipt_kind=row["receipt_kind"],
                error_code=row["error_code"],
                error_detail=row["error_detail"],
                settled=bool(row["settled"]),
                created_at_ms=row["created_at_ms"],
                updated_at_ms=row["updated_at_ms"],
            )
        except (KeyError, TypeError, ValueError) as exc:
            raise StoreCorruptionError("invalid repo saga projection") from exc
