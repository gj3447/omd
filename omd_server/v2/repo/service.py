"""Imperative repo-saga shell around SQLite, authority, and Git plumbing."""

from __future__ import annotations

import time
from typing import Callable

from .contracts import (
    AuthorityRequest,
    IntegrateRequest,
    MutationAuthority,
    MutationReservation,
    RegisteredRepository,
    RepositoryRegistry,
    SagaRecord,
    SagaStatus,
    deterministic_commit_metadata,
    request_fingerprint,
)
from .errors import (
    AuthorityRejected,
    GitExecutionError,
    IdempotencyConflict,
    MergeConflict,
    PathPolicyError,
    ReadSetStale,
    RepoConfigurationError,
    WriteSetViolation,
)
from .git import GitPlumbing
from .policy import audit_reads, audit_writes, validate_reservation
from .repository import record_matches_repository, repository_drift_error
from .settlement import SagaSettlementMixin
from .store import SQLiteRepoSagaStore
from .validation import validate_integrate_request
from .worker import worker_lock


class RepoSagaService(SagaSettlementMixin):
    """A single-worker engine library; it intentionally exposes no transport."""

    def __init__(
        self,
        *,
        registry: RepositoryRegistry,
        store: SQLiteRepoSagaStore,
        authority: MutationAuthority,
        fault_injector: Callable[[str], None] | None = None,
        clock: Callable[[], float] = time.time,
    ):
        if authority is None:  # type: ignore[comparison-overlap]
            raise RepoConfigurationError("mutation authority is mandatory")
        self.registry = registry
        self.store = store
        self.authority = authority
        self._fault = fault_injector or (lambda _point: None)
        self._clock = clock

    def integrate(self, request: IntegrateRequest) -> SagaRecord:
        repository = validate_integrate_request(request, self.registry)
        try:
            with worker_lock(repository):
                record = self._prepare_locked(request, repository)
                if record.status in {
                    SagaStatus.CANDIDATE_READY,
                    SagaStatus.PUBLISHING,
                    SagaStatus.APPLIED,
                }:
                    return self._publish_locked(record, repository)
                if record.status.terminal:
                    return self._settle_terminal(record)
                return record
        except RepoConfigurationError as exc:
            return self._quarantine_existing(request.operation_id, exc)

    def prepare(self, request: IntegrateRequest) -> SagaRecord:
        repository = validate_integrate_request(request, self.registry)
        try:
            with worker_lock(repository):
                return self._prepare_locked(request, repository)
        except RepoConfigurationError as exc:
            return self._quarantine_existing(request.operation_id, exc)

    def publish(self, operation_id: str) -> SagaRecord:
        record = self.store.load(operation_id)
        if record.status.terminal:
            return self._settle_terminal(record)
        if record.status is SagaStatus.APPLIED:
            return self._settle_applied(record)
        try:
            repository = self.registry.get(record.repo_id)
            with worker_lock(repository):
                return self._publish_locked(self.store.load(operation_id), repository)
        except RepoConfigurationError as exc:
            return self._quarantine_existing(operation_id, exc)

    def recover_all(self) -> tuple[SagaRecord, ...]:
        recovered: list[SagaRecord] = []
        for pending in self.store.recoverable():
            record = self.store.load(pending.operation_id)
            if record.status.terminal:
                recovered.append(self._settle_terminal(record))
                continue
            if record.status is SagaStatus.APPLIED:
                recovered.append(self._settle_applied(record))
                continue
            try:
                repository = self.registry.get(record.repo_id)
                with worker_lock(repository):
                    if record.status in {
                        SagaStatus.INTENT_DURABLE,
                        SagaStatus.RESERVED,
                        SagaStatus.INPUTS_PINNED,
                    }:
                        prepared = self._drive_prepare(record, repository)
                        if prepared.status is SagaStatus.CANDIDATE_READY:
                            prepared = self._publish_locked(prepared, repository)
                        recovered.append(prepared)
                    else:
                        recovered.append(self._publish_locked(record, repository))
            except RepoConfigurationError as exc:
                recovered.append(self._quarantine_existing(record.operation_id, exc))
        return tuple(recovered)

    def _prepare_locked(
        self, request: IntegrateRequest, repository: RegisteredRepository
    ) -> SagaRecord:
        fingerprint = request_fingerprint(request)
        replay = self.store.find_request(
            request.domain_id, request.client_id, request.request_id
        )
        if replay is not None:
            if replay.operation_id != request.operation_id or replay.fingerprint != fingerprint:
                raise IdempotencyConflict("request key is bound to different repo input")
            if not replay.status.terminal and not record_matches_repository(
                replay, repository
            ):
                return self._quarantine_existing(
                    replay.operation_id, repository_drift_error(replay, repository)
                )
            if replay.status in {
                SagaStatus.INTENT_DURABLE,
                SagaStatus.RESERVED,
                SagaStatus.INPUTS_PINNED,
            }:
                return self._drive_prepare(replay, repository)
            return replay
        try:
            by_operation = self.store.load(request.operation_id)
        except KeyError:
            by_operation = None
        if by_operation is not None:
            if by_operation.fingerprint != fingerprint:
                raise IdempotencyConflict("operation ID is bound to different repo input")
            if not by_operation.status.terminal and not record_matches_repository(
                by_operation, repository
            ):
                return self._quarantine_existing(
                    by_operation.operation_id,
                    repository_drift_error(by_operation, repository),
                )
            return by_operation

        git = GitPlumbing(repository)
        git.verify_object(request.source_oid, "commit")
        git.verify_object(request.read_base_oid, "commit")
        expected_target = git.read_target()
        created = max(0, int(self._clock() * 1000))
        metadata = deterministic_commit_metadata(
            request.operation_id, request.source_oid, expected_target, created
        )
        record = SagaRecord(
            operation_id=request.operation_id,
            domain_id=request.domain_id,
            client_id=request.client_id,
            request_id=request.request_id,
            fingerprint=fingerprint,
            repo_id=request.repo_id,
            repository_identity=repository.identity_digest,
            claim_id=request.claim_id,
            fence=request.fence,
            fence_digest=request.fence.vector_digest,
            source_oid=request.source_oid,
            read_base_oid=request.read_base_oid,
            target_ref=repository.target_ref,
            expected_target_oid=expected_target,
            metadata=metadata,
            status=SagaStatus.INTENT_DURABLE,
            revision=1,
            created_at_ms=created,
            updated_at_ms=created,
        )
        durable = self.store.create_or_replay(record)
        self._fault("after_intent")
        return self._drive_prepare(durable, repository)

    def _drive_prepare(
        self,
        record: SagaRecord,
        repository: RegisteredRepository,
    ) -> SagaRecord:
        if not record_matches_repository(record, repository):
            return self._quarantine_existing(
                record.operation_id, repository_drift_error(record, repository)
            )
        git = GitPlumbing(repository)
        if record.status is SagaStatus.INTENT_DURABLE:
            try:
                reservation = self.authority.reserve(
                    AuthorityRequest(
                        operation_id=record.operation_id,
                        domain_id=record.domain_id,
                        repo_id=record.repo_id,
                        claim_id=record.claim_id,
                        fence=record.fence,
                    )
                )
            except AuthorityRejected as exc:
                return self._terminal(
                    record,
                    SagaStatus.POLICY_REJECTED,
                    "AUTHORITY_REJECTED",
                    str(exc),
                )
            self._fault("after_authority_reserve_before_record")
            try:
                validate_reservation(record, reservation, record.fence)
            except Exception as exc:
                return self._terminal(
                    record,
                    SagaStatus.POLICY_REJECTED,
                    "AUTHORITY_REJECTED",
                    str(exc),
                )
            record = self.store.record_reservation(record.operation_id, reservation)
            self._fault("after_reservation")

        if record.status is SagaStatus.RESERVED:
            try:
                reservation = self._verified_reservation(record)
                git.pin(record.operation_id, "source", record.source_oid)
                git.pin(record.operation_id, "read-base", record.read_base_oid)
                git.pin(record.operation_id, "target", record.expected_target_oid)
                validate_reservation(record, reservation, None)
            except RepoConfigurationError:
                raise
            except Exception as exc:
                return self._terminal(
                    record,
                    SagaStatus.POLICY_REJECTED,
                    "INPUT_PIN_REJECTED",
                    str(exc),
                )
            record = self.store.record_inputs_pinned(record.operation_id)
            self._fault("after_inputs_pinned")

        if record.status is SagaStatus.INPUTS_PINNED:
            try:
                reservation = self._verified_reservation(record)
                claims = reservation.claims
                if not git.is_ancestor(record.read_base_oid, record.source_oid):
                    raise PathPolicyError("read base is not an ancestor of source")
                if not git.is_ancestor(record.read_base_oid, record.expected_target_oid):
                    raise ReadSetStale("target history no longer contains read base")
                audit_reads(git, record, claims)
                tree_oid = git.merge_tree(
                    record.expected_target_oid, record.source_oid
                )
                self._fault("after_merge_tree")
                audit_writes(git, record, tree_oid, claims)
                candidate_oid = git.commit_tree(
                    tree_oid=tree_oid,
                    target_oid=record.expected_target_oid,
                    source_oid=record.source_oid,
                    metadata=record.metadata,
                )
                self._fault("after_commit_tree")
                git.pin(record.operation_id, "candidate", candidate_oid)
                record = self.store.record_candidate(
                    record.operation_id, tree_oid, candidate_oid
                )
                self._fault("after_candidate_recorded")
                return record
            except MergeConflict as exc:
                return self._terminal(
                    record,
                    SagaStatus.MERGE_CONFLICT,
                    "MERGE_CONFLICT",
                    str(exc),
                )
            except ReadSetStale as exc:
                return self._terminal(
                    record, SagaStatus.READ_STALE, "READ_STALE", str(exc)
                )
            except WriteSetViolation as exc:
                return self._terminal(
                    record,
                    SagaStatus.WRITESET_REJECTED,
                    "WRITESET_REJECTED",
                    str(exc),
                )
            except (PathPolicyError, GitExecutionError, AuthorityRejected) as exc:
                return self._terminal(
                    record,
                    SagaStatus.POLICY_REJECTED,
                    "POLICY_REJECTED",
                    str(exc),
                )
        return record

    def _publish_locked(
        self, record: SagaRecord, repository: RegisteredRepository
    ) -> SagaRecord:
        if record.status.terminal:
            return self._settle_terminal(record)
        if not record_matches_repository(record, repository):
            return self._quarantine_existing(
                record.operation_id, repository_drift_error(record, repository)
            )
        if record.status is SagaStatus.APPLIED:
            return self._settle_applied(record)
        if record.status not in (SagaStatus.CANDIDATE_READY, SagaStatus.PUBLISHING):
            raise RepoConfigurationError(
                f"operation is not publishable from {record.status.value}"
            )
        if record.candidate_oid is None or record.reservation_id is None:
            return self._terminal(
                record,
                SagaStatus.IN_DOUBT,
                "MISSING_DURABLE_CANDIDATE",
                "publication projection is incomplete",
            )
        git = GitPlumbing(repository)

        if record.status is SagaStatus.CANDIDATE_READY:
            try:
                self._verified_reservation(record)
            except AuthorityRejected as exc:
                return self._terminal(
                    record,
                    SagaStatus.POLICY_REJECTED,
                    "PUBLISH_AUTHORITY_REJECTED",
                    str(exc),
                )
            try:
                current = git.read_target()
            except GitExecutionError as exc:
                return self._terminal(
                    record,
                    SagaStatus.REF_STALE,
                    "TARGET_UNAVAILABLE_BEFORE_PUBLICATION",
                    str(exc),
                )
            if current != record.expected_target_oid:
                return self._terminal(
                    record,
                    SagaStatus.REF_STALE,
                    "REF_STALE",
                    f"target moved before publication: {current}",
                )
            record = self.store.mark_publishing(record.operation_id)
            self._fault("after_publishing_recorded")

        try:
            self._verified_reservation(record)
        except AuthorityRejected as exc:
            return self._terminal(
                record,
                SagaStatus.IN_DOUBT,
                "PUBLISH_AUTHORITY_LOST",
                str(exc),
            )
        try:
            current = git.read_target()
        except GitExecutionError as exc:
            return self._terminal(
                record, SagaStatus.IN_DOUBT, "PUBLISH_OBSERVATION_FAILED", str(exc)
            )
        assert record.candidate_oid is not None
        if current == record.candidate_oid:
            applied = self.store.record_applied(record.operation_id, "exact_target")
            return self._settle_applied(applied)
        try:
            if git.is_ancestor(record.candidate_oid, current):
                applied = self.store.record_applied(
                    record.operation_id, "candidate_in_target_history"
                )
                return self._settle_applied(applied)
        except GitExecutionError as exc:
            return self._terminal(
                record, SagaStatus.IN_DOUBT, "ANCESTRY_UNKNOWN", str(exc)
            )
        if current != record.expected_target_oid:
            return self._terminal(
                record,
                SagaStatus.IN_DOUBT,
                "EXTERNAL_REF_REWRITE",
                f"target is neither expected nor candidate: {current}",
            )
        if not repository.omd_exclusive:
            return self._terminal(
                record,
                SagaStatus.IN_DOUBT,
                "NON_EXCLUSIVE_TARGET",
                "retry is unsafe without the OMD-owned no-rewind contract",
            )

        self._fault("before_ref_cas")
        try:
            self._verified_reservation(record)
            result = git.update_target(
                record.candidate_oid, record.expected_target_oid, record.operation_id
            )
        except Exception as exc:
            return self._terminal(
                record, SagaStatus.IN_DOUBT, "REF_CAS_EXECUTION_FAILED", str(exc)
            )
        if result.returncode == 0:
            self._fault("after_ref_cas")
            applied = self.store.record_applied(record.operation_id, "cas_success")
            self._fault("after_applied_recorded")
            return self._settle_applied(applied)

        try:
            observed = git.read_target()
            if observed == record.candidate_oid or git.is_ancestor(
                record.candidate_oid, observed
            ):
                applied = self.store.record_applied(
                    record.operation_id, "cas_response_lost"
                )
                return self._settle_applied(applied)
            detail = f"CAS returned {result.returncode}; target observed as {observed}"
        except Exception as exc:
            detail = f"CAS returned {result.returncode}; observation failed: {exc}"
        return self._terminal(
            record, SagaStatus.IN_DOUBT, "REF_CAS_UNCERTAIN", detail
        )

    def _verified_reservation(self, record: SagaRecord) -> MutationReservation:
        if record.reservation_id is None:
            raise AuthorityRejected("saga has no durable reservation receipt")
        reservation = self.authority.verify(record.reservation_id)
        validate_reservation(record, reservation, None)
        return reservation
