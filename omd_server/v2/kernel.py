"""Pure OMD v2 decision kernel.

``decide`` performs no I/O and never mutates its input.  It returns domain
events plus effect intentions.  A store must commit the idempotency binding,
events, and outbox rows atomically before any effect is executed.

# KG: finding-tpa-tcw-omd-core-engine-20260714
"""

from __future__ import annotations

import hashlib

from .errors import DomainError, ErrorCode, InvariantViolation
from .fencing import make_fence
from .model import (
    Accepted,
    ClaimChanged,
    ClaimCommand,
    ClaimExpired,
    ClaimGranted,
    ClaimRecord,
    ClaimRegistered,
    ClaimReleased,
    ClaimRenewed,
    ClaimStatus,
    CommandEnvelope,
    Decision,
    DomainEffect,
    DomainEvent,
    DomainState,
    FenceVector,
    IdempotencyRecord,
    IdempotencyRecorded,
    MalformedCommand,
    MaintenanceTick,
    PendingFenced,
    PendingTimedOut,
    Rejected,
    ReleaseCommand,
    RenewCommand,
)
from .resource import (
    claim_sets_conflict,
)
from .reducer import assert_invariants, evolve
from .validation import PROTOCOL_VERSION, validate_claim, validate_envelope


def _error(code: ErrorCode, **details: object) -> DomainError:
    return DomainError.make(code, **details)


def _operation_id(state: DomainState, request: CommandEnvelope) -> str:
    raw = "\x00".join(
        (
            state.domain_id,
            request.principal.client_id,
            request.request_id,
        )
    ).encode("utf-8")
    return f"op_{hashlib.sha256(raw).hexdigest()[:24]}"


def _claim_id(operation_id: str) -> str:
    return f"clm_{hashlib.sha256(operation_id.encode()).hexdigest()[:24]}"


def _idempotency_key(request: CommandEnvelope) -> tuple[str, str]:
    return request.principal.client_id, request.request_id


def _recorded_error(
    request: CommandEnvelope,
    operation_id: str,
    now_ms: int,
    error: DomainError,
) -> Decision:
    record = IdempotencyRecord(
        fingerprint=request.fingerprint,
        operation_id=operation_id,
        claim_id=None,
        frozen_error=error,
    )
    event = IdempotencyRecorded(_idempotency_key(request), record, now_ms)
    return Decision((event,), (), Rejected(error))


def _project_result(
    state: DomainState, record: IdempotencyRecord, *, replayed: bool
):
    if record.frozen_error is not None:
        return Rejected(record.frozen_error, replayed=replayed)
    if record.claim_id is None:
        return Accepted(record.operation_id, None, None, None, replayed=replayed)
    claim = state.claims.get(record.claim_id)
    if claim is None:
        raise InvariantViolation(
            f"idempotency record references missing claim {record.claim_id}"
        )
    return Accepted(
        operation_id=record.operation_id,
        claim_id=claim.claim_id,
        status=claim.status,
        fence=claim.fence if claim.status is ClaimStatus.ACTIVE else None,
        replayed=replayed,
    )


def _live_pending(record: ClaimRecord, now_ms: int) -> bool:
    return record.status is ClaimStatus.PENDING and record.wait_deadline_ms > now_ms


def _can_grant(state: DomainState, candidate: ClaimRecord, now_ms: int) -> bool:
    if state.session_epochs.get(
        (candidate.owner.client_id, candidate.owner.agent_id)
    ) != candidate.owner.session_epoch:
        return False
    for record in state.claims.values():
        if record.claim_id == candidate.claim_id:
            continue
        if record.status is ClaimStatus.ACTIVE and claim_sets_conflict(
            record.claims, candidate.claims
        ):
            return False
        if (
            _live_pending(record, now_ms)
            and record.enqueue_seq < candidate.enqueue_seq
            and claim_sets_conflict(record.claims, candidate.claims)
        ):
            return False
    return True


def _promotion_events(state: DomainState, now_ms: int) -> tuple[ClaimGranted, ...]:
    simulated = state
    events: list[ClaimGranted] = []
    epoch = state.next_grant_epoch
    pending = sorted(
        (
            record
            for record in state.claims.values()
            if _live_pending(record, now_ms)
            and state.session_epochs.get(
                (record.owner.client_id, record.owner.agent_id)
            )
            == record.owner.session_epoch
        ),
        key=lambda record: (record.enqueue_seq, record.claim_id),
    )
    for record in pending:
        current = simulated.claims[record.claim_id]
        if current.status is not ClaimStatus.PENDING:
            continue
        if not _can_grant(simulated, current, now_ms):
            continue
        fence = make_fence(current, epoch)
        event = ClaimGranted(
            claim_id=current.claim_id,
            lease_deadline_ms=now_ms + current.requested_lease_ttl_ms,
            fence=fence,
            occurred_at_ms=now_ms,
        )
        events.append(event)
        simulated = evolve(simulated, (event,))
        epoch += 1
    return tuple(events)


def maintenance_events(state: DomainState, now_ms: int) -> tuple[DomainEvent, ...]:
    """Expire due records and promote eligible waiters at one linearization point."""

    due: list[DomainEvent] = []
    ordered = sorted(
        state.claims.values(), key=lambda record: (record.enqueue_seq, record.claim_id)
    )
    for record in ordered:
        current_epoch = state.session_epochs.get(
            (record.owner.client_id, record.owner.agent_id)
        )
        if (
            record.status is ClaimStatus.PENDING
            and current_epoch != record.owner.session_epoch
        ):
            due.append(PendingFenced(record.claim_id, now_ms))
        elif (
            record.status is ClaimStatus.ACTIVE
            and record.lease_deadline_ms is not None
            and record.lease_deadline_ms <= now_ms
        ):
            due.append(ClaimExpired(record.claim_id, now_ms))
        elif (
            record.status is ClaimStatus.PENDING
            and record.wait_deadline_ms <= now_ms
        ):
            due.append(PendingTimedOut(record.claim_id, now_ms))
    after_due = evolve(state, tuple(due))
    return tuple(due) + _promotion_events(after_due, now_ms)


def effects_for_events(events: tuple[DomainEvent, ...]) -> tuple[DomainEffect, ...]:
    effects: list[ClaimChanged] = []
    for event in events:
        status = None
        claim_id = None
        if isinstance(event, ClaimRegistered):
            status, claim_id = ClaimStatus.PENDING, event.record.claim_id
        elif isinstance(event, ClaimGranted):
            status, claim_id = ClaimStatus.ACTIVE, event.claim_id
        elif isinstance(event, ClaimRenewed):
            status, claim_id = ClaimStatus.ACTIVE, event.claim_id
        elif isinstance(event, ClaimReleased):
            status, claim_id = ClaimStatus.RELEASED, event.claim_id
        elif isinstance(event, ClaimExpired):
            status, claim_id = ClaimStatus.EXPIRED, event.claim_id
        elif isinstance(event, PendingTimedOut):
            status, claim_id = ClaimStatus.TIMED_OUT, event.claim_id
        elif isinstance(event, PendingFenced):
            status, claim_id = ClaimStatus.FENCED, event.claim_id
        if status is not None and claim_id is not None:
            effects.append(ClaimChanged(claim_id, status, event.occurred_at_ms))
    return tuple(effects)


def _finish_success(
    state: DomainState,
    request: CommandEnvelope,
    operation_id: str,
    claim_id: str | None,
    now_ms: int,
    domain_events: tuple[DomainEvent, ...],
) -> Decision:
    idem = IdempotencyRecord(request.fingerprint, operation_id, claim_id)
    events = domain_events + (
        IdempotencyRecorded(_idempotency_key(request), idem, now_ms),
    )
    next_state = evolve(state, events)
    return Decision(
        events,
        effects_for_events(domain_events),
        _project_result(next_state, idem, replayed=False),
    )


def _decide_claim(
    state: DomainState,
    request: CommandEnvelope,
    operation_id: str,
    command: ClaimCommand,
    now_ms: int,
) -> Decision:
    claims, error = validate_claim(state, request, command)
    if error is not None or claims is None:
        return _recorded_error(request, operation_id, now_ms, error or _error(ErrorCode.INVALID_RESOURCE))

    claim_id = _claim_id(operation_id)
    record = ClaimRecord(
        claim_id=claim_id,
        operation_id=operation_id,
        owner=request.principal,
        claims=claims,
        status=ClaimStatus.PENDING,
        enqueue_seq=state.next_enqueue_seq,
        enqueued_at_ms=now_ms,
        requested_lease_ttl_ms=command.lease_ttl_ms,
        requested_wait_timeout_ms=command.wait_timeout_ms,
        wait_deadline_ms=now_ms + command.wait_timeout_ms,
        lease_deadline_ms=None,
        fence=None,
    )
    registered = ClaimRegistered(record, now_ms)
    events: tuple[DomainEvent, ...] = (registered,)
    with_pending = evolve(state, events)
    if _can_grant(with_pending, record, now_ms):
        events += (
            ClaimGranted(
                claim_id=claim_id,
                lease_deadline_ms=now_ms + command.lease_ttl_ms,
                fence=make_fence(record, state.next_grant_epoch),
                occurred_at_ms=now_ms,
            ),
        )
    return _finish_success(
        state, request, operation_id, claim_id, now_ms, events
    )


def _claim_mutation_error(
    state: DomainState,
    request: CommandEnvelope,
    claim_id: str,
    fence: FenceVector,
) -> tuple[ClaimRecord | None, DomainError | None]:
    record = state.claims.get(claim_id)
    if record is None:
        return None, _error(ErrorCode.UNKNOWN_CLAIM, claim_id=claim_id)
    if record.owner != request.principal:
        return None, _error(ErrorCode.NOT_OWNER, claim_id=claim_id)
    if record.status is not ClaimStatus.ACTIVE:
        return None, _error(
            ErrorCode.CLAIM_NOT_ACTIVE,
            claim_id=claim_id,
            status=record.status.value,
        )
    if record.fence is None or fence != record.fence:
        return None, _error(ErrorCode.STALE_FENCE_VECTOR, claim_id=claim_id)
    return record, None


def _decide_release(
    state: DomainState,
    request: CommandEnvelope,
    operation_id: str,
    command: ReleaseCommand,
    now_ms: int,
) -> Decision:
    _, error = _claim_mutation_error(
        state, request, command.claim_id, command.fence
    )
    if error is not None:
        return _recorded_error(request, operation_id, now_ms, error)
    released = ClaimReleased(command.claim_id, now_ms)
    after_release = evolve(state, (released,))
    events: tuple[DomainEvent, ...] = (released,) + _promotion_events(
        after_release, now_ms
    )
    return _finish_success(
        state, request, operation_id, command.claim_id, now_ms, events
    )


def _decide_renew(
    state: DomainState,
    request: CommandEnvelope,
    operation_id: str,
    command: RenewCommand,
    now_ms: int,
) -> Decision:
    record, error = _claim_mutation_error(
        state, request, command.claim_id, command.fence
    )
    if error is None and (
        type(command.lease_ttl_ms) is not int or command.lease_ttl_ms <= 0
    ):
        error = _error(ErrorCode.INVALID_TTL)
    if error is not None or record is None:
        return _recorded_error(request, operation_id, now_ms, error or _error(ErrorCode.UNKNOWN_CLAIM))
    event = ClaimRenewed(
        claim_id=record.claim_id,
        lease_deadline_ms=max(
            record.lease_deadline_ms or 0,
            now_ms + command.lease_ttl_ms,
        ),
        requested_lease_ttl_ms=command.lease_ttl_ms,
        occurred_at_ms=now_ms,
    )
    return _finish_success(
        state, request, operation_id, record.claim_id, now_ms, (event,)
    )


def _decide_tick(
    state: DomainState,
    request: CommandEnvelope,
    operation_id: str,
    now_ms: int,
) -> Decision:
    events = maintenance_events(state, now_ms)
    return _finish_success(state, request, operation_id, None, now_ms, events)


def _decide_command(
    state: DomainState,
    request: CommandEnvelope,
    operation_id: str,
    now_ms: int,
) -> Decision:
    command = request.command
    if isinstance(command, ClaimCommand):
        return _decide_claim(state, request, operation_id, command, now_ms)
    if isinstance(command, ReleaseCommand):
        return _decide_release(state, request, operation_id, command, now_ms)
    if isinstance(command, RenewCommand):
        return _decide_renew(state, request, operation_id, command, now_ms)
    if isinstance(command, MalformedCommand):
        return _recorded_error(request, operation_id, now_ms, command.error)
    return _recorded_error(
        request,
        operation_id,
        now_ms,
        _error(ErrorCode.UNSUPPORTED_COMMAND, type=type(command).__qualname__),
    )


def decide(state: DomainState, request: CommandEnvelope, now_ms: int) -> Decision:
    """Return deterministic events/effects/result without mutating ``state``."""

    envelope_error = validate_envelope(state, request, now_ms)
    if envelope_error is not None:
        return Decision((), (), Rejected(envelope_error))

    # Every valid mutation attempt, including an idempotent replay or key-reuse
    # rejection, first linearizes due deadlines. Otherwise a replay that wins
    # the writer lock at the exact lease deadline could return an expired fence
    # as ACTIVE before the supervisor gets its turn.
    maintenance = maintenance_events(state, now_ms)
    prepared = evolve(state, maintenance)
    maintenance_effects = effects_for_events(maintenance)

    key = _idempotency_key(request)
    recorded = prepared.idempotency.get(key)
    if recorded is not None:
        if recorded.fingerprint != request.fingerprint:
            return Decision(
                maintenance,
                maintenance_effects,
                Rejected(
                    _error(
                        ErrorCode.IDEMPOTENCY_KEY_REUSE,
                        client_id=key[0],
                        request_id=key[1],
                    )
                ),
            )
        return Decision(
            maintenance,
            maintenance_effects,
            _project_result(prepared, recorded, replayed=True),
        )

    operation_id = _operation_id(prepared, request)
    if isinstance(request.command, MaintenanceTick):
        decision = _decide_tick(prepared, request, operation_id, now_ms)
        return Decision(
            maintenance + decision.events,
            maintenance_effects + decision.effects,
            decision.result,
        )

    decision = _decide_command(prepared, request, operation_id, now_ms)
    return Decision(
        maintenance + decision.events,
        maintenance_effects + decision.effects,
        decision.result,
    )
