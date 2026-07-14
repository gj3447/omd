"""Event reducer and executable aggregate invariants for OMD v2."""

from __future__ import annotations

import hashlib
from dataclasses import replace

from .errors import InvariantViolation
from .fencing import fence_digest
from .model import (
    ClaimExpired,
    ClaimGranted,
    ClaimRegistered,
    ClaimReleased,
    ClaimRenewed,
    ClaimStatus,
    DomainEvent,
    DomainState,
    IdempotencyRecorded,
    PendingFenced,
    PendingTimedOut,
)
from .resource import (
    AccessMode,
    claim_sets_conflict,
    claim_spec_key,
    overlaps,
    validate_resource_id,
)


def _expect_status(
    record, expected: ClaimStatus, event: DomainEvent
) -> None:
    if record.status is not expected:
        raise InvariantViolation(
            f"illegal {type(event).__name__} transition from {record.status.value}"
        )


def _operation_id(domain_id: str, key: tuple[str, str]) -> str:
    raw = "\x00".join((domain_id, key[0], key[1])).encode("utf-8")
    return f"op_{hashlib.sha256(raw).hexdigest()[:24]}"


def evolve(state: DomainState, events: tuple[DomainEvent, ...]) -> DomainState:
    """Apply events to a fresh immutable projection."""

    claims = dict(state.claims)
    idempotency = dict(state.idempotency)
    last_now_ms = state.last_now_ms
    next_enqueue_seq = state.next_enqueue_seq
    next_grant_epoch = state.next_grant_epoch

    for event in events:
        if event.occurred_at_ms < last_now_ms:
            raise InvariantViolation("event time regressed")
        last_now_ms = event.occurred_at_ms
        if isinstance(event, ClaimRegistered):
            if event.record.claim_id in claims:
                raise InvariantViolation(f"duplicate claim {event.record.claim_id}")
            if event.record.status is not ClaimStatus.PENDING:
                raise InvariantViolation("new claim must start pending")
            if event.record.fence is not None or event.record.lease_deadline_ms is not None:
                raise InvariantViolation("new claim has a partial grant")
            if (
                type(event.record.enqueued_at_ms) is not int
                or event.record.enqueued_at_ms != event.occurred_at_ms
                or type(event.record.requested_lease_ttl_ms) is not int
                or event.record.requested_lease_ttl_ms <= 0
                or type(event.record.requested_wait_timeout_ms) is not int
                or event.record.requested_wait_timeout_ms <= 0
                or type(event.record.wait_deadline_ms) is not int
                or event.record.wait_deadline_ms
                != event.occurred_at_ms + event.record.requested_wait_timeout_ms
            ):
                raise InvariantViolation("new claim has invalid deadline authority")
            session_key = (event.record.owner.client_id, event.record.owner.agent_id)
            if state.session_epochs.get(session_key) != event.record.owner.session_epoch:
                raise InvariantViolation("claim owner session is not current")
            if event.record.enqueue_seq != next_enqueue_seq:
                raise InvariantViolation("enqueue sequence is not the next global sequence")
            claims[event.record.claim_id] = event.record
            next_enqueue_seq += 1
        elif isinstance(event, ClaimGranted):
            record = claims[event.claim_id]
            _expect_status(record, ClaimStatus.PENDING, event)
            if event.lease_deadline_ms != (
                event.occurred_at_ms + record.requested_lease_ttl_ms
            ):
                raise InvariantViolation("grant deadline does not match requested TTL")
            if record.wait_deadline_ms <= event.occurred_at_ms:
                raise InvariantViolation("cannot grant after the wait deadline")
            session_key = (record.owner.client_id, record.owner.agent_id)
            if state.session_epochs.get(session_key) != record.owner.session_epoch:
                raise InvariantViolation("cannot grant a stale session")
            for other in claims.values():
                if other.claim_id == record.claim_id:
                    continue
                active_conflict = (
                    other.status is ClaimStatus.ACTIVE
                    and claim_sets_conflict(other.claims, record.claims)
                )
                earlier_waiter = (
                    other.status is ClaimStatus.PENDING
                    and other.wait_deadline_ms > event.occurred_at_ms
                    and other.enqueue_seq < record.enqueue_seq
                    and claim_sets_conflict(other.claims, record.claims)
                )
                if active_conflict or earlier_waiter:
                    raise InvariantViolation("grant violates admission ordering")
            claims[event.claim_id] = replace(
                record,
                status=ClaimStatus.ACTIVE,
                lease_deadline_ms=event.lease_deadline_ms,
                fence=event.fence,
            )
            epochs = {entry.grant_epoch for entry in event.fence.entries}
            if len(epochs) != 1:
                raise InvariantViolation("one claim-set grant must use one epoch")
            event_epoch = next(iter(epochs))
            if event_epoch != next_grant_epoch:
                raise InvariantViolation("grant epoch is not the next global epoch")
            next_grant_epoch += 1
        elif isinstance(event, ClaimRenewed):
            record = claims[event.claim_id]
            _expect_status(record, ClaimStatus.ACTIVE, event)
            session_key = (record.owner.client_id, record.owner.agent_id)
            if (
                type(event.requested_lease_ttl_ms) is not int
                or event.requested_lease_ttl_ms <= 0
            ):
                raise InvariantViolation("renewal has an invalid requested TTL")
            expected_deadline_ms = max(
                record.lease_deadline_ms or 0,
                event.occurred_at_ms + event.requested_lease_ttl_ms,
            )
            if (
                record.lease_deadline_ms is None
                or record.lease_deadline_ms <= event.occurred_at_ms
                or event.lease_deadline_ms != expected_deadline_ms
                or state.session_epochs.get(session_key) != record.owner.session_epoch
            ):
                raise InvariantViolation("renewal violates lease deadline authority")
            claims[event.claim_id] = replace(
                record, lease_deadline_ms=event.lease_deadline_ms
            )
        elif isinstance(event, ClaimReleased):
            record = claims[event.claim_id]
            _expect_status(record, ClaimStatus.ACTIVE, event)
            session_key = (record.owner.client_id, record.owner.agent_id)
            if (
                record.lease_deadline_ms is None
                or record.lease_deadline_ms <= event.occurred_at_ms
                or state.session_epochs.get(session_key) != record.owner.session_epoch
            ):
                raise InvariantViolation("release violates lease deadline authority")
            claims[event.claim_id] = replace(
                record, status=ClaimStatus.RELEASED
            )
        elif isinstance(event, ClaimExpired):
            record = claims[event.claim_id]
            _expect_status(record, ClaimStatus.ACTIVE, event)
            if (
                record.lease_deadline_ms is None
                or record.lease_deadline_ms > event.occurred_at_ms
            ):
                raise InvariantViolation("expiry occurred before its deadline")
            claims[event.claim_id] = replace(
                record, status=ClaimStatus.EXPIRED
            )
        elif isinstance(event, PendingTimedOut):
            record = claims[event.claim_id]
            _expect_status(record, ClaimStatus.PENDING, event)
            session_key = (record.owner.client_id, record.owner.agent_id)
            if (
                record.wait_deadline_ms > event.occurred_at_ms
                or state.session_epochs.get(session_key) != record.owner.session_epoch
            ):
                raise InvariantViolation("pending timeout occurred before its deadline")
            claims[event.claim_id] = replace(
                record, status=ClaimStatus.TIMED_OUT
            )
        elif isinstance(event, PendingFenced):
            record = claims[event.claim_id]
            _expect_status(record, ClaimStatus.PENDING, event)
            session_key = (record.owner.client_id, record.owner.agent_id)
            if state.session_epochs.get(session_key) == record.owner.session_epoch:
                raise InvariantViolation("cannot fence a current pending session")
            claims[event.claim_id] = replace(
                record, status=ClaimStatus.FENCED
            )
        elif isinstance(event, IdempotencyRecorded):
            previous = idempotency.get(event.key)
            if previous is not None and previous != event.record:
                raise InvariantViolation(f"idempotency overwrite {event.key}")
            idempotency[event.key] = event.record
        else:  # pragma: no cover - defensive boundary for future events
            raise InvariantViolation(f"unsupported event {type(event).__qualname__}")

    return DomainState(
        domain_id=state.domain_id,
        last_now_ms=last_now_ms,
        next_enqueue_seq=next_enqueue_seq,
        next_grant_epoch=next_grant_epoch,
        repo_policies=state.repo_policies,
        session_epochs=state.session_epochs,
        claims=claims,
        idempotency=idempotency,
    )


def assert_invariants(state: DomainState) -> None:
    """Executable safety invariants; corrupt state is never repaired silently."""

    if not state.domain_id or "\x00" in state.domain_id:
        raise InvariantViolation("invalid domain identity")
    if state.last_now_ms < 0 or state.next_enqueue_seq < 1 or state.next_grant_epoch < 1:
        raise InvariantViolation("invalid aggregate counters")
    for repo_id, policy in state.repo_policies.items():
        if repo_id != policy.repo_id:
            raise InvariantViolation("repository policy key mismatch")
    for (client_id, agent_id), epoch in state.session_epochs.items():
        if (
            not client_id
            or not agent_id
            or "\x00" in client_id
            or "\x00" in agent_id
            or type(epoch) is not int
            or epoch < 1
        ):
            raise InvariantViolation("invalid session registry entry")

    ordered = sorted(state.claims.values(), key=lambda item: item.claim_id)
    active = [record for record in ordered if record.status is ClaimStatus.ACTIVE]
    for index, left in enumerate(active):
        for right in active[index + 1 :]:
            if claim_sets_conflict(left.claims, right.claims):
                raise InvariantViolation(
                    f"conflicting active claims: {left.claim_id}, {right.claim_id}"
                )

    seen_sequences: set[int] = set()
    seen_epochs: set[int] = set()
    seen_operations: set[str] = set()
    max_epoch = 0
    max_enqueue_seq = 0
    for record in ordered:
        if not record.claim_id or not record.operation_id:
            raise InvariantViolation("empty claim identity")
        if record.operation_id in seen_operations:
            raise InvariantViolation(f"duplicate operation {record.operation_id}")
        seen_operations.add(record.operation_id)
        if record.enqueue_seq < 1:
            raise InvariantViolation(f"invalid enqueue_seq {record.claim_id}")
        max_enqueue_seq = max(max_enqueue_seq, record.enqueue_seq)
        if record.enqueue_seq in seen_sequences:
            raise InvariantViolation(f"duplicate enqueue_seq {record.enqueue_seq}")
        seen_sequences.add(record.enqueue_seq)
        session_key = (record.owner.client_id, record.owner.agent_id)
        current_epoch = state.session_epochs.get(session_key)
        if (
            record.owner.session_epoch < 1
            or current_epoch is None
            or current_epoch < record.owner.session_epoch
        ):
            raise InvariantViolation(f"missing claim owner session {record.claim_id}")
        if (
            record.status is ClaimStatus.PENDING
            and current_epoch != record.owner.session_epoch
        ):
            raise InvariantViolation(f"stale pending owner {record.claim_id}")
        if not record.claims:
            raise InvariantViolation(f"empty claim set {record.claim_id}")
        if (
            type(record.enqueued_at_ms) is not int
            or record.enqueued_at_ms < 0
            or record.enqueued_at_ms > state.last_now_ms
            or type(record.requested_lease_ttl_ms) is not int
            or record.requested_lease_ttl_ms <= 0
            or type(record.requested_wait_timeout_ms) is not int
            or record.requested_wait_timeout_ms <= 0
            or type(record.wait_deadline_ms) is not int
            or record.wait_deadline_ms < 0
            or record.wait_deadline_ms
            != record.enqueued_at_ms + record.requested_wait_timeout_ms
        ):
            raise InvariantViolation(f"invalid claim deadlines {record.claim_id}")
        if record.claims != tuple(sorted(record.claims, key=claim_spec_key)):
            raise InvariantViolation(f"noncanonical claim order {record.claim_id}")
        for index, spec in enumerate(record.claims):
            if not isinstance(spec.mode, AccessMode):
                raise InvariantViolation(f"invalid access mode {record.claim_id}")
            resource = spec.resource
            if resource.domain_id != state.domain_id:
                raise InvariantViolation(f"foreign-domain claim {record.claim_id}")
            policy = state.repo_policies.get(resource.repo_id)
            if policy is None or validate_resource_id(resource, policy) is not None:
                raise InvariantViolation(f"invalid resource {record.claim_id}")
            for right in record.claims[index + 1 :]:
                if overlaps(resource, right.resource):
                    raise InvariantViolation(f"self-overlap {record.claim_id}")
        if record.status is ClaimStatus.PENDING:
            if record.fence is not None or record.lease_deadline_ms is not None:
                raise InvariantViolation(f"partial grant {record.claim_id}")
            if record.wait_deadline_ms <= state.last_now_ms:
                raise InvariantViolation(
                    f"pending claim deadline elapsed {record.claim_id}"
                )
            continue
        if record.status is ClaimStatus.ACTIVE and (
            record.fence is None or record.lease_deadline_ms is None
        ):
            raise InvariantViolation(f"active claim lacks lease proof {record.claim_id}")
        if (
            record.status is ClaimStatus.ACTIVE
            and record.lease_deadline_ms is not None
            and record.lease_deadline_ms <= state.last_now_ms
        ):
            raise InvariantViolation(f"active claim deadline elapsed {record.claim_id}")
        if record.fence is None:
            if record.status not in {ClaimStatus.TIMED_OUT, ClaimStatus.FENCED}:
                raise InvariantViolation(f"terminal claim lacks grant proof {record.claim_id}")
            continue
        fence = record.fence
        if fence.claim_id != record.claim_id or fence.owner != record.owner:
            raise InvariantViolation(f"fence ownership mismatch {record.claim_id}")
        expected_resources = tuple(
            spec.resource for spec in sorted(record.claims, key=claim_spec_key)
        )
        actual_resources = tuple(entry.resource for entry in fence.entries)
        if actual_resources != expected_resources:
            raise InvariantViolation(f"fence closure mismatch {record.claim_id}")
        epochs = {entry.grant_epoch for entry in fence.entries}
        if len(epochs) != 1:
            raise InvariantViolation(f"mixed fence epoch {record.claim_id}")
        epoch = next(iter(epochs))
        if epoch < 1:
            raise InvariantViolation(f"invalid fence epoch {record.claim_id}")
        if epoch in seen_epochs:
            raise InvariantViolation(f"reused global fence epoch {epoch}")
        seen_epochs.add(epoch)
        max_epoch = max(max_epoch, epoch)
        if fence.vector_digest != fence_digest(
            fence.claim_id, fence.owner, fence.entries
        ):
            raise InvariantViolation(f"fence digest mismatch {record.claim_id}")

    if state.next_grant_epoch <= max_epoch:
        raise InvariantViolation("next grant epoch is not monotonic")
    if state.next_enqueue_seq <= max_enqueue_seq:
        raise InvariantViolation("next enqueue sequence is not monotonic")

    live_pending = [
        record
        for record in ordered
        if record.status is ClaimStatus.PENDING
        and record.wait_deadline_ms > state.last_now_ms
    ]
    for pending in live_pending:
        for granted in active:
            if (
                pending.enqueue_seq < granted.enqueue_seq
                and claim_sets_conflict(pending.claims, granted.claims)
            ):
                raise InvariantViolation(
                    f"no-barging violation: {pending.claim_id}, {granted.claim_id}"
                )

    claim_links: dict[str, list[tuple[tuple[str, str], object]]] = {}
    for key, record in state.idempotency.items():
        if (
            len(key) != 2
            or not key[0]
            or not key[1]
            or "\x00" in key[0]
            or "\x00" in key[1]
            or record.operation_id != _operation_id(state.domain_id, key)
            or len(record.fingerprint) != 64
            or any(character not in "0123456789abcdef" for character in record.fingerprint)
        ):
            raise InvariantViolation(f"invalid idempotency binding {key}")
        if record.claim_id is not None and record.frozen_error is not None:
            raise InvariantViolation(f"ambiguous idempotency result {key}")
        if record.claim_id is not None and record.claim_id not in state.claims:
            raise InvariantViolation(
                f"idempotency references missing claim {record.claim_id}"
            )
        if record.claim_id is not None:
            claim_links.setdefault(record.claim_id, []).append((key, record))
    for claim in ordered:
        links = [
            (key, idem)
            for key, idem in claim_links.get(claim.claim_id, [])
            if idem.operation_id == claim.operation_id
        ]
        if len(links) != 1:
            raise InvariantViolation(f"claim idempotency mismatch {claim.claim_id}")
        key, idem = links[0]
        if key[0] != claim.owner.client_id:
            raise InvariantViolation(f"claim operation binding mismatch {claim.claim_id}")
