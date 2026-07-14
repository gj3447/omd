from __future__ import annotations

from dataclasses import replace

import pytest

from omd_server.v2.errors import ErrorCode, InvariantViolation
from omd_server.v2.fencing import make_fence
from omd_server.v2.kernel import assert_invariants, decide, evolve
from omd_server.v2.model import (
    Accepted,
    ClaimCommand,
    ClaimExpired,
    ClaimGranted,
    ClaimRegistered,
    ClaimReleased,
    ClaimRenewed,
    ClaimStatus,
    CommandEnvelope,
    DomainState,
    Principal,
    Rejected,
    ReleaseCommand,
    RenewCommand,
)
from omd_server.v2.resource import (
    AccessMode,
    ClaimSpec,
    RepoPolicy,
    ResourceId,
    SelectorKind,
    canonicalize_resource,
)


DOMAIN = "hardening"
REPO = RepoPolicy("repo")
ALICE = Principal("client", "alice", 1)
BOB = Principal("client", "bob", 1)


def state() -> DomainState:
    return DomainState.empty(
        domain_id=DOMAIN,
        repo_policies=(REPO,),
        session_epochs={("client", "alice"): 1, ("client", "bob"): 1},
    )


def spec(path: str, mode: AccessMode = AccessMode.WRITE) -> ClaimSpec:
    return ClaimSpec(
        canonicalize_resource(
            domain_id=DOMAIN,
            policy=REPO,
            raw_path=path,
            selector=SelectorKind.EXACT,
        ),
        mode,
    )


def request(
    request_id: str,
    command,
    *,
    principal: Principal = ALICE,
) -> CommandEnvelope:
    return CommandEnvelope.create(
        protocol_version=2,
        domain_id=DOMAIN,
        principal=principal,
        request_id=request_id,
        command=command,
    )


def claim(path: str, *, ttl_ms: int = 1_000, mode=AccessMode.WRITE):
    return ClaimCommand((spec(path, mode),), ttl_ms, 5_000)


def applied(current: DomainState, envelope: CommandEnvelope, now_ms: int):
    decision = decide(current, envelope, now_ms)
    return evolve(current, decision.events), decision


def test_mutable_input_cannot_detach_fingerprint_from_executed_claim() -> None:
    claims = [spec("a.py")]
    command = ClaimCommand(claims, 1_000, 5_000)
    envelope = request("mutable", command)

    claims[0] = spec("b.py")
    next_state, decision = applied(state(), envelope, 10)

    assert isinstance(decision.result, Accepted)
    admitted = next_state.claims[decision.result.claim_id]
    assert admitted.claims[0].resource.segments == ("a.py",)


@pytest.mark.parametrize(
    "bad_resource",
    [
        "not-a-resource",
        ResourceId(DOMAIN, REPO.repo_id, (1,), SelectorKind.EXACT),
    ],
)
def test_malformed_typed_resource_becomes_a_typed_rejection(bad_resource) -> None:
    command = ClaimCommand(
        (ClaimSpec(bad_resource, AccessMode.WRITE),), 1_000, 5_000
    )

    decision = decide(state(), request("malformed", command), 10)

    assert isinstance(decision.result, Rejected)
    assert decision.result.error.code is ErrorCode.INVALID_RESOURCE


def test_due_expiry_is_linearized_before_late_renew() -> None:
    current, granted = applied(state(), request("claim", claim("a.py", ttl_ms=5)), 10)
    assert isinstance(granted.result, Accepted)
    record = current.claims[granted.result.claim_id]
    assert record.fence is not None

    current, renewed = applied(
        current,
        request("late-renew", RenewCommand(record.claim_id, record.fence, 1_000)),
        15,
    )

    assert isinstance(renewed.result, Rejected)
    assert renewed.result.error.code is ErrorCode.CLAIM_NOT_ACTIVE
    assert current.claims[record.claim_id].status is ClaimStatus.EXPIRED


def test_due_replay_linearizes_expiry_before_projecting_the_result() -> None:
    envelope = request("claim", claim("a.py", ttl_ms=5))
    current, granted = applied(state(), envelope, 10)
    assert isinstance(granted.result, Accepted)

    replay = decide(current, envelope, 15)
    next_state = evolve(current, replay.events)

    assert isinstance(replay.result, Accepted)
    assert replay.result.replayed is True
    assert replay.result.status is ClaimStatus.EXPIRED
    assert any(isinstance(event, ClaimExpired) for event in replay.events)
    assert next_state.claims[granted.result.claim_id].status is ClaimStatus.EXPIRED


def test_reducer_rejects_release_at_the_lease_deadline() -> None:
    current, granted = applied(
        state(), request("claim", claim("a.py", ttl_ms=5)), 10
    )
    assert isinstance(granted.result, Accepted)

    with pytest.raises(InvariantViolation, match="deadline authority"):
        evolve(current, (ClaimReleased(granted.result.claim_id, 15),))


def test_reducer_rejects_grant_at_the_wait_deadline() -> None:
    current, holder = applied(state(), request("holder", claim("a.py")), 10)
    short_wait = ClaimCommand((spec("a.py"),), 1_000, 3)
    current, waiter = applied(
        current, request("waiter", short_wait, principal=BOB), 11
    )
    assert isinstance(holder.result, Accepted)
    assert isinstance(waiter.result, Accepted)
    waiter_record = current.claims[waiter.result.claim_id]
    forged_fence = make_fence(waiter_record, current.next_grant_epoch)

    with pytest.raises(InvariantViolation, match="wait deadline"):
        evolve(
            current,
            (
                ClaimReleased(holder.result.claim_id, 14),
                ClaimGranted(waiter.result.claim_id, 1_014, forged_fence, 14),
            ),
        )


def test_reducer_rejects_grant_duration_beyond_requested_ttl() -> None:
    current, holder = applied(state(), request("holder", claim("a.py")), 10)
    current, waiter = applied(
        current, request("waiter", claim("a.py"), principal=BOB), 11
    )
    assert isinstance(holder.result, Accepted)
    assert isinstance(waiter.result, Accepted)
    waiter_record = current.claims[waiter.result.claim_id]
    forged_fence = make_fence(waiter_record, current.next_grant_epoch)

    with pytest.raises(InvariantViolation, match="requested TTL"):
        evolve(
            current,
            (
                ClaimReleased(holder.result.claim_id, 12),
                ClaimGranted(waiter.result.claim_id, 999_999, forged_fence, 12),
            ),
        )


def test_reducer_rejects_renewal_duration_beyond_requested_ttl() -> None:
    current, granted = applied(state(), request("claim", claim("a.py")), 10)
    assert isinstance(granted.result, Accepted)

    with pytest.raises(InvariantViolation, match="deadline authority"):
        evolve(
            current,
            (
                ClaimRenewed(
                    granted.result.claim_id,
                    lease_deadline_ms=999_999,
                    requested_lease_ttl_ms=5,
                    occurred_at_ms=11,
                ),
            ),
        )


def test_invariants_reject_a_due_pending_projection() -> None:
    current, _ = applied(state(), request("holder", claim("a.py")), 10)
    short_wait = ClaimCommand((spec("a.py"),), 1_000, 3)
    current, waiter = applied(
        current, request("waiter", short_wait, principal=BOB), 11
    )
    assert isinstance(waiter.result, Accepted)
    assert waiter.result.status is ClaimStatus.PENDING

    with pytest.raises(InvariantViolation, match="pending claim deadline elapsed"):
        assert_invariants(replace(current, last_now_ms=14))


def test_reducer_rejects_registration_deadline_beyond_requested_wait() -> None:
    decision = decide(state(), request("claim", claim("a.py")), 10)
    registered = next(
        event for event in decision.events if isinstance(event, ClaimRegistered)
    )
    forged = replace(
        registered,
        record=replace(
            registered.record,
            wait_deadline_ms=registered.record.wait_deadline_ms + 1,
        ),
    )

    with pytest.raises(InvariantViolation, match="deadline authority"):
        evolve(state(), (forged,))


def test_due_expiry_unblocks_conflicting_fresh_claim_in_same_decision() -> None:
    current, first = applied(state(), request("first", claim("a.py", ttl_ms=5)), 10)
    assert isinstance(first.result, Accepted)

    current, second = applied(
        current,
        request("second", claim("a.py"), principal=BOB),
        15,
    )

    assert isinstance(second.result, Accepted)
    assert second.result.status is ClaimStatus.ACTIVE
    assert current.claims[first.result.claim_id].status is ClaimStatus.EXPIRED


def test_session_rollover_fences_the_stale_process() -> None:
    current, first = applied(state(), request("first", claim("a.py")), 10)
    assert isinstance(first.result, Accepted)
    epochs = dict(current.session_epochs)
    epochs[("client", "alice")] = 2
    current = replace(current, session_epochs=epochs)

    stale = decide(current, request("stale", claim("b.py")), 11)
    fresh = decide(
        current,
        request("fresh", claim("b.py"), principal=Principal("client", "alice", 2)),
        11,
    )

    assert isinstance(stale.result, Rejected)
    assert stale.result.error.code is ErrorCode.STALE_SESSION
    assert isinstance(fresh.result, Accepted)


def test_stale_pending_claim_is_fenced_instead_of_promoted() -> None:
    current, holder = applied(state(), request("holder", claim("a.py")), 10)
    current, waiter = applied(
        current, request("waiter", claim("a.py"), principal=BOB), 11
    )
    assert isinstance(holder.result, Accepted)
    assert isinstance(waiter.result, Accepted)
    holder_record = current.claims[holder.result.claim_id]
    assert holder_record.fence is not None
    epochs = dict(current.session_epochs)
    epochs[("client", "bob")] = 2
    # Model the atomic store rollover result immediately before its fenced
    # system event; the next mutation prelude must still fail closed.
    current = replace(current, session_epochs=epochs)

    current, released = applied(
        current,
        request(
            "release",
            ReleaseCommand(holder_record.claim_id, holder_record.fence),
        ),
        12,
    )

    assert isinstance(released.result, Accepted)
    assert current.claims[waiter.result.claim_id].status is ClaimStatus.FENCED


def test_reducer_rejects_pending_to_released_transition() -> None:
    current, _ = applied(state(), request("holder", claim("a.py")), 10)
    current, waiter = applied(
        current, request("waiter", claim("a.py"), principal=BOB), 11
    )
    assert isinstance(waiter.result, Accepted)

    with pytest.raises(InvariantViolation, match="illegal ClaimReleased"):
        evolve(current, (ClaimReleased(waiter.result.claim_id, 12),))


def test_invariants_detect_a_later_reader_barging_past_waiting_writer() -> None:
    current, _ = applied(
        state(), request("reader", claim("a.py", mode=AccessMode.READ)), 10
    )
    current, _ = applied(
        current, request("writer", claim("a.py"), principal=BOB), 11
    )
    current, later = applied(
        current,
        request("later", claim("b.py", mode=AccessMode.READ), principal=ALICE),
        12,
    )
    assert isinstance(later.result, Accepted)
    later_record = current.claims[later.result.claim_id]
    assert later_record.fence is not None
    epoch = later_record.fence.entries[0].grant_epoch
    forged_base = replace(
        later_record,
        claims=(spec("a.py", AccessMode.READ),),
        fence=None,
    )
    forged = replace(forged_base, fence=make_fence(forged_base, epoch))
    claims = dict(current.claims)
    claims[forged.claim_id] = forged

    with pytest.raises(InvariantViolation, match="no-barging"):
        assert_invariants(replace(current, claims=claims))


def test_reducer_cannot_mask_transient_barging_inside_one_event_batch() -> None:
    current, holder = applied(
        state(), request("holder", claim("a.py", mode=AccessMode.READ)), 10
    )
    current, _ = applied(
        current, request("writer", claim("a.py"), principal=BOB), 11
    )
    current, later = applied(
        current,
        request("later-reader", claim("a.py", mode=AccessMode.READ)),
        12,
    )
    assert isinstance(holder.result, Accepted)
    assert isinstance(later.result, Accepted)
    later_record = current.claims[later.result.claim_id]
    forged_fence = make_fence(later_record, current.next_grant_epoch)

    with pytest.raises(InvariantViolation, match="admission ordering"):
        evolve(
            current,
            (
                ClaimReleased(holder.result.claim_id, 13),
                ClaimGranted(later.result.claim_id, 1_013, forged_fence, 13),
                ClaimReleased(later.result.claim_id, 13),
            ),
        )


def test_reducer_rejects_a_skipped_global_grant_epoch() -> None:
    current, holder = applied(state(), request("holder", claim("a.py")), 10)
    current, waiter = applied(
        current, request("waiter", claim("a.py"), principal=BOB), 11
    )
    assert isinstance(holder.result, Accepted)
    assert isinstance(waiter.result, Accepted)
    waiter_record = current.claims[waiter.result.claim_id]
    skipped = make_fence(waiter_record, current.next_grant_epoch + 1)

    with pytest.raises(InvariantViolation, match="next global epoch"):
        evolve(
            current,
            (
                ClaimReleased(holder.result.claim_id, 12),
                ClaimGranted(waiter.result.claim_id, 1_012, skipped, 12),
            ),
        )


def test_reducer_rejects_a_skipped_global_enqueue_sequence() -> None:
    decision = decide(state(), request("claim", claim("a.py")), 10)
    registered = next(
        event for event in decision.events if isinstance(event, ClaimRegistered)
    )
    skipped = replace(
        registered,
        record=replace(registered.record, enqueue_seq=registered.record.enqueue_seq + 1),
    )

    with pytest.raises(InvariantViolation, match="next global sequence"):
        evolve(state(), (skipped,))
