from __future__ import annotations

from dataclasses import replace

import pytest

from omd_server.v2.errors import ErrorCode
from omd_server.v2.kernel import assert_invariants, decide, evolve
from omd_server.v2.model import (
    Accepted,
    ClaimCommand,
    ClaimStatus,
    CommandEnvelope,
    DomainState,
    MaintenanceTick,
    Principal,
    Rejected,
    ReleaseCommand,
    RenewCommand,
)
from omd_server.v2.resource import (
    AccessMode,
    CaseMode,
    ClaimSpec,
    RepoPolicy,
    ResourceId,
    SelectorKind,
    canonicalize_resource,
)


DOMAIN = "symposium"
REPO = RepoPolicy(repo_id="omd", case_mode=CaseMode.SENSITIVE)
SESSIONS = {
    ("client-a", agent): 1
    for agent in ("agent-a", "agent-b", "agent-c", "maintenance", "other-agent")
}


def empty_state() -> DomainState:
    return DomainState.empty(
        domain_id=DOMAIN,
        repo_policies=(REPO,),
        session_epochs=SESSIONS,
    )


def principal(
    agent_id: str = "agent-a",
    *,
    client_id: str = "client-a",
    session_epoch: int = 1,
) -> Principal:
    return Principal(
        client_id=client_id,
        agent_id=agent_id,
        session_epoch=session_epoch,
    )


def resource(
    path: str, selector: SelectorKind = SelectorKind.EXACT
) -> ResourceId:
    return canonicalize_resource(
        domain_id=DOMAIN,
        policy=REPO,
        raw_path=path,
        selector=selector,
    )


def claim(
    *paths: str,
    mode: AccessMode = AccessMode.WRITE,
    ttl_ms: int = 1_000,
    wait_ms: int = 5_000,
) -> ClaimCommand:
    return ClaimCommand(
        claims=tuple(ClaimSpec(resource(path), mode) for path in paths),
        lease_ttl_ms=ttl_ms,
        wait_timeout_ms=wait_ms,
    )


def envelope(
    request_id: str,
    command,
    *,
    actor: Principal | None = None,
    protocol_version: int = 2,
    domain_id: str = DOMAIN,
) -> CommandEnvelope:
    return CommandEnvelope.create(
        protocol_version=protocol_version,
        domain_id=domain_id,
        principal=actor or principal(),
        request_id=request_id,
        command=command,
    )


def execute(
    state: DomainState, request: CommandEnvelope, now_ms: int
) -> tuple[DomainState, object]:
    decision = decide(state, request, now_ms)
    next_state = evolve(state, decision.events)
    assert_invariants(next_state)
    return next_state, decision


def accepted(decision) -> Accepted:
    assert isinstance(decision.result, Accepted), decision.result
    return decision.result


def rejected(decision, code: ErrorCode) -> Rejected:
    assert isinstance(decision.result, Rejected), decision.result
    assert decision.result.error.code is code
    return decision.result


def test_conflicting_write_claims_never_become_active_together() -> None:
    state, first = execute(empty_state(), envelope("r1", claim("src/a.py")), 10)
    state, second = execute(
        state,
        envelope("r2", claim("src/a.py"), actor=principal("agent-b")),
        11,
    )

    assert accepted(first).status is ClaimStatus.ACTIVE
    assert accepted(second).status is ClaimStatus.PENDING
    assert sum(c.status is ClaimStatus.ACTIVE for c in state.claims.values()) == 1


def test_read_claims_may_share_the_same_resource() -> None:
    state, first = execute(
        empty_state(), envelope("r1", claim("src/a.py", mode=AccessMode.READ)), 10
    )
    state, second = execute(
        state,
        envelope(
            "r2",
            claim("src/a.py", mode=AccessMode.READ),
            actor=principal("agent-b"),
        ),
        11,
    )

    assert accepted(first).status is ClaimStatus.ACTIVE
    assert accepted(second).status is ClaimStatus.ACTIVE
    assert sum(c.status is ClaimStatus.ACTIVE for c in state.claims.values()) == 2


def test_claim_set_is_all_or_none_when_one_resource_is_blocked() -> None:
    state, _ = execute(empty_state(), envelope("r1", claim("src/b.py")), 10)
    state, decision = execute(
        state,
        envelope(
            "r2",
            claim("src/a.py", "src/b.py"),
            actor=principal("agent-b"),
        ),
        11,
    )
    result = accepted(decision)
    record = state.claims[result.claim_id]

    assert result.status is ClaimStatus.PENDING
    assert record.fence is None
    assert record.lease_deadline_ms is None
    assert tuple(spec.resource.segments for spec in record.claims) == (
        ("src", "a.py"),
        ("src", "b.py"),
    )


def test_self_overlapping_claim_set_is_rejected() -> None:
    command = ClaimCommand(
        claims=(
            ClaimSpec(resource("src", SelectorKind.SUBTREE), AccessMode.WRITE),
            ClaimSpec(resource("src/a.py"), AccessMode.WRITE),
        ),
        lease_ttl_ms=1_000,
        wait_timeout_ms=1_000,
    )

    decision = decide(empty_state(), envelope("r1", command), 10)

    rejected(decision, ErrorCode.SELF_OVERLAPPING_CLAIM_SET)


def test_queued_writer_prevents_new_reader_barging() -> None:
    state, _ = execute(
        empty_state(), envelope("reader-1", claim("src/a.py", mode=AccessMode.READ)), 10
    )
    state, writer = execute(
        state,
        envelope(
            "writer",
            claim("src/a.py", mode=AccessMode.WRITE),
            actor=principal("agent-b"),
        ),
        11,
    )
    state, reader = execute(
        state,
        envelope(
            "reader-2",
            claim("src/a.py", mode=AccessMode.READ),
            actor=principal("agent-c"),
        ),
        12,
    )

    assert accepted(writer).status is ClaimStatus.PENDING
    assert accepted(reader).status is ClaimStatus.PENDING


def test_disjoint_request_is_not_globally_head_of_line_blocked() -> None:
    state, _ = execute(empty_state(), envelope("holder", claim("src/a.py")), 10)
    state, _ = execute(
        state,
        envelope(
            "blocked",
            claim("src/a.py"),
            actor=principal("agent-b"),
        ),
        11,
    )
    state, disjoint = execute(
        state,
        envelope(
            "disjoint",
            claim("src/z.py"),
            actor=principal("agent-c"),
        ),
        12,
    )

    assert accepted(disjoint).status is ClaimStatus.ACTIVE


def test_release_promotes_oldest_conflicting_waiter_with_requested_ttl() -> None:
    state, holder = execute(empty_state(), envelope("holder", claim("src/a.py")), 10)
    state, waiter = execute(
        state,
        envelope(
            "waiter",
            claim("src/a.py", ttl_ms=321),
            actor=principal("agent-b"),
        ),
        11,
    )
    holder_result = accepted(holder)
    holder_fence = state.claims[holder_result.claim_id].fence
    assert holder_fence is not None

    state, _ = execute(
        state,
        envelope(
            "release-holder",
            ReleaseCommand(holder_result.claim_id, holder_fence),
        ),
        50,
    )
    waiter_record = state.claims[accepted(waiter).claim_id]

    assert waiter_record.status is ClaimStatus.ACTIVE
    assert waiter_record.lease_deadline_ms == 371


def test_maintenance_tick_times_out_waiter_and_never_promotes_it() -> None:
    state, holder = execute(empty_state(), envelope("holder", claim("src/a.py")), 10)
    state, waiter = execute(
        state,
        envelope(
            "waiter",
            claim("src/a.py", wait_ms=20),
            actor=principal("agent-b"),
        ),
        11,
    )
    state, _ = execute(
        state,
        envelope("tick", MaintenanceTick(), actor=principal("maintenance")),
        31,
    )
    holder_result = accepted(holder)
    holder_fence = state.claims[holder_result.claim_id].fence
    assert holder_fence is not None
    state, _ = execute(
        state,
        envelope(
            "release-holder",
            ReleaseCommand(holder_result.claim_id, holder_fence),
        ),
        32,
    )

    assert state.claims[accepted(waiter).claim_id].status is ClaimStatus.TIMED_OUT


def test_regrant_uses_a_strictly_newer_global_fence_epoch() -> None:
    state, first = execute(empty_state(), envelope("first", claim("src/a.py")), 10)
    first_result = accepted(first)
    first_fence = state.claims[first_result.claim_id].fence
    assert first_fence is not None
    state, _ = execute(
        state,
        envelope("release", ReleaseCommand(first_result.claim_id, first_fence)),
        20,
    )
    state, second = execute(
        state,
        envelope("second", claim("src/a.py"), actor=principal("agent-b")),
        21,
    )
    second_fence = state.claims[accepted(second).claim_id].fence
    assert second_fence is not None

    assert second_fence.entries[0].grant_epoch > first_fence.entries[0].grant_epoch


def test_fence_vector_rejects_missing_or_tampered_entries() -> None:
    state, decision = execute(
        empty_state(), envelope("claim", claim("src/a.py", "src/b.py")), 10
    )
    result = accepted(decision)
    fence = state.claims[result.claim_id].fence
    assert fence is not None
    incomplete = replace(fence, entries=fence.entries[:1])

    rejected(
        decide(
            state,
            envelope("release", ReleaseCommand(result.claim_id, incomplete)),
            11,
        ),
        ErrorCode.STALE_FENCE_VECTOR,
    )


def test_exact_principal_ownership_includes_session_epoch() -> None:
    state, decision = execute(empty_state(), envelope("claim", claim("src/a.py")), 10)
    result = accepted(decision)
    fence = state.claims[result.claim_id].fence
    assert fence is not None
    rolled = dict(state.session_epochs)
    rolled[("client-a", "agent-a")] = 2
    state = replace(state, session_epochs=rolled)

    rejected(
        decide(
            state,
            envelope(
                "release",
                ReleaseCommand(result.claim_id, fence),
                actor=principal(session_epoch=2),
            ),
            11,
        ),
        ErrorCode.NOT_OWNER,
    )


def test_renew_requires_current_fence_and_preserves_it() -> None:
    state, decision = execute(empty_state(), envelope("claim", claim("src/a.py")), 10)
    result = accepted(decision)
    fence = state.claims[result.claim_id].fence
    assert fence is not None
    state, renewed = execute(
        state,
        envelope("renew", RenewCommand(result.claim_id, fence, lease_ttl_ms=2_000)),
        20,
    )

    assert accepted(renewed).fence == fence
    assert state.claims[result.claim_id].lease_deadline_ms == 2_020


def test_same_idempotency_key_and_command_replays_without_events() -> None:
    request = envelope("same", claim("src/a.py"))
    state, first = execute(empty_state(), request, 10)
    replay = decide(state, request, 11)

    assert replay.events == ()
    assert replay.effects == ()
    assert accepted(replay).replayed is True
    assert accepted(replay).claim_id == accepted(first).claim_id


@pytest.mark.parametrize(
    ("changed", "code"),
    [
        (envelope("same", claim("src/b.py")), ErrorCode.IDEMPOTENCY_KEY_REUSE),
        (
            envelope("same", claim("src/a.py"), actor=principal("other-agent")),
            ErrorCode.IDEMPOTENCY_KEY_REUSE,
        ),
        (
            envelope("same", claim("src/a.py"), actor=principal(session_epoch=2)),
            ErrorCode.STALE_SESSION,
        ),
    ],
)
def test_idempotency_key_reuse_with_different_identity_is_rejected(
    changed, code: ErrorCode
) -> None:
    state, _ = execute(empty_state(), envelope("same", claim("src/a.py")), 10)

    rejected(decide(state, changed, 11), code)


def test_replay_projects_current_state_instead_of_cached_pending_json() -> None:
    state, holder = execute(empty_state(), envelope("holder", claim("src/a.py")), 10)
    waiter_request = envelope(
        "waiter", claim("src/a.py"), actor=principal("agent-b")
    )
    state, waiter = execute(state, waiter_request, 11)
    assert accepted(waiter).status is ClaimStatus.PENDING
    holder_result = accepted(holder)
    holder_fence = state.claims[holder_result.claim_id].fence
    assert holder_fence is not None
    state, _ = execute(
        state,
        envelope("release", ReleaseCommand(holder_result.claim_id, holder_fence)),
        20,
    )

    replay = decide(state, waiter_request, 21)

    assert replay.events == ()
    assert accepted(replay).status is ClaimStatus.ACTIVE
    assert accepted(replay).fence is not None


def test_claim_input_order_has_one_fingerprint_and_identity() -> None:
    claims_ab = ClaimCommand(
        claims=(
            ClaimSpec(resource("src/a.py"), AccessMode.WRITE),
            ClaimSpec(resource("src/b.py"), AccessMode.WRITE),
        ),
        lease_ttl_ms=1_000,
        wait_timeout_ms=1_000,
    )
    claims_ba = replace(claims_ab, claims=tuple(reversed(claims_ab.claims)))
    request_ab = envelope("same", claims_ab)
    request_ba = envelope("same", claims_ba)

    assert request_ab.fingerprint == request_ba.fingerprint
    state, first = execute(empty_state(), request_ab, 10)
    replay = decide(state, request_ba, 11)
    assert accepted(replay).claim_id == accepted(first).claim_id


def test_decide_is_deterministic_and_does_not_mutate_input_state() -> None:
    state = empty_state()
    before = state
    request = envelope("r1", claim("src/a.py"))

    left = decide(state, request, 10)
    right = decide(state, request, 10)

    assert left == right
    assert state == before
    assert state.claims == {}


def test_clock_regression_is_rejected() -> None:
    state, _ = execute(empty_state(), envelope("r1", claim("src/a.py")), 100)

    rejected(
        decide(
            state,
            envelope("r2", claim("src/b.py"), actor=principal("agent-b")),
            99,
        ),
        ErrorCode.CLOCK_REGRESSION,
    )


@pytest.mark.parametrize(
    ("envelope_case", "code"),
    [
        (
            envelope(
                "empty",
                ClaimCommand(claims=(), lease_ttl_ms=1_000, wait_timeout_ms=1_000),
            ),
            ErrorCode.EMPTY_CLAIM_SET,
        ),
        (envelope("ttl", claim("src/a.py", ttl_ms=0)), ErrorCode.INVALID_TTL),
        (
            envelope("wait", claim("src/a.py", wait_ms=0)),
            ErrorCode.INVALID_WAIT_TIMEOUT,
        ),
        (
            envelope("protocol", claim("src/a.py"), protocol_version=1),
            ErrorCode.UNSUPPORTED_PROTOCOL,
        ),
        (
            envelope("domain", claim("src/a.py"), domain_id="other"),
            ErrorCode.DOMAIN_MISMATCH,
        ),
    ],
)
def test_invalid_commands_fail_closed(envelope_case, code: ErrorCode) -> None:
    rejected(decide(empty_state(), envelope_case, 10), code)


def test_unregistered_repository_is_rejected() -> None:
    foreign = ResourceId(
        domain_id=DOMAIN,
        repo_id="unknown",
        segments=("src", "a.py"),
        selector=SelectorKind.EXACT,
    )
    command = ClaimCommand(
        claims=(ClaimSpec(foreign, AccessMode.WRITE),),
        lease_ttl_ms=1_000,
        wait_timeout_ms=1_000,
    )

    rejected(
        decide(empty_state(), envelope("unknown-repo", command), 10),
        ErrorCode.UNKNOWN_REPOSITORY,
    )
