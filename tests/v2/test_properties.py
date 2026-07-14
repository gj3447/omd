from __future__ import annotations

from hypothesis import given, settings, strategies as st

from omd_server.v2.kernel import assert_invariants, decide, evolve
from omd_server.v2.model import ClaimCommand, CommandEnvelope, DomainState, Principal
from omd_server.v2.resource import (
    AccessMode,
    CaseMode,
    ClaimSpec,
    RepoPolicy,
    SelectorKind,
    canonicalize_resource,
)


DOMAIN = "symposium"
REPO = RepoPolicy(repo_id="omd", case_mode=CaseMode.SENSITIVE)


def make_resource(path: str, selector: SelectorKind = SelectorKind.EXACT):
    return canonicalize_resource(
        domain_id=DOMAIN,
        policy=REPO,
        raw_path=path,
        selector=selector,
    )


@settings(max_examples=150, deadline=None)
@given(
    requests=st.lists(
        st.tuples(
            st.sampled_from(["src/a.py", "src/b.py", "src/pkg", "docs/a.md"]),
            st.sampled_from([AccessMode.READ, AccessMode.WRITE]),
        ),
        min_size=1,
        max_size=30,
    )
)
def test_arbitrary_claim_stream_preserves_safety_invariants(requests) -> None:
    state = DomainState.empty(
        domain_id=DOMAIN,
        repo_policies=(REPO,),
        session_epochs={
            ("client", f"agent-{index}"): 1 for index in range(len(requests))
        },
    )

    for index, (path, mode) in enumerate(requests):
        command = ClaimCommand(
            claims=(ClaimSpec(make_resource(path), mode),),
            lease_ttl_ms=1_000,
            wait_timeout_ms=10_000,
        )
        request = CommandEnvelope.create(
            protocol_version=2,
            domain_id=DOMAIN,
            principal=Principal("client", f"agent-{index}", 1),
            request_id=f"request-{index}",
            command=command,
        )
        decision = decide(state, request, index + 1)
        state = evolve(state, decision.events)
        assert_invariants(state)


@settings(max_examples=100, deadline=None)
@given(paths=st.lists(st.sampled_from(["a", "b", "c", "d"]), min_size=1, max_size=4, unique=True))
def test_claim_permutation_has_canonical_fingerprint(paths) -> None:
    claims = tuple(
        ClaimSpec(make_resource(f"src/{path}.py"), AccessMode.WRITE)
        for path in paths
    )
    command = ClaimCommand(claims, lease_ttl_ms=1_000, wait_timeout_ms=1_000)
    reverse = ClaimCommand(
        tuple(reversed(claims)), lease_ttl_ms=1_000, wait_timeout_ms=1_000
    )
    kwargs = dict(
        protocol_version=2,
        domain_id=DOMAIN,
        principal=Principal("client", "agent", 1),
        request_id="same",
    )

    assert CommandEnvelope.create(command=command, **kwargs).fingerprint == CommandEnvelope.create(
        command=reverse, **kwargs
    ).fingerprint
