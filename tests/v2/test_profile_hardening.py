from __future__ import annotations

import os
import sys
from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta
from pathlib import Path
from threading import Event

import anyio

from omd_server.v2.errors import ErrorCode
from omd_server.v2.model import ClaimStatus, Principal
from omd_server.v2.profile import LeaseService, ResourceRequest
from omd_server.v2.resource import AccessMode, RepoPolicy
from omd_server.v2.server import _runtime_identity, build_lease_server, default_db_path
from omd_server.v2.store import SQLiteCoordinationStore


DOMAIN = "hardening"
REPO = RepoPolicy("repo")
OWNER = Principal("client", "agent", 1)


class Clock:
    now_ms = 10

    def __call__(self) -> int:
        return self.now_ms


def make_service(tmp_path: Path, cls=LeaseService):
    store = SQLiteCoordinationStore(tmp_path / "lease.db")
    store.initialize()
    store.create_domain(domain_id=DOMAIN, repo_policies=(REPO,))
    store.register_session(
        domain_id=DOMAIN,
        client_id=OWNER.client_id,
        agent_id=OWNER.agent_id,
        registered_at_ms=0,
    )
    clock = Clock()
    return cls(store, DOMAIN, clock_ms=clock), store, clock


def claim(lease: LeaseService, request_id: str = "claim"):
    return lease.claim_set(
        principal=OWNER,
        request_id=request_id,
        resources=(ResourceRequest("a.py", AccessMode.WRITE),),
        lease_ttl_ms=1_000,
        wait_timeout_ms=5_000,
    )


def test_malformed_mutation_is_durably_idempotent(tmp_path: Path) -> None:
    lease, _, _ = make_service(tmp_path)
    granted = claim(lease)

    malformed = lease.release_claim_set(
        principal=OWNER,
        request_id="release-once",
        claim_id=granted["claim_id"],
        fence={},
    )
    changed_retry = lease.release_claim_set(
        principal=OWNER,
        request_id="release-once",
        claim_id=granted["claim_id"],
        fence=granted["fence"],
    )

    assert malformed["error"]["code"] == ErrorCode.STALE_FENCE_VECTOR.value
    assert changed_retry["error"]["code"] == ErrorCode.IDEMPOTENCY_KEY_REUSE.value
    assert lease.claim_status(granted["claim_id"])["status"] == "active"


def test_mutation_response_uses_the_committed_revision_snapshot(tmp_path: Path) -> None:
    entered = Event()
    resume = Event()

    class PausingService(LeaseService):
        pause_once = True

        def _receipt_to_wire(self, receipt):
            if self.pause_once:
                self.pause_once = False
                entered.set()
                assert resume.wait(timeout=5)
            return super()._receipt_to_wire(receipt)

    lease, store, clock = make_service(tmp_path, PausingService)
    concurrent = LeaseService(store, DOMAIN, clock_ms=clock)
    with ThreadPoolExecutor(max_workers=1) as pool:
        future = pool.submit(claim, lease)
        assert entered.wait(timeout=5)
        snapshot = store.read_domain(DOMAIN)
        record = next(iter(snapshot.state.claims.values()))
        assert record.fence is not None
        clock.now_ms = 11
        released = concurrent.release_claim_set(
            principal=OWNER,
            request_id="release",
            claim_id=record.claim_id,
            fence={
                "claim_id": record.fence.claim_id,
                "owner": {
                    "client_id": OWNER.client_id,
                    "agent_id": OWNER.agent_id,
                    "session_epoch": OWNER.session_epoch,
                },
                "entries": [
                    {
                        "resource": {
                            "domain_id": entry.resource.domain_id,
                            "repo_id": entry.resource.repo_id,
                            "segments": list(entry.resource.segments),
                            "selector": entry.resource.selector.value,
                        },
                        "grant_epoch": entry.grant_epoch,
                    }
                    for entry in record.fence.entries
                ],
                "vector_digest": record.fence.vector_digest,
            },
        )
        assert released["status"] == "released"
        resume.set()
        original = future.result(timeout=5)

    assert original["revision"] == 1
    assert original["status"] == "active"
    assert original["fence"] is not None
    assert store.read_domain(DOMAIN).revision == 2


def test_mcp_mutations_have_no_caller_supplied_identity_fields(tmp_path: Path) -> None:
    mcp = build_lease_server(
        tmp_path / "server.db",
        domain_id=DOMAIN,
        repo_policy=REPO,
        client_id="client",
        agent_id="transport",
    )

    async def schemas():
        return {tool.name: tool.parameters for tool in await mcp.list_tools()}

    tool_schemas = anyio.run(schemas)
    for name in ("claim_set", "renew_claim_set", "release_claim_set"):
        properties = tool_schemas[name]["properties"]
        assert not {"client_id", "agent_id", "session_epoch"} & properties.keys()


def test_default_identity_is_isolated_but_stable_agent_rolls_over(
    monkeypatch,
) -> None:
    monkeypatch.delenv("OMD_V2_CLIENT_ID", raising=False)
    monkeypatch.delenv("OMD_V2_AGENT_ID", raising=False)
    first_default = _runtime_identity(None, None)
    second_default = _runtime_identity(None, None)
    assert first_default[0] != second_default[0]
    assert first_default[1] != second_default[1]

    monkeypatch.setenv("OMD_V2_AGENT_ID", "stable-worker")
    first_stable = _runtime_identity(None, None)
    second_stable = _runtime_identity(None, None)
    assert first_stable == second_stable


def test_multiple_supervisors_use_independent_idempotency_namespaces(
    tmp_path: Path,
) -> None:
    lease, store, clock = make_service(tmp_path)
    granted = claim(lease)
    first = store.register_session(
        domain_id=DOMAIN,
        client_id="__omd_v2_runtime__:one",
        agent_id="maintenance",
        registered_at_ms=1,
    )
    second = store.register_session(
        domain_id=DOMAIN,
        client_id="__omd_v2_runtime__:two",
        agent_id="maintenance",
        registered_at_ms=1,
    )
    clock.now_ms = granted["lease_deadline_ms"]

    def tick(principal: Principal):
        return lease.maintenance_tick(
            principal=principal,
            request_id=f"maintenance:{granted['lease_deadline_ms']}",
        )

    with ThreadPoolExecutor(max_workers=2) as pool:
        results = tuple(pool.map(tick, (first, second)))

    assert all(result["ok"] is True for result in results)
    assert lease.claim_status(granted["claim_id"])["status"] == "expired"


def test_due_tick_carries_observed_deadline_across_clock_rollback(
    tmp_path: Path,
) -> None:
    lease, _, clock = make_service(tmp_path)
    granted = lease.claim_set(
        principal=OWNER,
        request_id="short",
        resources=(ResourceRequest("a.py", AccessMode.WRITE),),
        lease_ttl_ms=5,
        wait_timeout_ms=5_000,
    )
    deadline = granted["lease_deadline_ms"]
    clock.now_ms = deadline
    observed_now_ms = lease.now_ms()
    clock.now_ms = deadline - 1

    tick = lease.maintenance_tick(
        principal=OWNER,
        request_id=f"maintenance:{deadline}",
        observed_now_ms=observed_now_ms,
    )

    assert tick["ok"] is True
    assert lease.claim_status(granted["claim_id"])["status"] == "expired"


def test_stdio_runtime_automatically_expires_a_lease(tmp_path: Path) -> None:
    from mcp.client.session import ClientSession
    from mcp.client.stdio import StdioServerParameters, stdio_client

    async def exercise() -> None:
        env = dict(os.environ)
        env.update(
            OMD_V2_CLIENT_ID="stdio-test",
            OMD_V2_AGENT_ID="expiry-test",
            OMD_V2_MAINTENANCE_INTERVAL_MS="10",
        )
        params = StdioServerParameters(
            command=sys.executable,
            args=["-m", "omd_server.v2.server", str(tmp_path / "stdio.db")],
            cwd=str(tmp_path),
            env=env,
        )
        async with stdio_client(params) as streams:
            async with ClientSession(
                *streams, read_timeout_seconds=timedelta(seconds=8)
            ) as session:
                await session.initialize()
                granted = await session.call_tool(
                    "claim_set",
                    {
                        "request_id": "short-lease",
                        "resources": [{"path": "a.py"}],
                        "lease_ttl_ms": 40,
                        "wait_timeout_ms": 1_000,
                    },
                )
                waiter = await session.call_tool(
                    "claim_set",
                    {
                        "request_id": "queued-claim",
                        "resources": [{"path": "a.py"}],
                        "lease_ttl_ms": 1_000,
                        "wait_timeout_ms": 1_000,
                    },
                )
                assert waiter.structuredContent["status"] == "pending"
                await anyio.sleep(0.15)
                status = await session.call_tool(
                    "claim_status",
                    {"claim_id": granted.structuredContent["claim_id"]},
                )
                assert status.structuredContent["status"] == "expired"
                promoted = await session.call_tool(
                    "claim_status",
                    {"claim_id": waiter.structuredContent["claim_id"]},
                )
                assert promoted.structuredContent["status"] == "active"

    anyio.run(exercise)


def test_default_database_path_is_persistent_and_cwd_independent() -> None:
    path = default_db_path()

    assert path.is_absolute()
    assert path.parts[-3:] == ("omd", "v2", "lease.db")
