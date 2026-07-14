from __future__ import annotations

import ast
import inspect
import sys
from datetime import timedelta
from pathlib import Path

import anyio

from omd_server.v2.errors import ErrorCode
from omd_server.v2.model import ClaimStatus, Principal
from omd_server.v2.profile import (
    LEASE_ONLY_CAPABILITIES,
    LeaseService,
    ResourceRequest,
)
from omd_server.v2.resource import AccessMode, CaseMode, RepoPolicy, SelectorKind
from omd_server.v2.server import build_lease_server
from omd_server.v2.store import SQLiteCoordinationStore


DOMAIN = "symposium"
REPO = RepoPolicy(repo_id="omd", case_mode=CaseMode.SENSITIVE)


class FakeClock:
    def __init__(self, now_ms: int = 10):
        self.now_ms = now_ms

    def __call__(self) -> int:
        return self.now_ms


def service(tmp_path: Path) -> tuple[LeaseService, SQLiteCoordinationStore, FakeClock]:
    store = SQLiteCoordinationStore(tmp_path / "lease.db")
    store.initialize()
    store.create_domain(domain_id=DOMAIN, repo_policies=(REPO,))
    store.register_session(
        domain_id=DOMAIN,
        client_id="client",
        agent_id="agent-a",
        registered_at_ms=0,
    )
    store.register_session(
        domain_id=DOMAIN,
        client_id="runtime",
        agent_id="maintenance",
        registered_at_ms=0,
    )
    clock = FakeClock()
    return LeaseService(store, DOMAIN, clock_ms=clock), store, clock


def test_lease_profile_has_no_git_or_task_lifecycle_capabilities() -> None:
    names = {capability.name for capability in LEASE_ONLY_CAPABILITIES}

    assert names == {
        "about",
        "claim_set",
        "renew_claim_set",
        "release_claim_set",
        "claim_status",
        "domain_status",
    }
    assert not names & {"connect", "commit", "start", "finish", "push", "merge"}


def test_profile_import_graph_does_not_reference_legacy_core_or_git() -> None:
    import omd_server.v2.profile as profile_module
    import omd_server.v2.server as server_module

    imported: set[str] = set()
    for module in (profile_module, server_module):
        tree = ast.parse(inspect.getsource(module))
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                imported.update(alias.name for alias in node.names)
            elif isinstance(node, ast.ImportFrom):
                imported.add(node.module or "")

    assert not any(name.endswith(("core", "gitio")) for name in imported)


def test_service_claim_release_round_trip_requires_exact_fence(
    tmp_path: Path,
) -> None:
    lease, _, clock = service(tmp_path)
    owner = Principal("client", "agent-a", 1)
    claimed = lease.claim_set(
        principal=owner,
        request_id="claim",
        resources=(ResourceRequest("src/a.py", AccessMode.WRITE),),
        lease_ttl_ms=1_000,
        wait_timeout_ms=5_000,
    )
    assert claimed["ok"] is True
    assert claimed["status"] == ClaimStatus.ACTIVE.value
    assert claimed["fence"] is not None
    revision = claimed["revision"]

    wrong_owner = lease.release_claim_set(
        principal=Principal("client", "agent-a", 2),
        request_id="wrong-session",
        claim_id=claimed["claim_id"],
        fence=claimed["fence"],
    )
    assert wrong_owner["ok"] is False
    assert wrong_owner["error"]["code"] == ErrorCode.STALE_SESSION.value

    clock.now_ms = 20
    released = lease.release_claim_set(
        principal=owner,
        request_id="release",
        claim_id=claimed["claim_id"],
        fence=claimed["fence"],
    )
    assert released["ok"] is True
    assert released["status"] == ClaimStatus.RELEASED.value
    # A stale transport session is rejected before application idempotency.
    assert released["revision"] == revision + 1


def test_status_is_a_pure_projection_and_never_ticks(tmp_path: Path) -> None:
    lease, store, clock = service(tmp_path)
    claimed = lease.claim_set(
        principal=Principal("client", "agent-a", 1),
        request_id="claim",
        resources=(ResourceRequest("src/a.py", AccessMode.WRITE),),
        lease_ttl_ms=5,
        wait_timeout_ms=5_000,
    )
    before = store.read_domain(DOMAIN)
    clock.now_ms = 1_000

    status = lease.claim_status(claimed["claim_id"])
    domain = lease.domain_status()
    after = store.read_domain(DOMAIN)

    assert status["status"] == ClaimStatus.ACTIVE.value
    assert domain["revision"] == before.revision
    assert after == before


def test_explicit_internal_tick_expires_lease(tmp_path: Path) -> None:
    lease, _, clock = service(tmp_path)
    claimed = lease.claim_set(
        principal=Principal("client", "agent-a", 1),
        request_id="claim",
        resources=(ResourceRequest("src/a.py", AccessMode.WRITE),),
        lease_ttl_ms=5,
        wait_timeout_ms=5_000,
    )
    clock.now_ms = 15

    lease.maintenance_tick(
        principal=Principal("runtime", "maintenance", 1), request_id="tick-15"
    )

    assert lease.claim_status(claimed["claim_id"])["status"] == ClaimStatus.EXPIRED.value


def test_subtree_selector_is_explicit_not_an_arbitrary_glob(tmp_path: Path) -> None:
    lease, _, _ = service(tmp_path)

    claimed = lease.claim_set(
        principal=Principal("client", "agent-a", 1),
        request_id="claim",
        resources=(
            ResourceRequest(
                "src/pkg", AccessMode.WRITE, selector=SelectorKind.SUBTREE
            ),
        ),
        lease_ttl_ms=1_000,
        wait_timeout_ms=5_000,
    )

    assert claimed["ok"] is True
    assert claimed["resources"][0]["selector"] == "subtree"


def test_fastmcp_surface_is_exactly_the_lease_profile(tmp_path: Path) -> None:
    mcp = build_lease_server(
        str(tmp_path / "server.db"), domain_id=DOMAIN, repo_policy=REPO
    )

    async def list_names() -> set[str]:
        return {tool.name for tool in await mcp.list_tools()}

    names = anyio.run(list_names)
    assert names == {capability.name for capability in LEASE_ONLY_CAPABILITIES}


def test_stdio_initializes_with_only_lease_tools(tmp_path: Path) -> None:
    from mcp.client.session import ClientSession
    from mcp.client.stdio import StdioServerParameters, stdio_client

    async def smoke() -> None:
        params = StdioServerParameters(
            command=sys.executable,
            args=["-m", "omd_server.v2.server", str(tmp_path / "stdio.db")],
            cwd=str(tmp_path),
        )
        async with stdio_client(params) as streams:
            async with ClientSession(
                *streams, read_timeout_seconds=timedelta(seconds=8)
            ) as session:
                initialized = await session.initialize()
                tools = await session.list_tools()
                assert initialized.serverInfo.name == "omd-v2-lease"
                assert {tool.name for tool in tools.tools} == {
                    capability.name for capability in LEASE_ONLY_CAPABILITIES
                }

    anyio.run(smoke)
