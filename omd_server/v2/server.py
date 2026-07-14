"""FastMCP surface for the OMD v2 lease-only runtime profile."""

from __future__ import annotations

import getpass
import asyncio
import logging
import os
import socket
import time
import uuid
from functools import partial
from pathlib import Path

from .model import Principal
from .profile import LEASE_ONLY_CAPABILITIES, LeaseService, ResourceRequest
from .resource import CaseMode, RepoPolicy
from .store import SQLiteCoordinationStore


LOGGER = logging.getLogger(__name__)


LEASE_ONLY_INSTRUCTIONS = """\
OMD v2 lease-only coordinates canonical repository resource claim-sets.
The stdio runtime binds one server-issued principal/session to the transport;
callers cannot choose identity fields. Claims are all-or-none and renew/release
require the complete FenceVector returned by claim_set. Status tools are pure
reads. A runtime-owned supervisor linearizes expiries and waiter promotion.

This local stdio profile deliberately has NO Git, merge, commit, worktree,
task lifecycle, or push capabilities. It is a coordination boundary, not an
authentication boundary against processes that can open the SQLite file.
"""


try:
    import anyio
    from fastmcp import FastMCP
    from fastmcp.server.lifespan import lifespan
except ImportError:  # pragma: no cover - optional server extra
    anyio = None
    FastMCP = None
    lifespan = None


def default_db_path() -> Path:
    """Return one cwd-independent persistent path for all stdio sessions."""

    explicit = os.environ.get("OMD_V2_DB_PATH")
    if explicit:
        return Path(explicit).expanduser().resolve()
    state_root = os.environ.get("OMD_V2_STATE_DIR") or os.environ.get(
        "XDG_STATE_HOME"
    )
    root = Path(state_root).expanduser() if state_root else Path.home() / ".local/state"
    return (root / "omd" / "v2" / "lease.db").resolve()


def _wall_clock_ms() -> int:
    return time.time_ns() // 1_000_000


def _runtime_identity(
    client_id: str | None, agent_id: str | None
) -> tuple[str, str]:
    transport_id = uuid.uuid4().hex
    agent = agent_id or os.environ.get("OMD_V2_AGENT_ID")
    if agent is None:
        agent = f"stdio-{transport_id}"
    client = client_id or os.environ.get("OMD_V2_CLIENT_ID")
    if client is None:
        client = f"local:{getpass.getuser()}@{socket.gethostname()}:{agent}"
    return client, agent


def _resource_requests(resources: list[dict[str, str]]) -> tuple[ResourceRequest, ...]:
    # LeaseService owns enum/path validation so malformed application inputs
    # travel through its durable idempotency rejection path.
    return tuple(
        ResourceRequest(
            path=resource.get("path", ""),
            mode=resource.get("mode", "write"),  # type: ignore[arg-type]
            selector=resource.get("selector", "exact"),  # type: ignore[arg-type]
            repo_id=resource.get("repo_id") or None,
        )
        for resource in resources
    )


async def _maintenance_loop(
    service: LeaseService,
    principal: Principal,
    interval_ms: int,
) -> None:
    """Drive explicit maintenance without hiding failures or mutating reads."""

    assert anyio is not None
    while True:
        deadline = await anyio.to_thread.run_sync(
            service.next_maintenance_deadline_ms
        )
        now_ms = service.now_ms()
        if deadline is not None and deadline <= now_ms:
            result = await anyio.to_thread.run_sync(
                partial(
                    service.maintenance_tick,
                    principal=principal,
                    request_id=f"maintenance:{deadline}",
                    observed_now_ms=now_ms,
                )
            )
            if result.get("ok") is not True:
                raise RuntimeError(f"maintenance tick rejected: {result}")
            await anyio.sleep(0)
            continue
        delay_ms = interval_ms
        if deadline is not None:
            delay_ms = min(interval_ms, max(1, deadline - now_ms))
        await anyio.sleep(delay_ms / 1_000)


def build_lease_server(
    db_path: str | os.PathLike[str] | None = None,
    *,
    domain_id: str = "default",
    repo_policy: RepoPolicy | None = None,
    client_id: str | None = None,
    agent_id: str | None = None,
    maintenance_interval_ms: int = 250,
):
    if FastMCP is None or anyio is None or lifespan is None:
        raise RuntimeError("fastmcp is not installed; install omd[server]")
    if type(maintenance_interval_ms) is not int or maintenance_interval_ms <= 0:
        raise ValueError("maintenance_interval_ms must be a positive integer")

    policy = repo_policy or RepoPolicy(repo_id="repo")
    store = SQLiteCoordinationStore(db_path or default_db_path())
    store.initialize()
    now_ms = _wall_clock_ms()
    store.create_domain(
        domain_id=domain_id, repo_policies=(policy,), created_at_ms=now_ms
    )
    transport_client, transport_agent = _runtime_identity(client_id, agent_id)
    principal = store.register_session(
        domain_id=domain_id,
        client_id=transport_client,
        agent_id=transport_agent,
        clock_ms=_wall_clock_ms,
    )
    runtime_identity = uuid.uuid4().hex
    maintenance_principal = store.register_session(
        domain_id=domain_id,
        client_id=f"__omd_v2_runtime__:{runtime_identity}",
        agent_id="maintenance",
        clock_ms=_wall_clock_ms,
    )
    service = LeaseService(store, domain_id)
    supervisor_failure: dict[str, BaseException] = {}

    def ensure_supervisor() -> None:
        error = supervisor_failure.get("error")
        if error is not None:
            raise RuntimeError("lease maintenance supervisor failed") from error

    @lifespan
    async def lease_lifespan(_server):
        task = asyncio.create_task(
            _maintenance_loop(
                service, maintenance_principal, maintenance_interval_ms
            ),
            name="omd-v2-maintenance",
        )

        def record_failure(completed: asyncio.Task) -> None:
            if completed.cancelled():
                return
            error = completed.exception()
            if error is not None:
                supervisor_failure["error"] = error
                LOGGER.critical(
                    "OMD v2 maintenance supervisor stopped",
                    exc_info=(type(error), error, error.__traceback__),
                )

        task.add_done_callback(record_failure)
        try:
            yield {"lease_service": service, "principal": principal}
        finally:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

    mcp = FastMCP(
        "omd-v2-lease",
        instructions=LEASE_ONLY_INSTRUCTIONS,
        lifespan=lease_lifespan,
    )

    @mcp.tool()
    def about() -> dict[str, object]:
        """Describe the narrow lease-only profile and bound local session."""
        ensure_supervisor()
        return {
            "name": "OMD v2 lease-only",
            "domain_id": domain_id,
            "repo_id": policy.repo_id,
            "principal": {
                "client_id": principal.client_id,
                "agent_id": principal.agent_id,
                "session_epoch": principal.session_epoch,
            },
            "trust_boundary": "local-stdio-and-sqlite-file-permissions",
            "capabilities": [
                {"name": item.name, "mutates": item.mutates}
                for item in LEASE_ONLY_CAPABILITIES
            ],
            "excluded": ["git", "merge", "commit", "worktree", "push"],
        }

    @mcp.tool()
    def claim_set(
        request_id: str,
        resources: list[dict[str, str]],
        lease_ttl_ms: int = 600_000,
        wait_timeout_ms: int = 600_000,
    ) -> dict[str, object]:
        """Atomically grant or queue a canonical resource claim-set."""
        ensure_supervisor()
        return service.claim_set(
            principal=principal,
            request_id=request_id,
            resources=_resource_requests(resources),
            lease_ttl_ms=lease_ttl_ms,
            wait_timeout_ms=wait_timeout_ms,
        )

    @mcp.tool()
    def renew_claim_set(
        request_id: str,
        claim_id: str,
        fence: dict[str, object],
        lease_ttl_ms: int = 600_000,
    ) -> dict[str, object]:
        """Renew an ACTIVE claim using the exact current FenceVector."""
        ensure_supervisor()
        return service.renew_claim_set(
            principal=principal,
            request_id=request_id,
            claim_id=claim_id,
            fence=fence,
            lease_ttl_ms=lease_ttl_ms,
        )

    @mcp.tool()
    def release_claim_set(
        request_id: str,
        claim_id: str,
        fence: dict[str, object],
    ) -> dict[str, object]:
        """Release an ACTIVE claim using the exact current FenceVector."""
        ensure_supervisor()
        return service.release_claim_set(
            principal=principal,
            request_id=request_id,
            claim_id=claim_id,
            fence=fence,
        )

    @mcp.tool()
    def claim_status(claim_id: str) -> dict[str, object]:
        """Pure read of one claim owned by this bound session."""
        ensure_supervisor()
        return service.claim_status(claim_id, principal=principal)

    @mcp.tool()
    def domain_status() -> dict[str, object]:
        """Pure read of aggregate counts and this session's claims."""
        ensure_supervisor()
        return service.domain_status(principal=principal)

    return mcp


def main() -> None:
    import sys

    case_mode = CaseMode(os.environ.get("OMD_V2_CASE_MODE", "sensitive"))
    db_path = sys.argv[1] if len(sys.argv) > 1 else None
    build_lease_server(
        db_path,
        domain_id=os.environ.get("OMD_V2_DOMAIN_ID", "default"),
        repo_policy=RepoPolicy(
            repo_id=os.environ.get("OMD_V2_REPO_ID", "repo"),
            case_mode=case_mode,
        ),
        maintenance_interval_ms=int(
            os.environ.get("OMD_V2_MAINTENANCE_INTERVAL_MS", "250")
        ),
    ).run(show_banner=False, log_level="WARNING")


if __name__ == "__main__":
    main()
