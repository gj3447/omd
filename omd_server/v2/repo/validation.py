"""Transport-independent validation for repo integration requests."""

from __future__ import annotations

from ..fencing import fence_digest
from ..model import FenceVector
from .contracts import (
    IntegrateRequest,
    RegisteredRepository,
    RepositoryRegistry,
    require_safe_id,
)
from .errors import RepoConfigurationError


def validate_integrate_request(
    request: IntegrateRequest, registry: RepositoryRegistry
) -> RegisteredRepository:
    if not isinstance(request, IntegrateRequest) or request.protocol_version != 1:
        raise RepoConfigurationError("repo protocol version 1 is required")
    require_safe_id(request.operation_id, "operation_id")
    require_safe_id(request.repo_id, "repo_id")
    for field, value in (
        ("domain_id", request.domain_id),
        ("client_id", request.client_id),
        ("request_id", request.request_id),
        ("claim_id", request.claim_id),
    ):
        if not isinstance(value, str) or not value or "\x00" in value or len(value) > 256:
            raise RepoConfigurationError(f"invalid {field}")
    if not isinstance(request.fence, FenceVector):
        raise RepoConfigurationError("complete FenceVector is required")
    if request.fence.claim_id != request.claim_id:
        raise RepoConfigurationError("claim and fence IDs disagree")
    if request.fence.owner.client_id != request.client_id:
        raise RepoConfigurationError("request client does not own the fence")
    if not request.fence.entries:
        raise RepoConfigurationError("empty FenceVector")
    expected_digest = fence_digest(
        request.fence.claim_id, request.fence.owner, request.fence.entries
    )
    if expected_digest != request.fence.vector_digest:
        raise RepoConfigurationError("FenceVector integrity mismatch")
    for entry in request.fence.entries:
        if (
            entry.resource.domain_id != request.domain_id
            or entry.resource.repo_id != request.repo_id
        ):
            raise RepoConfigurationError("fence resource is in another domain/repo")
    return registry.get(request.repo_id)
