"""Isolated worktree-free Git saga engine.

This subpackage is intentionally not imported by :mod:`omd_server.v2` and has
no MCP or command-line surface. A durable mutation-authority adapter is a
mandatory constructor dependency; the lease-only profile provides none.
"""

from .contracts import (
    AuthorityRequest,
    CommitMetadata,
    IntegrateRequest,
    MutationAuthority,
    MutationReservation,
    RegisteredRepository,
    RepositoryRegistry,
    SagaRecord,
    SagaStatus,
)
from .errors import (
    AuthorityRejected,
    GitExecutionError,
    IdempotencyConflict,
    RepoConfigurationError,
    RepoSagaError,
)
from .service import RepoSagaService
from .store import SQLiteRepoSagaStore

__all__ = [
    "AuthorityRejected",
    "AuthorityRequest",
    "CommitMetadata",
    "GitExecutionError",
    "IdempotencyConflict",
    "IntegrateRequest",
    "MutationAuthority",
    "MutationReservation",
    "RegisteredRepository",
    "RepoConfigurationError",
    "RepoSagaError",
    "RepoSagaService",
    "RepositoryRegistry",
    "SagaRecord",
    "SagaStatus",
    "SQLiteRepoSagaStore",
]
