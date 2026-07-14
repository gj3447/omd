"""OMD v2 functional coordination kernel.

This package is intentionally independent from the legacy Coordinator.  It
contains typed domain state, the pure transition kernel, and an explicit local
SQLite/service boundary. Task lifecycle remains outside v2. The sibling
``v2.repo`` engine library is deliberately not imported into this lease
profile.
"""

from .kernel import assert_invariants, decide, evolve
from .model import (
    ClaimCommand,
    CommandEnvelope,
    Decision,
    DomainState,
    FenceVector,
    Principal,
    ReleaseCommand,
    RenewCommand,
)
from .profile import LeaseService, ResourceRequest
from .resource import (
    AccessMode,
    CaseMode,
    ClaimSpec,
    RepoPolicy,
    ResourceId,
    SelectorKind,
)
from .store import SQLiteCoordinationStore, StoreCorruptionError

__all__ = [
    "AccessMode",
    "CaseMode",
    "ClaimCommand",
    "ClaimSpec",
    "CommandEnvelope",
    "Decision",
    "DomainState",
    "FenceVector",
    "LeaseService",
    "Principal",
    "ReleaseCommand",
    "RenewCommand",
    "RepoPolicy",
    "ResourceId",
    "ResourceRequest",
    "SQLiteCoordinationStore",
    "SelectorKind",
    "StoreCorruptionError",
    "assert_invariants",
    "decide",
    "evolve",
]
