"""Fail-closed error boundary for the isolated OMD v2 repo saga."""

from __future__ import annotations


class RepoSagaError(RuntimeError):
    """Base class for repo-engine failures."""


class RepoConfigurationError(RepoSagaError):
    """A registered repository violates the alpha trust contract."""


class GitExecutionError(RepoSagaError):
    """A fixed Git plumbing command failed or returned invalid output."""


class GitTimeoutError(GitExecutionError):
    """A Git subprocess exceeded its bounded runtime."""


class GitOutputLimitError(GitExecutionError):
    """A Git subprocess exceeded its bounded output budget."""


class StoreCorruptionError(RepoSagaError):
    """Persisted saga rows disagree with their schema or transition history."""


class IdempotencyConflict(RepoSagaError):
    """One public request key was reused with different canonical input."""


class TransitionConflict(RepoSagaError):
    """A stale worker attempted an illegal or superseded state transition."""


class WorkerBusy(RepoSagaError):
    """Another repo worker owns the process/repository singleton lock."""


class AuthorityRejected(RepoSagaError):
    """The durable mutation authority refused or lost the reservation."""


class MergeConflict(RepoSagaError):
    """Git reported a non-clean merge; its conflict tree is not a candidate."""


class PathPolicyError(RepoSagaError):
    """A Git tree path or mode is outside the admitted alpha grammar."""


class WriteSetViolation(RepoSagaError):
    """The immutable candidate changes an unauthorized path."""


class ReadSetStale(RepoSagaError):
    """The integration target changed a declared read dependency."""
