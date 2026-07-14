"""Public value and error contracts for the OMD v2 SQLite adapter."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Callable

from .model import CommandResult, DomainEffect, DomainEvent, DomainState


class StoreError(RuntimeError):
    pass


class DomainNotFound(StoreError):
    pass


class DomainConfigurationConflict(StoreError):
    pass


class RevisionConflict(StoreError):
    def __init__(self, expected: int, actual: int):
        self.expected = expected
        self.actual = actual
        super().__init__(f"revision conflict: expected {expected}, actual {actual}")


class SchemaVersionError(StoreError):
    pass


class JournalModeError(StoreError):
    pass


class SQLiteVersionError(StoreError):
    pass


class StoreCorruptionError(StoreError):
    """Persisted projections disagree; callers must quarantine the database."""


@dataclass(frozen=True, slots=True)
class DomainSnapshot:
    state: DomainState
    revision: int


@dataclass(frozen=True, slots=True)
class ExecutionReceipt:
    result: CommandResult
    revision: int
    state: DomainState
    events: tuple[DomainEvent, ...]
    effects: tuple[DomainEffect, ...]


@dataclass(frozen=True, slots=True)
class OutboxRecord:
    domain_id: str
    revision: int
    seq: int
    effect: DomainEffect
    attempts: int
    created_at_ms: int


FaultInjector = Callable[[str], None]
