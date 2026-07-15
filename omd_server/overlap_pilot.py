"""PROM16 R3 exploratory nominal-window and branch-tip conflict pilot.

The coordinator database records lease requests and terminal lease state.  It
does not record continuous agent execution, so this module deliberately calls
the temporal signal a *nominal lease/request window*, not observed concurrency.
Likewise, ``git merge-tree`` answers a counterfactual pairwise branch-tip
question; it is not a replay of OMD's historical connect order or integration
base.

The tool is read-only with respect to both measured inputs:

* SQLite is read from a stable temporary copy of the database and WAL.  The
  source database, WAL, and SHM files are never opened by SQLite.
* Git objects produced by ``merge-tree`` go to an isolated bare repository.
  Source repository configuration, hooks, attributes, and merge drivers are
  not trusted or executed.

This is an exploratory measurement tool, not an OMD safety gate or a metric
promotion decision.
"""

from __future__ import annotations

import argparse
import copy
import hashlib
import itertools
import json
import math
import os
import re
import shutil
import sqlite3
import subprocess
import sys
import tempfile
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Iterator, Mapping, Sequence

from ._const import LEGACY_ATTEMPT_OPENERS
from .disjoint import sets_overlap


SCHEMA_V2 = "omd-base-overlap-pilot/v2"
SCHEMA_V3 = "omd-base-overlap-pilot/v3"
# Public compatibility name: callers which imported SCHEMA before provenance v3
# still describe the legacy report.  Native reports select SCHEMA_V3 at runtime.
SCHEMA = SCHEMA_V2
ORACLE_POLICY = "isolated-bare-pairwise-tip-merge-tree/v1"
SNAPSHOT_POLICY = "stable-filesystem-copy-db-plus-wal/v1"
LEGACY_V2 = "LEGACY_V2"
NATIVE_V3 = "NATIVE_V3"
_WRITE_CAPABLE_MODES = frozenset(("exclusive", "shared", "write"))
_OID_LENGTH = {"sha1": 40, "sha256": 64}
_SNAPSHOT_ATTEMPTS = 3
_NATIVE_ORBIT_COLUMNS = (
    "attempt_id",
    "requested_at",
    "granted_at",
    "requested_ttl",
    "terminal_at",
    "terminal_effective_at",
    "reclaimed_at",
    "terminal_reason",
)


class PilotDataError(RuntimeError):
    """The durable input cannot support an honest pilot report."""


@dataclass(frozen=True)
class ScopeRule:
    """A named cohort selected from task ID, agent ID, and declared paths."""

    name: str
    pattern: str

    def __post_init__(self) -> None:
        if not self.name.strip() or not self.pattern:
            raise ValueError("scope requires non-empty NAME and REGEX")
        try:
            re.compile(self.pattern)
        except re.error as exc:
            raise ValueError(f"invalid scope regex {self.pattern!r}: {exc}") from exc

    @classmethod
    def from_spec(cls, spec: str) -> "ScopeRule":
        if "=" not in spec:
            raise ValueError("scope must be NAME=REGEX")
        name, pattern = spec.split("=", 1)
        return cls(name=name, pattern=pattern)

    def matches(self, attempt: "TaskAttempt") -> bool:
        haystack = "\n".join(
            (attempt.task_id, attempt.agent_id, *attempt.scope_paths)
        )
        return re.search(self.pattern, haystack, flags=re.MULTILINE) is not None


@dataclass(frozen=True)
class OrbitIntent:
    """One durable write-capable orbit row and its nominal activity window."""

    orbit_id: str
    paths: tuple[str, ...]
    started_at: float
    ended_at: float
    end_source: str
    mode: str
    state: str
    provenance_mode: str = LEGACY_V2


@dataclass(frozen=True)
class ConnectAttempt:
    """One immutable, admitted native connect try."""

    connect_attempt_id: str
    connect_seq: int
    token_id: str
    orbit_ids: tuple[str, ...]
    orbit_fences: tuple[tuple[str, int], ...]
    coordinator_epoch: int | None
    trigger_kind: str
    barrier_id: str | None
    barrier_generation: int | None
    started_at: float
    branch_tip_sha: str
    integration_base_sha: str
    candidate_tree_sha: str | None
    candidate_commit_sha: str | None
    candidate_prepared_at: float | None
    terminal_at: float
    outcome: str
    outcome_code: str
    merge_sha: str | None
    merge_gen: int | None
    resolution_source: str
    detail: str | None


@dataclass(frozen=True)
class TaskAttempt:
    """One task/agent grouping containing its exact orbit windows."""

    task_id: str
    agent_id: str
    orbits: tuple[OrbitIntent, ...]
    acquisition_epochs: int
    branch_tip_sha: str | None
    branch_tip_provenance: str
    merge_sha: str | None
    merged_at: float | None
    attempt_id: str | None = None
    attempt_ordinal: int | None = None
    repo_id: str | None = None
    repo_root: str | None = None
    integration_branch: str | None = None
    declared_writes: tuple[str, ...] = ()
    declared_shared: tuple[str, ...] = ()
    opened_at: float | None = None
    opened_by: str | None = None
    attempt_started_at: float | None = None
    finished_at: float | None = None
    finish_source: str | None = None
    finished_by: str | None = None
    worktree_base_sha: str | None = None
    branch: str | None = None
    terminal_at: float | None = None
    terminal_state: str | None = None
    terminal_reason: str | None = None
    actor_trust: str | None = None
    canonical_connect_outcome: str | None = None
    canonical_connect_outcome_code: str | None = None
    connect_attempts: tuple[ConnectAttempt, ...] = ()
    provenance_mode: str = LEGACY_V2

    @property
    def paths(self) -> tuple[str, ...]:
        return tuple(sorted({path for orbit in self.orbits for path in orbit.paths}))

    @property
    def scope_paths(self) -> tuple[str, ...]:
        if self.provenance_mode == NATIVE_V3:
            return tuple(sorted(set(self.declared_writes + self.declared_shared)))
        return self.paths

    @property
    def node_id(self) -> str | tuple[str, str]:
        if self.attempt_id is not None:
            return self.attempt_id
        return self.task_id, self.agent_id

    @property
    def display_id(self) -> str:
        if self.attempt_id is not None:
            return f"{self.task_id}@{self.agent_id}#{self.attempt_id}"
        return f"{self.task_id}@{self.agent_id}"

    @property
    def started_at(self) -> float:
        if self.orbits:
            return min(orbit.started_at for orbit in self.orbits)
        if self.attempt_started_at is not None:
            return self.attempt_started_at
        if self.opened_at is not None:
            return self.opened_at
        raise PilotDataError(f"attempt {self.display_id}: no start timestamp")

    @property
    def ended_at(self) -> float:
        if self.orbits:
            return max(orbit.ended_at for orbit in self.orbits)
        if self.terminal_at is not None:
            return self.terminal_at
        return self.started_at

    @property
    def orbit_rows(self) -> int:
        return len(self.orbits)

    @property
    def modes(self) -> tuple[str, ...]:
        return tuple(sorted({orbit.mode for orbit in self.orbits}))

    @property
    def states(self) -> tuple[str, ...]:
        return tuple(sorted({orbit.state for orbit in self.orbits}))


@dataclass(frozen=True)
class PilotReport:
    _payload: dict

    def to_dict(self) -> dict:
        return copy.deepcopy(self._payload)


@dataclass(frozen=True)
class _OracleResult:
    outcome: str
    detail: str = ""


@dataclass(frozen=True)
class _GitRepoFacts:
    repo: Path
    common_dir: Path
    object_dir: Path
    object_format: str
    git_version: str

    def manifest(self) -> dict:
        return {
            "repo": str(self.repo),
            "common_dir": str(self.common_dir),
            "object_dir": str(self.object_dir),
            "object_format": self.object_format,
            "git_version": self.git_version,
        }


@dataclass(frozen=True)
class _LoadedPilotInput:
    attempts: tuple[TaskAttempt, ...]
    provenance_mode: str
    canonical_input_sha256: str
    source_metadata: dict


def _require_columns(db: sqlite3.Connection, table: str, required: set[str]) -> None:
    columns = {row[1] for row in db.execute(f"PRAGMA table_info({table})")}
    missing = sorted(required - columns)
    if missing:
        raise PilotDataError(f"{table} missing required columns: {', '.join(missing)}")


def _table_columns(db: sqlite3.Connection, table: str) -> set[str]:
    return {str(row[1]) for row in db.execute(f"PRAGMA table_info({table})")}


def _table_has_rows(db: sqlite3.Connection, table: str) -> bool:
    if not _table_columns(db, table):
        return False
    return db.execute(f"SELECT 1 FROM {table} LIMIT 1").fetchone() is not None


def _detect_provenance_mode(db: sqlite3.Connection) -> str:
    """Detect writer generation from durable content, never schema presence alone."""

    if _table_has_rows(db, "task_attempts") or _table_has_rows(
        db, "connect_attempts"
    ):
        return NATIVE_V3

    orbit_columns = _table_columns(db, "orbits")
    native_orbit_columns = set(_NATIVE_ORBIT_COLUMNS) & orbit_columns
    if native_orbit_columns:
        predicate = " OR ".join(
            f"{column} IS NOT NULL" for column in sorted(native_orbit_columns)
        )
        if db.execute(f"SELECT 1 FROM orbits WHERE {predicate} LIMIT 1").fetchone():
            return NATIVE_V3

    task_columns = _table_columns(db, "tasks")
    pointer_columns = {"attempt_id", "connect_attempt_id"} & task_columns
    if pointer_columns:
        predicate = " OR ".join(
            f"{column} IS NOT NULL" for column in sorted(pointer_columns)
        )
        if db.execute(f"SELECT 1 FROM tasks WHERE {predicate} LIMIT 1").fetchone():
            return NATIVE_V3

    # Store migration creates empty native tables and NULL provenance columns.  That
    # is deliberately still the legacy dataset: no historical facts were invented.
    return LEGACY_V2


def _file_signature(path: Path) -> tuple[int, int, int, int] | None:
    try:
        stat = path.stat()
    except FileNotFoundError:
        return None
    return stat.st_dev, stat.st_ino, stat.st_size, stat.st_mtime_ns


def _source_signatures(path: Path) -> tuple[object, object]:
    return _file_signature(path), _file_signature(Path(f"{path}-wal"))


@contextmanager
def _snapshot_connection(path: Path) -> Iterator[sqlite3.Connection]:
    """Open a stable DB+WAL copy without letting SQLite touch source SHM bytes."""

    resolved = path.expanduser().resolve()
    if not resolved.is_file():
        raise PilotDataError(f"coordination DB does not exist: {resolved}")

    with tempfile.TemporaryDirectory(prefix="omd-overlap-sqlite-") as raw_tmp:
        tmp = Path(raw_tmp)
        copied_db = tmp / "coord.db"
        copied_wal = Path(f"{copied_db}-wal")
        for _ in range(_SNAPSHOT_ATTEMPTS):
            before = _source_signatures(resolved)
            if before[0] is None:
                raise PilotDataError(f"coordination DB disappeared: {resolved}")
            copied_db.unlink(missing_ok=True)
            copied_wal.unlink(missing_ok=True)
            shutil.copyfile(resolved, copied_db)
            source_wal = Path(f"{resolved}-wal")
            if before[1] is not None:
                try:
                    shutil.copyfile(source_wal, copied_wal)
                except FileNotFoundError:
                    pass
            after = _source_signatures(resolved)
            if before == after and (before[1] is None) == (not copied_wal.exists()):
                break
        else:
            raise PilotDataError(
                "coordination DB changed during snapshot copy; retry when write traffic is lower"
            )

        db = sqlite3.connect(f"{copied_db.as_uri()}?mode=ro", uri=True)
        db.row_factory = sqlite3.Row
        try:
            yield db
        finally:
            db.close()


def _decode_paths(
    raw: str,
    orbit_id: str,
    *,
    max_paths_per_orbit: int,
    max_pathspec_bytes: int,
) -> tuple[str, ...]:
    if not isinstance(raw, str) or len(raw.encode("utf-8")) > max_pathspec_bytes:
        raise PilotDataError(
            f"orbit {orbit_id}: pathspec exceeds {max_pathspec_bytes} bytes"
        )
    try:
        value = json.loads(raw)
    except (TypeError, json.JSONDecodeError) as exc:
        raise PilotDataError(f"orbit {orbit_id}: invalid pathspec JSON") from exc
    if (
        not isinstance(value, list)
        or not value
        or not all(isinstance(path, str) and path.strip() for path in value)
    ):
        raise PilotDataError(
            f"orbit {orbit_id}: pathspec must be a non-empty string list"
        )
    if len(value) > max_paths_per_orbit:
        raise PilotDataError(
            f"orbit {orbit_id}: pathspec exceeds {max_paths_per_orbit} selectors"
        )
    return tuple(sorted(set(value)))


def _finite_timestamp(value: object, *, field: str, orbit_id: str) -> float:
    try:
        timestamp = float(value)
    except (TypeError, ValueError) as exc:
        raise PilotDataError(f"orbit {orbit_id}: {field} is not numeric") from exc
    if not math.isfinite(timestamp):
        raise PilotDataError(f"orbit {orbit_id}: {field} must be finite")
    return timestamp


def _finite_native_timestamp(value: object, *, field: str, entity: str) -> float:
    try:
        timestamp = float(value)
    except (TypeError, ValueError) as exc:
        raise PilotDataError(f"{entity}: {field} is not numeric") from exc
    if not math.isfinite(timestamp):
        raise PilotDataError(f"{entity}: {field} must be finite")
    return timestamp


def _native_string_list(
    raw: object, *, field: str, entity: str, allow_empty: bool = True
) -> tuple[str, ...]:
    try:
        value = json.loads(raw) if isinstance(raw, str) else None
    except json.JSONDecodeError as exc:
        raise PilotDataError(f"{entity}: invalid {field} JSON") from exc
    if (
        not isinstance(value, list)
        or (not allow_empty and not value)
        or not all(isinstance(item, str) and item.strip() for item in value)
    ):
        qualifier = "non-empty " if not allow_empty else ""
        raise PilotDataError(f"{entity}: {field} must be a {qualifier}string list")
    return tuple(value)


def _acquisition_epoch_count(orbits: Sequence[OrbitIntent]) -> int:
    """Count disconnected nominal windows inside one task/agent grouping."""

    if not orbits:
        return 0
    ordered = sorted(orbits, key=lambda orbit: (orbit.started_at, orbit.ended_at))
    epochs = 1
    current_end = ordered[0].ended_at
    for orbit in ordered[1:]:
        if orbit.started_at >= current_end:
            epochs += 1
            current_end = orbit.ended_at
        else:
            current_end = max(current_end, orbit.ended_at)
    return epochs


def _optional_native_timestamp(
    value: object, *, field: str, entity: str
) -> float | None:
    if value is None:
        return None
    return _finite_native_timestamp(value, field=field, entity=entity)


def _native_row_payload(row: sqlite3.Row, columns: Sequence[str]) -> dict:
    return {column: row[column] for column in columns}


def _load_native_v3(
    db: sqlite3.Connection,
    *,
    created_before: float | None,
    exclude_task_ids: Iterable[str],
    max_paths_per_orbit: int,
    max_pathspec_bytes: int,
) -> _LoadedPilotInput:
    """Load immutable attempt provenance; no mutable task projection is read."""

    orbit_columns = {
        "orbit_id", "task_id", "agent_id", "pathspec", "mode", "state",
        "kind", "fence", "created_at", "expires_at", "released_at",
        *_NATIVE_ORBIT_COLUMNS,
    }
    attempt_order = (
        "attempt_id", "task_id", "attempt_ordinal", "agent_id", "repo_id",
        "repo_root", "integration_branch", "writes", "shared", "opened_at",
        "opened_by", "started_at", "finished_at", "finish_source",
        "finished_by", "worktree_base_sha", "branch", "terminal_at",
        "terminal_state", "terminal_reason", "actor_trust",
    )
    connect_order = (
        "connect_attempt_id", "attempt_id", "task_id", "connect_seq",
        "token_id", "orbit_ids", "orbit_fences", "coordinator_epoch",
        "trigger_kind", "barrier_id", "barrier_generation", "started_at",
        "branch_tip_sha", "integration_base_sha", "candidate_tree_sha",
        "candidate_commit_sha", "candidate_prepared_at", "terminal_at", "outcome",
        "outcome_code", "merge_sha", "merge_gen", "resolution_source", "detail",
    )
    orbit_order = (
        "orbit_id", "task_id", "agent_id", "pathspec", "mode", "state",
        "kind", "fence", "created_at", "expires_at", "released_at",
        *_NATIVE_ORBIT_COLUMNS,
    )
    _require_columns(db, "orbits", orbit_columns)
    _require_columns(db, "task_attempts", set(attempt_order))
    _require_columns(db, "connect_attempts", set(connect_order))
    attempt_rows = list(db.execute(
        f"SELECT {','.join(attempt_order)} FROM task_attempts "
        "ORDER BY task_id,attempt_ordinal,attempt_id"
    ))
    connect_rows = list(db.execute(
        f"SELECT {','.join(connect_order)} FROM connect_attempts "
        "ORDER BY attempt_id,connect_seq,connect_attempt_id"
    ))
    orbit_rows = list(db.execute(
        f"SELECT {','.join(orbit_order)} FROM orbits "
        "ORDER BY kind,requested_at,created_at,orbit_id"
    ))

    attempts_by_id: dict[str, dict] = {}
    ordinal_keys: set[tuple[str, int]] = set()
    legacy_adapter_attempt_ids: set[str] = set()
    legacy_adapter_owner_keys: set[tuple[str, str]] = set()
    for row in attempt_rows:
        attempt_id = str(row["attempt_id"] or "")
        entity = f"task_attempt {attempt_id or '<missing>'}"
        task_id = str(row["task_id"] or "")
        agent_id = str(row["agent_id"] or "")
        if not attempt_id or not task_id or not agent_id:
            raise PilotDataError(
                f"{entity}: attempt_id, task_id, and agent_id must be non-empty"
            )
        if attempt_id in attempts_by_id:
            raise PilotDataError(f"duplicate task_attempt attempt_id: {attempt_id}")
        try:
            ordinal = int(row["attempt_ordinal"])
        except (TypeError, ValueError) as exc:
            raise PilotDataError(f"{entity}: attempt_ordinal must be an integer") from exc
        if ordinal < 1 or ordinal != row["attempt_ordinal"]:
            raise PilotDataError(f"{entity}: attempt_ordinal must be a positive integer")
        ordinal_key = task_id, ordinal
        if ordinal_key in ordinal_keys:
            raise PilotDataError(f"task {task_id}: duplicate attempt_ordinal {ordinal}")
        ordinal_keys.add(ordinal_key)

        opened_at = _finite_native_timestamp(
            row["opened_at"], field="opened_at", entity=entity
        )
        opened_by = str(row["opened_by"] or "")
        legacy_adapter = opened_by in LEGACY_ATTEMPT_OPENERS
        if legacy_adapter:
            legacy_adapter_attempt_ids.add(attempt_id)
            legacy_adapter_owner_keys.add((task_id, agent_id))
            attempts_by_id[attempt_id] = {
                "row": row, "task_id": task_id, "agent_id": agent_id,
                "ordinal": ordinal, "opened_at": opened_at,
                "opened_by": opened_by, "legacy_adapter": True,
                "orbits": [], "connects": [],
            }
            continue
        started_at = _optional_native_timestamp(
            row["started_at"], field="started_at", entity=entity
        )
        finished_at = _optional_native_timestamp(
            row["finished_at"], field="finished_at", entity=entity
        )
        terminal_at = _optional_native_timestamp(
            row["terminal_at"], field="terminal_at", entity=entity
        )
        if started_at is not None and started_at < opened_at:
            raise PilotDataError(f"{entity}: started_at precedes opened_at")
        if finished_at is not None and (started_at is None or finished_at < started_at):
            raise PilotDataError(
                f"{entity}: finished_at requires and must follow started_at"
            )
        if (finished_at is None) != (row["finish_source"] is None):
            raise PilotDataError(
                f"{entity}: finished_at and finish_source must be recorded together"
            )
        terminal_state = str(row["terminal_state"] or "") or None
        terminal_reason = str(row["terminal_reason"] or "") or None
        if terminal_at is None or terminal_state is None or terminal_reason is None:
            raise PilotDataError(
                f"{entity}: native attempt is structurally incomplete (terminal projection)"
            )
        if terminal_at < opened_at:
            raise PilotDataError(f"{entity}: terminal_at precedes opened_at")
        if opened_by not in ("CLAIM", "START"):
            raise PilotDataError(f"{entity}: unsupported opened_by {opened_by!r}")
        actor_trust = str(row["actor_trust"] or "")
        if not actor_trust:
            raise PilotDataError(f"{entity}: actor_trust must be non-empty")
        attempts_by_id[attempt_id] = {
            "row": row, "task_id": task_id, "agent_id": agent_id,
            "ordinal": ordinal,
            "writes": _native_string_list(row["writes"], field="writes", entity=entity),
            "shared": _native_string_list(row["shared"], field="shared", entity=entity),
            "opened_at": opened_at, "started_at": started_at,
            "finished_at": finished_at, "terminal_at": terminal_at,
            "terminal_state": terminal_state, "terminal_reason": terminal_reason,
            "opened_by": opened_by, "actor_trust": actor_trust,
            "legacy_adapter": False,
            "orbits": [], "connects": [],
        }

    merge_generations: dict[int, str] = {}
    legacy_adapter_connect_rows: list[sqlite3.Row] = []
    legacy_adapter_connect_snapshots: list[
        tuple[str, dict, tuple[str, ...], dict[str, int]]
    ] = []
    for row in connect_rows:
        connect_id = str(row["connect_attempt_id"] or "")
        entity = f"connect_attempt {connect_id or '<missing>'}"
        attempt_id = str(row["attempt_id"] or "")
        if not connect_id or not attempt_id:
            raise PilotDataError(f"{entity}: identity must be non-empty")
        owner = attempts_by_id.get(attempt_id)
        if owner is None:
            raise PilotDataError(f"{entity}: orphan attempt_id {attempt_id}")
        if str(row["task_id"] or "") != owner["task_id"]:
            raise PilotDataError(f"{entity}: task_id does not match task_attempt")
        try:
            seq = int(row["connect_seq"])
        except (TypeError, ValueError) as exc:
            raise PilotDataError(f"{entity}: connect_seq must be an integer") from exc
        if seq < 1 or seq != row["connect_seq"]:
            raise PilotDataError(f"{entity}: connect_seq must be a positive integer")
        orbit_ids = _native_string_list(
            row["orbit_ids"], field="orbit_ids", entity=entity, allow_empty=False
        )
        if len(set(orbit_ids)) != len(orbit_ids):
            raise PilotDataError(f"{entity}: orbit_ids contains duplicates")
        try:
            raw_fences = json.loads(row["orbit_fences"])
        except (TypeError, json.JSONDecodeError) as exc:
            raise PilotDataError(f"{entity}: invalid orbit_fences JSON") from exc
        if not isinstance(raw_fences, dict) or set(raw_fences) != set(orbit_ids):
            raise PilotDataError(
                f"{entity}: orbit_fences keys must exactly match orbit_ids"
            )
        fences: list[tuple[str, int]] = []
        for orbit_id in orbit_ids:
            fence = raw_fences[orbit_id]
            if not isinstance(fence, int) or isinstance(fence, bool):
                raise PilotDataError(f"{entity}: orbit fence must be an integer")
            fences.append((orbit_id, fence))
        candidate_values = (
            row["candidate_tree_sha"],
            row["candidate_commit_sha"],
            row["candidate_prepared_at"],
        )
        candidate_present = tuple(value is not None for value in candidate_values)
        if any(candidate_present) and not all(candidate_present):
            raise PilotDataError(
                f"{entity}: connect candidate attestation must be complete"
            )
        candidate_tree_sha: str | None = None
        candidate_commit_sha: str | None = None
        candidate_prepared_at: float | None = None
        if all(candidate_present):
            candidate_tree_sha = str(row["candidate_tree_sha"] or "")
            candidate_commit_sha = str(row["candidate_commit_sha"] or "")
            if not candidate_tree_sha or not candidate_commit_sha:
                raise PilotDataError(
                    f"{entity}: connect candidate identities must be non-empty"
                )
            candidate_prepared_at = _finite_native_timestamp(
                row["candidate_prepared_at"],
                field="candidate_prepared_at",
                entity=entity,
            )
        if owner["legacy_adapter"]:
            legacy_adapter_connect_rows.append(row)
            legacy_adapter_connect_snapshots.append(
                (connect_id, owner, orbit_ids, raw_fences)
            )
            continue
        started_at = _finite_native_timestamp(
            row["started_at"], field="started_at", entity=entity
        )
        terminal_at = _optional_native_timestamp(
            row["terminal_at"], field="terminal_at", entity=entity
        )
        outcome = str(row["outcome"] or "")
        outcome_code = str(row["outcome_code"] or "")
        if terminal_at is None or not outcome or not outcome_code:
            raise PilotDataError(f"{entity}: native connect try is structurally incomplete")
        if terminal_at < started_at:
            raise PilotDataError(f"{entity}: terminal_at precedes started_at")
        if candidate_prepared_at is not None:
            if candidate_prepared_at < started_at:
                raise PilotDataError(
                    f"{entity}: candidate_prepared_at precedes started_at"
                )
            if candidate_prepared_at > terminal_at:
                raise PilotDataError(
                    f"{entity}: candidate_prepared_at follows terminal_at"
                )
        branch_tip_sha = str(row["branch_tip_sha"] or "")
        integration_base_sha = str(row["integration_base_sha"] or "")
        if not branch_tip_sha or not integration_base_sha:
            raise PilotDataError(
                f"{entity}: branch_tip_sha and integration_base_sha are required"
            )
        merge_sha = str(row["merge_sha"] or "") or None
        merge_gen: int | None = None
        if row["merge_gen"] is not None:
            try:
                merge_gen = int(row["merge_gen"])
            except (TypeError, ValueError) as exc:
                raise PilotDataError(f"{entity}: merge_gen must be an integer") from exc
            if merge_gen < 1 or merge_gen != row["merge_gen"]:
                raise PilotDataError(f"{entity}: merge_gen must be a positive integer")
        if outcome == "MERGED":
            if merge_sha is None or merge_gen is None:
                raise PilotDataError(f"{entity}: MERGED requires merge_sha and merge_gen")
            if candidate_commit_sha is None:
                no_op_codes = {
                    "ALREADY_INTEGRATED",
                    "RECOVERED_ALREADY_INTEGRATED",
                }
                if (
                    outcome_code not in no_op_codes
                    or merge_sha != integration_base_sha
                ):
                    raise PilotDataError(
                        f"{entity}: MERGED requires an exact candidate commit "
                        "attestation unless it is an already-integrated no-op"
                    )
            elif candidate_commit_sha != merge_sha:
                raise PilotDataError(
                    f"{entity}: MERGED merge_sha does not match the attested "
                    "candidate commit"
                )
            previous = merge_generations.setdefault(merge_gen, connect_id)
            if previous != connect_id:
                raise PilotDataError(f"duplicate global merge_gen {merge_gen}")
        elif merge_gen is not None:
            raise PilotDataError(f"{entity}: only MERGED may carry merge_gen")
        resolution_source = str(row["resolution_source"] or "")
        if not resolution_source:
            raise PilotDataError(f"{entity}: resolution_source must be non-empty")
        connect = ConnectAttempt(
            connect_attempt_id=connect_id, connect_seq=seq,
            token_id=str(row["token_id"] or ""), orbit_ids=orbit_ids,
            orbit_fences=tuple(fences),
            coordinator_epoch=(int(row["coordinator_epoch"])
                               if row["coordinator_epoch"] is not None else None),
            trigger_kind=str(row["trigger_kind"] or ""),
            barrier_id=(str(row["barrier_id"])
                        if row["barrier_id"] is not None else None),
            barrier_generation=(int(row["barrier_generation"])
                                if row["barrier_generation"] is not None else None),
            started_at=started_at, branch_tip_sha=branch_tip_sha,
            integration_base_sha=integration_base_sha,
            candidate_tree_sha=candidate_tree_sha,
            candidate_commit_sha=candidate_commit_sha,
            candidate_prepared_at=candidate_prepared_at,
            terminal_at=terminal_at,
            outcome=outcome, outcome_code=outcome_code, merge_sha=merge_sha,
            merge_gen=merge_gen, resolution_source=resolution_source,
            detail=str(row["detail"]) if row["detail"] is not None else None,
        )
        owner["connects"].append(connect)

    # Mutable task pointers are never measurement evidence, but a pointer is itself
    # a native marker.  Refuse dangling markers instead of silently producing an
    # empty/native-looking report or falling back to v2.
    task_columns = _table_columns(db, "tasks")
    if {"task_id", "attempt_id", "connect_attempt_id"} <= task_columns:
        connect_ids = {
            str(row["connect_attempt_id"]) for row in connect_rows
        }
        for row in db.execute(
            "SELECT task_id,attempt_id,connect_attempt_id FROM tasks "
            "WHERE attempt_id IS NOT NULL OR connect_attempt_id IS NOT NULL"
        ):
            if row["attempt_id"] is not None and str(row["attempt_id"]) not in attempts_by_id:
                raise PilotDataError(
                    f"task {row['task_id']}: dangling native attempt_id pointer"
                )
            if (
                row["connect_attempt_id"] is not None
                and str(row["connect_attempt_id"]) not in connect_ids
            ):
                raise PilotDataError(
                    f"task {row['task_id']}: dangling native connect_attempt_id pointer"
                )

    legacy_workload_rows: list[sqlite3.Row] = []
    legacy_adapter_orbit_rows: list[sqlite3.Row] = []
    legacy_adapter_orbit_ids_by_owner: dict[tuple[str, str], set[str]] = {}
    native_workload_rows = 0
    unbound_native_rows = 0
    non_granted_native_rows = 0
    native_orbit_ids: set[str] = set()
    for row in orbit_rows:
        if str(row["kind"]) != "orbit":
            continue
        orbit_id = str(row["orbit_id"] or "")
        entity = f"orbit {orbit_id or '<missing>'}"
        if not orbit_id:
            raise PilotDataError("workload orbit_id must be non-empty")
        task_id = str(row["task_id"] or "")
        agent_id = str(row["agent_id"] or "")
        attempt_id = str(row["attempt_id"] or "")
        adapter_owner_key = (task_id, agent_id)
        adapter_bound_owner = (
            attempts_by_id.get(attempt_id) if attempt_id else None
        )
        if adapter_bound_owner and adapter_bound_owner["legacy_adapter"]:
            if (
                task_id != adapter_bound_owner["task_id"]
                or agent_id != adapter_bound_owner["agent_id"]
            ):
                raise PilotDataError(
                    f"{entity}: task/agent identity mismatches legacy adapter"
                )
            legacy_adapter_orbit_rows.append(row)
            legacy_adapter_orbit_ids_by_owner.setdefault(
                adapter_owner_key, set()
            ).add(orbit_id)
            continue
        legacy_transition_overlay = (
            not attempt_id
            and row["requested_at"] is None
            and row["requested_ttl"] is None
        )
        if legacy_transition_overlay:
            # A pre-R3 row remains attempt_id/requested_* = NULL by design.
            # Post-migration promotion/release/reclaim may populate grant or
            # terminal fields only; that overlay is still legacy evidence.
            if adapter_owner_key in legacy_adapter_owner_keys:
                legacy_adapter_orbit_rows.append(row)
                legacy_adapter_orbit_ids_by_owner.setdefault(
                    adapter_owner_key, set()
                ).add(orbit_id)
            else:
                legacy_workload_rows.append(row)
            continue
        provenance_values = [row[column] for column in _NATIVE_ORBIT_COLUMNS]
        if all(value is None for value in provenance_values):
            if attempt_id:
                raise PilotDataError(
                    f"{entity}: attempt-bound workload orbit lacks native provenance"
                )
            legacy_workload_rows.append(row)
            continue
        native_workload_rows += 1
        if orbit_id in native_orbit_ids:
            raise PilotDataError(f"duplicate native orbit_id: {orbit_id}")
        native_orbit_ids.add(orbit_id)
        requested_at = _optional_native_timestamp(
            row["requested_at"], field="requested_at", entity=entity
        )
        requested_ttl = _optional_native_timestamp(
            row["requested_ttl"], field="requested_ttl", entity=entity
        )
        if requested_at is None or requested_ttl is None or requested_ttl <= 0:
            raise PilotDataError(
                f"{entity}: partial native provenance (requested_at/requested_ttl)"
            )
        granted_at = _optional_native_timestamp(
            row["granted_at"], field="granted_at", entity=entity
        )
        terminal_at = _optional_native_timestamp(
            row["terminal_at"], field="terminal_at", entity=entity
        )
        terminal_effective_at = _optional_native_timestamp(
            row["terminal_effective_at"], field="terminal_effective_at", entity=entity
        )
        reclaimed_at = _optional_native_timestamp(
            row["reclaimed_at"], field="reclaimed_at", entity=entity
        )
        if granted_at is not None and granted_at < requested_at:
            raise PilotDataError(f"{entity}: granted_at precedes requested_at")
        if terminal_at is not None and terminal_at < requested_at:
            raise PilotDataError(f"{entity}: terminal_at precedes requested_at")
        if reclaimed_at is not None and (terminal_at is None or reclaimed_at < terminal_at):
            raise PilotDataError(f"{entity}: reclaimed_at precedes terminal_at")
        mode = str(row["mode"])
        if mode not in _WRITE_CAPABLE_MODES:
            continue
        if not task_id:
            if attempt_id:
                raise PilotDataError(f"{entity}: attempt_id without task_id")
            unbound_native_rows += 1
            continue
        if not attempt_id:
            raise PilotDataError(
                f"{entity}: task-bound native write/shared orbit lacks attempt_id"
            )
        owner = attempts_by_id.get(attempt_id)
        if owner is None:
            raise PilotDataError(f"{entity}: orphan attempt_id {attempt_id}")
        if task_id != owner["task_id"] or agent_id != owner["agent_id"]:
            raise PilotDataError(f"{entity}: task/agent identity mismatches task_attempt")
        state = str(row["state"])
        if granted_at is None:
            if (state != "DENIED" or terminal_at is None
                    or terminal_effective_at is None or not row["terminal_reason"]):
                raise PilotDataError(
                    f"{entity}: non-granted native orbit is structurally incomplete"
                )
            non_granted_native_rows += 1
            continue
        if (state not in ("RELEASED", "EXPIRED") or terminal_at is None
                or terminal_effective_at is None or not row["terminal_reason"]):
            raise PilotDataError(f"{entity}: granted native orbit is structurally incomplete")
        if terminal_effective_at < granted_at:
            raise PilotDataError(f"{entity}: terminal_effective_at precedes granted_at")
        if owner["started_at"] is None:
            raise PilotDataError(f"{entity}: exposure requires task_attempt.started_at")
        exposure_start = max(owner["started_at"], granted_at)
        if terminal_effective_at < exposure_start:
            raise PilotDataError(f"{entity}: terminal_effective_at precedes exposure start")
        owner["orbits"].append(OrbitIntent(
            orbit_id=orbit_id,
            paths=_decode_paths(
                row["pathspec"], orbit_id,
                max_paths_per_orbit=max_paths_per_orbit,
                max_pathspec_bytes=max_pathspec_bytes,
            ),
            started_at=exposure_start, ended_at=terminal_effective_at,
            end_source="TERMINAL_EFFECTIVE_AT", mode=mode, state=state,
            provenance_mode=NATIVE_V3,
        ))

    workload_rows_by_id = {
        str(row["orbit_id"]): row
        for row in orbit_rows if str(row["kind"]) == "orbit"
    }
    for connect_id, owner, orbit_ids, fences in legacy_adapter_connect_snapshots:
        adapter_id = str(owner["row"]["attempt_id"])
        for orbit_id in orbit_ids:
            row = workload_rows_by_id.get(orbit_id)
            if row is None:
                raise PilotDataError(
                    f"connect_attempt {connect_id}: legacy adapter orbit {orbit_id} "
                    "does not exist as a workload orbit"
                )
            if (
                orbit_id not in legacy_adapter_orbit_ids_by_owner.get(
                    (owner["task_id"], owner["agent_id"]), set()
                )
                or str(row["task_id"] or "") != owner["task_id"]
                or str(row["agent_id"] or "") != owner["agent_id"]
                or str(row["attempt_id"] or "") not in ("", adapter_id)
                or row["fence"] != fences[orbit_id]
            ):
                raise PilotDataError(
                    f"connect_attempt {connect_id}: legacy adapter orbit "
                    f"identity mismatch for {orbit_id}"
                )

    for attempt_id, owner in attempts_by_id.items():
        if owner["legacy_adapter"]:
            continue
        connects = sorted(owner["connects"], key=lambda connect: connect.connect_seq)
        if [connect.connect_seq for connect in connects] != list(range(1, len(connects) + 1)):
            raise PilotDataError(
                f"task_attempt {attempt_id}: connect_seq must be contiguous from 1"
            )
        merged = [connect for connect in connects if connect.outcome == "MERGED"]
        if len(merged) > 1:
            raise PilotDataError(f"task_attempt {attempt_id}: multiple MERGED connect tries")
        if (owner["terminal_state"] == "MERGED") != bool(merged):
            raise PilotDataError(
                f"task_attempt {attempt_id}: terminal_state/MERGED connect mismatch"
            )
        owned_orbit_ids = {orbit.orbit_id for orbit in owner["orbits"]}
        for connect in connects:
            missing = set(connect.orbit_ids) - owned_orbit_ids
            if missing:
                raise PilotDataError(
                    f"connect_attempt {connect.connect_attempt_id}: orbit_ids are not "
                    f"native exposure orbits of attempt {attempt_id}: {sorted(missing)}"
                )

    excluded = set(exclude_task_ids)
    attempts: list[TaskAttempt] = []
    for attempt_id, owner in sorted(
        attempts_by_id.items(),
        key=lambda item: (item[1]["task_id"], item[1]["ordinal"], item[0]),
    ):
        if owner["legacy_adapter"]:
            continue
        if owner["task_id"] in excluded:
            continue
        if created_before is not None and owner["opened_at"] >= created_before:
            continue
        row = owner["row"]
        orbits = tuple(sorted(
            owner["orbits"], key=lambda orbit: (orbit.started_at, orbit.orbit_id)
        ))
        connects = tuple(sorted(
            owner["connects"], key=lambda connect: connect.connect_seq
        ))
        first_connect = connects[0] if connects else None
        attempts.append(TaskAttempt(
            task_id=owner["task_id"], agent_id=owner["agent_id"], orbits=orbits,
            acquisition_epochs=_acquisition_epoch_count(orbits),
            branch_tip_sha=first_connect.branch_tip_sha if first_connect else None,
            branch_tip_provenance=("NATIVE_FIRST_CONNECT_PHASE_A" if first_connect
                                   else "MISSING_NO_CONNECT_ATTEMPT"),
            merge_sha=first_connect.merge_sha if first_connect else None,
            merged_at=(first_connect.terminal_at
                       if first_connect and first_connect.outcome == "MERGED" else None),
            attempt_id=attempt_id, attempt_ordinal=owner["ordinal"],
            repo_id=str(row["repo_id"]) if row["repo_id"] is not None else None,
            repo_root=str(row["repo_root"]) if row["repo_root"] is not None else None,
            integration_branch=(str(row["integration_branch"])
                                if row["integration_branch"] is not None else None),
            declared_writes=owner["writes"], declared_shared=owner["shared"],
            opened_at=owner["opened_at"], opened_by=owner["opened_by"],
            attempt_started_at=owner["started_at"], finished_at=owner["finished_at"],
            finish_source=(str(row["finish_source"])
                           if row["finish_source"] is not None else None),
            finished_by=(str(row["finished_by"])
                         if row["finished_by"] is not None else None),
            worktree_base_sha=(str(row["worktree_base_sha"])
                               if row["worktree_base_sha"] is not None else None),
            branch=str(row["branch"]) if row["branch"] is not None else None,
            terminal_at=owner["terminal_at"], terminal_state=owner["terminal_state"],
            terminal_reason=owner["terminal_reason"], actor_trust=owner["actor_trust"],
            canonical_connect_outcome=first_connect.outcome if first_connect else None,
            canonical_connect_outcome_code=(first_connect.outcome_code
                                            if first_connect else None),
            connect_attempts=connects, provenance_mode=NATIVE_V3,
        ))

    merge_order = [
        {
            "merge_gen": connect.merge_gen, "attempt_id": attempt_id,
            "connect_attempt_id": connect.connect_attempt_id,
            "task_id": attempts_by_id[attempt_id]["task_id"],
            "branch_tip_sha": connect.branch_tip_sha,
            "integration_base_sha": connect.integration_base_sha,
            "merge_sha": connect.merge_sha,
        }
        for attempt_id, owner in attempts_by_id.items()
        if not owner["legacy_adapter"]
        for connect in owner["connects"] if connect.outcome == "MERGED"
    ]
    merge_order.sort(key=lambda item: (item["merge_gen"], item["connect_attempt_id"]))
    legacy_write_rows = sum(
        str(row["mode"]) in _WRITE_CAPABLE_MODES for row in legacy_workload_rows
    )
    adapter_attempt_rows = [
        row for row in attempt_rows
        if str(row["attempt_id"] or "") in legacy_adapter_attempt_ids
    ]
    native_attempt_rows = [
        row for row in attempt_rows
        if str(row["attempt_id"] or "") not in legacy_adapter_attempt_ids
    ]
    adapter_connect_ids = {
        str(row["connect_attempt_id"]) for row in legacy_adapter_connect_rows
    }
    native_connect_rows = [
        row for row in connect_rows
        if str(row["connect_attempt_id"] or "") not in adapter_connect_ids
    ]
    adapter_orbit_ids = {
        str(row["orbit_id"]) for row in legacy_adapter_orbit_rows
    }
    legacy_orbit_ids = {
        str(row["orbit_id"]) for row in legacy_workload_rows
    }
    adapter_bound_orbit_rows = [
        row for row in legacy_adapter_orbit_rows if row["attempt_id"] is not None
    ]
    adapter_null_orbit_rows = [
        row for row in legacy_adapter_orbit_rows if row["attempt_id"] is None
    ]
    adapter_write_rows = sum(
        str(row["mode"]) in _WRITE_CAPABLE_MODES
        for row in legacy_adapter_orbit_rows
    )
    raw_payload = {
        "provenance_mode": NATIVE_V3,
        "task_attempts": [
            _native_row_payload(row, attempt_order) for row in native_attempt_rows
        ],
        "native_workload_orbits": [
            _native_row_payload(row, orbit_order) for row in orbit_rows
            if str(row["kind"]) == "orbit"
            and str(row["orbit_id"]) not in adapter_orbit_ids
            and str(row["orbit_id"]) not in legacy_orbit_ids
            and any(row[column] is not None for column in _NATIVE_ORBIT_COLUMNS)
        ],
        "connect_attempts": [
            _native_row_payload(row, connect_order) for row in native_connect_rows
        ],
        "excluded_synthetic_legacy_task_attempts": [
            _native_row_payload(row, attempt_order) for row in adapter_attempt_rows
        ],
        "excluded_synthetic_legacy_connect_attempts": [
            _native_row_payload(row, connect_order)
            for row in legacy_adapter_connect_rows
        ],
        "excluded_synthetic_legacy_orbits": [
            _native_row_payload(row, orbit_order)
            for row in legacy_adapter_orbit_rows
        ],
        "excluded_legacy_workload_orbits": [
            _native_row_payload(row, orbit_order) for row in legacy_workload_rows
        ],
    }
    source_metadata = {
        "provenance_mode": NATIVE_V3,
        "task_attempt_rows": len(attempt_rows),
        "connect_attempt_rows": len(connect_rows),
        "native_workload_orbit_rows": native_workload_rows,
        "native_task_attempt_rows": len(native_attempt_rows),
        "native_connect_attempt_rows": len(native_connect_rows),
        "synthetic_legacy_attempt_rows_excluded": len(adapter_attempt_rows),
        "synthetic_legacy_connect_attempt_rows_excluded": len(
            legacy_adapter_connect_rows
        ),
        "synthetic_legacy_bound_orbit_rows_excluded": len(
            adapter_bound_orbit_rows
        ),
        "synthetic_legacy_null_attempt_orbit_rows_excluded": len(
            adapter_null_orbit_rows
        ),
        "synthetic_legacy_write_capable_orbit_rows_excluded": adapter_write_rows,
        "legacy_workload_orbit_rows_excluded": len(legacy_workload_rows),
        "legacy_write_capable_orbit_rows_excluded": legacy_write_rows,
        "unbound_native_write_capable_orbit_rows_excluded": unbound_native_rows,
        "non_granted_native_write_capable_orbit_rows_excluded": non_granted_native_rows,
        "merge_order": merge_order,
    }
    return _LoadedPilotInput(
        attempts=tuple(attempts), provenance_mode=NATIVE_V3,
        canonical_input_sha256=_canonical_json_sha256(raw_payload),
        source_metadata=source_metadata,
    )


def _load_pilot_input(
    path: str | Path,
    *,
    created_before: float | None = None,
    exclude_task_ids: Iterable[str] = (),
    max_paths_per_orbit: int = 1_000,
    max_pathspec_bytes: int = 1_000_000,
) -> _LoadedPilotInput:
    """Load workload orbits from an invocation-time DB+WAL snapshot."""

    if created_before is not None and not math.isfinite(created_before):
        raise ValueError("created_before must be finite")
    if max_paths_per_orbit < 1 or max_pathspec_bytes < 1:
        raise ValueError("pathspec limits must be positive")
    with _snapshot_connection(Path(path)) as db:
        provenance_mode = _detect_provenance_mode(db)
        if provenance_mode == NATIVE_V3:
            return _load_native_v3(
                db,
                created_before=created_before,
                exclude_task_ids=exclude_task_ids,
                max_paths_per_orbit=max_paths_per_orbit,
                max_pathspec_bytes=max_pathspec_bytes,
            )
        _require_columns(
            db,
            "orbits",
            {
                "orbit_id",
                "task_id",
                "agent_id",
                "pathspec",
                "mode",
                "state",
                "kind",
                "expires_at",
                "created_at",
                "released_at",
            },
        )
        _require_columns(
            db,
            "tasks",
            {"task_id", "branch_tip_sha", "merge_sha", "merged_at"},
        )
        rows = list(
            db.execute(
                """
                SELECT o.orbit_id, o.task_id, o.agent_id, o.pathspec, o.mode,
                       o.state, o.expires_at, o.created_at, o.released_at,
                       t.branch_tip_sha, t.merge_sha, t.merged_at
                  FROM orbits AS o
                  LEFT JOIN tasks AS t ON t.task_id = o.task_id
                 WHERE o.kind = 'orbit'
                 ORDER BY o.created_at, o.task_id, o.agent_id, o.orbit_id
                """
            )
        )

    excluded = set(exclude_task_ids)
    task_agents: dict[str, set[str]] = {}
    for row in rows:
        task_id = str(row["task_id"] or "")
        agent_id = str(row["agent_id"] or "")
        if not task_id or not agent_id:
            raise PilotDataError(
                "workload orbit task_id and agent_id must be non-empty"
            )
        task_agents.setdefault(task_id, set()).add(agent_id)

    groups: dict[tuple[str, str], dict] = {}
    for row in rows:
        if str(row["mode"]) not in _WRITE_CAPABLE_MODES:
            continue
        orbit_id = str(row["orbit_id"] or "")
        task_id = str(row["task_id"] or "")
        agent_id = str(row["agent_id"] or "")
        if not orbit_id:
            raise PilotDataError("workload orbit_id must be non-empty")
        start = _finite_timestamp(
            row["created_at"], field="created_at", orbit_id=orbit_id
        )
        if created_before is not None and start >= created_before:
            continue
        if task_id in excluded:
            continue

        if row["released_at"] is not None:
            end = _finite_timestamp(
                row["released_at"], field="released_at", orbit_id=orbit_id
            )
            end_source = "RELEASED_AT"
        elif row["expires_at"] is not None:
            end = _finite_timestamp(
                row["expires_at"], field="expires_at", orbit_id=orbit_id
            )
            end_source = "EXPIRES_AT_PROXY"
        else:
            end = start
            end_source = "START_ONLY"
        if end < start:
            raise PilotDataError(f"orbit {orbit_id}: end precedes start")

        key = task_id, agent_id
        group = groups.setdefault(
            key,
            {
                "orbits": [],
                "branch_tips": set(),
                "merge_shas": set(),
                "merged_at": set(),
            },
        )
        group["orbits"].append(
            OrbitIntent(
                orbit_id=orbit_id,
                paths=_decode_paths(
                    row["pathspec"],
                    orbit_id,
                    max_paths_per_orbit=max_paths_per_orbit,
                    max_pathspec_bytes=max_pathspec_bytes,
                ),
                started_at=start,
                ended_at=end,
                end_source=end_source,
                mode=str(row["mode"]),
                state=str(row["state"]),
            )
        )
        if row["branch_tip_sha"]:
            group["branch_tips"].add(str(row["branch_tip_sha"]))
        if row["merge_sha"]:
            group["merge_shas"].add(str(row["merge_sha"]))
        if row["merged_at"] is not None:
            merged_at = float(row["merged_at"])
            if not math.isfinite(merged_at):
                raise PilotDataError(f"task {task_id}: merged_at must be finite")
            group["merged_at"].add(merged_at)

    attempts: list[TaskAttempt] = []
    for (task_id, agent_id), group in sorted(groups.items()):
        if len(group["branch_tips"]) > 1:
            raise PilotDataError(f"task {task_id}: conflicting branch_tip_sha values")
        if len(group["merge_shas"]) > 1:
            raise PilotDataError(f"task {task_id}: conflicting merge_sha values")
        if len(group["merged_at"]) > 1:
            raise PilotDataError(f"task {task_id}: conflicting merged_at values")

        ordered_orbits = tuple(
            sorted(
                group["orbits"],
                key=lambda orbit: (orbit.started_at, orbit.orbit_id),
            )
        )
        acquisition_epochs = _acquisition_epoch_count(ordered_orbits)
        ambiguous_multi_orbit_history = len(ordered_orbits) > 1 and (
            acquisition_epochs != 1
            or any(orbit.end_source != "RELEASED_AT" for orbit in ordered_orbits)
        )
        raw_tip = next(iter(group["branch_tips"]), None)
        merged_at = next(iter(group["merged_at"]), None)
        if raw_tip is None:
            branch_tip = None
            tip_provenance = "MISSING"
        elif len(task_agents[task_id]) != 1:
            branch_tip = None
            tip_provenance = "WITHHELD_MULTI_AGENT_TASK"
        elif ambiguous_multi_orbit_history:
            branch_tip = None
            tip_provenance = "WITHHELD_AMBIGUOUS_MULTI_ORBIT_HISTORY"
        elif created_before is not None and (
            merged_at is None or merged_at >= created_before
        ):
            branch_tip = None
            tip_provenance = "WITHHELD_CUTOFF_UNBOUND"
        else:
            branch_tip = raw_tip
            tip_provenance = (
                "AVAILABLE_MERGED_BEFORE_CUTOFF_SINGLE_NOMINAL_EPOCH"
                if created_before is not None
                else "AVAILABLE_SINGLE_AGENT_SINGLE_NOMINAL_EPOCH_SNAPSHOT"
            )

        attempts.append(
            TaskAttempt(
                task_id=task_id,
                agent_id=agent_id,
                orbits=ordered_orbits,
                acquisition_epochs=acquisition_epochs,
                branch_tip_sha=branch_tip,
                branch_tip_provenance=tip_provenance,
                merge_sha=next(iter(group["merge_shas"]), None),
                merged_at=merged_at,
            )
        )
    return _LoadedPilotInput(
        attempts=tuple(attempts),
        provenance_mode=LEGACY_V2,
        canonical_input_sha256=_input_digest(attempts),
        source_metadata={},
    )


def load_attempts(
    path: str | Path,
    *,
    created_before: float | None = None,
    exclude_task_ids: Iterable[str] = (),
    max_paths_per_orbit: int = 1_000,
    max_pathspec_bytes: int = 1_000_000,
) -> list[TaskAttempt]:
    """Load legacy task/agent groups or authoritative native attempts."""

    loaded = _load_pilot_input(
        path,
        created_before=created_before,
        exclude_task_ids=exclude_task_ids,
        max_paths_per_orbit=max_paths_per_orbit,
        max_pathspec_bytes=max_pathspec_bytes,
    )
    return list(loaded.attempts)


def _canonical_json_sha256(value: object) -> str:
    payload = json.dumps(
        value, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    )
    return hashlib.sha256(payload.encode()).hexdigest()


def _implementation_hashes() -> dict[str, str]:
    files = (Path(__file__), Path(__file__).with_name("disjoint.py"))
    return {
        f"omd_server/{path.name}": hashlib.sha256(path.read_bytes()).hexdigest()
        for path in files
    }


def _input_digest(attempts: Sequence[TaskAttempt]) -> str:
    native = any(attempt.provenance_mode == NATIVE_V3 for attempt in attempts)
    canonical = [
        {
            "task_id": attempt.task_id,
            "agent_id": attempt.agent_id,
            "paths": list(attempt.paths),
            "branch_tip_sha": attempt.branch_tip_sha,
            "branch_tip_provenance": attempt.branch_tip_provenance,
            "acquisition_epochs": attempt.acquisition_epochs,
            "merge_sha": attempt.merge_sha,
            "merged_at": attempt.merged_at,
            **(
                {
                    "provenance_mode": attempt.provenance_mode,
                    "attempt_id": attempt.attempt_id,
                    "attempt_ordinal": attempt.attempt_ordinal,
                    "repo_id": attempt.repo_id,
                    "repo_root": attempt.repo_root,
                    "integration_branch": attempt.integration_branch,
                    "declared_writes": list(attempt.declared_writes),
                    "declared_shared": list(attempt.declared_shared),
                    "opened_at": attempt.opened_at,
                    "opened_by": attempt.opened_by,
                    "attempt_started_at": attempt.attempt_started_at,
                    "finished_at": attempt.finished_at,
                    "finish_source": attempt.finish_source,
                    "finished_by": attempt.finished_by,
                    "worktree_base_sha": attempt.worktree_base_sha,
                    "branch": attempt.branch,
                    "terminal_at": attempt.terminal_at,
                    "terminal_state": attempt.terminal_state,
                    "terminal_reason": attempt.terminal_reason,
                    "actor_trust": attempt.actor_trust,
                    "canonical_connect_outcome": attempt.canonical_connect_outcome,
                    "canonical_connect_outcome_code": (
                        attempt.canonical_connect_outcome_code
                    ),
                    "connect_attempts": [
                        {
                            "connect_attempt_id": connect.connect_attempt_id,
                            "connect_seq": connect.connect_seq,
                            "token_id": connect.token_id,
                            "orbit_ids": list(connect.orbit_ids),
                            "orbit_fences": [list(item) for item in connect.orbit_fences],
                            "coordinator_epoch": connect.coordinator_epoch,
                            "trigger_kind": connect.trigger_kind,
                            "barrier_id": connect.barrier_id,
                            "barrier_generation": connect.barrier_generation,
                            "started_at": connect.started_at,
                            "branch_tip_sha": connect.branch_tip_sha,
                            "integration_base_sha": connect.integration_base_sha,
                            "candidate_tree_sha": connect.candidate_tree_sha,
                            "candidate_commit_sha": connect.candidate_commit_sha,
                            "candidate_prepared_at": connect.candidate_prepared_at,
                            "terminal_at": connect.terminal_at,
                            "outcome": connect.outcome,
                            "outcome_code": connect.outcome_code,
                            "merge_sha": connect.merge_sha,
                            "merge_gen": connect.merge_gen,
                            "resolution_source": connect.resolution_source,
                            "detail": connect.detail,
                        }
                        for connect in attempt.connect_attempts
                    ],
                }
                if native
                else {}
            ),
            "orbits": [
                {
                    "orbit_id": orbit.orbit_id,
                    "paths": list(orbit.paths),
                    "started_at": orbit.started_at,
                    "ended_at": orbit.ended_at,
                    "end_source": orbit.end_source,
                    "mode": orbit.mode,
                    "state": orbit.state,
                    **(
                        {"provenance_mode": orbit.provenance_mode}
                        if native
                        else {}
                    ),
                }
                for orbit in attempt.orbits
            ],
        }
        for attempt in attempts
    ]
    return _canonical_json_sha256(canonical)


def _overlapping_time_orbits(
    left: TaskAttempt, right: TaskAttempt
) -> list[tuple[OrbitIntent, OrbitIntent]]:
    return [
        (left_orbit, right_orbit)
        for left_orbit in left.orbits
        for right_orbit in right.orbits
        if max(left_orbit.started_at, right_orbit.started_at)
        < min(left_orbit.ended_at, right_orbit.ended_at)
    ]


def _windows_overlap(left: TaskAttempt, right: TaskAttempt) -> bool:
    return bool(_overlapping_time_orbits(left, right))


def _canonical_paths(paths: Sequence[str], roots: Sequence[str]) -> tuple[str, ...]:
    canonical: set[str] = set()
    ordered_roots = sorted(roots, key=len, reverse=True)
    for path in paths:
        normalized = path
        for root in ordered_roots:
            if normalized.startswith(f"{root}/"):
                normalized = normalized[len(root) + 1 :]
                break
            if normalized.rstrip("/") == root:
                raise PilotDataError(
                    f"path root itself is not a repository-relative write selector: {path}"
                )
        canonical.add(normalized)
    return tuple(sorted(canonical))


def _overlapping_write_orbits(
    temporal_pairs: Sequence[tuple[OrbitIntent, OrbitIntent]],
    roots: Sequence[str],
) -> list[tuple[OrbitIntent, OrbitIntent]]:
    return [
        (left_orbit, right_orbit)
        for left_orbit, right_orbit in temporal_pairs
        if sets_overlap(
            _canonical_paths(left_orbit.paths, roots),
            _canonical_paths(right_orbit.paths, roots),
        )
    ]


def _git_env() -> dict[str, str]:
    allowed = ("PATH", "SYSTEMROOT", "TMPDIR", "TEMP", "TMP")
    env = {name: os.environ[name] for name in allowed if name in os.environ}
    env.update(
        {
            "LC_ALL": "C",
            "GIT_CONFIG_NOSYSTEM": "1",
            "GIT_CONFIG_GLOBAL": os.devnull,
            "GIT_ATTR_NOSYSTEM": "1",
            "GIT_OPTIONAL_LOCKS": "0",
            "GIT_TERMINAL_PROMPT": "0",
            "GIT_NO_REPLACE_OBJECTS": "1",
        }
    )
    return env


def _probe_git_repo(repo: Path) -> _GitRepoFacts:
    resolved = repo.expanduser().resolve()
    env = _git_env()

    def probe(*args: str) -> str:
        try:
            proc = subprocess.run(
                ["git", "-C", str(resolved), *args],
                check=False,
                capture_output=True,
                text=True,
                timeout=15,
                env=env,
            )
        except subprocess.TimeoutExpired as exc:
            raise PilotDataError(f"Git repository probe timed out: {resolved}") from exc
        if proc.returncode != 0:
            detail = proc.stderr.strip().replace("\n", " ")[:500]
            raise PilotDataError(f"not a readable Git repository: {resolved}: {detail}")
        return proc.stdout.strip()

    common_dir = Path(
        probe("rev-parse", "--path-format=absolute", "--git-common-dir")
    ).resolve()
    object_dir = Path(
        probe("rev-parse", "--path-format=absolute", "--git-path", "objects")
    ).resolve()
    object_format = probe("rev-parse", "--show-object-format")
    if object_format not in _OID_LENGTH:
        raise PilotDataError(
            f"unsupported Git object format {object_format!r}: {resolved}"
        )
    if not object_dir.is_dir():
        raise PilotDataError(f"Git object directory is missing: {object_dir}")
    try:
        version_proc = subprocess.run(
            ["git", "--version"],
            check=True,
            capture_output=True,
            text=True,
            timeout=15,
            env=env,
        )
    except (subprocess.CalledProcessError, subprocess.TimeoutExpired) as exc:
        raise PilotDataError("unable to determine Git version") from exc
    return _GitRepoFacts(
        repo=resolved,
        common_dir=common_dir,
        object_dir=object_dir,
        object_format=object_format,
        git_version=version_proc.stdout.strip(),
    )


def _valid_oid(value: str, object_format: str) -> bool:
    length = _OID_LENGTH[object_format]
    return len(value) == length and re.fullmatch(r"[0-9a-fA-F]+", value) is not None


def _merge_tree_oracle(
    facts: _GitRepoFacts, left_sha: str, right_sha: str
) -> _OracleResult:
    """Run pairwise merge-tree in a config- and attribute-isolated bare repo."""

    if not _valid_oid(left_sha, facts.object_format) or not _valid_oid(
        right_sha, facts.object_format
    ):
        return _OracleResult(
            "ERROR",
            f"branch tips must be full {facts.object_format} object IDs",
        )

    with tempfile.TemporaryDirectory(prefix="omd-overlap-git-") as raw_tmp:
        tmp = Path(raw_tmp)
        bare = tmp / "oracle.git"
        env = _git_env()
        try:
            init = subprocess.run(
                [
                    "git",
                    "init",
                    "--bare",
                    "--quiet",
                    f"--object-format={facts.object_format}",
                    str(bare),
                ],
                check=False,
                capture_output=True,
                text=True,
                timeout=15,
                env=env,
            )
        except subprocess.TimeoutExpired:
            return _OracleResult("ERROR", "isolated Git initialization timed out")
        if init.returncode != 0:
            detail = init.stderr.strip().replace("\n", " ")[:500]
            return _OracleResult(
                "ERROR", detail or "isolated Git initialization failed"
            )

        # info/attributes has highest precedence and forces the built-in text
        # merge driver, neutralizing repository-controlled merge=<driver> rules.
        (bare / "info" / "attributes").write_text("* merge\n", encoding="utf-8")
        for key, value in (
            ("core.attributesFile", os.devnull),
            ("core.hooksPath", os.devnull),
        ):
            try:
                config = subprocess.run(
                    [
                        "git",
                        "--git-dir",
                        str(bare),
                        "config",
                        key,
                        value,
                    ],
                    check=False,
                    capture_output=True,
                    text=True,
                    timeout=15,
                    env=env,
                )
            except subprocess.TimeoutExpired:
                return _OracleResult("ERROR", "isolated Git configuration timed out")
            if config.returncode != 0:
                detail = config.stderr.strip().replace("\n", " ")[:500]
                return _OracleResult(
                    "ERROR", detail or "isolated Git configuration failed"
                )
        env["GIT_ALTERNATE_OBJECT_DIRECTORIES"] = str(facts.object_dir)
        with tempfile.TemporaryFile(mode="w+t", encoding="utf-8") as error_log:
            try:
                proc = subprocess.run(
                    [
                        "git",
                        "--git-dir",
                        str(bare),
                        "merge-tree",
                        "--write-tree",
                        left_sha,
                        right_sha,
                    ],
                    check=False,
                    stdout=subprocess.DEVNULL,
                    stderr=error_log,
                    text=True,
                    env=env,
                    timeout=30,
                )
            except subprocess.TimeoutExpired:
                return _OracleResult("ERROR", "git merge-tree timed out")
            error_log.seek(0)
            oracle_stderr = error_log.read(500)
    if proc.returncode == 0:
        return _OracleResult("CLEAN")
    if proc.returncode == 1:
        return _OracleResult("CONFLICT")
    detail = oracle_stderr.strip().replace("\n", " ")
    return _OracleResult("ERROR", detail or f"git merge-tree exited {proc.returncode}")


def _uses_expiry_proxy(
    orbit_pairs: Sequence[tuple[OrbitIntent, OrbitIntent]],
) -> bool:
    return any(
        (
            left.provenance_mode == LEGACY_V2
            and left.end_source != "RELEASED_AT"
        )
        or (
            right.provenance_mode == LEGACY_V2
            and right.end_source != "RELEASED_AT"
        )
        for left, right in orbit_pairs
    )


def _window_basis(
    orbit_pairs: Sequence[tuple[OrbitIntent, OrbitIntent]],
) -> str:
    if all(
        orbit.provenance_mode == NATIVE_V3
        for pair in orbit_pairs
        for orbit in pair
    ):
        return "NATIVE_TERMINAL_EFFECTIVE_AT"
    released = [
        orbit.end_source == "RELEASED_AT" for pair in orbit_pairs for orbit in pair
    ]
    if all(released):
        return "RELEASED_ONLY"
    if not any(released):
        return "EXPIRY_PROXY_ONLY"
    return "MIXED_RELEASED_AND_EXPIRY_PROXY"


def _graph_stats(
    edges: Sequence[tuple[TaskAttempt, TaskAttempt]],
) -> tuple[int, int]:
    adjacency: dict[object, set[object]] = {}
    for left, right in edges:
        left_node = left.node_id
        right_node = right.node_id
        adjacency.setdefault(left_node, set()).add(right_node)
        adjacency.setdefault(right_node, set()).add(left_node)
    remaining = set(adjacency)
    components = 0
    while remaining:
        components += 1
        stack = [remaining.pop()]
        while stack:
            node = stack.pop()
            neighbors = adjacency[node] & remaining
            remaining.difference_update(neighbors)
            stack.extend(neighbors)
    return len(adjacency), components


def _scope_payload(
    name: str,
    attempts: Sequence[TaskAttempt],
    repo_facts: _GitRepoFacts | None,
    path_roots: Sequence[str],
    evidence_limit: int,
    max_candidate_pairs: int,
    max_oracle_pairs: int,
    max_pair_comparisons: int,
    max_orbit_pair_comparisons: int,
    max_path_pair_comparisons: int,
    provenance_mode: str,
) -> dict:
    candidates: list[
        tuple[
            TaskAttempt,
            TaskAttempt,
            list[tuple[OrbitIntent, OrbitIntent]],
            list[tuple[OrbitIntent, OrbitIntent]],
        ]
    ] = []
    pair_comparisons = len(attempts) * (len(attempts) - 1) // 2
    if pair_comparisons > max_pair_comparisons:
        raise PilotDataError(
            f"scope {name}: pair comparison limit {max_pair_comparisons} exceeded"
        )
    orbit_pair_comparisons = 0
    path_pair_comparisons = 0
    structurally_ineligible_pairs_excluded = 0
    cross_repository_pairs_excluded = 0
    for left, right in itertools.combinations(
        sorted(
            attempts,
            key=lambda attempt: (
                attempt.task_id,
                attempt.agent_id,
                attempt.attempt_id or "",
            ),
        ),
        2,
    ):
        orbit_pair_comparisons += left.orbit_rows * right.orbit_rows
        if orbit_pair_comparisons > max_orbit_pair_comparisons:
            raise PilotDataError(
                f"scope {name}: orbit pair comparison limit "
                f"{max_orbit_pair_comparisons} exceeded"
            )
        temporal_pairs = _overlapping_time_orbits(left, right)
        if not temporal_pairs:
            continue
        path_pair_comparisons += sum(
            len(left_orbit.paths) * len(right_orbit.paths)
            for left_orbit, right_orbit in temporal_pairs
        )
        if path_pair_comparisons > max_path_pair_comparisons:
            raise PilotDataError(
                f"scope {name}: path pair comparison limit "
                f"{max_path_pair_comparisons} exceeded"
            )
        if left.task_id == right.task_id or left.agent_id == right.agent_id:
            structurally_ineligible_pairs_excluded += 1
            continue
        if provenance_mode == NATIVE_V3 and (
            left.repo_id,
            left.integration_branch,
        ) != (
            right.repo_id,
            right.integration_branch,
        ):
            cross_repository_pairs_excluded += 1
            continue
        candidates.append(
            (
                left,
                right,
                temporal_pairs,
                _overlapping_write_orbits(temporal_pairs, path_roots),
            )
        )
        if len(candidates) > max_candidate_pairs:
            raise PilotDataError(
                f"scope {name}: candidate pair limit {max_candidate_pairs} exceeded"
            )

    oracle_call_count = sum(
        1
        for left, right, _, _ in candidates
        if repo_facts is not None and left.branch_tip_sha and right.branch_tip_sha
    )
    if oracle_call_count > max_oracle_pairs:
        raise PilotDataError(
            f"scope {name}: Git oracle pair limit {max_oracle_pairs} exceeded"
        )

    pair_evidence: list[dict] = []
    oracle_clean = 0
    oracle_conflict = 0
    oracle_errors = 0
    oracle_missing = 0
    oracle_eligible = 0
    declared_overlap_count = 0
    nominal_expiry_count = 0
    declared_expiry_count = 0
    candidate_basis_counts = (
        {"NATIVE_TERMINAL_EFFECTIVE_AT": 0}
        if provenance_mode == NATIVE_V3
        else {
            "RELEASED_ONLY": 0,
            "EXPIRY_PROXY_ONLY": 0,
            "MIXED_RELEASED_AND_EXPIRY_PROXY": 0,
        }
    )
    declared_basis_counts = dict.fromkeys(candidate_basis_counts, 0)

    for left, right, temporal_pairs, write_pairs in candidates:
        declared_overlap = bool(write_pairs)
        if declared_overlap:
            declared_overlap_count += 1
        nominal_basis = _window_basis(temporal_pairs)
        candidate_basis_counts[nominal_basis] += 1
        uses_expiry = _uses_expiry_proxy(temporal_pairs)
        if uses_expiry:
            nominal_expiry_count += 1
        if declared_overlap and _uses_expiry_proxy(write_pairs):
            declared_expiry_count += 1
        if declared_overlap:
            declared_basis_counts[_window_basis(write_pairs)] += 1

        oracle = _OracleResult("MISSING_PROVENANCE")
        if repo_facts is not None and left.branch_tip_sha and right.branch_tip_sha:
            oracle_eligible += 1
            oracle = _merge_tree_oracle(
                repo_facts, left.branch_tip_sha, right.branch_tip_sha
            )
            if oracle.outcome == "CLEAN":
                oracle_clean += 1
            elif oracle.outcome == "CONFLICT":
                oracle_conflict += 1
            else:
                oracle_errors += 1
        else:
            oracle_missing += 1

        if len(pair_evidence) < evidence_limit:
            pair_evidence.append(
                {
                    "tasks": [left.task_id, right.task_id],
                    "agents": [left.agent_id, right.agent_id],
                    "paths": [list(left.paths), list(right.paths)],
                    "branch_tips": [left.branch_tip_sha, right.branch_tip_sha],
                    "branch_tip_provenance": [
                        left.branch_tip_provenance,
                        right.branch_tip_provenance,
                    ],
                    "acquisition_epochs": [
                        left.acquisition_epochs,
                        right.acquisition_epochs,
                    ],
                    "declared_overlap": declared_overlap,
                    "uses_expiry_proxy": uses_expiry,
                    "nominal_window_basis": nominal_basis,
                    "nominal_window_overlap_seconds_max": max(
                        min(left_orbit.ended_at, right_orbit.ended_at)
                        - max(left_orbit.started_at, right_orbit.started_at)
                        for left_orbit, right_orbit in temporal_pairs
                    ),
                    "declared_overlap_window_seconds_max": (
                        max(
                            min(left_orbit.ended_at, right_orbit.ended_at)
                            - max(left_orbit.started_at, right_orbit.started_at)
                            for left_orbit, right_orbit in write_pairs
                        )
                        if write_pairs
                        else None
                    ),
                    "temporally_overlapping_orbits": [
                        {
                            "orbit_ids": [left_orbit.orbit_id, right_orbit.orbit_id],
                            "snapshot_states": [left_orbit.state, right_orbit.state],
                            "end_sources": [
                                left_orbit.end_source,
                                right_orbit.end_source,
                            ],
                            "started_at": [
                                left_orbit.started_at,
                                right_orbit.started_at,
                            ],
                            "ended_at": [
                                left_orbit.ended_at,
                                right_orbit.ended_at,
                            ],
                        }
                        for left_orbit, right_orbit in temporal_pairs
                    ],
                    "declared_overlapping_orbits": [
                        {
                            "orbit_ids": [left_orbit.orbit_id, right_orbit.orbit_id],
                            "snapshot_states": [left_orbit.state, right_orbit.state],
                            "end_sources": [
                                left_orbit.end_source,
                                right_orbit.end_source,
                            ],
                            "started_at": [
                                left_orbit.started_at,
                                right_orbit.started_at,
                            ],
                            "ended_at": [
                                left_orbit.ended_at,
                                right_orbit.ended_at,
                            ],
                            "paths": [list(left_orbit.paths), list(right_orbit.paths)],
                            "canonical_paths": [
                                list(_canonical_paths(left_orbit.paths, path_roots)),
                                list(_canonical_paths(right_orbit.paths, path_roots)),
                            ],
                        }
                        for left_orbit, right_orbit in write_pairs
                    ],
                    "git_oracle": oracle.outcome,
                    "oracle_detail": oracle.detail,
                }
            )
            if provenance_mode == NATIVE_V3:
                pair_evidence[-1].update(
                    {
                        "attempt_ids": [left.attempt_id, right.attempt_id],
                        "attempt_ordinals": [
                            left.attempt_ordinal,
                            right.attempt_ordinal,
                        ],
                        "repository_identities": [
                            [left.repo_id, left.integration_branch],
                            [right.repo_id, right.integration_branch],
                        ],
                        "canonical_first_connect_outcomes": [
                            left.canonical_connect_outcome,
                            right.canonical_connect_outcome,
                        ],
                    }
                )

    candidate_count = len(candidates)
    resolved_oracles = oracle_clean + oracle_conflict
    candidate_nodes, candidate_components = _graph_stats(
        [(left, right) for left, right, _, _ in candidates]
    )
    declared_nodes, declared_components = _graph_stats(
        [(left, right) for left, right, _, write_pairs in candidates if write_pairs]
    )
    if not attempts:
        coverage_status = "NO_DATA"
    elif candidate_count == 0:
        coverage_status = "NO_CANDIDATE_WINDOWS"
    elif resolved_oracles < candidate_count:
        coverage_status = "INCOMPLETE"
    else:
        coverage_status = "COMPLETE"

    if candidate_count == 0:
        declared_status = "NO_CANDIDATES"
    elif declared_overlap_count == 0:
        declared_status = "ZERO"
    else:
        declared_status = "NONZERO"

    payload = {
        "scope": name,
        "path_roots": list(path_roots),
        "task_agent_groups": len(attempts),
        "orbit_rows": sum(attempt.orbit_rows for attempt in attempts),
        "pair_comparisons": pair_comparisons,
        "orbit_pair_comparisons": orbit_pair_comparisons,
        "path_pair_comparisons": path_pair_comparisons,
        "candidate_nominal_window_pairs": candidate_count,
        "candidate_unique_task_pairs": len(
            {
                tuple(sorted((left.task_id, right.task_id)))
                for left, right, _, _ in candidates
            }
        ),
        "candidate_graph_groups": candidate_nodes,
        "candidate_graph_components": candidate_components,
        "candidate_pairs_with_expiry_proxy": nominal_expiry_count,
        "candidate_window_basis_counts": candidate_basis_counts,
        "structurally_ineligible_nominal_pairs_excluded": (
            structurally_ineligible_pairs_excluded
        ),
        "declared_overlap_pairs": declared_overlap_count,
        "declared_overlap_unique_task_pairs": len(
            {
                tuple(sorted((left.task_id, right.task_id)))
                for left, right, _, write_pairs in candidates
                if write_pairs
            }
        ),
        "declared_overlap_graph_groups": declared_nodes,
        "declared_overlap_graph_components": declared_components,
        "declared_overlap_pairs_with_expiry_proxy": declared_expiry_count,
        "declared_overlap_window_basis_counts": declared_basis_counts,
        "declared_overlap_pair_fraction": (
            declared_overlap_count / candidate_count if candidate_count else None
        ),
        "declared_overlap_status": declared_status,
        "oracle_eligible_pairs": oracle_eligible,
        "pairwise_tip_clean_pairs": oracle_clean,
        "pairwise_tip_conflict_pairs": oracle_conflict,
        "git_oracle_errors": oracle_errors,
        "git_provenance_missing_pairs": oracle_missing,
        "oracle_coverage": resolved_oracles / candidate_count
        if candidate_count
        else None,
        "pairwise_tip_conflict_pair_fraction": (
            oracle_conflict / resolved_oracles if resolved_oracles else None
        ),
        "oracle_coverage_status": coverage_status,
        "counterfactual_pairwise_tip_conflict_observed": oracle_conflict > 0,
        "field_endpoint_status": "NOT_ASSESSED",
        "descriptive_only": True,
        "statistical_inference_permitted": False,
        "pair_evidence": pair_evidence,
        "pair_evidence_truncated": len(pair_evidence) < candidate_count,
    }
    if provenance_mode == NATIVE_V3:
        merge_order = [
            {
                "merge_gen": connect.merge_gen,
                "attempt_id": attempt.attempt_id,
                "connect_attempt_id": connect.connect_attempt_id,
                "task_id": attempt.task_id,
                "merge_sha": connect.merge_sha,
            }
            for attempt in attempts
            for connect in attempt.connect_attempts
            if connect.outcome == "MERGED"
        ]
        merge_order.sort(
            key=lambda item: (item["merge_gen"], item["connect_attempt_id"])
        )
        outcome_counts: dict[str, int] = {}
        for attempt in attempts:
            outcome = attempt.canonical_connect_outcome or "NO_CONNECT_ATTEMPT"
            outcome_counts[outcome] = outcome_counts.get(outcome, 0) + 1
        payload.update(
            {
                "attempt_rows": len(attempts),
                "cross_repository_nominal_pairs_excluded": (
                    cross_repository_pairs_excluded
                ),
                "canonical_first_connect_outcome_counts": dict(
                    sorted(outcome_counts.items())
                ),
                "merge_order": merge_order,
            }
        )
    return payload


def evaluate_attempts(
    attempts: Sequence[TaskAttempt],
    scopes: Sequence[ScopeRule],
    *,
    git_repos: Mapping[str, str | Path] | None = None,
    path_roots: Mapping[str, Sequence[str | Path]] | None = None,
    source_db: str | Path | None = None,
    source_filters: Mapping[str, object] | None = None,
    provenance_mode: str | None = None,
    canonical_input_sha256: str | None = None,
    source_metadata: Mapping[str, object] | None = None,
    evidence_limit: int = 100,
    max_candidate_pairs: int = 10_000,
    max_oracle_pairs: int = 1_000,
    max_pair_comparisons: int = 100_000,
    max_orbit_pair_comparisons: int = 1_000_000,
    max_path_pair_comparisons: int = 100_000,
    max_paths_per_orbit: int = 1_000,
    max_pathspec_bytes: int = 1_000_000,
) -> PilotReport:
    if not scopes:
        raise ValueError("at least one scope is required")
    scope_names = {scope.name for scope in scopes}
    if len(scope_names) != len(scopes):
        raise ValueError("scope names must be unique")
    if evidence_limit < 0:
        raise ValueError("evidence_limit must be non-negative")
    if (
        max_candidate_pairs < 0
        or max_oracle_pairs < 0
        or max_pair_comparisons < 0
        or max_orbit_pair_comparisons < 0
        or max_path_pair_comparisons < 0
    ):
        raise ValueError("pair limits must be non-negative")
    if max_paths_per_orbit < 1 or max_pathspec_bytes < 1:
        raise ValueError("pathspec limits must be positive")
    attempt_modes = {attempt.provenance_mode for attempt in attempts}
    if provenance_mode is None:
        if NATIVE_V3 in attempt_modes:
            provenance_mode = NATIVE_V3
        else:
            provenance_mode = LEGACY_V2
    if provenance_mode not in (LEGACY_V2, NATIVE_V3):
        raise ValueError(f"unknown provenance mode: {provenance_mode}")
    if attempt_modes - {provenance_mode}:
        raise PilotDataError("legacy and native attempts cannot be mixed in one report")
    schema = SCHEMA_V3 if provenance_mode == NATIVE_V3 else SCHEMA_V2

    raw_repo_map = dict(git_repos or {})
    unknown_repos = sorted(set(raw_repo_map) - scope_names)
    if unknown_repos:
        raise ValueError(
            f"Git repo supplied for unknown scopes: {', '.join(unknown_repos)}"
        )
    repo_map = {
        name: _probe_git_repo(Path(path)) for name, path in raw_repo_map.items()
    }

    root_map: dict[str, tuple[str, ...]] = {}
    for name, raw_roots in (path_roots or {}).items():
        if name not in scope_names:
            raise ValueError(f"path roots supplied for unknown scope: {name}")
        normalized_roots: set[str] = set()
        for raw_root in raw_roots:
            root = str(raw_root).rstrip("/")
            if not root or any(char in root for char in "*?["):
                raise ValueError(f"path root must be a non-glob literal: {raw_root}")
            normalized_roots.add(root)
        root_map[name] = tuple(sorted(normalized_roots))

    assigned: dict[str, list[TaskAttempt]] = {scope.name: [] for scope in scopes}
    ambiguous: list[str] = []
    unclassified: list[str] = []
    for attempt in attempts:
        matched = [scope.name for scope in scopes if scope.matches(attempt)]
        if len(matched) == 1:
            assigned[matched[0]].append(attempt)
        elif matched:
            ambiguous.append(attempt.display_id)
        else:
            unclassified.append(attempt.display_id)

    canonical_input_sha = canonical_input_sha256 or _input_digest(attempts)
    filters = dict(source_filters or {})
    measurement_config = {
        "schema": schema,
        "implementation_files_sha256": _implementation_hashes(),
        "measurement_hash_policy": "canonical-report-without-measurement-sha256/v1",
        "snapshot_policy": SNAPSHOT_POLICY,
        "oracle_policy": ORACLE_POLICY,
        "source_db": (
            str(Path(source_db).expanduser().resolve()) if source_db else None
        ),
        "source_filters": filters,
        "scopes": [{"name": scope.name, "pattern": scope.pattern} for scope in scopes],
        "path_roots": {
            scope.name: list(root_map.get(scope.name, ())) for scope in scopes
        },
        "git_repositories": {
            name: facts.manifest() for name, facts in sorted(repo_map.items())
        },
        "evidence_limit": evidence_limit,
        "max_candidate_pairs": max_candidate_pairs,
        "max_oracle_pairs": max_oracle_pairs,
        "max_pair_comparisons": max_pair_comparisons,
        "max_orbit_pair_comparisons": max_orbit_pair_comparisons,
        "max_path_pair_comparisons": max_path_pair_comparisons,
        "max_paths_per_orbit": max_paths_per_orbit,
        "max_pathspec_bytes": max_pathspec_bytes,
    }
    source_payload = {
        "coord_db": measurement_config["source_db"],
        "canonical_input_sha256": canonical_input_sha,
        "task_agent_groups": len(attempts),
        "orbit_rows": sum(attempt.orbit_rows for attempt in attempts),
        "filters": filters,
    }
    if provenance_mode == NATIVE_V3:
        source_payload.update(dict(source_metadata or {}))
        source_payload["provenance_mode"] = NATIVE_V3
        source_payload["attempt_rows"] = len(attempts)
    if provenance_mode == NATIVE_V3:
        semantics = {
            "temporal_signal": (
                "different task and agent attempts with intersecting half-open "
                "authoritative execution exposure windows"
            ),
            "window_start": "max(task_attempts.started_at, orbits.granted_at)",
            "window_end": "orbits.terminal_effective_at (exclusive)",
            "attempt_identity": (
                "task_attempts.attempt_id; task_id/agent_id reuse never coalesces requeues"
            ),
            "connect_authority": (
                "connect_seq=1 supplies the canonical Phase-A tip/outcome; all retries "
                "remain in the input digest; merge_gen supplies global successful order"
            ),
            "declared_overlap": (
                "OMD conservative glob intersection between temporally intersecting "
                "native write-capable workload orbit rows"
            ),
            "git_oracle": (
                "counterfactual pairwise git merge-tree --write-tree over first-admitted "
                "connect tips with neutral built-in text-merge attributes in a "
                "config-isolated bare repository"
            ),
            "limitations": [
                "lease exposure is not continuous agent execution",
                "pairwise branch-tip merge is not historical integration-base/order replay",
                "neutral built-in attributes can differ from repository-native custom merge semantics",
                "candidate pairs share tasks and are not independent statistical samples",
            ],
        }
    else:
        semantics = {
            "temporal_signal": (
                "different task and agent with intersecting half-open nominal "
                "lease/request windows"
            ),
            "window_start": (
                "created_at request-row creation; it may precede grant after PENDING "
                "promotion because granted_at is not persisted"
            ),
            "window_end": (
                "released_at when present; otherwise expires_at proxy; otherwise "
                "a zero-width start-only window"
            ),
            "declared_overlap": (
                "OMD conservative glob intersection between temporally intersecting "
                "write-capable workload orbit rows"
            ),
            "git_oracle": (
                "counterfactual pairwise git merge-tree --write-tree over provenance-"
                "eligible branch tips with neutral built-in text-merge attributes in "
                "a config-isolated bare repository"
            ),
            "limitations": [
                "nominal lease/request-window overlap is not continuous agent execution",
                "pairwise branch-tip merge is not historical OMD connect/base/order replay",
                "neutral built-in attributes can differ from repository-native custom merge semantics",
                "candidate pairs share tasks and are not independent statistical samples",
            ],
        }
    payload = {
        "schema": schema,
        "measurement_config": measurement_config,
        "source": source_payload,
        "semantics": semantics,
        "metric_promotion": {
            "ready": False,
            "status": "NOT_ASSESSED_BY_EXPLORATORY_PILOT",
            "reason": (
                "pair edges are not independent samples and this tool has no "
                "preregistered exposure or break-even criterion"
            ),
        },
        "ambiguous_task_agent_groups": sorted(set(ambiguous)),
        "unclassified_task_agent_groups": sorted(set(unclassified)),
        "scopes": [
            _scope_payload(
                scope.name,
                assigned[scope.name],
                repo_map.get(scope.name),
                root_map.get(scope.name, ()),
                evidence_limit,
                max_candidate_pairs,
                max_oracle_pairs,
                max_pair_comparisons,
                max_orbit_pair_comparisons,
                max_path_pair_comparisons,
                provenance_mode,
            )
            for scope in scopes
        ],
    }
    payload["measurement_sha256"] = _canonical_json_sha256(payload)
    return PilotReport(payload)


def run_pilot(
    db_path: str | Path,
    scopes: Sequence[ScopeRule],
    *,
    git_repos: Mapping[str, str | Path] | None = None,
    path_roots: Mapping[str, Sequence[str | Path]] | None = None,
    evidence_limit: int = 100,
    created_before: float | None = None,
    exclude_task_ids: Iterable[str] = (),
    max_candidate_pairs: int = 10_000,
    max_oracle_pairs: int = 1_000,
    max_pair_comparisons: int = 100_000,
    max_orbit_pair_comparisons: int = 1_000_000,
    max_path_pair_comparisons: int = 100_000,
    max_paths_per_orbit: int = 1_000,
    max_pathspec_bytes: int = 1_000_000,
) -> PilotReport:
    excluded = tuple(sorted(set(exclude_task_ids)))
    loaded = _load_pilot_input(
        db_path,
        created_before=created_before,
        exclude_task_ids=excluded,
        max_paths_per_orbit=max_paths_per_orbit,
        max_pathspec_bytes=max_pathspec_bytes,
    )
    return evaluate_attempts(
        loaded.attempts,
        scopes,
        git_repos=git_repos,
        path_roots=path_roots,
        source_db=db_path,
        source_filters={
            "created_before_exclusive": created_before,
            "excluded_task_ids": list(excluded),
        },
        provenance_mode=loaded.provenance_mode,
        canonical_input_sha256=loaded.canonical_input_sha256,
        source_metadata=loaded.source_metadata,
        evidence_limit=evidence_limit,
        max_candidate_pairs=max_candidate_pairs,
        max_oracle_pairs=max_oracle_pairs,
        max_pair_comparisons=max_pair_comparisons,
        max_orbit_pair_comparisons=max_orbit_pair_comparisons,
        max_path_pair_comparisons=max_path_pair_comparisons,
        max_paths_per_orbit=max_paths_per_orbit,
        max_pathspec_bytes=max_pathspec_bytes,
    )


def _named_paths(specs: Iterable[str]) -> dict[str, Path]:
    result: dict[str, Path] = {}
    for spec in specs:
        if "=" not in spec:
            raise ValueError("Git repo must be NAME=PATH")
        name, raw_path = spec.split("=", 1)
        if not name or not raw_path:
            raise ValueError("Git repo must be NAME=PATH")
        if name in result:
            raise ValueError(f"duplicate Git repo scope: {name}")
        result[name] = Path(raw_path)
    return result


def _named_roots(specs: Iterable[str]) -> dict[str, list[str]]:
    result: dict[str, list[str]] = {}
    for spec in specs:
        if "=" not in spec:
            raise ValueError("path root must be NAME=PREFIX")
        name, prefix = spec.split("=", 1)
        if not name or not prefix:
            raise ValueError("path root must be NAME=PREFIX")
        result.setdefault(name, []).append(prefix)
    return result


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Measure OMD nominal-window write-set overlap and pairwise branch-tip "
            "oracle coverage"
        )
    )
    parser.add_argument(
        "--db",
        required=True,
        help="OMD coordination SQLite DB (read through a stable temporary DB+WAL copy)",
    )
    parser.add_argument(
        "--scope",
        action="append",
        required=True,
        metavar="NAME=REGEX",
        help="cohort selector over task ID, agent ID, and paths; repeatable",
    )
    parser.add_argument(
        "--git-repo",
        action="append",
        default=[],
        metavar="NAME=PATH",
        help="isolated pairwise branch-tip Git oracle for a named scope; repeatable",
    )
    parser.add_argument(
        "--path-root",
        action="append",
        default=[],
        metavar="NAME=PREFIX",
        help="strip a historical clone root before overlap comparison; repeatable",
    )
    parser.add_argument("--evidence-limit", type=int, default=100)
    parser.add_argument("--max-candidate-pairs", type=int, default=10_000)
    parser.add_argument("--max-oracle-pairs", type=int, default=1_000)
    parser.add_argument("--max-pair-comparisons", type=int, default=100_000)
    parser.add_argument("--max-orbit-pair-comparisons", type=int, default=1_000_000)
    parser.add_argument("--max-path-pair-comparisons", type=int, default=100_000)
    parser.add_argument("--max-paths-per-orbit", type=int, default=1_000)
    parser.add_argument("--max-pathspec-bytes", type=int, default=1_000_000)
    parser.add_argument(
        "--created-before",
        type=float,
        default=None,
        metavar="UNIX_SECONDS",
        help="include only orbit rows created strictly before this timestamp",
    )
    parser.add_argument(
        "--exclude-task",
        action="append",
        default=[],
        metavar="TASK_ID",
        help="exclude a measurement/control task; repeatable",
    )
    parser.add_argument(
        "--require-complete-oracle",
        "--require-measured",
        dest="require_complete_oracle",
        action="store_true",
        help="exit 3 unless every scope has complete pairwise Git-oracle coverage",
    )
    parser.add_argument("--compact", action="store_true", help="emit one-line JSON")
    args = parser.parse_args(argv)
    try:
        scopes = [ScopeRule.from_spec(spec) for spec in args.scope]
        report = run_pilot(
            args.db,
            scopes,
            git_repos=_named_paths(args.git_repo),
            path_roots=_named_roots(args.path_root),
            evidence_limit=args.evidence_limit,
            created_before=args.created_before,
            exclude_task_ids=args.exclude_task,
            max_candidate_pairs=args.max_candidate_pairs,
            max_oracle_pairs=args.max_oracle_pairs,
            max_pair_comparisons=args.max_pair_comparisons,
            max_orbit_pair_comparisons=args.max_orbit_pair_comparisons,
            max_path_pair_comparisons=args.max_path_pair_comparisons,
            max_paths_per_orbit=args.max_paths_per_orbit,
            max_pathspec_bytes=args.max_pathspec_bytes,
        )
    except (OSError, PilotDataError, ValueError, sqlite3.Error) as exc:
        print(f"omd-overlap-pilot: {exc}", file=sys.stderr)
        return 2

    payload = report.to_dict()
    print(
        json.dumps(
            payload,
            ensure_ascii=False,
            sort_keys=True,
            indent=None if args.compact else 2,
            separators=(",", ":") if args.compact else None,
        )
    )
    if payload["ambiguous_task_agent_groups"] or any(
        scope["git_oracle_errors"] for scope in payload["scopes"]
    ):
        return 2
    if args.require_complete_oracle and any(
        scope["oracle_coverage_status"] != "COMPLETE" for scope in payload["scopes"]
    ):
        return 3
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
