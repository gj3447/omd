"""Pure scheduler admission kernel.

The Coordinator owns serialization and persistence.  This module owns only the
deterministic part of admission: mode compatibility, exact overlap, rank, and
blocker classification.  Initial claim and later promotion must both call this
same kernel so that queue policy cannot drift between the two paths.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from typing import Any, Iterable, Mapping, Sequence

from .disjoint import sets_overlap


ADMISSION_POLICY_VERSION = "omd-admission/fair-reservation-v1"
LEGACY_ADMISSION_POLICY_VERSION = "omd-admission/legacy-backfill-v1"
MODES = frozenset({"read", "write", "shared"})


def canonical_json(value: Any) -> str:
    """Return the byte-stable JSON form used by admission digests."""
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )


def sha256_json(value: Any) -> str:
    return hashlib.sha256(canonical_json(value).encode("utf-8")).hexdigest()


def normalize_pathspec(pathspec: Sequence[str]) -> tuple[str, ...]:
    """Canonicalize a path set without changing glob spelling semantics."""
    if isinstance(pathspec, (str, bytes)):
        raise TypeError("pathspec must be a sequence of strings")
    paths = tuple(sorted(set(pathspec)))
    if not paths or not all(isinstance(path, str) and path for path in paths):
        raise ValueError("pathspec must contain at least one non-empty string")
    return paths


def pathspec_digest(pathspec: Sequence[str]) -> str:
    return sha256_json(list(normalize_pathspec(pathspec)))


def modes_compatible(left: str, right: str) -> bool:
    """Compatibility matrix from scheduler_admission_engine.json."""
    if left not in MODES or right not in MODES:
        raise ValueError(f"unsupported orbit mode pair: {left!r}, {right!r}")
    return (left == right == "read") or (left == right == "shared")


def rank_key(priority: int, queue_seq: int) -> tuple[int, int]:
    """Smaller tuple means higher rank: priority DESC, durable seq ASC."""
    return (-int(priority), int(queue_seq))


def _paths(row: Mapping[str, Any]) -> Sequence[str]:
    raw = row["pathspec"]
    return json.loads(raw) if isinstance(raw, str) else raw


def exact_conflict(
    pathspec: Sequence[str],
    mode: str,
    row: Mapping[str, Any],
) -> bool:
    return not modes_compatible(mode, str(row["mode"])) and sets_overlap(
        pathspec, _paths(row)
    )


@dataclass(frozen=True)
class AdmissionRequest:
    pathspec: tuple[str, ...]
    mode: str
    priority: int
    queue_seq: int
    orbit_id: str | None = None

    @classmethod
    def build(
        cls,
        pathspec: Sequence[str],
        mode: str,
        priority: int,
        queue_seq: int,
        orbit_id: str | None = None,
    ) -> "AdmissionRequest":
        if mode not in MODES:
            raise ValueError(f"unsupported orbit mode: {mode!r}")
        if isinstance(priority, bool) or not isinstance(priority, int):
            raise ValueError("priority must be an integer")
        if isinstance(queue_seq, bool) or not isinstance(queue_seq, int) or queue_seq < 0:
            raise ValueError("queue_seq must be a non-negative integer")
        return cls(normalize_pathspec(pathspec), mode, priority, queue_seq, orbit_id)


@dataclass(frozen=True)
class AdmissionDecision:
    outcome: str
    held_blockers: tuple[str, ...]
    pending_predecessors: tuple[str, ...]

    @property
    def blocker_ids(self) -> tuple[str, ...]:
        return self.held_blockers + self.pending_predecessors

    @property
    def grantable(self) -> bool:
        return self.outcome == "GRANT"


def decide_admission(
    request: AdmissionRequest,
    held: Iterable[Mapping[str, Any]],
    pending: Iterable[Mapping[str, Any]],
) -> AdmissionDecision:
    """Classify exact blockers for one request.

    A PENDING row blocks only when it conflicts and outranks the request.  This
    deliberately avoids a global queue head: unrelated work remains grantable.
    """
    held_ids = tuple(sorted(
        str(row["orbit_id"])
        for row in held
        if row.get("orbit_id") != request.orbit_id
        and exact_conflict(request.pathspec, request.mode, row)
    ))
    request_rank = rank_key(request.priority, request.queue_seq)
    pending_ids = tuple(sorted(
        str(row["orbit_id"])
        for row in pending
        if row.get("orbit_id") != request.orbit_id
        and exact_conflict(request.pathspec, request.mode, row)
        # A live row with missing rank authority is corrupt or only partially
        # migrated.  Treat it as a blocker instead of silently overtaking it.
        and (
            row.get("queue_seq") is None
            or rank_key(int(row.get("priority") or 0), int(row["queue_seq"]))
            < request_rank
        )
    ))
    return AdmissionDecision(
        outcome="QUEUE" if held_ids or pending_ids else "GRANT",
        held_blockers=held_ids,
        pending_predecessors=pending_ids,
    )


def authority_snapshot_hash(
    held: Iterable[Mapping[str, Any]],
    pending: Iterable[Mapping[str, Any]],
    *,
    coordinator_epoch: int | None,
) -> str:
    """Digest the authority facts consulted by a decision.

    This is observability/provenance for the current slice.  The transaction
    supplying these rows remains the authority; an event may never authenticate
    itself merely by presenting this digest.
    """
    rows = []
    for state, source in (("HELD", held), ("PENDING", pending)):
        for row in source:
            rows.append(
                {
                    "state": state,
                    "orbit_id": row.get("orbit_id"),
                    "agent_id": row.get("agent_id"),
                    "mode": row.get("mode"),
                    "pathspec": list(_paths(row)),
                    "priority": int(row.get("priority") or 0),
                    "queue_seq": row.get("queue_seq"),
                    "fence": row.get("fence"),
                }
            )
    rows.sort(key=lambda row: (row["state"], row["orbit_id"] or ""))
    return sha256_json({"coordinator_epoch": coordinator_epoch, "orbits": rows})
