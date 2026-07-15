"""Pure scheduler admission kernel.

The Coordinator owns serialization and persistence.  This module owns only the
deterministic part of admission: mode compatibility, exact overlap, rank, and
blocker classification.  Initial claim and later promotion must both call this
same kernel so that queue policy cannot drift between the two paths.
"""

from __future__ import annotations

import hashlib
import json
import math
from dataclasses import dataclass
from typing import Any, Iterable, Mapping, Sequence

from .disjoint import sets_overlap


STATIC_ADMISSION_POLICY_VERSION = "omd-admission/fair-reservation-v1"
LEGACY_ADMISSION_POLICY_VERSION = "omd-admission/legacy-backfill-v1"
AGING_POLICY_SCHEMA = "omd-admission/saturating-aging-policy-v2"
DEFAULT_ADMISSION_AGING_QUANTUM = 60.0
DEFAULT_ADMISSION_MAX_AGE_BOOST = 10
MIN_ADMISSION_PRIORITY = -(1 << 63)
MAX_ADMISSION_PRIORITY = (1 << 63) - 1
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


@dataclass(frozen=True)
class QueuePolicy:
    """Content-addressed queue ranking policy.

    The defaults are operational choices, not proof-derived constants.  The
    complete envelope is pinned per repository DB so a restart cannot silently
    reinterpret an existing v2 request with different aging parameters.
    """

    aging_quantum: float = DEFAULT_ADMISSION_AGING_QUANTUM
    max_age_boost: int = DEFAULT_ADMISSION_MAX_AGE_BOOST

    def __post_init__(self):
        if isinstance(self.aging_quantum, bool):
            raise ValueError("aging_quantum must be finite and positive")
        try:
            quantum = float(self.aging_quantum)
        except (TypeError, ValueError) as exc:
            raise ValueError("aging_quantum must be finite and positive") from exc
        if not math.isfinite(quantum) or quantum <= 0:
            raise ValueError("aging_quantum must be finite and positive")
        if (
            not isinstance(self.max_age_boost, int)
            or isinstance(self.max_age_boost, bool)
            or self.max_age_boost < 0
            or self.max_age_boost > MAX_ADMISSION_PRIORITY
        ):
            raise ValueError(
                "max_age_boost must be a non-negative signed-64-bit integer"
            )
        object.__setattr__(self, "aging_quantum", quantum)

    @property
    def envelope(self) -> dict[str, Any]:
        return {
            "schema": AGING_POLICY_SCHEMA,
            "rank": "base_priority_plus_saturating_age_boost",
            "priority_domain": "signed-64-with-ceiling-headroom/v1",
            "aging_quantum": self.aging_quantum,
            "max_age_boost": self.max_age_boost,
        }

    @property
    def version(self) -> str:
        return f"{AGING_POLICY_SCHEMA}/sha256:{sha256_json(self.envelope)}"

    def accepts_base_priority(
        self, base_priority: int, *, policy_version: str | None = None
    ) -> bool:
        """Whether a row's full policy range stays inside SQLite INTEGER."""
        if (
            isinstance(base_priority, bool)
            or not isinstance(base_priority, int)
            or not MIN_ADMISSION_PRIORITY <= base_priority <= MAX_ADMISSION_PRIORITY
        ):
            return False
        version = self.version if policy_version is None else policy_version
        if version in {
            STATIC_ADMISSION_POLICY_VERSION,
            LEGACY_ADMISSION_POLICY_VERSION,
        }:
            return True
        return (
            version == self.version
            and base_priority <= MAX_ADMISSION_PRIORITY - self.max_age_boost
        )

    def effective_priority(
        self,
        base_priority: int,
        *,
        policy_version: str,
        enqueued_at: float | None,
        observed_at: float,
        allow_unenqueued: bool = False,
    ) -> int | None:
        """Return the replayable rank value, or ``None`` for unknown authority."""
        if policy_version in {
            STATIC_ADMISSION_POLICY_VERSION,
            LEGACY_ADMISSION_POLICY_VERSION,
        }:
            return base_priority if self.accepts_base_priority(
                base_priority, policy_version=policy_version
            ) else None
        if not self.accepts_base_priority(
            base_priority, policy_version=policy_version
        ):
            return None
        if (
            not isinstance(observed_at, (int, float))
            or isinstance(observed_at, bool)
            or not math.isfinite(observed_at)
        ):
            return None
        if enqueued_at is None and allow_unenqueued:
            enqueued_at = observed_at
        if (
            not isinstance(enqueued_at, (int, float))
            or isinstance(enqueued_at, bool)
            or not math.isfinite(enqueued_at)
        ):
            return None
        age = max(0.0, float(observed_at) - float(enqueued_at))
        if self.max_age_boost == 0:
            boost = 0
        else:
            # Division may legitimately overflow to +inf for a positive
            # subnormal quantum. Saturate before floor() so the policy remains
            # total for every finite, positive configured quantum.
            steps = age / self.aging_quantum
            boost = (
                self.max_age_boost
                if steps >= self.max_age_boost
                else math.floor(steps)
            )
        return base_priority + boost

    def rank_key(
        self,
        base_priority: int,
        queue_seq: int,
        *,
        policy_version: str,
        enqueued_at: float | None,
        observed_at: float,
        allow_unenqueued: bool = False,
    ) -> tuple[int, int] | None:
        if isinstance(queue_seq, bool) or not isinstance(queue_seq, int) or queue_seq < 0:
            return None
        effective = self.effective_priority(
            base_priority,
            policy_version=policy_version,
            enqueued_at=enqueued_at,
            observed_at=observed_at,
            allow_unenqueued=allow_unenqueued,
        )
        return None if effective is None else (-effective, queue_seq)


DEFAULT_QUEUE_POLICY = QueuePolicy()
ADMISSION_POLICY_VERSION = DEFAULT_QUEUE_POLICY.version


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
    policy_version: str = ADMISSION_POLICY_VERSION
    enqueued_at: float | None = None

    @classmethod
    def build(
        cls,
        pathspec: Sequence[str],
        mode: str,
        priority: int,
        queue_seq: int,
        orbit_id: str | None = None,
        policy_version: str = ADMISSION_POLICY_VERSION,
        enqueued_at: float | None = None,
    ) -> "AdmissionRequest":
        if mode not in MODES:
            raise ValueError(f"unsupported orbit mode: {mode!r}")
        if isinstance(priority, bool) or not isinstance(priority, int):
            raise ValueError("priority must be an integer")
        if not MIN_ADMISSION_PRIORITY <= priority <= MAX_ADMISSION_PRIORITY:
            raise ValueError("priority must be a signed-64-bit integer")
        if isinstance(queue_seq, bool) or not isinstance(queue_seq, int) or queue_seq < 0:
            raise ValueError("queue_seq must be a non-negative integer")
        if not isinstance(policy_version, str) or not policy_version:
            raise ValueError("policy_version must be a non-empty string")
        if enqueued_at is not None and (
            not isinstance(enqueued_at, (int, float))
            or isinstance(enqueued_at, bool)
            or not math.isfinite(enqueued_at)
        ):
            raise ValueError("enqueued_at must be a finite timestamp")
        return cls(
            normalize_pathspec(pathspec),
            mode,
            priority,
            queue_seq,
            orbit_id,
            policy_version,
            enqueued_at,
        )


@dataclass(frozen=True)
class AdmissionDecision:
    outcome: str
    held_blockers: tuple[str, ...]
    pending_predecessors: tuple[str, ...]
    base_priority: int
    effective_priority: int
    observed_at: float

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
    *,
    policy: QueuePolicy = DEFAULT_QUEUE_POLICY,
    observed_at: float = 0.0,
) -> AdmissionDecision:
    """Classify exact blockers for one request.

    A PENDING row blocks only when it conflicts and outranks the request.  This
    deliberately avoids a global queue head: unrelated work remains grantable.
    """
    if (
        not isinstance(observed_at, (int, float))
        or isinstance(observed_at, bool)
        or not math.isfinite(observed_at)
    ):
        raise ValueError("observed_at must be a finite timestamp")
    held_ids = tuple(sorted(
        str(row["orbit_id"])
        for row in held
        if row.get("orbit_id") != request.orbit_id
        and exact_conflict(request.pathspec, request.mode, row)
    ))
    request_rank = policy.rank_key(
        request.priority,
        request.queue_seq,
        policy_version=request.policy_version,
        enqueued_at=request.enqueued_at,
        observed_at=observed_at,
        allow_unenqueued=True,
    )
    if request_rank is None:
        raise ValueError("request queue policy authority is unavailable")
    effective_priority = -request_rank[0]

    def pending_outranks(row: Mapping[str, Any]) -> bool:
        if row.get("queue_seq") is None or not row.get("policy_version"):
            return True
        predecessor_rank = policy.rank_key(
            int(row.get("priority") or 0),
            int(row["queue_seq"]),
            policy_version=row["policy_version"],
            enqueued_at=row.get("enqueued_at"),
            observed_at=observed_at,
        )
        return predecessor_rank is None or predecessor_rank < request_rank

    pending_ids = tuple(sorted(
        str(row["orbit_id"])
        for row in pending
        if row.get("orbit_id") != request.orbit_id
        and exact_conflict(request.pathspec, request.mode, row)
        # A live row with missing rank authority is corrupt or only partially
        # migrated.  Treat it as a blocker instead of silently overtaking it.
        and pending_outranks(row)
    ))
    return AdmissionDecision(
        outcome="QUEUE" if held_ids or pending_ids else "GRANT",
        held_blockers=held_ids,
        pending_predecessors=pending_ids,
        base_priority=request.priority,
        effective_priority=effective_priority,
        observed_at=float(observed_at),
    )


def authority_snapshot_hash(
    held: Iterable[Mapping[str, Any]],
    pending: Iterable[Mapping[str, Any]],
    *,
    coordinator_epoch: int | None,
    policy: QueuePolicy | None = None,
    observed_at: float | None = None,
) -> str:
    """Digest the authority facts consulted by a decision.

    This is observability/provenance for the current slice.  The transaction
    supplying these rows remains the authority; an event may never authenticate
    itself merely by presenting this digest.
    """
    rows = []
    for state, source in (("HELD", held), ("PENDING", pending)):
        for row in source:
            item = {
                    "state": state,
                    "orbit_id": row.get("orbit_id"),
                    "agent_id": row.get("agent_id"),
                    "mode": row.get("mode"),
                    "pathspec": list(_paths(row)),
                    "priority": int(row.get("priority") or 0),
                    "queue_seq": row.get("queue_seq"),
                    "policy_version": row.get("policy_version"),
                    "enqueued_at": row.get("enqueued_at"),
                    "fence": row.get("fence"),
                }
            if policy is not None and state == "PENDING":
                item["effective_priority"] = policy.effective_priority(
                    item["priority"],
                    policy_version=(
                        item["policy_version"] or STATIC_ADMISSION_POLICY_VERSION
                    ),
                    enqueued_at=item["enqueued_at"],
                    observed_at=observed_at,
                )
            rows.append(item)
    rows.sort(key=lambda row: (row["state"], row["orbit_id"] or ""))
    snapshot = {"coordinator_epoch": coordinator_epoch, "orbits": rows}
    if policy is not None:
        if (
            not isinstance(observed_at, (int, float))
            or isinstance(observed_at, bool)
            or not math.isfinite(observed_at)
        ):
            raise ValueError("observed_at must be finite when policy is supplied")
        snapshot.update(
            observed_at=observed_at,
            queue_policy_version=policy.version,
        )
    return sha256_json(snapshot)
