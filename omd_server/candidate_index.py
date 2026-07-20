"""Derived, transaction-local conflict candidate index.

The index is not authority.  It is rebuilt from the complete HELD/PENDING
snapshot and may only remove rows that cannot overlap under the exact glob
algebra.  Admission still exact-checks every returned row.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from typing import Any, Iterable, Mapping, Sequence

from .disjoint import glob_segments


CANDIDATE_INDEX_VERSION = "omd-admission/literal-prefix-candidates-v1"
ADMISSION_MODES = frozenset({"read", "write", "shared"})
_WILDCARD = re.compile(r"[*?\[]")
_HELD = "HELD"
_PENDING = "PENDING"
_Key = tuple[str, int]


class CandidateIndexInputError(ValueError):
    """The snapshot cannot be indexed without guessing its glob semantics."""


@dataclass(frozen=True)
class CandidateScan:
    """Bounded observability for one candidate query."""

    version: str
    active_held: int
    active_pending: int
    candidate_held: int
    candidate_pending: int
    full_scan_fallback: bool
    fallback_reason: str | None = None

    def as_dict(self) -> dict[str, Any]:
        return {
            "candidate_index_version": self.version,
            "active_held": self.active_held,
            "active_pending": self.active_pending,
            "candidate_held": self.candidate_held,
            "candidate_pending": self.candidate_pending,
            "full_scan_fallback": self.full_scan_fallback,
            "fallback_reason": self.fallback_reason,
        }


@dataclass(frozen=True)
class CandidateSelection:
    held: tuple[Mapping[str, Any], ...]
    pending: tuple[Mapping[str, Any], ...]
    scan: CandidateScan


@dataclass
class _TrieNode:
    rows: set[_Key] = field(default_factory=set)
    children: dict[str, "_TrieNode"] = field(default_factory=dict)


def _pathspec(value: Any) -> tuple[str, ...]:
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except (TypeError, ValueError) as exc:
            raise CandidateIndexInputError("malformed pathspec JSON") from exc
    if type(value) not in (list, tuple):
        raise CandidateIndexInputError("pathspec must be a sequence of strings")
    # The exact oracle receives the original authority row after this derived
    # prefilter. Index only the concrete JSON/list and normalized-tuple forms
    # used by production; an arbitrary iterable can hide a one-shot iterator
    # and would make the later exact check observe a different pathspec.
    paths = tuple(value)
    if not paths or not all(type(path) is str and path for path in paths):
        raise CandidateIndexInputError(
            "pathspec must contain at least one non-empty string"
        )
    return paths


def literal_glob_prefix(glob: str) -> tuple[str, ...] | None:
    """Maximal literal segment prefix, or ``None`` for a global pattern.

    A wildcard in a later segment preserves the preceding literal prefix.
    A wildcard in the first segment (including ``**`` and character classes)
    can overlap any root and is therefore globally resident.
    """
    segments = glob_segments(glob)
    if not segments or any(segment == "" for segment in segments):
        raise CandidateIndexInputError("glob contains an empty segment")
    prefix = []
    for segment in segments:
        if segment == "**" or _WILDCARD.search(segment):
            break
        prefix.append(segment)
    return tuple(prefix) or None


class ConflictCandidateIndex:
    """Immutable-after-build prefix trie over one complete active snapshot."""

    def __init__(
        self,
        held: Iterable[Mapping[str, Any]],
        pending: Iterable[Mapping[str, Any]],
    ):
        self.held = tuple(held)
        self.pending = tuple(pending)
        self._root = _TrieNode()
        self._global: set[_Key] = set()
        self._covered: set[_Key] = set()
        for state, rows in ((_HELD, self.held), (_PENDING, self.pending)):
            for index, row in enumerate(rows):
                key = (state, index)
                try:
                    mode = row["mode"]
                    if mode not in ADMISSION_MODES:
                        raise CandidateIndexInputError(
                            "active row has an unsupported mode"
                        )
                    paths = _pathspec(row["pathspec"])
                    prefixes = tuple(literal_glob_prefix(path) for path in paths)
                except (KeyError, TypeError, CandidateIndexInputError) as exc:
                    raise CandidateIndexInputError(
                        "active row has an unindexable pathspec"
                    ) from exc
                if any(prefix is None for prefix in prefixes):
                    self._global.add(key)
                else:
                    for prefix in set(prefixes):
                        node = self._root
                        for segment in prefix:
                            node = node.children.setdefault(segment, _TrieNode())
                        node.rows.add(key)
                self._covered.add(key)
        expected = len(self.held) + len(self.pending)
        if len(self._covered) != expected:
            raise CandidateIndexInputError("candidate index coverage mismatch")

    @staticmethod
    def _descendants(node: _TrieNode, selected: set[_Key]) -> None:
        selected.update(node.rows)
        for child in node.children.values():
            ConflictCandidateIndex._descendants(child, selected)

    def _comparable_rows(self, prefix: tuple[str, ...]) -> set[_Key]:
        """Rows whose literal prefix is an ancestor or descendant of prefix."""
        selected: set[_Key] = set()
        node = self._root
        selected.update(node.rows)
        for segment in prefix:
            node = node.children.get(segment)
            if node is None:
                return selected
            selected.update(node.rows)
        self._descendants(node, selected)
        return selected

    def select(self, request_pathspec: Sequence[str]) -> CandidateSelection:
        paths = _pathspec(request_pathspec)
        prefixes = tuple(literal_glob_prefix(path) for path in paths)
        if any(prefix is None for prefix in prefixes):
            return self.full_selection("request_unknown_prefix")
        selected = set(self._global)
        for prefix in set(prefixes):
            selected.update(self._comparable_rows(prefix))
        held = tuple(
            row for index, row in enumerate(self.held) if (_HELD, index) in selected
        )
        pending = tuple(
            row
            for index, row in enumerate(self.pending)
            if (_PENDING, index) in selected
        )
        return CandidateSelection(
            held,
            pending,
            CandidateScan(
                version=CANDIDATE_INDEX_VERSION,
                active_held=len(self.held),
                active_pending=len(self.pending),
                candidate_held=len(held),
                candidate_pending=len(pending),
                full_scan_fallback=False,
            ),
        )

    def full_selection(self, reason: str) -> CandidateSelection:
        return CandidateSelection(
            self.held,
            self.pending,
            CandidateScan(
                version=CANDIDATE_INDEX_VERSION,
                active_held=len(self.held),
                active_pending=len(self.pending),
                candidate_held=len(self.held),
                candidate_pending=len(self.pending),
                full_scan_fallback=True,
                fallback_reason=reason,
            ),
        )


def select_conflict_candidates(
    request_pathspec: Sequence[str],
    held: Iterable[Mapping[str, Any]],
    pending: Iterable[Mapping[str, Any]],
    *,
    enabled: bool = True,
) -> CandidateSelection:
    """Build and query a safe derived index, falling back on any uncertainty."""
    held_rows = tuple(held)
    pending_rows = tuple(pending)

    def full(reason: str) -> CandidateSelection:
        return CandidateSelection(
            held_rows,
            pending_rows,
            CandidateScan(
                version=CANDIDATE_INDEX_VERSION,
                active_held=len(held_rows),
                active_pending=len(pending_rows),
                candidate_held=len(held_rows),
                candidate_pending=len(pending_rows),
                full_scan_fallback=True,
                fallback_reason=reason,
            ),
        )

    if not enabled:
        return full("index_disabled")
    try:
        return ConflictCandidateIndex(held_rows, pending_rows).select(request_pathspec)
    except CandidateIndexInputError:
        return full("unindexable_snapshot")
    except Exception:  # noqa: BLE001 - optimization failure must preserve authority
        return full("index_error")
