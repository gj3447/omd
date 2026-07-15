"""M2a conservative candidate-index soundness and differential contracts."""

from __future__ import annotations

import json

import pytest

hypothesis = pytest.importorskip("hypothesis")
from hypothesis import given, settings, strategies as st

from omd_server import Coordinator, Emitter, admission
from omd_server.admission import (
    STATIC_ADMISSION_POLICY_VERSION,
    AdmissionRequest,
    decide_admission,
    exact_conflict,
)
from omd_server.candidate_index import (
    CANDIDATE_INDEX_VERSION,
    literal_glob_prefix,
    select_conflict_candidates,
)
from omd_server.disjoint import sets_overlap


MODES = ("read", "write", "shared")


def _row(
    orbit_id: str,
    pathspec: list[str],
    *,
    mode: str = "write",
    priority: int = 0,
    queue_seq: int = 0,
):
    return {
        "orbit_id": orbit_id,
        "agent_id": orbit_id,
        "mode": mode,
        "pathspec": json.dumps(pathspec),
        "priority": priority,
        "queue_seq": queue_seq,
        "policy_version": STATIC_ADMISSION_POLICY_VERSION,
        "enqueued_at": 0.0,
        "fence": None,
    }


def _semantic(decision):
    return (
        decision.outcome,
        decision.held_blockers,
        decision.pending_predecessors,
        decision.base_priority,
        decision.effective_priority,
        decision.observed_at,
    )


def test_literal_prefix_uses_the_exact_oracle_normalization():
    assert literal_glob_prefix("./src/auth/*.py") == ("src", "auth")
    assert literal_glob_prefix("src/auth/") == ("src", "auth")
    assert literal_glob_prefix("src/a.py") == ("src", "a.py")
    assert literal_glob_prefix("**/a.py") is None
    assert literal_glob_prefix("[ab]/a.py") is None


def test_prefix_trie_keeps_ancestors_descendants_and_global_rows():
    held = [
        _row("exact", ["src/auth/a.py"]),
        _row("ancestor", ["src/**"]),
        _row("sibling", ["src/ui/a.py"]),
        _row("other-root", ["docs/**"]),
        _row("global", ["**/settings.py"]),
    ]
    selection = select_conflict_candidates(["src/auth/a.py"], held, [])
    assert {row["orbit_id"] for row in selection.held} == {
        "exact",
        "ancestor",
        "global",
    }
    assert selection.scan.version == CANDIDATE_INDEX_VERSION
    assert selection.scan.active_held == 5
    assert selection.scan.candidate_held == 3
    assert selection.scan.full_scan_fallback is False


def test_unknown_request_and_invalid_snapshot_fail_back_to_full_scan():
    rows = [_row("a", ["src/a.py"]), _row("b", ["docs/b.py"])]
    unknown = select_conflict_candidates(["**/a.py"], rows, [])
    assert unknown.held == tuple(rows)
    assert unknown.scan.full_scan_fallback is True
    assert unknown.scan.fallback_reason == "request_unknown_prefix"

    malformed = [dict(rows[0], pathspec="{not-json")]
    invalid = select_conflict_candidates(["src/a.py"], malformed, rows[1:])
    assert invalid.held == tuple(malformed)
    assert invalid.pending == tuple(rows[1:])
    assert invalid.scan.full_scan_fallback is True
    assert invalid.scan.fallback_reason == "unindexable_snapshot"


def test_disabled_index_is_an_explicit_full_scan_path():
    rows = [_row("a", ["src/a.py"]), _row("b", ["docs/b.py"])]
    request = AdmissionRequest.build(["src/a.py"], "write", 0, 2)
    decision = decide_admission(
        request, rows, [], candidate_index_enabled=False
    )
    assert decision.held_blockers == ("a",)
    assert decision.candidate_scan.full_scan_fallback is True
    assert decision.candidate_scan.fallback_reason == "index_disabled"
    assert decision.candidate_scan.candidate_held == 2


def test_one_shot_authority_pathspec_falls_back_without_consuming_it():
    row = _row("one-shot", ["src/**"])
    row["pathspec"] = iter(["src/**"])
    request = AdmissionRequest.build(["src/a.py"], "write", 0, 1)
    decision = decide_admission(request, [row], [])
    assert decision.held_blockers == ("one-shot",)
    assert decision.candidate_scan.full_scan_fallback is True
    assert decision.candidate_scan.fallback_reason == "unindexable_snapshot"


@pytest.mark.parametrize("mode", ["CORRUPT", None, ["write"]])
def test_invalid_disjoint_authority_mode_is_not_hidden_by_prefilter(mode):
    row = _row("corrupt", ["docs/**"])
    row["mode"] = mode
    request = AdmissionRequest.build(["src/a.py"], "write", 0, 1)
    with pytest.raises((TypeError, ValueError)):
        decide_admission(request, [row], [])


def test_missing_disjoint_authority_mode_is_not_hidden_by_prefilter():
    row = _row("corrupt", ["docs/**"])
    del row["mode"]
    request = AdmissionRequest.build(["src/a.py"], "write", 0, 1)
    with pytest.raises(KeyError):
        decide_admission(request, [row], [])


def test_prefilter_reduces_exact_checks_without_changing_the_oracle(monkeypatch):
    rows = [_row("blocker", ["src/**"])] + [
        _row(f"disjoint-{index}", [f"docs/area-{index}/**"])
        for index in range(40)
    ]
    request = AdmissionRequest.build(["src/a.py"], "write", 0, 41)
    original = admission.exact_conflict
    calls = []

    def counted(*args, **kwargs):
        calls.append(args[2]["orbit_id"])
        return original(*args, **kwargs)

    monkeypatch.setattr(admission, "exact_conflict", counted)
    indexed = decide_admission(request, rows, [])
    assert indexed.held_blockers == ("blocker",)
    assert calls == ["blocker"]
    assert indexed.candidate_scan.candidate_held == 1
    assert indexed.candidate_scan.active_held == 41

    calls.clear()
    full = decide_admission(request, rows, [], candidate_index_enabled=False)
    assert _semantic(indexed) == _semantic(full)
    assert len(calls) == 41


@pytest.mark.parametrize("request_mode", MODES)
@pytest.mark.parametrize("blocker_mode", MODES)
def test_all_mode_pairs_match_the_full_exact_decision(request_mode, blocker_mode):
    held = [_row("same-path", ["src/**"], mode=blocker_mode)]
    request = AdmissionRequest.build(["src/a.py"], request_mode, 0, 1)
    indexed = decide_admission(request, held, [])
    full = decide_admission(request, held, [], candidate_index_enabled=False)
    assert _semantic(indexed) == _semantic(full)


def test_coordinator_emits_bounded_candidate_scan_observation(tmp_path):
    class Collector:
        def __init__(self):
            self.events = []

        def ship(self, events):
            self.events.extend(events)

    collector = Collector()
    omd = Coordinator(
        str(tmp_path / "omd.db"),
        agent_ttl=None,
        events=Emitter(collector),
    )
    result = omd.claim("agent", ["src/a.py"])
    scans = [
        event
        for event in collector.events
        if event["event"] == "admission_candidates_scanned"
    ]
    assert result["state"] == "HELD"
    assert len(scans) == 1
    assert scans[0] == {
        "cid": result["orbit_id"],
        "correlation_id": result["orbit_id"],
        "cycle_id": result["orbit_id"],
        "service": "omd",
        "event": "admission_candidates_scanned",
        "repository_id": omd.repository_id,
        "mode": "write",
        "candidate_index_version": CANDIDATE_INDEX_VERSION,
        "active_held": 0,
        "active_pending": 0,
        "candidate_held": 0,
        "candidate_pending": 0,
        "full_scan_fallback": False,
        "fallback_reason": None,
    }
    omd.close()


_SEGMENT = st.sampled_from(
    ("src", "docs", "auth", "ui", "a.py", "b.py", "*", "?", "**", "[ab]")
)


@st.composite
def _glob(draw):
    segments = draw(st.lists(_SEGMENT, min_size=1, max_size=3))
    value = "/".join(segments)
    if draw(st.booleans()):
        value = "./" + value
    if draw(st.booleans()) and not value.endswith("**"):
        value += "/"
    return value


_PATHSPEC = st.lists(_glob(), min_size=1, max_size=3, unique=True)
_ROW_SPEC = st.tuples(_PATHSPEC, st.sampled_from(MODES), st.integers(-3, 3))


@settings(max_examples=250, deadline=None)
@given(
    request_paths=_PATHSPEC,
    request_mode=st.sampled_from(MODES),
    rows=st.lists(_ROW_SPEC, min_size=0, max_size=10),
    split=st.integers(0, 10),
    request_priority=st.integers(-3, 3),
)
def test_indexed_candidates_are_a_superset_and_decisions_match_full_scan(
    request_paths,
    request_mode,
    rows,
    split,
    request_priority,
):
    materialized = [
        _row(
            f"orbit-{index}",
            pathspec,
            mode=mode,
            priority=priority,
            queue_seq=index,
        )
        for index, (pathspec, mode, priority) in enumerate(rows)
    ]
    cut = min(split, len(materialized))
    held = materialized[:cut]
    pending = materialized[cut:]
    selection = select_conflict_candidates(request_paths, held, pending)
    candidate_ids = {
        row["orbit_id"] for row in (*selection.held, *selection.pending)
    }
    exact_overlap_ids = {
        row["orbit_id"]
        for row in materialized
        if sets_overlap(request_paths, json.loads(row["pathspec"]))
    }
    assert exact_overlap_ids <= candidate_ids

    request = AdmissionRequest.build(
        request_paths,
        request_mode,
        request_priority,
        len(materialized) + 1,
    )
    indexed = decide_admission(request, held, pending, observed_at=17.0)
    full = decide_admission(
        request,
        held,
        pending,
        observed_at=17.0,
        candidate_index_enabled=False,
    )
    assert _semantic(indexed) == _semantic(full)
    assert {
        row["orbit_id"]
        for row in materialized
        if exact_conflict(request.pathspec, request.mode, row)
    } <= candidate_ids
