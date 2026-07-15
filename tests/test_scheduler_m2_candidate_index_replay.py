"""Fail-closed replay/envelope regressions for the M2a optimization seam."""

from __future__ import annotations

import json

import pytest

from omd_server.admission import (
    STATIC_ADMISSION_POLICY_VERSION,
    AdmissionRequest,
    decide_admission,
)


def _row(pathspec):
    return {
        "orbit_id": "authority",
        "agent_id": "authority",
        "mode": "write",
        "pathspec": pathspec,
        "priority": 0,
        "queue_seq": 0,
        "policy_version": STATIC_ADMISSION_POLICY_VERSION,
        "enqueued_at": 0.0,
        "fence": None,
    }


class OnePassIterable:
    def __init__(self, values):
        self._iterator = iter(values)

    def __iter__(self):
        return self._iterator


class OnePassList(list):
    def __init__(self, values):
        super().__init__(values)
        self._iterator = super().__iter__()

    def __iter__(self):
        return self._iterator


@pytest.mark.parametrize(
    "pathspec",
    [
        pytest.param(iter(["src/**"]), id="iterator"),
        pytest.param(OnePassIterable(["src/**"]), id="iterator-wrapper"),
        pytest.param(OnePassList(["src/**"]), id="list-subclass"),
    ],
)
def test_prefilter_falls_back_before_consuming_one_pass_authority(pathspec):
    request = AdmissionRequest.build(["src/a.py"], "write", 0, 1)
    decision = decide_admission(request, [_row(pathspec)], [])
    assert decision.held_blockers == ("authority",)
    assert decision.candidate_scan.full_scan_fallback is True
    assert decision.candidate_scan.fallback_reason == "unindexable_snapshot"


@pytest.mark.parametrize("candidate_index_enabled", [True, False])
@pytest.mark.parametrize("missing", [False, True], ids=["invalid", "missing"])
def test_disjoint_invalid_mode_has_the_same_fail_closed_result(
    candidate_index_enabled,
    missing,
):
    row = _row(json.dumps(["docs/**"]))
    if missing:
        del row["mode"]
        error = KeyError
    else:
        row["mode"] = "CORRUPT"
        error = ValueError
    request = AdmissionRequest.build(["src/a.py"], "write", 0, 1)
    with pytest.raises(error):
        decide_admission(
            request,
            [row],
            [],
            candidate_index_enabled=candidate_index_enabled,
        )
