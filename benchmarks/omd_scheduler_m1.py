#!/usr/bin/env python3
"""M1 no-overtaking OOPTDD producer over the real Coordinator.

The negative control replaces only the pure decision function in-process.  It
is not a production setting and cannot be enabled through Coordinator, MCP, or
CLI configuration.
"""

from __future__ import annotations

from contextlib import contextmanager
from typing import Any, Iterator

from benchmarks.omd_scheduler_m0 import fairness_probe
from omd_server import core


DEFAULT_CID = "omd-scheduler-m1-newer"


@contextmanager
def injected_pending_predecessor_bypass(enabled: bool) -> Iterator[None]:
    original = core.decide_admission
    if not enabled:
        yield
        return

    def held_only(request, held, pending):
        del pending
        return original(request, held, ())

    core.decide_admission = held_only
    try:
        yield
    finally:
        core.decide_admission = original


def run_ooptdd(
    backend: Any,
    cid: str = DEFAULT_CID,
    *,
    inject_pending_bypass: bool = False,
) -> dict[str, Any]:
    with injected_pending_predecessor_bypass(inject_pending_bypass):
        observation = fairness_probe(cid=cid, backend=backend)
    return {
        **observation,
        "schema": "omd.m1.fair-admission-observation.v1",
        "pending_predecessor_bypass_injected": inject_pending_bypass,
    }
