#!/usr/bin/env python3
"""Produce the locked M0 OOPTDD positive/negative/restored receipt."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from benchmarks.omd_scheduler_m0 import REPO_ROOT, run_ooptdd


DEFAULT_GATE = REPO_ROOT / "evidence" / "omd_scheduler_m0" / "m0_measurement_gate.yaml"
DEFAULT_CID = "omd-scheduler-m0-ooptdd-fresh"


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _one(gate: dict[str, Any], cid: str, *, drop: bool) -> dict[str, Any]:
    from ooptdd.backends import MemoryBackend, memory as memory_backend
    from ooptdd.gate import evaluate, evidence_tier

    memory_backend.reset()
    backend = MemoryBackend(drop=drop)
    observation = run_ooptdd(backend, cid)
    result = evaluate(backend, gate)
    return {
        "drop_required_events": drop,
        "observation": observation,
        "gate_result": result,
        "evidence_tier": evidence_tier(result),
    }


def produce(gate_path: Path, cid: str) -> dict[str, Any]:
    from ooptdd.gate import load_gate

    gate = load_gate(str(gate_path))
    positive = _one(gate, cid, drop=False)
    negative = _one(gate, cid, drop=True)
    restored = _one(gate, cid, drop=False)
    if not positive["gate_result"]["ok"]:
        raise RuntimeError(f"positive gate did not turn green: {positive['gate_result']}")
    if negative["gate_result"]["ok"]:
        raise RuntimeError("injected silent drop did not turn the same gate red")
    if not restored["gate_result"]["ok"]:
        raise RuntimeError("positive gate did not recover after removing the injection")
    return {
        "schema": "omd-scheduler-m0-ooptdd-run/v1",
        "cid": cid,
        "gate": {
            "path": str(gate_path.relative_to(REPO_ROOT)),
            "sha256": _sha256(gate_path),
        },
        "producer": {
            "path": str(Path(__file__).resolve().relative_to(REPO_ROOT)),
            "sha256": _sha256(Path(__file__).resolve()),
            "entrypoint": "benchmarks.omd_scheduler_m0:run_ooptdd",
        },
        "started_after_gate_lock": True,
        "completed_at": datetime.now(timezone.utc).isoformat(),
        "positive": positive,
        "negative": negative,
        "restored_positive": restored,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--gate", default=str(DEFAULT_GATE))
    parser.add_argument("--cid", default=DEFAULT_CID)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()
    payload = produce(Path(args.gate).resolve(), args.cid)
    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    temporary = output.with_suffix(output.suffix + ".tmp")
    temporary.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    os.replace(temporary, output)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
