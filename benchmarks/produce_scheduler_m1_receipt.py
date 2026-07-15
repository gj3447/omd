#!/usr/bin/env python3
"""Produce frozen-gate M1 positive/injected-negative/restored evidence."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import shlex
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from benchmarks.omd_scheduler_m0 import REPO_ROOT
from benchmarks.omd_scheduler_m1 import DEFAULT_CID, run_ooptdd


DEFAULT_GATE = REPO_ROOT / "gates" / "scheduler_fairness.yaml"
DEFAULT_SPEC = REPO_ROOT / "spec" / "omd_scheduler_m1_ooptdd.yaml"
ENTRYPOINT = REPO_ROOT / "benchmarks" / "omd_scheduler_m1.py"
FROZEN_GATE_SHA256 = "7e249d738e941c2a56e6d8846ddc2d5b6489c95a0238d5471301c63bea19c4d1"


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _git_head() -> str | None:
    result = subprocess.run(
        ["git", "rev-parse", "HEAD"],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
    )
    return result.stdout.strip() if result.returncode == 0 else None


def _one(gate: dict[str, Any], cid: str, *, bypass: bool) -> dict[str, Any]:
    from ooptdd.backends import MemoryBackend, memory as memory_backend
    from ooptdd.gate import evaluate, evidence_tier

    memory_backend.reset()
    backend = MemoryBackend()
    observation = run_ooptdd(
        backend, cid, inject_pending_bypass=bypass
    )
    result = evaluate(backend, gate)
    return {
        "pending_predecessor_bypass_injected": bypass,
        "observation": observation,
        "gate_result": result,
        "evidence_tier": evidence_tier(result),
    }


def _assert_observation(
    label: str, run: dict[str, Any], *, expected_newer: str, expected_pass: bool
) -> None:
    observation = run["observation"]
    expected = {
        "newer_claim_state": expected_newer,
        "no_overtaking_passed": expected_pass,
        "store_readback_complete": True,
    }
    mismatches = {
        key: {"expected": value, "actual": observation.get(key)}
        for key, value in expected.items()
        if observation.get(key) != value
    }
    if observation.get("older_waiter_state") != "PENDING":
        mismatches["older_waiter_state"] = {
            "expected": "PENDING",
            "actual": observation.get("older_waiter_state"),
        }
    if mismatches:
        raise RuntimeError(f"{label} store readback mismatch: {mismatches}")


def produce(
    gate_path: Path, cid: str, *, spec_path: Path = DEFAULT_SPEC
) -> dict[str, Any]:
    from ooptdd.gate import load_gate

    gate_sha256 = _sha256(gate_path)
    if gate_sha256 != FROZEN_GATE_SHA256:
        raise RuntimeError(
            f"frozen gate drift: expected {FROZEN_GATE_SHA256}, got {gate_sha256}"
        )
    # Hash both specifications before the first positive run.
    spec_sha256 = _sha256(spec_path)
    gate = load_gate(str(gate_path))
    positive = _one(gate, cid, bypass=False)
    negative = _one(gate, cid, bypass=True)
    restored = _one(gate, cid, bypass=False)
    if not positive["gate_result"]["ok"]:
        raise RuntimeError(f"M1 positive gate did not turn green: {positive['gate_result']}")
    if negative["gate_result"]["ok"]:
        raise RuntimeError("pending-predecessor bypass did not turn the frozen gate red")
    if not restored["gate_result"]["ok"]:
        raise RuntimeError("M1 gate did not recover after removing the injection")
    _assert_observation(
        "positive", positive, expected_newer="PENDING", expected_pass=True
    )
    _assert_observation(
        "negative", negative, expected_newer="HELD", expected_pass=False
    )
    _assert_observation(
        "restored", restored, expected_newer="PENDING", expected_pass=True
    )

    producer = Path(__file__).resolve()
    return {
        "schema": "omd-scheduler-m1-ooptdd-run/v1",
        "cid": cid,
        "gate": {
            "path": str(gate_path.relative_to(REPO_ROOT)),
            "sha256": gate_sha256,
        },
        "spec": {
            "path": str(spec_path.relative_to(REPO_ROOT)),
            "sha256": spec_sha256,
            "locked_before_positive_run": True,
        },
        "subject": {
            "git_head_before_evidence_commit": _git_head(),
            "admission_path": "omd_server/admission.py",
            "admission_sha256": _sha256(REPO_ROOT / "omd_server" / "admission.py"),
            "core_path": "omd_server/core.py",
            "core_sha256": _sha256(REPO_ROOT / "omd_server" / "core.py"),
        },
        "producer": {
            "path": str(producer.relative_to(REPO_ROOT)),
            "sha256": _sha256(producer),
            "command": " ".join((sys.executable, str(producer))),
            "cwd": str(REPO_ROOT),
            "entrypoint": "benchmarks.produce_scheduler_m1_receipt:produce",
            "source_symbol": "produce",
            "scenario_entrypoint": "benchmarks.omd_scheduler_m1:run_ooptdd",
            "entrypoint_path": str(ENTRYPOINT.relative_to(REPO_ROOT)),
            "entrypoint_sha256": _sha256(ENTRYPOINT),
            "git_head": _git_head(),
            "real_code_path": True,
            "exit_code": 0,
        },
        "completed_at": datetime.now(timezone.utc).isoformat(),
        "positive": positive,
        "negative": negative,
        "restored_positive": restored,
    }


def _write_json_atomic(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    os.replace(temporary, path)


def build_receipt(run: dict[str, Any], run_path: Path) -> dict[str, Any]:
    """Bind the materialized run without supplying an independent judgment."""
    run_path = run_path.resolve()
    try:
        displayed_run_path = str(run_path.relative_to(REPO_ROOT))
    except ValueError:
        displayed_run_path = str(run_path)
    run_sha256 = _sha256(run_path)
    return {
        "schema_version": "symposium-ooptdd-receipt/v1",
        "template_only": False,
        "receipt_id": "omd-scheduler-m1-fair-admission",
        "cycle_id": "omd-scheduler-m1-fair-admission-20260715",
        "requirement_group": "OMD-SCHEDULER-M1-FAIR-ADMISSION",
        "spec": run["spec"],
        "producer": {
            "command": run["producer"]["command"],
            "cwd": run["producer"]["cwd"],
            "entrypoint": run["producer"]["entrypoint"],
            "source_path": run["producer"]["path"],
            "source_symbol": run["producer"]["source_symbol"],
            "git_head": run["producer"]["git_head"],
            "real_code_path": run["producer"]["real_code_path"],
            "exit_code": run["producer"]["exit_code"],
        },
        "correlation": {"cid": run["cid"]},
        "requirements": [
            {
                "id": "M1-OVERTAKING-DEFECT",
                "role": "guard_defect",
                "event": "orbit_granted",
            },
            {
                "id": "M1-PENDING-PREDECESSOR-GUARD",
                "role": "guard_mechanism",
                "event": "orbit_pending",
            },
        ],
        "positive": {
            "observed_verdict": "green",
            "receipt_path": displayed_run_path,
            "receipt_sha256": run_sha256,
            "evidence_tier": run["positive"]["evidence_tier"],
            "charge_ratio": run["positive"]["gate_result"]["scope"][
                "charge_ratio"
            ],
            "forbidden_events_passed": True,
        },
        "negative_oracle": {
            "spec_sha256": run["spec"]["sha256"],
            "technique": "fault_injection",
            "injection": "Ignore conflicting PENDING predecessors in decide_admission.",
            "observed_verdict": "red",
            "receipt_path": displayed_run_path,
            "receipt_sha256": run_sha256,
            "restored": run["restored_positive"]["gate_result"]["ok"],
        },
        "oracle": {
            "emit_identity": "MemoryBackend",
            "read_identity": "MemoryBackend",
            "separate_source": False,
            "corroborated": False,
        },
        "source_binding": {
            "path": run["producer"]["path"],
            "symbol": run["producer"]["source_symbol"],
            "sha256": run["producer"]["sha256"],
        },
        "subject_binding": run["subject"],
        "judgment": {
            "status": "AWAITING_INDEPENDENT_JUDGE",
            "supplied_by_producer": False,
        },
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--gate", default=str(DEFAULT_GATE))
    parser.add_argument("--spec", default=str(DEFAULT_SPEC))
    parser.add_argument("--cid", default=DEFAULT_CID)
    parser.add_argument("--output", required=True)
    parser.add_argument("--receipt-output")
    args = parser.parse_args()
    payload = produce(
        Path(args.gate).resolve(),
        args.cid,
        spec_path=Path(args.spec).resolve(),
    )
    payload["producer"]["command"] = shlex.join(
        [sys.executable, str(Path(__file__).resolve()), *sys.argv[1:]]
    )
    output = Path(args.output).resolve()
    _write_json_atomic(output, payload)
    if args.receipt_output:
        _write_json_atomic(Path(args.receipt_output), build_receipt(payload, output))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
