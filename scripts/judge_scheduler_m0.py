#!/usr/bin/env python3
"""Independent, fail-closed LakatoTree consumer for OMD scheduler M0 evidence.

The implementer writes a verdict-free ``lakato-evidence-record/v1``.  This
wrapper verifies hashes and timing, recomputes both preregistered obligations
from raw episodes, optionally performs a fresh replay, and only then delegates
the numeric result to LakatoTree's deterministic ``judge_record`` kernel.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import subprocess
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


REPO_ROOT = Path(__file__).resolve().parents[1]
FORBIDDEN_EVIDENCE_KEYS = {
    "verdict",
    "verdict_source",
    "metric_verdict",
    "manual_verdict",
    "human_verdict",
}


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def load_json(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError(f"{path} must contain a JSON object")
    return value


def forbidden_key_paths(value: Any, prefix: str = "$") -> list[str]:
    found: list[str] = []
    if isinstance(value, dict):
        for key, child in value.items():
            child_path = f"{prefix}.{key}"
            if key in FORBIDDEN_EVIDENCE_KEYS:
                found.append(child_path)
            found.extend(forbidden_key_paths(child, child_path))
    elif isinstance(value, list):
        for index, child in enumerate(value):
            found.extend(forbidden_key_paths(child, f"{prefix}[{index}]"))
    return found


def _parse_time(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError("timestamps must be timezone-aware")
    return parsed


def _benchmark_from_evidence(evidence: dict[str, Any]) -> dict[str, Any]:
    return evidence["measurement"]["derived"]["performance_baseline"]["benchmark"]


def recompute_obligations(
    preregistration: dict[str, Any],
    benchmark: dict[str, Any],
) -> dict[str, Any]:
    expected_fairness = preregistration["descriptive_only"]["no_overtaking_pass_rate"]["episodes"]
    fairness_rows = [
        row for row in benchmark.get("episodes", []) if row.get("scenario") == "pending-overtake"
    ]
    fairness_observations = fairness_rows[0].get("observations", []) if len(fairness_rows) == 1 else []
    fairness_complete = (
        len(fairness_rows) == 1
        and fairness_rows[0].get("repeats") == expected_fairness
        and len(fairness_observations) == expected_fairness
        and all(
            observation.get("holder_state") == "HELD"
            and observation.get("older_waiter_state") == "PENDING"
            and observation.get("newer_claim_state") == "HELD"
            and observation.get("older_arrived_first") is True
            and observation.get("newer_granted_while_older_pending") is True
            and observation.get("no_overtaking_passed") is False
            and observation.get("store_readback_complete") is True
            for observation in fairness_observations
        )
    )

    contract = preregistration["benchmark_contract"]
    expected_workers = set(contract["workers"])
    expected_operations = contract["total_claims_per_episode"]
    expected_repeats = contract["measured_episodes"]
    claim_rows = [
        row for row in benchmark.get("episodes", []) if row.get("scenario") == "claim-scale"
    ]
    groups: dict[int, list[dict[str, Any]]] = {}
    for row in claim_rows:
        groups.setdefault(row.get("workers"), []).append(row)
    matrix_complete = set(groups) == expected_workers
    if matrix_complete:
        for workers in expected_workers:
            rows = groups[workers]
            if len(rows) != expected_repeats:
                matrix_complete = False
                break
            for row in rows:
                raw = row.get("raw_latency_ns") or []
                if (
                    row.get("requested") != expected_operations
                    or row.get("completed") != expected_operations
                    or len(raw) != expected_operations
                    or not all(isinstance(value, int) and value >= 0 for value in raw)
                    or not isinstance(row.get("throughput_ops_s"), (int, float))
                    or not math.isfinite(row["throughput_ops_s"])
                    or row["throughput_ops_s"] <= 0
                    or not row.get("invariants")
                    or not all(row["invariants"].values())
                    or row.get("errors")
                ):
                    matrix_complete = False
                    break
            if not matrix_complete:
                break

    obligations_met = int(fairness_complete) + int(matrix_complete)
    return {
        "fairness_replication_complete": fairness_complete,
        "claim_matrix_complete": matrix_complete,
        "m0_replay_obligations_met": obligations_met,
        "required": preregistration["prediction"]["required_value"],
        "fairness_observations": len(fairness_observations),
        "claim_cells": {str(key): len(value) for key, value in sorted(groups.items())},
    }


def validate_inputs(
    prereg_path: Path,
    evidence_path: Path,
    preregistration: dict[str, Any],
    evidence: dict[str, Any],
) -> list[str]:
    errors: list[str] = []
    if preregistration.get("schema_version") != "omd-scheduler-preregistration/v2":
        errors.append("unsupported preregistration schema")
    if preregistration.get("study_kind") != "preregistered_replication_of_disclosed_exploratory_observation":
        errors.append("M0 study kind must disclose prior exploratory observation")
    if preregistration.get("registered_before_measurement") is not True:
        errors.append("preregistration is not marked before measurement")
    forbidden = forbidden_key_paths(evidence)
    if forbidden:
        errors.append(f"evidence contains forbidden verdict keys: {forbidden}")

    prereg_sha = sha256(prereg_path)
    actual_request_sha = (evidence.get("preregistration") or {}).get("request_sha256")
    if actual_request_sha != prereg_sha:
        errors.append(f"preregistration SHA mismatch: {actual_request_sha} != {prereg_sha}")
    try:
        registered = _parse_time(preregistration["registered_at"])
        measured = _parse_time(evidence["measurement"]["started_at"])
        if registered >= measured:
            errors.append("registration did not precede fresh measurement")
    except (KeyError, TypeError, ValueError) as exc:
        errors.append(f"timestamp validation failed: {exc}")

    harness_path = REPO_ROOT / "benchmarks" / "omd_scheduler_m0.py"
    expected_harness_sha = preregistration.get("harness", {}).get("script_sha256")
    if expected_harness_sha != sha256(harness_path):
        errors.append("measurement harness differs from preregistered SHA")
    expected_judge_sha = preregistration.get("judge", {}).get("script_sha256")
    if expected_judge_sha != sha256(Path(__file__).resolve()):
        errors.append("judge wrapper differs from preregistered SHA")

    provenance = {
        item.get("name"): item
        for item in (evidence.get("provenance") or {}).get("inputs", [])
        if isinstance(item, dict)
    }
    if provenance.get("measurement_harness", {}).get("sha256") != expected_harness_sha:
        errors.append("evidence harness provenance does not match preregistration")
    return errors


def replay_benchmark(preregistration: dict[str, Any]) -> tuple[dict[str, Any], dict[str, Any]]:
    contract = preregistration["benchmark_contract"]
    with tempfile.TemporaryDirectory(prefix="omd-m0-judge-replay-") as tmp:
        output = Path(tmp) / "replay.json"
        command = [
            sys.executable,
            str(REPO_ROOT / "benchmarks" / "omd_scheduler_m0.py"),
            "--scenarios",
            "fairness,claims",
            "--workers",
            ",".join(str(value) for value in contract["workers"]),
            "--claim-operations",
            str(contract["total_claims_per_episode"]),
            "--fairness-repeats",
            str(preregistration["descriptive_only"]["no_overtaking_pass_rate"]["episodes"]),
            "--warmups",
            str(contract["warmup_episodes"]),
            "--repeats",
            str(contract["measured_episodes"]),
            "--output",
            str(output),
        ]
        result = subprocess.run(
            command,
            cwd=REPO_ROOT,
            capture_output=True,
            text=True,
            check=False,
        )
        execution = {
            "command": command,
            "exit_code": result.returncode,
            "stdout_sha256": hashlib.sha256(result.stdout.encode()).hexdigest(),
            "stderr_sha256": hashlib.sha256(result.stderr.encode()).hexdigest(),
        }
        if result.returncode != 0 or not output.exists():
            raise RuntimeError(
                f"benchmark replay failed with {result.returncode}: {result.stderr[-1000:]}"
            )
        return load_json(output), execution


def _write_output(payload: dict[str, Any], destination: Path) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary = destination.with_suffix(destination.suffix + ".tmp")
    temporary.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    os.replace(temporary, destination)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--prereg", required=True)
    parser.add_argument("--evidence", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--judge-id", default="progress_judge.omd_scheduler_m0")
    parser.add_argument("--implementer-id", default="codex-omd-scheduler-m0-20260715-root")
    parser.add_argument("--replay", action="store_true")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    prereg_path = Path(args.prereg).resolve()
    evidence_path = Path(args.evidence).resolve()
    output_path = Path(args.output).resolve()
    preregistration = load_json(prereg_path)
    evidence = load_json(evidence_path)
    errors = validate_inputs(prereg_path, evidence_path, preregistration, evidence)

    try:
        from lakatos.programme.evidence import is_grounded, validate_record
        from lakatos.programme.record_judge import judge_record
    except ImportError as exc:
        errors.append(f"LakatoTree import failed; set PYTHONPATH to its repository: {exc}")
        is_grounded = validate_record = judge_record = None

    if validate_record is not None:
        errors.extend(validate_record(evidence))
        if not is_grounded(evidence):
            errors.append("evidence is not grounded")

    recomputed = recompute_obligations(preregistration, _benchmark_from_evidence(evidence))
    recorded_value = evidence.get("measurement", {}).get("value")
    if recorded_value != recomputed["m0_replay_obligations_met"]:
        errors.append(
            f"evidence value {recorded_value} != recomputed {recomputed['m0_replay_obligations_met']}"
        )

    replay = None
    if args.replay and not errors:
        try:
            replay_bench, replay_execution = replay_benchmark(preregistration)
            replay_result = recompute_obligations(preregistration, replay_bench)
            replay = {"execution": replay_execution, "recomputed": replay_result}
            if replay_result["m0_replay_obligations_met"] != preregistration["prediction"]["required_value"]:
                errors.append("fresh independent replay did not meet both M0 obligations")
        except Exception as exc:  # noqa: BLE001 - judge must fail closed
            errors.append(f"replay failed: {type(exc).__name__}: {exc}")

    response: dict[str, Any] = {
        "schema": "omd-scheduler-m0-judge-response/v1",
        "programme": preregistration.get("programme"),
        "branch": preregistration.get("branch"),
        "roles": {"implementer": args.implementer_id, "judge": args.judge_id},
        "inputs": {
            "preregistration_path": str(prereg_path),
            "preregistration_sha256": sha256(prereg_path),
            "evidence_path": str(evidence_path),
            "evidence_sha256": sha256(evidence_path),
            "judge_script_sha256": sha256(Path(__file__).resolve()),
        },
        "recomputed": recomputed,
        "replay": replay,
        "executed_at": datetime.now(timezone.utc).isoformat(),
    }

    required = preregistration["prediction"]["required_value"]
    if errors:
        response.update({"status": "invalid", "errors": errors})
        exit_code = 2
    elif recomputed["m0_replay_obligations_met"] != required:
        response.update(
            {
                "status": "abstain",
                "reason": f"M0 obligations {recomputed['m0_replay_obligations_met']} != {required}",
            }
        )
        exit_code = 3
    else:
        engine_result = judge_record(evidence)
        if engine_result.get("status") != "judged":
            response.update(
                {"status": "abstain", "reason": "LakatoTree engine abstained", "engine": engine_result}
            )
            exit_code = 3
        else:
            response.update({"status": "judged", "engine": engine_result})
            exit_code = 0

    _write_output(response, output_path)
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
