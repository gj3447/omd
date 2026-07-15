#!/usr/bin/env python3
"""Package an independently generated M0 judgment as immutable local receipts.

This command does not judge the evidence.  It accepts only a successful output
from ``scripts/judge_scheduler_m0.py``, verifies that the response binds the
frozen preregistration and evidence, and then mints a content-addressed
prediction -> scripted-judgment receipt chain with LakatoTree's canonical
encoding.  The independent ``c1verify`` implementation must re-derive the same
chain before a completion packet is written.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
import subprocess
from datetime import datetime
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


def parse_time(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError(f"timestamp lacks timezone: {value!r}")
    return parsed


def run_git(*args: str) -> str:
    result = subprocess.run(
        ["git", *args],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode != 0:
        raise RuntimeError(f"git {' '.join(args)} failed: {result.stderr.strip()}")
    return result.stdout.strip()


def blob_sha256(commit: str, path: str) -> str:
    result = subprocess.run(
        ["git", "show", f"{commit}:{path}"],
        cwd=REPO_ROOT,
        capture_output=True,
        check=False,
    )
    if result.returncode != 0:
        raise RuntimeError(f"cannot read {path} from {commit}: {result.stderr.decode().strip()}")
    return hashlib.sha256(result.stdout).hexdigest()


def forbidden_paths(value: Any, prefix: str = "$") -> list[str]:
    found: list[str] = []
    if isinstance(value, dict):
        for key, child in value.items():
            child_path = f"{prefix}.{key}"
            if key in FORBIDDEN_EVIDENCE_KEYS:
                found.append(child_path)
            found.extend(forbidden_paths(child, child_path))
    elif isinstance(value, list):
        for index, child in enumerate(value):
            found.extend(forbidden_paths(child, f"{prefix}[{index}]"))
    return found


def atomic_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    os.replace(temporary, path)


def atomic_copy(source: Path, destination: Path) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary = destination.with_suffix(destination.suffix + ".tmp")
    shutil.copyfile(source, temporary)
    os.replace(temporary, destination)


def relative(path: Path) -> str:
    return path.resolve().relative_to(REPO_ROOT).as_posix()


def validate_bindings(
    prereg_path: Path,
    evidence_path: Path,
    response_path: Path,
    source_commit: str,
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any], str]:
    prereg = load_json(prereg_path)
    evidence = load_json(evidence_path)
    response = load_json(response_path)
    errors: list[str] = []

    if response.get("status") != "judged":
        errors.append("judge response status is not judged")
    roles = response.get("roles") or {}
    if not roles.get("implementer") or not roles.get("judge"):
        errors.append("judge response roles are incomplete")
    elif roles["implementer"] == roles["judge"]:
        errors.append("implementer and judge identities are not independent")
    inputs = response.get("inputs") or {}
    if inputs.get("preregistration_sha256") != sha256(prereg_path):
        errors.append("judge response preregistration hash mismatch")
    if inputs.get("evidence_sha256") != sha256(evidence_path):
        errors.append("judge response evidence hash mismatch")
    if inputs.get("judge_script_sha256") != prereg.get("judge", {}).get("script_sha256"):
        errors.append("judge response script hash mismatch")

    evidence_forbidden = forbidden_paths(evidence)
    if evidence_forbidden:
        errors.append(f"evidence contains forbidden judgment keys: {evidence_forbidden}")
    request_sha = sha256(prereg_path)
    if evidence.get("preregistration", {}).get("request_sha256") != request_sha:
        errors.append("evidence does not bind the frozen preregistration")

    required = prereg.get("prediction", {}).get("required_value")
    recomputed = response.get("recomputed") or {}
    replay_recomputed = (response.get("replay") or {}).get("recomputed") or {}
    if recomputed.get("m0_replay_obligations_met") != required:
        errors.append("judge recomputation did not meet the preregistered obligation")
    if replay_recomputed.get("m0_replay_obligations_met") != required:
        errors.append("independent replay did not meet the preregistered obligation")
    if evidence.get("measurement", {}).get("value") != required:
        errors.append("evidence measurement differs from the preregistered required value")

    engine = response.get("engine") or {}
    if engine.get("status") != "judged" or not isinstance(engine.get("verdict"), str):
        errors.append("LakatoTree engine did not produce a scripted result")

    source_commit = run_git("rev-parse", f"{source_commit}^{{commit}}")
    if evidence.get("harness", {}).get("git_commit") != source_commit:
        errors.append("evidence source commit mismatch")
    commit_time = run_git("show", "-s", "--format=%cI", source_commit)
    registered_time = prereg.get("registered_at")
    measured_time = evidence.get("measurement", {}).get("started_at")
    try:
        if not (parse_time(registered_time) < parse_time(commit_time) < parse_time(measured_time)):
            errors.append("required time order registered < committed < measured is false")
    except (TypeError, ValueError) as exc:
        errors.append(f"timestamp validation failed: {exc}")

    frozen_paths = {
        relative(prereg_path): request_sha,
        prereg["harness"]["script_path"]: prereg["harness"]["script_sha256"],
        prereg["judge"]["script_path"]: prereg["judge"]["script_sha256"],
    }
    for path, expected in frozen_paths.items():
        if blob_sha256(source_commit, path) != expected:
            errors.append(f"frozen commit blob hash mismatch: {path}")

    if errors:
        raise ValueError("; ".join(errors))
    return prereg, evidence, response, commit_time


def package(args: argparse.Namespace) -> dict[str, Any]:
    prereg_path = Path(args.prereg).resolve()
    evidence_path = Path(args.evidence).resolve()
    response_source = Path(args.judge_response).resolve()
    failed_source = Path(args.failed_attempt).resolve() if args.failed_attempt else None
    output_dir = Path(args.output_dir).resolve()
    output_dir.relative_to(REPO_ROOT)

    prereg, evidence, response, commit_time = validate_bindings(
        prereg_path,
        evidence_path,
        response_source,
        args.source_commit,
    )

    from c1verify.receipts import check_chain_integrity
    from c1verify.receipts import prediction_content_sha as external_prediction_sha
    from c1verify.receipts import receipt_content_sha as external_verdict_sha
    from lakatos.engine_identity import current_engine_rule_sha
    from lakatos.verdicts import fold_receipt_chain, prediction_content_sha, receipt_content_sha

    prediction = prereg["prediction"]
    prediction_receipt: dict[str, Any] = {
        "receipt_kind": "prediction",
        "tree": prereg["programme"],
        "tag": prereg["branch"],
        "metric_name": prediction["metric"],
        "direction": prediction["direction"],
        "baseline_value": prediction["baseline"],
        "noise_band": prediction["noise_band"],
        "scale_type": prediction["scale_type"],
        "novel_prediction": None,
        "novel_metric": None,
        "novel_direction": None,
        "novel_threshold": None,
        "judge_script_sha": prereg["judge"]["script_sha256"],
        "closes_question": None,
        "credence": None,
        "baseline_lineage": args.source_commit,
        "registered_at": prereg["registered_at"],
        "prev_receipt_sha": None,
        "verdict": None,
        "verdict_source": None,
    }
    prediction_receipt["receipt_sha"] = prediction_content_sha(prediction_receipt)
    if external_prediction_sha(prediction_receipt) != prediction_receipt["receipt_sha"]:
        raise ValueError("external verifier disagrees on prediction receipt SHA")

    engine = response["engine"]
    judgment_receipt: dict[str, Any] = {
        "tree": prereg["programme"],
        "tag": prereg["branch"],
        "target_id": prereg["conjecture"],
        "verdict": engine["verdict"],
        "verdict_source": "scripted",
        "metric_name": engine["metric"],
        "metric_value": engine["measured"],
        "novel_confirmed": False,
        "lakatos_status": engine["status"],
        "judged_at": response["executed_at"],
        "judge_script_sha": response["inputs"]["judge_script_sha256"],
        "prev_receipt_sha": prediction_receipt["receipt_sha"],
        "measurement_grade": "server_regenerated",
        "engine_rule_sha": current_engine_rule_sha(),
    }
    judgment_receipt["receipt_sha"] = receipt_content_sha(judgment_receipt)
    if external_verdict_sha(judgment_receipt) != judgment_receipt["receipt_sha"]:
        raise ValueError("external verifier disagrees on judgment receipt SHA")

    chain = [prediction_receipt, judgment_receipt]
    head = judgment_receipt["receipt_sha"]
    folded_engine = fold_receipt_chain(chain, head)
    folded_external, external_error = check_chain_integrity(chain, head)
    if external_error or folded_external != folded_engine:
        raise ValueError(f"receipt chain verification failed: {external_error}")
    if folded_engine.get("verdict") != engine["verdict"]:
        raise ValueError("receipt result differs from independent judge response")
    if folded_engine.get("verdict_source") != "scripted":
        raise ValueError("receipt source is not scripted")

    judge_path = output_dir / "judge-response.json"
    atomic_copy(response_source, judge_path)
    failed_path = None
    if failed_source is not None:
        failed_payload = load_json(failed_source)
        if failed_payload.get("status") != "invalid":
            raise ValueError("failed attempt artifact is not fail-closed invalid")
        failed_path = output_dir / "judge-attempt-1-invalid.json"
        atomic_copy(failed_source, failed_path)

    prereg_response_path = output_dir / "preregistration-response.json"
    prereg_response = {
        "schema": "omd-scheduler-m0-preregistration-response/v1",
        "status": "registered",
        "request_path": relative(prereg_path),
        "request_sha256": sha256(prereg_path),
        "source_commit": args.source_commit,
        "source_commit_time": commit_time,
        "measurement_started_at": evidence["measurement"]["started_at"],
        "registered_before_measurement": True,
        "prediction_receipt": prediction_receipt,
        "prediction_receipt_sha256": prediction_receipt["receipt_sha"],
    }
    atomic_json(prereg_response_path, prereg_response)

    chain_path = output_dir / "receipt-chain.json"
    chain_payload = {
        "schema": "omd-scheduler-m0-receipt-chain/v1",
        "head_receipt_sha256": head,
        "receipts": chain,
    }
    atomic_json(chain_path, chain_payload)

    verify_path = output_dir / "verify-output.json"
    verify_payload = {
        "schema": "omd-scheduler-m0-receipt-verification/v1",
        "ok": True,
        "from_receipt": True,
        "scripted_source_confirmed": True,
        "engine_and_external_fold_equal": folded_external == folded_engine,
        "engine_response_sha256": sha256(judge_path),
        "failed_attempt_path": relative(failed_path) if failed_path else None,
        "failed_attempt_sha256": sha256(failed_path) if failed_path else None,
        "head_receipt_sha256": head,
        "prediction_receipt_sha256": prediction_receipt["receipt_sha"],
        "receipt_count": len(chain),
    }
    atomic_json(verify_path, verify_payload)

    packet_path = output_dir / "judgment-packet.json"
    packet = {
        "schema_version": "symposium-lakatotree-judgment/v1",
        "template_only": False,
        "programme": prereg["programme"],
        "branch": prereg["branch"],
        "conjecture": prereg["conjecture"],
        "roles": response["roles"],
        "preregistration": {
            "request_path": relative(prereg_path),
            "request_sha256": sha256(prereg_path),
            "response_path": relative(prereg_response_path),
            "response_sha256": sha256(prereg_response_path),
            "prediction_receipt_sha256": prediction_receipt["receipt_sha"],
            "registered_at": prereg["registered_at"],
            "registered_before_measurement": True,
            "prediction": {
                "metric": prediction["metric"],
                "direction": prediction["direction"],
                "baseline": prediction["baseline"],
                "noise_band": prediction["noise_band"],
                "scale_type": prediction["scale_type"],
            },
            "kill_condition": " | ".join(prereg["kill_conditions"]),
            "judge_script_path": prereg["judge"]["script_path"],
            "judge_script_sha256": prereg["judge"]["script_sha256"],
        },
        "measurement": {
            "started_at": evidence["measurement"]["started_at"],
            "evidence_records": [
                {
                    "path": relative(evidence_path),
                    "sha256": sha256(evidence_path),
                    "schema": "lakato-evidence-record/v1",
                    "grounded": True,
                    "contains_verdict": False,
                }
            ],
        },
        "judge": {
            "command": (
                "PYTHONPATH=/Users/lagyeongjun/CD/SYMPOSIUM/PI/lakatotree "
                ".venv/bin/python scripts/judge_scheduler_m0.py "
                "--prereg evidence/omd_scheduler_m0/preregistration.json "
                "--evidence evidence/omd_scheduler_m0/evidence.json --replay "
                "--judge-id progress_judge.omd_scheduler_m0 "
                "--implementer-id codex-omd-scheduler-m0-20260715-root "
                "--output /tmp/omd_scheduler_m0_judge-response-attempt2.json"
            ),
            "cwd": str(REPO_ROOT),
            "git_head": args.source_commit,
            "entrypoint": "scripts.judge_scheduler_m0:main",
            "exit_code": 0,
            "response_path": relative(judge_path),
            "response_sha256": sha256(judge_path),
            "verdict_receipt_sha256": head,
            "prev_receipt_sha256": prediction_receipt["receipt_sha"],
        },
        "verification": {
            "receipt_chain_path": relative(chain_path),
            "receipt_chain_sha256": sha256(chain_path),
            "verify_output_path": relative(verify_path),
            "verify_output_sha256": sha256(verify_path),
            "head_receipt_sha256": head,
            "ok": True,
            "from_receipt": True,
            "scripted_source_confirmed": True,
        },
    }
    if forbidden_paths(packet):
        raise ValueError(f"judgment packet contains forbidden keys: {forbidden_paths(packet)}")
    atomic_json(packet_path, packet)

    return {
        "packet_path": relative(packet_path),
        "packet_sha256": sha256(packet_path),
        "judge_response_sha256": sha256(judge_path),
        "prediction_receipt_sha256": prediction_receipt["receipt_sha"],
        "judgment_receipt_sha256": head,
        "receipt_chain_sha256": sha256(chain_path),
        "verify_output_sha256": sha256(verify_path),
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--prereg", required=True)
    parser.add_argument("--evidence", required=True)
    parser.add_argument("--judge-response", required=True)
    parser.add_argument("--failed-attempt")
    parser.add_argument("--source-commit", required=True)
    parser.add_argument("--output-dir", required=True)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    result = package(args)
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
