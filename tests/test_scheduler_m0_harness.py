from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

import pytest

from benchmarks.omd_scheduler_m0 import (
    SCHEMA_VERSION,
    _Collector,
    claim_episode,
    connect_episode,
    fairness_probe,
    nearest_rank,
    parse_workers,
    run_ooptdd,
)
from benchmarks.produce_scheduler_m0_receipt import produce
from scripts.judge_scheduler_m0 import forbidden_key_paths, recompute_obligations


ROOT = Path(__file__).resolve().parents[1]


def test_nearest_rank_and_worker_parser_are_explicit():
    values = [9, 1, 5, 3]
    assert nearest_rank(values, 0.50) == 3
    assert nearest_rank(values, 0.95) == 9
    assert parse_workers("1,2,4") == [1, 2, 4]
    with pytest.raises(ValueError):
        parse_workers("1,1")
    with pytest.raises(ValueError):
        nearest_rank([], 0.50)


def test_claim_episode_runs_real_coordinator_and_checks_fences():
    episode = claim_episode(workers=2, operations=8)
    assert episode["scenario"] == "claim-scale"
    assert episode["completed"] == 8
    assert episode["latency_ns"]["count"] == 8
    assert all(episode["invariants"].values()), episode


def test_fairness_probe_reports_real_store_readback_without_freezing_outcome():
    observation = fairness_probe(cid="scheduler-m0-test")
    assert observation["holder_state"] == "HELD"
    assert observation["older_waiter_state"] == "PENDING"
    assert observation["older_arrived_first"] is True
    assert observation["store_readback_complete"] is True
    assert isinstance(observation["no_overtaking_passed"], bool)
    assert observation["newer_claim_state"] in {"HELD", "PENDING"}


def test_ooptdd_entrypoint_emits_derived_measurement_after_real_trace():
    collector = _Collector()
    result = run_ooptdd(collector, "scheduler-m0-ooptdd-test")
    matching = [
        event
        for event in collector.events
        if event.get("event") == "scheduler_fairness_probe_completed"
    ]
    assert len(matching) == 1
    assert matching[0]["no_overtaking_passed"] == result["no_overtaking_passed"]
    assert matching[0]["store_readback_complete"] is True


def test_connect_episode_uses_real_git_and_preserves_integration_invariants():
    if subprocess.run(["git", "--version"], capture_output=True).returncode != 0:
        pytest.skip("git unavailable")
    episode = connect_episode(workers=2, tasks=2, check_delay_ms=1.0)
    assert episode["scenario"] == "connect-serial"
    assert episode["completed"] == 2
    assert episode["check_driver"] == "in_process_fixed_delay_hook"
    assert all(episode["invariants"].values()), episode


def test_cli_stdout_is_machine_readable_json():
    command = [
        sys.executable,
        str(ROOT / "benchmarks" / "omd_scheduler_m0.py"),
        "--scenarios",
        "fairness,claims",
        "--workers",
        "1",
        "--claim-operations",
        "4",
        "--fairness-repeats",
        "1",
        "--repeats",
        "1",
        "--warmups",
        "0",
        "--output",
        "-",
    ]
    result = subprocess.run(command, cwd=ROOT, capture_output=True, text=True, check=True)
    payload = json.loads(result.stdout)
    assert payload["schema"] == SCHEMA_VERSION
    assert payload["harness_failures"] == []


def test_locked_ooptdd_gate_can_fail_and_recover(tmp_path):
    pytest.importorskip("ooptdd")
    gate = ROOT / "evidence" / "omd_scheduler_m0" / "m0_measurement_gate.yaml"
    receipt = produce(gate, "omd-scheduler-m0-ooptdd-fresh")
    assert receipt["positive"]["gate_result"]["ok"] is True
    assert receipt["negative"]["gate_result"]["ok"] is False
    assert receipt["restored_positive"]["gate_result"]["ok"] is True


def test_independent_judge_recomputes_obligations_and_rejects_nested_verdicts():
    prereg = {
        "prediction": {"required_value": 2.0},
        "descriptive_only": {"no_overtaking_pass_rate": {"episodes": 1}},
        "benchmark_contract": {
            "workers": [1],
            "total_claims_per_episode": 2,
            "measured_episodes": 1,
        },
    }
    observation = {
        "holder_state": "HELD",
        "older_waiter_state": "PENDING",
        "newer_claim_state": "HELD",
        "older_arrived_first": True,
        "newer_granted_while_older_pending": True,
        "no_overtaking_passed": False,
        "store_readback_complete": True,
    }
    benchmark = {
        "episodes": [
            {"scenario": "pending-overtake", "repeats": 1, "observations": [observation]},
            {
                "scenario": "claim-scale",
                "workers": 1,
                "requested": 2,
                "completed": 2,
                "raw_latency_ns": [1, 2],
                "throughput_ops_s": 3.0,
                "invariants": {"all_held": True},
                "errors": [],
            },
        ]
    }
    result = recompute_obligations(prereg, benchmark)
    assert result["m0_replay_obligations_met"] == 2
    assert forbidden_key_paths({"measurement": {"manual_verdict": "green"}}) == [
        "$.measurement.manual_verdict"
    ]
