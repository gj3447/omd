#!/usr/bin/env python3
"""OMD scheduler redesign M0: reproducible, real-code characterization.

This harness deliberately changes no runtime behavior.  It measures the real
``Coordinator`` through three deterministic scenarios:

* ``fairness`` reproduces or refutes older-PENDING overtaking;
* ``claims`` measures disjoint claim scaling against one Coordinator/SQLite DB;
* ``connect`` measures end-to-end connect serialization with real Git objects.

Fairness failure is data, not a harness process failure.  The process exits
non-zero only when setup, execution, schema, or safety-oracle integrity fails.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import platform
import sqlite3
import statistics
import subprocess
import sys
import tempfile
import threading
import time
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Iterable


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from omd_server import Coordinator, Emitter  # noqa: E402


SCHEMA_VERSION = "omd.m0.benchmark.v1"
DEFAULT_CID = "omd-scheduler-m0-baseline"


class _Collector:
    def __init__(self) -> None:
        self.events: list[dict[str, Any]] = []

    def ship(self, envelopes: Iterable[dict[str, Any]]) -> None:
        self.events.extend(dict(envelope) for envelope in envelopes)


def _git(args: list[str], cwd: Path | None = None, *, check: bool = True) -> str:
    result = subprocess.run(
        ["git", *args],
        cwd=str(cwd or REPO_ROOT),
        check=check,
        capture_output=True,
        text=True,
    )
    return result.stdout.strip()


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def nearest_rank(values: Iterable[int | float], quantile: float) -> int | float:
    """Return the documented nearest-rank quantile for a non-empty sample."""
    ordered = sorted(values)
    if not ordered:
        raise ValueError("nearest_rank requires at least one value")
    if not 0 < quantile <= 1:
        raise ValueError("quantile must be in (0, 1]")
    return ordered[max(0, math.ceil(quantile * len(ordered)) - 1)]


def latency_summary(values_ns: Iterable[int]) -> dict[str, int | float]:
    values = list(values_ns)
    if not values:
        return {"count": 0, "min": 0, "mean": 0.0, "p50": 0, "p95": 0, "p99": 0, "max": 0}
    return {
        "count": len(values),
        "min": min(values),
        "mean": statistics.fmean(values),
        "p50": nearest_rank(values, 0.50),
        "p95": nearest_rank(values, 0.95),
        "p99": nearest_rank(values, 0.99),
        "max": max(values),
    }


def parse_workers(raw: str | Iterable[int]) -> list[int]:
    if isinstance(raw, str):
        values = [int(item) for item in raw.split(",") if item.strip()]
    else:
        values = [int(item) for item in raw]
    if not values or any(value <= 0 for value in values):
        raise ValueError("workers must contain positive integers")
    if len(values) != len(set(values)):
        raise ValueError("workers must not contain duplicates")
    return values


def _parallel_calls(
    items: list[Any],
    workers: int,
    call: Callable[[Any], Any],
) -> dict[str, Any]:
    if workers <= 0 or workers > len(items):
        raise ValueError("workers must be in [1, len(items)]")
    buckets = [items[index::workers] for index in range(workers)]
    barrier = threading.Barrier(workers)
    started_ns: dict[str, int] = {}
    results: list[list[dict[str, Any]]] = [[] for _ in range(workers)]
    ended_ns: list[int | None] = [None] * workers
    errors: list[dict[str, Any]] = []
    error_lock = threading.Lock()

    def worker(worker_id: int) -> None:
        try:
            token = barrier.wait()
            if token == 0:
                started_ns["value"] = time.perf_counter_ns()
            while "value" not in started_ns:
                pass
            for item in buckets[worker_id]:
                before = time.perf_counter_ns()
                value = call(item)
                after = time.perf_counter_ns()
                results[worker_id].append(
                    {"item": item, "latency_ns": after - before, "value": value}
                )
        except BaseException as exc:  # noqa: BLE001 - benchmark must surface worker failures
            with error_lock:
                errors.append(
                    {
                        "worker": worker_id,
                        "type": type(exc).__name__,
                        "message": str(exc),
                    }
                )
        finally:
            ended_ns[worker_id] = time.perf_counter_ns()

    threads = [
        threading.Thread(target=worker, args=(worker_id,), name=f"omd-m0-{worker_id}")
        for worker_id in range(workers)
    ]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    if "value" not in started_ns:
        raise RuntimeError("parallel barrier never released")
    completed_ends = [value for value in ended_ns if value is not None]
    return {
        "wall_ns": max(completed_ends) - started_ns["value"],
        "calls": [row for bucket in results for row in bucket],
        "errors": errors,
    }


def _state(omd: Coordinator, orbit_id: str) -> str | None:
    row = omd.store.get_orbit(orbit_id)
    return row["state"] if row else None


def fairness_probe(
    *,
    cid: str = DEFAULT_CID,
    backend: Any | None = None,
) -> dict[str, Any]:
    """Run the real holder -> broad waiter -> later narrow claimant trace."""
    with tempfile.TemporaryDirectory(prefix="omd-m0-fairness-") as tmp:
        root = Path(tmp)
        collector = backend or _Collector()
        omd = Coordinator(
            str(root / "omd.db"),
            agent_ttl=None,
            events=Emitter(collector),
            sweep_interval=None,
        )
        holder_agent = f"{cid}-holder"
        older_agent = f"{cid}-older"
        newer_agent = cid
        try:
            holder = omd.claim(holder_agent, ["src/a.py"], priority=0, ttl=3600.0)
            older = omd.claim(older_agent, ["src/**"], priority=10, ttl=3600.0)
            newer = omd.claim(newer_agent, ["src/b.py"], priority=0, ttl=3600.0)

            holder_row = omd.store.get_orbit(holder["orbit_id"])
            older_row = omd.store.get_orbit(older["orbit_id"])
            newer_row = omd.store.get_orbit(newer["orbit_id"])
            initial = {
                "holder": holder_row["state"],
                "older": older_row["state"],
                "newer": newer_row["state"],
            }

            omd.release(holder["orbit_id"], holder_agent, holder["fence"])
            after_holder_release = {
                "older": _state(omd, older["orbit_id"]),
                "newer": _state(omd, newer["orbit_id"]),
            }

            # Drain whichever orbit was promoted/granted first, then observe that
            # the other waiter can eventually progress.  This is cleanup plus a
            # bounded liveness observation, not the no-overtaking safety oracle.
            for response, agent in ((older, older_agent), (newer, newer_agent)):
                row = omd.store.get_orbit(response["orbit_id"])
                if row and row["state"] == "HELD":
                    omd.release(response["orbit_id"], agent, row["fence"])
            for response, agent in ((older, older_agent), (newer, newer_agent)):
                row = omd.store.get_orbit(response["orbit_id"])
                if row and row["state"] == "HELD":
                    omd.release(response["orbit_id"], agent, row["fence"])

            final = {
                "older": _state(omd, older["orbit_id"]),
                "newer": _state(omd, newer["orbit_id"]),
            }
            no_overtaking_passed = initial["newer"] == "PENDING"
            observation = {
                "schema": "omd.m0.fairness-observation.v1",
                "cid": cid,
                "holder_state": initial["holder"],
                "older_waiter_state": initial["older"],
                "newer_claim_state": initial["newer"],
                "after_holder_release": after_holder_release,
                "final": final,
                "older_priority": older_row["priority"],
                "newer_priority": newer_row["priority"],
                "older_created_at": older_row["created_at"],
                "newer_created_at": newer_row["created_at"],
                "older_arrived_first": older_row["created_at"] < newer_row["created_at"],
                "newer_granted_while_older_pending": (
                    initial["older"] == "PENDING" and initial["newer"] == "HELD"
                ),
                "no_overtaking_passed": no_overtaking_passed,
                "store_readback_complete": all(
                    row is not None for row in (holder_row, older_row, newer_row)
                ),
            }
            Emitter(collector).emit(
                "scheduler_fairness_probe_completed",
                cid,
                older_waiter_state=observation["older_waiter_state"],
                newer_claim_state=observation["newer_claim_state"],
                no_overtaking_passed=observation["no_overtaking_passed"],
                store_readback_complete=observation["store_readback_complete"],
            )
            return observation
        finally:
            omd.close()


def run_ooptdd(backend: Any, cid: str) -> dict[str, Any]:
    """OOPTDD entrypoint: real Coordinator producer plus backend readback seam."""
    return fairness_probe(cid=cid, backend=backend)


def claim_episode(*, workers: int, operations: int, repeat: int = 0) -> dict[str, Any]:
    if operations < workers:
        raise ValueError("operations must be at least workers")
    with tempfile.TemporaryDirectory(prefix="omd-m0-claims-") as tmp:
        root = Path(tmp)
        omd = Coordinator(
            str(root / "omd.db"),
            agent_ttl=None,
            sweep_interval=None,
        )
        try:
            result = _parallel_calls(
                list(range(operations)),
                workers,
                lambda item: omd.claim(
                    f"claim-agent-{repeat:02d}-{item:06d}",
                    [f"bench/{item:06d}.py"],
                    ttl=3600.0,
                ),
            )
            values = [row["value"] for row in result["calls"]]
            states = Counter(value.get("state", "MISSING") for value in values)
            fences = [value.get("fence") for value in values if value.get("fence") is not None]
            latencies = [row["latency_ns"] for row in result["calls"]]
            completed = len(values)
            invariants = {
                "all_held": states == {"HELD": operations},
                "unique_fences": len(set(fences)) == operations,
                "consecutive_fences": bool(fences) and max(fences) - min(fences) + 1 == operations,
                "final_held_count_matches": len(omd.store.held_orbits()) == operations,
                "no_thread_errors": not result["errors"],
            }
            return {
                "scenario": "claim-scale",
                "repeat": repeat,
                "workers": workers,
                "requested": operations,
                "completed": completed,
                "wall_ns": result["wall_ns"],
                "throughput_ops_s": completed * 1_000_000_000 / result["wall_ns"],
                "latency_ns": latency_summary(latencies),
                "raw_latency_ns": latencies,
                "outcomes": dict(sorted(states.items())),
                "fence_min": min(fences) if fences else None,
                "fence_max": max(fences) if fences else None,
                "unique_fences": len(set(fences)),
                "final_held_count": len(omd.store.held_orbits()),
                "invariants": invariants,
                "errors": result["errors"],
            }
        finally:
            omd.close()


def _init_repo(root: Path) -> None:
    root.mkdir()
    hooks = root.parent / "empty-hooks"
    hooks.mkdir()
    _git(["init", "-b", "main", str(root)], cwd=root.parent)
    _git(["config", "user.name", "omd-bench"], cwd=root)
    _git(["config", "user.email", "omd-bench@example.invalid"], cwd=root)
    _git(["config", "commit.gpgSign", "false"], cwd=root)
    _git(["config", "core.hooksPath", str(hooks)], cwd=root)
    (root / "README.md").write_text("base\n", encoding="utf-8")
    _git(["add", "README.md"], cwd=root)
    _git(["commit", "-m", "base"], cwd=root)
    _git(["checkout", "-b", "dev"], cwd=root)


def _prepare_connect_task(omd: Coordinator, index: int) -> tuple[str, str, int]:
    task = f"connect-{index:03d}"
    agent = f"connect-agent-{index:03d}"
    area = f"bench/task-{index:03d}"
    declared = omd.declare(task, writes=[f"{area}/**"])
    if not declared["ok"]:
        raise RuntimeError(f"declare failed: {declared}")
    omd.next_task(agent)
    claim = omd.claim(agent, [f"{area}/**"], task_id=task, ttl=3600.0)
    if claim["state"] != "HELD":
        raise RuntimeError(f"claim failed: {claim}")
    started = omd.start(task, agent)
    worktree = Path(started["worktree"])
    (worktree / area).mkdir(parents=True)
    (worktree / area / "result.txt").write_text(f"task={index}\n", encoding="utf-8")
    committed = omd.commit(task, f"feat: benchmark task {index}", agent, claim["fence"])
    if not committed["ok"]:
        raise RuntimeError(f"commit failed: {committed}")
    finished = omd.finish(task, agent, claim["fence"])
    if finished.get("state") != "DONE":
        raise RuntimeError(f"finish failed: {finished}")
    return task, agent, claim["fence"]


def connect_episode(
    *,
    workers: int,
    tasks: int,
    check_delay_ms: float,
    repeat: int = 0,
) -> dict[str, Any]:
    if tasks < workers:
        raise ValueError("tasks must be at least workers")
    delay_s = check_delay_ms / 1000.0
    with tempfile.TemporaryDirectory(prefix="omd-m0-connect-") as tmp:
        root = Path(tmp)
        repo = root / "repo"
        _init_repo(repo)
        omd = Coordinator(
            str(root / "omd.db"),
            repo=str(repo),
            worktrees_dir=str(root / "worktrees"),
            integration_branch="main",
            agent_ttl=None,
            merge_timeout=max(30.0, tasks * max(delay_s, 0.001) * 8),
            integration_check=(sys.executable, "-c", "pass"),
            integration_check_timeout=max(5.0, delay_s * 4),
            require_integration_check=True,
            sweep_interval=None,
        )
        active_checks = 0
        max_active_checks = 0
        check_durations: list[int] = []
        active_lock = threading.Lock()

        def fixed_check(argv: Any, cwd: str, *, timeout: float, output_limit: int):
            del argv, cwd, timeout, output_limit
            nonlocal active_checks, max_active_checks
            before = time.perf_counter_ns()
            with active_lock:
                active_checks += 1
                max_active_checks = max(max_active_checks, active_checks)
            try:
                time.sleep(delay_s)
            finally:
                with active_lock:
                    active_checks -= 1
                check_durations.append(time.perf_counter_ns() - before)
            return 0, "", "", False

        try:
            prepared = [_prepare_connect_task(omd, index) for index in range(tasks)]
            omd.git._run_operator_check = fixed_check
            result = _parallel_calls(
                prepared,
                workers,
                lambda item: omd.connect(item[0], item[1], item[2]),
            )
            values = [row["value"] for row in result["calls"]]
            latencies = [row["latency_ns"] for row in result["calls"]]
            states = Counter(value.get("state", "MISSING") for value in values)
            integration = Path(omd.integration_worktree)
            expected_files = [
                integration / f"bench/task-{index:03d}/result.txt" for index in range(tasks)
            ]
            clean = _git(["status", "--porcelain"], cwd=integration) == ""
            invariants = {
                "all_merged": states == {"MERGED": tasks},
                "no_thread_errors": not result["errors"],
                "no_merge_token_leak": omd.store.all_held_merge_tokens() == [],
                "integration_worktree_clean": clean,
                "all_expected_files_present": all(path.exists() for path in expected_files),
            }
            return {
                "scenario": "connect-serial",
                "repeat": repeat,
                "workers": workers,
                "requested": tasks,
                "completed": len(values),
                "check_delay_ms": check_delay_ms,
                "check_driver": "in_process_fixed_delay_hook",
                "wall_ns": result["wall_ns"],
                "throughput_tasks_s": len(values) * 1_000_000_000 / result["wall_ns"],
                "latency_ns": latency_summary(latencies),
                "raw_latency_ns": latencies,
                "check_latency_ns": latency_summary(check_durations),
                "raw_check_latency_ns": check_durations,
                "max_active_checks": max_active_checks,
                "serialized_check_floor_ns": int(tasks * delay_s * 1_000_000_000),
                "wall_minus_check_floor_ns": result["wall_ns"] - int(tasks * delay_s * 1_000_000_000),
                "outcomes": dict(sorted(states.items())),
                "invariants": invariants,
                "errors": result["errors"],
            }
        finally:
            omd.close()


def _source_metadata() -> dict[str, Any]:
    script = Path(__file__).resolve()
    status_lines = _git(["status", "--porcelain"]).splitlines()
    return {
        "git_head": _git(["rev-parse", "HEAD"]),
        "git_branch": _git(["branch", "--show-current"]),
        "git_dirty": bool(status_lines),
        "dirty_paths": status_lines,
        "benchmark_path": str(script.relative_to(REPO_ROOT)),
        "benchmark_sha256": _sha256(script),
        "python": sys.version,
        "sqlite": sqlite3.sqlite_version,
        "git": _git(["--version"]),
        "platform": platform.platform(),
        "cpu_count": os.cpu_count(),
        "perf_counter_resolution_s": time.get_clock_info("perf_counter").resolution,
        "percentile_method": "nearest-rank ceil(q*n)",
    }


def _aggregate(episodes: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[tuple[str, int], list[dict[str, Any]]] = defaultdict(list)
    for episode in episodes:
        if "workers" in episode:
            grouped[(episode["scenario"], episode["workers"])].append(episode)
    aggregate: list[dict[str, Any]] = []
    baseline: dict[str, float] = {}
    for (scenario, workers), rows in sorted(grouped.items()):
        metric = "throughput_ops_s" if scenario == "claim-scale" else "throughput_tasks_s"
        throughputs = [row[metric] for row in rows]
        median_throughput = statistics.median(throughputs)
        if workers == 1:
            baseline[scenario] = median_throughput
        aggregate.append(
            {
                "scenario": scenario,
                "workers": workers,
                "episodes": len(rows),
                f"{metric}_median": median_throughput,
                f"{metric}_min": min(throughputs),
                f"{metric}_max": max(throughputs),
                "p99_latency_ns_median": statistics.median(
                    row["latency_ns"]["p99"] for row in rows
                ),
            }
        )
    for row in aggregate:
        base = baseline.get(row["scenario"])
        metric = (
            "throughput_ops_s_median"
            if row["scenario"] == "claim-scale"
            else "throughput_tasks_s_median"
        )
        if base:
            row["speedup_vs_one_worker"] = row[metric] / base
            row["parallel_efficiency"] = row["speedup_vs_one_worker"] / row["workers"]
    return aggregate


def run_suite(
    *,
    scenarios: list[str],
    workers: list[int],
    claim_operations: int,
    connect_tasks: int,
    check_delay_ms: float,
    fairness_repeats: int,
    warmups: int,
    repeats: int,
    cid: str = DEFAULT_CID,
) -> dict[str, Any]:
    started = datetime.now(timezone.utc).isoformat()
    episodes: list[dict[str, Any]] = []
    warmup_episodes: list[dict[str, Any]] = []
    failures: list[dict[str, Any]] = []

    if "fairness" in scenarios:
        observations = []
        for repeat in range(fairness_repeats):
            observation = fairness_probe(cid=f"{cid}-{repeat:02d}")
            observation["repeat"] = repeat
            observations.append(observation)
            if not observation["store_readback_complete"] or not observation["older_arrived_first"]:
                failures.append({"scenario": "fairness", "repeat": repeat, "observation": observation})
        episodes.append(
            {
                "scenario": "pending-overtake",
                "repeats": fairness_repeats,
                "no_overtaking_passes": sum(
                    1 for observation in observations if observation["no_overtaking_passed"]
                ),
                "overtaking_observed": sum(
                    1
                    for observation in observations
                    if observation["newer_granted_while_older_pending"]
                ),
                "observations": observations,
            }
        )

    matrix_workers = list(workers)
    for warmup in range(warmups):
        order = matrix_workers if warmup % 2 == 0 else list(reversed(matrix_workers))
        if "claims" in scenarios:
            for worker_count in order:
                episode = claim_episode(
                    workers=worker_count,
                    operations=claim_operations,
                    repeat=-(warmup + 1),
                )
                episode["warmup"] = True
                warmup_episodes.append(episode)
                if not all(episode["invariants"].values()):
                    failures.append({"scenario": "claims-warmup", "episode": episode})
        if "connect" in scenarios:
            for worker_count in order:
                episode = connect_episode(
                    workers=worker_count,
                    tasks=connect_tasks,
                    check_delay_ms=check_delay_ms,
                    repeat=-(warmup + 1),
                )
                episode["warmup"] = True
                warmup_episodes.append(episode)
                if not all(episode["invariants"].values()):
                    failures.append({"scenario": "connect-warmup", "episode": episode})
    for repeat in range(repeats):
        order = matrix_workers if repeat % 2 == 0 else list(reversed(matrix_workers))
        if "claims" in scenarios:
            for worker_count in order:
                episode = claim_episode(
                    workers=worker_count,
                    operations=claim_operations,
                    repeat=repeat,
                )
                episode["execution_order"] = len(episodes)
                episodes.append(episode)
                if not all(episode["invariants"].values()):
                    failures.append({"scenario": "claims", "episode": episode})
        if "connect" in scenarios:
            for worker_count in order:
                episode = connect_episode(
                    workers=worker_count,
                    tasks=connect_tasks,
                    check_delay_ms=check_delay_ms,
                    repeat=repeat,
                )
                episode["execution_order"] = len(episodes)
                episodes.append(episode)
                if not all(episode["invariants"].values()):
                    failures.append({"scenario": "connect", "episode": episode})

    return {
        "schema": SCHEMA_VERSION,
        "started_at_utc": started,
        "completed_at_utc": datetime.now(timezone.utc).isoformat(),
        "source": _source_metadata(),
        "config": {
            "scenarios": scenarios,
            "workers": workers,
            "claim_operations": claim_operations,
            "connect_tasks": connect_tasks,
            "check_delay_ms": check_delay_ms,
            "fairness_repeats": fairness_repeats,
            "warmups": warmups,
            "repeats": repeats,
            "client_model": "threads_single_coordinator",
            "persistent_db": True,
            "agent_ttl": None,
        },
        "warmup_episodes": warmup_episodes,
        "episodes": episodes,
        "aggregates": _aggregate(episodes),
        "harness_failures": failures,
    }


def _fairness_replication_complete(benchmark: dict[str, Any], expected: int) -> bool:
    rows = [row for row in benchmark["episodes"] if row["scenario"] == "pending-overtake"]
    if len(rows) != 1 or rows[0]["repeats"] != expected:
        return False
    observations = rows[0]["observations"]
    return len(observations) == expected and all(
        observation["holder_state"] == "HELD"
        and observation["older_waiter_state"] == "PENDING"
        and observation["newer_claim_state"] == "HELD"
        and observation["older_arrived_first"] is True
        and observation["newer_granted_while_older_pending"] is True
        and observation["no_overtaking_passed"] is False
        and observation["store_readback_complete"] is True
        for observation in observations
    )


def _claim_matrix_complete(
    benchmark: dict[str, Any],
    *,
    workers: list[int],
    operations: int,
    repeats: int,
) -> bool:
    rows = [row for row in benchmark["episodes"] if row["scenario"] == "claim-scale"]
    by_workers: dict[int, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        by_workers[row["workers"]].append(row)
    if set(by_workers) != set(workers):
        return False
    for worker_count in workers:
        group = by_workers[worker_count]
        if len(group) != repeats:
            return False
        for row in group:
            if (
                row["requested"] != operations
                or row["completed"] != operations
                or len(row.get("raw_latency_ns", [])) != operations
                or not all(
                    isinstance(value, int) and value >= 0
                    for value in row.get("raw_latency_ns", [])
                )
                or not math.isfinite(row["throughput_ops_s"])
                or row["throughput_ops_s"] <= 0
                or not all(row["invariants"].values())
            ):
                return False
    return True


def build_evidence_record(
    preregistration: dict[str, Any],
    preregistration_path: Path,
    benchmark: dict[str, Any],
    argv: list[str],
) -> dict[str, Any]:
    contract = preregistration["benchmark_contract"]
    fairness_complete = _fairness_replication_complete(
        benchmark, preregistration["descriptive_only"]["no_overtaking_pass_rate"]["episodes"]
    )
    matrix_complete = _claim_matrix_complete(
        benchmark,
        workers=contract["workers"],
        operations=contract["total_claims_per_episode"],
        repeats=contract["measured_episodes"],
    )
    obligations = int(fairness_complete) + int(matrix_complete)
    prereg_sha = _sha256(preregistration_path)
    core_path = REPO_ROOT / "omd_server" / "core.py"
    script_path = Path(__file__).resolve()
    fairness_row = next(
        row for row in benchmark["episodes"] if row["scenario"] == "pending-overtake"
    )
    n8 = next(
        (row for row in benchmark["aggregates"] if row["scenario"] == "claim-scale" and row["workers"] == 8),
        None,
    )
    env = benchmark["source"]
    return {
        "schema": "lakato-evidence-record/v1",
        "programme": preregistration["programme"],
        "branch": preregistration["branch"],
        "conjecture": "M0_scheduler_baseline_replication",
        "node_tag": "m0_reproducible_baseline",
        "preregistration": {
            "claim": preregistration["conjecture"],
            "predicted": {
                "metric": preregistration["prediction"]["metric"],
                "value": preregistration["prediction"]["baseline"],
                "unit": "count",
            },
            "noise_band": preregistration["prediction"]["noise_band"],
            "direction": preregistration["prediction"]["direction"],
            "kill_condition": "m0_replay_obligations_met != 2",
            "registered_before_measurement": preregistration["registered_before_measurement"],
            "request_sha256": prereg_sha,
            "registered_at": preregistration["registered_at"],
        },
        "measurement": {
            "metric": "m0_replay_obligations_met",
            "value": obligations,
            "unit": "count",
            "scope": "replication-and-descriptive-baseline-only",
            "started_at": benchmark["started_at_utc"],
            "completed_at": benchmark["completed_at_utc"],
            "derived": {
                "fairness_replication_complete": fairness_complete,
                "claim_matrix_complete": matrix_complete,
                "no_overtaking": {
                    "episodes": fairness_row["observations"],
                    "pass_rate": fairness_row["no_overtaking_passes"] / fairness_row["repeats"],
                },
                "performance_baseline": {
                    "performance_claim": False,
                    "parallel_efficiency_n8": n8.get("parallel_efficiency") if n8 else None,
                    "benchmark": benchmark,
                },
            },
        },
        "provenance": {
            "grounded": True,
            "inputs": [
                {
                    "name": "omd_subject",
                    "source": "omd_server/core.py",
                    "git_commit": env["git_head"],
                    "sha256": _sha256(core_path),
                },
                {
                    "name": "measurement_harness",
                    "source": "benchmarks/omd_scheduler_m0.py",
                    "git_commit": env["git_head"],
                    "sha256": _sha256(script_path),
                },
                {
                    "name": "locked_preregistration",
                    "source": str(preregistration_path.relative_to(REPO_ROOT)),
                    "sha256": prereg_sha,
                },
            ],
        },
        "harness": {
            "script": "benchmarks/omd_scheduler_m0.py",
            "git_commit": env["git_head"],
            "env": json.dumps(
                {key: env[key] for key in ("python", "sqlite", "git", "platform")},
                sort_keys=True,
            ),
            "argv": argv,
            "timestamp": benchmark["started_at_utc"],
        },
        "findings": [
            {
                "kind": "opens",
                "frontier": "q_m1_no_overtaking_fix",
                "body": "M1 must make the separately locked no-overtaking gate green.",
            },
            {
                "kind": "opens",
                "frontier": "q_throughput_improvement",
                "body": "No improvement is claimed until a later arm compares against this baseline.",
            },
        ],
    }


def _write_output(payload: dict[str, Any], destination: str) -> None:
    rendered = json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n"
    if destination == "-":
        sys.stdout.write(rendered)
        return
    path = Path(destination)
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(rendered, encoding="utf-8")
    os.replace(temporary, path)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--scenarios",
        default="fairness,claims",
        help="comma-separated fairness,claims,connect",
    )
    parser.add_argument("--workers", default="1,2,4,8")
    parser.add_argument("--claim-operations", type=int, default=300)
    parser.add_argument("--connect-tasks", type=int, default=8)
    parser.add_argument("--check-delay-ms", type=float, default=150.0)
    parser.add_argument("--fairness-repeats", type=int, default=10)
    parser.add_argument("--warmups", type=int, default=1)
    parser.add_argument("--repeats", type=int, default=10)
    parser.add_argument("--cid", default=DEFAULT_CID)
    parser.add_argument(
        "--prereg",
        help="locked preregistration; when set, emit lakato-evidence-record/v1",
    )
    parser.add_argument("--output", default="-")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    scenarios = [item.strip() for item in args.scenarios.split(",") if item.strip()]
    unknown = sorted(set(scenarios) - {"fairness", "claims", "connect"})
    if unknown:
        raise SystemExit(f"unknown scenarios: {', '.join(unknown)}")
    workers = parse_workers(args.workers)
    if args.claim_operations < max(workers):
        raise SystemExit("claim operations must be at least the largest worker count")
    if "connect" in scenarios and args.connect_tasks < max(workers):
        raise SystemExit("connect tasks must be at least the largest worker count")
    if args.fairness_repeats <= 0 or args.repeats <= 0 or args.warmups < 0:
        raise SystemExit("measured repeat counts must be positive and warmups non-negative")
    payload = run_suite(
        scenarios=scenarios,
        workers=workers,
        claim_operations=args.claim_operations,
        connect_tasks=args.connect_tasks,
        check_delay_ms=args.check_delay_ms,
        fairness_repeats=args.fairness_repeats,
        warmups=args.warmups,
        repeats=args.repeats,
        cid=args.cid,
    )
    output = payload
    if args.prereg:
        prereg_path = Path(args.prereg).resolve()
        prereg = json.loads(prereg_path.read_text(encoding="utf-8"))
        output = build_evidence_record(prereg, prereg_path, payload, list(sys.argv))
    _write_output(output, args.output)
    return 1 if payload["harness_failures"] else 0


if __name__ == "__main__":
    raise SystemExit(main())
