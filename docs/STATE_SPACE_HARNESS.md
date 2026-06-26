# OMD state-space harness

This directory adds two complementary checks for OMD's small but dangerous
coordination core.

## TLA+ model

The `spec/` directory contains three executable abstractions. They intentionally
ignore implementation detail where doing so keeps the checked state space small.

`spec/omd_lease.tla` models the lease/fence/task core:

- no overlapping `HELD` write orbit
- no duplicate live fence
- no task reaches `MERGED` without releasing its write orbit

`spec/omd_connect.tla` models split-phase `connect`:

- Phase A captures the write lease and repo-wide merge token
- Phase B records an abstract merge success/failure outside the critical section
- Phase C either records `merge_sha` and releases the write lease, or rolls back
  to retryable `DONE`
- restart recovery can forward-complete or roll back a task left in `CONNECTING`

`spec/omd_leader.tla` models D14 leader fencing:

- a live DB has at most one leader
- takeover increments epoch
- a stale coordinator may wake up, but cannot mutate unless its local epoch
  still matches the DB leader epoch; the model checks that stale mutation is not
  an enabled transition

Run with TLC:

```bash
scripts/run_tlc.sh
```

Or with an already-downloaded TLA+ tools jar:

```bash
TLA2TOOLS_JAR=/path/to/tla2tools.jar scripts/run_tlc.sh
```

The checked configs are intentionally small, CI-sized models. Increase those
constants only for local deep runs after the small models are green; state
explosion is expected.

## Python implementation harness

`tests/test_stateful_harness.py` uses Hypothesis rule-based state machines
against the real `Coordinator` API. It does not prove the implementation, but it
does produce long random command sequences and checks always-on invariants after
each step. The generated operations now cover ordinary claims, owner/fence
release, stale-fence release attempts, renewals, task lifecycle, connect retry
ids, heartbeat, and bail/zombie fencing.

Run:

```bash
.venv/bin/pytest tests/test_stateful_harness.py -q
```

If Hypothesis is not installed the test skips. For local dev:

```bash
uv pip install hypothesis
```

This is a harness/KG-style receipt: the TLA+ model owns the abstract state-space
claim, while the Python state machine binds that claim back to the concrete
`Coordinator`. The KG anchor is `omd-state-space-harness-20260626` in
`docs/longinus_bindings.json`.
