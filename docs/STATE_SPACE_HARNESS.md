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
  still matches the DB leader epoch; the model records `lastWriter` to make
  this a direct invariant

Run with TLC:

```bash
cd spec
tlc2 omd_lease.tla
tlc2 omd_connect.tla
tlc2 omd_leader.tla
```

Or with a local TLA+ tools jar:

```bash
cd spec
java -cp /path/to/tla2tools.jar tlc2.TLC omd_lease.tla
```

The finite configs use three coordinators/agents/tasks and a small resource set.
Increase those constants only after the small models are green; state explosion
is expected.

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
