# OMD state-space harness

This directory adds two complementary checks for OMD's small but dangerous
coordination core.

## TLA+ model

`spec/omd_lease.tla` is an executable abstraction of the lease/fence/task
state space. It intentionally ignores Git and SQLite details and checks the
core safety properties:

- no overlapping `HELD` write orbit
- no duplicate live fence
- no task reaches `MERGED` without releasing its write orbit

Run with TLC:

```bash
cd spec
tlc2 omd_lease.tla
```

Or with a local TLA+ tools jar:

```bash
cd spec
java -cp /path/to/tla2tools.jar tlc2.TLC omd_lease.tla
```

The finite config in `omd_lease.cfg` uses three agents, three tasks, and two
abstract write resources. Increase those constants only after the small model is
green; state explosion is expected.

## Python implementation harness

`tests/test_stateful_harness.py` uses Hypothesis rule-based state machines
against the real `Coordinator` API. It does not prove the implementation, but it
does produce long random command sequences and checks the same always-on
invariants after each step.

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
