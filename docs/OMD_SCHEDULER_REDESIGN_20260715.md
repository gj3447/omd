# OMD safe conflict-aware scheduler redesign

Status: **M0 evidence slice implemented; runtime behavior unchanged.**  The
connect pipeline and no-overtaking changes remain preregistered implementation
fronts.  Do not describe this branch as an optimized scheduler.

## Decision

Keep SQLite/WAL as the lease and attempt authority.  Split long Git and checker
effects away from the authority transaction, and reduce the unavoidable
single-ref serial section to an expected-old-SHA publication CAS plus short
finalization transaction.

The durable boundary is an orthogonal `ConnectAttempt` aggregate inside OMD's
`L_RT` runtime, not a second service and not more states added to the existing
Task enum.  Task remains a compatibility projection:

| ConnectAttempt | Legacy Task projection |
|---|---|
| `QUEUED..VALIDATED` | `DONE` |
| `PUBLISH_INTENT..PUBLISHED` | `CONNECTING` |
| `FINALIZED` | `MERGED` |
| pre-publish `RED/FAILED/STALE/CANCELLED` | `DONE` |
| ambiguous publish outcome | `CONNECTING` fail-stop |

Machine-readable authorities:

- `spec/connect_pipeline_engine.json`
- `spec/connect_pipeline_fsm.json`
- `spec/connect_pipeline_traces.json`
- `spec/connect_pipeline_loop.json`

## Evidence for the ordering

The first bottleneck to remove is not SQLite itself.

1. `_cs()` combines a process-wide `RLock` and SQLite write transaction.
2. `start()` and `commit()` perform Git subprocess work in that critical
   section.
3. `connect()` polls at 10 ms while waiting for a repository-wide token.
4. The token covers merge, the complete integration check, and optional push.
5. Path admission checks HELD orbits but not older conflicting PENDING orbits.

Therefore the implementation order is: measure, restore fairness, remove busy
polling and slow effects from the state transaction, then introduce exact-tree
validation and short CAS publication.  Replacing SQLite before those changes
would retain the long repository fence.

## Required invariants

1. A published ref resolves to the exact candidate tree named by a green
   receipt.
2. A receipt binds base SHA, task tip SHA, candidate SHA/tree, write-set digest,
   fence set, and check-policy digest.
3. A stale base cannot reuse a previous receipt.
4. `PUBLISH_INTENT(expected_old,new_sha,effect_id)` is durable before ref
   mutation.
5. Only the configured publisher capability may mutate the integration ref.
6. Unknown publication outcomes are reconciled by authoritative ref readback;
   they are never blindly retried or destructively reset.
7. Finalization records Task `MERGED`, generation/log, lease release, and outbox
   intents atomically after publication evidence.
8. Older or higher-priority conflicting PENDING admission cannot be overtaken by
   a new claimant.
9. Every wait has bounded capacity, cancellation, and exhaustion; no fixed
   10 ms authoritative polling loop remains.

## M0: frozen evidence, not a runtime fix

M0 adds a standard-library real-code harness and locks the next behavior gate.

```text
benchmarks/omd_scheduler_m0.py
  fairness  -> holder src/a.py HELD
               older src/** PENDING
               newer src/b.py current outcome
  claims    -> N={1,2,4,8}, one Coordinator, fresh SQLite DB per episode
  connect   -> opt-in real Git/worktree/merge with a fixed checker delay
```

The preregistered replication uses 10 fairness traces and 10 measured claim
episodes per worker count after one warmup.  Raw per-call latencies, episode
aggregates, environment, source hashes, and exact argv are retained.  Episode,
not individual call, is the statistical unit.

M0 explicitly does **not** claim throughput improvement, linear scaling,
optimality, a fairness fix, or novel discovery.  Earlier exploratory values are
disclosed and excluded.  The independent LakatoTree metric is only
`m0_replay_obligations_met`:

1. the known real-code overtaking trace is reproduced exactly;
2. every locked performance cell and raw sample is complete.

The evidence record contains no verdict.  `scripts/judge_scheduler_m0.py`
checks preregistration/source hashes, timestamps, recursive verdict-key absence,
grounding, raw episode completeness, and optional fresh replay before calling
LakatoTree's deterministic `judge_record()`.

The separately locked `gates/scheduler_fairness.yaml` is intentionally RED on
M0.  M1 must make that same hash green; editing the gate to fit output is
forbidden.

## Subsequent vertical slices

### M1 — fair admission and durable waiting

Admission rule:

```text
grant(R) iff
  no conflicting HELD
  and no older-or-higher-priority conflicting PENDING
```

Add durable queue sequence, aging floor, capacity/backpressure, and notification
or condition-driven wakeup.  Preserve priority first and FIFO within a priority.
The synchronous API becomes a bounded compatibility waiter over the queue.

Required receipts:

- positive: later narrow claimant remains PENDING behind older broad waiter;
- injected negative: bypassing the PENDING reservation makes the locked gate
  RED;
- liveness: after blockers release, the oldest eligible waiter becomes HELD;
- regression: disjoint claims still become HELD.

### M2 — effect split

Move worktree creation, Git add/commit/diff audit, cleanup, and push to explicit
intent/effect/finalize seams.  No Git or checker subprocess may run in a live
SQLite state transaction.  Every effect gets a stable idempotency key and typed
failure.

Promotion gate: transaction/lock hold metrics show no subprocess duration in
the critical section, while existing fencing and recovery suites remain green.

### M3 — exact candidate and short publish CAS

Prepare a private candidate, validate that exact tree outside the publish
fence, persist `PUBLISH_INTENT`, then update the configured ref only when its
current SHA equals the receipt base SHA.  A mismatch terminates that attempt as
STALE and creates a new generation for regeneration and revalidation.

Roll out `legacy -> shadow -> prepared canary -> prepared default` per
repository/ref.  Suggested falsifier: publish-token p95 still exceeds
`max(100 ms, 1% of checker duration)`.

### M4 — amortized validation

Start with small pairwise-disjoint exclusive batches.  Construct a private
ordered candidate chain, validate the final combined tree once, and publish it
with one CAS.  A red batch moves no authoritative ref and is dissolved for
individual diagnosis.  Shared/hot-file tasks and internal dependencies are
excluded in v1.

Only after batch receipts are stable should a bounded adaptive speculative
prefix train be attempted.  If cold-workload CAS stale ratio remains above 10%,
shrink speculation and prefer batching.

## Hot files and storage scaling

Same-hunk non-commutative changes are irreducibly serial.  Prefer generated
fragments plus one deterministic materializer, then structured semantic merge
only where an algebra is explicit.  Otherwise use a dedicated serial lane.

The natural shard is `(repo_id, integration_ref)`.  A per-repository SQLite
authority may be introduced only when measured writer wait is at least 10% of
wall time or violates a preregistered SLO.  Path-prefix sharding does not remove
single-ref publication and is deferred.

## Adoption track

Runtime optimization alone is insufficient.  The harness-facing paved path is:

```text
pull_begin(agent) -> atomically select/reserve/lease/create worktree/export context
complete(task)    -> commit/finish/queue connect/read back terminal receipt
```

Rollout: shadow telemetry, assisted default, managed mode requiring
agent/fence/bail epoch and strict write-set, then authoritative mode where only
the OMD publisher identity can update the protected integration ref.  A local
fail-soft push topology must not be called non-bypassable.

## Coordination and current dependency

M0 owns only disjoint benchmark/spec/docs/evidence paths under OMD task
`omd-scheduler-m0-evidence-20260715-codex`.  A separate active task,
`omd-r3-attempt-provenance-v1-20260715-codex`, owns `core.py`, `store.py`,
`gitio.py`, and related tests.  M1/M2 implementation must wait for or explicitly
coordinate with that task; M0 does not bypass its lease.

## Verification commands

```bash
python3 /Users/lagyeongjun/CD/SYMPOSIUM/SKILLS/engine-design/scripts/validate_engine_spec.py \
  spec/connect_pipeline_engine.json
python3 /Users/lagyeongjun/CD/SYMPOSIUM/SKILLS/fsm-design/scripts/validate_fsm.py \
  spec/connect_pipeline_fsm.json
python3 /Users/lagyeongjun/CD/SYMPOSIUM/SKILLS/fsm-design/scripts/run_fsm_traces.py \
  spec/connect_pipeline_fsm.json spec/connect_pipeline_traces.json
python3 /Users/lagyeongjun/CD/SYMPOSIUM/SKILLS/loop-engineering/scripts/validate_loop_contract.py \
  spec/connect_pipeline_loop.json
/Users/lagyeongjun/CD/SYMPOSIUM/PI/ooptdd-loop/.venv/bin/ooptdd-loop \
  validate-spec spec/omd_scheduler_m0_ooptdd.yaml --json
PYTHONDONTWRITEBYTECODE=1 .venv/bin/python -m pytest -q \
  -p no:cacheprovider tests/test_scheduler_m0_harness.py
```
