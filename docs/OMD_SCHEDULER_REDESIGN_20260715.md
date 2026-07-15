# OMD safe conflict-aware scheduler redesign

Status: **M1 fair-admission runtime slice implemented; full M1 remains open.**
The runtime now enforces conflicting PENDING predecessor order with durable
queue tickets, shared initial/promotion decisions, reservation-cycle safety,
original-TTL promotion, exact request replay conflict checks and typed semantic
decision projection. Finite deadlines and sweep/restart timeout delivery are
implemented. Standalone wait cancellation authenticates the PENDING owner,
request generation and bail epoch, then atomically projects semantic CANCELLED
to legacy DENIED and reconciles promotion. Task cancellation terminalizes
associated PENDING/HELD admission rows and repairs legacy orphans on restart.
Direct embedded Coordinators deliver deadlines with a default 1-second authority
tick and explicit `None`/`0` opt-out. Under singleton enforcement a separate
lifecycle heartbeat worker refreshes the leader lease at `leader_ttl/3`,
independently of slow sweep execution. The MCP server defers background sweep and outbox delivery
until lifespan entry, while the CLI remains one-shot. Public renew/release, due lease expiry and
both PENDING/HELD owner-reclaim paths now pass through the typed lifecycle
reducer before legacy mutation. Repository wait capacity is durable and bounded
(default 1024; `0` is no-wait); a full queue returns a replayable semantic
`QUEUE_FULL` receipt without a row, fence or ticket, while disjoint work still
grants. A content-addressed saturating-aging v2 policy is pinned per DB (the
operational default is a 60-second quantum and +10 ceiling), uses one observed
time per authority transaction, persists rank evidence, and rechecks dynamic
reservation cycles after rank changes and immediate grants. Finite aging does
not replace the wait-deadline liveness bound. State-changing semantic admission
edges now share one authority transaction with a durable notification outbox;
post-commit delivery is stable-ID, claim-token fenced, per request-generation
FIFO and deduplicatable at-least-once. The commit hook only arms a timer;
strict notifier attempts run outside authority/effect locks with finite timeout,
one live attempt per event and a bounded live/late-outcome registry. Manual
flush and timer delivery share one lifecycle gate; shutdown rejects new drains
and joins accepted drains/effects before handoff. Retry wakes derive from
durable `available_at`/`claim_deadline`, independently of the deadline sweep.
Replacement timer start has a finite three-attempt retry while an existing later
timer is preserved. Legacy fail-soft telemetry also enters its sink only after
the authority transaction and RLock are released, preventing sink reentrancy
from creating a dispatcher/authority lock inversion.
Cross-stream predecessor rows keep a multi-orbit reclaim's
`coordination_notification/v1` summary behind every released-orbit fact, while
empty-agent reclaim uses a synthetic coordination stream. The conservative
transaction-local candidate prefilter is implemented; a persistent/sublinear
index and the prepared Connect pipeline remain implementation fronts. Do not
describe this branch as the complete
durable waiter, an optimized scheduler, a production rollout or a scientific
progress result. The governing `L_IDE` lifecycle is documented in
[`OMD_SCHEDULER_DEVELOPMENT_HARNESS_20260715.md`](./OMD_SCHEDULER_DEVELOPMENT_HARNESS_20260715.md).

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
| `QUEUED`, `PREPARING`, `VALIDATING`, `VALIDATED` | `DONE` |
| `PUBLISH_INTENT`, `PUBLISHING`, `RECONCILING`, `UNKNOWN_OUTCOME`, `FINALIZING` | `CONNECTING` |
| `FINALIZED`, `FINALIZED_CANCELED`, `FINALIZED_TIMED_OUT`, or `FINALIZED_BUDGET_EXHAUSTED` after confirmed publication | `MERGED` |
| pre-intent `RED`, `FAILED`, `CANCELLED`, `TIMED_OUT`, `BUDGET_EXHAUSTED` | `DONE` with a typed retry/new-generation or terminal reason |
| ambiguous post-intent outcome or `NEEDS_REPAIR` | `CONNECTING` fail-stop |

There is intentionally no `PUBLISHED` state in the current FSM. A publisher
response is not authority. Publication becomes trusted only after an
independent reader observes the exact candidate commit on the configured ref,
resolves that commit to the green receipt's tree, and finalization commits the
Task, lease and outbox facts.

Machine-readable authorities:

- [`scheduler_admission_engine.json`](../spec/scheduler_admission_engine.json)
- [`scheduler_admission_fsm.json`](../spec/scheduler_admission_fsm.json)
- [`scheduler_admission_traces.json`](../spec/scheduler_admission_traces.json)
- [`scheduler_development_loop.json`](../spec/scheduler_development_loop.json)
  (`L_IDE` development lifecycle)
- [`connect_pipeline_engine.json`](../spec/connect_pipeline_engine.json)
- [`connect_pipeline_fsm.json`](../spec/connect_pipeline_fsm.json)
- [`connect_pipeline_traces.json`](../spec/connect_pipeline_traces.json)
- [`connect_pipeline_loop.json`](../spec/connect_pipeline_loop.json)

The admission and connect contracts are `L_RT`. The development cycle is
`L_IDE`. Similar state names across those layers are projections, not shared
mutable state and not proof of conformance.

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

1. A finalized ref equals the exact candidate commit named by a green receipt,
   and that commit resolves to the receipt's candidate tree.
2. A receipt binds repository, integration ref, base SHA, task tip SHA,
   candidate commit/tree, write-set digest, complete fence-set digest,
   check-policy digest, environment digest and attempt generation.
3. A stale base cannot reuse a previous receipt.
4. `PUBLISH_INTENT(expected_old,new_sha,effect_id)` is durable before ref
   mutation.
5. Only the configured publisher capability may mutate the integration ref;
   validation workers and the producer never receive that capability.
6. A fresh `publish_authorization/v1` envelope binds owner, bail epoch,
   coordinator epoch, complete fence set, receipt, destination, artifact,
   generation, `development_context_hash`, expiry and unique nonce to the
   publish effect. Exact replay is rejected after consumption.
7. Unknown publication outcomes are reconciled by authoritative ref readback;
   they are never blindly retried or destructively reset. Exhausting bounded
   reconciliation preserves effect identity and evidence in `NEEDS_REPAIR`.
8. A canonical `connect_finalization_receipt/v1` binds the independently
   observed commit and tree, merge log, released fence and outbox intent;
   finalization records Task `MERGED` and those facts atomically after
   publication evidence.
9. Older or higher-priority conflicting PENDING admission cannot be overtaken by
   a new claimant.
10. Every wait has bounded capacity, cancellation, and exhaustion; no fixed
   10 ms authoritative polling loop remains.

## Authority and publication protocol

SQLite/WAL is the single mutable authority for `OrbitRequest`, lease and
`ConnectAttempt` facts. The pure reducers do not read clocks, Git, network,
credentials or checker output. They accept typed events and persist state plus
outbox intent before an environmental adapter runs.

The capability split is:

| Capability | Owner | Authority boundary |
|---|---|---|
| Lease and admission facts | `LeaseAuthority` | Exact owner, bail epoch, coordinator epoch and complete fence set are read from SQLite, not echoed by a caller |
| Candidate construction | `CandidateGit` | Private candidate only; cannot update the integration ref |
| Validation | `ValidationRunner` | Produces a receipt bound to the complete attempt identity; cannot publish |
| Publish authorization | Protected-ref policy authority | One-time token binds action, artifact, destination, generation, evidence and expiry |
| Ref mutation | `IntegrationPublisher` | Consumes the already-durable publish effect, then applies expected-old/new-commit CAS |
| Outcome classification | Independent ref reader | Reads authoritative commit and resolved tree; does not trust publisher success |
| Repair | Repair operator | Classifies preserved unknown evidence; cannot reset history to manufacture success |

The current intended effect-critical sequence is:

```text
VALIDATED
  -> PUBLISH_INTENT
  -> PUBLISHING
  -> RECONCILING
  -> FINALIZING
  -> FINALIZED | FINALIZED_CANCELED | FINALIZED_TIMED_OUT
               | FINALIZED_BUDGET_EXHAUSTED
```

Cancellation, timeout and budget exhaustion are applied immediately only before
this critical path. Once publish intent is durable, they are recorded as pending
interrupts. Publication is reconciled and finalized before the typed interrupt
is reported; a late interrupt never undoes landed history.

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

### M0 measured result (2026-07-15)

The frozen run at source commit
`b10b756219eda7d94f9e0784aa71f5203e279b3c` completed 10 fairness episodes and
40 claim-scaling episodes (24,000 raw claim latency samples).  The known
admission defect reproduced in all 10 fairness episodes: the older, higher
priority `src/**` waiter remained PENDING while the newer `src/b.py` request
was HELD.  Therefore the no-overtaking pass rate is `0/10 = 0.0`.

The descriptive single-Coordinator baseline is:

| clients | median throughput (claims/s) | speedup vs 1 | parallel efficiency | median episode p99 (ms) |
|---:|---:|---:|---:|---:|
| 1 | 260.650 | 1.000 | 1.000 | 9.803 |
| 2 | 259.788 | 0.997 | 0.498 | 16.826 |
| 4 | 253.556 | 0.973 | 0.243 | 33.365 |
| 8 | 259.258 | 0.995 | 0.124 | 58.748 |

These numbers are a baseline for later paired comparison, not an estimate of
global capacity.  They show no throughput scaling in this one-process,
one-Coordinator claim workload while tail latency rises with client count.
They do not isolate SQLite from the process-wide critical section and
increasing HELD-conflict scan, and they do not include the optional real-Git
connect scenario.

The OOPTDD receipt exercised the real Coordinator path.  The positive and
restored-positive observations reached the in-memory readback backend with
charge ratio `1.0`; dropping the required event at that backend made the same
locked gate RED with charge ratio `0.0`.

The first independent judge attempt correctly failed closed because the
LakatoTree Python environment lacked OMD's `transitions` dependency.  The
failed response is retained as `judge-attempt-1-invalid.json`.  Re-running the
unchanged judge, evidence, hashes, and criteria under OMD's virtual environment
with LakatoTree on `PYTHONPATH` completed a fresh 10 + 40 episode replay and
recomputed both obligations.  LakatoTree's scripted result is `partial`: the
reproducible measurement mechanism improved from its registered baseline, but
there is no preregistered novel target and no scheduler behavior improvement.

Durable evidence:

- `evidence/omd_scheduler_m0/evidence.json`
- `evidence/omd_scheduler_m0/ooptdd_receipt.json`
- `evidence/omd_scheduler_m0/judge-response.json`
- `evidence/omd_scheduler_m0/judgment-packet.json`
- `evidence/omd_scheduler_m0/receipt-chain.json`

## M0.5: development harness and contract hardening

M0.5 separates the development control plane from the runtime mechanisms:

- `spec/scheduler_development_loop.json` owns the `L_IDE` development cycle,
  including contract freeze, edit lease, producer/reviewer separation, evidence,
  judgment, landing, checkpoint, retry and correction;
- `spec/scheduler_admission_engine.json` plus its FSM and traces own the `L_RT`
  admission decision and per-request lifecycle;
- the connect engine, FSM, traces and loop own the `L_RT` candidate, validation,
  publish-intent, CAS, readback and finalization protocol.

The connect FSM is the lifecycle SSOT. Its `loop-contract/v1` companion is a
conservative runner aggregate, not a second state machine: it folds detailed
readback branches into receipt-or-abandon control outcomes while preserving
`publication_outcome`, `publication_committed` and `failure_class`. Thus runner
`FAILED_PERMANENT` with `unknown_effect_needs_repair` means only that automatic
work stopped; the runtime attempt remains `NEEDS_REPAIR` and the Task remains
`CONNECTING` fail-stop. Folded `RECEIPT_CONFIRMED` is outcome-neutral and emits
no publication action: the FSM has already finalized observed NEW or invalidated
authorization for observed OLD.

The `L_IDE` loop consumes the same `publish_authorization/v1` envelope as the
connect FSM. Development-only cycle, slice, contract, evidence and judge facts
are bound through `development_context_hash`; the runtime receipt and effect
identity remain explicit. A stale or revoked pre-intent approval stops editing,
releases the OMD lease through a typed receipt, and reaches `SUSPENDED` only
after authoritative release readback. Post-intent cancel, timeout and budget
events are deferred until publication is reconciled. Their landed variants
project to Task `MERGED`; pre-intent variants project to Task `DONE`; an
unclassified exhausted effect preserves Task `CONNECTING` for repair.

The development harness applies ICVC as follows:

| Axis | M0.5 realization |
|---|---|
| Inform | Content-addressed cycle manifest binds source, contract, gate, lease, role, command, environment and artifact identities |
| Constrain | Typed phase graph, OMD write-set fence, immutable preregistration, capability split, protected ref and bounded budgets prevent unsafe skips |
| Verify | Producer tests are followed by read-only contract validation, positive/negative/restored OOPTDD receipts, scripted judgment and authoritative commit/tree readback |
| Correct | `REWORK`, `SUSPENDED`, `AWAITING_JUDGE`, `RETRY_EXHAUSTED`, `UNKNOWN_OUTCOME` and `NEEDS_REPAIR` preserve evidence and require fenced resume |

The cycle uses a directed graph and producer-reviewer pattern. Its fail-safe
envelope is 96 steps, 256 aggregate tool calls, three retries per transition,
28,800 seconds wall time, recursion depth one and parallelism four. At 80% it
stops starting new verification work and checkpoints. Three identical complete
progress fingerprints without new durable state, diff, receipt or authority
observation terminate in `RETRY_EXHAUSTED`.

Checkpoints are written at every transition, immediately around effects, after
receipts and before handoff. Resume requires compatible workflow/state schemas,
base, contract and artifact hashes plus a freshly read back `HELD` fence.
Trajectory replay may diverge; deterministic replay additionally freezes
sources, tools, policies, clock/ID inputs and external responses.

M0.5 is not a runtime implementation. The current abstract trace runner proves
schema, transition, guard-outcome and invalid-event coverage using declared
guard booleans. It does not yet evaluate real payload guards, differentially
replay a production reducer or prove cross-contract projections. Those remain
promotion gates, not implied success.

## Subsequent vertical slices

### M1 — fair admission and durable waiting

Admission rule:

```text
grant(R) iff
  no conflicting HELD
  and no older-or-higher-priority conflicting PENDING
```

Add durable queue sequence, versioned saturating aging, capacity/backpressure,
and notification or condition-driven wakeup. Preserve effective priority first
and FIFO within an effective-priority tie.
The synchronous API becomes a bounded compatibility waiter over the queue.

Required receipts:

- positive: later narrow claimant remains PENDING behind older broad waiter;
- injected negative: bypassing the PENDING reservation makes the locked gate
  RED;
- restored positive: restoring the rule makes the unchanged locked gate green;
- liveness: after blockers release, the oldest eligible waiter becomes HELD;
- regression: disjoint claims still become HELD;
- bounded wait: every admitted PENDING request reaches `HELD`, `TIMED_OUT`,
  `CANCELLED`, `DENIED`, or explicit overload;
- cycle and restart: reservation precedence does not introduce an undetected
  wait-for cycle, and queue sequence/deadline/policy survive restart;
- index soundness: every exact full-scan conflict is present in the candidate
  index superset.

M1 is falsified if a lower-ranked conflicting request overtakes its predecessor,
if an unrelated global queue head blocks disjoint work, if the index misses an
exact conflict, if a bounded wait has no typed resolution, or if reservation
precedence creates an undetected cycle.

Implemented fairness slice:

- every ordinary orbit receives a monotonic `queue_seq`; legacy PENDING rows
  receive a deterministic one-time backfill of reconstructable
  request/rank/deadline fields, ordered by `(created_at, orbit_id)`; current
  decision metadata is recorded by reconciliation;
- the pure admission kernel compares effective priority descending and queue sequence
  ascending only among exact, mode-incompatible overlaps. For v2 rows,
  `effective=base+min(ceiling,floor(age/quantum))`; legacy v1 rows receive boost
  zero, and new v2 priority must leave signed-64 headroom for its ceiling;
- the active canonical policy envelope and content-addressed version are pinned
  in repository metadata. Startup fails before recovery on drift or corruption;
  rank, authority snapshot and decision share one observed transaction time,
  and decision schema/time/effective priority persist for restart readback;
- initial admission and promotion use that same kernel, so the older broad
  waiter prevents the later narrow claim while unrelated work still grants;
- promotion restores the persisted requested TTL rather than a hard-coded 600s;
- wait-for cycle detection combines HELD-owner and reservation-precedence edges,
  recomputes time-varying rank edges before promotion, and rechecks after an
  immediate grant. It denies the newest participating PENDING ticket until the
  graph is acyclic;
- canonical `admission_decision/v2` payloads bind the nine-field request
  identity, trusted authority snapshot, base/effective priority, single
  `observed_at` and decision variant, then execute the
  JSON FSM's context/effect bindings before legacy projection;
- a live admission id or DONE-cached transport id with a different agent, verb,
  path, mode, priority, reason or bail epoch returns typed
  `idempotency_conflict` without repeating the effect;
- due PENDING requests take the semantic `WAIT_TIMEOUT` path before promotion
  during sweep and restart reconciliation;
- policy-denial retry advances the durable request generation, while ordinary
  non-policy terminal replay cannot create another admission generation. Explicit
  non-policy retry is a separate `rollover_claim` operation fenced by a distinct
  operation ID and the exact latest predecessor orbit, owner, generation and
  current bail epoch; it creates a new `N+1` machine rather than reopening a
  final state. Owner-reclaim projections are excluded because reclaim retires
  the owner. Exact operation receipt replay is retained for `idem_ttl`; after
  GC, the latest-predecessor fence still prevents another successor;
- split-phase Connect and barrier effects reserve the exact idempotency
  envelope until their unlocked phase completes or is safely cleared. A
  deterministically ordered DB- and repo-scoped process/file effect locks
  (inherited by Git/check children) cover
  reservation through finalization, while task/token/idempotency rows carry one
  immutable attempt id and monotonic owner generation for Phase-C/recovery CAS;
- subprocess crash cuts verify same-repo/different-DB exclusion and prove that
  an inherited child descriptor keeps both domains fenced after parent death;
  Git common-directory identity also collapses main/linked-worktree aliases;
- merge tokens are internal-only and cannot pass through public `release`, and
  a persisted `CONNECTING` generation cannot be overwritten by either direct
  or barrier Connect before recovery resolves it;
- a live second Coordinator cannot recover that attempt. Once the effect lock
  proves the old process tree is gone, repo-bound recovery requires exact
  attempt-trailer plus merge-SHA readback; DB-only execution remains explicit;
- Connect Phase A now resolves an immutable candidate SHA and integration-base
  SHA before auditing. Git authority failures fail closed before token/state
  mutation; normal and barrier Phase B merge only that audited candidate, and
  Phase C/recovery require candidate ancestry before accepting an exact trailer.
  A concurrent integration-base advance is a typed retry that releases the
  attempt rather than leaving `CONNECTING` stuck;
- startup schema work is versioned and authority-gated. Current schema is a
  read-only fast path until leader admission; a legacy marker must first obtain
  the same-DB effect fence and a real leader generation, while unknown/future
  markers fail without mutation. Reused coordinator labels and resigned
  instances cannot bypass the instance/epoch fence;
- reclaim/bail now increments a durable per-task retry counter. Attempts beyond
  `max_reclaims` enter the permanent `POISONED` terminal, stay excluded from
  dispatch/barrier liveness, and are reported separately from requeued tasks in
  the durable coordination receipt; the preceding outbox schema migrates the
  new counter before using it;
- the lifecycle heartbeat tolerates observable transient failures and resets
  its streak after success, but emits a typed stop and exits after five
  consecutive failures rather than spinning or dying silently;
- task cancel invalidates stale claim replay, and barrier trip revalidates its
  generation and `TRIPPING` state before every effect and final success;
- the unchanged frozen gate is green normally, RED under a test-only pure
  predecessor bypass, and green again after restoration.

The materialized M1 receipt is `arrived` evidence from one in-memory
producer/readback backend. It explicitly records no separate oracle and awaits
independent judgment; it is not promoted to `external_verdict`.

The explicit non-policy request-generation rollover runtime is implemented.
Still open before a closed development cycle: a prospective M1-specific
preregistration and identity-distinct judgment, plus protected-ref
finalization. The existing materialized receipt remains execution evidence and
cannot be retrospectively promoted. Candidate-index soundness is implemented as a transaction-local
maximal-literal-prefix prefilter with exact verification, full-scan fallback and
indexed/full property tests. It reduces exact glob comparisons but is neither
persisted authority nor a sublinear database lookup. The existing Connect path now has process-tree effect fencing,
durable attempt generations and exact Git proof, but the prepared
`ConnectAttempt`/expected-old protected-ref pipeline remains open.

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
current SHA equals the receipt base SHA. Every publisher response, including a
stale-CAS claim, enters independent authoritative readback. Observed OLD
invalidates the consumed authorization and requires a fresh authorization
before retry; observed NEW proceeds to finalization; observed OTHER or bounded
readback exhaustion preserves the attempt as `NEEDS_REPAIR` without rewriting
history.

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

## Evidence, judgment and landing chain

Every runtime slice must retain a hash-bound chain rather than infer completion
from producer prose:

```text
contract/gate/preregistration freeze
  -> current OMD HELD lease readback
  -> baseline and locked negative input
  -> implementation diff and producer verification
  -> positive / injected-negative / restored evidence freeze
  -> read-only contract, projection and implementation validation
  -> identity-distinct scripted progress judgment
  -> one-time exact-action publication authorization
  -> durable publish intent and publisher attempt
  -> independent authoritative commit and resolved-tree readback
  -> finalization receipt
  -> lease release and pending-interrupt reconciliation
```

The evidence record contains no verdict. A missing judge leaves the development
cycle in `AWAITING_JUDGE`; the implementer cannot supply a replacement. A cycle
is `CLOSED` only when the scripted judgment, authoritative commit/tree readback,
finalization receipt and lease release all bind the same contract, evidence and
generation. See
[`OMD_SCHEDULER_DEVELOPMENT_HARNESS_20260715.md`](./OMD_SCHEDULER_DEVELOPMENT_HARNESS_20260715.md)
for the complete actor, checkpoint, replay and interrupt contract.

## Coordination and current dependency

M0 owned only disjoint benchmark/spec/docs/evidence paths under OMD task
`omd-scheduler-m0-evidence-20260715-codex`. At the time M0 was recorded, the
separate task `omd-r3-attempt-provenance-v1-20260715-codex` owned `core.py`,
`store.py`, `gitio.py`, and related tests.

Before any M1/M2 runtime implementation, read the current OMD authority and Git
base again, confirm that the R3 work has landed or explicitly coordinate its
branch, and acquire a fresh non-overlapping `HELD` write-set. This M0.5 contract
and documentation slice does not authorize edits to runtime files.

## Verification commands

Run from the OMD repository root.

### Contract and trace gate

```bash
: "${SYMPOSIUM_ROOT:?set SYMPOSIUM_ROOT to the SYMPOSIUM checkout}"
ENGINE_VALIDATOR="$SYMPOSIUM_ROOT/SKILLS/engine-design/scripts/validate_engine_spec.py"
FSM_VALIDATOR="$SYMPOSIUM_ROOT/SKILLS/fsm-design/scripts/validate_fsm.py"
FSM_TRACES="$SYMPOSIUM_ROOT/SKILLS/fsm-design/scripts/run_fsm_traces.py"
LOOP_VALIDATOR="$SYMPOSIUM_ROOT/SKILLS/loop-engineering/scripts/validate_loop_contract.py"

python3 "$ENGINE_VALIDATOR" \
  spec/scheduler_admission_engine.json
python3 "$FSM_VALIDATOR" \
  spec/scheduler_admission_fsm.json
python3 "$FSM_TRACES" \
  spec/scheduler_admission_fsm.json spec/scheduler_admission_traces.json
python3 "$LOOP_VALIDATOR" \
  spec/scheduler_development_loop.json

python3 "$ENGINE_VALIDATOR" \
  spec/connect_pipeline_engine.json
python3 "$FSM_VALIDATOR" \
  spec/connect_pipeline_fsm.json
python3 "$FSM_TRACES" \
  spec/connect_pipeline_fsm.json spec/connect_pipeline_traces.json
python3 "$LOOP_VALIDATOR" \
  spec/connect_pipeline_loop.json
```

### Frozen M0 history, live M1 evidence and repository gate

```bash
: "${SYMPOSIUM_ROOT:?set SYMPOSIUM_ROOT to the SYMPOSIUM checkout}"
OOPTDD_LOOP_ROOT="${OOPTDD_LOOP_ROOT:-$(cd "$SYMPOSIUM_ROOT/.." && pwd)/ooptdd-loop}"
test -f "$OOPTDD_LOOP_ROOT/ooptdd_loop/cli.py"
export PYTHONPATH="$OOPTDD_LOOP_ROOT${PYTHONPATH:+:$PYTHONPATH}"
OOPTDD_LOOP=(.venv/bin/python -m ooptdd_loop.cli)
python3 - <<'PY'
import hashlib
from pathlib import Path

expected = "7e249d738e941c2a56e6d8846ddc2d5b6489c95a0238d5471301c63bea19c4d1"
actual = hashlib.sha256(Path("gates/scheduler_fairness.yaml").read_bytes()).hexdigest()
print(actual)
raise SystemExit(actual != expected)
PY
"${OOPTDD_LOOP[@]}" \
  validate-spec spec/omd_scheduler_m0_ooptdd.yaml --json
"${OOPTDD_LOOP[@]}" \
  validate-spec spec/omd_scheduler_m1_ooptdd.yaml --json
"${OOPTDD_LOOP[@]}" \
  run spec/omd_scheduler_m1_ooptdd.yaml --json
PYTHONDONTWRITEBYTECODE=1 .venv/bin/python -m pytest -q \
  -p no:cacheprovider \
  tests/test_scheduler_m0_harness.py \
  tests/test_scheduler_m1_admission.py \
  tests/test_scheduler_m1_outbox.py \
  tests/test_scheduler_admission_conformance.py \
  tests/test_sinks.py \
  tests/test_d2_reclaim.py \
  tests/test_ltdd_claim.py \
  tests/test_dogfood_parallel_dev.py \
  tests/test_multiagent_session.py \
  tests/test_d9_idempotency.py \
  tests/test_m1_connect_process_fencing.py \
  tests/test_m1_connect_effect_process.py
.venv/bin/python -m benchmarks.produce_scheduler_m1_receipt \
  --gate gates/scheduler_fairness.yaml \
  --cid omd-scheduler-m1-newer \
  --output evidence/omd_scheduler_m1/ooptdd_run.json \
  --receipt-output evidence/omd_scheduler_m1/ooptdd_receipt.json
.venv/bin/python "$SYMPOSIUM_ROOT/SKILLS/ooptdd-receipt/scripts/validate_receipt.py" \
  evidence/omd_scheduler_m1/ooptdd_receipt.json --verify-linked --root .
.venv/bin/python - <<'PY'
import hashlib, json
from pathlib import Path

run = json.loads(Path("evidence/omd_scheduler_m1/ooptdd_run.json").read_text())
receipt = json.loads(Path("evidence/omd_scheduler_m1/ooptdd_receipt.json").read_text())
subject = run["subject"]
assert receipt["subject_binding"] == subject
for name in ("admission", "candidate_index", "core", "disjoint"):
    path = Path(subject[f"{name}_path"])
    assert hashlib.sha256(path.read_bytes()).hexdigest() == subject[f"{name}_sha256"]
print("M1 subject binding: OK")
PY
git diff --check
```

### Full local and CI-equivalent core gates

```bash
PYTHONDONTWRITEBYTECODE=1 make verify
scripts/run_tlc.sh
```

`make verify` runs the complete Python suite and mandatory conformance check.
CI also runs the informational adoption harness and TLA+ job. Because CI may
skip private OOPTDD-dependent tests when that dependency is unavailable, the
explicit local OOPTDD validation remains required for an OOPTDD receipt claim.
The TLC smoke requires Java and, when the jar is not cached, `curl` plus network
access. Both TLC launchers pin `tla2tools` v1.7.4 and fail closed unless the jar
SHA-256 is
`936a262061c914694dfd669a543be24573c45d5aa0ff20a8b96b23d01e050e88`.

## Verification limits and honest status

- The engine, FSM and loop validators prove their individual schemas and static
  consistency. They do not prove that all four contracts project to one
  production implementation.
- The abstract trace runner still accepts fixture-provided guard results. A
  separate executable admission reducer now computes real nine-field identity,
  trusted-authority, queue sequence and replay guards and compares production
  decision projection; Connect payload guards remain abstract.
- A repository-wide cross-contract validator and full Connect
  model-to-production differential replay remain promotion blockers.
- Admission decisions and every implemented lifecycle path, including due
  `WAIT_TIMEOUT`, standalone wait `CANCEL`, renew/release, lease expiry and both
  owner-reclaim variants, are bound to current runtime code. MCP deadline
  delivery is default-on; repository capacity and typed overload are bound;
  content-addressed aging and dynamic rank-cycle resolution are implemented;
  the state-edge notification outbox has crash/replay and claim-token fencing;
  a conservative candidate prefilter is exact-verified and property-tested;
  embedded deadline ticks are default-on with explicit opt-out, weak-reference
  cleanup, terminal close/start linearization, constructor rollback and
  close-before-resign lifecycle tests. Under singleton enforcement a separate
  heartbeat worker remains live through sweep/notifier joins and refreshes the
  leader lease independently of slow sweep execution. Task-bound
  `CANCEL`/`RELEASE` projection is also implemented. The prepared Connect
  pipeline is still contract-only.
- M0's measured numbers and LakatoTree `partial` verdict describe reproducible
  evidence machinery only. They do not establish a fairness fix, throughput
  improvement, near-linear scaling, optimality or novel discovery.
- Protected-ref non-bypassability requires an enforced remote policy and sole
  publisher identity. A local fail-soft push topology is insufficient.

The honest current state is therefore: **M1 fairness runtime and admission
decision conformance implemented; full durable waiting, Connect runtime,
cross-contract proof and scientific promotion pending**.
