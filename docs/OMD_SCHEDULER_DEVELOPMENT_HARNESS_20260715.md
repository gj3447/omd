# OMD scheduler development harness

Status: **M1 fair-admission runtime slice implemented; full M1 remains open.**
Initial claim and promotion now share one pure compatibility/rank/blocker
kernel, persist durable `queue_seq` and original TTL, prevent conflicting
PENDING overtaking, reject reservation-edge cycles, and bind canonical typed
decision payloads to the semantic admission FSM before projecting legacy Orbit
state. Finite deadlines and sweep/restart `WAIT_TIMEOUT` delivery are now in
the slice. Cancelling a task now atomically maps its associated PENDING row to
semantic `CANCEL` and its HELD row to semantic `RELEASE`, with restart repair
for legacy orphan rows. Default autonomous deadline delivery, a standalone
admission-wait cancel API, queue capacity/aging, notification outbox, candidate
indexing, the prepared Connect pipeline, and protected-ref control plane remain
unimplemented. Do not
describe this slice as the complete durable waiter, an optimized scheduler, a
production rollout, or a scientific progress result.

The runtime redesign and frozen M0 evidence are documented in
[`OMD_SCHEDULER_REDESIGN_20260715.md`](./OMD_SCHEDULER_REDESIGN_20260715.md).
This document defines the harness that must govern each later implementation
slice.

## 1. Layer boundary

Harness is a three-tier family. The four ICVC axes organize this development
harness instance; they do not redefine the family and they do not replace the
runtime orchestration model.

| Layer | Owned mechanism | Semantic authority |
|---|---|---|
| `L_IDE` development harness | Contract freeze, OMD edit lease, producer/reviewer separation, evidence freeze, judgment, landing, checkpoint and correction lifecycle | [`scheduler_development_loop.json`](../spec/scheduler_development_loop.json) |
| `L_RT` admission runtime | Deterministic grant/queue/deny decision, conflicting predecessor order, deadlines, cancellation, promotion and fencing | [`scheduler_admission_engine.json`](../spec/scheduler_admission_engine.json), [`scheduler_admission_fsm.json`](../spec/scheduler_admission_fsm.json), [`scheduler_admission_traces.json`](../spec/scheduler_admission_traces.json) |
| `L_RT` connect runtime | Candidate preparation, exact-identity validation, one-time publish authorization, expected-old ref CAS, readback reconciliation and finalization | [`connect_pipeline_engine.json`](../spec/connect_pipeline_engine.json), [`connect_pipeline_fsm.json`](../spec/connect_pipeline_fsm.json), [`connect_pipeline_traces.json`](../spec/connect_pipeline_traces.json), [`connect_pipeline_loop.json`](../spec/connect_pipeline_loop.json) |
| Legacy compatibility projection | Existing `Task` state remains an API projection; it is not the source of truth for an admission request or connect attempt | `SERVER_SPEC.md`, `CONCURRENCY.md` |

The `L_IDE` lifecycle is a directed graph with a producer-reviewer pattern.
The `L_RT` admission and connect lifecycles are deterministic state machines
around SQLite/WAL authority and typed effect ports. MCP and CLI are adapters;
neither is an ICVC axis or a second state authority.

## 2. Scope and non-goals

This harness owns one preregistered scheduler development slice at a time. It
owns the order and evidence needed to move that slice from proposal to an
authoritatively landed result. It does not own the scheduler's product policy or
the runtime's mutable state.

In scope:

- freeze a slice objective, falsifiers, gate hashes and preregistration before
  implementation evidence is accepted;
- require a current OMD `HELD` write-set lease before repository edits;
- keep producer, observer, contract validator, progress judge, publisher and
  ref reader as capability-distinct roles;
- preserve content-addressed positive, injected-negative and restored-positive
  receipts;
- reconcile an ambiguous publication before honoring a late cancellation,
  timeout or budget interrupt;
- make every retry, suspension, repair and resume path explicit.

Out of scope for M0.5:

- changing `Coordinator.claim`, `_promote_pending`, `connect`, `core.py`,
  `store.py` or `gitio.py`;
- claiming that the M1 no-overtaking gate is green;
- claiming throughput improvement, linear scaling, global optimality or novel
  empirical discovery;
- treating structural JSON validation as model-to-production conformance;
- granting the producer authority to judge, approve or publish its own output.

## 3. Authorities, actors and capabilities

| Actor | Capability | Must not do |
|---|---|---|
| Cycle controller | Advance the frozen `L_IDE` graph, account budgets, checkpoint, dispatch bounded work | Hand-enter a scientific verdict or skip a required state |
| OMD `LeaseAuthority` | Grant, read back, fence, renew and release the exact edit write-set | Infer authority from producer prose or a stale caller-echoed fence |
| Implementer | Edit only paths covered by the current `HELD` lease; run producer tests | Modify a frozen gate, preregistration, judge, verdict or protected ref |
| OOPTDD observer | Capture real-code positive, injected-negative and restored-positive evidence | Implement the slice or convert missing telemetry into success |
| Contract validator | Read-only engine/FSM/trace/loop, projection and implementation conformance | Edit the implementation or accept schema validity as runtime equivalence |
| Independent progress judge | Recompute preregistered metrics from frozen evidence and emit a scripted verdict | Modify evidence, implement runtime code or accept a hand-entered verdict |
| Protected-ref policy authority | Issue one-time authorization bound to the exact action and evidence tuple | Authorize a different base, tree, destination, generation or expired action |
| `IntegrationPublisher` | Consume an already-durable publish effect and perform the configured expected-old ref CAS | Validate its own result, mint its own authorization or blindly retry an unknown outcome |
| Independent ref reader | Read the authoritative commit and resolved tree after the publisher reports | Trust publisher success without ref and tree readback |
| Repair operator | Classify `UNKNOWN_OUTCOME` or broken provenance using preserved evidence | Destructively reset published history or invent a successful receipt |

SQLite/WAL remains the single mutable authority for admission requests, leases
and connect attempts. The only capability allowed to update the configured
integration ref is `IntegrationPublisher`. A validation worker never receives
that capability.

## 4. L_IDE development lifecycle

The complete semantic source is
[`spec/scheduler_development_loop.json`](../spec/scheduler_development_loop.json). The
main path is:

```text
DRAFT
  -> CONTRACT_LOCKED
  -> LEASE_PENDING -> LEASE_HELD
  -> BASELINED
  -> IMPLEMENTING
  -> PRODUCER_VERIFIED
  -> EVIDENCE_FROZEN
  -> CONTRACT_VALIDATED
  -> JUDGING
  -> PROMOTABLE
  -> CONNECT_QUEUED
  -> PUBLISHING
  -> RECONCILING
  -> FINALIZING
  -> CLOSED
```

The following are first-class paths, not prose exceptions:

- `REWORK`: the same frozen gate remains authoritative while a new
  implementation generation is produced;
- `SUSPEND_RELEASING` / `SUSPENDED`: a pre-publication approval failure first
  stops editing and releases any `HELD` lease with an authoritative receipt;
  other lease or external blockers retain a durable checkpoint and no edit
  authority;
- `AWAITING_JUDGE`: the independent judge is unavailable; no substitute verdict
  is inferred;
- `UNKNOWN_OUTCOME`: publication cannot yet be classified and must be read back;
- `RETRY_EXHAUSTED`: before publication, three identical progress fingerprints
  occurred without a new durable receipt or authority observation; unresolved
  post-publication effects enter runtime `NEEDS_REPAIR`, while the bounded
  `L_IDE` runner uses evidence-preserving `ABANDON -> FINALIZING ->
  FAILED_PERMANENT` and leaves the legacy Task `CONNECTING` fail-stop;
- `NEEDS_REPAIR`: checkpoint integrity, provenance, projection or publication
  evidence cannot be reconciled safely;
- `FAILED_PERMANENT`, `BUDGET_EXHAUSTED`, `TIMED_OUT`, `CANCELED`: typed runner
  outcomes distinct from success. Their receipt discriminators determine
  whether the runtime Task is `DONE`, `MERGED`, or remains `CONNECTING`.

`WAIT_HUMAN` is represented by the suspended and repair paths, not by silently
terminating the cycle. A cycle is `CLOSED` only when all of the following refer
to the same frozen contract and generation:

1. producer verification is green;
2. positive, negative and restored evidence is content-addressed and frozen;
3. a read-only contract validator accepts model, projection and implementation
   conformance;
4. an identity-distinct scripted judge accepts the preregistered evidence;
5. the protected-ref action is authorized once for the exact artifact;
6. authoritative readback equals `candidate_commit_sha` and that commit resolves
   to `candidate_tree_sha`;
7. the connect finalization receipt is valid;
8. the OMD edit lease is released and the pending-interrupt ledger is empty.

Producer self-report, a passing JSON schema validator, or a publisher success
response cannot satisfy this predicate alone.

## 5. ICVC inside the L_IDE instance

| Axis | Concrete structure | Failure it prevents |
|---|---|---|
| **Inform** | Cycle manifest binds source commit, contract and gate hashes, lease facts, role identities, exact commands, environment digest and artifact paths | Coding against an unknown base or accepting unbound evidence |
| **Constrain** | Typed phase graph, `HELD` write-set fencing, capability separation, immutable preregistration, bounded budgets and protected-ref policy | Editing while merely `PENDING`, unsafe phase skips, producer self-approval and unbounded retry |
| **Verify** | Producer tests followed by independent contract validation, frozen OOPTDD positive/negative/restored receipts, scripted judgment and authoritative commit/tree readback | Rubber-stamp approval and publisher self-attestation |
| **Correct** | `REWORK`, `SUSPENDED`, `AWAITING_JUDGE`, `RETRY_EXHAUSTED`, `UNKNOWN_OUTCOME` and `NEEDS_REPAIR` preserve evidence and require a fenced resume | Repeating the same failed generation, losing an interrupt or hiding an ambiguous effect |

These axes govern the development instance. The runtime admission mechanism is
still a rank/blocker decision table and FSM; the runtime connect mechanism is
still an intent/effect/reconciliation FSM.

## 6. Budgets and no-progress control

The versioned fail-safe envelope is not a performance target:

| Budget | Maximum |
|---|---:|
| Steps | 96 |
| Tool calls, including descendants | 256 |
| Retries per transition | 3 |
| Wall time | 28,800 seconds |
| Suspended time | 86,400 seconds |
| Tokens | 200,000 |
| Cost units | 500 |
| Recursion depth | 1 |
| Parallelism | 4 |

At 80% of the envelope the controller stops starting new verification work and
checkpoints for handoff. The external budget controller owns the hard boundary.

The no-progress fingerprint contains cycle state, implementation diff hash,
gate hash, error class and authoritative lease or ref observation. Durable state
movement, a changed diff under the same gate, a new receipt or a changed
authority observation is progress. Three consecutive identical fingerprints
without one of those gains terminate in `RETRY_EXHAUSTED`; changing irrelevant
prose does not reset the counter.

## 7. Checkpoint, resume and replay

Checkpoint at every state transition, before every external effect, after every
receipt and before delegation handoff. Persist at least:

- cycle, slice, generation and checkpoint sequence;
- workflow and state schema versions;
- base commit and all contract/gate hashes;
- current OMD lease receipt, write-set digest, fence-set digest and bail epoch;
- implementation diff and artifact manifest;
- role identities, budgets, retries, pending effects and pending interrupts;
- candidate commit/tree, evidence, judge and publish receipt hashes.

Resume only when the workflow schema, state schema, base commit, contract hashes
and artifact hashes are compatible. Editing requires a freshly read back
`HELD` fence; a stale generation is read-only evidence. A contract or base change
creates an explicit superseding generation and never rewrites a frozen bundle.

The three replay terms are distinct:

- **resume** continues from a compatible durable checkpoint under fresh
  authority;
- **trajectory replay** reruns the frozen contract and gates against a new
  implementation generation and may produce a different artifact;
- **deterministic replay** additionally freezes sources, policies, tools,
  clock/ID inputs and external responses in a read-only sandbox.

An ordinary checkpoint resume must not be called deterministic replay.

## 8. Interrupt and publication protocol

Cancellation, timeout and budget exhaustion apply immediately to safe
nonterminal states. They are deferred after the effect-critical path begins:

```text
CONNECT_QUEUED -> PUBLISHING -> RECONCILING
               -> UNKNOWN_OUTCOME -> FINALIZING
```

In those states the interrupt is recorded durably. The publisher effect is not
abandoned: the authoritative ref is read, the attempt is finalized, and only
then is the interrupt reported. A late interrupt never rewinds a landed ref or
projects a published Task away from `MERGED`.

The current connect FSM has no `PUBLISHED` state. Publication is an externally
visible effect that is not trusted until independent readback. The current
sequence is:

```text
VALIDATED
  -> PUBLISH_INTENT       # one-time authority consumed and intent durable
  -> PUBLISHING           # expected-old/new-commit CAS dispatched
  -> RECONCILING          # publisher response is only a claim
  -> FINALIZING           # commit and tree independently read back
  -> FINALIZED | FINALIZED_CANCELED | FINALIZED_TIMED_OUT
               | FINALIZED_BUDGET_EXHAUSTED
```

`CAS_REPORTED_UNKNOWN` also enters reconciliation. Readback of the expected old
SHA invalidates the authorization and returns to `VALIDATED`; readback of the
new commit and tree proceeds to finalization; any other or unreadable ref enters
`NEEDS_REPAIR` or bounded `UNKNOWN_OUTCOME`. Blind CAS retry and destructive
reset are forbidden. Exhausting bounded reconciliation does not recast an
ambiguous external effect as a retry failure: it preserves the effect identity,
pending interrupts and repair checkpoint in `NEEDS_REPAIR`.

The green receipt and publish authorization bind the full identity tuple:

The canonical one-time envelope is `publish_authorization/v1`. It first binds
generic action/artifact/destination, visibility, scope, actor, expiry, nonce and
rationale fields, plus a `development_context_hash` over the cycle, slice,
generation, base, contract, evidence and judge receipt. It then binds:

```text
attempt_id, repo_id, integration_ref,
owner_agent, bail_epoch, coordinator_epoch,
authority_snapshot_hash, approval_hash,
base_sha, task_tip_sha,
candidate_commit_sha, candidate_tree_sha,
write_set_digest, fence_set_digest, check_policy_digest,
environment_digest, attempt_generation, receipt_hash,
expected_old_sha, new_commit_sha, publish_effect_id
```

A same-tree but wrong-commit readback is not success.

## 9. Projections between layers

### 9.1 Development cycle to admission runtime

| L_IDE state/evidence | L_RT admission fact |
|---|---|
| `LEASE_PENDING` | An `OrbitRequest` is `REQUESTED` or `PENDING`; edits are forbidden |
| `LEASE_HELD` | The request is `HELD` with current owner, full fence set, bail epoch, expiry and write-set digest |
| `SUSPEND_RELEASING` | Editing is stopped; admission remains `HELD` only until the bound release effect is acknowledged |
| `SUSPENDED` | L_IDE `lease_status` is `pending`, `released`, or `lost`; `lost` is a local discriminator, not an admission state. Admission readback is `PENDING` or an authoritative FSM-valid non-`HELD` outcome such as `RELEASED`, `EXPIRED`, `DENIED`, `CANCELLED`, or `TIMED_OUT`; a fresh request and generation are required |
| Cycle finalization | The edit lease is `RELEASED` or otherwise terminal and read back; a lease-only OMD coordination task row is canceled after release as harness cleanup, not as admission-wait cancellation |

Initial admission and later promotion must use the same compatibility,
predecessor, rank, cycle and capacity contract.

### 9.2 Development cycle to connect runtime

| L_IDE state | L_RT connect fact |
|---|---|
| `PROMOTABLE` | Development evidence and scripted judgment are acceptable; no ref mutation is yet authorized |
| `CONNECT_QUEUED` | Exact one-time publication authorization is being bound to a `VALIDATED` attempt |
| `PUBLISHING` | Connect attempt is `PUBLISH_INTENT` or `PUBLISHING` |
| `RECONCILING` | Connect attempt is `RECONCILING` and an independent reader owns classification |
| `UNKNOWN_OUTCOME` | Connect attempt is `UNKNOWN_OUTCOME`; success is unreachable without readback |
| `FINALIZING` | Connect attempt is `FINALIZING`; Task/lease/outbox facts are committed from publication evidence |
| `CLOSED` | Connect attempt is `FINALIZED`, exact commit/tree readback and finalization receipt are valid, and the edit lease is released |
| Interrupt after publish intent | Connect attempt may be `FINALIZED_CANCELED`, `FINALIZED_TIMED_OUT` or `FINALIZED_BUDGET_EXHAUSTED`; landed history remains published while the L_IDE cycle reports the typed interrupt |
| `FAILED_PERMANENT` with `repair_required=true` | The bounded development runner has stopped, but the connect attempt is `NEEDS_REPAIR` and the Task remains `CONNECTING` until authoritative repair |

### 9.3 Connect attempt to legacy Task

The existing Task enum is a compatibility view, not the connect state source of
truth:

| ConnectAttempt | Legacy Task projection |
|---|---|
| `QUEUED`, `PREPARING`, `VALIDATING`, `VALIDATED` | `DONE` |
| `PUBLISH_INTENT`, `PUBLISHING`, `RECONCILING`, `UNKNOWN_OUTCOME`, `FINALIZING` | `CONNECTING` |
| `FINALIZED`, `FINALIZED_CANCELED`, `FINALIZED_TIMED_OUT`, or `FINALIZED_BUDGET_EXHAUSTED` after confirmed publication | `MERGED` |
| Pre-intent `RED`, `FAILED`, `CANCELLED`, `TIMED_OUT`, `BUDGET_EXHAUSTED` | `DONE` with a typed retry/new-generation or terminal reason |
| Ambiguous post-intent outcome or `NEEDS_REPAIR` | `CONNECTING` fail-stop until authoritative repair |

This projection must be checked explicitly. Similar state names across the
development loop, connect loop and connect FSM are not proof of conformance.

## 10. M1 contract, falsifiers and evidence

M1 changes runtime admission only after its contract and evidence plan are
frozen. The required admission rule is:

```text
grant(R) iff
  no incompatible overlapping HELD blocker exists
  and no higher-ranked conflicting PENDING predecessor exists
```

Rank is effective priority descending and then durable queue sequence ascending.
Precedence exists only between conflicting, mode-incompatible requests. A global
queue head must not block disjoint work.

Required M1 evidence:

1. **positive no-overtaking:** a later narrow claimant remains `PENDING` behind
   the older broad conflicting waiter;
2. **injected negative:** bypassing the predecessor reservation makes the same
   frozen gate RED;
3. **restored positive:** restoring the rule makes the unchanged gate green;
4. **liveness:** after blockers release or expire, the oldest eligible waiter
   becomes `HELD`;
5. **disjoint regression:** unrelated compatible work remains immediately
   grantable;
6. **bounded wait:** every pending request reaches `HELD`, `TIMED_OUT`,
   `CANCELLED`, `DENIED` or explicit overload;
7. **cycle safety:** adding reservation precedence cannot create an undetected
   wait-for cycle;
8. **restart:** queue sequence, policy envelope, deadline and blocker semantics
   survive authority restart;
9. **index soundness:** every conflict found by the exact full scan is contained
   in the candidate index result.

M1 is falsified if any newer lower-ranked conflicting request becomes `HELD`
while its predecessor remains `PENDING`, if a disjoint request is blocked by an
unrelated global head, if an indexed scan misses an exact conflict, if a bounded
wait has no typed resolution, or if reservation precedence creates an
undetected cycle.

The frozen M1 gate is `gates/scheduler_fairness.yaml`. Its M0-registered SHA-256
is:

```text
7e249d738e941c2a56e6d8846ddc2d5b6489c95a0238d5471301c63bea19c4d1
```

M1 must make that same gate green. Editing the gate to fit implementation output
invalidates the downstream evidence.

### 10.1 Implemented M1 fairness slice

The current runtime closes evidence items 1--5 and 7 for the bounded real-code
trace, and the queue-order part of item 8:

- `omd_server/admission.py` is the pure mode compatibility, exact-overlap,
  priority/FIFO rank and blocker decision table used by both `claim()` and
  `_promote_pending()`;
- SQLite persists monotonic `queue_seq`, requested TTL, policy version, path
  digest, request id/generation, bail epoch, enqueue/deadline timestamps,
  authority snapshot, canonical decision id/type and blockers. Legacy PENDING
  rows receive the reconstructable request/rank/deadline fields through a
  deterministic one-time backfill ordered by `(created_at, orbit_id)`; the next
  reconciliation records current decision metadata. `created_at` is not the M1
  ordering authority;
- the combined wait-for graph includes HELD-owner and higher-ranked conflicting
  PENDING-reservation edges, so a new reservation cycle is denied before it is
  exposed;
- `omd_server/admission_contract.py` loads the JSON FSM transition, context
  update and effect bindings, computes nine-field identity, trusted authority,
  queue-sequence and replay guards from typed payloads, and verifies the
  one-way legacy projection;
- reconciliation expires HELD authority, reclaims stale owners, delivers due
  `WAIT_TIMEOUT` decisions, and only then considers promotion. The same order
  runs on sweep and restart, so an overdue waiter cannot be promoted;
- live admission identities own the global request-id namespace, and terminal
  rows preserve their generation history; policy-denial retry advances the
  durable request generation, while exact completed claim replay cannot create
  a second generation-zero effect;
- split-phase Connect and barrier trips reserve their exact request envelope
  across unlocked effects. Deterministically ordered DB- and repo-scoped
  process/file effect locks span
  reservation, Phase A/B/C and response finalization; Git/check children inherit
  its descriptor. Task, merge-token and idempotency rows bind the same immutable
  attempt id, process instance and monotonic owner generation, and Phase C
  compare-and-sets that full authority tuple before release or finalization;
- real subprocess crash-cut tests prove that the repo domain serializes two
  database authorities (including main/linked-worktree aliases via Git common
  directory) and that inherited Git/checker descriptors keep both locks held
  after the Coordinator parent is killed;
- public `release` is restricted to path orbits: internal merge tokens cannot
  be released through the lease API, and an unresolved `CONNECTING` generation
  is non-reentrant for both direct and barrier-driven Connect;
- recovery can take over only after acquiring that effect lock. Repo-bound
  success additionally requires the recorded merge SHA to match the exact
  attempt trailer read back from the integration branch; explicit DB-only mode
  permits `merge_sha=null`. A second live Coordinator therefore skips recovery
  rather than stealing a peer's CONNECTING state, token, pin or INFLIGHT row;
- Phase A audits exact candidate/base SHAs and fails closed on Git read errors;
  Phase B merges the immutable candidate, while Phase C and restart proof also
  require candidate ancestry. Integration-base drift is a clean retry for both
  direct and barrier Connect, not an unprovable rollback;
- schema initialization uses a durable version marker written only after
  schema migration and WAL activation. Missing-marker migration requires the
  same-DB effect fence plus a leader generation even on leadership-disabled
  surfaces; current schema performs no migration before leader admission and
  unknown versions are rejected read-only;
- a task-bound claim checks task admission eligibility before transport-cache or
  live-orbit replay, so cancel/merge cannot replay an old HELD fence. Barrier
  trip checks its generation and `TRIPPING` state before every member effect and
  final success, preventing a concurrent abort from yielding `ok:true/BROKEN`;
- `benchmarks/produce_scheduler_m1_receipt.py` runs the unchanged frozen gate
  as positive, a test-only pure-kernel predecessor bypass as RED, and the
  restored runtime as green. The bypass is not a Coordinator/MCP/CLI option;
- `spec/omd_admission.tla` models fair admission, compatible modes, durable
  order, unique fencing, no incompatible overlap, no lower-ranked overtaking
  and eventual resolution under release/promotion fairness assumptions.

The M1 receipt is honestly tiered `arrived`: producer emission and readback use
the same in-memory backend, `oracle.separate_source=false`, and the receipt
remains `AWAITING_INDEPENDENT_JUDGE`. It is execution evidence, not an
independent scientific judgment.

This does **not** close evidence item 6. New PENDING rows persist a finite,
typed `wait_deadline`, and sweep/restart reconciliation delivers the semantic
timeout transition. A standalone `cancel_wait` operation now authenticates the
PENDING owner, request generation and bail epoch, projects semantic `CANCELLED`
to legacy `DENIED`, and reconciles eligible promotion once. Embedded
`Coordinator` instances remain inline-only unless periodic sweep is explicitly
enabled. The MCP server starts a 1-second sweep by default inside its lifespan
(`OMD_SWEEP_INTERVAL=0` is the explicit opt-out), stops and joins it before
leader handoff, and therefore delivers idle wait deadlines without a foreground
verb. Capacity/overload is still absent, so the complete bounded admission
contract is not yet established. Task cancellation also terminalizes its own
PENDING/HELD rows.
Item 9 has no
candidate index to prove yet (the runtime uses the sound full exact scan).
Policy-denial generation rollover is implemented; explicit non-denial rollover
and public maintenance events `RENEW`, `RELEASE` and reclaim are not yet routed
through the semantic reducer. Standalone wait cancellation and the task-bound
cancel path are the narrow exceptions: they project `CANCEL` (and task-bound
`RELEASE`) before legacy mutation. The attempt fencing above hardens the existing Connect
implementation; it does not implement the prepared
`ConnectAttempt`/expected-old protected-ref publication pipeline.

## 11. Evidence, judgment and landing chain

For each runtime slice, retain this order and bind every arrow by hashes:

```text
contract/gate/preregistration freeze
  -> OMD HELD lease readback
  -> real-code baseline and locked negative input
  -> implementation diff + producer tests
  -> positive / injected-negative / restored evidence freeze
  -> read-only cross-contract and implementation validation
  -> independent scripted progress judgment
  -> one-time exact-action publication authorization
  -> durable publish intent
  -> publisher attempt
  -> independent authoritative commit + tree readback
  -> finalization receipt
  -> lease release and pending-interrupt reconciliation
  -> CLOSED
```

The evidence record itself contains no verdict. The judge recomputes the locked
metric, verifies provenance and hashes, optionally performs a fresh replay, and
then emits a scripted receipt. A missing judge remains `AWAITING_JUDGE`; it is
not a reason for the producer or controller to supply a verdict.

M0 examples of the required durable shapes live under
`evidence/omd_scheduler_m0/`: `preregistration.json`, `ooptdd_receipt.json`,
`evidence.json`, `judge-response.json`, `judgment-packet.json`,
`receipt-chain.json`, `verification-summary.json` and `pi-cycle.json`.

## 12. Verification commands

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
  tests/test_scheduler_admission_conformance.py \
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
for name in ("admission", "core"):
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

`make verify` runs the full Python suite plus mandatory conformance. CI also
runs the informational adoption harness and TLA+ model job. The private OOPTDD
dependency may be absent and skipped in CI, so the explicit local OOPTDD command
above remains a delivery gate when claiming its receipt path was exercised.
The TLC smoke requires Java and, when the jar is not cached, `curl` plus network
access. Both TLC launchers now pin `tla2tools` v1.7.4 and verify SHA-256
`936a262061c914694dfd669a543be24573c45d5aa0ff20a8b96b23d01e050e88`
before executing any model.

## 13. Current delivery gate versus future runtime closure

The historical M0.5 contract delivery is complete. The current M1 fairness
slice is ready to commit only when:

- admission engine/FSM/trace validators and the payload-driven production
  projection suite pass;
- the frozen M1 gate hash is unchanged;
- the same gate is green, RED under the test-only predecessor bypass and green
  again after restoration;
- fair ordering, liveness, disjoint progress, reservation-cycle, restart,
  legacy migration and request-id conflict tests pass;
- the historical M0 evidence hashes remain unchanged and its harness treats
  that receipt as pinned history rather than rerunning the obsolete defect gate;
- `git diff --check` is green and the diff is limited to the declared OMD
  write-set;
- the branch is committed and published intentionally, then the OMD edit orbit
  is released and the lease-only coordination task row is canceled/read back as
  harness cleanup. Admission wait cancellation is a separate `cancel_wait`
  capability bound to the PENDING row's owner, generation and bail epoch.

Landing those files makes the **M1 fairness implementation slice** durable. It
does not make full M1 or the development cycle `CLOSED`: embedded-runtime
autonomous delivery, overload/aging, candidate-index soundness,
maintenance-event reducer binding, an independent scripted progress judgment
and finalization receipts remain future work.

## 14. Known limitations and promotion blockers

1. `run_fsm_traces.py` still consumes declared `guard_results`; it proves
   abstract structural coverage. Admission decisions now have a separate real
   payload reducer/conformance suite, but Connect receipt and finalization guards
   do not.
2. Admission payload guards cover decision events and production projection.
   The remaining lifecycle maintenance events, Connect same-tree/wrong-commit,
   authorization and finalization receipts are not yet executable end to end.
3. No repository cross-contract validator currently proves engine, admission
   FSM, connect FSM, both loop projections and production reducer conformance.
   Individual schema validators are necessary but insufficient.
4. Production admission/grant/queue/promotion/denial, due-timeout and standalone
   wait-cancellation decisions are bound to the JSON semantic reducer. MCP
   timeout delivery is autonomous by default; embedded-runtime delivery,
   overload, aging, notification outbox and the
   remaining maintenance-event bindings remain the open M1 front. Task-bound
   `CANCEL`/`RELEASE` projection is also implemented.
5. The prepared candidate, expected-old ref CAS, independent ref reader and
   finalization protocol are contracts, not the current runtime path.
6. The connect loop is a conservative `loop-contract/v1` runner aggregate over
   the connect FSM SSOT. It intentionally folds detailed `REF_READ_OLD` and
   `REF_READ_OTHER` branches into receipt-or-abandon control outcomes while
   preserving `publication_outcome`, `publication_committed` and
   `failure_class`. Its folded `RECEIPT_CONFIRMED` transition is outcome-neutral
   and never dispatches publication finalization: the FSM already finalized NEW
   or invalidated authorization for OLD. Executable projection conformance
   remains required before production promotion; matching names alone prove
   nothing.
7. M0 measurements remain a descriptive baseline. They are not evidence of a
   behavior or throughput improvement, and M0's LakatoTree `partial` verdict
   applies to reproducible evidence machinery only.
8. Protected-ref non-bypassability requires a real remote policy and sole
   publisher identity. A local fail-soft push topology cannot establish it.

Until these blockers are closed, the honest status is **M1 fairness runtime and
decision-payload conformance implemented; full durable waiting, Connect runtime,
cross-contract proof and scientific promotion pending**.
