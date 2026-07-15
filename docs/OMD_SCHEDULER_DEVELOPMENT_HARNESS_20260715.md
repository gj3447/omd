# OMD scheduler development harness

Status: **M0.5 design and delivery contract only.** The machine-readable
development, admission, and connect contracts exist and pass their structural
validators. No M1 scheduler behavior, durable waiter, prepared connect runtime,
or protected-ref control plane is implemented by this document slice. Do not
describe it as an optimized scheduler, a production rollout, or a scientific
progress result.

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
| Cycle finalization | The edit lease is `RELEASED` or otherwise terminal and read back; a lease-only OMD task is canceled after release |

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

### Frozen M0 regression and repository gate

```bash
: "${SYMPOSIUM_ROOT:?set SYMPOSIUM_ROOT to the SYMPOSIUM checkout}"
OOPTDD_LOOP_BIN="${OOPTDD_LOOP_BIN:-$SYMPOSIUM_ROOT/PI/ooptdd-loop/.venv/bin/ooptdd-loop}"
test -x "$OOPTDD_LOOP_BIN"
python3 - <<'PY'
import hashlib
from pathlib import Path

expected = "7e249d738e941c2a56e6d8846ddc2d5b6489c95a0238d5471301c63bea19c4d1"
actual = hashlib.sha256(Path("gates/scheduler_fairness.yaml").read_bytes()).hexdigest()
print(actual)
raise SystemExit(actual != expected)
PY
"$OOPTDD_LOOP_BIN" \
  validate-spec spec/omd_scheduler_m0_ooptdd.yaml --json
PYTHONDONTWRITEBYTECODE=1 .venv/bin/python -m pytest -q \
  -p no:cacheprovider tests/test_scheduler_m0_harness.py
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
access. `scripts/run_tlc.sh` currently downloads a mutable latest release, so
that smoke is useful but is not pinned reproducible evidence; pinning the jar
version and SHA-256 is an M1 promotion requirement.

## 13. Current delivery gate versus future runtime closure

This M0.5 document/spec delivery is ready to commit only when:

- this document and the redesign document link to all eight machine-readable
  authorities;
- both engines, both FSMs, both trace sets and both loop contracts pass their
  individual validators;
- the frozen M1 gate hash is unchanged;
- the M0 harness regression remains green;
- `git diff --check` is green and the diff is limited to the declared OMD
  write-set;
- the branch is committed and published intentionally, then the OMD edit orbit
  is released and the lease-only coordination task is canceled/read back.

That delivery does **not** make an M1 implementation cycle `CLOSED`. A future
runtime slice additionally needs the new real-code behavior, frozen
positive/negative/restored receipts, implementation conformance, an independent
scripted judgment, authoritative landing readback and finalization receipts.

## 14. Known limitations and promotion blockers

1. `run_fsm_traces.py` currently consumes declared `guard_results`; it proves
   structural transition and true/false guard-outcome coverage, not that real
   payload comparison code computed those values.
2. A payload-driven guard runner for complete receipt identity, authority
   replay, same-tree/wrong-commit and finalization receipt checks is not yet
   implemented.
3. No repository cross-contract validator currently proves engine, admission
   FSM, connect FSM, both loop projections and production reducer conformance.
   Individual schema validators are necessary but insufficient.
4. No production admission reducer is bound to
   `spec/scheduler_admission_fsm.json`; M1 remains unimplemented.
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

Until these blockers are closed, the honest status is **contracts structurally
validated; runtime and scientific promotion pending**.
