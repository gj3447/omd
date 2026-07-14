# OMD Engine v2

Status: `0.1.0a1`, experimental, additive, and isolated from the legacy
`Coordinator`. No active v1 lease or task is migrated into v2. The canary is
authoritative only inside its local stdio + SQLite-file trust boundary.

## Why v2 starts with a new kernel

The production prototype proved the core idea—server-authoritative resource
leases, fencing, queueing, and Git integration—but it also coupled those
concerns in one coordinator. In particular, optional mutation fences, raw path
identity, mutable read APIs, generic storage setters, and split-phase Git work
left safety dependent on call ordering.

V2 uses a functional core and an imperative shell:

```text
raw request
  -> canonical resource ingress
  -> decide(state, command, now)
  -> domain events + effect intentions
  -> SQLite BEGIN IMMEDIATE / revision CAS
  -> state + events + idempotency + outbox commit
  -> future effect dispatcher after commit (absent in this milestone)
```

The first milestone contains lease coordination only. Git integration, task
lifecycle, semaphores, barriers, and v1 compatibility are not hidden behind
flags; they are absent from the lease-only type and MCP surface.

## Provenance and design lineage

The v2 branch starts from the exact DellTower OMD development commit:

- OMD `18de80f8eb2af254b0450e7fde0f843da0011d7f`
- source branch `feat/omd-develop-queue-q1-q9`
- local working branch `codex/omd-v2-kernel`

The following DellTower checkouts were inspected as design references. No
source was copied from them.

| Reference | Commit | Mechanism used |
|---|---:|---|
| XState `packages/core/src/transition.ts` | `229d976c1496` | Pure snapshot/event transition returning executable actions without running them |
| Temporal `service/history/hsm/*` | `ad9949520c8e` | State transition output separated from durable tasks |
| Temporal `service/matching/fairness.md` | `ad9949520c8e` | Stable ordering, no-barging, pinned progress, and fencing failure analysis |
| SQLite `src/wal.c`, `test/wal*.test` | `02ea41d5241e` | One writer, WAL snapshots, busy-snapshot behavior, crash/fault tests |
| Git `git-update-ref.adoc`, `git-merge-tree.adoc` | `e9019fcafe00` | Expected-old ref CAS and worktree-free merge-tree protocol for the future Git saga |
| LangGraph checkpoint conformance | `bdb323ef` | Persistence conformance shape only; its shared-connection/finally-commit wrapper was rejected |

Git is GPLv2. The future integration adapter will invoke Git as a CLI and will
not copy Git implementation code.

## Decision areas

| Area | V2 decision |
|---|---|
| AST/data types | Frozen, slotted dataclasses and closed enum/union variants |
| Workflow | `decide -> evolve -> atomic commit -> durable outbox`; dispatcher is a future milestone |
| Design pattern | Functional core / imperative shell; event projection; transactional outbox |
| Project structure | Independent `omd_server.v2` subpackage; legacy imports forbidden in lease profile |
| Data flow | Canonical command envelope to deterministic events and revision-coherent receipt projection |
| Algorithm | Conflict-local FIFO/no-barging admission; no global head-of-line blocking |
| Store | File-backed local SQLite WAL, per-operation connection, `BEGIN IMMEDIATE`, revision CAS |
| Class design | Composition around `LeaseService`; no boolean profile combinations in the kernel |

## Typed boundary

Every mutation is wrapped in a `CommandEnvelope` with:

- protocol version;
- coordination domain;
- exact principal `(client_id, agent_id, session_epoch)`;
- request ID;
- typed command;
- a server-computed canonical SHA-256 command fingerprint.

The MCP tools accept neither principal fields nor the fingerprint. At process
startup the store atomically increments a normalized
`(domain, client, agent) -> current_session_epoch` registry and binds the issued
principal to that stdio transport. An older process receives `STALE_SESSION`
after a newer session with the same configured client/agent identity starts.

Reusing `(client_id, request_id)` with a different command, agent, or session
fails with `IDEMPOTENCY_KEY_REUSE` once the session itself is valid. Successful
replay emits no new operation/idempotency event and projects the operation's
current state. Due deadline transitions may still be emitted before that
projection. A malformed mutation that reaches `LeaseService` is reduced to a
canonical wire digest and durably rejected through the same idempotency path.
JSON-RPC/schema failures rejected by the transport before application dispatch
are outside that claim.

The public request-ID namespace is the whole `(domain_id, client_id)` pair; it
does not reset for a new agent or session. An identical replay therefore
requires the same current principal/session and fingerprint. Stable clients
must never recycle old request IDs across restart, and an explicitly shared
client ID must coordinate globally unique request IDs across all callers.

Frozen contracts deep-copy caller collections before fingerprinting, and the
kernel recomputes the semantic fingerprint before execution. Mutable input
cannot change the admitted resource after the envelope is hashed.

## Canonical resources

```text
ResourceId = (
  coordination domain,
  stable repository ID,
  NFC/case-policy-normalized POSIX segments,
  EXACT | SUBTREE
)
```

Rules:

- only relative POSIX paths are accepted;
- absolute paths, NUL, backslashes, empty segments, `.`, and `..` are rejected;
- dotfiles are preserved (`.env != env`);
- repository case policy and Unicode NFC are applied once at ingress;
- registered symlink prefixes are rejected rather than resolved through;
- arbitrary glob grammar is not part of v2; subtree selection is explicit;
- overlap compares path segments, so `src/a` is not a prefix of `src/ab`.

Repository IDs are registry identities, not mutable branch names or realpath
hashes.

## Claim-set admission and fencing

A claim-set is a single atomic unit. Internal overlapping selectors are
rejected, and partial grants do not exist.

For a pending candidate `p`:

```text
grant(p) iff
  no ACTIVE claim conflicts with p
  and no earlier live PENDING claim conflicts with p
```

This blocks a new reader from overtaking a queued writer while still allowing
later requests on disjoint resources to proceed.

Every grant receives a `FenceVector` containing the claim ID, exact principal,
the complete canonical resource set, a shared global grant epoch, and an
integrity digest. Renew and release compare the entire vector with current
state. A scalar epoch, missing entry, changed owner/session, or mixed old/new
vector is insufficient.

Before every valid mutation attempt—including replay and idempotency-key reuse
rejection—the same write transaction first expires due leases, times out due
waiters, and promotes eligible claims. Thus neither renewal nor replay at or
after a deadline can win a race between periodic maintenance ticks. The runtime
supervisor also issues internal `MaintenanceTick` commands so expiry and
promotion proceed while clients are otherwise idle.

Session rollover is itself a serialized maintenance boundary. A pending claim
owned by the superseded epoch becomes `FENCED`; an already-active lease remains
valid only until its normal TTL. In the same transaction, other due waiters are
timed out and newly eligible waiters are promoted, so a multi-resource stale
waiter cannot poison an unrelated queue.

## Executable invariants

1. Canonical identity: one logical path has one `ResourceId` under a policy.
2. No conflicting active claim-sets.
3. All-or-none claim-set activation.
4. Active FenceVector resources exactly equal claim resources.
5. Global grant epochs are unique and strictly advance.
6. Principal ownership includes client, agent, and session epoch.
7. No later grant overtakes an earlier live conflicting pending claim.
8. One idempotency key binds one command fingerprint and operation.
9. Replay creates no new operation/idempotency event; due maintenance may
   commit before the current projection is returned.
10. Every request persists its enqueue time, requested lease TTL, requested
    wait timeout, and their exact derived deadlines.
11. Committed time cannot regress.
12. `decide` does not mutate state or perform I/O.
13. Projection, event log, idempotency key, and outbox are one transaction.
14. Effects are intentions only until their authoritative events commit.
15. Every claim/resource belongs to the aggregate domain and a registered repo.
16. Every event follows the explicit legal claim-state transition table.
17. Every persisted claim retains its originating idempotency binding.
18. Grant, renew, release, expiry, timeout, and session-fence events obey their
    exact deadline/session authority at the transition point.

`assert_invariants()` runs after event reduction and whenever a SQLite snapshot
is loaded. The loader also checks the row codec, row/state domain identity,
session registry, and normalized idempotency projection. Corruption in those
checked classes is quarantined by exception rather than repaired by guessing.
Idempotency event records are decoded and compared semantically with their
normalized projection; however, v2 does not yet decode/replay the full domain
event log to rebuild state and does not claim complete corruption detection or
automatic repair.

## SQLite transaction contract

The store is deliberately local-only and file-backed:

- WAL mode must be active;
- each read or command opens and closes its own connection;
- reads use a consistent deferred transaction and never run maintenance;
- commands acquire `BEGIN IMMEDIATE` before loading the aggregate;
- command and session-registration clocks are sampled only after the writer
  lock, then clamped to the last committed logical time;
- a due supervisor observation is carried into its transaction as a monotonic
  lower bound, so wall-clock rollback cannot idempotently poison a deadline tick;
- every aggregate command update uses
  `UPDATE ... WHERE revision = expected RETURNING revision`;
- schema `CHECK`, primary-key, and foreign-key constraints fail closed;
- idempotency has a normalized `(domain, client, request)` primary key;
- session epochs use an atomic normalized `(domain, client, agent)`
  upsert/increment row;
- rollover maintenance may atomically add an aggregate revision, events, and
  outbox effects alongside that session increment;
- event and outbox sequence keys are `(domain, revision, seq)`;
- any exception rolls back every transaction artifact, including rollover
  session state.

The schema requires SQLite 3.37 or newer for `STRICT` tables. The gate was
checked against SQLite 3.53.1 on the Mac and 3.46.1 on DellTower.
This alpha uses schema version 3 and projection codec version 2. It fails closed
on an older v2 preview database; archive that database and start the canary on
a new path because no in-place preview migration is provided.

The outbox is durable but the first milestone intentionally has no dispatcher.
That prevents an experimental profile from producing external side effects.

## Runtime profiles

The first canary is `omd_server.v2.server` and exposes exactly:

- `about`
- `claim_set`
- `renew_claim_set`
- `release_claim_set`
- `claim_status`
- `domain_status`

`claim_status` and `domain_status` are pure reads. Expiration and promotion are
performed by explicit internal `MaintenanceTick` commands owned by the stdio
runtime supervisor. Every valid mutation attempt independently performs the
same due-transition prelude in its transaction, so correctness does not depend
on the poll interval. Supervisor failure is logged loudly and makes subsequent
tools fail closed. The stdio profile has no leader lease, Git adapter, merge,
commit, worktree, push, or task lifecycle tool.

The six tools bind one server-issued principal. `claim_status` is owner-scoped;
`domain_status` exposes aggregate status counts plus only this session's claim
records. Neither status API publishes another session's FenceVector.

The default database is cwd-independent:
`$OMD_V2_DB_PATH`, else `$OMD_V2_STATE_DIR/omd/v2/lease.db`, else
`$XDG_STATE_HOME/omd/v2/lease.db`, else
`~/.local/state/omd/v2/lease.db`. An explicit CLI path is also accepted.

Example canary startup:

```bash
OMD_V2_DOMAIN_ID=my-workspace \
OMD_V2_REPO_ID=my-repo \
omd-v2-lease
```

By default the runtime generates a transport-unique agent and a client derived
from it, isolating request-ID namespaces. Set a stable `OMD_V2_AGENT_ID` when
restarts represent the same logical agent; the derived client is then stable,
each restart advances the pair's epoch, and the older process is fenced. If
the agent is stable across restart—or `OMD_V2_CLIENT_ID` is explicitly
shared—its callers must use globally unique request IDs that are never
recycled. This is not authentication against a local process able to open or
modify the SQLite file; deploy with OS file permissions and one trusted local
user. The server extra pins the tested FastMCP API range to
`fastmcp>=3.4.4,<4`.

## Verification

The v2 suite includes:

- exact path, Unicode, case, dotfile, and symlink-boundary counterexamples;
- claim-set atomicity and conflict-local no-barging examples;
- FenceVector truncation, tampering, ownership, and epoch tests;
- strict idempotency and current-projection replay tests;
- malformed-wire idempotency and mutable-command fingerprint counterexamples;
- session rollover/stale-process fencing and caller-identity schema checks;
- multi-resource rollover liveness and writer-lock clock-order regressions;
- transaction-prelude deadline enforcement plus real idle stdio expiry;
- exact-deadline idempotent replay and forged transition counterexamples;
- exact requested grant/renew TTL and registration wait provenance checks;
- supervisor clock-rollback/request-ID-poisoning regression;
- exact global enqueue and grant sequence reducer checks;
- revision-coherent response interleaving regression;
- deterministic and property-generated safety streams;
- SQLite writer serialization and revision CAS;
- failure injection after idempotency, aggregate CAS, and outbox writes;
- pure status checks;
- in-process and real stdio MCP capability-surface checks;
- domain/idempotency corruption quarantine counterexamples;
- fresh-process legacy import isolation;
- an actual PEP 517 wheel-content smoke build.

Run:

```bash
.venv/bin/python -m pytest -q tests/v2
.venv/bin/python -m pytest -q
.venv/bin/python -m omd_server.conformance
uv build --wheel
```

## Migration and next boundary

V2 does not reinterpret or migrate live v1 `HELD`, `PENDING`, `CONNECTING`, or
merge-token state. Safe adoption order is:

1. run v2 lease-only on a new database and shadow canonicalized decisions;
2. compare v1/v2 behavior without granting authority to v2;
3. admit only new claim-sets into a canary domain;
4. drain v1 active leases before any authority switch;
5. build the repo daemon and Git integration saga separately.

The Git saga must capture immutable source and target OIDs, build a candidate
tree with `git merge-tree --write-tree`, create a commit with `commit-tree`,
audit the immutable post-tree delta, update the target ref with expected-old
CAS, persist a sink receipt, and recover forward from durable intent. It must
not be added to the lease-only stdio process.

This DellTower-lineage branch is internal/non-publishable until the user selects
an explicit project license. Its ancestry must never be pushed directly onto
the scrubbed public history; export only new v2 commits and apply them to the
reviewed public base.
