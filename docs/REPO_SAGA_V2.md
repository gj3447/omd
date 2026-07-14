# OMD v2 Repo Saga

Status: implemented in-process repo-saga engine-library alpha. It has no
daemon, CLI, or MCP transport and no production lease-to-reservation authority
adapter. The lease-only profile cannot publish; authoritative production
publication remains **NO-GO**.

## Semantic anchor

The repo saga is a single-writer, worktree-free Git publication engine. It
turns one immutable source commit plus a durable OMD mutation reservation into
at most one expected-old update of one registered integration ref.

It is deliberately a sibling of the lease engine, not another lease tool:

```text
lease engine                    repo saga
------------                    ---------
claim + FenceVector             durable mutation reservation
                                 |
                                 v
                     immutable tree/commit candidate
                                 |
                                 v
                     expected-old target-ref CAS
                                 |
                                 v
                        durable sink receipt
```

The Git CAS is a concurrency precondition, not a lease fence. Publication is
therefore forbidden unless an authority adapter has atomically converted the
current ACTIVE claim into a saga-owned reservation that continues blocking
conflicting grants until terminal settlement. The stock lease-only profile has
no such adapter, so it cannot publish.

## Crystallized boundary

The first alpha is intentionally narrow:

- one pre-registered local repository per saga;
- one direct, fully qualified `refs/heads/*` target selected by the registry;
- target is configured as OMD-exclusive, never checked out, sole-writer, and
  no-rewind; exclusivity is a deployment assertion, not auto-discovery;
- one active critical section per Git common dir via a process thread lock and
  POSIX `flock`; there is no HA or distributed worker fencing;
- immutable full source, read-base, target, tree, and candidate OIDs;
- clean merges only;
- case-sensitive, valid UTF-8 NFC Git paths without normalization collisions;
- no worktree, checkout, push, fetch, rerere, submodule operation, custom merge
  driver, hook, automatic rebase, or v1 state migration; symlink and gitlink
  deltas are rejected.

Callers provide no repository path and no target ref. They select a registry
identity. A registry entry binds that identity to a canonical Git common dir,
object format, target ref, state directory, and ownership policy.

### Repository binding and deployment trust

Each saga persists a repository-instance digest covering the canonical common
and object directories plus their device/inode identities, the target ref,
object format, the OMD-exclusivity assertion, and the absolute Git binary path,
device/inode, and recorded version. Registration, worker entry, and operational
Git plumbing revalidate the live directory owner and digest before commands.
Non-empty or special-file object alternates are outside the alpha. A registry
remap or live same-path
replacement after a durable intent is quarantined as `IN_DOUBT` before it is
touched. The owner-only state directory must be disjoint from the source
worktree, common dir, and object storage; it is registry configuration, not
part of this persisted repository digest. The SQLite file is forced to mode
`0600`, and daemon-created state directories to `0700`.

This is not a hostile same-UID sandbox. Deployment requires a dedicated
service UID and protected parent directories for the Git common/object dirs,
Git binary, state/execution dirs, SQLite database, and worker lock. Owner and
inode checks prevent accidental remaps but cannot eliminate malicious same-UID
TOCTOU or in-place binary/config mutation. Those actors are outside this alpha
authority boundary.

## State machine

```text
INTENT_DURABLE
  -> RESERVED
  -> INPUTS_PINNED
  -> CANDIDATE_READY
  -> PUBLISHING
  -> APPLIED
  -> RECEIPTED

Terminal rejection/quarantine states:
  MERGE_CONFLICT
  POLICY_REJECTED
  WRITESET_REJECTED
  READ_STALE
  REF_STALE
  IN_DOUBT
```

Every transition and its projection commit in one SQLite transaction. Git runs
without a SQLite writer transaction. An append-only event row accompanies each
projection transition. `(domain_id, client_id, request_id)` binds one canonical
request fingerprint; reuse with different input is rejected. Recovery is
system-owned by operation ID and does not depend on a new client session.

`IN_DOUBT` is terminal only to automatic callers and recovery. It never
auto-settles: if a mutation reservation was acquired, it remains held and the
saga is excluded from `recover_all` until a future operator-resolution protocol
decides the sink outcome. Other rejection terminals retry idempotent settlement.

The deterministic commit metadata, first parent (captured target), second
parent (source), message, candidate tree, and candidate commit are persisted
before the target CAS. A retry must recreate the same commit OID.

## Git execution contract

Git 2.38 or newer is required. Fixed commands run with an absolute Git binary,
an environment allowlist, bounded output, and a timeout. System/global config,
replace objects, terminal prompts, and hooks are disabled.

Candidate construction uses a clean bare execution Git dir whose object
database points at the registered repository. Its config keys are allowlisted;
system/global attributes are disabled; `core.attributesFile` is `/dev/null`;
and daemon-owned, non-group/world-writable `info/attributes` must contain
exactly `* merge`. This higher-precedence rule prevents committed
`.gitattributes` from selecting an external merge driver. Real-ref commands
force `core.hooksPath=/dev/null`. Object alternates are rechecked at runtime.

The protocol is:

1. Persist intent and deterministic metadata.
2. Idempotently acquire the durable mutation reservation.
3. Pin exact source, read-base, and expected-target OIDs under private
   `refs/omd/pins/<operation>/...` refs.
4. Require read-base to be an ancestor of both source and expected target.
5. Audit `read-base -> expected-target` against authoritative READ resources.
6. Run non-stdin `merge-tree --write-tree`. Only exit status 0 can produce a
   candidate. Exit status 1 is a conflict even though stdout contains a tree.
7. Read the raw NUL-delimited, rename-disabled tree delta. Reject invalid UTF-8,
   non-NFC paths, normalization collisions, symlink modes, gitlinks, and every
   path not covered by an authoritative WRITE resource. A rename is audited as
   deletion plus addition.
8. Run deterministic `commit-tree` with target first and source second; pin the
   resulting commit before recording `CANDIDATE_READY`.
9. Persist `PUBLISHING`, revalidate the durable reservation, then execute one
   `update-ref --no-deref <target> <candidate> <expected-old>` CAS.
10. Persist the observed sink receipt, settle the reservation, and record
    `RECEIPTED`.

Private pin refs are retained in the alpha. Garbage collection is a separate
future saga; silently deleting pins during recovery would recreate the object
loss window they close.

## Crash and ambiguity matrix

On recovery from `PUBLISHING`, compare the exact persisted OIDs with the direct
target ref:

| Observation | Recovery |
|---|---|
| target equals candidate | record `APPLIED` and settle forward |
| candidate is in current target history | record `APPLIED` and settle forward |
| target equals expected old | retry only under the configured OMD-owned, sole-writer, no-rewind contract |
| target differs before the CAS begins | `REF_STALE`; never rebase the same operation |
| missing/symbolic/diverged/externally rewritten target after an uncertain CAS | `IN_DOUBT`; no automatic retry or success claim |

In a general repository, `target == expected-old` cannot distinguish “CAS never
ran” from “CAS succeeded and an external actor rewound it.” Such repositories
are outside the alpha authority boundary. Reflogs may provide supporting
evidence but are never negative proof.

A `target + receipt` `update-ref --stdin` transaction is not used. The default
files ref backend commits individual refs sequentially, so a crash can expose a
prefix of a nominal multi-ref transaction. SQLite plus the single target CAS is
the honest baseline; uncertainty is represented rather than guessed away.

## Production acceptance gates

The isolated library exercises the Git/state-machine gates below. Production
publication remains blocked on gate 2's real durable authority adapter and the
deployment boundary above; the in-memory test authority is not proof of
cross-process grant exclusion.

1. READ-only or expired authority cannot obtain a mutation reservation.
2. Reservation ownership blocks a competing WRITE grant through settlement.
3. A malicious merge driver and `reference-transaction` hook execute no code.
4. Every conflict form leaves the target ref unchanged.
5. Request-ID reuse with changed input is rejected.
6. Crash injection before and after every Git effect converges to one receipt or
   a fail-closed terminal state.
7. A target move never causes automatic rebase or overwrite.
8. Out-of-write-set rename, invalid path encoding, symlink, and gitlink deltas
   fail closed.
9. The user worktree and index remain byte-for-byte untouched.
10. The lease-only stdio server still exposes exactly its original six tools.

Current executable coverage includes deterministic SHA-1 and SHA-256 clean
publication, every declared killpoint, response-loss recovery, target drift,
registry remap/live replacement, post-registration alternates, READ-only
authority, changed-input replay, conflict, rename, NFD/non-UTF-8 paths, symlink,
gitlink, malicious driver/hook, controlled attributes, output/timeout bounds,
worktree/index invariance, owner-only disjoint state, concurrent settlement,
and wheel inclusion. A real adapter must still prove that reservation ownership
durably blocks competing WRITE grants through settlement and that expired
claims cannot reserve.

## Deferred boundaries

The durable lease-to-reservation adapter, public repo-daemon transport, HA
worker fencing, push/outbox delivery, worktree lifecycle, reftable-only atomic
receipts, and operator resolution of `IN_DOUBT` are later milestones. Until the
first item exists and passes the authority gates, this package is a tested
engine library, not a production integration command.

## Evidence and rejected inheritance

The design follows the DellTower OMD split-phase outline but rejects v1's
mutable branch merge, trailer-based recovery, unchecked idempotency replay,
Git I/O inside SQLite writer transactions, inline fail-soft push, and worktree
side effects before durable receipts.

Git's official `merge-tree`, `commit-tree`, and `update-ref` documentation and
the files ref backend were inspected at `e9019fcafe0040228b8631c30f97ae1adb61bcdc`.
During the 2026-07-14 audit the local Mac reported Git 2.50.1 and DellTower
reported Git 2.53.0. The admitted minimum is Git 2.38, whose documented
`merge-tree --write-tree` interface is required.
