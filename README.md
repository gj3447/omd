# OMD — 입체운행물방울 (Orbital Motion Droplet)

멀티에이전트 **병렬 개발 코디네이터**. 사도 **OMC**(입체운행구름, Orbital Motion Cloud) 예하 **군단장**. 내부 불변식 코어 = **SINGULON**(특이점).

N개의 코딩 에이전트(물방울)를 **입체(서로소 write-set 궤도)** 로 따로따로 병렬 운행시키고, **분열(merge conflict)=0** 을 *사전* 보장한 뒤(선의공리 특이점 조건: 부분이 전체를 잠식 못 함), **CLOUD CONNECT(응결=merge)** 로 하나의 구름에 통합한다.

서버권위 **강제형 write-set lease**(advisory도 lock-free도 아닌 4번째 지점)가 핵심 IP — git worktree 격리(=자존자)와 강제 경로조정(=특이점)을 결합한다.

## 문서
- [`CONCEPT.md`](./CONCEPT.md) — 컨셉·은유·아키텍처·선행연구 & 차별점(Longinus 바인딩)
- [`SERVER_SPEC.md`](./SERVER_SPEC.md) — 데이터 모델·상태머신(Orbit/Task/Agent/Barrier)·SINGULON 불변식·OSS 검증(ABC)·추천 스택
- [`CONCURRENCY.md`](./CONCURRENCY.md) — **동시성·실패모드 정밀 설계** (긴급 탈출·고아 lease/플래그·데드락/기아·크래시 복구·14차원 + 교차작용 A–H + P0/P1/P2 로드맵)
- [`docs/OMD_DEMPSEY_ROLL.md`](./docs/OMD_DEMPSEY_ROLL.md) — 서로소 궤도·merge fence 운영 규율 초안 (`PRELIMINARY` / `VerdictPending`)

## 캐논 계층
`사도 OMC(입체운행구름) → 군단장 OMD → 군단(병렬 에이전트 물방울들)`

## 상태
**프로토타입 동작 — 3겹 검증(green).** ① **`pytest`** (475 passed — dev+server extras가 설치된 2026-07-15 로컬 full suite) · ② **TLA+ 모델 체크** (`spec/*.tla` 3종 — leader·lease·connect — CI `tla` 잡에서 TLC) · ③ **Hypothesis stateful** (lease/fence 코어를 무작위 연산열로 흔드는 2종 — in-memory + 영속 SQLite/WAL 재시작 내구성). 구현됨: 입체 glob 교집합 · SQLite lease+fence · Orbit/Task FSM · SINGULON 2지점 강제 · 실물 git worktree+CLOUD CONNECT(merge)+fencing · **green-only pre-commit integration gate** · 유계 liveness · 좀비 회수 · 데드락 wait-for 사이클 감지 · 우선순위 promote · FastMCP 35툴 · CLI parity · **P0 동시성 11/11 + D1–D14 하드닝**.

> ⚠ **검증 범위(정직한 표기).** TLA+ 모델은 **bounded·abstract** 다 — 작은 상수 공간(예: `Tasks = {t1, t2}`)에서만 망라 탐색하고, git split-phase·실시간·크래시 타이밍을 추상화한다. Hypothesis stateful 은 **단일 프로세스 in-proc** 모델(실제 멀티프로세스/멀티노드 레이스 그 자체가 아니라 코어 불변식의 모델). 즉 "model check + stateful + pytest green" 은 *설계 수준* 보증이지 분산 배포의 전수 보증은 아니다. 동시성·실패모드 전수 분석과 로드맵은 [`CONCURRENCY.md`](./CONCURRENCY.md). (예: 좀비 회수는 `agent_ttl` 을 켜야 동작, 기본 비활성.)

## Quickstart
```bash
pip install -e .            # 코어 + transitions   (서버: -e '.[server]')
pytest -q                   # dev+server extras 설치 환경: 475 passed (2026-07-15)

# CLI (MCP 툴과 동일 동사)
omd declare auth --writes 'src/auth/**'
omd declare ui   --writes 'src/ui/**'
omd next agA                                 # → {"task_id":"auth", ...}  (서로소 작업 추천)
omd claim agA 'src/auth/**' --task auth       # → HELD (fence 1)
omd claim agB 'src/auth/login.py'             # → PENDING (겹침=비입체)
omd claim agC 'src/ui/**'   --task ui         # → HELD (서로소)
omd status

# MCP 서버 기동
python -m omd_server.server omd.db
```
```python
from omd_server import Coordinator

omd = Coordinator(
    "/path/to/repo/.omd.db",                   # fence가 재기동을 넘어 보존되는 영속 DB
    repo="/path/to/repo",
    integration_check=("make", "verify"),      # operator 고정 argv; connect caller 입력 아님
    integration_check_timeout=1800,
    require_integration_check=True,
)
s = omd.begin(
    "A", "agA", writes=["a/**"],
    ttl=3600, liveness_ttl=1800,                # 유계 silence window (반복 fake heartbeat 아님)
)
# ... s["worktree"] 안에서만 편집 ...
omd.commit("A", "feat: a", "agA", s["fence"])
omd.finish("A", "agA", s["fence"])
omd.connect("A", "agA", s["fence"])
# 후보 merge에서 make verify가 green일 때만 commit→MERGED. red면 main 불변+DONE 재시도.
```

`writes`와 `shared`는 한 task 안에서도 서로소여야 한다. 현재 glob 문법은
`parent/** EXCEPT parent/hot.py`를 표현하지 않으므로, hot 파일을 shared로 옮길 때는 원래
exclusive glob도 함께 더 작은 서로소 단위로 재분할한다.

## Field overlap pilot (PROM16 R3)

`python -m omd_server.overlap_pilot` runs an exploratory, read-only measurement
that keeps three claims separate:

1. cross-task execution-exposure windows overlap;
2. the temporally intersecting orbit rows have overlapping declared write sets;
3. two provenance-eligible branch tips conflict in a counterfactual pairwise
   `git merge-tree` oracle.

Neither (1) nor (2) proves continuous concurrent execution. Claim (3) is not a
replay of the historical OMD connect base, order, or outcome. It runs for every
candidate window pair, including pairs whose declarations are disjoint,
because non-strict deployments may have changed files outside their declarations.

The report has two content-selected semantics. Table presence or a schema version
alone is not evidence:

- **Native v3 (`omd-base-overlap-pilot/v3`).** Any durable native marker—an
  attempt/connect row, a task attempt pointer, or a non-NULL native orbit provenance
  field—selects v3. Once selected, the analyzer never falls back to v2. Legacy orbit
  rows in a mixed database are excluded from v3 exposure calculations and reported
  as excluded counts (and remain bound into the input digest).
- **Legacy v2 (`omd-base-overlap-pilot/v2`).** This is selected only when the
  snapshot contains no native marker. A migrated database with empty
  `task_attempts`/`connect_attempts` tables and all-NULL provenance columns therefore
  remains legacy; migration does not manufacture historical facts.

### Native attempt provenance (v3)

`task_attempts` is the immutable execution-generation authority. Its `attempt_id`
and per-task `attempt_ordinal` keep requeues separate even when `task_id`, `agent_id`,
and paths are reused. The row also freezes the agent, repository identity/root,
integration branch, and declared `writes`/`shared` sets for that attempt. Mutable
columns in `tasks` are only a live projection and are not used as historical pilot
evidence.

Each native workload orbit records demand time and TTL (`requested_at`,
`requested_ttl`) separately from the actual grant (`granted_at`). A delayed PENDING
promotion receives its original requested TTL from the grant time, not from request
time; there is no separate claim that `created_at` was the grant. For a granted
write/shared orbit, the v3 half-open exposure interval is exactly:

```text
[ max(task_attempts.started_at, orbits.granted_at),
  orbits.terminal_effective_at )
```

`terminal_at` is when the coordinator observed/recorded termination;
`terminal_effective_at` is the lease-semantics endpoint (for example the expiry
deadline rather than a later sweep time). v3 does not substitute `created_at`,
`expires_at`, or `released_at` when one of these authoritative fields is missing.

`connect_attempts` preserves one row per admitted Phase-A try. `connect_seq` is an
attempt-local retry ordinal; each row captures the exact branch tip, integration
base, participating orbit IDs/fences, terminal outcome/code, resolution source, and
any `merge_sha`/`merge_gen`. The first admitted try (`connect_seq=1`) supplies the
canonical tip and outcome for the base-overlap cohort, avoiding a successful-retry
survivor bias. Every retry remains in the canonical input digest. `merge_gen`, not
`connect_seq`, supplies global successful integration order.

For a newly published merge, Phase B creates the exact candidate commit object and
single-assigns its tree OID, commit OID, and preparation time to that connect row
*before* the integration-ref compare-and-swap. Publication is refused if this
durable seal cannot be written. Phase C and crash recovery accept only that exact
commit OID; matching trailers or parent shape alone are insufficient. An
already-integrated branch is the explicit no-candidate case: its recorded
`merge_sha` must equal the captured integration base.

Barrier-driven tries carry `trigger_kind`, `barrier_id`, and `barrier_generation`,
so a barrier connect is not silently reinterpreted as a direct connect. Synthetic
rolling-upgrade adapter attempts, their connects, adapter-bound native rows, and
associated `attempt_id=NULL` pre-R3 rows are ineligible for the native v3 cohort.
They remain explicit exclusion metadata in the canonical input digest and source
counters, so one adapter cannot poison otherwise eligible native evidence. A
pre-R3 row that later gains only grant/terminal transition fields is likewise kept
as excluded legacy evidence because its request identity is still unknowable.
Unrelated partial native markers, orphan references, and identity mismatches remain
strict fail-closed errors.

Native v3 is strict and fail-closed. Partial identities, orphan/mismatched
attempt-orbit-connect references, invalid timestamp/state matrices, incomplete
terminal projections, or inconsistent MERGED/`merge_gen` evidence produce a data
error (CLI exit 2) instead of a nominal v2 measurement. A structurally unfinished
live attempt/connect may therefore need to reach a durable terminal state before a
v3 field report can be emitted.

### Legacy nominal evidence (v2)

Legacy v2 preserves the earlier, explicitly incomplete semantics. Its window begins
at `created_at`, which is request-row creation and may precede the actual grant after
a PENDING promotion. It ends at `released_at`, or at nominal `expires_at` when no
release was recorded. The interval can include waiting time or overstate an early
reclaim; it is a nominal lease/request proxy, not a held-lease or execution trace.
Task-level tips are withheld for multi-agent requeues, ambiguous multi-orbit
histories, and—under a cutoff—unless a single-agent task was durably merged before
that cutoff.

```bash
python -m omd_server.overlap_pilot \
  --db /path/to/omd_coord.db \
  --created-before 1784075303.5643721 \
  --scope 'lakatotree=(^lt-|lakatotree)' \
  --path-root lakatotree=/old/worktree/lakatotree \
  --git-repo lakatotree=/path/to/lakatotree \
  --require-complete-oracle
```

At invocation, the analyzer takes a stable temporary copy of the SQLite base
file plus WAL, then opens only that copy; it does not let SQLite touch the live
DB, WAL, or SHM files. `--created-before` limits selected membership; it does not
turn the live database into an earlier as-of snapshot.

The Git oracle creates a temporary bare repository with its own object sink and
neutral attributes. It reads source objects through alternates but does not load
the measured repository's config, hooks, attributes override, or custom merge
drivers. Its safe built-in text-merge result may therefore differ from the
repository's native custom-merge behavior. Missing tips produce
`oracle_coverage_status=INCOMPLETE`; Git-oracle errors and ambiguous scope
assignment exit 2. `--require-complete-oracle` exits 3 unless every scope has
complete candidate-pair coverage (`--require-measured` remains a compatibility
alias only). In v3 the oracle consumes the first admitted connect tip; the recorded
integration base and `merge_gen` remain provenance, but the pairwise oracle is still
counterfactual and does not by itself prove historical merge-parent identity.

The report preserves both a canonical input digest (decoded v2 groups or
authoritative v3 provenance rows) and a measurement digest that binds scope
regexes, path roots, cutoff/exclusions, evidence and pair limits, Git
identity/version/object format, oracle policy, and implementation
and overlap-dependency hashes. The measurement digest covers the full canonical
report, not only its inputs. Pair/orbit/path-selector budgets fail closed before
unbounded Cartesian work. Repeat `--path-root` when one logical repository
appears under several historical clone roots. Pair edges may share task/agent
groups, so fractions are descriptive only; the field endpoint and metric
promotion remain explicitly `NOT_ASSESSED`.

The provenance contract relies on database constraints/triggers plus Coordinator
and Store compare-and-set paths to keep identities append-preserved and lifecycle
projections single-assignment during normal OMD operation. This is **not** a
cryptographic tamper-evident ledger: there are no signatures, attestations, hash
chains, or WORM storage guarantees. A privileged actor who can rewrite or replace
the SQLite files can forge history. Report hashes bind the captured payload and make
comparisons reproducible; without an independently retained trusted digest, they do
not prove who produced the underlying rows or that the database was never altered.

From a source checkout, `scripts/omd_overlap_pilot.py` is an equivalent shim.
