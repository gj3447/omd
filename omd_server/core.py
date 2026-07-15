"""OMD Coordinator — 군단장 코어 로직.

SINGULON 불변식을 2지점에서 강제:
  ① claim/next     : write-set이 활성 HELD 궤도와 서로소(입체)일 때만 grant/배정 (사전)
  ② connect(merge) : 작업 중 lease가 만료/해제됐으면 응결 거부 (fencing, merge 게이트)
=> CLOUD CONNECT 시 충돌(분열)이 구조적으로 0.

**동시성 임계구역(D1, CONCURRENCY.md §D1).** 모든 변이 동사는 `with self._cs():`
( = 프로세스내 단일 writer 직렬화 RLock + `store.tx()`(BEGIN IMMEDIATE/WAL) )
한 트랜잭션 안에서 일어난다. check-then-act(claim의 충돌검사→grant, fence 발급)가 원자적이라
동시 호출에도 SINGULON이 깨지지 않는다(P0-1 TOCTOU·P0-2 fence중복 닫힘). `tx()`는 재진입 가능하여
한 동사가 sweep/_promote_pending을 같은 트랜잭션으로 호출한다.

**관측가능성(LTDD).** 각 동사는 구조화 이벤트(events.Emitter)를 방출해 외부 store에서 도착-검증된다.
단 µs 동시성 레이스 자체는 트레이스가 아니라 직접 불변식 테스트로 본다(METHODOLOGY 원칙 7).

증분3(CONCURRENCY §D1/§3.B/§D8/§D11): connect는 이제 **split-phase**다.
  Phase A(락+tx): write-orbit 재검증(P0-4 fence==captured) + repo-wide merge_token 획득 +
                  task→CONNECTING + 궤도 pin(merging=1) + intent 영속 + 커밋.
  Phase B(락 밖): 전용 통합 worktree에서 `checkout integration_branch` + `merge --no-ff`
                  (subprocess 타임아웃, §E). 충돌/타임아웃이면 abort.
  Phase C(락+tx): merge_sha 먼저 기록(P0-6) → task→MERGED → write-orbit 해제 + merge_token 반납
                  + unpin + promote. Phase B 실패면 CONNECTING→DONE rollback(재시도가능) + 토큰반납.
재기동 시 `_recover()`(§D8)가 CONNECTING task를 git 진실(trailer-probe)과 조정하고 dangling
merge_token을 abort한다.
"""

from __future__ import annotations

import hashlib
import json
import math
import os
import threading
import time
import uuid
from contextlib import contextmanager

try:
    import fcntl
except ImportError:  # pragma: no cover - multi-process OMD currently targets Unix.
    fcntl = None

from . import bypass_audit, fsm, task_state
from .admission import (
    AGING_POLICY_SCHEMA,
    DEFAULT_ADMISSION_AGING_QUANTUM,
    DEFAULT_ADMISSION_MAX_AGE_BOOST,
    AdmissionRequest,
    QueuePolicy,
    authority_snapshot_hash,
    canonical_json,
    decide_admission,
    exact_conflict,
    normalize_pathspec,
    pathspec_digest,
    sha256_json,
)
from .admission_config import DEFAULT_ADMISSION_QUEUE_CAPACITY
from .admission_contract import bind_decision_id, project_legacy, step as admission_step
from .disjoint import path_in_globs, sets_overlap
from .events import NOOP
from .gitio import (
    GitError,
    GitIntegrationCheckError,
    GitIntegrationCheckTimeout,
    GitIntegrationMutation,
    GitIntegrationPreconditionError,
    GitMergeConflict,
    GitNothingToCommit,
    GitRepo,
    GitRollbackError,
    GitTimeout,
)
from ._barriers import BarrierMixin
from ._const import LATCH_RANK, MERGE_PIN_GRACE_S, WRITE_MODES
from ._flags import FlagMixin
from ._sems import SemMixin
from .store import Store

# Phase B(락밖 merge) 서브프로세스 타임아웃(§E — 무한 hang 방지). pin은 이보다 길게 잡아
# 타임아웃→abort→rollback이 완료될 시간을 준다. MERGE_PIN_GRACE_S/WRITE_MODES/LATCH_RANK 는
# mixin 과 공유하므로 _const 로 이동(순환 import 회피, apt-cleanup Q7).
MERGE_TIMEOUT_S = 120.0

# D14 리더-lease(코디네이터 singleton). 기동 시 리더 lease 를 획득(또는 살아있는 리더 감지 시
# 거부). last_heartbeat 가 이 TTL 을 넘으면 죽은 리더로 보고 takeover 가능(fence=epoch +1 로
# 옛 리더의 잔여 변이는 stale leader_epoch 로 차단). 권장: leader heartbeat 주기 = TTL/3.
LEADER_TTL_S = 30.0

# Repository-authority PENDING rows are bounded by default.  This is an
# operational default, not a value derived from the scheduler proof contract.

# A task-bound orbit is acquired before execution starts.  Later lifecycle
# states may retain an old claim response, but they are not admission authority.
TASK_ADMISSION_STATES = frozenset({"PENDING", "BLOCKED", "READY"})

ADMISSION_NOTIFICATION_SCHEMA = "admission_notification/v1"
COORDINATION_NOTIFICATION_SCHEMA = "coordination_notification/v1"
ADMISSION_NOTIFICATION_EVENTS = {
    "ADMISSION_GRANTED": "orbit_granted",
    "ADMISSION_QUEUED": "orbit_pending",
    "ADMISSION_DENIED": "orbit_denied",
    "ADMISSION_REJECTED": "orbit_rejected",
    "PROMOTION_GRANTED": "orbit_granted",
    "PROMOTION_DENIED": "orbit_denied",
    "RELEASE": "orbit_released",
    "CANCEL": "orbit_cancelled",
    "WAIT_TIMEOUT": "orbit_timed_out",
    "LEASE_EXPIRED": "orbit_expired",
    "WAIT_OWNER_RECLAIMED": "orbit_released",
    "LEASE_OWNER_RECLAIMED": "orbit_released",
}
AUXILIARY_NOTIFICATION_EVENTS = {
    # Compatibility telemetry causally follows the final reclaimed orbit in
    # the same durable request stream; it is not an admission FSM transition.
    "AGENT_RECLAIMED": "agent_reclaimed",
}
OUTBOX_NOTIFICATION_EVENTS = {
    **ADMISSION_NOTIFICATION_EVENTS,
    **AUXILIARY_NOTIFICATION_EVENTS,
}
OUTBOX_NOTIFICATION_SCHEMAS = {
    **{
        event: ADMISSION_NOTIFICATION_SCHEMA
        for event in ADMISSION_NOTIFICATION_EVENTS
    },
    **{
        event: COORDINATION_NOTIFICATION_SCHEMA
        for event in AUXILIARY_NOTIFICATION_EVENTS
    },
}
DURABLE_ADMISSION_TELEMETRY_EVENTS = frozenset(
    OUTBOX_NOTIFICATION_EVENTS.values()
)
ADMISSION_OUTBOX_LEASE_TTL = 30.0
ADMISSION_OUTBOX_MAX_RETRY_DELAY = 60.0
ADMISSION_OUTBOX_TIMER_START_RETRIES = 3


_EFFECT_LOCKS_GUARD = threading.Lock()
_EFFECT_LOCKS: dict[str, threading.Lock] = {}


def _process_effect_lock(key: str) -> threading.Lock:
    """Return the process-local half of a DB-scoped external-effect lock."""
    with _EFFECT_LOCKS_GUARD:
        return _EFFECT_LOCKS.setdefault(key, threading.Lock())


def _normalize_sweep_interval(value):
    """Return a positive finite interval, or ``None`` for an explicit off value."""
    if value is None:
        return None
    if isinstance(value, bool):
        raise ValueError("sweep_interval must be a finite non-negative number")
    try:
        interval = float(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(
            "sweep_interval must be a finite non-negative number"
        ) from exc
    if not math.isfinite(interval) or interval < 0:
        raise ValueError("sweep_interval must be a finite non-negative number")
    return interval or None


class CoordinatorConflict(RuntimeError):
    """D14: 같은 DB 에 살아있는 다른 코디네이터(리더 lease 보유)가 있어 기동을 거부한다.
    in-process actor 직렬화는 프로세스당이라, 한 DB 에 코디네이터 둘 = writer 둘 = SINGULON
    무효. 단일 인스턴스 전용을 *명시적으로 강제*(§D14)."""


class _IdemSlot:
    """멱등 래퍼의 슬롯. hit=캐시 적중(본문 skip), value=동사 본문이 set한 응답."""
    __slots__ = ("hit", "value", "deferred", "terminal")

    def __init__(self):
        self.hit = False
        self.value = None
        self.deferred = False
        self.terminal = False

    def set(self, value):
        self.value = value
        return value

    def defer(self):
        """Keep this exact request envelope INFLIGHT across a split phase."""
        self.deferred = True

    def set_terminal(self, value):
        """Cache a deterministic terminal failure receipt for exact replay."""
        self.value = value
        self.terminal = True
        return value


class Coordinator(FlagMixin, SemMixin, BarrierMixin):
    def __init__(self, db_path: str = ":memory:", repo: str | None = None,
                 worktrees_dir: str | None = None, agent_ttl: float | None = 90.0,
                 events=None, integration_branch: str | None = None,
                 merge_timeout: float | None = None, *,
                 coordinator_id: str | None = None, leader_ttl: float = LEADER_TTL_S,
                 allow_memory_db: bool = False,
                 enforce_single_coordinator: bool = True,
                 auto_push: str | None = None,
                 idem_ttl: float | None = 3600.0,
                 admission_wait_timeout: float = 3600.0,
                 admission_queue_capacity: int = DEFAULT_ADMISSION_QUEUE_CAPACITY,
                 admission_aging_quantum: float = DEFAULT_ADMISSION_AGING_QUANTUM,
                 admission_max_age_boost: int = DEFAULT_ADMISSION_MAX_AGE_BOOST,
                 strict_writeset: bool = False,
                 sweep_interval: float | None = None,
                 notification_timeout: float = 5.0,
                 notification_max_inflight: int = 8,
                 integration_check=None,
                 integration_check_timeout: float = 300.0,
                 integration_check_output_limit: int = 16_384,
                 require_integration_check: bool = False):
        # Q11: 검사 명령은 MCP caller가 connect 때 보내는 원격 명령이 아니라, 신뢰된 operator가
        # 기동 시 고정하는 argv다. shell 문자열은 받지 않는다.
        if integration_check is not None:
            if isinstance(integration_check, (str, bytes)):
                raise ValueError(
                    "integration_check must be a non-empty argv sequence, not a shell string"
                )
            try:
                integration_check = tuple(integration_check)
            except TypeError as exc:
                raise ValueError("integration_check must be a non-empty argv sequence") from exc
            if not integration_check or not all(isinstance(arg, str) for arg in integration_check):
                raise ValueError("integration_check must contain only argv strings")
            if repo is None:
                raise ValueError("integration_check requires a git repo")
        if require_integration_check and integration_check is None:
            raise ValueError("require_integration_check=True requires integration_check argv")
        try:
            integration_check_timeout = float(integration_check_timeout)
        except (TypeError, ValueError) as exc:
            raise ValueError("integration_check_timeout must be finite and positive") from exc
        if not math.isfinite(integration_check_timeout) or integration_check_timeout <= 0:
            raise ValueError("integration_check_timeout must be finite and positive")
        if (not isinstance(integration_check_output_limit, int)
                or isinstance(integration_check_output_limit, bool)
                or integration_check_output_limit <= 0):
            raise ValueError("integration_check_output_limit must be a positive integer")
        try:
            admission_wait_timeout = float(admission_wait_timeout)
        except (TypeError, ValueError) as exc:
            raise ValueError("admission_wait_timeout must be finite and positive") from exc
        if not math.isfinite(admission_wait_timeout) or admission_wait_timeout <= 0:
            raise ValueError("admission_wait_timeout must be finite and positive")
        try:
            notification_timeout = float(notification_timeout)
        except (TypeError, ValueError) as exc:
            raise ValueError("notification_timeout must be finite and positive") from exc
        if not math.isfinite(notification_timeout) or notification_timeout <= 0:
            raise ValueError("notification_timeout must be finite and positive")
        if (
            not isinstance(notification_max_inflight, int)
            or isinstance(notification_max_inflight, bool)
            or notification_max_inflight <= 0
        ):
            raise ValueError("notification_max_inflight must be a positive integer")
        if (not isinstance(admission_queue_capacity, int)
                or isinstance(admission_queue_capacity, bool)
                or admission_queue_capacity < 0):
            raise ValueError(
                "admission_queue_capacity must be a non-negative integer"
            )
        admission_policy = QueuePolicy(
            aging_quantum=admission_aging_quantum,
            max_age_boost=admission_max_age_boost,
        )
        sweep_interval = _normalize_sweep_interval(sweep_interval)
        # §D14: `:memory:` 디폴트 금지 — 재기동마다 모든 fence/leader_epoch 가 0 으로 리셋되어
        # 낡은 토큰/잔여 merge 와 충돌(고스트 writer). 영속 DB 필수. 단위테스트는 allow_memory_db=
        # True 로 명시 opt-in(프로세스 1개, 재기동 없음 — fence 리셋 위험 없음).
        if db_path == ":memory:" and not allow_memory_db:
            raise ValueError(
                "OMD requires a persistent DB path (got ':memory:'). An in-memory DB "
                "resets fence/leader epoch to 0 on every restart, colliding with stale "
                "tokens. Pass a file path, or allow_memory_db=True for single-process tests.")
        # Open only the bootstrap meta table here.  Domain schema migration is
        # authority-gated below, after the effect domains and leader admission
        # machinery exist.  A rejected second Coordinator must not migrate.
        self.store = Store(db_path, initialize=False)
        self.git = GitRepo(repo) if repo else None
        self.coordinator_id = coordinator_id or f"coord-{uuid.uuid4().hex[:12]}"
        # A caller-provided coordinator_id is an observability label and can be
        # reused.  Split-effect ownership needs a never-reused process identity.
        self.instance_id = f"inst-{uuid.uuid4().hex}"
        effect_domains = []
        if db_path == ":memory:":
            db_effect_domain = (f"memory:{id(self.store)}", None)
        else:
            durable_db = os.path.realpath(db_path)
            db_effect_domain = (
                f"db:{durable_db}", durable_db + ".connect-effect.lock"
            )
        effect_domains.append(db_effect_domain)
        durable_repo = None
        if self.git is not None:
            durable_repo = self.git.common_dir()
            effect_domains.append(
                (
                    f"repo:{durable_repo}",
                    os.path.join(durable_repo, "omd-connect-effect.lock"),
                )
            )
        effect_domains = sorted(dict(effect_domains).items())
        if fcntl is None and any(path is not None for _, path in effect_domains):
            raise RuntimeError("durable split effects require Unix advisory file locks")
        self._effect_process_locks = [
            _process_effect_lock(key) for key, _ in effect_domains
        ]
        self._effect_lock_paths = [
            path for _, path in effect_domains if path is not None
        ]
        # Schema migration shares the DB half of the split-effect fence, but
        # must not claim the repository half: an unrelated DB may initialize
        # safely while another coordinator uses the same Git repository.
        self._schema_effect_process_lock = _process_effect_lock(
            db_effect_domain[0]
        )
        self._schema_effect_lock_path = db_effect_domain[1]
        # M1 request identity needs a stable authority domain.  Compute a seed here,
        # then persist/read it after leadership acquisition so moving/restoring the
        # DB cannot silently change request identity.
        identity_path = durable_repo if durable_repo is not None else db_path
        repository_id_seed = "repo-" + hashlib.sha256(
            os.path.realpath(identity_path).encode("utf-8")
        ).hexdigest()
        self.repository_id = repository_id_seed
        self.leader_ttl = leader_ttl
        self.enforce_single_coordinator = enforce_single_coordinator
        self.leader_epoch = None  # 리더 lease 획득 후 채워짐(현 리더 세대)
        # heartbeat 만료 시 좀비 회수. 기본 ON(P0-7) — None=비활성. 끄면 죽은 물방울의
        # 궤도/작업이 영구 고아가 된다(사용자 핵심 우려). 권장 90s, renew는 TTL/3 주기.
        self.agent_ttl = agent_ttl
        self.events = events or NOOP
        self.notification_timeout = notification_timeout
        self.notification_max_inflight = notification_max_inflight
        self._lock = threading.RLock()  # 프로세스내 단일 writer(actor 대용) — D1
        # §D3/D4 주기적 백그라운드 sweep(opt-in). None/0=off(embedded 기본=inline-only). 켜면
        # 만료 lease/permit/좀비 회수가 동사 호출과 무관하게 진행 → 유휴 후 첫 호출 spike 해소.
        # 스레드 안전: 변이는 전부 _cs(RLock 직렬화) + store(check_same_thread=False, WAL).
        self._sweep_interval = sweep_interval
        self._sweep_stop = threading.Event()
        self._sweep_thread = None
        # Outbox delivery is scheduled independently from authority/effect
        # locks.  A post-commit hook only arms this timer; notifier I/O always
        # runs on the timer worker and transient failures wake at their durable
        # available_at/claim_deadline rather than waiting for another verb.
        self._outbox_timer_lock = threading.Lock()
        self._outbox_dispatch_lock = threading.Lock()
        self._outbox_timer = None
        self._outbox_timer_due = None
        self._outbox_active_threads = set()
        self._outbox_closed = False
        self._notification_attempt_lock = threading.Lock()
        self._notification_attempts = {}
        self._notification_closed = False
        # §D14 + schema authority: current-version startup is a read-only fast
        # path until leader admission.  A pending migration first fences every
        # split effect on the same DB, then acquires leadership, then mutates schema.
        # Thus a rejected peer and a process racing a live Git child are both
        # unable to exercise migration authority.
        schema_requires_migration = self.store.schema_requires_migration()
        if not schema_requires_migration:
            if self.enforce_single_coordinator:
                self._acquire_leadership()
        else:
            with self._schema_effect(blocking=False) as owns_effect:
                if not owns_effect:
                    self._emit(
                        "schema_migration_blocked", self.coordinator_id,
                        reason="live_effect_lock_held",
                    )
                    raise RuntimeError(
                        "OMD schema migration requires exclusive split-effect "
                        "authority; a live external effect still holds the fence"
                    )
                # Migration always takes a real leader generation, even for a
                # runtime that will subsequently operate with singleton
                # enforcement disabled.  This closes the check-to-migrate race.
                self._acquire_leadership()
                try:
                    self.store.initialize()
                except BaseException:
                    if self.leader_epoch is not None:
                        self.resign()
                    raise
                if not self.enforce_single_coordinator:
                    self.resign()
        with self.store.tx():
            persisted_repository_id = self.store.get_meta("repository_id")
            if persisted_repository_id is None:
                self.store.set_meta("repository_id", repository_id_seed)
                persisted_repository_id = repository_id_seed
            self.repository_id = persisted_repository_id
            persisted_capacity = self.store.get_meta("admission_queue_capacity")
            persisted_policy_version = self.store.get_meta(
                "admission_queue_policy_version"
            )
            persisted_policy_envelope = self.store.get_meta(
                "admission_queue_policy_envelope"
            )
            persisted_policy_marker = self.store.get_meta(
                "admission_policy_initialized"
            )
            policy_pair_missing = (
                persisted_policy_version is None
                and persisted_policy_envelope is None
            )
            persisted_v2_policy_versions = {
                row["policy_version"]
                for row in self.store.db.execute(
                    "SELECT DISTINCT policy_version FROM orbits WHERE "
                    "policy_version LIKE ?",
                    (f"{AGING_POLICY_SCHEMA}/%",),
                ).fetchall()
            }
            persisted_v2_decision = self.store.db.execute(
                "SELECT 1 FROM orbits WHERE "
                "decision_schema='admission_decision/v2' "
                "LIMIT 1",
            ).fetchone() is not None
            persisted_v2_authority = bool(
                persisted_v2_policy_versions or persisted_v2_decision
            )
            # schema_version and the authority pins historically completed in
            # separate transactions.  A missing marker plus a wholly absent
            # policy pair is the one resumable crash cut, but only before any v2
            # row could have observed a concrete policy.  Once the marker or v2
            # evidence exists, any missing key is corruption and fails closed.
            policy_initialization_allowed = (
                persisted_policy_marker is None
                and not persisted_v2_authority
                and (schema_requires_migration or policy_pair_missing)
            )
            capacity_policy_error = None
            capacity_mismatch = False
            initialize_capacity = (
                persisted_capacity is None and policy_initialization_allowed
            )
            if persisted_capacity is None:
                if policy_initialization_allowed:
                    durable_capacity = admission_queue_capacity
                else:
                    capacity_policy_error = (
                        "durable admission_queue_capacity is missing"
                    )
            else:
                try:
                    durable_capacity = int(persisted_capacity)
                except (TypeError, ValueError):
                    capacity_policy_error = (
                        "durable admission_queue_capacity is not an integer"
                    )
                else:
                    if durable_capacity < 0:
                        capacity_policy_error = (
                            "durable admission_queue_capacity is negative"
                        )
                    capacity_mismatch = (
                        durable_capacity != admission_queue_capacity
                    )
            expected_policy_envelope = canonical_json(admission_policy.envelope)
            aging_policy_error = (
                None
                if persisted_policy_marker in (None, "1")
                else "durable admission policy completion marker is invalid"
            )
            aging_policy_mismatch = False
            initialize_aging_policy = (
                policy_pair_missing and policy_initialization_allowed
            )
            if policy_pair_missing:
                if policy_initialization_allowed:
                    durable_policy = admission_policy
                else:
                    aging_policy_error = (
                        "durable admission queue policy is missing"
                    )
            elif persisted_policy_version is None or persisted_policy_envelope is None:
                aging_policy_error = "durable admission queue policy is incomplete"
            else:
                try:
                    decoded_policy_envelope = json.loads(
                        persisted_policy_envelope
                    )
                    if not isinstance(decoded_policy_envelope, dict):
                        raise ValueError(
                            "admission queue policy envelope must be an object"
                        )
                    durable_policy = QueuePolicy(
                        **{
                            key: value
                            for key, value in decoded_policy_envelope.items()
                            if key in {"aging_quantum", "max_age_boost"}
                        }
                    )
                except (TypeError, ValueError, json.JSONDecodeError) as exc:
                    aging_policy_error = (
                        f"durable admission queue policy is invalid: {exc}"
                    )
                else:
                    if canonical_json(durable_policy.envelope) != persisted_policy_envelope:
                        aging_policy_error = (
                            "durable admission queue policy envelope is non-canonical"
                        )
                    elif durable_policy.version != persisted_policy_version:
                        aging_policy_error = (
                            "durable admission queue policy version does not match its envelope"
                        )
                    aging_policy_mismatch = (
                        durable_policy != admission_policy
                    )
            if (
                persisted_policy_version is not None
                and persisted_v2_policy_versions
                - {persisted_policy_version}
            ):
                aging_policy_error = (
                    "durable admission queue policy does not match persisted v2 rows"
                )
            if not (
                capacity_policy_error
                or capacity_mismatch
                or aging_policy_error
                or aging_policy_mismatch
            ):
                if initialize_capacity:
                    self.store.set_meta(
                        "admission_queue_capacity", admission_queue_capacity
                    )
                if initialize_aging_policy:
                    self.store.set_meta(
                        "admission_queue_policy_version", admission_policy.version
                    )
                    self.store.set_meta(
                        "admission_queue_policy_envelope", expected_policy_envelope
                    )
                if persisted_policy_marker is None:
                    self.store.set_meta("admission_policy_initialized", "1")
        if (
            capacity_policy_error
            or capacity_mismatch
            or aging_policy_error
            or aging_policy_mismatch
        ):
            # Queue policy is repository authority, not a per-process hint.
            # Refuse configuration drift before recovery or any admission work.
            if self.leader_epoch is not None:
                self.resign()
            self.store.db.close()
            if capacity_policy_error:
                policy_message = capacity_policy_error
            elif aging_policy_error:
                policy_message = aging_policy_error
            elif capacity_mismatch:
                policy_message = (
                    "admission_queue_capacity conflicts with the durable repository "
                    f"policy ({persisted_capacity})"
                )
            else:
                policy_message = (
                    "admission aging configuration conflicts with the durable "
                    f"repository policy ({persisted_policy_version})"
                )
            raise ValueError(policy_message)
        self.merge_timeout = merge_timeout if merge_timeout is not None else MERGE_TIMEOUT_S
        self.integration_check = integration_check
        self.integration_check_timeout = integration_check_timeout
        self.integration_check_output_limit = integration_check_output_limit
        self.require_integration_check = bool(require_integration_check)
        # 연결(connect=merge) 직후 통합 브랜치를 이 remote 로 push — 로컬 누적 divergence 방지
        # (operator "커밋하면 바로 sync"의 OMD 내장판). None=off(기본·기존동작). env OMD_AUTO_PUSH 폴백.
        # push 실패는 fail-soft(merge 는 로컬 반영됨) — connect 성공 유지.
        self.auto_push = auto_push if auto_push is not None else (os.environ.get("OMD_AUTO_PUSH") or None)
        # §D9 멱등 캐시 GC TTL(초). 기본 1h — 어떤 현실적 MCP 재시도 윈도우보다 길어 replay 안전.
        # None=GC 안 함(기존동작). _sweep_inline 이 idem_ttl 지난 DONE 행 정리(무한누적 차단).
        self.idem_ttl = idem_ttl
        # M1 admission payloads carry a concrete wait deadline. Inline/periodic
        # sweep and restart reconciliation deliver WAIT_TIMEOUT before promotion.
        # Embedded delivery is opt-in; the MCP server owns a default lifespan sweep.
        self.admission_wait_timeout = admission_wait_timeout
        self.admission_queue_capacity = admission_queue_capacity
        self.admission_policy = admission_policy
        # P5 strict-writeset: True 면 commit-time 에 write-set 위반 즉시 거부+soft-reset(빠른 fail-loud).
        # 기본 off(connect-time enforce 유지=하위호환). env OMD_STRICT_WRITESET 폴백(정확 truthy 파싱).
        self.strict_writeset = bool(strict_writeset) or (
            (os.environ.get("OMD_STRICT_WRITESET") or "").strip().lower() in ("1", "true", "yes", "on"))
        self.integration_branch = integration_branch
        self.integration_worktree = None
        self.merge_resource = "cloud:default"   # repo-wide merge_token 키(§D11)
        if self.git:
            self.worktrees_dir = worktrees_dir or (self.git.root.rstrip("/") + "-omd-worktrees")
            os.makedirs(self.worktrees_dir, exist_ok=True)
            # 통합 브랜치: 명시 안 하면 레포 현재 브랜치(보통 main) — 사용자 HEAD가 아니라
            # 전용 worktree에서만 변이된다(§D11).
            if self.integration_branch is None:
                self.integration_branch = self.git.current_branch()
            self.integration_worktree = self.git.root.rstrip("/") + "-omd-integration"
            # P3 증분13(O2): rerere 레인 — 물방울 rebase 해소가 기록되고 동일충돌 재발 시
            # 자동 재적용(rr-cache 는 worktree 공유). fail-soft(rerere 불가여도 OMD 는 동작).
            try:
                self.git.enable_rerere()
            except GitError:
                pass
        # 재기동 복구(§D8, 멱등) — git↔DB 조정 + dangling merge_token abort.
        self._recover()
        # A crash after authority commit but before notification ACK leaves a
        # durable row.  Startup only arms the dispatcher: notifier I/O must not
        # delay construction or hold startup/effect authority.
        self._wake_admission_outbox()
        # 리더십·복구가 끝난 *뒤*에만 백그라운드 sweep 을 발사(변이 전 writer-둘 방지).
        if self._sweep_interval is not None:
            self.start_sweep()

    def start_sweep(self, interval=None):
        """Start periodic authority maintenance after startup and recovery.

        Server surfaces may defer this call until lifespan entry, so tool listing
        has no hidden writer and shutdown can join the sweep before leader handoff.
        """
        target = self._sweep_interval if interval is None else _normalize_sweep_interval(
            interval
        )
        if target is None:
            return {"ok": True, "enabled": False, "noop": True}
        thread = self._sweep_thread
        if thread is not None and thread.is_alive():
            if self._sweep_interval != target:
                raise RuntimeError(
                    "periodic sweep is already running with a different interval"
                )
            return {"ok": True, "enabled": True, "already": True, "interval": target}
        self._sweep_interval = target
        self._sweep_stop.clear()
        self._sweep_thread = threading.Thread(
            target=self._periodic_sweep_loop,
            args=(target,),
            name=f"omd-sweep-{self.coordinator_id}",
            daemon=True,
        )
        self._sweep_thread.start()
        return {"ok": True, "enabled": True, "interval": target}

    def _periodic_sweep_loop(self, interval):
        """만료 lease/permit/좀비를 주기적으로 회수(§D3/D4). Event.wait 로 자므로 stop 즉시 반응
        (인터벌 안 기다림). sweep 실패가 스레드를 죽이면 안 됨 → catch 후 다음 주기 재시도.
        리더십 상실(takeover 당한 좀비 리더)은 정지 — 좀비가 계속 변이하면 writer 둘."""
        while not self._sweep_stop.wait(interval):
            try:
                self.sweep()
            except CoordinatorConflict:
                self._emit("sweep_stopped", self.coordinator_id, reason="not_leader")
                return
            except Exception as e:  # noqa: BLE001 — 스레드 생존 우선(silent skip 아님, emit)
                self._emit("sweep_error", self.coordinator_id, error=repr(e))

    def close(self):
        """Stop and join background sweep/outbox workers (idempotent)."""
        with self._outbox_timer_lock:
            # Close the public drain gate before taking the active-worker
            # snapshot.  Every accepted manual/timer drain is registered under
            # this same lock, so none can mutate durable delivery state after
            # close() returns.
            self._outbox_closed = True
            outbox_timer = self._outbox_timer
            outbox_active = list(self._outbox_active_threads)
            self._outbox_timer = None
            self._outbox_timer_due = None
            if outbox_timer is not None:
                outbox_timer.cancel()
        with self._notification_attempt_lock:
            # Existing registered drains may still reach delivery while close
            # waits for them.  Stop them from starting a new strict effect;
            # close later joins every effect that was already registered.
            self._notification_closed = True
        self._sweep_stop.set()
        th = self._sweep_thread
        if th is not None and th.is_alive():
            th.join(timeout=5.0)
            if th.is_alive():
                raise RuntimeError(
                    "periodic sweep did not stop; refusing unsafe coordinator handoff"
                )
        self._sweep_thread = None
        if (
            outbox_timer is not None
            and outbox_timer is not threading.current_thread()
            and outbox_timer.is_alive()
        ):
            outbox_timer.join(timeout=5.0)
            if outbox_timer.is_alive():
                raise RuntimeError(
                    "admission outbox dispatcher did not stop; refusing unsafe handoff"
                )
        for active in outbox_active:
            if active is threading.current_thread() or not active.is_alive():
                continue
            active.join(timeout=5.0)
            if active.is_alive():
                raise RuntimeError(
                    "active admission outbox delivery did not stop; refusing unsafe handoff"
                )
        with self._notification_attempt_lock:
            notification_attempts = list(self._notification_attempts.values())
        for attempt in notification_attempts:
            worker = attempt["thread"]
            if worker is threading.current_thread() or not worker.is_alive():
                continue
            worker.join(timeout=5.0)
            if worker.is_alive():
                raise RuntimeError(
                    "strict notifier effect is still live; refusing unsafe handoff"
                )
        with self._notification_attempt_lock:
            self._notification_attempts.clear()

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()
        return False

    # ---- 임계구역 / 이벤트 ----
    @contextmanager
    def _cs(self, *, leader_guard=True):
        """단일 writer 직렬화(RLock) + 원자 트랜잭션(BEGIN IMMEDIATE). 재진입 안전.
        §D14: 트랜잭션을 연 직후 leader-fence 검사 — 다른 코디네이터가 takeover 했으면(우리가
        좀비 리더) CoordinatorConflict 로 거부해 writer 둘이 한 DB 를 변이하는 것을 막는다.
        leader_guard=False 는 리더십 *획득 중*(아직 epoch 미설정)에만 쓴다."""
        committed_hooks = []
        with self._lock:
            with self.store.tx(committed_hooks=committed_hooks):
                if self.enforce_single_coordinator and leader_guard:
                    self._assert_leader()
                yield
        # External notification delivery must never run while the authority
        # RLock is held.  Coordination is already durable, so failures remain
        # fail-soft here and the outbox owns bounded retry.
        for hook in committed_hooks:
            try:
                hook()
            except Exception:  # noqa: BLE001
                pass

    @contextmanager
    def _connect_effect(self, *, blocking=False):
        """Fence Git/split recovery across Coordinator processes.

        Lock order is always effect lock -> ``_cs``/SQLite.  The descriptor is
        inherited by Git and operator-check children, so a dead parent cannot
        expose a still-running external effect to a recovery process.
        """
        acquired_locks = []
        fds = []
        try:
            for lock in self._effect_process_locks:
                if not lock.acquire(blocking):
                    yield False
                    return
                acquired_locks.append(lock)
            for path in self._effect_lock_paths:
                fd = os.open(path, os.O_RDWR | os.O_CREAT, 0o600)
                fds.append(fd)
                op = fcntl.LOCK_EX | (0 if blocking else fcntl.LOCK_NB)
                try:
                    fcntl.flock(fd, op)
                except BlockingIOError:
                    yield False
                    return
            if self.git is not None:
                with self.git.inherit_effect_lock(tuple(fds)):
                    yield True
            else:
                yield True
        finally:
            # Closing is the release operation.  Do not issue LOCK_UN here:
            # ``pass_fds`` children share this open file description, and an
            # explicit unlock would drop their crash fence while an escaped
            # descendant can still mutate the integration worktree.  The
            # kernel releases the flock after the last inherited descriptor
            # closes.
            for fd in reversed(fds):
                os.close(fd)
            for lock in reversed(acquired_locks):
                lock.release()

    @contextmanager
    def _schema_effect(self, *, blocking=False):
        """Fence schema migration against split effects on this exact DB.

        Repository effects from a different DB are unrelated to SQLite schema
        authority and therefore deliberately excluded.
        """
        acquired = self._schema_effect_process_lock.acquire(blocking)
        if not acquired:
            yield False
            return
        fd = None
        try:
            path = self._schema_effect_lock_path
            if path is not None:
                fd = os.open(path, os.O_RDWR | os.O_CREAT, 0o600)
                op = fcntl.LOCK_EX | (0 if blocking else fcntl.LOCK_NB)
                try:
                    fcntl.flock(fd, op)
                except BlockingIOError:
                    yield False
                    return
            yield True
        finally:
            if fd is not None:
                os.close(fd)
            self._schema_effect_process_lock.release()

    def _emit(self, event, cid, **attrs):
        if event in DURABLE_ADMISSION_TELEMETRY_EVENTS:
            # Semantic admission edges are emitted only by the committed outbox
            # dispatcher.  Direct in-transaction telemetry can survive a
            # rollback (notably begin's SAVEPOINT) and is therefore forbidden.
            return
        port = self.events
        if self.store._txn_depth > 0:
            # Legacy telemetry is still fail-soft, but external callbacks must
            # never run under the authority transaction/RLock. Besides avoiding
            # rollback ghosts, this establishes one lock order when a sink
            # re-enters the public outbox flush seam.
            frozen_attrs = dict(attrs)
            self.store.after_commit(
                lambda: port.emit(event, cid, **frozen_attrs)
            )
            return
        port.emit(event, cid, **attrs)

    def _has_admission_notifier(self) -> bool:
        """Whether the injected events port exposes strict outbox delivery."""
        deliver = getattr(self.events, "deliver", None)
        if not callable(deliver):
            return False
        sentinel = object()
        backend = getattr(self.events, "backend", sentinel)
        return backend is sentinel or backend is not None

    def _schedule_admission_outbox_at(self, due_at, *, immediate=False) -> None:
        """Arm at most one daemon dispatcher timer, preferring an earlier wake."""
        if not self._has_admission_notifier():
            return
        due_at = float(due_at)
        start_error = None
        with self._outbox_timer_lock:
            if self._outbox_closed:
                return
            previous_timer = self._outbox_timer
            previous_due = self._outbox_timer_due
            if (
                previous_timer is not None
                and previous_timer.is_alive()
                and previous_due is not None
                and previous_due <= due_at
            ):
                return
            delay = 0.0 if immediate else max(0.0, due_at - time.time())
            for _ in range(ADMISSION_OUTBOX_TIMER_START_RETRIES):
                timer = threading.Timer(
                    delay,
                    self._run_admission_outbox_dispatch,
                )
                timer.name = f"omd-outbox-{self.coordinator_id}"
                timer.daemon = True
                self._outbox_timer = timer
                self._outbox_timer_due = due_at
                try:
                    timer.start()
                except Exception as exc:  # noqa: BLE001 — transient runtime seam.
                    start_error = exc
                    self._outbox_timer = previous_timer
                    self._outbox_timer_due = previous_due
                    continue
                if previous_timer is not None:
                    previous_timer.cancel()
                return
        # Do not emit while holding the timer lock: an observability backend may
        # be slow. Existing later timers remain armed; otherwise surface a
        # bounded, explicit degradation instead of publishing a dead timer.
        self._emit(
            "admission_outbox_schedule_error",
            self.coordinator_id,
            error=repr(start_error),
            attempts=ADMISSION_OUTBOX_TIMER_START_RETRIES,
        )
        raise RuntimeError("admission outbox timer could not start") from start_error

    def _wake_admission_outbox(self) -> None:
        """After-commit hook: schedule only; never enter a notifier port."""
        # Do not consume the authority wall clock: admission/begin/sweep tests
        # bind one exact observed time to the whole transaction.
        self._schedule_admission_outbox_at(0.0, immediate=True)

    def _schedule_next_admission_outbox(self) -> None:
        if not self._has_admission_notifier() or self._outbox_closed:
            return
        with self._cs():
            due_at = self.store.next_admission_outbox_due_at()
        if due_at is not None:
            self._schedule_admission_outbox_at(due_at)

    def _run_admission_outbox_dispatch(self) -> None:
        current = threading.current_thread()
        with self._outbox_timer_lock:
            if self._outbox_timer is current:
                self._outbox_timer = None
                self._outbox_timer_due = None
            if self._outbox_closed:
                return
        if not self._outbox_dispatch_lock.acquire(blocking=False):
            return
        with self._outbox_timer_lock:
            if self._outbox_closed:
                self._outbox_dispatch_lock.release()
                return
            self._outbox_active_threads.add(current)
        reschedule = True
        try:
            try:
                self._drain_admission_outbox(_schedule=False)
            except CoordinatorConflict:
                reschedule = False
                self._emit(
                    "admission_outbox_stopped",
                    self.coordinator_id,
                    reason="not_leader",
                )
            except Exception as exc:  # noqa: BLE001 — durable rows remain retryable.
                self._emit(
                    "admission_outbox_error",
                    self.coordinator_id,
                    error=repr(exc),
                )
                self._schedule_admission_outbox_at(time.time() + 1.0)
            finally:
                self._outbox_dispatch_lock.release()
            if reschedule:
                try:
                    self._schedule_next_admission_outbox()
                except CoordinatorConflict:
                    pass
                except Exception as exc:  # noqa: BLE001 — preserve autonomous retry.
                    self._emit(
                        "admission_outbox_error",
                        self.coordinator_id,
                        error=repr(exc),
                        phase="schedule_next",
                    )
                    self._schedule_admission_outbox_at(time.time() + 1.0)
        finally:
            # Keep close() aware of this worker through the final DB read and
            # possible timer arm; clearing earlier permits post-close access.
            with self._outbox_timer_lock:
                self._outbox_active_threads.discard(current)

    def _deliver_admission_notification(self, envelope) -> None:
        """Run one bounded, registered strict attempt without per-retry leaks."""
        port = self.events
        event_id = envelope["event_id"]

        def raise_delivery_error(delivery_error):
            if isinstance(delivery_error, Exception):
                raise delivery_error
            raise RuntimeError(
                "strict notifier child raised "
                f"{type(delivery_error).__name__}: {delivery_error}"
            ) from delivery_error

        with self._notification_attempt_lock:
            if self._notification_closed:
                raise RuntimeError("strict notification dispatcher is closed")
            prior = self._notification_attempts.get(event_id)
            if prior is not None:
                if not prior["done"].is_set():
                    raise TimeoutError(
                        "a prior strict notification attempt is still live"
                    )
                self._notification_attempts.pop(event_id, None)
                if prior["errors"]:
                    raise_delivery_error(prior["errors"][0])
                # The prior attempt completed after the dispatcher's timeout.
                # Reuse that outcome so the freshly claimed row can be ACKed
                # without starting a duplicate external effect.
                return
            # Late completions are a bounded best-effort outcome cache.  Keep
            # the registry itself within max_inflight even if another
            # coordinator ACKed the row and this process never sees the same
            # event again.  Eviction can cause an at-least-once duplicate, but
            # the stable event/effect identities make that explicitly
            # deduplicatable by the sink.
            if len(self._notification_attempts) >= self.notification_max_inflight:
                for completed_event_id, completed in list(
                    self._notification_attempts.items()
                ):
                    if completed["done"].is_set():
                        self._notification_attempts.pop(completed_event_id, None)
                    if (
                        len(self._notification_attempts)
                        < self.notification_max_inflight
                    ):
                        break
            active = sum(
                not attempt["done"].is_set()
                for attempt in self._notification_attempts.values()
            )
            if active >= self.notification_max_inflight:
                raise RuntimeError("strict notification in-flight capacity exhausted")
            attempt = {"done": threading.Event(), "errors": [], "thread": None}

            def deliver():
                try:
                    port.deliver(envelope)
                except BaseException as exc:  # noqa: BLE001 — child-thread outcome.
                    # BaseException in this child thread is a delivery failure,
                    # not a process-control signal for the coordinator caller.
                    # Recording it prevents SystemExit/KeyboardInterrupt from
                    # being mistaken for a successful strict effect and ACKed.
                    attempt["errors"].append(exc)
                finally:
                    attempt["done"].set()

            worker = threading.Thread(
                target=deliver,
                name=f"omd-notify-{envelope.get('event_id', 'unknown')}",
                daemon=True,
            )
            attempt["thread"] = worker
            self._notification_attempts[event_id] = attempt
            try:
                worker.start()
            except BaseException:
                if self._notification_attempts.get(event_id) is attempt:
                    self._notification_attempts.pop(event_id, None)
                attempt["done"].set()
                raise
        if not attempt["done"].wait(self.notification_timeout):
            raise TimeoutError(
                "strict notification attempt exceeded "
                f"{self.notification_timeout:g}s"
            )
        with self._notification_attempt_lock:
            if self._notification_attempts.get(event_id) is attempt:
                self._notification_attempts.pop(event_id, None)
        if attempt["errors"]:
            raise_delivery_error(attempt["errors"][0])

    def flush_admission_outbox(self, *, limit=100, now=None, _schedule=True):
        """Run one lifecycle-fenced public outbox drain.

        The timer dispatcher calls the private drain after registering itself;
        manual callers register here.  close() closes the shared gate and joins
        every accepted caller before allowing a coordinator handoff.
        """
        current = threading.current_thread()
        with self._outbox_timer_lock:
            if self._outbox_closed:
                raise RuntimeError("admission outbox dispatcher is closed")
            self._outbox_active_threads.add(current)
        try:
            # Serialize with the timer worker.  This makes an explicit flush a
            # deterministic readback barrier instead of returning while a timer
            # still owns due rows as DELIVERING.
            with self._outbox_dispatch_lock:
                with self._outbox_timer_lock:
                    if self._outbox_closed:
                        raise RuntimeError("admission outbox dispatcher is closed")
                return self._drain_admission_outbox(
                    limit=limit, now=now, _schedule=_schedule
                )
        finally:
            with self._outbox_timer_lock:
                self._outbox_active_threads.discard(current)

    def _drain_admission_outbox(self, *, limit=100, now=None, _schedule=True):
        """Deliver due admission notifications with leased at-least-once replay."""
        if not isinstance(limit, int) or isinstance(limit, bool) or limit <= 0:
            raise ValueError("admission outbox delivery limit must be positive")
        if not self._has_admission_notifier():
            with self._cs():
                stats = self.store.admission_outbox_stats()
                return {
                    "delivered": 0,
                    "failed": 0,
                    "delivered_total": stats["delivered"],
                    "pending": stats["pending"],
                    "delivering": stats["delivering"],
                    "head": stats["head"],
                }
        delivered = failed = 0
        for _ in range(limit):
            observed_at = time.time() if now is None else now
            with self._cs():
                row = self.store.claim_next_admission_outbox(
                    self.instance_id,
                    observed_at,
                    lease_ttl=ADMISSION_OUTBOX_LEASE_TTL,
                )
            if row is None:
                break
            claim_token = row["claim_token"]
            error = None
            try:
                envelope = json.loads(row["payload"])
                if canonical_json(envelope) != row["payload"]:
                    raise ValueError("notification payload is non-canonical")
                if sha256_json(envelope) != row["payload_sha256"]:
                    raise ValueError("notification payload digest mismatch")
                expected_schema = OUTBOX_NOTIFICATION_SCHEMAS.get(
                    row["transition_kind"]
                )
                if expected_schema is None:
                    raise ValueError("notification transition is not durable")
                if envelope.get("notification_schema") != expected_schema:
                    raise ValueError("notification schema mismatch")
                if row["schema_version"] != expected_schema:
                    raise ValueError("outbox row schema mismatch")
                if envelope.get("event_id") != row["event_id"]:
                    raise ValueError("notification event identity mismatch")
                for field in (
                    "repository_id",
                    "request_id",
                    "request_generation",
                    "orbit_id",
                ):
                    if envelope.get(field) != row[field]:
                        raise ValueError(
                            f"notification {field} does not match outbox row"
                        )
                if envelope.get("semantic_event") != row["transition_kind"]:
                    raise ValueError("notification transition does not match outbox row")
                expected_event = OUTBOX_NOTIFICATION_EVENTS.get(
                    row["transition_kind"]
                )
                if expected_event is None or envelope.get("event") != expected_event:
                    raise ValueError("notification event does not match transition")
                if (
                    envelope.get("cid") != row["correlation_id"]
                    or envelope.get("correlation_id") != row["correlation_id"]
                    or envelope.get("cycle_id") != row["correlation_id"]
                ):
                    raise ValueError("notification correlation identity mismatch")
                effect_prefix = (
                    "admission-effect-"
                    if expected_schema == ADMISSION_NOTIFICATION_SCHEMA
                    else "coordination-effect-"
                )
                expected_effect_key = effect_prefix + sha256_json({
                    "schema": expected_schema,
                    "repository_id": row["repository_id"],
                    "request_id": row["request_id"],
                    "request_generation": row["request_generation"],
                    "orbit_id": row["orbit_id"],
                    "transition_kind": row["transition_kind"],
                })
                if (
                    row["effect_key"] != expected_effect_key
                    or envelope.get("effect_key") != expected_effect_key
                ):
                    raise ValueError("notification effect identity mismatch")
                self._deliver_admission_notification(envelope)
            except Exception as exc:  # noqa: BLE001 — durable retry owns failure.
                error = f"{type(exc).__name__}: {exc}"[:1000]
            finished_at = time.time() if now is None else now
            if error is None:
                with self._cs():
                    if not self.store.ack_admission_outbox(
                        row["event_id"], claim_token, finished_at
                    ):
                        raise RuntimeError(
                            "admission outbox claim token was lost before ACK"
                        )
                delivered += 1
                continue
            delay = min(
                ADMISSION_OUTBOX_MAX_RETRY_DELAY,
                float(2 ** min(max(int(row["attempts"]) - 1, 0), 6)),
            )
            with self._cs():
                if not self.store.retry_admission_outbox(
                    row["event_id"], claim_token, finished_at + delay, error
                ):
                    raise RuntimeError(
                        "admission outbox claim token was lost before retry"
                    )
            failed += 1
        with self._cs():
            stats = self.store.admission_outbox_stats()
            result = {
                "delivered": delivered,
                "failed": failed,
                "delivered_total": stats["delivered"],
                "pending": stats["pending"],
                "delivering": stats["delivering"],
                "head": stats["head"],
            }
        if _schedule:
            self._schedule_next_admission_outbox()
        return result

    # ---- D14 코디네이터 singleton / HA 입장 (§D14) ----
    @staticmethod
    def _leader_alive(cur, now=None) -> bool:
        if cur is None:
            return False
        now = time.time() if now is None else now
        incumbent_ttl = cur.get("ttl", LEADER_TTL_S)
        return (now - cur.get("last_heartbeat", 0)) <= incumbent_ttl

    def _acquire_leadership(self):
        """기동 시 리더 lease 획득. 살아있는 다른 코디네이터(heartbeat 가 TTL 안)가 있으면
        CoordinatorConflict 로 거부 — 한 DB 에 코디네이터 둘(=writer 둘)을 막는다.
        죽은(=heartbeat 가 TTL 초과한) 리더는 takeover(epoch +1 로 fence — 옛 리더가 GC-pause
        뒤 깨어나 변이하려 해도 stale leader_epoch 로 차단). CAS 는 _cs(BEGIN IMMEDIATE) 안에서
        돌아 동시 기동 둘 중 하나만 성공한다(멀티프로세스 row-lock + 단일 writer)."""
        with self._cs(leader_guard=False):
            now = time.time()
            cur = self.store.get_leader()
            if cur is not None:
                # coordinator_id는 재사용 가능한 관측 라벨이다. 같은 라벨의
                # 새 process instance도 live incumbent가 아니므로 항상 거부한다.
                if self._leader_alive(cur, now):
                    self._emit("leader_conflict", self.coordinator_id,
                               incumbent=cur.get("coordinator_id"),
                               last_heartbeat=cur.get("last_heartbeat"))
                    raise CoordinatorConflict(
                        f"another live coordinator holds the leader lease "
                        f"(incumbent={cur.get('coordinator_id')}, "
                        f"epoch={cur.get('epoch')}); refusing to start a second "
                        f"coordinator on the same DB (§D14 single-instance).")
            prev_epoch = cur["epoch"] if cur else None
            new_epoch = (cur["epoch"] + 1) if cur else 1
            lease = {"coordinator_id": self.coordinator_id, "epoch": new_epoch,
                     "started_at": now, "last_heartbeat": now, "ttl": self.leader_ttl}
            if not self.store.cas_leader(prev_epoch, lease):
                # 다른 코디네이터가 우리 검사~CAS 사이에 끼어들어 lease 를 가져감 — 거부.
                raise CoordinatorConflict(
                    "leader lease was taken concurrently during startup (§D14).")
            self.leader_epoch = new_epoch
            self._emit("leader_acquired", self.coordinator_id, epoch=new_epoch,
                       took_over_from=(cur.get("coordinator_id") if cur else None),
                       took_over=(cur is not None))

    def _assert_leader(self):
        """현 프로세스가 여전히 리더인지 확인(다른 코디네이터가 takeover 했으면 fence-out).
        리더 lease 가 우리 epoch/id 가 아니면 우리는 좀비 리더 — 어떤 변이도 하면 안 된다.
        _cs() 안에서 변이 직전 호출(write-fence)."""
        cur = self.store.get_leader()
        if (cur is None or cur.get("coordinator_id") != self.coordinator_id
                or cur.get("epoch") != self.leader_epoch):
            self._emit("leader_fenced_out", self.coordinator_id,
                       my_epoch=self.leader_epoch,
                       current=(cur.get("epoch") if cur else None))
            raise CoordinatorConflict(
                f"coordinator {self.coordinator_id} (epoch={self.leader_epoch}) is no "
                f"longer leader — another coordinator took over. Refusing to mutate.")

    def coordinator_heartbeat(self) -> dict:
        """리더 lease keepalive(권장 주기 = leader_ttl/3). 먼저 우리가 여전히 리더인지 확인
        (takeover 됐으면 거부) → last_heartbeat 갱신. 이걸 멈추면(프로세스 사망/hang) TTL 후
        다른 코디네이터가 takeover 할 수 있다(영구 점유 불가)."""
        with self._cs():
            self._assert_leader()
            cur = self.store.get_leader()
            cur["last_heartbeat"] = time.time()
            self.store.write_leader(cur)
            return {"ok": True, "coordinator_id": self.coordinator_id,
                    "epoch": self.leader_epoch}

    def resign(self) -> dict:
        """자발적 리더십 반납(graceful shutdown). lease 를 비워(epoch 유지) 다음 코디네이터가
        TTL 대기 없이 즉시 takeover. 우리가 리더가 아니면 no-op."""
        with self._cs(leader_guard=False):
            cur = self.store.get_leader()
            if (cur is None or cur.get("coordinator_id") != self.coordinator_id
                    or cur.get("epoch") != self.leader_epoch):
                return {"ok": True, "noop": True}
            # last_heartbeat=0 으로 만들어 즉시 만료 처리(epoch 는 보존 → 다음 리더가 +1).
            cur["last_heartbeat"] = 0
            self.store.write_leader(cur)
            self.leader_epoch = None
            self._emit("leader_resigned", self.coordinator_id)
            return {"ok": True, "coordinator_id": self.coordinator_id}

    # ---- 내부 (모두 임계구역 안에서 호출됨) ----
    def _conflicts(self, pathspec, mode) -> list[str]:
        """pathspec/mode가 충돌하는 활성 HELD 궤도 id들. read↔read 공존; shared↔shared 공존
        (P2 hot 공유파일 레인 — 응결은 3-way, 배타 write/read 와 겹치면 여전히 충돌)."""
        return [
            o["orbit_id"]
            for o in self.store.held_orbits()
            if exact_conflict(pathspec, mode, o)
        ]

    def _rank_observation(self, row, observed_at):
        """Return persisted rank inputs plus the v1/v2 effective priority."""
        base_priority = int(row.get("priority") or 0)
        policy_version = row.get("policy_version")
        effective_priority = self.admission_policy.effective_priority(
            base_priority,
            policy_version=policy_version,
            enqueued_at=row.get("enqueued_at"),
            observed_at=observed_at,
            allow_unenqueued=row.get("state") != "PENDING",
        )
        return {
            "base_priority": base_priority,
            "effective_priority": effective_priority,
            "observed_at": observed_at,
            "policy_version": policy_version,
        }

    def _ordered_pending(self, observed_at, pending=None):
        """Order valid ranks dynamically; corrupt/unknown authority sorts first."""
        rows = list(self.store.pending_orbits() if pending is None else pending)

        def key(row):
            rank = self.admission_policy.rank_key(
                int(row.get("priority") or 0),
                row.get("queue_seq"),
                policy_version=row.get("policy_version"),
                enqueued_at=row.get("enqueued_at"),
                observed_at=observed_at,
            )
            if rank is None:
                return (0, 0, int(row.get("queue_seq") or -1), row["orbit_id"])
            return (1, rank[0], rank[1], row["orbit_id"])

        return sorted(rows, key=key)

    def _admission_decision(
        self,
        pathspec,
        mode,
        priority,
        queue_seq,
        orbit_id=None,
        *,
        policy_version=None,
        enqueued_at=None,
        observed_at,
    ):
        """Shared initial-claim/promotion decision over one authority snapshot."""
        held = self.store.held_orbits()
        pending = self.store.pending_orbits()
        request = AdmissionRequest.build(
            pathspec,
            mode,
            priority,
            queue_seq,
            orbit_id=orbit_id,
            policy_version=policy_version or self.admission_policy.version,
            enqueued_at=enqueued_at,
        )
        decision = decide_admission(
            request,
            held,
            pending,
            policy=self.admission_policy,
            observed_at=observed_at,
        )
        self._emit(
            "admission_candidates_scanned",
            orbit_id or "admission-preview",
            repository_id=self.repository_id,
            mode=mode,
            **decision.candidate_scan.as_dict(),
        )
        snapshot_hash = authority_snapshot_hash(
            held,
            pending,
            coordinator_epoch=self.leader_epoch,
            policy=self.admission_policy,
            observed_at=observed_at,
        )
        return decision, snapshot_hash

    def _admission_identity(self, row=None, **values):
        source = dict(row or {})
        source.update(values)
        orbit_id = source["orbit_id"]
        request_id = source.get("request_id") or f"internal:{orbit_id}"
        digest = source.get("pathspec_digest")
        if digest is None:
            raw_paths = source["pathspec"]
            digest = pathspec_digest(
                json.loads(raw_paths) if isinstance(raw_paths, str) else raw_paths
            )
        return {
            "repository_id": self.repository_id,
            "request_id": request_id,
            "orbit_id": orbit_id,
            "request_generation": int(source.get("request_generation") or 0),
            "owner_agent": source["agent_id"],
            "bail_epoch": int(source.get("bail_epoch") or 0),
            "mode": source["mode"],
            "pathspec_digest": digest,
            "policy_version": (
                source.get("policy_version") or self.admission_policy.version
            ),
        }

    def _admission_payload(self, event_type, identity, snapshot_hash, **variant):
        payload = {
            **identity,
            "actor": self.coordinator_id,
            "event_id": f"evt-{uuid.uuid4().hex}",
            "authority_snapshot_hash": snapshot_hash,
            **variant,
        }
        return bind_decision_id(event_type, payload)

    def _enqueue_admission_notification(
        self, event_type, payload, context, *, predecessor_event_ids=()
    ):
        telemetry_event = OUTBOX_NOTIFICATION_EVENTS.get(event_type)
        if telemetry_event is None:
            return None
        notification_schema = OUTBOX_NOTIFICATION_SCHEMAS[event_type]
        required = (
            "repository_id",
            "request_id",
            "request_generation",
            "orbit_id",
            "event_id",
        )
        missing = [key for key in required if key not in payload]
        if missing:
            raise RuntimeError(
                f"admission notification identity missing: {missing}"
            )
        effect_prefix = (
            "admission-effect-"
            if notification_schema == ADMISSION_NOTIFICATION_SCHEMA
            else "coordination-effect-"
        )
        effect_key = effect_prefix + sha256_json({
            "schema": notification_schema,
            "repository_id": payload["repository_id"],
            "request_id": payload["request_id"],
            "request_generation": payload["request_generation"],
            "orbit_id": payload["orbit_id"],
            "transition_kind": event_type,
        })
        correlation_id = (
            context.get("owner_agent")
            or payload.get("owner_agent")
            or payload["request_id"]
        )
        envelope = {
            "cid": correlation_id,
            "correlation_id": correlation_id,
            "cycle_id": correlation_id,
            "service": getattr(self.events, "service", "omd"),
            "event": telemetry_event,
            "notification_schema": notification_schema,
            "semantic_event": event_type,
            "effect_key": effect_key,
            "promoted": event_type == "PROMOTION_GRANTED",
            "deadlock": event_type == "PROMOTION_DENIED",
            **payload,
        }
        encoded = canonical_json(envelope)
        created_at = (
            float(payload["observed_at"])
            if payload.get("observed_at") is not None
            else time.time()
        )
        self.store.enqueue_admission_outbox(
            event_id=payload["event_id"],
            effect_key=effect_key,
            schema_version=notification_schema,
            repository_id=payload["repository_id"],
            request_id=payload["request_id"],
            request_generation=payload["request_generation"],
            orbit_id=payload["orbit_id"],
            transition_kind=event_type,
            correlation_id=correlation_id,
            payload=encoded,
            payload_sha256=sha256_json(envelope),
            created_at=created_at,
            predecessor_event_ids=predecessor_event_ids,
        )
        self.store.after_commit(
            self._wake_admission_outbox,
            key=("admission-outbox", id(self)),
        )
        return effect_key

    def _enqueue_agent_reclaimed_notification(
        self,
        agent_id,
        final_orbit,
        *,
        voluntary,
        observed_at,
        orbit_count,
        task_count,
        predecessor_event_ids=(),
    ):
        predecessor_event_ids = tuple(sorted(set(predecessor_event_ids)))
        if final_orbit is None:
            agent = self.store.get_agent(agent_id)
            bail_epoch = int((agent or {}).get("bail_epoch") or 0)
            stream_identity = {
                "repository_id": self.repository_id,
                "agent_id": agent_id,
                "bail_epoch": bail_epoch,
            }
            final_orbit = {
                "request_id": f"internal:agent-reclaim:{agent_id}",
                "request_generation": bail_epoch,
                "orbit_id": "agent-reclaim:" + sha256_json(stream_identity),
                "authority_snapshot_hash": authority_snapshot_hash(
                    self.store.held_orbits(),
                    self.store.pending_orbits(),
                    coordinator_epoch=self.leader_epoch,
                ),
            }
        payload = {
            "repository_id": self.repository_id,
            "request_id": final_orbit.get("request_id")
            or f"internal:{final_orbit['orbit_id']}",
            "request_generation": int(final_orbit.get("request_generation") or 0),
            "orbit_id": final_orbit["orbit_id"],
            "owner_agent": agent_id,
            "actor": self.coordinator_id,
            "event_id": f"evt-{uuid.uuid4().hex}",
            "authority_snapshot_hash": final_orbit["authority_snapshot_hash"],
            "voluntary": bool(voluntary),
            "orbits": int(orbit_count),
            "tasks": int(task_count),
            "observed_at": observed_at,
            "predecessor_event_ids": list(predecessor_event_ids),
        }
        return self._enqueue_admission_notification(
            "AGENT_RECLAIMED",
            payload,
            {"owner_agent": agent_id},
            predecessor_event_ids=predecessor_event_ids,
        )

    def _assert_admission_projection(
        self, context, event_type, payload, snapshot_hash, expected
    ):
        reduced = admission_step(
            context,
            event_type,
            payload,
            trusted_authority_snapshot_hash=snapshot_hash,
        )
        if not reduced.accepted:
            raise RuntimeError(
                f"admission reducer rejected authority decision: {reduced.reason}"
            )
        projection = project_legacy(reduced.context["state"], reduced.context)
        if projection.state != expected:
            raise RuntimeError(
                f"admission projection mismatch: semantic={reduced.context['state']} "
                f"legacy={projection.state} expected={expected}"
            )
        self._enqueue_admission_notification(event_type, payload, reduced.context)
        return reduced

    def _reduce_orbit_lifecycle(self, orbit, event_type, **variant):
        """Bind a runtime maintenance event to the semantic OrbitRequest FSM."""
        expected = {
            "RENEW": ("HELD", "HELD"),
            "RELEASE": ("RELEASED", "RELEASED"),
            "LEASE_EXPIRED": ("EXPIRED", "EXPIRED"),
            "WAIT_OWNER_RECLAIMED": ("CANCELLED", "DENIED"),
            "LEASE_OWNER_RECLAIMED": ("EXPIRED", "EXPIRED"),
        }
        if event_type not in expected:
            raise ValueError(f"unsupported orbit lifecycle event: {event_type}")
        snapshot_hash = authority_snapshot_hash(
            self.store.held_orbits(),
            self.store.pending_orbits(),
            coordinator_epoch=self.leader_epoch,
        )
        identity = self._admission_identity(orbit)
        payload = {
            "repository_id": identity["repository_id"],
            "request_id": identity["request_id"],
            "orbit_id": identity["orbit_id"],
            "request_generation": identity["request_generation"],
            "actor": self.coordinator_id,
            "authority_snapshot_hash": snapshot_hash,
            "event_id": f"evt-{uuid.uuid4().hex}",
        }
        if event_type in {
            "RENEW", "RELEASE", "WAIT_OWNER_RECLAIMED", "LEASE_OWNER_RECLAIMED"
        }:
            payload.update(
                owner_agent=identity["owner_agent"],
                bail_epoch=identity["bail_epoch"],
            )
        if event_type in {
            "RENEW", "RELEASE", "LEASE_EXPIRED", "LEASE_OWNER_RECLAIMED"
        }:
            payload["fence"] = orbit["fence"]
        payload.update(variant)
        context = {**identity, "state": orbit["state"]}
        if orbit["fence"] is not None:
            context["fence"] = orbit["fence"]
        if event_type == "LEASE_EXPIRED":
            context["lease_deadline"] = orbit["expires_at"]
        semantic_state, legacy_state = expected[event_type]
        reduced = self._assert_admission_projection(
            context, event_type, payload, snapshot_hash, legacy_state
        )
        if reduced.context["state"] != semantic_state:
            raise RuntimeError(
                f"admission lifecycle mismatch: event={event_type} "
                f"semantic={reduced.context['state']} expected={semantic_state}"
            )
        return snapshot_hash, payload, reduced

    def _pending_owner_fresh(self, orbit, now):
        """Fail closed when a queued request no longer has a live owner."""
        agent = self.store.get_agent(orbit["agent_id"])
        if agent is None or agent["state"] != "WORKING":
            return False
        if not self.agent_ttl:
            return True
        ttl = agent["liveness_ttl"] or self.agent_ttl
        return agent["last_heartbeat"] >= now - ttl

    def _timeout_pending(self, now):
        """Project due semantic WAIT_TIMEOUT events to legacy DENIED rows."""
        timed_out = []
        for orbit in self.store.due_pending_orbits(now):
            held = self.store.held_orbits()
            pending = self.store.pending_orbits()
            snapshot_hash = authority_snapshot_hash(
                held, pending, coordinator_epoch=self.leader_epoch
            )
            identity = self._admission_identity(orbit)
            payload = {
                "repository_id": identity["repository_id"],
                "request_id": identity["request_id"],
                "orbit_id": identity["orbit_id"],
                "request_generation": identity["request_generation"],
                "actor": self.coordinator_id,
                "authority_snapshot_hash": snapshot_hash,
                "observed_at": now,
                "event_id": f"evt-{uuid.uuid4().hex}",
            }
            context = {
                **identity,
                "state": "PENDING",
                "queue_seq": orbit["queue_seq"],
                "wait_deadline": orbit["wait_deadline"],
            }
            self._assert_admission_projection(
                context, "WAIT_TIMEOUT", payload, snapshot_hash, "DENIED"
            )
            self.store.set_orbit(
                orbit["orbit_id"],
                state=fsm.advance("orbit", "PENDING", "deny"),
                released_at=now,
                authority_snapshot_hash=snapshot_hash,
                decision_id=None,
                decision_type="WAIT_TIMEOUT",
                blocker_ids=[],
                terminal_reason="wait_timeout",
            )
            timed_out.append(orbit["orbit_id"])
            self._emit(
                "orbit_timed_out",
                orbit["agent_id"],
                orbit_id=orbit["orbit_id"],
                queue_seq=orbit["queue_seq"],
                wait_deadline=orbit["wait_deadline"],
                observed_at=now,
            )
        return timed_out

    def _promote_pending(self, now=None):
        # Same pure decision table as claim(). Dynamic effective priority is
        # evaluated once at ``now``; disjoint requests may all promote in one pass.
        now = time.time() if now is None else now
        for o in self._ordered_pending(now):
            # Defense in depth: all public callers reconcile deadlines/owners
            # first, but a direct internal call must still never grant either.
            if o["wait_deadline"] is not None and o["wait_deadline"] <= now:
                continue
            if not self._pending_owner_fresh(o, now):
                continue
            rank_observation = self._rank_observation(o, now)
            if rank_observation["effective_priority"] is None:
                self._emit(
                    "orbit_policy_unavailable",
                    o["agent_id"],
                    orbit_id=o["orbit_id"],
                    policy_version=o.get("policy_version"),
                )
                continue
            decision, snapshot_hash = self._admission_decision(
                json.loads(o["pathspec"]),
                o["mode"],
                o["priority"],
                o["queue_seq"],
                orbit_id=o["orbit_id"],
                policy_version=o["policy_version"],
                enqueued_at=o["enqueued_at"],
                observed_at=now,
            )
            identity = self._admission_identity(o)
            if decision.grantable:
                fence = self.store.next_fence()
                # §D12: PENDING read-궤도가 뒤늦게 grant 될 때도 현 통합 gen 을 박는다.
                rg = self.store.integration_gen() if o["mode"] == "read" else ...
                ttl = o["requested_ttl"] if o["requested_ttl"] is not None else 600.0
                lease_deadline = now + ttl
                payload = self._admission_payload(
                    "PROMOTION_GRANTED",
                    identity,
                    snapshot_hash,
                    queue_seq=o["queue_seq"],
                    fence=fence,
                    lease_deadline=lease_deadline,
                    base_priority=decision.base_priority,
                    effective_priority=decision.effective_priority,
                    observed_at=decision.observed_at,
                )
                context = {
                    **identity,
                    "state": "PENDING",
                    "queue_seq": o["queue_seq"],
                    **rank_observation,
                }
                self._assert_admission_projection(
                    context, "PROMOTION_GRANTED", payload, snapshot_hash, "HELD"
                )
                self.store.set_orbit(
                    o["orbit_id"],
                    state=fsm.advance("orbit", "PENDING", "grant"),
                    expires_at=lease_deadline,
                    fence=fence,
                    read_gen=rg,
                    authority_snapshot_hash=snapshot_hash,
                    decision_id=payload["decision_id"],
                    decision_type="PROMOTION_GRANTED",
                    decision_schema="admission_decision/v2",
                    decision_observed_at=decision.observed_at,
                    decision_effective_priority=decision.effective_priority,
                    blocker_ids=[],
                )
                self._emit("orbit_granted", o["agent_id"], orbit_id=o["orbit_id"],
                           fence=fence, mode=o["mode"], promoted=True,
                           queue_seq=o["queue_seq"], decision_id=payload["decision_id"])
            else:
                blocker_fingerprint = sha256_json(list(decision.blocker_ids))
                payload = self._admission_payload(
                    "PROMOTION_BLOCKED",
                    identity,
                    snapshot_hash,
                    queue_seq=o["queue_seq"],
                    blocker_fingerprint=blocker_fingerprint,
                    base_priority=decision.base_priority,
                    effective_priority=decision.effective_priority,
                    observed_at=decision.observed_at,
                )
                context = {
                    **identity,
                    "state": "PENDING",
                    "queue_seq": o["queue_seq"],
                    **rank_observation,
                }
                self._assert_admission_projection(
                    context, "PROMOTION_BLOCKED", payload, snapshot_hash, "PENDING"
                )
                self.store.set_orbit(
                    o["orbit_id"],
                    authority_snapshot_hash=snapshot_hash,
                    decision_id=payload["decision_id"],
                    decision_type="PROMOTION_BLOCKED",
                    decision_schema="admission_decision/v2",
                    decision_observed_at=decision.observed_at,
                    decision_effective_priority=decision.effective_priority,
                    blocker_ids=list(decision.blocker_ids),
                )

    def _reconcile_admission(self, now=None, *, reclaim=True):
        """One ordering for release, connect, recovery, and periodic sweep.

        Safety depends on this sequence: expire old HELD authority, reclaim stale
        owners, terminate due PENDING requests, then consider promotion.
        """
        now = time.time() if now is None else now
        for orbit in self.store.due_orbits(now):
            snapshot_hash, _, _ = self._reduce_orbit_lifecycle(
                orbit, "LEASE_EXPIRED", observed_at=now
            )
            self.store.set_orbit(
                orbit["orbit_id"],
                state=fsm.advance("orbit", "HELD", "expire"),
                released_at=now,
                authority_snapshot_hash=snapshot_hash,
                decision_id=None,
                decision_type="LEASE_EXPIRED",
                terminal_reason="lease_expired",
            )
            self._emit("orbit_expired", orbit["agent_id"], orbit_id=orbit["orbit_id"])
        if reclaim and self.agent_ttl:
            self._reclaim_zombies_inline(now=now, promote=False)
        self._timeout_pending(now)
        self._deny_reservation_cycles(now)
        self._promote_pending(now)

    def _wait_for(self, observed_at, *, with_sources=False):
        """Combined HELD ownership + higher-ranked PENDING reservation graph."""
        held = self.store.held_orbits()
        pending = self.store.pending_orbits()
        by_id = {o["orbit_id"]: o for o in held + pending}
        edges: dict = {}
        edge_sources: dict[tuple[str, str], set[str]] = {}
        for p in pending:
            if self._rank_observation(p, observed_at)["effective_priority"] is None:
                # Unknown policy authority is fail-closed for promotion. It can
                # still block valid newer rows, but cannot assert its own edges.
                continue
            request = AdmissionRequest.build(
                json.loads(p["pathspec"]),
                p["mode"],
                p["priority"],
                p["queue_seq"],
                orbit_id=p["orbit_id"],
                policy_version=p["policy_version"],
                enqueued_at=p["enqueued_at"],
            )
            decision = decide_admission(
                request,
                held,
                pending,
                policy=self.admission_policy,
                observed_at=observed_at,
            )
            for blocker_id in decision.blocker_ids:
                blocker = by_id[blocker_id]
                if blocker["agent_id"] != p["agent_id"]:
                    source = p["agent_id"]
                    target = blocker["agent_id"]
                    edges.setdefault(source, set()).add(target)
                    edge_sources.setdefault((source, target), set()).add(
                        p["orbit_id"]
                    )
        if with_sources:
            return edges, edge_sources
        return edges

    def _cycle_with(self, node, observed_at) -> bool:
        """node가 wait-for 그래프에서 자기 자신으로 되돌아오는 사이클에 있나(데드락)."""
        edges = self._wait_for(observed_at)

        def dfs(n, path):
            for m in edges.get(n, ()):
                if m == node:
                    return True
                if m not in path and dfs(m, path | {m}):
                    return True
            return False

        return dfs(node, {node})

    def _deny_reservation_cycles(
        self,
        observed_at,
        *,
        reason="reservation_cycle_after_rank_change",
    ):
        """Resolve cycles introduced by time-varying rank before promotion.

        Aging can reverse a reservation edge after insertion, and a new HELD
        grant can add an ownership edge. Preserve older reservations by denying
        the largest queue_seq that participates in the discovered agent cycle,
        then recompute until the graph is acyclic.
        """
        denied = []
        while True:
            edges, edge_sources = self._wait_for(
                observed_at, with_sources=True
            )
            cycle = self._find_cycle(edges)
            if cycle is None:
                return denied
            source_ids = set()
            for source, target in zip(cycle, cycle[1:]):
                source_ids.update(edge_sources.get((source, target), ()))
            candidates = [
                self.store.get_orbit(orbit_id) for orbit_id in source_ids
            ]
            candidates = [
                row for row in candidates if row is not None and row["state"] == "PENDING"
            ]
            if not candidates:
                raise RuntimeError(
                    "reservation cycle has no authoritative PENDING source"
                )
            victim = max(
                candidates,
                key=lambda row: (int(row["queue_seq"]), row["orbit_id"]),
            )
            decision, snapshot_hash = self._admission_decision(
                json.loads(victim["pathspec"]),
                victim["mode"],
                victim["priority"],
                victim["queue_seq"],
                orbit_id=victim["orbit_id"],
                policy_version=victim["policy_version"],
                enqueued_at=victim["enqueued_at"],
                observed_at=observed_at,
            )
            identity = self._admission_identity(victim)
            payload = self._admission_payload(
                "PROMOTION_DENIED",
                identity,
                snapshot_hash,
                queue_seq=victim["queue_seq"],
                reason=reason,
                base_priority=decision.base_priority,
                effective_priority=decision.effective_priority,
                observed_at=decision.observed_at,
            )
            context = {
                **identity,
                "state": "PENDING",
                "queue_seq": victim["queue_seq"],
                **self._rank_observation(victim, observed_at),
            }
            self._assert_admission_projection(
                context,
                "PROMOTION_DENIED",
                payload,
                snapshot_hash,
                "DENIED",
            )
            self.store.set_orbit(
                victim["orbit_id"],
                state=fsm.advance("orbit", "PENDING", "deny"),
                released_at=observed_at,
                authority_snapshot_hash=snapshot_hash,
                decision_id=payload["decision_id"],
                decision_type="PROMOTION_DENIED",
                decision_schema="admission_decision/v2",
                decision_observed_at=decision.observed_at,
                decision_effective_priority=decision.effective_priority,
                blocker_ids=list(decision.blocker_ids),
                terminal_reason=reason,
            )
            denied.append(victim["orbit_id"])
            self._emit(
                "orbit_denied",
                victim["agent_id"],
                orbit_id=victim["orbit_id"],
                deadlock=True,
                dynamic_rank_cycle=True,
                queue_seq=victim["queue_seq"],
                decision_id=payload["decision_id"],
            )

    # ---- task 의존 DAG 사이클 게이트 (§D7, P0-10) ----
    def _dep_graph(self, extra_edges=None) -> dict:
        """task→deps 의존 그래프(엣지 t→d = 'd가 t보다 먼저'). DB의 모든 task `deps` +
        선택적 `extra_edges`(예: 추가하려는 후보 엣지)를 합친다. 임계구역 안에서만 호출."""
        g: dict = {}
        for t in self.store.all_tasks():
            g.setdefault(t["task_id"], set())
            for d in json.loads(t["deps"] or "[]"):
                g.setdefault(t["task_id"], set()).add(d)
        for (src, dst) in (extra_edges or []):
            g.setdefault(src, set()).add(dst)
        return g

    def _find_cycle(self, graph) -> list[str] | None:
        """방향 그래프에 사이클이 있으면 그 사이클 경로(노드 리스트)를, 없으면 None.
        DFS 색칠(WHITE/GRAY/BLACK) — GRAY 노드로 되돌아가는 back-edge가 사이클."""
        WHITE, GRAY, BLACK = 0, 1, 2
        color = {n: WHITE for n in graph}
        stack: list[str] = []

        def visit(n):
            color[n] = GRAY
            stack.append(n)
            for m in sorted(graph.get(n, ()), key=str):
                c = color.get(m, WHITE)
                if c == GRAY:
                    # back-edge → 사이클. stack[m..] + m 닫힘.
                    return stack[stack.index(m):] + [m]
                if c == WHITE:
                    cyc = visit(m)
                    if cyc:
                        return cyc
            color[n] = BLACK
            stack.pop()
            return None

        for n in sorted(graph, key=str):
            if color.get(n, WHITE) == WHITE:
                cyc = visit(n)
                if cyc:
                    return cyc
        return None

    def _would_cycle(self, task_id, deps) -> list[str] | None:
        """task_id 가 `deps`(after-목록)를 가질 때 의존 그래프에 사이클이 생기면 그 경로,
        아니면 None. self-dep(task_id ∈ deps)는 길이-1 사이클로 잡힌다. 후보 task가 아직
        DB에 없어도(declare 직전) 후보 엣지로 가상 추가해 전역 재검(Kahn/DFS 동치)."""
        extra = [(task_id, d) for d in (deps or [])]
        g = self._dep_graph(extra_edges=extra)
        g.setdefault(task_id, set())
        return self._find_cycle(g)

    def _sweep_inline(self, now=None):
        """임계구역 안에서 도는 sweep 본체(만료 회수 + 좀비 회수 + promote). tx 자기관리 안 함."""
        now = time.time() if now is None else now
        self._reconcile_admission(now)
        # D3(§1.2): TTL 만료된 flag_ephemeral lease — 보유자가 renew 안 함(GC-pause/사망) →
        # 받쳐주던 EPHEMERAL 플래그를 BROKEN(자동 clear) + 대기자 PRODUCER_DEAD 기상. 영구 hang 0.
        for fl in self.store.due_flag_leases(now):
            self._break_ephemeral_flags_for_lease(fl["orbit_id"], reason="producer_dead")
            self.store.set_orbit(fl["orbit_id"],
                                 state=fsm.advance("orbit", "HELD", "expire"))
            self._emit("flag_lease_expired", fl["agent_id"], orbit_id=fl["orbit_id"])
        # D4(§1.2): TTL 만료된 sem_permit — 보유자가 renew/heartbeat 안 함(GC-pause/사망) →
        # permit EXPIRE → 가용 = max − count(ACTIVE) 가 구조적으로 복구(누수 0). 정수 카운터를
        # 쓰면 죽을 때마다 새서 결국 0(영구 정지)인 고전 버그를 permit=lease 로 원천 차단.
        expired_sems = set()
        for p in self.store.due_sem_permits(now):
            self.store.set_orbit(p["orbit_id"],
                                 state=fsm.advance("orbit", "HELD", "expire"),
                                 released_at=now)
            self._emit("sem_permit_expired", p["agent_id"], orbit_id=p["orbit_id"],
                       sem=p["resource_key"])
            expired_sems.add(p["resource_key"])
        for sem_id in expired_sems:
            self._promote_sem_waiters(sem_id)  # 복구된 슬롯을 줄선 순서대로(no-overtaking) 부여
        # D5(§D5/§1.2): ARMED 배리어의 사망(write-lease 만료=참가자 GC-pause/죽음)·타임아웃을
        # break/shrink 로 반영 — 누군가 sweep 하면(poll/status/arrive) 반영되므로 영구 hang 0.
        # 트립 plan(전원 도착)은 sweep 에서 실행하지 않는다(merge 는 락 밖이라 arrive 가 돌린다).
        for b in self.store.all_barriers(states=("ARMED",)):
            self._barrier_eval(b["barrier_id"])
        # §D9 멱등 캐시 GC: idem_ttl 지난 DONE 행 정리(무한누적 차단). INFLIGHT(진행중)은
        # completed_at NULL 로 보존. now 는 위에서 이미 정의됨(시각 일관).
        if self.idem_ttl:
            self.store.gc_idem(now - self.idem_ttl)
        return now

    def _reclaim_zombies_inline(self, *, now=None, promote=True):
        """heartbeat 끊긴 물방울(involuntary) — 단일 회수 루틴으로 위임.
        F2: 생존창은 per-agent(liveness_ttl 선언, 미선언=agent_ttl) — 판정은 store 쿼리가 원자."""
        if not self.agent_ttl:
            return []
        out = []
        now = time.time() if now is None else now
        for a in self.store.stale_agents(now, self.agent_ttl):
            # Phase B merge/check subprocess는 coordinator가 직접 관측하고 write-orbit에 유계 pin을
            # 박는다. 그 한가운데서 heartbeat만 보고 회수하면 checker와 abort가 동시에 달린다.
            active_connect_pin = any(
                t["state"] == "CONNECTING" and any(
                    o["merging"]
                    for o in self.store.pinned_orbits_for_task(t["task_id"])
                )
                for t in self.store.tasks_for_agent(a["agent_id"])
            )
            if active_connect_pin:
                continue
            self._reclaim_agent_inline(
                a["agent_id"], voluntary=False, now=now, promote=promote
            )
            out.append(a["agent_id"])
        return out

    def _reclaim_agent_inline(
        self, agent_id, *, voluntary, now=None, promote=True
    ):
        """긴급탈출(voluntary `bail`) / 좀비회수(involuntary) **단일 루틴** (D2).
        이 agent가 쥔 모든 궤도(HELD/PENDING)를 해제하고, 진행중 작업(CLAIMED/IN_ORBIT/CONNECTING)을
        requeue하고, worktree+브랜치를 정리하고, agent를 RETIRE한다 → 어떤 보유물도 고아가 안 된다.
        멱등 — 도중 죽어도 sweeper가 같은 루틴으로 마저 정리(이중해제·누락 없음)."""
        now = time.time() if now is None else now
        ag = self.store.get_agent(agent_id)
        if ag is None or ag["state"] == "RETIRED":
            return {"agent": agent_id, "noop": True}
        self.store.set_agent_state(agent_id, "BAILING" if voluntary else "ZOMBIE")
        # §D6: bail_epoch bump — 회수 전 epoch를 든 GC-pause 좀비가 살아나도 모든 변이가
        # stale bail_epoch로 FENCED_OUT (부활 방지). state 리셋(heartbeat)으로 못 우회.
        self.store.bump_bail_epoch(agent_id)
        freed, requeued, release_event_ids = [], [], []
        # 죽은 보유자의 merge_token: dangling merge를 abort 후 토큰 반납(§D11/§E).
        for mt in self.store.merge_tokens_owned_by(agent_id, ("HELD",)):
            self._abort_dangling_merge(mt)
            self.store.set_orbit(mt["orbit_id"],
                                 state=fsm.advance("orbit", "HELD", "expire"),
                                 released_at=now)
            self._emit("merge_token_reclaimed", agent_id, orbit_id=mt["orbit_id"])
        # 죽은 보유자의 flag_ephemeral lease: 받쳐주던 EPHEMERAL 플래그를 BROKEN(자동 clear)
        # + lease EXPIRE + 대기자 PRODUCER_DEAD 기상(§1.2 — "작업중 플래그 영구 잔존" 해소).
        for fl in self.store.flag_leases_owned_by(agent_id, ("HELD",)):
            self._break_ephemeral_flags_for_lease(fl["orbit_id"], reason="producer_dead")
            self.store.set_orbit(fl["orbit_id"],
                                 state=fsm.advance("orbit", "HELD", "expire"),
                                 released_at=now)
            self._emit("flag_lease_reclaimed", agent_id, orbit_id=fl["orbit_id"])
        # 죽은 보유자의 sem_permit: EXPIRE → 가용 슬롯 복구(누수 0, §D4). 복구된 세마포어의
        # 대기자를 줄선 순서로 부여(no-overtaking, §D7) — 영구 hang/기아 없음.
        reclaimed_sems = set()
        for p in self.store.sem_permits_owned_by(agent_id, ("HELD",)):
            self.store.set_orbit(p["orbit_id"],
                                 state=fsm.advance("orbit", "HELD", "expire"),
                                 released_at=now)
            self._emit("sem_permit_reclaimed", agent_id, orbit_id=p["orbit_id"],
                       sem=p["resource_key"])
            reclaimed_sems.add(p["resource_key"])
        # 이 agent 가 어딘가 줄서 있었으면(아직 permit 못 받음) 그 대기 등록을 취소.
        for w in self.store.db.execute(
                "SELECT * FROM sem_waiters WHERE agent_id=? AND state='WAITING'",
                (agent_id,)).fetchall():
            self.store.set_sem_waiter(w["waiter_id"], state="CANCELLED")
        for o in self.store.orbits_owned_by_agent(agent_id, ("HELD", "PENDING")):
            reclaim_reason = "bail" if voluntary else "reclaim"
            trig = "expire" if o["state"] == "HELD" else "deny"  # HELD→EXPIRED, PENDING→DENIED
            if o["state"] == "HELD":
                decision_type = "LEASE_OWNER_RECLAIMED"
                terminal_reason = "lease_owner_reclaimed"
                snapshot_hash, release_payload, _ = self._reduce_orbit_lifecycle(
                    o, decision_type, observed_at=now, reason=reclaim_reason
                )
            else:
                decision_type = "WAIT_OWNER_RECLAIMED"
                terminal_reason = "wait_owner_reclaimed"
                snapshot_hash, release_payload, _ = self._reduce_orbit_lifecycle(
                    o, decision_type, observed_at=now, no_lease_fence=0,
                    reason=reclaim_reason,
                )
            release_event_ids.append(release_payload["event_id"])
            # merging pin은 회수와 함께 해제(§E pin은 유계 — 보유자 사망도 한 경계).
            self.store.set_orbit(
                o["orbit_id"],
                state=fsm.advance("orbit", o["state"], trig),
                released_at=now,
                merging=0,
                authority_snapshot_hash=snapshot_hash,
                decision_id=None,
                decision_type=decision_type,
                blocker_ids=[],
                terminal_reason=terminal_reason,
            )
            # §D12: 회수되는 read-궤도의 stale 신호 플래그도 청산(LIVE 누수 방지). 보유자가
            # 죽었으므로 connect 차단은 어차피 부활차단(bail_epoch)이 맡는다.
            if o["mode"] == "read":
                self._clear_read_stale_signal(o["orbit_id"])
            freed.append(o["orbit_id"])
            self._emit("orbit_released", agent_id, orbit_id=o["orbit_id"],
                       reason=reclaim_reason)
        # §D5/§3.D: 이 agent 의 **모든** task 를 멤버로 둔 활성 배리어를 재평가 대상에 모은다.
        # 이 agent 의 write-orbit 이 위에서 이미 해제됐으므로(lease 사망), 그 task 가 requeue
        # 되든(IN_ORBIT 등) 안 되든(이미 DONE) 배리어 입장에선 참가자 사망이다 → break/shrink.
        affected_barriers = set()
        for t in self.store.tasks_for_agent(agent_id):
            for b in self.store.barriers_with_task(t["task_id"]):
                affected_barriers.add(b["barrier_id"])
            if t["state"] in ("CLAIMED", "IN_ORBIT", "CONNECTING"):  # CONNECTING 포함(P0-9)
                s = fsm.advance("task", t["state"], "abort")
                s = fsm.advance("task", s, "requeue")  # ABORTED→PENDING
                self.store.set_task(t["task_id"], state=s, agent_id=None)
                requeued.append(t["task_id"])
                if self.git and t["worktree"]:
                    self.git.remove_worktree(t["worktree"])
                    if t["branch"]:
                        self.git.delete_branch(t["branch"])  # P0-8: 안 지우면 다음 start() 막힘
        # 죽은 참가자가 든 배리어 재평가(can_trip=False — reclaim 중엔 merge 안 함; 이 경로는
        # 사망이라 절대 fill 되지 않고 break/shrink 만 일어난다). 영구 hang 0(전원 BROKEN 기상).
        for bid in affected_barriers:
            self._barrier_eval(bid)
        self.store.set_agent_state(agent_id, "RETIRED")
        self._enqueue_agent_reclaimed_notification(
            agent_id,
            self.store.get_orbit(freed[-1]) if freed else None,
            voluntary=voluntary,
            observed_at=now,
            orbit_count=len(freed),
            task_count=len(requeued),
            predecessor_event_ids=release_event_ids,
        )
        for sem_id in reclaimed_sems:
            self._promote_sem_waiters(sem_id)  # 복구된 슬롯을 줄선 순서로 부여(§D7)
        if promote:
            self._reconcile_admission(now=now, reclaim=False)
        return {"agent": agent_id, "voluntary": voluntary, "orbits": freed, "tasks": requeued}

    def _check_owner(self, o, agent_id, fence):
        """소유+fence 가드(D6). 통과면 None, 아니면 거부 dict. 오추방된 좀비/타 agent 차단."""
        if o["agent_id"] != agent_id:
            return {"ok": False, "reason": "not owner", "owner": o["agent_id"]}
        if o["fence"] != fence:
            return {"ok": False, "reason": "stale fence", "fenced_out": True,
                    "current": o["fence"], "yours": fence}
        return None

    def _check_task_write_fence(self, task_id, agent_id, fence):
        """finish/commit/connect의 D6 가드(opt-in): caller가 (agent,fence)를 주면
        task.owner==agent ∧ 모든 write-orbit HELD ∧ fence==task_fence 여야 한다. 다중
        write/shared orbit의 task_fence는 배리어와 동일하게 max(individual fences). 통과면 None,
        아니면 fenced_out 거부 dict. 작업 중 lease가 만료/재부여(ABA)됐으면 여기서 잡힌다.
        (agent/fence 둘 다 None이면 검사 skip — 증분2까지의 무인자 호출 하위호환.)"""
        if agent_id is None and fence is None:
            return None
        t = self.store.get_task(task_id)
        if t is None:
            return {"ok": False, "reason": "no such task"}
        if agent_id is not None and t["agent_id"] not in (agent_id, None):
            return {"ok": False, "reason": "not owner", "owner": t["agent_id"],
                    "fenced_out": True}
        writes = [o for o in self.store.orbits_for_task(task_id)
                  if o["mode"] in WRITE_MODES]
        stale = [o["orbit_id"] for o in writes if o["state"] != "HELD"]
        if not stale:
            for o in writes:
                if agent_id is not None and o["agent_id"] != agent_id:
                    stale.append(o["orbit_id"])
            task_fence = max(
                (o["fence"] for o in writes if o["fence"] is not None), default=None
            )
            if fence is not None and fence != task_fence:
                stale.extend(o["orbit_id"] for o in writes if o["orbit_id"] not in stale)
        if stale:
            return {"ok": False, "fenced_out": True,
                    "reason": "stale fence: write lease expired/released during work",
                    "stale": stale}
        return None

    # ---- bail_epoch 생존 가드 (§D6 잔여, 좀비 GC-pause 부활 방지) ----
    def _check_alive(self, agent_id, bail_epoch):
        """좀비 부활 차단(§D6). 통과면 None, 아니면 fenced_out 거부 dict.
        (a) agent가 회수/탈출 중(RETIRED/ZOMBIE/BAILING)이면 차단 — 죽은 자는 변이 못 함.
        (b) caller가 bail_epoch를 줬는데 현재값과 다르면 차단 — GC-pause로 멈췄던 좀비가 회수
            (epoch bump) 뒤 깨어나 옛 epoch로 변이하려는 것. heartbeat의 state 리셋(WORKING)으로는
            못 우회한다(epoch는 단조·보존). agent_id/bail_epoch 둘 다 None이면 검사 skip(하위호환)."""
        if agent_id is None:
            return None
        ag = self.store.get_agent(agent_id)
        if ag is None:
            return None  # 미등록 — 다른 게이트가 처리(예: 신규 claim은 여기서 upsert).
        if ag["state"] in ("RETIRED", "ZOMBIE", "BAILING"):
            return {"ok": False, "reason": "agent reclaimed", "fenced_out": True,
                    "agent_state": ag["state"]}
        if bail_epoch is not None and ag["bail_epoch"] != bail_epoch:
            return {"ok": False, "reason": "stale bail_epoch", "fenced_out": True,
                    "current": ag["bail_epoch"], "yours": bail_epoch}
        return None

    # ---- 멱등성 (§D9, at-least-once MCP exactly-once 효과) ----
    @staticmethod
    def _arg_hash(verb, args) -> str:
        return hashlib.sha256(
            (verb + "|" + json.dumps(args, sort_keys=True, default=str)).encode()).hexdigest()

    def _split_effect_busy(self, request_id, agent_id, verb, args):
        """Read-only response while another process owns the effect lock."""
        if request_id is None:
            return {"ok": False, "reason": "connect_effect_in_progress", "retry": True}
        arg_hash = self._arg_hash(verb, args)
        with self._cs():
            prior = self.store.get_idem(request_id)
            if prior is None:
                return {"ok": False, "reason": "connect_effect_in_progress", "retry": True}
            if (prior["agent_id"] != agent_id or prior["verb"] != verb
                    or prior["arg_hash"] != arg_hash):
                return {
                    "ok": False,
                    "reason": "idempotency_conflict",
                    "request_id": request_id,
                    "original": {
                        "agent_id": prior["agent_id"], "verb": prior["verb"],
                        "arg_hash": prior["arg_hash"],
                    },
                    "received": {
                        "agent_id": agent_id, "verb": verb, "arg_hash": arg_hash,
                    },
                }
            if prior["status"] == "DONE":
                response = json.loads(prior["response"])
                return (dict(response, replayed=True)
                        if isinstance(response, dict) else response)
            return {
                "ok": False, "reason": "request_inflight",
                "request_id": request_id, "retry": True,
            }

    @staticmethod
    def _is_success(res) -> bool:
        """성공 종단인가 — 성공만 캐시(§3.C). 거부(ok:false)·fenced_out·deadlock·재시도(retry)는
        캐시 금지: 세상이 바뀌면 같은 request_id 재시도가 성공할 수 있어야 한다."""
        if not isinstance(res, dict):
            return res is not None
        if res.get("ok") is False:
            return False
        if res.get("fenced_out") or res.get("deadlock") or res.get("retry"):
            return False
        if res.get("state") in ("DENIED", "PENDING"):
            return False
        return True

    @contextmanager
    def _idem(self, request_id, agent_id, verb, args):
        """변이 동사 멱등 래퍼(임계구역 안). request_id가 None이면 패스스루.
        DONE이면 캐시 응답을 yield(본문 skip 신호=cached). 아니면 INFLIGHT 등록 후 본문 실행,
        성공 종단만 DONE 캐시, 비성공은 clear(재시도 가능). 호출 패턴:
            with self._idem(rid, ag, 'claim', args) as cache:
                if cache.hit: return cache.value
                res = <본문>; cache.set(res); return res
        """
        cache = _IdemSlot()
        if request_id is None:
            yield cache
            return
        arg_hash = self._arg_hash(verb, args)
        # PENDING is deliberately not a generic DONE cache entry because its
        # response evolves.  The live orbit row still owns the request-id
        # namespace, so another verb/agent cannot reuse it meanwhile.
        live_request = self.store.orbit_by_request(request_id)
        if live_request is not None and (
            verb != "claim" or live_request["agent_id"] != agent_id
        ):
            cache.hit = True
            cache.value = {
                "ok": False,
                "reason": "idempotency_conflict",
                "request_id": request_id,
                "original": {
                    "agent_id": live_request["agent_id"],
                    "verb": "claim",
                    "orbit_id": live_request["orbit_id"],
                },
                "received": {"agent_id": agent_id, "verb": verb},
            }
            self._emit(
                "idempotency_conflict",
                agent_id,
                request_id=request_id,
                original_agent=live_request["agent_id"],
                original_verb="claim",
                received_verb=verb,
            )
            yield cache
            return
        prior = self.store.get_idem(request_id)
        if prior is not None and (
            prior["agent_id"] != agent_id
            or prior["verb"] != verb
            or prior["arg_hash"] != arg_hash
        ):
            cache.hit = True
            cache.value = {
                "ok": False,
                "reason": "idempotency_conflict",
                "request_id": request_id,
                "original": {
                    "agent_id": prior["agent_id"],
                    "verb": prior["verb"],
                    "arg_hash": prior["arg_hash"],
                },
                "received": {
                    "agent_id": agent_id,
                    "verb": verb,
                    "arg_hash": arg_hash,
                },
            }
            self._emit(
                "idempotency_conflict",
                agent_id,
                request_id=request_id,
                original_agent=prior["agent_id"],
                original_verb=prior["verb"],
                received_verb=verb,
            )
            yield cache
            return
        if prior is not None and prior["status"] == "DONE":
            cache.hit = True
            cache.value = json.loads(prior["response"])
            cache.value = dict(cache.value, replayed=True) if isinstance(cache.value, dict) else cache.value
            yield cache
            return
        reopened = False
        if prior is not None and prior["status"] == "RETRYABLE":
            if not self.store.reopen_idem_exact(
                request_id, agent_id, verb, arg_hash
            ):
                raise RuntimeError(
                    f"idempotency ownership lost while reopening {verb}:{request_id}"
                )
            reopened = True
        elif prior is not None:
            cache.hit = True
            cache.value = {
                "ok": False,
                "reason": "request_inflight",
                "request_id": request_id,
                "retry": True,
            }
            yield cache
            return
        if not reopened:
            self.store.begin_idem(request_id, agent_id, verb, arg_hash, args)
        try:
            yield cache
        except BaseException:
            self.store.clear_idem_exact(request_id, agent_id, verb, arg_hash)
            raise
        if cache.deferred:
            return
        if cache.value is not None and (
            cache.terminal or self._is_success(cache.value)
        ):
            if not self.store.finish_idem_exact(
                request_id, agent_id, verb, arg_hash, cache.value
            ):
                raise RuntimeError(
                    f"idempotency ownership lost while finishing {verb}:{request_id}"
                )
        else:
            self.store.clear_idem_exact(request_id, agent_id, verb, arg_hash)

    def _complete_split_idem(self, request_id, agent_id, verb, args, response):
        """Finish/clear the exact envelope reserved before a split phase."""
        operation = (
            response.pop("_idempotency_operation", None)
            if isinstance(response, dict) else None
        )
        if request_id is None:
            return response
        arg_hash = self._arg_hash(verb, args)
        with self._cs():
            if operation is not None:
                row = self.store.get_idem(request_id)
                owns = (
                    row is not None and row["status"] == "INFLIGHT"
                    and row.get("operation_id") == operation.get("attempt_id")
                    and row.get("owner_instance") == operation.get("owner_instance")
                    and row.get("owner_generation") == operation.get("owner_generation")
                )
                if not owns:
                    return {
                        "ok": False, "fenced_out": True,
                        "reason": "split idempotency ownership lost",
                        "request_id": request_id,
                    }
            if self._is_success(response):
                if not self.store.finish_idem_exact(
                    request_id, agent_id, verb, arg_hash, response
                ):
                    raise RuntimeError(
                        f"idempotency ownership lost while finishing {verb}:{request_id}"
                    )
            else:
                self.store.clear_idem_exact(request_id, agent_id, verb, arg_hash)
        return response

    def _finish_split_idem_terminal(self, request_id, agent_id, verb, args, response):
        """Cache an authoritative split-phase terminal result, including failure.

        This is reserved for ambiguous exception cuts whose durable state has
        already been reconciled to a terminal outcome.  Ordinary retryable
        failures continue through _complete_split_idem and are not cached.
        """
        if request_id is None:
            return response
        arg_hash = self._arg_hash(verb, args)
        with self._cs():
            if not self.store.finish_idem_exact(
                request_id, agent_id, verb, arg_hash, response
            ):
                raise RuntimeError(
                    f"idempotency ownership lost while terminalizing {verb}:{request_id}"
                )
        return response

    def _clear_split_idem(self, request_id, agent_id, verb, args):
        if request_id is None:
            return
        with self._cs():
            self.store.clear_idem_exact(
                request_id, agent_id, verb, self._arg_hash(verb, args)
            )

    # ---- merge_token / 통합 worktree (§D11) ----
    def _trailer(self, task_id, attempt_id=None) -> str:
        """Exact Git proof key (legacy task key when no attempt is supplied)."""
        if attempt_id is not None:
            return f"OMD-Connect-Attempt: {attempt_id}"
        return f"OMD-Connect: {task_id}"

    def _ensure_integration_wt(self):
        """전용 통합 worktree를 보장(멱등). 사용자 HEAD(root)는 절대 안 건드림(§D11). 락 밖 호출 OK."""
        if not self.git:
            return None
        self.git.ensure_integration_worktree(self.integration_worktree, self.integration_branch)
        return self.integration_worktree

    def _abort_dangling_merge(self, mt):
        """merge_token 보유자가 죽으며 남긴 통합 worktree의 진행중 머지를 중단(§D11)."""
        if not self.git or not self.integration_worktree:
            return
        if os.path.isdir(self.integration_worktree):
            self.git.abort_merge(self.integration_worktree)

    def _acquire_merge_token_locked(self, agent_id, *, operation_id=None,
                                    owner_instance=None, owner_generation=None):
        """repo-wide merge_token(Semaphore max=1, §D11) 획득. 가용(다른 HELD 토큰 없음)이면 부여,
        아니면 None. 임계구역(_cs) 안에서만 호출 — 초과부여 레이스 차단."""
        if self.store.held_merge_token(self.merge_resource) is not None:
            return None  # 이미 누가 응결 중 — 직렬화(한 번에 하나만)
        fence = self.store.next_fence()
        tok = self.store.add_orbit(
            task_id=None, agent_id=agent_id, pathspec=[], mode="write",
            state="HELD", fence=fence, expires_at=None, reason="merge_token",
            kind="merge_token", resource_key=self.merge_resource)
        self.store.set_orbit(
            tok, merge_started_mono=time.monotonic(), operation_id=operation_id,
            owner_instance=owner_instance, owner_generation=owner_generation,
        )
        return tok

    def _release_merge_token_locked(self, token_id):
        tok = self.store.get_orbit(token_id)
        if tok and tok["state"] == "HELD":
            self.store.set_orbit(token_id,
                                 state=fsm.advance("orbit", "HELD", "release"),
                                 released_at=time.time())

    # ---- 재기동 복구 (§D8, P0-6) ----
    def _recover(self):
        """Try startup recovery only after proving no live external effect exists."""
        with self._connect_effect(blocking=False) as owns_effect:
            if not owns_effect:
                self._emit(
                    "connect_recovery_skipped", self.coordinator_id,
                    reason="live_effect_lock_held",
                )
                return {"recovered": False, "reason": "live_effect_lock_held"}
            return self._recover_under_effect()

    def _recover_under_effect(self):
        """재기동 시 git↔DB 조정(멱등). CONNECTING(또는 connect_intent 있는) task를 git 진실과
        맞춘다: 통합 브랜치에 trailer가 있으면 전진수정(→MERGED+해제+worktree 제거), 없으면
        rollback(→DONE, connect 재호출 가능). dangling merge_token은 abort 후 반납."""
        with self._cs():
            wt = None
            wt_error = None
            protected_token_ids = set()
            unresolved_connect = False
            if self.git:
                try:
                    wt = self._ensure_integration_wt()
                except GitError as exc:
                    wt_error = exc
                    wt = None
            for t in self.store.tasks_by_state(["CONNECTING"]):
                # The effect lock proves the previous executor is gone.  Take
                # over its durable generation only after validating every
                # modern binding.  Phase A writes task/token/idempotency in one
                # transaction, so a partial modern tuple is corruption rather
                # than a legitimate crash cut and must remain fail-stopped.
                stored_attempt_id = t.get("connect_attempt_id")
                legacy_attempt = stored_attempt_id is None
                attempt_id = (f"recover-{uuid.uuid4().hex}"
                              if legacy_attempt else stored_attempt_id)
                previous_owner = t.get("connect_owner_instance")
                previous_generation = int(t.get("connect_owner_generation") or 0)
                owner_generation = previous_generation + 1
                token_id = t.get("connect_token_id")
                token = self.store.get_orbit(token_id) if token_id else None
                binding_error = None
                if legacy_attempt and (
                        previous_owner is not None or previous_generation != 0):
                    binding_error = "legacy_attempt_binding_mismatch"
                if not legacy_attempt and (
                        not isinstance(attempt_id, str) or not attempt_id
                        or not isinstance(previous_owner, str) or not previous_owner
                        or previous_generation <= 0):
                    binding_error = "modern_attempt_binding_invalid"
                token_agents = {
                    value for value in (
                        t.get("agent_id"), f"connect:{t['task_id']}",
                        f"barrier:{t['task_id']}",
                    ) if value is not None
                }
                if legacy_attempt and token is None:
                    candidates = [
                        candidate for candidate in self.store.all_held_merge_tokens()
                        if candidate.get("resource_key") == self.merge_resource
                    ]
                    candidate = candidates[0] if len(candidates) == 1 else None
                    if (candidate is not None
                            and candidate["state"] == "HELD"
                            and candidate.get("agent_id") in token_agents
                            and candidate.get("operation_id") is None
                            and candidate.get("owner_instance") is None
                            and candidate.get("owner_generation") is None):
                        token = candidate
                        token_id = candidate["orbit_id"]
                if legacy_attempt:
                    if token is not None and (
                            token["state"] != "HELD"
                            or token.get("kind") != "merge_token"
                            or token.get("resource_key") != self.merge_resource
                            or token.get("agent_id") not in token_agents
                            or token.get("operation_id") is not None
                            or token.get("owner_instance") is not None
                            or token.get("owner_generation") is not None):
                        binding_error = "legacy_token_binding_mismatch"
                elif token is None:
                    binding_error = "modern_token_missing"
                elif not (
                        token["state"] == "HELD"
                        and token.get("kind") == "merge_token"
                        and token.get("resource_key") == self.merge_resource
                        and token.get("agent_id") in token_agents
                        and token.get("operation_id") == attempt_id
                        and token.get("owner_instance") == previous_owner
                        and int(token.get("owner_generation") or 0)
                        == previous_generation):
                    binding_error = "modern_token_binding_mismatch"

                request_id = t.get("connect_request_id")
                request_arg_hash = t.get("connect_arg_hash")
                has_request_binding = (
                    request_id is not None or request_arg_hash is not None
                )
                row = None
                if has_request_binding:
                    if (not isinstance(request_id, str) or not request_id
                            or not isinstance(request_arg_hash, str)
                            or not request_arg_hash):
                        binding_error = binding_error or "invalid_request_binding"
                    else:
                        row = self.store.get_idem(request_id)
                        if (row is None or row["status"] != "INFLIGHT"
                                or row["verb"] != "connect"
                                or row["arg_hash"] != request_arg_hash):
                            binding_error = binding_error or "idempotency_binding_missing"
                        elif legacy_attempt:
                            if (row.get("operation_id") is not None
                                    or row.get("owner_instance") is not None
                                    or row.get("owner_generation") is not None):
                                binding_error = (
                                    binding_error or
                                    "legacy_idempotency_binding_mismatch"
                                )
                        elif not (
                                row.get("operation_id") == attempt_id
                                and row.get("owner_instance") == previous_owner
                                and int(row.get("owner_generation") or 0)
                                == previous_generation):
                            binding_error = (
                                binding_error or
                                "modern_idempotency_binding_mismatch"
                            )

                if binding_error is not None:
                    unresolved_connect = True
                    # Under a malformed modern tuple we cannot prove which
                    # HELD token is unrelated.  Preserve all as forensic
                    # authority instead of expiring or adopting one.
                    protected_token_ids.update(
                        mt["orbit_id"] for mt in self.store.all_held_merge_tokens()
                    )
                    for orbit in self.store.pinned_orbits_for_task(t["task_id"]):
                        self.store.set_orbit(orbit["orbit_id"], merge_deadline=None)
                    self._emit(
                        "connect_recovery_unresolved", t["task_id"],
                        reason=binding_error, attempt_id=attempt_id,
                    )
                    continue

                if not legacy_attempt:
                    self.store.set_task(
                        t["task_id"], connect_attempt_id=attempt_id,
                        connect_owner_instance=self.instance_id,
                        connect_owner_generation=owner_generation,
                        connect_token_id=token_id,
                    )
                    self.store.set_orbit(
                        token_id, operation_id=attempt_id,
                        owner_instance=self.instance_id,
                        owner_generation=owner_generation,
                    )
                    if has_request_binding:
                        bound = self.store.takeover_idem_operation_exact(
                            request_id, row["agent_id"], "connect", request_arg_hash,
                            operation_id=attempt_id, previous_owner=previous_owner,
                            previous_generation=previous_generation,
                            owner_instance=self.instance_id,
                            owner_generation=owner_generation,
                        )
                        if not bound:
                            raise RuntimeError(
                                f"connect recovery ownership CAS lost {request_id}"
                            )
                # Legacy rows have only task trailers as proof.  Do not mint a
                # synthetic modern attempt identity: retaining NULL bindings
                # lets later unresolved restarts and legacy idempotency proof
                # continue to use that original authority honestly.
                t = self.store.get_task(t["task_id"])
                merged_sha = None
                repo_bound = bool(t.get("connect_repo_bound"))
                if repo_bound and (self.git is None or wt is None):
                    unresolved_connect = True
                    if token_id is not None:
                        protected_token_ids.add(token_id)
                    for orbit in self.store.pinned_orbits_for_task(t["task_id"]):
                        self.store.set_orbit(orbit["orbit_id"], merge_deadline=None)
                    self._emit(
                        "connect_recovery_unresolved", t["task_id"],
                        reason="repo_unavailable", error=str(wt_error) if wt_error else None,
                        attempt_id=attempt_id,
                    )
                    continue
                if repo_bound:
                    proof = (self._trailer(t["task_id"])
                             if legacy_attempt else
                             self._trailer(t["task_id"], attempt_id))
                    try:
                        merged_sha = self.git.branch_in_integration(
                            wt, self.integration_branch, proof, strict=True
                        )
                    except GitError as exc:
                        unresolved_connect = True
                        if token_id is not None:
                            protected_token_ids.add(token_id)
                        for orbit in self.store.pinned_orbits_for_task(t["task_id"]):
                            self.store.set_orbit(orbit["orbit_id"], merge_deadline=None)
                        self._emit(
                            "connect_recovery_unresolved", t["task_id"],
                            reason="git_proof_unavailable", error=str(exc),
                            attempt_id=attempt_id,
                        )
                        continue
                if merged_sha:
                    if not legacy_attempt:
                        candidate_sha = t.get("branch_tip_sha")
                        try:
                            if not candidate_sha:
                                raise GitError("missing audited branch tip")
                            self.git.assert_ancestor(
                                candidate_sha, merged_sha, cwd=wt
                            )
                        except GitError as exc:
                            unresolved_connect = True
                            if token_id is not None:
                                protected_token_ids.add(token_id)
                            for orbit in self.store.pinned_orbits_for_task(t["task_id"]):
                                self.store.set_orbit(
                                    orbit["orbit_id"], merge_deadline=None
                                )
                            self._emit(
                                "connect_recovery_unresolved", t["task_id"],
                                reason="candidate_ancestry_unproven", error=str(exc),
                                attempt_id=attempt_id,
                            )
                            continue
                    # git 진실: 이미 응결됨 → 전진수정(P0-6: merge_sha 기록 후 해제).
                    now = time.time()
                    self.store.set_task(t["task_id"], merge_sha=merged_sha,
                                        merged_at=now)
                    # Crash-forward repair must project the same read-coherence
                    # authority as healthy Phase C.  The write orbits are still
                    # HELD here, so capture their globs before releasing them.
                    new_gen = self.store.bump_integration_gen()
                    merged_globs = self._merged_write_globs(t["task_id"])
                    self.store.append_merge_log(
                        new_gen, t["task_id"], merged_globs
                    )
                    stale_reads = self._mark_stale_reads(
                        t["task_id"], new_gen, merged_globs
                    )
                    self._release_task_write_orbits(t["task_id"])
                    self.store.set_task(t["task_id"],
                                        state=fsm.advance("task", "CONNECTING", "merged"),
                                        connect_intent_at=None, integration_base_sha=None)
                    if token_id is not None:
                        self._release_merge_token_locked(token_id)
                    self.store.set_flag(t["task_id"], "merged")
                    if self.git and t["worktree"]:
                        self.git.remove_worktree(t["worktree"])
                    self._emit("connect_recovered", t["task_id"], merge_sha=merged_sha,
                               outcome="merged", gen=new_gen,
                               stale_reads=len(stale_reads))
                else:
                    # checked merge는 영속한 pre-merge HEAD로 abort+검증한다. 실패하면
                    # Coordinator 기동을 fail-stop해 DB/token/pin과 증거를 보존한다.
                    integration_base = t.get("integration_base_sha")
                    if repo_bound:
                        if not integration_base:
                            unresolved_connect = True
                            if token_id is not None:
                                protected_token_ids.add(token_id)
                            self._emit(
                                "connect_recovery_unresolved", t["task_id"],
                                reason="missing_integration_base", attempt_id=attempt_id,
                            )
                            continue
                        try:
                            self.git.abort_merge_verified(wt, integration_base)
                        except GitRollbackError:
                            # A verified rollback failure means repository state
                            # may already have escaped the persisted attempt.  Do
                            # not silently turn that safety violation into an
                            # ordinary unresolved recovery on startup.
                            raise
                        except GitError as exc:
                            unresolved_connect = True
                            if token_id is not None:
                                protected_token_ids.add(token_id)
                            for orbit in self.store.pinned_orbits_for_task(t["task_id"]):
                                self.store.set_orbit(
                                    orbit["orbit_id"], merge_deadline=None
                                )
                            self._emit(
                                "connect_recovery_unresolved", t["task_id"],
                                reason="rollback_unproven", error=str(exc),
                                attempt_id=attempt_id,
                            )
                            continue
                    # 검증된 미머지 → rollback(재시도가능). 궤도 unpin(merging=0).
                    for o in self.store.pinned_orbits_for_task(t["task_id"]):
                        self.store.set_orbit(o["orbit_id"], merging=0, merge_deadline=None)
                    self.store.set_task(t["task_id"],
                                        state=fsm.advance("task", "CONNECTING", "rollback"),
                                        connect_intent_at=None, integration_base_sha=None,
                                        connect_attempt_id=None,
                                        connect_owner_instance=None,
                                        connect_token_id=None,
                                        connect_request_id=None, connect_arg_hash=None,
                                        connect_repo_bound=0)
                    if token_id is not None:
                        self._release_merge_token_locked(token_id)
                    self._emit("connect_recovered", t["task_id"], outcome="rollback")
            # dangling merge_token: 재기동 시점에 HELD인 토큰은 정의상 dangling이다 —
            # merge_token은 connect Phase B 동안만 잠깐 보유되고, 그 Phase는 프로세스에 묶여
            # 재기동을 가로질러 살아있을 수 없다(§D11). 위에서 모든 CONNECTING task를 이미
            # git 진실과 조정했으므로(MERGED/DONE), 남은 토큰은 전부 abort+반납해 누수를 막는다.
            for mt in self.store.all_held_merge_tokens():
                if mt["orbit_id"] in protected_token_ids:
                    continue
                self._abort_dangling_merge(mt)
                self.store.set_orbit(mt["orbit_id"],
                                     state=fsm.advance("orbit", "HELD", "expire"),
                                     released_at=time.time())
                self._emit("merge_token_reclaimed", mt["agent_id"],
                           orbit_id=mt["orbit_id"], reason="recover")
            # §3.D 배리어-bound 단위복구(증분11) — task-단위 조정이 끝난 *뒤* 배리어를 단위로
            # 조정한다(위에서 CONNECTING 이 전부 MERGED/DONE 으로 수렴했으므로 여기의 멤버
            # 상태 = git 진실).
            if not unresolved_connect:
                self._barrier_recover()
            # Older coordinators could persist ABORTED while leaving a task's
            # admission rows live.  Repair that invariant before reconciliation
            # gets any chance to promote an orphaned PENDING request.
            for task in self.store.tasks_by_state(["ABORTED"]):
                self._terminalize_cancelled_task_orbits(
                    task["task_id"], reason="recovery"
                )
            self._reconcile_admission()
            self._recover_split_idempotency()

    def _merged_task_effect_proven(self, task):
        """Return whether a MERGED task has authority for recovered success."""
        if task is None or task.get("state") != "MERGED":
            return False, "task_not_merged"
        # The durable attempt mode is authority.  Attaching a repo to a later
        # Coordinator must not retroactively turn an explicit DB-only effect
        # into a Git publication that never existed.
        requires_git = bool(task.get("connect_repo_bound"))
        if not requires_git:
            return True, None
        if self.git is None:
            return False, "repo_unavailable"
        merge_sha = task.get("merge_sha")
        if not merge_sha:
            return False, "merge_sha_missing"
        attempt_id = task.get("connect_attempt_id")
        trailer = (self._trailer(task["task_id"], attempt_id)
                   if attempt_id else self._trailer(task["task_id"]))
        try:
            wt = self._ensure_integration_wt()
            proven = self.git.branch_in_integration(
                wt, self.integration_branch, trailer, strict=True
            )
        except GitError as exc:
            return False, f"git_proof_unavailable:{exc}"
        if proven != merge_sha:
            return False, "trailer_sha_mismatch"
        if attempt_id:
            candidate_sha = task.get("branch_tip_sha")
            if not candidate_sha:
                return False, "candidate_sha_missing"
            try:
                self.git.assert_ancestor(candidate_sha, proven, cwd=wt)
            except GitError as exc:
                return False, f"candidate_ancestry_unproven:{exc}"
        return True, None

    def _barrier_recover(self):
        """§3.D: TRIPPING 중 크래시한 배리어를 *단위*로 조정(임계구역 안, _recover 말미).
        전 멤버 MERGED = 트립이 사실상 완료 → TRIPPED 전진수정. 일부만 MERGED = 반쪽 트립 →
        BROKEN(coordinator_crash_partial_trip) fail-loud — "BROKEN 신호 없이 반쪽 MERGED" 함정
        폐쇄. MERGED 는 단조 사실이라 되돌리지 않고(§D5 deviation 1과 동일 계약), 미응결
        task 는 task-단위 복구가 이미 재시도 가능 상태로 되돌려 놓았다. ARMED/종단은 불가침."""
        for b in self.store.all_barriers(states=["TRIPPING"]):
            parts = self.store.barrier_parties(b["barrier_id"], b["generation"])
            tasks = [self.store.get_task(p["task_id"]) for p in parts]
            if parts and all(t is not None and t["state"] == "MERGED" for t in tasks):
                proofs = [self._merged_task_effect_proven(t) for t in tasks]
                if not all(ok for ok, _ in proofs):
                    self._emit(
                        "barrier_recovery_unresolved", b["name"], barrier=b["name"],
                        generation=b["generation"],
                        reasons=[reason for ok, reason in proofs if not ok],
                    )
                    continue
                self.store.set_barrier(b["barrier_id"],
                                       state=fsm.advance("barrier", "TRIPPING", "trip"))
                self._emit("barrier_recovered", b["name"], barrier=b["name"],
                           generation=b["generation"], outcome="tripped")
            else:
                self._break_barrier(b, reason="coordinator_crash_partial_trip")
                self._emit("barrier_recovered", b["name"], barrier=b["name"],
                           generation=b["generation"], outcome="broken")

    def _recover_split_idempotency(self):
        """Reconcile durable split-phase request reservations after restart.

        The task/barrier state machines above are authoritative.  A committed
        effect is completed to DONE with a reconstructed terminal response; a
        proven non-effect becomes RETRYABLE while retaining its original
        envelope.  Malformed, legacy, or still-ambiguous rows remain INFLIGHT
        (fail-closed) instead of being blanket-deleted.
        """
        finalized = retryable = unresolved = 0

        def finish(row, response):
            nonlocal finalized
            if not self.store.finish_idem_exact(
                row["request_id"], row["agent_id"], row["verb"],
                row["arg_hash"], response,
            ):
                raise RuntimeError(
                    "idempotency ownership lost during recovery "
                    f"{row['verb']}:{row['request_id']}"
                )
            finalized += 1

        def reopen_later(row):
            nonlocal retryable
            if not self.store.mark_idem_retryable_exact(
                row["request_id"], row["agent_id"], row["verb"], row["arg_hash"]
            ):
                raise RuntimeError(
                    "idempotency ownership lost during retry recovery "
                    f"{row['verb']}:{row['request_id']}"
                )
            retryable += 1

        for row in self.store.inflight_idem():
            reason = None
            try:
                args = json.loads(row["args_json"]) if row.get("args_json") else None
            except (TypeError, ValueError, json.JSONDecodeError):
                args = None
            if not isinstance(args, list):
                reason = "missing_or_invalid_args"
            elif self._arg_hash(row["verb"], args) != row["arg_hash"]:
                reason = "arg_hash_mismatch"
            elif row["verb"] == "connect" and len(args) == 5 \
                    and args[1] == row["agent_id"]:
                task = self.store.get_task(args[0])
                if task is not None and task["state"] == "MERGED":
                    # A MERGED task may complete only the exact split envelope
                    # that produced it.  Keep a narrow legacy allowance for
                    # pre-fencing rows where both sides have no operation
                    # binding at all; mixed/partial bindings fail closed.
                    modern_binding = (
                        task.get("connect_attempt_id") is not None
                        and task.get("connect_request_id") == row["request_id"]
                        and task.get("connect_arg_hash") == row["arg_hash"]
                        and task.get("connect_attempt_id") == row.get("operation_id")
                        and task.get("connect_owner_instance")
                        == row.get("owner_instance")
                        and int(task.get("connect_owner_generation") or 0)
                        == int(row.get("owner_generation") or 0)
                    )
                    legacy_binding = (
                        task.get("connect_attempt_id") is None
                        and task.get("connect_request_id") is None
                        and task.get("connect_arg_hash") is None
                        and row.get("operation_id") is None
                        and row.get("owner_instance") is None
                        and row.get("owner_generation") is None
                    )
                    proof_ok = modern_binding or legacy_binding
                    if proof_ok:
                        proof_ok, _ = self._merged_task_effect_proven(task)
                    if proof_ok:
                        finish(row, {
                            "ok": True,
                            "task_id": args[0],
                            "state": "MERGED",
                            "merge_sha": task["merge_sha"],
                            "recovered": True,
                        })
                    else:
                        reason = "repo_merge_proof_missing"
                elif task is None or task["state"] != "CONNECTING":
                    reopen_later(row)
                else:
                    reason = "connect_still_ambiguous"
            elif row["verb"] == "barrier_arrive" and len(args) == 4:
                barrier = self.store.barrier_by_name(args[0])
                party = (
                    self.store.get_barrier_party(
                        barrier["barrier_id"], barrier["generation"], args[1]
                    )
                    if barrier is not None else None
                )
                if barrier is None:
                    reason = "barrier_missing"
                elif party is None or not party["arrived"] \
                        or party["agent_id"] != row["agent_id"] \
                        or (args[2] is not None and party["arrive_fence"] != args[2]):
                    reason = "barrier_envelope_mismatch"
                elif barrier["state"] in ("TRIPPED", "CONSUMED"):
                    parts = self.store.barrier_parties(
                        barrier["barrier_id"], barrier["generation"]
                    )
                    task_proofs = [
                        self._merged_task_effect_proven(
                            self.store.get_task(p["task_id"])
                        ) for p in parts
                    ]
                    if not parts or not all(ok for ok, _ in task_proofs):
                        reason = "barrier_repo_proof_missing"
                    else:
                        merged = [
                            p["task_id"] for p in parts
                            if (self.store.get_task(p["task_id"]) or {}).get("state")
                            == "MERGED"
                        ]
                        finish(row, {
                            "ok": True,
                            "state": barrier["state"],
                            "name": barrier["name"],
                            "generation": barrier["generation"],
                            "merged": merged,
                            "recovered": True,
                        })
                elif barrier["state"] == "BROKEN":
                    finish(row, {
                        "ok": False,
                        "state": "BROKEN",
                        "name": barrier["name"],
                        "generation": barrier["generation"],
                        "reason": barrier["break_reason"] or "broken",
                        "recovered": True,
                    })
                elif barrier["state"] == "ARMED":
                    reason = "barrier_not_committed"
                else:
                    reason = "barrier_still_ambiguous"
            else:
                reason = "unsupported_split_request"

            if reason is not None:
                unresolved += 1
                self._emit(
                    "idempotency_inflight_unresolved",
                    row["agent_id"] or self.coordinator_id,
                    request_id=row["request_id"], verb=row["verb"], reason=reason,
                )

        if finalized or retryable or unresolved:
            self._emit(
                "idempotency_inflight_recovered",
                self.coordinator_id,
                finalized=finalized, retryable=retryable, unresolved=unresolved,
            )

    # ---- write-set 파일시스템 감사 (§D10, P0-11 = "최대 구멍") ----
    def _claimed_write_globs(self, task_id, writes) -> list[str]:
        """task의 HELD write-orbit pathspec들의 합집합(claimed write-set). `writes`는 Phase A가
        이미 모은 write-orbit row 리스트 — 거기서 glob을 펼친다."""
        globs: list[str] = []
        for o in writes:
            for g in json.loads(o["pathspec"]):
                globs.append(g)
        return globs

    def _writeset_audit(self, task_id, candidate_ref, write_globs, *,
                        base_ref=None) -> list[str]:
        """candidate가 exact base 대비 건드린 파일 중 claimed write-set 밖 경로들.

        §D10 option 2(저비용 pre-connect 감사): `git diff --name-only base...candidate`의 모든
        경로가 claimed write-globs 에 정확히 덮여야 한다. 안 덮인 경로 = 분열 위험 = 거부 대상.
        DB-only coordinator만 명시적으로 감사를 건너뛴다. Repo-bound authority에서 ref가
        없거나 Git 읽기가 실패하면 예외를 유지해 Phase A가 token/state 변이 전에 fail closed한다.
        """
        if not self.git:
            return []
        if not candidate_ref or not base_ref:
            raise GitError(
                f"write-set audit refs unavailable for {task_id}: "
                f"base={base_ref!r} candidate={candidate_ref!r}"
            )
        changed = self.git.changed_paths(candidate_ref, base_ref)
        # path_in_globs = 정확매칭(soundness: 덮인다를 절대 거짓-양성으로 안 냄). 안 덮이면 위반.
        return [p for p in changed if not path_in_globs(p, write_globs)]

    def _snapshot_connect_candidate(self, task_id, branch, write_globs):
        """Capture and audit the immutable Git inputs for one connect attempt.

        Resolving before the audit closes the mutable-branch TOCTOU: later
        commits may advance the task branch, but Phase B merges only the SHA
        returned here.  The integration base is also exact so the audit and
        rollback proof describe the same candidate generation.
        """
        if not self.git:
            return None, None, []
        if not branch:
            raise GitError(f"task {task_id} has no branch for repo-bound connect")
        branch_tip = self.git.branch_tip(branch, strict=True)
        integration_base = self.git.branch_tip(self.integration_branch, strict=True)
        offending = self._writeset_audit(
            task_id, branch_tip, write_globs, base_ref=integration_base
        )
        return branch_tip, integration_base, offending

    def _release_task_write_orbits(self, task_id):
        """task의 HELD write-orbit 전부 해제 + unpin(merge_sha 기록 *후* 호출 — P0-6 순서)."""
        for o in self.store.orbits_for_task(task_id):
            if o["mode"] in WRITE_MODES and o["state"] == "HELD":
                snapshot_hash, _, _ = self._reduce_orbit_lifecycle(o, "RELEASE")
                self.store.set_orbit(
                    o["orbit_id"],
                    state=fsm.advance("orbit", "HELD", "release"),
                    released_at=time.time(),
                    merging=0,
                    merge_deadline=None,
                    authority_snapshot_hash=snapshot_hash,
                    decision_id=None,
                    decision_type="RELEASE",
                )

    # ---- D12 read-set 코히런스 (§D12, 유령 읽기) ----
    def _merged_write_globs(self, task_id) -> list[str]:
        """방금 응결된 task 가 통합 브랜치에 추가/변경한 경로 글로브. 권위 소스는 그 task 의
        claimed write-set(이미 P0-11 감사로 *실제* write-set == 선언 write-set 이 강제됨).
        repo 가 있으면 실제 changed_paths(구체 경로)도 합쳐 더 정밀히 — 둘 다 overlap 판정에 쓴다."""
        globs = []
        t = self.store.get_task(task_id)
        if t:
            try:
                globs.extend(json.loads(t["writes"] or "[]"))
                globs.extend(json.loads(t["shared"] or "[]"))
            except (TypeError, ValueError):
                pass
        # write-orbit pathspec(해제 직전에 부르므로 아직 잡을 수 있을 때 합집합) 도 포함.
        for o in self.store.orbits_for_task(task_id):
            if o["mode"] in WRITE_MODES:
                globs.extend(json.loads(o["pathspec"]))
        return list(dict.fromkeys(globs))  # 중복 제거(순서 보존)

    def _mark_stale_reads(self, merged_task_id, new_gen, merged_globs=None):
        """응결(merge)이 통합을 new_gen 으로 전진시켰다. 이 응결이 추가/변경한 경로와 **겹치는**
        live HELD read-궤도(자기보다 옛 gen 에서 분기) 를 stale=1 로 표시 → 그 consumer 는
        connect 전 rebase/재독 강제(§D12). 신호는 D3 EPHEMERAL 플래그/이벤트로(consumer 가 안다).
        주: read↔write 배타성 때문에 *live* read-궤도가 겹치는 일은 드물다(연속점유 시) — 주된
        코히런스 게이트는 connect 의 merge_log 검사다. 이건 그 보조(즉시 신호)다."""
        if merged_globs is None:
            merged_globs = self._merged_write_globs(merged_task_id)
        if not merged_globs:
            return []
        affected = []
        for r in self.store.live_read_orbits():
            if r["task_id"] == merged_task_id:
                continue  # 자기 자신의 read 는 무관
            if r["stale"]:
                continue  # 이미 표시됨(멱등)
            # read 가 분기한 gen 이 이번 응결 *이전*이어야 유령(이후면 이미 본 것). None=보수적 표시.
            rg = r["read_gen"]
            if rg is not None and rg >= new_gen:
                continue
            if sets_overlap(json.loads(r["pathspec"]), merged_globs):
                self.store.set_orbit(r["orbit_id"], stale=1)
                self._emit("read_stale", r["agent_id"], orbit_id=r["orbit_id"],
                           task=r["task_id"], by_task=merged_task_id, gen=new_gen)
                # D3 이벤트/플래그 신호: consumer 가 자기 read-coherence 키로 flag_wait 관측 가능.
                # epoch 는 보존·증가(이전 refresh 가 CLEARED 로 만든 뒤 재-stale 도 단조 전진) →
                # 옛 epoch 로 register 한 대기자가 깨어난다(§D3 register→poll).
                key = self._read_stale_key(r["orbit_id"])
                prev = self.store.get_flag_row(key)
                epoch = (prev["epoch"] + 1) if prev else 0
                self.store.upsert_flag(
                    key, value=str(new_gen), set_by=merged_task_id,
                    flag_type="LATCH", rank=0, status="LIVE", epoch=epoch)
                self._wake_flag_waiters(key)
                affected.append(r["orbit_id"])
        return affected

    def _ghost_reads(self, task) -> list[str]:
        """task 의 선언 reads 와 겹치는, read_synced_gen *이후*의 응결 write-globs(유령 읽기).
        read 를 한 적 없는(read_synced_gen=None) task 는 코히런스 대상 아님 → 빈 리스트.
        궤도를 release 해도 read_synced_gen 이 task 에 남으므로 read↔write 배타성을 안 깨고
        consumer connect 시점에 정확히 판정한다(§D12)."""
        if task is None:
            return []
        synced = task["read_synced_gen"]
        if synced is None:
            return []  # 이 task 는 read claim 을 한 적 없음 — 코히런스 무관
        try:
            reads = json.loads(task["reads"] or "[]")
        except (TypeError, ValueError):
            reads = []
        if not reads:
            return []
        ghost = []
        for m in self.store.merges_since(synced):
            if m["task_id"] == task["task_id"]:
                continue  # 자기 자신의 응결은 무관
            try:
                mglobs = json.loads(m["globs"])
            except (TypeError, ValueError):
                mglobs = []
            if sets_overlap(reads, mglobs):
                # 겹치는 응결의 globs 중 reads 와 실제로 교차하는 것만 보고(진단성).
                ghost.extend(g for g in mglobs if sets_overlap(reads, [g]))
        return list(dict.fromkeys(ghost))  # 중복 제거(순서 보존)

    @staticmethod
    def _read_stale_key(orbit_id) -> str:
        """consumer 가 자기 read-궤도의 stale 신호를 flag_wait 로 관측하는 D3 플래그 키(§D12)."""
        return f"read_stale:{orbit_id}"

    def _clear_read_stale_signal(self, orbit_id):
        """read-궤도가 refresh/회수되면 그 stale 신호 플래그를 CLEARED + epoch +1(대기자 기상).
        flag 가 LIVE 로 영구 잔존(누수)하지 않게 한다."""
        key = self._read_stale_key(orbit_id)
        f = self.store.get_flag_row(key)
        if f is not None and f["status"] == "LIVE":
            self.store.set_flag_status(key, status="CLEARED", epoch=f["epoch"] + 1)
            self._wake_flag_waiters(key)

    def read_refresh(self, task_id, agent_id, fence, *, request_id=None, bail_epoch=None):
        """consumer 가 rebase/재독을 마쳤다고 선언 → task 의 read-set 동기화 gen 을 현 통합 gen
        으로 재앵커(+ 살아있는 read-궤도가 있으면 stale 해제·재앵커) (§D12). **task 중심**: read 를
        읽고 release 한 뒤(read↔write 배타라 producer 가 그 영역을 쓰려면 read 가 비어야 함)에도
        코히런스가 task 에 남으므로, 이 동사가 그 task 차원 동기화를 갱신한다.
        소유+fence 가드 — connect 와 동일하게 caller 가 그 task 의 write-orbit (agent,fence)를
        쥐고 있어야(남의 task 를 못 재앵커). 물방울 계약: connect 가 read_stale 로 거부되면
        worktree 를 통합 최신으로 rebase 한 뒤 이 동사로 청산하고 다시 connect 한다."""
        with self._cs():
            with self._idem(request_id, agent_id, "read_refresh",
                            [task_id, fence]) as cache:
                if cache.hit:
                    return cache.value
                dead = self._check_alive(agent_id, bail_epoch)
                if dead:
                    return cache.set(dead)
                t = self.store.get_task(task_id)
                if t is None:
                    return cache.set({"ok": False, "reason": "no such task"})
                # 소유+fence: caller 가 이 task 의 HELD write-orbit 을 (agent,fence)로 쥐어야.
                writes = [o for o in self.store.orbits_for_task(task_id)
                          if o["mode"] in WRITE_MODES]
                live = [o for o in writes if o["state"] == "HELD"]
                if not live:
                    return cache.set({"ok": False, "reason": "no held write orbit for task",
                                      "fenced_out": True})
                if agent_id is not None and any(o["agent_id"] != agent_id for o in live):
                    return cache.set({"ok": False, "reason": "not owner",
                                      "fenced_out": True})
                if fence is not None and all(o["fence"] != fence for o in live):
                    return cache.set({"ok": False, "reason": "stale fence",
                                      "fenced_out": True})
                gen = self.store.integration_gen()
                self.store.set_task(task_id, read_synced_gen=gen)
                # 살아있는 read-궤도가 있으면(release_read=False 경로) 그것도 재앵커+stale 해제.
                for o in self.store.orbits_for_task(task_id):
                    if o["mode"] == "read" and o["state"] == "HELD":
                        self.store.set_orbit(o["orbit_id"], read_gen=gen, stale=0)
                        self._clear_read_stale_signal(o["orbit_id"])
                self._emit("read_refreshed", agent_id, task=task_id, gen=gen)
                return cache.set({"ok": True, "task_id": task_id, "read_gen": gen,
                                  "stale": False})

    # ---- 공개 API (= MCP 툴 / CLI 동사) ----
    def declare(self, task_id, *, name="", writes=None, reads=None, deps=None, priority=0,
                shared=None):
        """shared(P2 레인): hot 공유파일 glob — 배타 writes 와 달리 다른 task 의 shared 궤도와
        겹쳐도 next_task/claim 을 막지 않는다(응결은 3-way, 충돌 시 shared_conflict retryable).
        단 한 task 안의 writes/shared 는 서로소여야 한다. 현재 glob 문법은 부모 glob에서 shared
        하위 경로를 빼는 EXCEPT를 표현하지 못하므로 중첩을 허용하면 같은 task의 배타 lease가
        자기 shared lease를 막는다. 조용한 오분류 대신 선언 단계에서 fail-loud 한다."""
        writes = list(writes or [])
        reads = list(reads or [])
        deps = list(deps or [])
        shared = list(shared or [])
        with self._cs():
            overlaps = [
                {"write": write_glob, "shared": shared_glob}
                for write_glob in writes
                for shared_glob in shared
                if sets_overlap([write_glob], [shared_glob])
            ]
            if overlaps:
                self._emit("declare_rejected", task_id,
                           reason="write_shared_overlap", overlaps=overlaps)
                return {
                    "ok": False,
                    "reason": "write_shared_overlap",
                    "task_id": task_id,
                    "overlaps": overlaps,
                    "hint": "partition writes and shared into disjoint globs; "
                            "shared is not an implicit exclusion from writes",
                }
            # P0-10/§D7: deps가 의존 DAG에 사이클을 만들면 거부(그래프 불변) — 안 그러면
            # 상호의존(A after B, B after A)이 둘 다 영구 BLOCKED. self-dep 도 잡힌다.
            if deps:
                cyc = self._would_cycle(task_id, deps)
                if cyc:
                    self._emit("declare_rejected", task_id, reason="dep_cycle", cycle=cyc)
                    return {"ok": False, "reason": "dep_cycle", "cycle": cyc,
                            "task_id": task_id}
            self.store.add_task(task_id=task_id, name=name, writes=writes,
                                reads=reads, deps=deps, state="PENDING",
                                priority=priority, shared=shared)
        return {"ok": True, "task_id": task_id, "state": "PENDING"}

    def depend(self, task_id, after):
        """task_id 에 의존 엣지(`task_id` after `after`)를 추가 — 단, 사이클을 만들면 **거부**
        (그래프 불변, P0-10/§D7). self-dep 도 거부. check-then-add 가 임계구역 안에서 원자."""
        with self._cs():
            t = self.store.get_task(task_id)
            if t is None:
                return {"ok": False, "reason": "no such task", "task_id": task_id}
            existing = json.loads(t["deps"] or "[]")
            if after in existing:
                return {"ok": True, "noop": True, "task_id": task_id, "after": after,
                        "deps": existing}
            cyc = self._would_cycle(task_id, existing + [after])
            if cyc:
                # 그래프 변경 없음 — 거부만.
                self._emit("depend_rejected", task_id, after=after, reason="dep_cycle",
                           cycle=cyc)
                return {"ok": False, "reason": "dep_cycle", "cycle": cyc,
                        "task_id": task_id, "after": after}
            new_deps = existing + [after]
            self.store.set_task_deps(task_id, new_deps)
            self._emit("depend_added", task_id, after=after)
            return {"ok": True, "task_id": task_id, "after": after, "deps": new_deps}

    def _intent_key(self, agent_id, pathspec, mode, task_id) -> str:
        """claim 자연 멱등 키(§D9): hash(agent, sorted(paths), mode, task)."""
        return self._arg_hash("claim",
                              [agent_id, sorted(pathspec), mode, task_id])

    def _replay_terminal_claim_overload(self, request_id, agent_id, args):
        """Replay only an exact terminal QUEUE_FULL receipt before task gating.

        Successful claim receipts may contain live fence authority and must keep
        flowing through the task eligibility checks.  Overload allocates no
        orbit or fence, so its exact DONE envelope remains safe and authoritative
        even if the optional task has since become terminal.
        """
        if request_id is None:
            return None
        prior = self.store.get_idem(request_id)
        if prior is None or prior["status"] != "DONE":
            return None
        if (
            prior["agent_id"] != agent_id
            or prior["verb"] != "claim"
            or prior["arg_hash"] != self._arg_hash("claim", args)
        ):
            return None
        try:
            response = json.loads(prior["response"])
        except (TypeError, ValueError):
            return None
        if not (
            isinstance(response, dict)
            and response.get("state") == "REJECTED"
            and response.get("code") == "QUEUE_FULL"
            and response.get("reason") == "queue_full"
        ):
            return None
        return dict(response, replayed=True)

    def claim(self, agent_id, pathspec, mode="write", *, ttl=600.0, task_id=None,
              reason="", priority=0, request_id=None, bail_epoch=None,
              _observed_at=None):
        if not isinstance(agent_id, str) or not agent_id:
            return {"ok": False, "state": "REJECTED", "reason": "invalid_agent_id"}
        if request_id is not None and (
            not isinstance(request_id, str) or not request_id
        ):
            return {"ok": False, "state": "REJECTED", "reason": "invalid_request_id"}
        if isinstance(pathspec, str):
            pathspec = [pathspec]
        try:
            pathspec = list(normalize_pathspec(pathspec))
            ttl = float(ttl)
            if not math.isfinite(ttl) or ttl <= 0:
                raise ValueError("ttl must be finite and positive")
            # AdmissionRequest performs the canonical mode/priority checks.  A dummy
            # sequence is sufficient because validation precedes authority mutation.
            AdmissionRequest.build(pathspec, mode, priority, 0)
        except (TypeError, ValueError) as exc:
            return {
                "ok": False,
                "state": "REJECTED",
                "reason": "invalid_admission_request",
                "detail": str(exc),
            }
        args = [agent_id, pathspec, mode, task_id, ttl, priority, reason, bail_epoch]
        with self._cs():
            terminal_overload = self._replay_terminal_claim_overload(
                request_id, agent_id, args
            )
            if terminal_overload is not None:
                return terminal_overload
            if task_id is not None:
                task = self.store.get_task(task_id)
                if task is None:
                    return {
                        "ok": False,
                        "state": "REJECTED",
                        "reason": "no such task",
                        "task_id": task_id,
                    }
                if task["state"] not in TASK_ADMISSION_STATES:
                    return {
                        "ok": False,
                        "state": "REJECTED",
                        "reason": "task_not_admission_eligible",
                        "task_id": task_id,
                        "task_state": task["state"],
                    }
            with self._idem(request_id, agent_id, "claim", args) as cache:
                if cache.hit:
                    return cache.value
                # begin() supplies an already-swept authority cut for every
                # member of one batch. Re-sweeping between members could promote
                # a waiter after the first grant and break all-or-none acquisition.
                observed_at = (
                    self._sweep_inline()
                    if _observed_at is None
                    else _observed_at
                )
                # §D6: 회수/탈출된 좀비는 새 궤도조차 못 잡음(부활 차단).
                dead = self._check_alive(agent_id, bail_epoch)
                if dead:
                    return cache.set(dead)
                self.store.upsert_agent(agent_id)
                agent = self.store.get_agent(agent_id)
                effective_bail_epoch = int(agent["bail_epoch"] if agent else 0)
                self._emit("orbit_requested", agent_id, mode=mode, paths=pathspec, task=task_id)
                # PENDING is not stored in the generic DONE cache, but its durable
                # request identity still forbids a same-id/different-intent mutation.
                request_generation = 0
                request_dup = self.store.latest_orbit_by_request(request_id)
                if request_dup is not None:
                    same_request = (
                        request_dup["agent_id"] == agent_id
                        and request_dup["mode"] == mode
                        and list(normalize_pathspec(json.loads(request_dup["pathspec"]))) == pathspec
                        and request_dup["task_id"] == task_id
                        and int(request_dup["priority"] or 0) == priority
                        and float(request_dup["requested_ttl"] or 600.0) == ttl
                        and int(request_dup["bail_epoch"] or 0) == effective_bail_epoch
                        and (request_dup["reason"] or "") == (reason or "")
                    )
                    if not same_request:
                        self._emit("idempotency_conflict", agent_id,
                                   request_id=request_id,
                                   original_orbit=request_dup["orbit_id"],
                                   received_verb="claim")
                        return cache.set({"ok": False,
                                          "reason": "idempotency_conflict",
                                          "request_id": request_id,
                                          "original_orbit": request_dup["orbit_id"]})
                    retryable_policy_denial = (
                        request_dup["state"] == "DENIED"
                        and request_dup["decision_type"]
                        in ("ADMISSION_DENIED", "PROMOTION_DENIED")
                    )
                    if retryable_policy_denial:
                        # §3.C treats a policy denial as a retryable attempt.  The
                        # retry advances the durable generation so the same request
                        # id never creates two generation-zero effects.
                        request_generation = self.store.next_request_generation(
                            request_id
                        )
                    else:
                        blockers = json.loads(request_dup["blocker_ids"] or "[]")
                        return cache.set({"orbit_id": request_dup["orbit_id"],
                                          "request_id": request_dup["request_id"],
                                          "request_generation": request_dup[
                                              "request_generation"],
                                          "bail_epoch": request_dup["bail_epoch"],
                                          "state": request_dup["state"],
                                          "fence": request_dup["fence"],
                                          "queue_seq": request_dup["queue_seq"],
                                          "conflicts": blockers,
                                          "decision_id": request_dup["decision_id"],
                                          "dedup": True})
                # §D9 의미적 멱등: dedup 우회돼도(다른 request_id·없음) 같은 의도면 기존 궤도 반환.
                # §3.C 교차: 단 **현재 caller가 그 궤도의 소유자**여야 살아있는 HELD를 돌려준다 —
                # 회수돼 타인에게 재부여된 lease를 우회로 넘기지 않음(fencing 무장 방지).
                ikey = self._intent_key(agent_id, pathspec, mode, task_id)
                dup = self.store.orbit_by_intent(ikey)
                if dup is not None and dup["agent_id"] == agent_id:
                    if request_id is not None and dup["request_id"] != request_id:
                        self._emit(
                            "idempotency_conflict", agent_id,
                            request_id=request_id,
                            original_orbit=dup["orbit_id"],
                            original_request_id=dup["request_id"],
                            received_verb="claim",
                        )
                        return cache.set({
                            "ok": False,
                            "reason": "idempotency_conflict",
                            "request_id": request_id,
                            "original_orbit": dup["orbit_id"],
                            "original_request_id": dup["request_id"],
                        })
                    self._emit("orbit_dedup", agent_id, orbit_id=dup["orbit_id"],
                               state=dup["state"])
                    out = {"orbit_id": dup["orbit_id"], "request_id": dup["request_id"],
                           "request_generation": dup["request_generation"],
                           "bail_epoch": dup["bail_epoch"],
                           "state": dup["state"], "fence": dup["fence"],
                           "queue_seq": dup["queue_seq"],
                           "conflicts": json.loads(dup["blocker_ids"] or "[]"),
                           "decision_id": dup["decision_id"], "dedup": True}
                    return cache.set(out)
                if not self.admission_policy.accepts_base_priority(priority):
                    return cache.set({
                        "ok": False,
                        "state": "REJECTED",
                        "reason": "invalid_admission_request",
                        "detail": (
                            "priority must leave signed-64-bit headroom for "
                            "max_age_boost"
                        ),
                    })
                # Preview the next durable ticket under the same BEGIN IMMEDIATE
                # transaction.  A terminal overload must not consume a ticket.
                queue_seq = self.store.current_seq() + 1
                oid = "orb-" + uuid.uuid4().hex[:12]
                semantic_request_id = request_id or f"internal:{oid}"
                decision, snapshot_hash = self._admission_decision(
                    pathspec,
                    mode,
                    priority,
                    queue_seq,
                    orbit_id=oid,
                    policy_version=self.admission_policy.version,
                    enqueued_at=observed_at,
                    observed_at=observed_at,
                )
                identity = self._admission_identity(
                    orbit_id=oid, request_id=semantic_request_id,
                    request_generation=request_generation,
                    agent_id=agent_id, bail_epoch=effective_bail_epoch, mode=mode,
                    pathspec=pathspec, pathspec_digest=pathspec_digest(pathspec),
                    policy_version=self.admission_policy.version,
                )
                if not decision.grantable:
                    queue_stats = self.store.pending_queue_stats()
                    queue_depth = queue_stats["depth"]
                    if queue_depth >= self.admission_queue_capacity:
                        retry_after_at = queue_stats["earliest_wait_deadline"]
                        if retry_after_at is None or retry_after_at <= observed_at:
                            retry_after_at = observed_at + min(
                                1.0, self.admission_wait_timeout
                            )
                        payload = self._admission_payload(
                            "ADMISSION_REJECTED",
                            identity,
                            snapshot_hash,
                            reason="queue_full",
                            queue_depth=queue_depth,
                            queue_capacity=self.admission_queue_capacity,
                            retry_after_at=retry_after_at,
                            base_priority=decision.base_priority,
                            effective_priority=decision.effective_priority,
                            observed_at=decision.observed_at,
                        )
                        self._assert_admission_projection(
                            {**identity, "state": "REQUESTED"},
                            "ADMISSION_REJECTED",
                            payload,
                            snapshot_hash,
                            None,
                        )
                        rejected = {
                            "ok": False,
                            "state": "REJECTED",
                            "reason": "queue_full",
                            "code": "QUEUE_FULL",
                            "retry": True,
                            "retry_requires_new_request_id": request_id is not None,
                            "retry_after_at": retry_after_at,
                            "queue_depth": queue_depth,
                            "queue_capacity": self.admission_queue_capacity,
                            "base_priority": decision.base_priority,
                            "effective_priority": decision.effective_priority,
                            "observed_at": decision.observed_at,
                            "repository_id": identity["repository_id"],
                            "request_id": identity["request_id"],
                            "request_generation": identity["request_generation"],
                            "orbit_id": identity["orbit_id"],
                            "owner_agent": identity["owner_agent"],
                            "bail_epoch": identity["bail_epoch"],
                            "mode": identity["mode"],
                            "pathspec_digest": identity["pathspec_digest"],
                            "policy_version": identity["policy_version"],
                            "authority_snapshot_hash": snapshot_hash,
                            "decision_id": payload["decision_id"],
                            "actor": payload["actor"],
                            "event_id": payload["event_id"],
                        }
                        self._emit(
                            "orbit_rejected",
                            agent_id,
                            orbit_id=oid,
                            request_id=semantic_request_id,
                            request_generation=request_generation,
                            reason="queue_full",
                            queue_depth=queue_depth,
                            queue_capacity=self.admission_queue_capacity,
                            retry_after_at=retry_after_at,
                            decision_id=payload["decision_id"],
                        )
                        # REJECTED is terminal for this request identity.  A later
                        # retry uses a fresh request id; exact retries replay this
                        # byte-equivalent receipt even if capacity has since freed.
                        return cache.set_terminal(rejected)
                    committed_seq = self.store.next_seq()
                    if committed_seq != queue_seq:
                        raise RuntimeError(
                            "admission queue ticket changed inside authority transaction"
                        )
                    enqueued_at = observed_at
                    wait_deadline = enqueued_at + self.admission_wait_timeout
                    payload = self._admission_payload(
                        "ADMISSION_QUEUED", identity, snapshot_hash,
                        queue_seq=queue_seq, enqueued_at=enqueued_at,
                        wait_deadline=wait_deadline,
                        base_priority=decision.base_priority,
                        effective_priority=decision.effective_priority,
                        observed_at=decision.observed_at,
                    )
                    self._assert_admission_projection(
                        {**identity, "state": "REQUESTED"}, "ADMISSION_QUEUED",
                        payload, snapshot_hash, "PENDING")
                    oid = self.store.add_orbit(
                        orbit_id=oid, task_id=task_id, agent_id=agent_id,
                        pathspec=pathspec, mode=mode, state="PENDING", reason=reason,
                        priority=priority, intent_key=ikey, queue_seq=queue_seq,
                        requested_ttl=ttl,
                        policy_version=self.admission_policy.version,
                        pathspec_digest=identity["pathspec_digest"],
                        request_id=semantic_request_id,
                        request_generation=request_generation,
                        bail_epoch=effective_bail_epoch,
                        authority_snapshot_hash=snapshot_hash,
                        decision_id=payload["decision_id"],
                        decision_type="ADMISSION_QUEUED",
                        decision_schema="admission_decision/v2",
                        decision_observed_at=decision.observed_at,
                        decision_effective_priority=decision.effective_priority,
                        blocker_ids=list(decision.blocker_ids),
                        enqueued_at=enqueued_at,
                        wait_deadline=wait_deadline)
                    if self._cycle_with(agent_id, observed_at):
                        held = self.store.held_orbits()
                        pending = self.store.pending_orbits()
                        denial_snapshot = authority_snapshot_hash(
                            held,
                            pending,
                            coordinator_epoch=self.leader_epoch,
                            policy=self.admission_policy,
                            observed_at=observed_at,
                        )
                        denial = self._admission_payload(
                            "PROMOTION_DENIED", identity, denial_snapshot,
                            queue_seq=queue_seq,
                            reason="reservation_cycle",
                            base_priority=decision.base_priority,
                            effective_priority=decision.effective_priority,
                            observed_at=decision.observed_at,
                        )
                        self._assert_admission_projection(
                            {
                                **identity,
                                "state": "PENDING",
                                "queue_seq": queue_seq,
                                "base_priority": decision.base_priority,
                                "effective_priority": decision.effective_priority,
                                "observed_at": decision.observed_at,
                            },
                            "PROMOTION_DENIED", denial, denial_snapshot, "DENIED")
                        self.store.set_orbit(
                            oid, state=fsm.advance("orbit", "PENDING", "deny"),
                            authority_snapshot_hash=denial_snapshot,
                            decision_id=denial["decision_id"],
                            decision_type="PROMOTION_DENIED",
                            decision_schema="admission_decision/v2",
                            decision_observed_at=decision.observed_at,
                            decision_effective_priority=decision.effective_priority,
                            terminal_reason="reservation_cycle")
                        self._emit("orbit_denied", agent_id, orbit_id=oid, deadlock=True,
                                   queue_seq=queue_seq, decision_id=denial["decision_id"])
                        # DENIED는 캐시 금지(§3.C) — 세상이 바뀌면 재시도가 성공할 수 있어야.
                        return cache.set({"orbit_id": oid,
                                          "request_id": semantic_request_id,
                                          "request_generation": request_generation,
                                          "bail_epoch": effective_bail_epoch,
                                          "state": "DENIED",
                                          "deadlock": True,
                                          "reason": "reservation_cycle",
                                          "queue_seq": queue_seq,
                                          "base_priority": decision.base_priority,
                                          "effective_priority": decision.effective_priority,
                                          "observed_at": decision.observed_at,
                                          "conflicts": list(decision.blocker_ids),
                                          "held_conflicts": list(decision.held_blockers),
                                          "pending_predecessors": list(
                                              decision.pending_predecessors),
                                          "decision_id": denial["decision_id"]})
                    self._emit("orbit_pending", agent_id, orbit_id=oid,
                               conflicts=len(decision.blocker_ids), queue_seq=queue_seq,
                               held_conflicts=len(decision.held_blockers),
                               pending_predecessors=len(decision.pending_predecessors),
                               decision_id=payload["decision_id"])
                    return cache.set({"orbit_id": oid,
                                      "request_id": semantic_request_id,
                                      "request_generation": request_generation,
                                      "bail_epoch": effective_bail_epoch,
                                      "state": "PENDING", "queue_seq": queue_seq,
                                      "wait_deadline": wait_deadline,
                                      "queue_depth": queue_depth,
                                      "queue_capacity": self.admission_queue_capacity,
                                      "base_priority": decision.base_priority,
                                      "effective_priority": decision.effective_priority,
                                      "observed_at": decision.observed_at,
                                      "conflicts": list(decision.blocker_ids),
                                      "held_conflicts": list(decision.held_blockers),
                                      "pending_predecessors": list(
                                          decision.pending_predecessors),
                                      "decision_id": payload["decision_id"]})
                committed_seq = self.store.next_seq()
                if committed_seq != queue_seq:
                    raise RuntimeError(
                        "admission queue ticket changed inside authority transaction"
                    )
                fence = self.store.next_fence()
                lease_deadline = observed_at + ttl
                payload = self._admission_payload(
                    "ADMISSION_GRANTED", identity, snapshot_hash,
                    fence=fence,
                    lease_deadline=lease_deadline,
                    base_priority=decision.base_priority,
                    effective_priority=decision.effective_priority,
                    observed_at=decision.observed_at,
                )
                self._assert_admission_projection(
                    {**identity, "state": "REQUESTED"}, "ADMISSION_GRANTED",
                    payload, snapshot_hash, "HELD")
                # §D12: read-궤도는 분기한 통합 generation 을 박는다 — 이후 겹치는 응결이
                # 이보다 새 gen 을 만들면 stale 로 표시돼 consumer 가 옛 base 위에 빌드하는 것을 막는다.
                read_gen = self.store.integration_gen() if mode == "read" else None
                oid = self.store.add_orbit(
                    orbit_id=oid, task_id=task_id, agent_id=agent_id,
                    pathspec=pathspec, mode=mode, state="HELD", fence=fence,
                    expires_at=lease_deadline, reason=reason, priority=priority,
                    intent_key=ikey, read_gen=read_gen, queue_seq=queue_seq,
                    requested_ttl=ttl,
                    policy_version=self.admission_policy.version,
                    pathspec_digest=identity["pathspec_digest"],
                    request_id=semantic_request_id,
                    request_generation=request_generation,
                    bail_epoch=effective_bail_epoch,
                    authority_snapshot_hash=snapshot_hash,
                    decision_id=payload["decision_id"],
                    decision_type="ADMISSION_GRANTED",
                    decision_schema="admission_decision/v2",
                    decision_observed_at=decision.observed_at,
                    decision_effective_priority=decision.effective_priority,
                    blocker_ids=[])
                # A fresh HELD ownership edge can close a cycle with another
                # request already queued by the same agent. Queue insertion and
                # promotion paths are checked separately; close the immediate-
                # grant cutpoint in this same authority transaction and clock.
                self._deny_reservation_cycles(
                    observed_at,
                    reason="reservation_cycle_after_grant",
                )
                # §D12: read claim 은 그 task 의 read-set 동기화 gen 을 박는다(궤도 생명과 분리 —
                # 궤도를 release 한 뒤에도 consumer 의 connect 가 코히런스를 검사하도록). 여러 read
                # 를 claim 하면 가장 옛 gen(보수적)로 고정한다.
                if mode == "read" and task_id is not None:
                    t = self.store.get_task(task_id)
                    if t is not None:
                        prev = t["read_synced_gen"]
                        if prev is None or read_gen < prev:
                            self.store.set_task(task_id, read_synced_gen=read_gen)
                self._emit("orbit_granted", agent_id, orbit_id=oid, fence=fence, mode=mode,
                           queue_seq=queue_seq, decision_id=payload["decision_id"])
                return cache.set({"orbit_id": oid, "request_id": semantic_request_id,
                                  "request_generation": request_generation,
                                  "state": "HELD", "fence": fence,
                                  "queue_seq": queue_seq, "conflicts": [],
                                  "base_priority": decision.base_priority,
                                  "effective_priority": decision.effective_priority,
                                  "observed_at": decision.observed_at,
                                  "decision_id": payload["decision_id"],
                                  "bail_epoch": effective_bail_epoch})

    def cancel_wait(self, orbit_id, agent_id, request_generation, *, bail_epoch,
                    request_id=None):
        """Cancel one authenticated PENDING admission request.

        ``request_id`` identifies this cancellation operation.  The semantic
        CANCEL payload remains bound to the original admission request id stored
        on the orbit row.  Exact operation replay is therefore checked before
        lifecycle and liveness validation, while every new operation must prove
        the current owner, request generation, and bail epoch.
        """
        if not isinstance(orbit_id, str) or not orbit_id:
            return {"ok": False, "state": "REJECTED", "reason": "invalid_orbit_id"}
        if not isinstance(agent_id, str) or not agent_id:
            return {"ok": False, "state": "REJECTED", "reason": "invalid_agent_id"}
        if request_id is not None and (
            not isinstance(request_id, str) or not request_id
        ):
            return {"ok": False, "state": "REJECTED", "reason": "invalid_request_id"}
        if (
            not isinstance(request_generation, int)
            or isinstance(request_generation, bool)
            or request_generation < 0
        ):
            return {
                "ok": False,
                "state": "REJECTED",
                "reason": "invalid_request_generation",
            }
        if (
            not isinstance(bail_epoch, int)
            or isinstance(bail_epoch, bool)
            or bail_epoch < 0
        ):
            return {"ok": False, "state": "REJECTED", "reason": "invalid_bail_epoch"}

        def cancelled_response(row, *, noop=False):
            response = {
                "ok": True,
                "orbit_id": row["orbit_id"],
                "request_id": row["request_id"],
                "cancel_request_id": request_id,
                "request_generation": row["request_generation"],
                "bail_epoch": row["bail_epoch"],
                "state": "CANCELLED",
                "legacy_state": "DENIED",
                "terminal_reason": row["terminal_reason"] or "cancelled",
            }
            if noop:
                response["noop"] = True
            return response

        with self._cs():
            idem_args = [orbit_id, request_generation, bail_epoch]
            with self._idem(
                request_id, agent_id, "cancel_wait", idem_args
            ) as cache:
                if cache.hit:
                    return cache.value
                orbit = self.store.get_orbit(orbit_id)
                if orbit is None:
                    return cache.set({"ok": False, "reason": "no such orbit"})
                if orbit.get("kind") != "orbit":
                    return cache.set({
                        "ok": False,
                        "reason": "resource is not a cancellable orbit",
                        "orbit_id": orbit_id,
                    })
                if orbit["agent_id"] != agent_id:
                    return cache.set({
                        "ok": False,
                        "reason": "not owner",
                        "owner": orbit["agent_id"],
                    })
                if int(orbit["request_generation"] or 0) != request_generation:
                    return cache.set({
                        "ok": False,
                        "reason": "stale request_generation",
                        "fenced_out": True,
                        "current": int(orbit["request_generation"] or 0),
                        "yours": request_generation,
                    })
                if int(orbit["bail_epoch"] or 0) != bail_epoch:
                    return cache.set({
                        "ok": False,
                        "reason": "stale bail_epoch",
                        "fenced_out": True,
                        "current": int(orbit["bail_epoch"] or 0),
                        "yours": bail_epoch,
                    })
                agent = self.store.get_agent(agent_id)
                if agent is None:
                    return cache.set({
                        "ok": False,
                        "reason": "agent not registered",
                        "fenced_out": True,
                    })
                dead = self._check_alive(agent_id, bail_epoch)
                if dead:
                    return cache.set(dead)
                # F2: an authenticated mutating request is itself liveness
                # evidence. Refresh before reconciliation so this live caller is
                # not reclaimed in the same transaction for an old heartbeat.
                self.store.upsert_agent(agent_id)
                if orbit["state"] == "DENIED" and orbit["decision_type"] == "CANCEL":
                    return cache.set(cancelled_response(orbit, noop=True))
                if orbit["state"] != "PENDING":
                    return cache.set({
                        "ok": False,
                        "reason": f"not PENDING: {orbit['state']}",
                        "state": orbit["state"],
                    })

                snapshot_hash = authority_snapshot_hash(
                    self.store.held_orbits(),
                    self.store.pending_orbits(),
                    coordinator_epoch=self.leader_epoch,
                )
                identity = self._admission_identity(orbit)
                payload = {
                    "repository_id": identity["repository_id"],
                    "request_id": identity["request_id"],
                    "orbit_id": identity["orbit_id"],
                    "request_generation": identity["request_generation"],
                    "actor": self.coordinator_id,
                    "owner_agent": identity["owner_agent"],
                    "bail_epoch": identity["bail_epoch"],
                    "authority_snapshot_hash": snapshot_hash,
                    "event_id": f"evt-{uuid.uuid4().hex}",
                }
                reduced = self._assert_admission_projection(
                    {**identity, "state": "PENDING"},
                    "CANCEL",
                    payload,
                    snapshot_hash,
                    "DENIED",
                )
                if reduced.context["state"] != "CANCELLED":
                    raise RuntimeError(
                        "admission cancellation did not reach semantic CANCELLED"
                    )
                now = time.time()
                self.store.set_orbit(
                    orbit_id,
                    state=fsm.advance("orbit", "PENDING", "deny"),
                    released_at=now,
                    authority_snapshot_hash=snapshot_hash,
                    decision_id=None,
                    decision_type="CANCEL",
                    blocker_ids=[],
                    terminal_reason="cancelled",
                )
                self._emit(
                    "orbit_cancelled",
                    agent_id,
                    orbit_id=orbit_id,
                    request_id=identity["request_id"],
                    request_generation=request_generation,
                    cancel_request_id=request_id,
                    queue_seq=orbit["queue_seq"],
                )
                self._reconcile_admission()
                cancelled = dict(orbit)
                cancelled["terminal_reason"] = "cancelled"
                return cache.set(cancelled_response(cancelled))

    def renew(self, orbit_id, agent_id, fence, ttl=600.0, *, request_id=None,
              bail_epoch=None):
        """궤도 lease 갱신(keepalive). 소유+fence 일치해야 — 오추방된 좀비는 FENCED_OUT."""
        try:
            ttl = float(ttl)
        except (TypeError, ValueError):
            return {"ok": False, "reason": "invalid ttl"}
        if not math.isfinite(ttl) or ttl <= 0:
            return {"ok": False, "reason": "invalid ttl"}
        with self._cs():
            with self._idem(request_id, agent_id, "renew",
                            [orbit_id, fence, ttl]) as cache:
                if cache.hit:
                    return cache.value
                dead = self._check_alive(agent_id, bail_epoch)
                if dead:
                    return cache.set(dead)
                self.store.upsert_agent(agent_id)
                o = self.store.get_orbit(orbit_id)
                if not o:
                    return cache.set({"ok": False, "reason": "no such orbit"})
                if o["state"] != "HELD":
                    return cache.set({"ok": False, "reason": f"not HELD: {o['state']}",
                                      "fenced_out": True})
                bad = self._check_owner(o, agent_id, fence)
                if bad:
                    return cache.set(bad)
                lease_deadline = time.time() + ttl
                snapshot_hash, _, _ = self._reduce_orbit_lifecycle(
                    o, "RENEW", lease_deadline=lease_deadline
                )
                self.store.set_orbit(
                    orbit_id,
                    state=fsm.advance("orbit", "HELD", "renew"),
                    expires_at=lease_deadline,
                    authority_snapshot_hash=snapshot_hash,
                    decision_id=None,
                    decision_type="RENEW",
                )
                self._emit("orbit_renewed", agent_id, orbit_id=orbit_id)
                return cache.set({"ok": True, "expires_in": ttl})

    def release(self, orbit_id, agent_id, fence, *, request_id=None, bail_epoch=None):
        """궤도 lease 반납. 소유+fence 일치해야(P0-3) — 아무나 남의 궤도 해제 불가.
        이미 RELEASED/EXPIRED면 멱등 OK(MCP 재시도 안전). §3.C: dedup 재생이 *재부여된* lease를
        풀지 않게 owner/fence 가드가 감싼다(release는 소유+fence 통과 후에만 작용)."""
        with self._cs():
            with self._idem(request_id, agent_id, "release",
                            [orbit_id, fence]) as cache:
                if cache.hit:
                    return cache.value
                dead = self._check_alive(agent_id, bail_epoch)
                if dead:
                    return cache.set(dead)
                self.store.upsert_agent(agent_id)   # F2: 활동=생존신호(mutating verb 가 liveness touch)
                o = self.store.get_orbit(orbit_id)
                if not o:
                    return cache.set({"ok": False, "reason": "no such orbit"})
                if o.get("kind") != "orbit":
                    return cache.set({
                        "ok": False,
                        "reason": f"resource is not a releasable orbit: {o.get('kind')}",
                        "orbit_id": orbit_id,
                    })
                if o["state"] in ("RELEASED", "EXPIRED", "DENIED"):
                    return cache.set({"ok": True, "noop": True, "state": o["state"]})
                if o["state"] != "HELD":
                    return cache.set({"ok": False, "reason": f"not HELD: {o['state']}"})
                if o.get("merging"):
                    return cache.set({
                        "ok": False, "reason": "connect_effect_in_progress",
                        "retry": True, "orbit_id": orbit_id,
                    })
                bad = self._check_owner(o, agent_id, fence)
                if bad:
                    return cache.set(bad)
                snapshot_hash, _, _ = self._reduce_orbit_lifecycle(o, "RELEASE")
                self.store.set_orbit(
                    orbit_id,
                    state=fsm.advance("orbit", "HELD", "release"),
                    released_at=time.time(),
                    authority_snapshot_hash=snapshot_hash,
                    decision_id=None,
                    decision_type="RELEASE",
                )
                self._emit("orbit_released", agent_id, orbit_id=orbit_id)
                self._reconcile_admission()
                return cache.set({"ok": True})

    def bail(self, agent_id, *, request_id=None):
        """물방울 긴급 탈출(자발). 보유 궤도 전부 해제 + 작업 requeue + worktree/브랜치 정리.
        멱등 — 비자발 좀비회수와 **단일 루틴**을 공유(둘 사이 누락/이중해제 없음). bail_epoch 검사는
        없음: 죽으려는 자의 탈출은 항상 허용돼야(자기 회수). request_id로 응답도 멱등."""
        with self._connect_effect(blocking=False) as owns_effect:
            if not owns_effect:
                return self._split_effect_busy(
                    request_id, agent_id, "bail", [agent_id]
                )
            self._recover_under_effect()
            with self._cs():
                with self._idem(request_id, agent_id, "bail", [agent_id]) as cache:
                    if cache.hit:
                        return cache.value
                    return cache.set(
                        self._reclaim_agent_inline(agent_id, voluntary=True)
                    )

    def heartbeat(self, agent_id, *, ttl=None):
        """물방울 생존 신호. §D6 표: 이미 회수(RETIRED)된 좀비에겐 `{fenced_out:true}` 회신 →
        좀비가 다음 heartbeat에서 자기 죽음을 안다(advisory). 살아있으면 현재 bail_epoch를 회신해
        물방울이 이후 변이에 실어 보내면 회수 후 부활을 서버가 거부할 수 있다(§D6).

        F2(채택마찰 2026-07-02): `ttl=` 로 *자기 페이스를 선언* — 이 agent 의 per-agent 생존창
        (liveness_ttl). 인터랙티브 세션(verb 간 침묵 수십 분)이 claim 직후 한 번 선언하면 좀비
        회수가 그 창을 존중한다. 미선언 agent 는 기본 agent_ttl(기계 물방울 crash-fast §D2 불변)."""
        if ttl is not None:
            try:
                ttl = float(ttl)
            except (TypeError, ValueError):
                return {"ok": False, "reason": "invalid_liveness_ttl",
                        "liveness_ttl": ttl}
            if not math.isfinite(ttl) or ttl <= 0:
                return {"ok": False, "reason": "invalid_liveness_ttl",
                        "liveness_ttl": ttl}
        with self._cs():
            ag = self.store.get_agent(agent_id)
            if ag is not None and ag["state"] == "RETIRED":
                # 회수된 좀비 — heartbeat로 부활시키지 않고 죽음을 통지(fence 복종 규율).
                return {"ok": False, "fenced_out": True, "reason": "agent reclaimed",
                        "bail_epoch": ag["bail_epoch"]}
            self.store.upsert_agent(agent_id)
            if ttl is not None:
                self.store.set_agent_liveness_ttl(agent_id, ttl)
            # D3(§1.2 / D2 §): heartbeat 한 번이 이 agent 의 모든 hb_bound flag_ephemeral lease 를
            # 갱신 — 건강한 producer 가 renew 깜빡해 자기 신호 플래그가 BROKEN 되는 일 방지.
            renewed = 0
            for fl in self.store.flag_leases_owned_by(agent_id, ("HELD",)):
                if fl["expires_at"] is not None:
                    base = fl["expires_at"] - fl["created_at"] if fl["created_at"] else None
                    ttl = base if (base and base > 0) else (self.agent_ttl or 90.0)
                    self.store.set_orbit(fl["orbit_id"], expires_at=time.time() + ttl)
                    renewed += 1
            # D4(§1.2/G): 건강한 보유자의 sem_permit 도 heartbeat 로 연장 — renew 깜빡으로 슬롯을
            # 잃지 않게(궤도/permit 의 비대칭 만료가 빌드 슬롯 이중배정을 부르는 §G 를 함께 닫음).
            permits_renewed = 0
            for p in self.store.sem_permits_owned_by(agent_id, ("HELD",)):
                if p["expires_at"] is not None:
                    base = p["expires_at"] - p["created_at"] if p["created_at"] else None
                    ttl = base if (base and base > 0) else (self.agent_ttl or 90.0)
                    self.store.set_orbit(p["orbit_id"], expires_at=time.time() + ttl)
                    permits_renewed += 1
            ag = self.store.get_agent(agent_id)
            return {"ok": True, "bail_epoch": ag["bail_epoch"],
                    "flag_leases_renewed": renewed, "sem_permits_renewed": permits_renewed}

    def reclaim_zombies(self):
        """heartbeat 끊긴 물방울 회수: HELD 궤도 만료 + 작업 requeue + worktree 정리."""
        if not self.agent_ttl:
            return {"reclaimed": []}
        with self._cs():
            return {"reclaimed": self._reclaim_zombies_inline()}

    def sweep(self):
        with self._connect_effect(blocking=False) as owns_effect:
            with self._cs():
                before = {o["orbit_id"] for o in self.store.held_orbits()}
            if owns_effect:
                self._recover_under_effect()
            with self._cs():
                self._sweep_inline()
                after = {o["orbit_id"] for o in self.store.held_orbits()}
                result = {"expired": sorted(before - after)}
            self._wake_admission_outbox()
            with self._cs():
                result["admission_outbox"] = self.store.admission_outbox_stats()
            return result

    def next_task(self, agent_id):
        """deps 충족 + write-set이 활성 HELD와 서로소인 작업 1개 → READY로 올려 반환.
        P2 레인: 선언 shared glob 은 shared HELD 궤도와의 겹침은 허용(공존) — 배타(write/read)
        HELD 와 겹치면 여전히 대기."""
        with self._cs():
            self._sweep_inline()
            held = self.store.held_orbits()
            held_specs = [(json.loads(o["pathspec"]), o["mode"]) for o in held]
            for t in self.store.tasks_by_state(["PENDING", "READY", "BLOCKED"]):
                deps = json.loads(t["deps"])
                if not all((self.store.get_task(d) or {}).get("state") == "MERGED" for d in deps):
                    continue
                writes = json.loads(t["writes"])
                if any(sets_overlap(writes, spec) for spec, _ in held_specs):
                    continue
                shared = json.loads(t["shared"] or "[]") if "shared" in t.keys() else []
                if any(sets_overlap(shared, spec)
                       for spec, m in held_specs if m != "shared"):
                    continue
                if t["state"] != "READY":
                    self.store.set_task(t["task_id"],
                                        state=fsm.advance("task", t["state"], "ready"))
                self._emit("task_ready", agent_id, task=t["task_id"])
                return self.store.get_task(t["task_id"])
            return None

    def task_conditions(self, task_id):
        """task 의 K8s식 직교 condition(deps_satisfied/held/heartbeat_fresh/merge_ready)을 store-join
        에서 파생 → 관측 이벤트 방출(cid=task_id). 순수 관측 read-verb — lifecycle 전이 없음.
        derive_task_phase 는 fsm state 위의 rollup(authoritative 아님). 미존재 task 는 None."""
        with self._cs():
            t = self.store.get_task(task_id)
            if t is None:
                return None
            c = task_state.task_conditions(t, self.store, time.time(), self.agent_ttl)
            phase = task_state.derive_task_phase(c, t["state"])
            self._emit("task_conditions", task_id, state=t["state"], phase=phase, **c)
            return {"task_id": task_id, "state": t["state"], "phase": phase, **c}

    def start(self, task_id, agent_id, *, request_id=None, bail_epoch=None):
        """READY task에 agent 배정 → IN_ORBIT. repo 바인딩 시 물방울 worktree 발사.
        §D9 의미적 멱등: 이미 이 agent로 시작된(IN_ORBIT/이후 + worktree 존재) task 재시도는
        worktree를 재생성하지 않고 기존 것을 반환한다 — `worktree add -b`가 기존 브랜치에서
        실패(GitError+중복행)하던 버그 차단."""
        with self._cs():
            with self._idem(request_id, agent_id, "start", [task_id]) as cache:
                if cache.hit:
                    return cache.value
                dead = self._check_alive(agent_id, bail_epoch)
                if dead:
                    return cache.set(dead)
                t = self.store.get_task(task_id)
                # 이미 시작됨(같은 agent) — worktree 재생성 금지(자연 멱등).
                if t["state"] in ("IN_ORBIT", "DONE", "CONNECTING", "MERGED") \
                        and t["agent_id"] == agent_id:
                    self._emit("task_start_dedup", agent_id, task=task_id)
                    return cache.set({"task_id": task_id, "state": t["state"],
                                      "worktree": t["worktree"], "branch": t["branch"],
                                      "dedup": True})
                s = t["state"]
                if s == "READY":
                    s = fsm.advance("task", s, "claim")
                s = fsm.advance("task", s, "start")  # CLAIMED→IN_ORBIT
                self.store.upsert_agent(agent_id)
                worktree = branch = None
                if self.git:
                    branch = f"omd/{task_id}"
                    worktree = os.path.join(self.worktrees_dir, task_id)
                    self.git.add_worktree(branch, worktree)
                self.store.set_task(task_id, state=s, agent_id=agent_id,
                                    worktree=worktree if self.git else ...,
                                    branch=branch if self.git else ...)
                self._emit("task_started", agent_id, task=task_id, worktree=worktree)
                return cache.set({"task_id": task_id, "state": s, "worktree": worktree,
                                  "branch": branch})

    def commit(self, task_id, msg, agent_id=None, fence=None, *, request_id=None,
               bail_epoch=None):
        """물방울 worktree의 변경을 커밋(repo 바인딩 시). 커밋 후 write-set 감사(§D10/P0-11)를
        **자문(advisory)** 으로 돌려 궤도 밖 경로를 조기 노출한다(`offending` 동봉). 단 connect
        게이트가 *권위* 강제 지점이므로 여기선 커밋을 되돌리지 않는다 — 물방울이 일찍 알아채게.
        §D6: caller가 (agent,fence)를 주면 owner∧write-orbit HELD∧fence==f 재검증(opt-in) —
        오추방된 좀비가 남의 worktree를 커밋하지 못하게."""
        if not self.git:
            return {"ok": False, "reason": "no repo bound"}
        with self._cs():
            with self._idem(request_id, agent_id, "commit",
                            [task_id, msg, fence]) as cache:
                if cache.hit:
                    return cache.value
                dead = self._check_alive(agent_id, bail_epoch)
                if dead:
                    return cache.set(dead)
                if agent_id:
                    self.store.upsert_agent(agent_id)   # F2: 활동=생존신호
                bad = self._check_task_write_fence(task_id, agent_id, fence)
                if bad:
                    self._emit("commit_rejected", task_id, reason=bad["reason"])
                    return cache.set(bad)
                t = self.store.get_task(task_id)
                if t is None:
                    return cache.set({"ok": False, "reason": "no such task"})
                writes = [o for o in self.store.orbits_for_task(task_id)
                          if o["mode"] in WRITE_MODES and o["state"] == "HELD"]
                if t["state"] in ("CONNECTING", "MERGED") or any(
                        o.get("merging") for o in writes):
                    # Phase A captured an immutable candidate.  Refuse the
                    # public writer as well so callers do not strand a later
                    # commit on a branch whose audited generation is already
                    # being connected.  Exact-SHA Phase B remains the final
                    # authority against out-of-band Git ref movement.
                    self._emit("commit_rejected", task_id, reason="connect_in_progress")
                    return cache.set({
                        "ok": False, "reason": "connect_in_progress",
                        "task_id": task_id, "state": t["state"],
                    })
                write_globs = self._claimed_write_globs(task_id, writes)
                if self.strict_writeset:
                    # P5 strict: 궤도-밖 경로를 **commit 전에** staged 에서 제외 → 위반이 history
                    # 진입 못 함(no wedge). 밖-경로는 working tree 에 보존(uncommitted) + 라우드
                    # 리포트. in-orbit 변경이 하나도 없으면 ok:False(nothing_in_orbit). git add -A
                    # 로 재staged 되어도 매 commit 마다 일관 제외 → livelock 0(기본 off=advisory).
                    self.git.stage_all(t["worktree"])
                    excluded = [p for p in self.git.staged_paths(t["worktree"])
                                if not path_in_globs(p, write_globs)]
                    if excluded:
                        self.git.unstage(t["worktree"], excluded)
                        self._emit("commit_excluded_out_of_orbit", task_id, excluded=excluded)
                    try:
                        sha = self.git.commit_staged(t["worktree"], msg)
                    except GitNothingToCommit:
                        return cache.set({"ok": False, "reason": "nothing_in_orbit",
                                          "excluded": excluded, "claimed": write_globs,
                                          "task_id": task_id})
                    self._emit("task_committed", t["agent_id"], task=task_id, sha=sha)
                    res = {"ok": True, "sha": sha}
                    if excluded:
                        res["excluded_out_of_orbit"] = excluded
                    return cache.set(res)
                # ---- advisory(기본) 경로 — 기존 동작 불변(commit 후 자문 감사, connect가 권위 거부) ----
                sha = self.git.commit_all(t["worktree"], msg)
                self._emit("task_committed", t["agent_id"], task=task_id, sha=sha)
                res = {"ok": True, "sha": sha}
                try:
                    branch_tip, integration_base, offending = \
                        self._snapshot_connect_candidate(
                            task_id, t["branch"], write_globs
                        )
                except GitError as exc:
                    # commit-time audit is advisory, but a failed observation
                    # must never be reported as a successful clean audit.
                    self._emit(
                        "commit_writeset_audit_unavailable", task_id, error=str(exc)
                    )
                    res["writeset_audit_unavailable"] = True
                    res["writeset_audit_error"] = str(exc)
                    return cache.set(res)
                res["audited_branch_tip_sha"] = branch_tip
                res["audited_integration_base_sha"] = integration_base
                if offending:
                    # 자문 경고 — connect에서 거부될 것임. 물방울은 지금 바로잡아야 한다.
                    self._emit("commit_writeset_warning", task_id, offending=offending)
                    res["writeset_violation"] = True
                    res["offending"] = offending
                return cache.set(res)

    def finish(self, task_id, agent_id=None, fence=None, *, request_id=None,
               bail_epoch=None):
        """작업 완료 표시(IN_ORBIT→DONE, `done` latch). §D6: caller가 (agent,fence)를 주면
        owner∧write-orbit HELD∧fence==f 재검증(opt-in) — 오추방된 좀비가 남의 task를 finish해
        분열을 부르지 못하게. 무인자 호출은 증분2까지 동작 유지(하위호환)."""
        with self._cs():
            with self._idem(request_id, agent_id, "finish", [task_id, fence]) as cache:
                if cache.hit:
                    return cache.value
                dead = self._check_alive(agent_id, bail_epoch)
                if dead:
                    return cache.set(dead)
                if agent_id:
                    self.store.upsert_agent(agent_id)   # F2: 활동=생존신호
                bad = self._check_task_write_fence(task_id, agent_id, fence)
                if bad:
                    self._emit("finish_rejected", task_id, reason=bad["reason"])
                    return cache.set(bad)
                t = self.store.get_task(task_id)
                # 의미적 멱등: 이미 DONE(또는 이후)이면 finish 재시도는 no-op.
                if t["state"] in ("DONE", "CONNECTING", "MERGED"):
                    return cache.set({"task_id": task_id, "state": t["state"], "noop": True})
                self.store.set_task(task_id, state=fsm.advance("task", t["state"], "finish"))
                self.store.set_flag(task_id, "done", set_by=t["agent_id"])
                self._emit("task_finished", t["agent_id"], task=task_id)
                return cache.set({"task_id": task_id, "state": "DONE"})

    def _terminalize_cancelled_task_orbits(self, task_id, *, reason):
        """Close every live admission row owned by a cancelled task.

        Task state and admission state are separate projections.  A task may
        still be PENDING/READY while one of its write sets is HELD, so changing
        only the task row can leak authority or let an orphaned waiter promote.
        Apply the semantic CANCEL/RELEASE events first and reconcile only after
        every associated row is terminal.
        """
        now = time.time()
        terminalized = []
        for orbit in sorted(
            self.store.orbits_for_task(task_id), key=lambda row: row["orbit_id"]
        ):
            # A successful immediate claim may still have a generic DONE replay
            # containing its old HELD fence.  Task cancellation supersedes that
            # response even when the orbit was already released separately.
            self.store.clear_idem(orbit["request_id"])
            if orbit["state"] not in ("PENDING", "HELD"):
                continue
            snapshot_hash = authority_snapshot_hash(
                self.store.held_orbits(),
                self.store.pending_orbits(),
                coordinator_epoch=self.leader_epoch,
            )
            identity = self._admission_identity(orbit)
            payload = {
                "repository_id": identity["repository_id"],
                "request_id": identity["request_id"],
                "orbit_id": identity["orbit_id"],
                "request_generation": identity["request_generation"],
                "actor": self.coordinator_id,
                "owner_agent": identity["owner_agent"],
                "bail_epoch": identity["bail_epoch"],
                "authority_snapshot_hash": snapshot_hash,
                "event_id": f"evt-{uuid.uuid4().hex}",
            }
            context = {**identity, "state": orbit["state"]}
            if orbit["state"] == "PENDING":
                self._assert_admission_projection(
                    context, "CANCEL", payload, snapshot_hash, "DENIED"
                )
                next_state = fsm.advance("orbit", "PENDING", "deny")
                decision_type = "CANCEL"
                event_type = "orbit_cancelled"
            else:
                payload["fence"] = orbit["fence"]
                context["fence"] = orbit["fence"]
                self._assert_admission_projection(
                    context, "RELEASE", payload, snapshot_hash, "RELEASED"
                )
                next_state = fsm.advance("orbit", "HELD", "release")
                decision_type = "RELEASE"
                event_type = "orbit_released"
            self.store.set_orbit(
                orbit["orbit_id"],
                state=next_state,
                released_at=now,
                merging=0,
                merge_deadline=None,
                authority_snapshot_hash=snapshot_hash,
                decision_id=None,
                decision_type=decision_type,
                blocker_ids=[],
                terminal_reason="task_cancelled",
            )
            if orbit["mode"] == "read":
                self._clear_read_stale_signal(orbit["orbit_id"])
            terminalized.append(orbit["orbit_id"])
            self._emit(
                event_type,
                orbit["agent_id"],
                orbit_id=orbit["orbit_id"],
                task=task_id,
                reason=f"task_cancel:{reason}" if reason else "task_cancel",
            )
        return terminalized

    def cancel(self, task_id, *, reason="", request_id=None):
        """F4(채택마찰 2026-07-02): **미시작** 태스크의 종결 verb — lease-only 흐름(declare+claim,
        start 미경유)의 태스크가 PENDING 으로 영구 잔류하던 갭 봉합. FSM 의 기존 `abort` 전이
        (source="*") 재사용이라 상태기계/TLA 모델 무변경 — PENDING/READY/BLOCKED → ABORTED(종결,
        requeue 로 재개 가능). 시작된 태스크(IN_ORBIT 이후)는 거부 — 진행중 작업의 무단 증발 금지,
        finish/bail 경유. 연결된 PENDING/HELD admission row도 같은 transaction에서 각각 semantic
        CANCEL/RELEASE로 종결한 뒤 한 번만 reconcile한다. 멱등: 이미 ABORTED면 legacy orphan도
        복구하고 {ok, already}. 미존재는 fail-loud(캐시 안 함)."""
        with self._cs():
            with self._idem(request_id, task_id, "cancel", [task_id]) as cache:
                if cache.hit:
                    return cache.value
                t = self.store.get_task(task_id)
                if t is None:
                    return {"ok": False, "reason": "no such task"}       # 캐시 금지 — 이후 declare 가능
                if t["state"] == "ABORTED":
                    terminalized = self._terminalize_cancelled_task_orbits(
                        task_id, reason=reason or "idempotent_repair"
                    )
                    self._reconcile_admission()
                    return cache.set({
                        "ok": True,
                        "already": True,
                        "state": "ABORTED",
                        "orbits": terminalized,
                    })
                if t["state"] not in ("PENDING", "READY", "BLOCKED"):
                    return {"ok": False,                                  # 캐시 금지 — finish 후 재시도 무해
                            "reason": f"cancel 은 미시작 태스크 전용(state={t['state']}) — "
                                      f"시작된 작업은 finish/bail 경유"}
                s = fsm.advance("task", t["state"], "abort")
                self.store.set_task(task_id, state=s)
                terminalized = self._terminalize_cancelled_task_orbits(
                    task_id, reason=reason
                )
                self._emit("task_cancelled", task_id, task=task_id, reason=reason)
                self._reconcile_admission()
                return cache.set({
                    "ok": True,
                    "state": s,
                    "reason": reason,
                    "orbits": terminalized,
                })

    # ---- CLOUD CONNECT — split-phase A–B–C (§3.B/§D8/§D11) ----
    def connect(self, task_id, agent_id=None, fence=None, push=None, *, request_id=None,
                bail_epoch=None):
        if request_id is not None and (
            not isinstance(request_id, str) or not request_id
        ):
            # Reserve no split-effect authority for an envelope recovery cannot
            # identify exactly.  This mirrors claim() and is deliberately before
            # the process/repo effect lock and every DB/Git mutation.
            return {"ok": False, "state": "REJECTED", "reason": "invalid_request_id"}
        idem_args = [task_id, agent_id, fence, push, bail_epoch]
        deadline = time.time() + max(self.merge_timeout, 5.0) + 10.0
        while True:
            with self._connect_effect(blocking=False) as owns_effect:
                if owns_effect:
                    # An already-running peer can recover a process that died
                    # after this Coordinator started; the same lock prevents
                    # touching a live peer.
                    self._recover_under_effect()
                    return self._connect_owned(
                        task_id, agent_id, fence, push,
                        request_id=request_id, bail_epoch=bail_epoch,
                    )
            busy = self._split_effect_busy(
                request_id, agent_id, "connect", idem_args
            )
            if busy.get("reason") in ("request_inflight", "idempotency_conflict"):
                return busy
            if time.time() >= deadline:
                return busy
            time.sleep(0.01)

    def _connect_owned(self, task_id, agent_id=None, fence=None, push=None, *,
                       request_id=None, bail_epoch=None):
        """CLOUD CONNECT(응결=merge). **split-phase** — git merge가 락(_cs) **밖**에서 돈다:
        push: per-call remote override(없으면 self.auto_push 상속). merge 직후 통합브랜치 push.
          A(락): write-orbit 재검증(P0-4 HELD∧fence==captured) + merge_token 획득 + →CONNECTING
                 + 궤도 pin(merging=1) + intent 영속 + 커밋.
          B(락밖): 전용 통합 worktree에서 merge --no-ff(타임아웃, §E). 충돌/타임아웃이면 abort.
          C(락): merge_sha 먼저 기록(P0-6) → →MERGED + write-orbit 해제 + merge_token 반납 + unpin.
        fencing: 작업 중 lease 만료/해제면 거부(stale fence). merge_token으로 동시 connect를 직렬화.
        §D9: request_id 멱등(성공만 캐시). split-phase라 _idem 트랜잭션을 Phase B에 걸칠 수 없어
        캐시 확인/기록을 짧은 _cs() 두 곳으로 나눈다. 의미적 멱등(already-MERGED)은 fencing 위(§3.C):
        connect는 owner/fence 통과 후에만 머지하므로 dedup 재생이 재부여 lease를 풀지 않는다."""
        idem_args = [task_id, agent_id, fence, push, bail_epoch]
        # Reserve the exact request envelope before leaving the transaction.  A
        # concurrent exact retry sees request_inflight; a different envelope
        # cannot hijack the id while Phase B is outside the lock.
        with self._cs():
            with self._idem(
                request_id, agent_id, "connect", idem_args
            ) as cache:
                if cache.hit:
                    return cache.value
                t0 = self.store.get_task(task_id)
                if t0 and t0["state"] == "MERGED":
                    return cache.set({
                        "ok": True,
                        "task_id": task_id,
                        "state": "MERGED",
                        "merge_sha": t0["merge_sha"],
                        "noop": True,
                    })
                cache.defer()

        try:
            deadline = time.time() + max(self.merge_timeout, 5.0) + 10.0
            while True:
                a = self._connect_phase_a(
                    task_id, agent_id, fence, bail_epoch,
                    request_id=request_id,
                    request_agent=agent_id,
                    request_arg_hash=(
                        self._arg_hash("connect", idem_args)
                        if request_id is not None else None
                    ),
                )
                if not a["ok"]:
                    if a.get("retry") and time.time() < deadline:
                        time.sleep(0.01)   # merge_token 경합 — 다른 connect 응결중. 곧 재시도.
                        continue
                    return self._complete_split_idem(
                        request_id, agent_id, "connect", idem_args, a
                    )
                if a.get("noop"):
                    return self._complete_split_idem(
                        request_id, agent_id, "connect", idem_args, a
                    )
                # ----- Phase B: 락 밖(no _cs, no live tx) git merge -----
                token_id, intent = a["token_id"], a["intent"]
                merge_sha, err = self._connect_phase_b(intent, push=push)
                # ----- Phase C: 락 안 — merge_sha 먼저 기록 후 해제(P0-6) -----
                res = self._connect_phase_c(task_id, token_id, intent, merge_sha, err)
                if res.pop("_preserve_idempotency", False):
                    return res
                res["_idempotency_operation"] = intent
                return self._complete_split_idem(
                    request_id, agent_id, "connect", idem_args, res
                )
        except BaseException:
            # Do not release the request id at an ambiguous split-phase cut.
            # Restart recovery will bind it to the reconciled task outcome (or
            # mark the exact envelope RETRYABLE when no effect committed).
            raise

    def complete_task(self, task_id, msg=None, agent_id=None, fence=None, push=None,
                      *, request_id=None, bail_epoch=None):
        """P5 — happy-path 원샷: (선택)commit → finish → connect(+push). verb 망각-스트랜드
        (finish 빼면 IN_ORBIT 기아·connect 빼면 미통합) 방지. INV: ok:True 는 **오직 최종
        task state == MERGED** 일 때뿐 — 어느 단계 거부든 {ok:False, stage:'commit'|'finish'|
        'connect', ...원본거부...}로 fail-loud 전파(거부 은폐 금지). 하위 verb 엔 request_id
        suffix(:commit/:finish/:connect)로 idem PK 분리."""
        def _rid(s):
            return f"{request_id}:{s}" if request_id else None
        committed = False
        if msg is not None:
            try:
                cr = self.commit(task_id, msg, agent_id, fence,
                                 request_id=_rid("commit"), bail_epoch=bail_epoch)
            except GitNothingToCommit:
                cr = {"ok": True, "noop": True}   # 변경 없음(구조적 판별) — commit skip
            except (GitError, GitTimeout) as e:
                return {"ok": False, "stage": "commit", "error": str(e)}   # 진짜 실패는 은폐 안 함
            if cr.get("ok") is False:
                return {**cr, "ok": False, "stage": "commit"}
            committed = not cr.get("noop")
        fr = self.finish(task_id, agent_id, fence,
                         request_id=_rid("finish"), bail_epoch=bail_epoch)
        if fr.get("ok") is False:   # finish 성공은 'ok' 키 없음(state=DONE); 거부만 ok=False
            return {**fr, "ok": False, "stage": "finish"}
        cn = self.connect(task_id, agent_id, fence, push=push,
                          request_id=_rid("connect"), bail_epoch=bail_epoch)
        st = self.store.get_task(task_id)
        final = st["state"] if st else None
        if cn.get("ok") and final == "MERGED":   # INV: ok ⟺ MERGED(store 권위 확인)
            return {**cn, "ok": True, "stage": "connect", "state": "MERGED",
                    "committed": committed}
        return {**cn, "ok": False, "stage": "connect", "state": final}

    def begin(self, task_id, agent_id, writes, *, reads=None, shared=None, deps=None,
              priority=0, name="", ttl=600.0, liveness_ttl=None,
              request_id=None, bail_epoch=None):
        """P1/P5 — happy-path 원샷 onboarding: declare → deps 게이트 → claim(write-set lease)
        → promote(READY) → start(물방울 worktree 발사). 7-verb 앞단을 한 호출로 접어 "그냥
        begin 하면 OMD 안에서 격리" 되게 한다(채택 자동화 enabler; complete_task 의 start-side
        dual). INV: ok:True ⟺ 최종 store state == IN_ORBIT. fail-loud — 어느 단계 거부든
        {ok:False, stage:'declare'|'deps'|'claim'|'start', ...}로 전파(worktree 는 claim HELD
        확인 *후* 에만 발사 → 충돌 시 낭비 0). request_id 는 하위 verb 에 suffix(:claim/:start)로
        분리해 멱등 재시작 안전(재발사·중복 orbit 없음).

        liveness_ttl은 detached keeper가 생존을 위조하는 대신 이 agent가 무응답일 수 있는 *유계*
        창을 한 번 선언한다. orbit ttl보다 길 수 없고, 생략하면 기존 agent_ttl crash-fast가 유지된다.
        성공 응답은 각 orbit_id/fence를 돌려줘 caller가 명시적 fenced renew를 할 수 있게 한다."""
        def _rid(s):
            return f"{request_id}:{s}" if request_id else None
        # 0) TTL 계약 — NaN/inf/0/음수는 영원한 또는 즉시 stale lease를 조용히 만들므로 거부.
        try:
            ttl = float(ttl)
        except (TypeError, ValueError):
            return {"ok": False, "stage": "validate", "reason": "invalid_ttl",
                    "ttl": ttl}
        if not math.isfinite(ttl) or ttl <= 0:
            return {"ok": False, "stage": "validate", "reason": "invalid_ttl",
                    "ttl": ttl}
        if liveness_ttl is not None:
            try:
                liveness_ttl = float(liveness_ttl)
            except (TypeError, ValueError):
                return {"ok": False, "stage": "validate",
                        "reason": "invalid_liveness_ttl",
                        "liveness_ttl": liveness_ttl}
            if not math.isfinite(liveness_ttl) or liveness_ttl <= 0:
                return {"ok": False, "stage": "validate",
                        "reason": "invalid_liveness_ttl",
                        "liveness_ttl": liveness_ttl}
            if liveness_ttl > ttl:
                return {"ok": False, "stage": "validate",
                        "reason": "liveness_exceeds_orbit_ttl",
                        "ttl": ttl, "liveness_ttl": liveness_ttl}
        specs = []
        if writes:
            specs.append(("write", list(writes), "claim"))
        if shared:
            specs.append(("shared", list(shared), "claim-shared"))

        def _legacy_resume_intents_live():
            return bool(specs) and all(
                (dup := self.store.orbit_by_intent(
                    self._intent_key(agent_id, paths, mode, task_id)
                ))
                is not None
                and dup["agent_id"] == agent_id
                and int(dup["priority"] or 0) == priority
                and self.admission_policy.accepts_base_priority(
                    priority,
                    policy_version=dup.get("policy_version"),
                )
                for mode, paths, _ in specs
            )

        legacy_resume = False
        if not self.admission_policy.accepts_base_priority(priority) and specs:
            with self._cs():
                legacy_resume = _legacy_resume_intents_live()
        if (
            not self.admission_policy.accepts_base_priority(priority)
            and not legacy_resume
        ):
            return {
                "ok": False,
                "stage": "validate",
                "reason": "invalid_priority",
                "priority": priority,
            }
        # 1) declare (task_id 키 upsert — 자연 멱등, 진행중 state 는 보존)
        dc = self.declare(task_id, name=name, writes=writes, reads=reads, deps=deps,
                          priority=priority, shared=shared)
        if dc.get("ok") is False:
            return {**dc, "ok": False, "stage": "declare"}
        # 2) deps 게이트 — 미충족이면 claim/worktree 없이 정지(task_state SSOT 술어).
        unmet = [d for d in (deps or [])
                 if (self.store.get_task(d) or {}).get("state") != "MERGED"]
        if unmet:
            self._emit("begin_blocked", task_id, reason="deps", unmet=unmet)
            return {"ok": False, "stage": "deps", "task_id": task_id, "unmet": unmet}
        # 2.5) 선택적 silence window를 claim의 inline sweep *전에* 선언한다. 반복 heartbeat가
        # 아니라 서버가 만료를 판정할 수 있는 단일 유계 계약이며, 실패하면 lease를 잡지 않는다.
        if liveness_ttl is not None:
            hb = self.heartbeat(agent_id, ttl=liveness_ttl)
            if hb.get("ok") is False:
                return {**hb, "ok": False, "stage": "liveness", "task_id": task_id}
        # 3) write/shared batch claim (fail-fast — worktree 발사 전).
        # 먼저 같은 임계구역에서 *전부* preflight해, 뒤쪽 shared가 충돌할 때 앞쪽 exclusive
        # HELD만 남는 partial acquisition을 막는다. 충돌한 클래스 하나만 PENDING으로 등록해
        # promote→begin 재시도가 자연스럽게 이어진다. writes/shared 자체 중첩은 declare가 거부.
        claims = {}
        with self._cs():
            observed_at = self._sweep_inline()
            if legacy_resume and not _legacy_resume_intents_live():
                return {
                    "ok": False,
                    "stage": "claim",
                    "reason": "invalid_priority",
                    "detail": "legacy resume authority is no longer live",
                    "priority": priority,
                }
            for offset, (mode, paths, rid_suffix) in enumerate(specs, start=1):
                dup = self.store.orbit_by_intent(
                    self._intent_key(agent_id, paths, mode, task_id)
                )
                if dup is not None and dup["agent_id"] == agent_id:
                    if dup["state"] != "HELD":
                        self._emit("begin_blocked", task_id, reason="claim",
                                   mode=mode, state=dup["state"])
                        return {"ok": False, "stage": "claim", "task_id": task_id,
                                "mode": mode, "orbit_id": dup["orbit_id"],
                                "state": dup["state"], "conflicts": []}
                    claims[mode] = dup
                    continue
                # Hypothetical sequence is exact while this _cs transaction owns the
                # writer lock.  It lets begin see PENDING predecessors before acquiring
                # any member of its batch, preventing partial HELD acquisition.
                admission, _ = self._admission_decision(
                    paths,
                    mode,
                    priority,
                    self.store.current_seq() + offset,
                    policy_version=self.admission_policy.version,
                    enqueued_at=observed_at,
                    observed_at=observed_at,
                )
                if not admission.grantable:
                    pending = self.claim(
                        agent_id, paths, mode=mode, ttl=ttl, task_id=task_id,
                        priority=priority,
                        request_id=_rid(rid_suffix), bail_epoch=bail_epoch,
                        _observed_at=observed_at,
                    )
                    self._emit("begin_blocked", task_id, reason="claim", mode=mode,
                               state=pending.get("state"))
                    return {"ok": False, "stage": "claim", "task_id": task_id,
                            "mode": mode, "orbit_id": pending.get("orbit_id"),
                            "state": pending.get("state"),
                            "conflicts": pending.get(
                                "conflicts", list(admission.blocker_ids))}
            # 전 클래스가 지금 grant 가능하거나 이미 HELD임을 확인한 뒤 같은 tx에서 획득.
            # The savepoint includes every nested claim side effect: orbit rows,
            # request generations, fence/queue counters, cycle denial, and DONE
            # idempotency receipts.  A defensive later-member failure therefore
            # restores the exact pre-batch authority cut instead of publishing a
            # stale HELD receipt and then trying to compensate with release().
            self.store.db.execute("SAVEPOINT omd_begin_batch_acquire")

            for mode, paths, rid_suffix in specs:
                if mode in claims:
                    continue
                claimed = self.claim(
                    agent_id, paths, mode=mode, ttl=ttl, task_id=task_id,
                    priority=priority,
                    request_id=_rid(rid_suffix), bail_epoch=bail_epoch,
                    _observed_at=observed_at,
                )
                if claimed.get("state") != "HELD":  # preflight 아래서는 방어적 불변식 가드
                    self.store.db.execute(
                        "ROLLBACK TO SAVEPOINT omd_begin_batch_acquire"
                    )
                    self.store.db.execute(
                        "RELEASE SAVEPOINT omd_begin_batch_acquire"
                    )
                    self._emit("begin_blocked", task_id, reason="claim", mode=mode,
                               state=claimed.get("state"))
                    return {"ok": False, "stage": "claim", "task_id": task_id,
                            "mode": mode, "orbit_id": claimed.get("orbit_id"),
                            "state": claimed.get("state"),
                            "conflicts": claimed.get("conflicts", []),
                            "rollback": "transaction"}
                claims[mode] = claimed
            self.store.db.execute("RELEASE SAVEPOINT omd_begin_batch_acquire")
        # 4) promote → READY (PENDING/BLOCKED 만; 이미 진행중이면 skip → 멱등 재시작).
        t = self.store.get_task(task_id)
        if t["state"] in ("PENDING", "BLOCKED"):
            with self._cs():
                self.store.set_task(task_id, state=fsm.advance("task", t["state"], "ready"))
        # 5) start (worktree 발사; IN_ORBIT 재시도는 start 가 자연 dedup).
        st = self.start(task_id, agent_id, request_id=_rid("start"), bail_epoch=bail_epoch)
        if st.get("ok") is False:
            return {**st, "ok": False, "stage": "start"}
        final = (self.store.get_task(task_id) or {}).get("state")
        fences = {mode: claimed.get("fence") for mode, claimed in claims.items()}
        task_fence = max((f for f in fences.values() if f is not None), default=None)
        orbit_descriptors = []
        for mode, claimed in claims.items():
            row = self.store.get_orbit(claimed["orbit_id"])
            orbit_descriptors.append({
                "orbit_id": claimed["orbit_id"],
                "mode": mode,
                "paths": json.loads(row["pathspec"]) if row else [],
                "state": row["state"] if row else claimed.get("state"),
                "fence": row["fence"] if row else claimed.get("fence"),
                "expires_at": row["expires_at"] if row else None,
            })
        primary = next(
            (o for o in orbit_descriptors if o["fence"] == task_fence),
            orbit_descriptors[0] if orbit_descriptors else None,
        )
        agent = self.store.get_agent(agent_id)
        return {"ok": final == "IN_ORBIT", "stage": "started", "task_id": task_id,
                "state": st.get("state"), "worktree": st.get("worktree"),
                "branch": st.get("branch"), "fence": task_fence, "fences": fences,
                "orbit_id": primary["orbit_id"] if primary else None,
                "orbits": orbit_descriptors,
                "bail_epoch": agent["bail_epoch"] if agent else None,
                "liveness_ttl": liveness_ttl}

    def _connect_phase_a(self, task_id, agent_id, fence, bail_epoch=None, *,
                         request_id=None, request_agent=None,
                         request_arg_hash=None):
        """Phase A(임계구역): fence 재검증(P0-4) + merge_token 획득 + intent 영속 + pin + →CONNECTING."""
        with self._cs():
            self._sweep_inline()
            # §D6: 회수/탈출된(또는 stale bail_epoch) 좀비의 connect는 부활 차단으로 거부.
            dead = self._check_alive(agent_id, bail_epoch)
            if dead:
                self._emit("connect_rejected", task_id, reason=dead["reason"])
                return dead
            t = self.store.get_task(task_id)
            if t is None:
                return {"ok": False, "reason": "no such task"}
            if t["state"] == "MERGED":
                return {"ok": True, "noop": True, "task_id": task_id, "state": "MERGED",
                        "merge_sha": t["merge_sha"]}
            if t["state"] == "CONNECTING":
                # Recovery owns the persisted attempt.  A fresh caller must
                # never replace its token/generation or downgrade repo-bound
                # proof into a DB-only attempt.
                return {
                    "ok": False,
                    "reason": "connect_attempt_unresolved",
                    "task_id": task_id,
                    "state": "CONNECTING",
                    "recovery_required": True,
                }
            writes = [o for o in self.store.orbits_for_task(task_id)
                      if o["mode"] in WRITE_MODES]
            if not writes:
                return {"ok": False, "reason": "no write orbit for task"}
            # P0-4: 모든 write-orbit이 HELD여야(만료/해제면 stale). + 호출자가 (agent,fence)를
            # 줬으면 owner∧fence==task_fence(max individual fences) — ABA를 동일성으로 잡는다.
            stale = [o["orbit_id"] for o in writes if o["state"] != "HELD"]
            if not stale and (agent_id is not None or fence is not None):
                for o in writes:
                    if agent_id is not None and o["agent_id"] != agent_id:
                        stale.append(o["orbit_id"])
                task_fence = max(
                    (o["fence"] for o in writes if o["fence"] is not None), default=None
                )
                if fence is not None and fence != task_fence:
                    stale.extend(o["orbit_id"] for o in writes if o["orbit_id"] not in stale)
            if stale:
                self._emit("connect_rejected", task_id, reason="stale_fence", stale=stale)
                return {"ok": False, "fenced_out": True,
                        "reason": "stale fence: lease expired/released during work",
                        "stale": stale}
            # P0-11/§D10 — write-set 파일시스템 강제("최대 구멍"). 브랜치가 claimed write-set
            # **밖**의 파일을 건드렸으면 거부(merge 안 함, 토큰 안 잡음, 상태 불변). 이것으로
            # SINGULON 토대 (c)가 성립: 선언상 서로소 write-set이 *실제* write-set이 된다.
            write_globs = self._claimed_write_globs(task_id, writes)
            try:
                branch_tip, integration_base, offending = \
                    self._snapshot_connect_candidate(
                        task_id, t["branch"], write_globs
                    )
            except GitError as exc:
                self._emit(
                    "connect_rejected", task_id,
                    reason="writeset_audit_unavailable", error=str(exc),
                )
                return {
                    "ok": False, "reason": "writeset_audit_unavailable",
                    "retryable": True, "error": str(exc), "task_id": task_id,
                }
            if offending:
                self._emit("connect_rejected", task_id, reason="writeset_violation",
                           offending=offending)
                return {"ok": False, "reason": "writeset_violation", "offending": offending,
                        "claimed": write_globs, "task_id": task_id}
            # §D12 read-set 코히런스 — 유령 읽기 차단. consumer 가 자기 read-set 을 동기화한
            # gen(read_synced_gen) *이후* 통합에 들어온 응결 중 자기 선언 reads 와 겹치는 게
            # 있으면 = 옛 base 위에 *조용히* 빌드(머지는 성공하되 로직이 틀림) → connect 거부.
            # 토큰 잡기 **전**에 검사(거부 시 토큰 쥐었다 반납하는 낭비/경합 회피).
            # 물방울 계약: rebase/재독 → read_refresh() 로 청산 후 재시도.
            ghost = self._ghost_reads(t)
            stale_orbits = [o["orbit_id"] for o in
                            self.store.stale_read_orbits_for_task(task_id)]
            if ghost or stale_orbits:
                self._emit("connect_rejected", task_id, reason="read_stale",
                           ghost_globs=ghost, stale_reads=stale_orbits)
                return {"ok": False, "reason": "read_stale", "task_id": task_id,
                        "ghost_globs": ghost, "stale_reads": stale_orbits,
                        "hint": "rebase onto integration tip, then read_refresh() your "
                                "read orbit(s) before retrying connect"}
            # merge_token(repo-wide Semaphore max=1) — 가용 아니면 retry(다른 connect 응결중).
            owner = agent_id or t["agent_id"] or f"connect:{task_id}"
            attempt_id = f"connect-{uuid.uuid4().hex}"
            owner_generation = int(t.get("connect_owner_generation") or 0) + 1
            token_id = self._acquire_merge_token_locked(
                owner, operation_id=attempt_id, owner_instance=self.instance_id,
                owner_generation=owner_generation,
            )
            if token_id is None:
                return {"ok": False, "retry": True, "reason": "merge in progress (token held)"}
            # task → CONNECTING (+ intent 영속: connect_fence/branch_tip_sha/connect_intent_at)
            s = t["state"]
            if s == "IN_ORBIT":
                s = fsm.advance("task", s, "finish")
            if s == "DONE":
                s = fsm.advance("task", s, "connect")  # DONE→CONNECTING
            else:
                # 비정상 상태에서 connect → 토큰 반납하고 거부.
                self._release_merge_token_locked(token_id)
                return {"ok": False, "reason": f"task not connectable: {s}"}
            cap_fence = max((o["fence"] for o in writes if o["fence"] is not None), default=None)
            self.store.set_task(task_id, state=s, connect_fence=cap_fence,
                                connect_intent_at=time.time(), branch_tip_sha=branch_tip,
                                integration_base_sha=integration_base,
                                connect_attempt_id=attempt_id,
                                connect_owner_instance=self.instance_id,
                                connect_owner_generation=owner_generation,
                                connect_token_id=token_id,
                                connect_request_id=request_id,
                                connect_arg_hash=request_arg_hash,
                                connect_repo_bound=1 if self.git else 0)
            if request_id is not None and not self.store.bind_idem_operation_exact(
                    request_id, request_agent, "connect", request_arg_hash,
                    operation_id=attempt_id, owner_instance=self.instance_id,
                    owner_generation=owner_generation):
                raise RuntimeError(
                    f"idempotency operation bind lost for connect:{request_id}"
                )
            # 궤도 pin(merging=1) — sweep/reclaim이 응결중 궤도를 건드리지 않게(§E, 유계).
            check_budget = self.integration_check_timeout if self.integration_check else 0.0
            deadline = (time.time() + max(self.merge_timeout, 5.0)
                        + check_budget + MERGE_PIN_GRACE_S)
            for o in writes:
                self.store.set_orbit(o["orbit_id"], merging=1, merge_deadline=deadline)
            self._emit("connect_started", task_id, token_id=token_id, fence=cap_fence)
            intent = {"task_id": task_id, "branch": t["branch"], "worktree": t["worktree"],
                      "writes": [o["orbit_id"] for o in writes],
                      "branch_tip_sha": branch_tip,
                      "integration_base_sha": integration_base,
                      "attempt_id": attempt_id,
                      "owner_instance": self.instance_id,
                      "owner_generation": owner_generation,
                      "token_id": token_id,
                      "token_agent": owner,
                      "request_id": request_id,
                      "request_agent": request_agent,
                      "request_arg_hash": request_arg_hash,
                      "repo_bound": bool(self.git)}
            return {"ok": True, "token_id": token_id, "intent": intent}

    def _diagnose_conflict(self, branch, conflict_files):
        """P3 증분13(O1): 통합측에서 충돌 경로를 건드린 원인 커밋들을 bypass_audit 분류
        (direct_commit/foreign_merge/forged_*/omd_connect)와 함께 지목 — '충돌의 범인'을
        기계가 말한다. fail-soft: 진단 실패는 빈 목록(복구 응답 자체를 막지 않음)."""
        if not self.git or not branch or not conflict_files:
            return []
        try:
            wt = self._ensure_integration_wt()
            mb = self.git.merge_base(branch, self.integration_branch, cwd=wt)
            rows = self.git.commits_touching(f"{mb}..{self.integration_branch}",
                                             conflict_files, cwd=wt)
            out = []
            for r in rows:
                c = bypass_audit.Commit(sha=r["sha"], parents=r["parents"],
                                        trailers=r["trailers"], author=r["author"],
                                        subject=r["subject"])
                out.append({"sha": r["sha"], "kind": bypass_audit.classify(c).value,
                            "author": r["author"], "subject": r["subject"]})
            return out
        except GitError:
            return []

    def _connect_phase_b(self, intent, push=None):
        """Phase B(**락 밖** — live tx 없음): 전용 통합 worktree에서 merge --no-ff(타임아웃, §E).
        절대 _cs()/store.tx()를 잡지 않는다 — 다른 코디네이터 변이가 이 동안 interleave 가능.
        push: per-call remote override(complete_task 등). None 이면 self.auto_push 상속."""
        if not self.git:
            return None, None   # repo 미바인딩 — DB-only 응결(테스트/드라이런)
        task_id, branch = intent["task_id"], intent["branch"]
        try:
            candidate_ref = intent.get("branch_tip_sha")
            if not isinstance(candidate_ref, str) or not candidate_ref:
                raise GitError(
                    f"connect intent for {task_id} has no audited branch tip"
                )
            expected_base = intent.get("integration_base_sha")
            current_base = self.git.branch_tip(
                self.integration_branch, strict=True
            )
            if not expected_base or current_base != expected_base:
                raise GitIntegrationPreconditionError(
                    f"integration branch drifted before connect {task_id}: "
                    f"expected {expected_base}, found {current_base}"
                )
            wt = self._ensure_integration_wt()
            msg = (f"CLOUD CONNECT {task_id}\n\n{self._trailer(task_id)}\n"
                   f"{self._trailer(task_id, intent['attempt_id'])}")
            sha = self.git.merge_into(wt, self.integration_branch, candidate_ref, msg,
                                      timeout=self.merge_timeout,
                                      check_argv=self.integration_check,
                                      check_timeout=self.integration_check_timeout,
                                      check_output_limit=self.integration_check_output_limit)
            attempt_trailer = self._trailer(task_id, intent["attempt_id"])
            if self.git.branch_in_integration(
                    wt, self.integration_branch, attempt_trailer, strict=True) != sha:
                # ``git merge --no-ff`` still returns "already up to date" when
                # the branch is an ancestor.  Persist an empty evidence commit
                # so this exact generation remains recoverable after a crash.
                sha = self.git.commit_empty_integration(wt, msg)
            self.git.assert_ancestor(candidate_ref, sha, cwd=wt)
            # 연결=merge 직후 remote sync(operator "커밋하면 바로 sync"의 OMD 내장판).
            # opt-in(push override > self.auto_push). fail-soft: push 실패해도 merge 는 로컬
            # 반영됨이라 connect 는 성공 유지(다음 connect/수동 push 가 따라잡음). 강제 push 안 함.
            remote = push if push is not None else self.auto_push
            if remote:
                try:
                    self.git.push_integration(wt, self.integration_branch, remote,
                                              timeout=self.merge_timeout)
                    self._emit("connect_pushed", task_id, remote=remote, merge_sha=sha)
                except (GitError, GitTimeout) as pe:
                    self._emit("connect_push_failed", task_id, remote=remote, error=str(pe))
            return sha, None
        except (GitError, GitTimeout) as e:
            return None, e

    def _connect_phase_c(self, task_id, token_id, intent, merge_sha, err):
        """Phase C(임계구역): Phase B 결과를 원자 반영. 성공이면 merge_sha 먼저 기록(P0-6) 후
        해제; 실패면 CONNECTING→DONE rollback(재시도가능). 어느 쪽이든 merge_token 반납 + unpin."""
        repo_bound = bool(intent.get("repo_bound"))
        proven_sha = None
        proof_error = None
        rollback_verified = not repo_bound
        if repo_bound and self.git is None:
            proof_error = "repo_unavailable"
        elif repo_bound:
            try:
                wt = self._ensure_integration_wt()
                proven_sha = self.git.branch_in_integration(
                    wt, self.integration_branch,
                    self._trailer(task_id, intent.get("attempt_id")),
                    strict=True,
                )
                if proven_sha:
                    candidate_sha = intent.get("branch_tip_sha")
                    if not candidate_sha:
                        raise GitError("missing audited branch tip")
                    self.git.assert_ancestor(candidate_sha, proven_sha, cwd=wt)
                    # Exact Git truth dominates a timeout/error returned after
                    # ref update.  Forward-complete this generation once.
                    merge_sha, err = proven_sha, None
                elif isinstance(err, GitIntegrationPreconditionError):
                    # The exact base-drift guard runs before merge_into(), and
                    # gitio precondition failures either precede mutation or
                    # return only after their own verified abort.  No rollback
                    # to the now-stale Phase-A base is required here.
                    rollback_verified = True
                elif err is not None and not isinstance(err, GitRollbackError):
                    integration_base = intent.get("integration_base_sha")
                    if integration_base is None:
                        proof_error = "missing_integration_base"
                    else:
                        self.git.abort_merge_verified(wt, integration_base)
                        rollback_verified = True
            except GitError as exc:
                proof_error = f"git_reconciliation_unavailable:{exc}"
        with self._cs():
            task = self.store.get_task(task_id)
            token = self.store.get_orbit(token_id)
            owns = (
                task is not None and task["state"] == "CONNECTING"
                and task.get("connect_attempt_id") == intent.get("attempt_id")
                and task.get("connect_owner_instance") == intent.get("owner_instance")
                and task.get("connect_owner_generation") == intent.get("owner_generation")
                and task.get("connect_token_id") == token_id
                and token is not None and token["state"] == "HELD"
                and token.get("kind") == "merge_token"
                and token.get("resource_key") == self.merge_resource
                and token.get("agent_id") == intent.get("token_agent")
                and token.get("operation_id") == intent.get("attempt_id")
                and token.get("owner_instance") == intent.get("owner_instance")
                and token.get("owner_generation") == intent.get("owner_generation")
            )
            if owns:
                for orbit_id in intent.get("writes", ()):
                    orbit = self.store.get_orbit(orbit_id)
                    if (orbit is None or orbit["task_id"] != task_id
                            or orbit["state"] != "HELD" or not orbit["merging"]):
                        owns = False
                        break
            request_id = intent.get("request_id")
            if owns and request_id is not None:
                idem = self.store.get_idem(request_id)
                owns = (
                    idem is not None and idem["status"] == "INFLIGHT"
                    and idem["agent_id"] == intent.get("request_agent")
                    and idem["verb"] == "connect"
                    and idem["arg_hash"] == intent.get("request_arg_hash")
                    and idem.get("operation_id") == intent.get("attempt_id")
                    and idem.get("owner_instance") == intent.get("owner_instance")
                    and idem.get("owner_generation") == intent.get("owner_generation")
                )
            if not owns:
                self._emit(
                    "connect_fenced", task_id, reason="attempt_ownership_lost",
                    attempt_id=intent.get("attempt_id"), token_id=token_id,
                )
                return {
                    "ok": False, "task_id": task_id, "fenced_out": True,
                    "reason": "connect attempt ownership lost",
                    "_preserve_idempotency": True,
                }
            if proof_error is not None:
                for orbit_id in intent.get("writes", ()):
                    self.store.set_orbit(orbit_id, merge_deadline=None)
                self._emit(
                    "connect_fail_stopped", task_id,
                    reason="git_reconciliation_unavailable",
                    error=proof_error, attempt_id=intent.get("attempt_id"),
                )
                return {
                    "ok": False, "task_id": task_id, "state": "CONNECTING",
                    "retryable": False,
                    "reason": "git_reconciliation_unavailable",
                    "error": proof_error,
                    "_preserve_idempotency": True,
                }
            if (err is None and intent.get("repo_bound")
                    and (not merge_sha or proven_sha != merge_sha)):
                for orbit_id in intent.get("writes", ()):
                    self.store.set_orbit(orbit_id, merge_deadline=None)
                self._emit(
                    "connect_fail_stopped", task_id,
                    reason="missing_exact_git_proof", merge_sha=merge_sha,
                    proven_sha=proven_sha, attempt_id=intent.get("attempt_id"),
                )
                return {
                    "ok": False, "task_id": task_id, "state": "CONNECTING",
                    "retryable": False, "reason": "missing_exact_git_proof",
                    "merge_sha": merge_sha, "proven_sha": proven_sha,
                    "_preserve_idempotency": True,
                }
            if err is not None:
                # checker가 tracked tree를 바꿔 merge --abort 뒤 원상복구를 증명하지 못한 경우는
                # 자동 DONE rollback 금지. CONNECTING+token+pin을 보존해 증거를 조사하게 한다.
                if isinstance(err, GitRollbackError):
                    for o in self.store.pinned_orbits_for_task(task_id):
                        self.store.set_orbit(o["orbit_id"], merge_deadline=None)
                    self._emit("connect_fail_stopped", task_id,
                               reason="integration_rollback_failed",
                               problems=list(err.problems))
                    return {
                        "ok": False,
                        "task_id": task_id,
                        "state": "CONNECTING",
                        "retryable": False,
                        "reason": "integration_rollback_failed",
                        "error": str(err),
                        "problems": list(err.problems),
                        "_preserve_idempotency": True,
                    }
                if repo_bound and not rollback_verified:
                    return {
                        "ok": False, "task_id": task_id, "state": "CONNECTING",
                        "retryable": False, "reason": "rollback_unproven",
                        "_preserve_idempotency": True,
                    }
                # Phase B 실패 → rollback(재시도가능). 궤도 unpin + 토큰 반납.
                for o in self.store.pinned_orbits_for_task(task_id):
                    self.store.set_orbit(o["orbit_id"], merging=0, merge_deadline=None)
                self.store.set_task(task_id, state=fsm.advance("task", "CONNECTING", "rollback"),
                                    connect_intent_at=None, integration_base_sha=None,
                                    connect_attempt_id=None,
                                    connect_owner_instance=None,
                                    connect_token_id=None,
                                    connect_request_id=None, connect_arg_hash=None,
                                    connect_repo_bound=0)
                self._release_merge_token_locked(token_id)
                self._reconcile_admission()
                if isinstance(err, GitIntegrationCheckTimeout):
                    reason = "integration_check_timeout"
                elif isinstance(err, GitIntegrationMutation):
                    reason = "integration_check_mutation"
                elif isinstance(err, GitIntegrationCheckError):
                    reason = "integration_check_failed"
                elif isinstance(err, GitIntegrationPreconditionError):
                    reason = "integration_precondition_failed"
                else:
                    reason = "merge timeout" if isinstance(err, GitTimeout) else "merge conflict"
                out = {"ok": False, "task_id": task_id, "state": "DONE", "retryable": True}
                if isinstance(err, GitIntegrationCheckError):
                    out.update({
                        "check_returncode": err.returncode,
                        "check_stdout": err.stdout,
                        "check_stderr": err.stderr,
                    })
                    if isinstance(err, GitIntegrationMutation):
                        out["mutations"] = list(err.mutations)
                # P3 증분13(O1): 충돌이면 진단 동봉 — 충돌 경로 + 통합측 원인 커밋
                # (bypass_audit 분류: 우회 여부·작성자까지 지목). Zuul reporter 교훈:
                # 실패는 유지하되 '왜/무엇 때문에'의 보고가 복구 UX 의 본체.
                if isinstance(err, GitMergeConflict):
                    out["conflict_files"] = err.conflicts
                    out["culprits"] = self._diagnose_conflict(
                                                              intent.get("branch_tip_sha")
                                                              or intent.get("branch"),
                                                              err.conflicts)
                # P2 shared 레인: shared 궤도를 쥔 task 의 merge conflict 는 불변식 버그가
                # 아니라 **정상사건**(같은 hunk 동시편집) — 경보 대신 rebase 복구 힌트(P3).
                # 배타(write-only) task 의 conflict 는 기존 '구조적 불가=경보' 의미론 유지.
                shared = any(o["mode"] == "shared"
                             for o in self.store.orbits_for_task(task_id))
                if reason == "merge conflict" and shared:
                    reason = "shared_conflict"
                    out["hint"] = ("shared-lane 3-way conflict (정상사건) — worktree 브랜치를 "
                                   "통합 tip 위로 rebase 해 충돌을 해소하고 connect 를 재시도")
                    self._emit("connect_shared_conflict", task_id, error=str(err))
                elif reason == "merge conflict":
                    out["hint"] = ("배타 write-set 충돌 = out-of-band 우회가 통합을 가른 것"
                                   "(culprits 로 원인 커밋 확인). worktree 브랜치를 통합 tip "
                                   "위로 rebase 해 충돌을 해소(해소는 rerere 가 기록·재사용)하고 "
                                   "connect 를 재시도")
                    self._emit("connect_aborted", task_id, reason=str(err),
                               conflicts=out.get("conflict_files", []))
                elif reason == "merge timeout":
                    self._emit("connect_aborted", task_id, reason=str(err))
                else:
                    self._emit("connect_gate_rejected", task_id, reason=reason,
                               error=str(err))
                if reason.startswith("integration_"):
                    out["reason"] = reason
                    out["error"] = str(err)
                else:
                    out["reason"] = f"{reason}: {err}"
                return out
            # 성공: P0-6 순서 — merge_sha 먼저 기록 → MERGED → write-orbit 해제(+unpin).
            self.store.set_task(task_id, merge_sha=merge_sha, merged_at=time.time())
            self.store.set_task(task_id, state=fsm.advance("task", "CONNECTING", "merged"),
                                connect_intent_at=None, integration_base_sha=None)
            # §D12: 통합 generation 전진 + 겹치는 live read-궤도 stale 표시(write-orbit 해제
            # **전** — pathspec 이 아직 잡힐 때 글로브를 모은다).
            new_gen = self.store.bump_integration_gen()
            merged_globs = self._merged_write_globs(task_id)
            # merge_log: 이 gen 에 통합으로 들어간 write-globs 기록 — consumer 가 release 한 뒤에도
            # connect 에서 자기 read-set 코히런스를 검사할 수 있게(궤도 생명과 분리, §D12).
            self.store.append_merge_log(new_gen, task_id, merged_globs)
            stale_reads = self._mark_stale_reads(task_id, new_gen, merged_globs)
            self._release_task_write_orbits(task_id)
            self._release_merge_token_locked(token_id)
            if self.git and intent.get("worktree"):
                self.git.remove_worktree(intent["worktree"])
            self.store.set_flag(task_id, "merged")
            self._emit("connect_merged", task_id, merge_sha=merge_sha,
                       gen=new_gen, stale_reads=len(stale_reads))
            self._reconcile_admission()
            return {"ok": True, "task_id": task_id, "state": "MERGED", "merge_sha": merge_sha,
                    "gen": new_gen, "stale_reads": stale_reads}

    def status(self):
        with self._cs():
            observed_at = self._sweep_inline()
            pending = []
            for row in self._ordered_pending(observed_at):
                rank = self._rank_observation(row, observed_at)
                effective = rank["effective_priority"]
                pending.append(
                    {
                        "orbit_id": row["orbit_id"],
                        "queue_seq": row["queue_seq"],
                        "base_priority": rank["base_priority"],
                        "effective_priority": effective,
                        "age_boost": (
                            None
                            if effective is None
                            else effective - rank["base_priority"]
                        ),
                        "enqueued_at": row["enqueued_at"],
                        "wait_deadline": row["wait_deadline"],
                        "policy_version": rank["policy_version"],
                    }
                )
            snapshot = self.store.snapshot()
            snapshot["admission_queue"] = {
                **self.store.pending_queue_stats(),
                "capacity": self.admission_queue_capacity,
                "observed_at": observed_at,
                "policy_version": self.admission_policy.version,
                "policy": self.admission_policy.envelope,
                "pending": pending,
            }
            return snapshot
