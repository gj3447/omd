# OMD 동시성·실패모드 정밀 설계 (CONCURRENCY)

> 군단장 코어(SINGULON)를 **모든 상황**에서 견디게 만드는 설계서.
> [`CONCEPT.md`](./CONCEPT.md)·[`SERVER_SPEC.md`](./SERVER_SPEC.md)가 *정상 경로*(은유·데이터모델·상태머신)를 정의한다면,
> 이 문서는 **물방울이 긴급 탈출하거나 죽거나 분단되거나 코디네이터가 크래시하는** 비정상 경로를 정의한다.

현 프로토타입은 19 tests green으로 **동작**하지만, 암묵적으로 "호출자가 하나뿐인 선의의 세계"를 가정한다.
동시 호출·긴급 탈출·크래시·분단이 들어오면 SINGULON 불변식(분열=0)이 깨지거나 영구 행(hang)이 난다.
아래 14개 차원은 그 가정을 전부 제거한 정밀 설계다.

**방법론 주석(자기검증).** 본 설계는 9개 실패모드 차원을 각각 *설계 → 적대적 검증 → 교차통합* 한 결과다.
9개 1차 설계가 **전부 FLAWED 판정**을 받았고(검증기가 잔여 레이스/신규 데드락/불변식 위반을 발견), 이 문서는 그 수정안을 반영한 2차본이다.
즉 "그럴듯하지만 틀린" 설계를 적대적으로 걸러낸 뒤의 것이다.

---

## 0. 통합 모델 — 기제 하나, 투영 넷 (one substrate, four projections)

경화(hardened) OMD에는 **단 하나의 프리미티브**만 있다:

> **소유되고(owned), 생존에 묶이고(liveness-bound), fence된 LEASE.**
> 모든 변이는 **하나의 직렬화된 임계구역**에서 일어나고, **진실의 원천은 둘**이다(병합은 git, 임대는 DB).
> 그리고 **모든 변이 동사에 fencing**이 걸린다.

### 0.1 SINGULON 정리의 일반화

기존 SINGULON(`SERVER_SPEC §3`)은 *"write-set이 서로소(입체)면 merge는 충돌하지 않는다"* 였다.
동시성/실패를 넣으면 이것만으로 불충분하다 — **죽은 물방울의 궤도가 영영 안 풀리면 입체(서로소)를 다시 만들 수 없고**,
**GC로 멈췄던 좀비의 낡은 fence가 merge를 몰고 가면** 선언이 서로소라도 분열이 난다. 그래서:

> **경화 SINGULON** = "(a) 모든 lease는 **유한 시간 내 회수**되고 ∧ (b) **fence가 현재값이 아니면** 어떤 lease도 구름을 변이시키지 못한다 ∧ (c) 선언된 write-set이 **파일시스템에서 실제로 강제**된다 ⇒ 응결 시 분열=0."

(c)는 완전성 비평이 찾은 가장 큰 구멍이다(§2 D10): 지금은 선언만 검사하고 *실제 쓰기 영역은 검사하지 않는다*.

### 0.2 네 프리미티브 = LEASE 한 행의 투영

LEASE 행: `{lease_id, kind, owner_agent, fence, epoch, lease_expires_at, hb_bound, state, ...}`

| 프리미티브 | kind | 부여 게이트 | 죽으면 |
|---|---|---|---|
| **궤도 orbit** | `orbit` (+`pathspec`,`mode`) | 입체(pairwise-disjoint) 검사 | TTL/heartbeat로 회수 → 경로 재개 |
| **플래그(소유형)** | `flag_ephemeral` | 소유+TTL | 자동 clear + 대기자 `PRODUCER_DEAD`로 기상 |
| **세마포어 permit** | `sem_permit` (+`resource_key`) | 가용 = `N − count(ACTIVE)` | permit 자동 만료 → 슬롯 복구 |
| **배리어 좌석** | `barrier_seat` | 세대(generation) 일치 | 배리어 BROKEN → 전원 기상 |
| **(병합 토큰)** | `merge_token` (capacity 1) | 통합 레포 Semaphore(max=1) | 회수 후 `git merge --abort` |

**중요한 예외 — LATCH(걸쇠).** `task=done`/`task=merged` 같은 **단조 사실**은 LEASE가 *아니다*.
소유자와 분리된 영속 사실이라 **producer가 죽어도 살아남아야** 한다(별도 `flags` 테이블, `ephemeral=0`, 회수 대상 아님).
이 분리가 핵심이다: latch를 lease로 합치면 *작업중 플래그가 고아가 되거나* 반대로 *완료된 진척이 소멸한다*.

### 0.3 네 기둥

1. **직렬화된 임계구역(D1)** — 모든 check-then-act가 한 번에 indivisible하게 커밋. 프로세스 내 asyncio **actor**(단일 소비자) + 그 아래 **`BEGIN IMMEDIATE`/WAL**(다중 프로세스 백스톱).
2. **fencing(D6)** — 신뢰 불가 실패감지기(Chandra–Toueg: completeness XOR accuracy)를 *드물게가 아니라 안전하게* 만든다. 모든 변이 동사가 `(agent_id, fence)`를 들고 오고, 현재 fence 소유자가 아니면 거부.
3. **두 진실의 원천(D8)** — **병합은 git이 진실**(통합 브랜치의 `--no-ff` 머지 커밋 + 고유 trailer만이 응결의 증거), **임대는 DB가 진실**. 재기동 시 둘을 조정.
4. **생존 묶음(D2)** — 자발적 `bail`과 비자발적 좀비 회수가 **단 하나의 회수 루틴**으로 수렴.

---

## 1. 사용자 핵심 시나리오 정면 답변 — "긴급 탈출인데 플래그/세마포어가 계속 세워져 있으면?"

이 질문이 설계 전체의 진앙이다. 정확히 답한다.

### 1.1 두 갈래가 한 루틴으로 — `reclaim_agent`

물방울이 사라지는 방식은 둘뿐이다. **둘 다 같은 회수 루틴**을 탄다(코드 경로가 하나여야 둘 사이 누락/이중해제가 없다):

- **자발적(voluntary)** — 물방울이 빠져나가기 직전 `bail(agent_id)` 호출. (≈ "나 탈출함, 내 거 다 거둬가")
- **비자발적(involuntary)** — `kill -9` 등으로 `bail`조차 못 부름 → heartbeat 만료 → sweeper가 **같은 루틴** 호출.

```python
def reclaim_agent(agent_id, *, voluntary):     # THE ONE ROUTINE — D1 임계구역 안, 한 트랜잭션
    ag = get_agent(agent_id)
    if ag is None: return {"reclaimed": agent_id, "noop": True}   # 멱등: 이미 회수됐으면 no-op
    # 1) 먼저 LATCH: 진행중인 이 agent의 renew/claim을 즉시 fenced-out 시킴
    set_agent_state(agent_id, "ZOMBIE" if not voluntary else "BAILING")
    bump_bail_epoch(agent_id)                  # 부활 방지(GC-pause 좀비가 못 살아남음)
    trig = "bail" if voluntary else "reclaim"
    # 2) 이 agent가 쥔 모든 HELD/PENDING lease를 한 쿼리로 — kind별 정리
    for L in leases_owned_by(agent_id, states=("HELD","PENDING")):
        set_lease(L.lease_id, state=advance("lease", L.state, trig), released_at=now())
        if   L.kind == "flag_ephemeral": delete_or_break_flag(L.resource_key)   # ← "still working" 플래그가 여기서 사라짐
        elif L.kind == "sem_permit":     pass   # 상태변경만으로 capacity = N - count(ACTIVE) 가 자동 복구
        elif L.kind == "barrier_seat":   withdraw_seat(L.resource_key)          # 배리어 break/shrink 트리거
        # kind == "orbit": 상태변경으로 경로 해제
    # 3) 진행중 작업 requeue  (CLAIMED/IN_ORBIT/CONNECTING 전부 — §3.H 참조)
    for t in tasks_for_agent(agent_id):
        if t.state in ("CLAIMED","IN_ORBIT","CONNECTING"):
            s = advance("task", advance("task", t.state, "abort"), "requeue")   # → PENDING
            set_task(t.task_id, state=s, agent_id=None)
            if git and t.worktree:
                git.remove_worktree(t.worktree)      # 멱등(에러 삼킴)
                git.delete_branch(f"omd/{t.task_id}")  # ★ 필수: 안 지우면 다음 start()가 막힌다(§3.F)
    set_agent_state(agent_id, "RETIRED")
    _promote_pending()                          # 큐 재평가는 끝에 한 번만
    return {"reclaimed": agent_id, "voluntary": voluntary, ...}

def bail(agent_id):                # 자발적 긴급 탈출 = 비자발적에서 감지기만 뺀 것
    return reclaim_agent(agent_id, voluntary=True)
```

`bail`이 **멱등**이므로: 물방울이 `bail` 부르다 그 중간에 죽어도, sweeper가 *같은 루틴*을 다시 돌려 나머지를 마저 정리한다. 이중 해제·누락 없음.

### 1.2 "작업중 플래그가 영구 잔존" 문제의 정확한 해소

사용자가 짚은 시나리오: *물방울이 `in_progress`/`build_running` 같은 플래그를 세우고 다른 물방울들이 그걸 기다리는데, 세운 놈이 죽으면 → 플래그가 영영 안 풀려 → 대기자 전원 영구 데드락.*

해소 = **플래그를 두 종류로 분리**(D3):

| 종류 | 예 | 소유/TTL | 죽으면 | 대기자 |
|---|---|---|---|---|
| **EPHEMERAL(소유 신호)** | `build_running`, `in_progress` | 소유 agent + lease TTL + heartbeat | **자동 clear/BROKEN** | `PRODUCER_DEAD`로 **기상**(행 아님) |
| **LATCH(단조 걸쇠)** | `task=done`, `task=merged` | 소유 분리, TTL 없음 | **살아남음**(사실은 참) | 정상 만족 |

핵심: 작업중 플래그는 **EPHEMERAL = lease**다. 따라서 §1.1 회수 루틴이 자동으로 지운다 → 절대 고아가 안 된다.
그리고 대기자는 **타임아웃 필수 + producer 사망 시 명시적 에러 기상**:

```python
# wait는 서버에서 블로킹하지 않는다(단일 스레드를 막으면 구름 전체가 직렬화됨). register → poll 패턴.
def flag_wait_register(agent, key, want, timeout):   # timeout=None 거부
    f = SELECT flags WHERE key=key
    if satisfied(f, want):  return {state:'SATISFIED'}
    if broken(f):           return {state:'BROKEN', reason:'producer_dead'}   # ← 죽은 producer
    return register_waiter(deadline=now()+timeout, observed_epoch=f.epoch)

def flag_wait_poll(wid):   # 클라가 만족/BROKEN/TIMEOUT 될 때까지 재호출(저렴·멱등)
    # 재검사는 value가 아니라 EPOCH로 — ABA/유령기상 안전
    ...
```

> **세마포어도 동일 원리**(D4): permit은 *카운터 감소가 아니라 lease*다. 죽은 보유자의 permit이 만료되면 `가용 = N − count(ACTIVE permit)`이 *구조적으로* 복구된다 — 누수 0. 정수 카운터를 쓰면 죽을 때마다 새서 결국 0이 되어 영구 정지하는 고전 버그를, "permit=lease"로 원천 차단.

> **배리어도 동일**(D5): 좌석은 lease, 참가자가 도착 전(또는 도착 후) 죽으면 배리어가 **BROKEN**되어 도착해 있던 전원이 에러로 기상한다(Java `BrokenBarrierException` / Python `Barrier.abort()` 시맨틱). 영구 hang 불가.

### 1.3 추방당한 좀비의 자기 탈출(involuntary 긴급 탈출)

물방울이 *오추방*(false-positive eviction: 느렸을 뿐 살아있었음) 됐다 살아나면, 자기가 유령임을 알아야 한다.
**서버가 생존을 판정하고, 물방울은 fence에 복종한다**(Kleppmann 규율). 모든 변이 호출이 `{fenced_out:true, your_epoch:k, current_epoch:k+1}`을 돌려주면 → 물방울은 **즉시 모든 쓰기/커밋 중단, connect 금지, 종료**. (§2 D6)

---

## 2. 실패모드 14차원

각 차원: **위험**(`BUG`=현재 코드가 안전하지 않음 / `GAP`=명세됐으나 미구현 / `HARDENING`) → **기제**.

### D1 — 코디네이터 원자성·임계구역  `[BUG]`

- **TOCTOU(check-then-act)** `core.py:93-108`: `_conflicts()`(읽기) 와 `add_orbit(HELD)`(쓰기) 사이에 트랜잭션이 없다. sqlite3가 execute마다 autocommit. 동시 claim 두 개가 둘 다 "충돌없음" 보고 후 겹치는 HELD 생성 → **분열**.
- **fence 중복** `store.py:55-62`: `SELECT value` → `INSERT`가 두 문장(중간 commit). 동시 두 호출이 같은 n 읽고 둘 다 n+1 기록 → **중복 fence** → fencing 무력화.

**기제 — actor + `BEGIN IMMEDIATE`/WAL (하이브리드 2계층):**
- **프로세스 내**: asyncio **command actor**(단일 소비자)가 코디네이터를 유일 writer로 만든다(FastMCP의 threadpool/async 분배와 무관). actor는 **git 같은 느린 작업을 lock 밖으로 빼는 yield 지점**을 준다.
- **프로세스 간**: 모든 트랜잭션이 WAL DB 위에서 `BEGIN IMMEDIATE`. RESERVED writer 잠금을 트랜잭션 *시작 시* 잡아(lazy DEFERRED의 SQLITE_BUSY-중도롤백 제거) writer를 직렬화. EXCLUSIVE 대신 IMMEDIATE — WAL 리더(status/snapshot)는 안 막음.
- **PRAGMA**(`Store.__init__`): `journal_mode=WAL; busy_timeout=5000; foreign_keys=ON; synchronous=NORMAL;` + `connect(isolation_level=None, check_same_thread=False)`(우리가 직접 BEGIN/COMMIT 발행). 메서드별 `self.db.commit()` 전부 제거 → `with Store.tx():` 한 곳에서 커밋.
- **fence 원자화**: `UPDATE meta SET value=CAST(value AS INTEGER)+1 ... RETURNING value;` 한 문장(SQLite≥3.35), `CREATE UNIQUE INDEX uq_orbits_fence ON orbits(fence) WHERE state IN ('PENDING','HELD','RESERVED')`로 코드 회귀 시에도 중복=IntegrityError.
- **임계구역 안에 함께**: `_promote_pending`·`_sweep_locked`는 **트리거한 동사와 같은 트랜잭션**에서 돈다("해제+재부여"가 원자적이어야 이중부여 없음). **git 작업은 반드시 lock 밖**(start의 worktree add, connect의 merge는 초 단위 — lock 잡고 있으면 서버 전체 정지). → `start`/`connect`는 **A(락)–B(락밖 git)–C(락)** 3-phase로 분할.

### D2 — 통합 lease 기반 + 긴급 탈출  `[BUG]`

- **`agent_ttl` 기본값 None** `core.py:24,133` → 좀비 회수가 **기본 비활성**. 사용자가 말한 "긴급 탈출 후 잔존"이 정확히 이 디폴트에서 영구화. → **기본 `heartbeat_ttl=90s`로 회수 항상 ON.**
- 기제: §1의 단일 lease 행 + `reclaim_agent` 단일 루틴 + `bail` 동사.
- **TTL 두 종류**: `heartbeat_ttl`(agent 전체 생존, 권장 90s, 30s마다 heartbeat) vs `lease_ttl`(개별 궤도, 권장 300s, **TTL/3=100s마다 renew** — etcd LeaseKeepAlive). `heartbeat(agent)` 한 번이 그 agent의 모든 hb_bound lease를 갱신(저렴한 UPDATE 하나)하여, 건강한 물방울이 renew 깜빡해 궤도 잃는 일 방지.

### D3 — 플래그/이벤트 크래시 시맨틱  `[BUG]`

- 현 플래그 `store.py:140-149`: owner/TTL/wait/generation 전무. set이 read-modify-write 보호 없는 lost-update.
- 기제: §1.2 — EPHEMERAL(=lease, 자동 clear, `PRODUCER_DEAD` 기상) vs LATCH(영속, 단조, fence보다 낮은 setter 무시). `epoch` 기반 재검사로 ABA/유령기상 안전. **wait는 timeout 필수.**
- **단조 LATCH**: `done`(rank 1) < `merged`(rank 2). 이미 terminal이면 같은 값 재발행은 멱등 no-op, 하향은 에러("un-finish 불가").

### D4 — 크래시 안전 세마포어(빌드 슬롯, max=N)  `[GAP]`

- 명세엔 있으나 미구현. 기제: permit=lease, `가용 = N − count(ACTIVE)`(저장 정수 아님). acquire는 D1 임계구역에서 — **초과배정 불가**(두 acquirer가 동시에 N-1을 보고 둘 다 N+1번째를 부여하는 레이스 제거).

```python
def acquire(agent, sem_id, *, ttl, no_wait=False, priority=0):   # D1 임계구역
    sweep_semaphore(sem_id)
    if (p := active_permit_for(agent, sem_id)): return reuse(p)   # 멱등(MCP 재시도)
    avail = sem.max_permits - count_active_permits(sem_id)
    if avail >= 1 and not has_earlier_waiter(sem_id, priority, agent):   # no-overtaking(D7)
        return grant_permit(agent, sem_id, fence=next_fence(), ttl)
    return WAITING if not no_wait else FAIL
def release(agent, permit_id, fence):   # OWNER+FENCE 검사(이중해제·재부여후해제 방지)
    ...
```

### D5 — 크래시 안전 배리어(응결 랑데부)  `[GAP]`

- 명세엔 ARMED→TRIPPED→CONSUMED, 미구현. 기제: **세대(generation) 스탬프 + BROKEN 종단 상태**.
- 참가자 사망(도착 전/후 모두) 또는 배리어 타임아웃 → `_break` → 전원 BROKEN 기상.
- **멤버십을 agent 수가 아니라 task 집합에 묶음**(요구된 수정): 회수로 task가 requeue되면 N 재계산. `policy='break'`(전원 깸) | `policy='shrink'`(죽은 멤버 빼고 진행, 단 그 멤버 의존자 없을 때만).
- **응결 배리어는 `connect_one(task, expected_fence)` 프리미티브로 트립** — 공개 `connect()`를 부르면 안 됨(그 안의 `self.sweep()`가 방금 검증한 궤도를 재진입으로 만료시킴, 검증기 적발).

```
barrier FSM: ARMED → TRIPPING → TRIPPED → CONSUMED  ⊕  (any) → BROKEN
_barrier_eval (도착 + sweep 둘 다 호출):
  live/dead 분류(도착했는데 owner stale=죽음, arrive_fence가 이동=stale 도착)
  deadline 지나고 미도착 있으면 → break('timeout')
  dead 있으면 → break 또는 shrink
  남은 expected 전원 arrived → fill(ARMED→TRIPPING) → _trip_commit(순서대로 응결, 각 task fence 재검증)
```

### D6 — 신뢰불가 실패감지기·전 동사 fencing  `[BUG]`

- 지금은 `connect()`만 fencing. `finish/commit/release/flag_set`는 owner/fence 검사 **전무**. `release(orbit_id)` `core.py:118-125`는 **아무나 남의 궤도 해제** 가능. 오추방됐다 살아난 좀비가 남의 작업을 finish/release/flag → 분열.
- **connect fence-blind** `core.py:222`: `state != "HELD"`만 보고 fence 비교 없음. 동시성하에서 connect 윈도우 동안 만료-후-재부여된 lease를 fence 동일성으로 못 잡음(ABA).

**기제 — 전 변이 동사 fence 가드 표(D1 임계구역 안):**

| 동사 | 키 | 검사(전부 통과해야) | 거부 |
|---|---|---|---|
| renew | (orbit, agent, f) | HELD ∧ owner==agent ∧ fence==f | FENCED_OUT |
| release | (orbit, agent, f) | HELD ∧ owner==agent ∧ fence==f (이미 RELEASED면 멱등 OK) | NOT_OWNER/FENCED_OUT |
| finish | (task, agent, f) | task.owner==agent ∧ write-orbit HELD ∧ fence==f | FENCED_OUT |
| commit | (task, agent, f) | finish와 동일(worktree 커밋하려면 write lease 보유) | FENCED_OUT |
| connect | (task, agent, f) | finish + **모든** write-orbit HELD ∧ fence==f (merge-gate) | FENCED_OUT |
| flag_set | (key, agent, epoch) | owner면 caller==owner ∧ epoch CAS | NOT_OWNER/STALE |
| heartbeat | (agent) | fence 없음. 단 state==RETIRED면 `{fenced_out:true}` 회신 → 좀비가 다음 heartbeat에서 자기 죽음을 앎 | (advisory) |

read-only(status/flag_get/next_task)는 fence 불필요. **시계: 내부 만료 비교는 monotonic clock**(NTP 점프·suspend/resume 방어, Kleppmann), wall clock은 감사용만.
**오추방은 불가피**(Chandra–Toueg) → fencing이 그것을 *드물게가 아니라 안전하게* 만든다: 진 쪽은 다음 호출에서 자기가 졌음을 알고 자기 탈출(§1.3). 두 물방울이 동시에 작업해도 fencing+merge-gate가 낡은 쪽 쓰기/머지를 거부 → 분열0 보존.

### D7 — 데드락·라이브락·기아·우선순위 역전  `[BUG]`

- **broad 궤도 영구 기아** `core.py:103`: claim이 대기 중 PENDING을 무시하고 HELD와만 안 겹치면 *즉시 grant*. 작은 claim 스트림이 `src/**` 대기자를 **영원히** 굶김(writer-starvation).
- **task 의존 사이클 미검출** `core.py:81`: A after B, B after A → 둘 다 영구 BLOCKED.
- **다단계 claim 순서 미강제**(명세 §6.3): 별도 claim 여러 개 하면 데드락(사이클-deny로만 사후 포착).

**기제:**
- `_would_cycle` DFS 게이트를 `declare`/`depend`에 — 의존 DAG 비순환 보장(Kahn으로 전역 재검).
- **데드락 예방**: *권장 경로 = 전체 write-set을 한 번에 원자 claim*(`claim_set`, hold-and-wait 없음). 동적 증분 claim은 `CANON_ORDER = sort by (glob_prefix, glob)`로; 서버는 사이클 닫는 claim을 **DENY**(wedge 아님).
- **반기아 no-overtaking 입장규칙**(phase-fair, writer-preference): 새 요청 R은 *(1) HELD와 서로소 ∧ (2) R과 충돌하며 R보다 먼저 줄선 head 대기자 B가 없을 때만* 즉시 grant; 아니면 PENDING. read↔read는 배리어를 안 세움(reader 스트림은 reader끼리 공존, 단 대기 *writer*에겐 양보 → readers-writers 공정성).
  - **요구된 수정(검증 적발)**: RESERVED head는 *아무것도 안 쥐고* 줄을 막으므로, `_wait_for`를 RESERVED head로 들어가는/나가는 엣지까지 포함하도록 확장해야 사이클이 보임(안 그러면 새 데드락). 큐 대기는 `QUEUE_WAIT_TTL` 후 DENIED로 **유계**.

### D8 — 코디네이터 프로세스 크래시 복구 + git↔DB 이중쓰기  `[BUG]`

- **재기동 시 절대 만료**: `expires_at`이 wall-clock. 서버가 1시간 죽었으면 전 lease 즉시 만료(대량 회수). DB 시계는 *멈춰 있었으므로* 절대 deadline은 무의미(Kleppmann).
- **이중쓰기 무2PC** `core.py:232-249`: connect = git merge → DB transition → release → remove. merge와 DB 사이 크래시면 **브랜치는 병합됐는데 task는 CONNECTING에 영구 고착**(MERGED도 재시도가능도 아님).
- `_recover()`가 `__init__`에 **없음**(복구 0).

**기제 — 재기동 복구 sweep(`__init__` 마지막, 멱등):**
```python
def _recover():
  # (a) lease 재앵커: 다운타임만큼 크레딧 — 대량만료 방지
  for o in held_orbits():
     remaining = o.expires_at - last_shutdown_ts          # 죽을 때 남았던 양
     grace = max(GRACE_MIN, (o.ttl or 600)/3)             # 최소 1 keepalive 창(TTL/3)
     set_orbit(o, state=reanchor, expires_at = now + max(min(remaining, ttl), grace))
  grace_all_agents(now)        # 재기동을 전멸로 오판해 전원 추방하는 일 방지(accuracy 위반 차단)
  # (b) CONNECTING task를 git 진실과 조정 (git=병합의 진실)
  for t in tasks_by_state(['CONNECTING']) + tasks_with_connect_intent():
     if git.branch_in_integration(t.branch, integration_branch):   # trailer-probe (--is-ancestor 아님)
        set_task(t, MERGED, merge_sha=find_merge_commit(...)); release_write_orbits(t); remove_worktree(t)  # 전진수정
     else:
        set_task(t, state=advance('task','CONNECTING','rollback'))  # → DONE: connect 재호출 가능
  # (c) worktree ↔ tasks 조정: 고아 worktree(start가 set_task 전 크래시) 정리/재바인딩
```
- **split-phase connect**(D1과 공유): A(락: fence 재검증 + intent/connect_fence/branch_tip_sha 기록 + **merge_token 획득** + →CONNECTING + 커밋) → B(락밖: `git merge --no-ff` + **subprocess 타임아웃**) → C(락: merge_sha 먼저 기록 *후* orbit 해제 — `core.py:241-247` 순서 수정 + →MERGED + merge_token 반납 + promote).

### D9 — 멱등성/exactly-once(at-least-once MCP)  `[BUG]`

- MCP는 at-least-once: 서버는 성공했는데 응답이 유실되면 클라가 재시도. 현재 claim 재시도→**두 번째 HELD 궤도(클라가 모르는 누수 lease)**, start 재시도→`worktree add -b`가 기존 브랜치에서 GitError+중복행, connect 재시도→이중 merge/이중 release.

**기제:**
- 모든 변이 동사에 클라 제공 `request_id`; D1 임계구역 안의 `idempotency(request_id→cached response)` 테이블. `INFLIGHT/DONE` 상태, **성공 종단만 캐시**(DENIED/stale-fence는 캐시 안 함 — 세상이 바뀌면 재시도 가능해야).
- **의미적 멱등**(dedup 우회돼도 안전): claim은 `intent_key=hash(agent,sorted(pathspec),mode,task)`로 기존 HELD/PENDING 궤도 **같은 fence로 반환**; start는 기존 worktree 감지; connect는 already-merged 감지. release는 이미 RELEASED/EXPIRED면 멱등 OK(`core.py:120` 거부 수정).
- **fencing과 교차**(§3.C): 재시도 release가 *재부여된* lease를 풀면 안 됨 → fence/owner 가드. dedup 캐시는 **현재 소유자 아닌 자에게 살아있는 HELD lease를 절대 반환 금지**.

### D10 — write-set의 파일시스템 강제(SINGULON 토대 미검사)  `[BUG, 최대 구멍]`

- **궤도는 순수 advisory다.** `commit_all`이 `git add -A`로 *worktree 전체*를 커밋(`gitio.py:43-47`), worktree는 sparse가 아닌 전체 체크아웃. `src/a/**`를 claim한 물방울이 `src/b/foo.py`를 고쳐도 커밋되고 응결됨. **선언상 서로소인 두 물방울이 실제로는 겹치는 커밋** → 분열, 그런데 D1~D9 검사는 전부 통과. **9차원 fencing/lease 전체가 파일시스템이 강제하지 않는 *선언*을 지킨다.**
- 기제 둘(택1 또는 병행):
  1. **scoped worktree**: sparse-checkout(cone) 으로 claim한 glob만 보이게 — 물리적으로 밖을 못 씀.
  2. **pre-connect 감사**(저비용·우선): commit/connect 게이트에서 `git diff --name-only`를 궤도 glob과 대조(`disjoint.globs_overlap` 재사용), 궤도 밖 경로가 있으면 **거부**. D6의 connect 게이트가 자연스러운 강제 지점.

### D11 — 통합 브랜치 소유(전역 경합 자원)  `[BUG]`

- `merge()`가 `cwd=self.root`에서 *현재 HEAD*에 머지(`gitio.py:49-59`) — 사용자 작업 체크아웃을 변이. 통합 브랜치 row 없음, 전용 worktree 없음, 락 없음.
- **두 입체(서로소) task가 동시에 connect**하면 둘 다 fence 통과 후 **같은 `.git/index`/HEAD를 경합** → `index.lock` 실패 또는 한쪽의 `merge --abort`가 다른쪽 진행중 머지를 파괴 → 부분 트리가 MERGED로 기록 = **좀비 없는 분열**. "병렬 개발"이 응결 단계에서 직렬로 붕괴.
- 기제: **repo-wide `merge_token`(Semaphore max=1, 그 자체가 lease)** + **전용 통합 worktree**(사용자 HEAD 안 건드림) + merge는 항상 명시적 `checkout integration_branch` 후. 토큰은 crash-safe(`merge_started_mono` 저장, 복구가 RETIRED 소유 토큰의 dangling `MERGE_HEAD`를 abort).

### D12 — read-set 코히런스/유령 읽기  `[GAP]`

- `reads`는 저장되나 `_conflicts`/`next_task`/`connect` 어디서도 사용 안 됨. consumer가 `src/api/**`를 read claim하고 작업하는데 producer가 `src/api/new.py`(읽을 때 없던 유령)를 응결하면, consumer는 옛 base에서 분기했으므로 **조용히 낡은 뷰 위에 빌드**(자기 머지는 성공하지만 *로직*이 틀림). SINGULON은 write-disjointness만 보장.
- 기제: 통합 브랜치 generation 추적; 응결이 live read-궤도와 겹치는 경로를 추가/변경하면 그 consumer의 read-lease를 **stale 표시** → consumer는 자기 connect 전 rebase/재독 강제. stale 신호는 D3 플래그/이벤트로.

### D13 — git/FS 기반 장애  `[GAP]`

- worktree 디스크풀: 물방울 N개 = 전체 체크아웃 N개. `add_worktree`/`commit_all` 실패(`GitError`)를 `start`/`commit`이 안 잡음 → 반쯤 만든 브랜치가 재시도를 막음. 백그라운드 `git gc`/외부 `git worktree prune`가 live worktree를 경합/고아화. `remove_worktree`가 에러를 삼켜(`gitio.py:62-65`) 이를 가림.
- 기제: `GitError`를 **transient/disk/fatal로 분류**(`gitio.py:13`는 단일 타입); worktree 생성 전 quota preflight; 관리 레포 auto-gc 비활성; ENOSPC 별도 처리. D9의 재시도 분류와 연동.

### D14 — 코디네이터 singleton / HA 입장  `[GAP]`

- D1의 in-process actor 직렬화는 **프로세스당**이다. 운영자가 HA로 FastMCP 2개를 한 DB에 띄우면 actor 불변식이 조용히 무효(actor 둘=writer 둘), 통합 머지(락 밖)는 한 레포에 두 writer가 무조정.
- 기제: **DB 리더-lease**(둘째 코디네이터를 fence) 또는 기동 시 다른 live 코디네이터 heartbeat 감지하면 **거부**. "단일 인스턴스 전용"을 *명시적으로 강제*. (`:memory:` 디폴트도 금지 — 재기동마다 fence가 0으로 리셋되어 낡은 토큰과 충돌. 영속 DB 필수.)

---

## 3. 위험한 교차작용 (A–H) — 단일 차원으로 안 잡히는 함정

> 1차 설계가 전부 FLAWED였던 이유. 각 차원은 옳아도 *둘이 만나면* 깨진다.

- **A. D7 RESERVED 배리어 × D2 회수 — 감지 불가 데드락.** RESERVED head는 아무것도 안 쥐고 줄을 막는데 `_wait_for`는 PENDING→HELD 엣지만 만든다. RESERVED로 들어가는 엣지가 안 보여 사이클 미감지 → D2 TTL 백스톱으로만 풀림. **그런데 D2 회수가 기본 비활성(agent_ttl=None)** → D7의 안전밸브가 디폴트에 없음. **수정**: D2 회수를 D7의 hard precondition으로 선언(항상 ON) + `_wait_for`를 RESERVED 노드까지 확장.
- **B. D1 전역 락 × D8 긴 git merge — 코디네이터 정지(두 설계 모순).** D8은 connect를 "D1 임계구역 안"이라 하고 그 안에서 초 단위 merge를 한다. D1은 "git은 락 밖(phase B)"이라 한다. **해소**: connect는 단 하나의 정의된 **A–B–C phase 분할**(merge는 락 밖, merge_token으로 직렬화) — D5/D6/D8이 *전부 이 한 계약*을 참조.
- **C. D9 dedup 캐시 × D6 fencing — 캐시가 fenced-out 결과를 주거나, 유효해진 걸 얼림.** (1) 실패(stale-fence)를 DONE으로 캐시하면 세상이 바뀐 뒤에도 같은 request_id 재시도가 낡은 거부를 영구 재생. (2) 더 위험: 의미적 fallback(`orbit_by_intent`)이 *살아있는 fence를 회수된 물방울에게* 넘기면 D6이 막으려던 부활을 무장. **수정**: 성공 종단만 캐시 + dedup 재생 경로를 D6 owner/fence가 감쌈.
- **D. D5 broken-barrier × D8 재기동 — CONNECTING 배리어 크래시 미조정.** D8은 task를 *개별로* git와 조정하지만, TRIPPING 중 크래시한 배리어(일부 머지, 일부 미머지)는 배리어의 원자성 의도를 모른 채 task1=MERGED, task3=rollback으로 만들어 랑데부를 반쪽 적용 + BROKEN 신호 없음. **수정**: D8 복구가 배리어-bound CONNECTING 집합을 *단위*로 처리(반쪽 트립 → BROKEN + 생존자 rollback).
- **E. D2 "CONNECTING 동안 lease pin" × D7 기아 × D2 생존 — pin이 생존 보장을 정지시킴.** merge가 무한 git subprocess(타임아웃 없음 `gitio.py:27`)면, pin이 궤도를 영구 HELD로 잡아 D7 no-overtaking이 겹치는 PENDING을 *영구* 기아. **수정**: pin을 **유계**로(merge deadline → abort → expire → promote). 이 bound가 D7/D5가 의존하는 공유 불변식.
- **F. fence-qualified worktree/branch × 요구 — 브랜치명 재사용이 재기동을 데드락(3차원 복합).** D2/D5/D8 전부 task를 requeue하고 `remove_worktree`를 부르지만, 그게 브랜치 `omd/<task>`를 *안 지운다*(`gitio.py:61-65`). `start()`의 `worktree add -b omd/<task>`(`gitio.py:38-41`)는 기존 브랜치면 **실패**. **세 차원이 전부 requeue 정상동작에 의존하는데 공유 `git branch -D` 하나가 없어 깨짐.** **수정**: 공유 `gitio.delete_branch`를 *모든* requeue 경로가 호출(또는 fence-qualified `omd/<task>-<fence>`).
- **G. D4 세마포어 TTL × D6 궤도 TTL — 비대칭 lease로 빌드 슬롯이 먼저 만료.** GC pause가 빌드 TTL보다 길고 궤도 TTL보단 짧으면, permit은 만료돼 B에게 재부여되는데 A의 write-궤도는 HELD → A의 connect가 (궤도만 보는) 게이트 통과해 머지하는 동안 B도 "같은 슬롯"에서 빌드. **수정**: 한 (agent,task)의 permit과 궤도가 **단일 lease epoch**를 공유해 함께 renew(D2).
- **H. D3 LATCH `done` vs `merged` × SINGULON — 이른 의존 해제가 입체 창을 재오픈.** `done≠merged`: finish 후에도 producer의 connect가 conflict→ABORT→requeue하며 *같은 경로를 재작성* 가능. `done`에 깨어난 consumer가 겹치는 경로를 claim. `_conflicts`는 *현재 HELD*만 보므로 producer 궤도가 release되면 입체 전제가 사라짐. **수정**: 의존 해제를 `done`이 아니라 **`merged`**(머지후 latch)에 — `CONCEPT.md:121`의 `flag wait <producer>=done` 관용구를 `=merged`로 변경.

---

## 4. 데이터 모델 / FSM / MCP verb 델타 (구현 명세)

### 4.1 SQLite 스키마

```sql
-- D1: PRAGMA WAL/busy_timeout/foreign_keys/synchronous; connect(isolation_level=None);  Store.tx()=BEGIN IMMEDIATE..COMMIT
-- D2/D4/D5 통합 LEASE (orbits 테이블 확장 — orbit 컬럼 유지)
ALTER orbits ADD kind TEXT NOT NULL DEFAULT 'orbit';   -- orbit|sem_permit|barrier_seat|flag_ephemeral|merge_token
ALTER orbits ADD owner_token TEXT;        -- 소유/release+renew 가드
ALTER orbits ADD epoch INTEGER DEFAULT 0; -- HELD→(EXPIRED|RELEASED) 시 증가; ABA 방어
ALTER orbits ADD ttl_seconds REAL;        -- 원 ttl(D8 재앵커; core.py:50 하드코딩 600 수정)
ALTER orbits ADD renewed_at REAL;
ALTER orbits ADD intent_key TEXT;         -- D9 자연 멱등
ALTER orbits ADD resource_key TEXT;       -- sem/barrier 이름
ALTER orbits ADD merging INTEGER DEFAULT 0; -- 머지중 pin(due_orbits·orbits_held_by_agent에서 skip)
ALTER orbits ADD enqueued_seq INTEGER;    -- D7 FIFO 티켓
ALTER orbits ADD eff_priority INTEGER DEFAULT 0;
CREATE UNIQUE INDEX uq_orbits_fence ON orbits(fence) WHERE state IN ('PENDING','HELD','RESERVED');
CREATE INDEX idx_orbits_intent ON orbits(intent_key, state);
CREATE INDEX idx_orbits_owner  ON orbits(agent_id, state);

ALTER tasks ADD owner_agent TEXT;  ADD connect_fence INTEGER;  ADD connect_intent_at REAL;
ALTER tasks ADD merge_sha TEXT;    ADD merged_at REAL;         ADD branch_tip_sha TEXT;
CREATE TABLE task_deps (task_id TEXT, after_id TEXT, PRIMARY KEY(task_id,after_id));

ALTER agents ADD hb_expires_at REAL;  ADD bail_epoch INTEGER DEFAULT 0;

-- D3 durable latch (lease 아님 — 회수 안 함)
ALTER flags ADD flag_type TEXT NOT NULL DEFAULT 'LATCH';  -- LATCH|EPHEMERAL
ALTER flags ADD epoch INTEGER DEFAULT 0;  ADD rank INTEGER DEFAULT 0;  -- done(1)<merged(2)
ALTER flags ADD status TEXT DEFAULT 'LIVE';  -- LIVE|CLEARED|BROKEN
ALTER flags ADD owner_agent TEXT;  ADD lease_id TEXT;       -- EPHEMERAL일 때 orbits FK
CREATE TABLE flag_waiters (waiter_id TEXT PRIMARY KEY, agent_id TEXT, key TEXT,
  want_value TEXT, observed_epoch INTEGER, deadline REAL, state TEXT, wake_reason TEXT);

-- 설정 레지스트리(lease 아님)
CREATE TABLE semaphores (sem_id TEXT PRIMARY KEY, max_permits INTEGER, cloud_id TEXT);
CREATE TABLE barriers (barrier_id TEXT PRIMARY KEY, name TEXT, kind TEXT, parties INTEGER,
  generation INTEGER DEFAULT 0, state TEXT, break_reason TEXT, policy TEXT, deadline_at REAL,
  UNIQUE(name, generation));
CREATE TABLE barrier_parties (barrier_id TEXT, generation INTEGER, task_id TEXT,
  agent_id TEXT, arrived INTEGER DEFAULT 0, arrive_fence INTEGER,
  PRIMARY KEY(barrier_id, generation, task_id));

-- D11 통합 타깃(머지 ref 고정 — transient-HEAD 버그 수정)
CREATE TABLE cloud (cloud_id TEXT PRIMARY KEY, repo TEXT, integration_branch TEXT NOT NULL, state TEXT);
-- D9 멱등
CREATE TABLE idempotency (request_id TEXT PRIMARY KEY, agent_id TEXT, verb TEXT,
  arg_hash TEXT, status TEXT, response TEXT, completed_at REAL);  -- INFLIGHT|DONE
-- D8/D14 meta: server_epoch, last_shutdown_ts, recovery_done_epoch, leader_lease
```

### 4.2 FSM 추가

```
ORBIT (fsm.py:11-19): states += RESERVED, BAILED
  + reserve: PENDING→RESERVED (D7 head 배리어)   + grant: RESERVED→HELD
  + evict:  HELD→EXPIRED (좀비 사유 스탬프; expire와 구분, D6)
  + bail:   HELD|PENDING→BAILED (자발 탈출, D2)
  + expire: PENDING|RESERVED→DENIED (큐 대기 유계, D7)   + reanchor: HELD→HELD (재기동, D8)
TASK (fsm.py:21-35):
  + rollback: CONNECTING→DONE (git상 미머지=재시도가능, D8)
  + reclaim requeue 집합에 CONNECTING 포함(abort:* 가 이미 커버, D2)
  + connect 재생(MERGED)은 FSM 전이 아님 — 코드 early-return 가드(D9)
새 kind:  permit(ACTIVE/RELEASED/EXPIRED)  ·  barrier(ARMED/TRIPPING/TRIPPED/CONSUMED/BROKEN)
          flag-LATCH(단조, clear/break 없음)  ·  flag-EPHEMERAL(set/clear→CLEARED/break→BROKEN)
```

### 4.3 MCP verb 델타

**신규(9):** `bail(agent)` · `depend(task, after)` · `flag_wait(key,want,timeout)`/`flag_wait_poll(waiter_id)` · `sem_declare(name,max)`/`acquire(sem,ttl)`/`sem_release(permit,fence)` · `barrier_declare(name,kind,task_ids,policy)`/`barrier_arrive(name,agent,task)`/`barrier_abort(name,agent)`.

**시그니처 변경(7):** `claim`/`renew`/`release`/`finish`/`commit`/`connect`/`flag_set` 전부 `(agent_id, fence)` + `request_id` 추가. `connect(task,fence)`는 A/B/C split-phase + merge_token. `flag_set`은 `flag_type`/`terminal`/`ttl` 추가. (원자성·fencing은 동사 추가 아니라 *기반의 성질*.)

---

## 5. 우선순위 로드맵 (P0/P1/P2)

### P0 — SINGULON 위반 또는 영구 고아/행을 내는 현존 버그

| # | 버그 (file:line) | 한 줄 수정 |
|---|---|---|
| P0-1 | claim TOCTOU — `_conflicts`(`core.py:93`)와 `add_orbit HELD`(`core.py:104`) 비원자 → 둘 다 grant → 분열 | claim 본문을 D1 `BEGIN IMMEDIATE` actor 임계구역으로 |
| P0-2 | next_fence 중복 토큰 — read-then-write+중간commit (`store.py:55-62`) | 단일 atomic `UPDATE ... +1 RETURNING` + live-fence UNIQUE 인덱스 + init 시 `MAX(fence)` seed |
| P0-3 | release 무소유체크 — 아무나 남의 궤도 해제 (`core.py:118-125`) → 이중부여 → 분열 | `(agent,fence)` 요구; 이미 RELEASED면 멱등 OK(`core.py:120` 수정) |
| P0-4 | connect fence-blind merge-gate — `state!='HELD'`만, fence 비교 없음 (`core.py:222`) → ABA | `connect(task,fence)`가 모든 write-궤도 `state==HELD ∧ fence==captured` 검증 |
| P0-5 | 공유 레포 동시 merge (`gitio.py:49-59`, per-orbit pin은 git 직렬화 못 함) → 두 입체 connect가 index 오염 | repo-wide merge_token(§D11) + 명시적 `checkout integration_branch` |
| P0-6 | 이중쓰기 무복구 (`core.py:236→240`) — 크래시면 브랜치 머지됐는데 task CONNECTING 영구고착 | intent+merge_sha+trailer 영속 + `_recover()`(D8) |
| P0-7 | **agent_ttl 기본 None ⇒ 전 회수 비활성** (`core.py:24,133`) → 죽은 물방울 궤도/플래그/permit/좌석 영구 고아 | 기본 `heartbeat_ttl=90s`, 회수 항상 ON, kind별 생존 |
| P0-8 | reclaim이 worktree만 지우고 **브랜치 미삭제** (`gitio.py:61-65`) → 다음 `start()`가 기존 브랜치에서 실패(`gitio.py:38-41`) → requeue task 영구 wedge | `gitio.delete_branch` 추가, 모든 requeue 경로가 호출 (또는 fence-qualified 브랜치) |
| P0-9 | reclaim이 CONNECTING task 무시 (`core.py:143`) → 머지중 사망이 CONNECTING+worktree 누수 | requeue 집합에 CONNECTING 포함 + git 조정 |
| P0-10 | task 의존 사이클 미검출 (`core.py:81`) → 상호의존 영구 BLOCKED | declare/depend에 `_would_cycle` 게이트 |
| P0-11 | **write-set FS 미강제** (`gitio.py:43-47` `git add -A`) → 선언상 서로소가 실제 겹침 = 분열, 전 검사 통과 | connect 게이트에서 `git diff --name-only` vs 궤도 glob 감사(§D10) |

### P1 — 누락된 안전(프리미티브 크래시안전, 전 fencing, 원자성)
WAL+busy_timeout+manual-txn / 통합 `leases` 테이블+`reclaim_agent` 단일루틴 / 전 동사 fence 가드 / ephemeral-flag PRODUCER_DEAD+LATCH 단조 / 크래시안전 세마포어 / 세대-스탬프 배리어+BROKEN / ZOMBIE latch+bail_epoch / no-overtaking(+RESERVED-in-wait-for) / split-phase WAL git verb 멱등 / 복구 sweep.

### P2 — 경화
monotonic clock 내부비교 / fence-qualified worktree 경로 / idempotency 테이블 GC / phase-fair reader↔writer 교대 / connect 재시도 예산+백오프 / 배리어 shrink는 의존자 없을 때만.

---

## 5.1 구현 진척 (increment log) — ooptdd 기반

> 정직 표기: 무엇이 **구현+테스트**됐고 무엇이 아직 **설계뿐**인지. 검증 규율 = ooptdd(LTDD).
> 관측가능 동작은 트레이스 게이트(`gates/*.yaml` + store 도착검증), µs 동시성 불변식은 직접 territory 테스트(원칙 7 log-free zone).

### ✅ 증분 1 — D1 토대 (P0-1 TOCTOU, P0-2 fence중복) — DONE
- `store.py`: autocommit+WAL 연결, `tx()`(재진입 `BEGIN IMMEDIATE`/COMMIT/ROLLBACK), 원자 `next_fence`(단일문 +1), `uq_orbits_fence` UNIQUE 인덱스(fail-closed), 메서드별 commit 제거.
- `core.py`: `_cs()`(프로세스내 단일 writer RLock + `tx()`)로 **모든 변이 동사** 래핑 → claim의 충돌검사→grant가 원자적. `_sweep_inline`/`_promote_pending`이 같은 트랜잭션에서 재진입.
- `events.py`: LTDD `Emitter`(backend 없으면 no-op). claim/release/sweep/connect 등 구조화 이벤트 방출.
- 게이트: `gates/claim.yaml`(orbit_requested→orbit_granted, fence value-pinned, must_order).
- 테스트(27 green): `test_ltdd_claim.py`(트레이스 도착 green + **silent-loss drop → RED**, evidence_tier='arrived'), `test_concurrency.py`(tx 원자 롤백, **N-동시 겹침 claim → 정확히 1 HELD**, 동시 서로소 → fence 유일·연속, **두 코디네이터/한 DB → 1 HELD**, UNIQUE-fence 백스톱).
- **변이검증(이빨 확인)**: `tx()` 무력화 시 two-coordinator claim이 **300회 중 76회 double-grant(분열)** → 테스트가 잡고 BEGIN IMMEDIATE가 닫음을 실증(fake green 아님).
- **알려진 부채**: start/connect의 git 서브프로세스가 아직 임계구역(lock+tx) *안에서* 돈다 → 멀티프로세스 stall 가능. split-phase connect(§3.B, §D8)로 다음 증분에서 분리.

### ⬜ 다음 증분 후보 (설계는 CONCURRENCY 완료, 구현 대기)
P0-3 release owner+fence 체크 · P0-7 `agent_ttl` 기본 ON + 통합 `reclaim_agent`(D2) + `bail` 동사 · P0-4 connect fence captured 비교 · P0-8 reclaim 시 `git branch -D` · P0-5/§D11 merge_token + split-phase connect · P0-6/§D8 `_recover()` · P0-10 의존 DAG 사이클 · P0-11/§D10 connect diff 감사 · D3 플래그(EPHEMERAL/LATCH+wait) · D4 세마포어 · D5 배리어.

---

## 6. 물방울(에이전트) 계약 요약

1. **renew 주기 = lease_ttl/3**(etcd 관용). `heartbeat(agent)` 한 번이 모든 hb_bound lease 갱신.
2. **자기 탈출(긴급)**: 회복 불가 시 종료 전 `bail(agent)` (멱등). 도중 죽어도 sweeper가 마저 정리.
3. **fence 복종**: 어떤 호출이든 `{fenced_out:true}` 또는 heartbeat가 그렇게 답하면 → **LOST 상태**: 쓰기·커밋·connect 즉시 중단, 종료/재생성. **서버가 생존을 판정한다.**
4. **자기 의심**: renew/heartbeat가 실패(타임아웃/네트워크)하면 추방 가능성 → renew 성공으로 fence 재확인 전까지 **쓰기 일시정지**. 가정만으로 재개 금지.
5. **worktree FS 에러(ENOENT 등)=추방 확정** → abort. worktree 스스로 재생성 금지.
6. **궤도 획득은 all-or-none 선호**: 전체 write-set을 한 번에 `claim_set`(데드락-free). 증분이면 `CANON_ORDER`. `{deadlock:true}`면 전부 release 후 재시도.
7. **wait는 timeout 필수**: `flag_wait` register→poll. `SATISFIED`/`TIMEOUT`/`BROKEN(producer_dead)` 전부 처리(BROKEN을 성공이나 hang으로 오인 금지).
8. **의존 해제는 `=done` 아니라 `=merged`** 대기(§3.H).
9. **connect는 멱등**: 재시도 안전(이미 머지됐으면 재머지 없이 `MERGED` 회신). 재기동 후 `server_epoch` 바뀌면 즉시 renew 한 번.

---

## 7. 미해결 / 결정거리

- **D14 HA 입장 확정**: 단일 인스턴스 강제(거부) vs 리더-lease 페일오버 — 후자는 통합 머지 조정까지 필요(범위 큼). 우선 *단일 인스턴스 강제 + `:memory:` 금지*.
- **D10 강제 방식**: sparse-checkout(완전 격리, 비용↑) vs pre-connect diff 감사(저렴, 사후) 중 v1 선택. (감사 우선 권장.)
- **D12 read-set 코히런스**의 정확한 무효화 정책(전 consumer 강제 rebase vs 알림만).
- **periodic sweep** 도입 여부(현재 inline only) — D1 throughput 예산(reads를 WAL 리더로, sweep/promote/barrier-reconcile을 유계 주기 tick으로) 과 함께.
- **durable 엔진**(DBOS) 채택은 여전히 선택(현 설계는 DB-backed FSM + git-진실 복구로 충분; 장기 크래시-내성이 정말 필요해지면).
- glob char-class 정밀 교집합(현 `disjoint.py`는 보수적 True — soundness 유지, 병렬도만 손해).

---

### 부록: 설계 산출 메타
9차원(D1–D9) fan-out → 차원별 적대적 검증(전부 FLAWED, 잔여/신규 레이스 적출) → 완전성 비평(D10–D14 + 교차작용 A–H) → 교차통합. 20 에이전트. 모든 `file:line`은 현 `omd_server/` 코드 대조 확인.
