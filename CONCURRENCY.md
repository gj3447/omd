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

(c)는 완전성 비평이 찾은 가장 큰 구멍이었다(§2 D10): 오래도록 선언만 검사하고 *실제 쓰기 영역은 검사하지 않았다*. **증분4(P0-11)에서 강제됨** — connect 게이트가 `git diff --name-only base...branch`의 모든 경로가 claimed write-set glob 에 덮이는지 감사하고, 밖이면 `writeset_violation` 으로 거부(merge 없음). 이로써 (a)·(b)·(c) 셋이 모두 성립 = P0 전부 닫힘.

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

### D12 — read-set 코히런스/유령 읽기  `[FIXED 증분9]`

- `reads`는 저장되나 `_conflicts`/`next_task`/`connect` 어디서도 사용 안 됨. consumer가 `src/api/**`를 read claim하고 작업하는데 producer가 `src/api/new.py`(읽을 때 없던 유령)를 응결하면, consumer는 옛 base에서 분기했으므로 **조용히 낡은 뷰 위에 빌드**(자기 머지는 성공하지만 *로직*이 틀림). SINGULON은 write-disjointness만 보장.
- 기제: 통합 브랜치 generation 추적; 응결이 live read-궤도와 겹치는 경로를 추가/변경하면 그 consumer의 read-lease를 **stale 표시** → consumer는 자기 connect 전 rebase/재독 강제. stale 신호는 D3 플래그/이벤트로.

### D13 — git/FS 기반 장애  `[GAP]`

- worktree 디스크풀: 물방울 N개 = 전체 체크아웃 N개. `add_worktree`/`commit_all` 실패(`GitError`)를 `start`/`commit`이 안 잡음 → 반쯤 만든 브랜치가 재시도를 막음. 백그라운드 `git gc`/외부 `git worktree prune`가 live worktree를 경합/고아화. `remove_worktree`가 에러를 삼켜(`gitio.py:62-65`) 이를 가림.
- 기제: `GitError`를 **transient/disk/fatal로 분류**(`gitio.py:13`는 단일 타입); worktree 생성 전 quota preflight; 관리 레포 auto-gc 비활성; ENOSPC 별도 처리. D9의 재시도 분류와 연동.

### D14 — 코디네이터 singleton / HA 입장  `[FIXED 증분9 — 단일 인스턴스 강제]`

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

### ✅ 증분 2 — D2 긴급탈출/통합회수 + D6(부분) — DONE (P0-3·P0-7·P0-8·P0-9)
- `core.py`: **단일 `_reclaim_agent_inline`**(자발 `bail` ∪ 비자발 좀비회수) — 보유 궤도(HELD/PENDING) 전부 해제 + 진행중 작업(CLAIMED/IN_ORBIT/**CONNECTING**, P0-9) requeue + worktree/브랜치 정리 + RETIRE. 멱등.
- `bail(agent)` 공개 동사(긴급 탈출). `agent_ttl` **기본 90s = 회수 ON**(P0-7; None=비활성).
- `release`/`renew`: **소유+fence 가드**(`_check_owner`) — 아무나 남의 궤도 해제 불가(P0-3). 오추방 좀비의 renew=FENCED_OUT. release 재시도는 멱등 no-op.
- `gitio.delete_branch`/`branch_exists`: reclaim 시 `omd/<task>` 삭제(P0-8) — 안 지우면 다음 `start()`가 '브랜치 존재'로 wedge.
- 테스트(38 green, 신규 11): `test_d2_reclaim.py`(bail→해제+requeue+promote, 멱등, 자발/비자발 수렴, 기본 ON, LTDD `gates/bail.yaml`), `test_d6_fence.py`(non-owner/stale-fence 거부·HELD 유지, 멱등 replay, 좀비 renew FENCED_OUT), `test_git_integration.py::test_reclaim_deletes_branch_so_restart_works`(P0-8 E2E).
- **변이검증**: `_check_owner` 우회 시 비소유 agent가 남의 궤도를 RELEASED 시킴(이중부여) → 테스트가 'not owner'로 잡음.
- 시그니처 변경: `release(orbit_id, agent, fence)` / `renew(orbit_id, agent, fence, ttl)` + `bail(agent)` (server/cli/tests 동반 갱신).

### ✅ 증분 3 — split-phase connect + merge_token + 복구 (P0-4·P0-5·P0-6) — DONE
- `core.py` **split-phase `connect(task, agent=None, fence=None)`** (§3.B/§D8/§D11) — git merge가 락(_cs)/tx **밖**에서 돈다("멀티프로세스 stall" TODO 해소):
  - **Phase A**(`_connect_phase_a`, 락+tx): 모든 write-orbit `state==HELD` 재검증, 호출자가 `(agent,fence)`를 주면 `owner==agent ∧ fence==captured`까지(P0-4 ABA 가드) → 불일치면 `fenced_out`. **repo-wide `merge_token`**(`kind='merge_token'`, capacity 1 = `held_merge_token` 단일행 → 가용 아니면 retry) 획득. task→CONNECTING, 궤도 **pin**(`merging=1`+유계 `merge_deadline`, §E), intent 영속(`connect_fence`/`branch_tip_sha`/`connect_intent_at`).
  - **Phase B**(`_connect_phase_b`, **락 밖, live tx 없음**): 전용 **통합 worktree**(`<root>-omd-integration`)에서 `checkout integration_branch` + `merge --no-ff`(고유 trailer `OMD-Connect: <task>`) + **subprocess 타임아웃**(`merge_timeout`, 기본 120s, §E). 충돌/타임아웃이면 `merge --abort`(`GitTimeout` 분리). **사용자 HEAD(root) 불침범**(§D11).
  - **Phase C**(`_connect_phase_c`, 락+tx): 성공이면 **`merge_sha` 먼저 기록**(P0-6 순서 버그 수정) → task→MERGED → write-orbit 해제+unpin → merge_token 반납 → droplet worktree 제거 → promote. 실패면 **CONNECTING→DONE rollback**(재시도가능, FSM 신규 전이) + unpin + 토큰 반납.
- **merge_token crash-safe**(§D11): `merge_started_mono` 저장. `_reclaim_agent_inline`이 죽은 보유자의 토큰을 `_abort_dangling_merge`(통합 worktree `merge --abort`) 후 반납 + 보유 궤도 unpin.
- **`_recover()`**(`__init__` 끝, 멱등, §D8/P0-6): 통합 worktree 보장 후 CONNECTING task를 git 진실과 조정 — trailer-probe(`branch_in_integration`, **줄단위 `^…$` 정확매칭** = prefix 오탐 차단)로 통합에 있으면 전진수정(merge_sha 기록+해제+worktree 제거), 없으면 rollback→DONE. 잔존 merge_token은 전부 abort+반납(재기동 시점 HELD=정의상 dangling).
- 스키마(`store.py`, additive·fresh-DB 친화 + 멱등 `_migrate` ALTER): orbits `kind`/`resource_key`/`merging`/`merge_deadline`/`merge_started_mono`; tasks `connect_fence`/`connect_intent_at`/`branch_tip_sha`/`merge_sha`/`merged_at`. `held_orbits`/`pending_orbits`/`due_orbits`는 `kind='orbit'`만(merge_token 제외) + `due_orbits`는 `merging=0`만(pin skip, §E).
- `gitio.py`: `_git(timeout=)` + `GitTimeout`, `ensure_integration_worktree`, `merge_into`(전용 worktree·명시 checkout), `branch_in_integration`(anchored trailer-probe), `branch_tip`, `abort_merge`/`has_merge_in_progress`. 구 `merge()`(root HEAD 변이) 제거.
- 테스트(**47 passed, 1 skipped**; 신규 10 = `test_d8_connect_splitphase.py`, +`gates/connect.yaml`): merge_token 상호배제(서로소 2 task 동시 connect→둘 다 MERGED·통합에 두 파일·index clean·동시 토큰 ≤1·누수 0), **git이 락 밖**(Phase B 시 `_txn_depth==0` ∧ 머지 중 다른 변이 interleave), P0-4 stale fence(만료+ABA) + 정상대조, `_recover()`(통합有→MERGED+merge_sha+해제+토큰반납 / 통합無→DONE rollback+재connect / trailer prefix 오탐 차단 / 멱등). 기존 git E2E는 통합 worktree 모델로 갱신(아래 deviation).
- **변이검증(이빨 3건 실증)**: ① merge_token 상호배제 무력화 → 동시 connect가 같은 통합 worktree index 경합으로 `fatal: 스태시 실패`, task B가 MERGED 못 됨 → 테스트 RED. ② P0-4 fence/owner-equality 가드 무력화 → ABA(궤도 HELD·fence bump) connect가 잘못 MERGED → 테스트 RED. ③ trailer-probe를 substring(-F)으로 회귀 → 미머지 'A'가 'AB' 응결로 오탐돼 MERGED → 복구 테스트 RED. 셋 다 복원 후 green.
- 시그니처: `connect(task, agent=None, fence=None)`(하위호환 — 기존 무인자 호출 유지) + `Coordinator(integration_branch=, merge_timeout=)`; server/cli 동반 갱신.
- **deviation(정직 표기)**:
  1. **통합 worktree는 별 브랜치 필요**. git은 한 브랜치를 두 worktree에 못 건다. 그래서 root(사용자 HEAD)와 integration_branch가 **다른 브랜치**여야 한다(§D11 "사용자 HEAD 불침범"의 자연스러운 귀결). 기존 git E2E 테스트를 root=`dev`/통합=`main`으로 갱신하고 단언을 통합 worktree 경로로 옮김(merge 결과를 root가 아니라 통합 worktree에서 확인). 명시 안 하면 `integration_branch=현재 브랜치`인데, 그게 root에 체크아웃돼 있으면 통합 worktree 생성이 실패하므로 **운영 계약 = root는 통합 브랜치에 머무르지 않는다**(또는 `integration_branch=`를 명시).
  2. merge_token 경합 시 `connect`는 즉시 실패가 아니라 **내부 재시도 루프**(deadline까지 10ms 폴링)로 직렬화 — 동시 connect 둘 다 결국 MERGED. (명세는 "획득"만 규정; non-blocking retry-return도 가능하나 호출자 단순화를 위해 내부 직렬화 택함.)
  3. P0-4 fence 가드는 **호출자가 `(agent,fence)`를 줄 때만** 엄격(ABA 차단). 무인자 `connect(task)`(기존 경로)는 write-orbit `state==HELD`만 검사(증분2까지의 동작 유지) — 즉 strict-fence는 opt-in. server/cli는 `--agent/--fence`를 노출.
- **남은 P0 부채**: P0-7~P0-9는 증분2에서 닫힘(merge_token reclaim도 이번에 추가). 미구현 = **P0-10**(declare/depend 의존 DAG 사이클 — claim 사이클만 잡힘), **P0-11/§D10**(connect 게이트의 `git diff --name-only` vs 궤도 glob 감사 = write-set FS 강제, "최대 구멍" 여전히 미강제). D3/D4/D5(플래그·세마포어·배리어), D6 잔여(finish/commit 소유+fence, bail_epoch), D9 idempotency 테이블, D12 read-set 코히런스, D14 HA 입장도 설계만.

### ✅ 증분 4 — 의존 DAG 사이클 + write-set FS 강제 (P0-10·P0-11) — DONE
> 마지막 두 미강제 SINGULON 지점. P0-11 강제로 SINGULON 토대 **(c)** 가 성립한다 —
> *선언상* 서로소 write-set이 *실제* write-set이 됨(궤도가 더 이상 advisory 아님).

- **P0-10 — 의존 DAG 사이클 게이트 (§D7)** (`core.py`):
  - `_find_cycle`(DFS WHITE/GRAY/BLACK 색칠 → back-edge 가 사이클, 경로 반환) + `_dep_graph`(전 task `deps` + 후보 엣지) + `_would_cycle(task_id, deps)`(후보 엣지 가상추가 후 전역 재검 — self-dep=길이1 사이클).
  - `declare(deps=...)`: deps가 사이클을 만들면 `{ok:false, reason:'dep_cycle', cycle:[...]}` (task 생성 안 함). 정상이면 `{ok:true, ...}`(기존 호출부는 반환값 미사용 — 비파괴).
  - **신규 동사 `depend(task, after)`** (MCP + CLI): 엣지를 추가하되 사이클을 만들면 **거부(그래프 불변)**. 이미 있는 엣지는 멱등 noop. check-then-add 가 `_cs()` 안에서 원자.
  - `store.all_tasks()`/`set_task_deps()` 추가.
- **P0-11 / §D10 — write-set 파일시스템 강제(저비용 pre-connect 감사, option 2)**:
  - `disjoint.path_matches_glob`/`path_in_globs`: 구체 경로가 glob 에 **정확** 매칭하나(세그먼트 단위, `**`=0+세그먼트, 한 세그먼트 내 `*`/`?`/`[...]`는 `fnmatchcase`). **`globs_overlap` 을 그대로 안 씀** — char-class를 보수적 overlap=True 로 over-report 하므로 감사에선 "덮인다"의 false-positive(=궤도 밖 쓰기 통과)가 되어 분열을 놓친다. 정확매처는 와일드카드-free 경로에 **절대 false-positive("덮인다")를 안 냄**(soundness).
  - `gitio.changed_paths(branch, base)`: `git diff --name-only --no-renames base...branch`(+`core.quotepath=false`) — merge-base 이후 branch가 건드린 모든 경로(통합 쪽 변경 제외). rename은 `--no-renames`로 원본+새경로 둘 다 나와 궤도 밖 이동도 잡힘. delete도 touched.
  - `core._writeset_audit`: claimed write-set = task의 HELD `mode='write'` 궤도 pathspec 합집합. 변경 경로 중 `path_in_globs` 로 안 덮이는 것 = `offending`.
  - **connect 게이트(Phase A)**: stale-fence 통과 직후 · merge_token 획득 **전** 감사. 위반이면 `{ok:false, reason:'writeset_violation', offending:[...]}` 반환 — **merge 안 함, 토큰 안 잡음, task 상태 불변, 통합 브랜치 불변**.
  - `commit`도 커밋 후 **자문(advisory)** 감사(`offending` 동봉 + `commit_writeset_warning`) — 조기 경고. 권위 강제 지점은 connect.
- 테스트(**61 passed, 1 skipped**; 신규 14):
  - `test_d7_dep_cycle.py`(7): depend back-edge(A→B→A)·3-노드 사이클·declare(deps) 사이클·self-dep 전부 `dep_cycle` 거부+그래프 불변; 유효 DAG 수락+unblock; depend 멱등; **territory check**(게이트 우회해 store 에 직접 상호의존 심으면 next_task=None=영구 BLOCKED).
  - `test_d10_writeset_fs.py`(7): 정확매처 soundness 단위; 궤도 밖 쓰기(a/** claim + b/foo)→`writeset_violation`+통합 불변+토큰누수0; 궤도 안만→MERGED; 다중 write-orbit 합집합 커버(a/**∪c/**)+합집합 밖(d/) 거부; 궤도 밖 rename→거부; **이빨**(감사 무력화 시 분열이 머지됨).
- **변이검증(이빨 실증, 복원함)**:
  ① write-set 감사 무력화(`_writeset_audit→[]`) → 궤도 밖 `b/foo.py` 가 `MERGED`(통합에 실제로 들어옴 = §D10 분열) → `test_out_of_bounds_write_is_rejected`·`test_multiple_orbits_one_path_outside_union`·`test_rename_out_of_bounds_is_rejected` **3건 RED**.
  ② 사이클 게이트 무력화(`_would_cycle→None`) → 상호의존 엣지가 수락되고 `next_task=None`(둘 다 영구 BLOCKED) → `test_d7_dep_cycle.py` **4건 RED**. 둘 다 복원 후 61 green.
- **deviation/한계(정직 표기)**:
  1. **commit 감사는 자문(non-blocking)** — 커밋은 되돌리지 않고 `offending` 만 회신. 권위 거부는 connect 게이트(명세 §D10 "connect 게이트가 자연스러운 강제 지점"). sparse-checkout(option 1, 물리 격리)은 미채택 — option 2(저비용 감사) 채택(§7 결정대로).
  2. 감사 기준 base = `integration_branch`(3-dot merge-base). 통합이 앞서 나가도(다른 task 선머지) 3-dot 가 *이 task의* 변경만 봐서 false-reject 없음(기존 sequential A→B E2E 통과로 확인).
  3. char-class 궤도에서도 감사는 **정확**(over-report 안 함). 단 claim/next 의 *입체검사* 는 여전히 `globs_overlap`(보수적) — 거기선 over-report 가 안전(병렬도만 손해). 둘의 비대칭은 의도적(감사=soundness on "덮임", 입체=soundness on "겹침").
- **남은 P0 부채**: **P0 전부 닫힘(P0-1~P0-11)**. 미구현 = D3/D4/D5(플래그·세마포어·배리어), D6 잔여(finish/commit 소유+fence, bail_epoch), D9 idempotency 테이블, D12 read-set 코히런스, D14 HA 입장 — 전부 설계만(P1/P2).

### ✅ 증분 5 — D6 잔여(finish/commit/connect 소유+fence + bail_epoch) + D9 멱등 — DONE
> 전 변이 동사 fencing 완성 + at-least-once MCP 의 exactly-once 효과. SINGULON 보존:
> 오추방 좀비가 남의 작업을 finish/commit/connect 못 하고, 재시도가 누수 lease·이중 merge·
> 이중 release 를 안 만든다.

- **D6 잔여(§D6 표, FENCED_OUT)** (`core.py`):
  - `_check_task_write_fence(task, agent, fence)`: caller가 `(agent,fence)`를 주면 `task.owner==agent ∧ 모든 write-orbit HELD ∧ fence==f` 재검증(opt-in, 증분3 connect 의 strict-fence 패턴과 동형). `finish`/`commit`/`connect` 전부 적용. 무인자 호출은 증분2까지 동작 유지(하위호환).
  - **bail_epoch(좀비 GC-pause 부활방지, §D6 §1.1)**: agents `bail_epoch` 컬럼(단조). `_reclaim_agent_inline`이 회수 시 `bump_bail_epoch`. `_check_alive(agent, bail_epoch)`가 모든 변이 앞에서 — (a) agent가 RETIRED/ZOMBIE/BAILING 이면 차단(죽은 자는 변이 불가), (b) caller가 든 bail_epoch가 현재값과 다르면 차단. **heartbeat의 state 리셋(WORKING)으로 못 우회** — epoch는 보존된다. `heartbeat`는 RETIRED 좀비에게 `{fenced_out:true}` 회신(부활 거부) + 살아있으면 현재 bail_epoch 회신(물방울이 이후 변이에 실어 보냄). `claim`(HELD)도 bail_epoch 회신.
- **D9 멱등(§D9, §3.C 교차)** (`core.py`/`store.py`):
  - `idempotency` 테이블(`request_id` PK, INFLIGHT/DONE) + `_idem(request_id,...)` 임계구역 래퍼. **성공 종단만 캐시**(`_is_success`: ok:false·fenced_out·deadlock·retry·DENIED 는 캐시 금지 → 세상이 바뀌면 재시도 가능, §3.C). 전 변이 동사(claim/renew/release/bail/start/commit/finish/connect/flag_set)에 적용. connect는 split-phase라 `_idem` tx를 Phase B에 걸칠 수 없어 캐시 확인/기록을 짧은 `_cs()` 두 곳으로 나눔.
  - **의미적 멱등**(dedup 캐시 우회돼도 안전): claim `intent_key=hash(agent,sorted(paths),mode,task)` → 같은 의도면 기존 HELD/PENDING 궤도 반환(`orbit_by_intent`, 누수 lease 0); start 기존 worktree 감지(이미 IN_ORBIT/이후+같은 agent면 `worktree add -b` 재호출 안 함); finish 이미 DONE 이면 noop; connect already-merged 회신(기존).
  - **§3.C fencing 교차**: (1) 성공만 캐시(stale-fence 거부 영구 재생 차단). (2) claim 의미적 dedup은 **현재 caller가 그 궤도의 소유자일 때만** 살아있는 HELD 를 반환 — 회수돼 타인에게 재부여된 lease를 우회로 넘기지 않음(부활 무장 차단). (3) release 재생은 owner/fence 가드가 감싸 *재부여된* lease를 풀지 않음.
- 스키마(additive·fresh-DB 친화 + 멱등 `_migrate`): agents `bail_epoch`; orbits `intent_key`(+`idx_orbits_intent`); 신규 `idempotency` 테이블(fresh-DB는 `_SCHEMA`, 기존 DB는 `CREATE IF NOT EXISTS`).
- 시그니처: `finish`/`commit`에 `(agent_id=None, fence=None)` 추가, 전 변이 동사에 `request_id`/`bail_epoch` kwarg(전부 None 디폴트=하위호환). `commit`/`heartbeat` 를 CLI 에도 노출. server/cli 동반 갱신.
- 테스트(**81 passed, 1 skipped**; 신규 20 = `test_d6_remaining.py` 10, `test_d9_idempotency.py` 10):
  - D6: finish stale-fence(ABA)·non-owner 거부+task 미완료, finish owner-fence OK, 무인자 하위호환, commit stale-fence 거부, commit owner-fence OK; **크래시 실패경로** — write-orbit 보유 task agent 회수 후 connect FENCED_OUT(merge 없음); **bail_epoch 이빨** — 회수 후 옛 epoch claim/renew 차단, heartbeat 가 RETIRED 좀비에 fenced_out 회신.
  - D9: claim 재시도 누수 0(request_id + intent_key 둘 다), DENIED/fenced_out 캐시 금지+재시도 성공, start 재시도 worktree 재생성 안 함, connect 재시도 머지커밋 1개·이미-MERGED noop, **merge conflict 캐시 금지(retryable)**, **§3.C** dedup release 가 재부여 agB lease 를 안 품.
- **변이검증(이빨 4건 실증, 복원함)**:
  ① `_check_alive` 무력화(`return None`) → 회수된 좀비가 옛 bail_epoch 로 claim 성공 → `test_stale_bail_epoch_blocks_resurrected_zombie` RED.
  ② `_check_task_write_fence` 무력화 → finish/commit 가 stale-fence/non-owner 를 통과 → 3건 RED(finish stale·non-owner, commit stale).
  ③ `_is_success`→`return True`(§3.C "성공만 캐시" 위반) → DENIED·fenced_out·merge-conflict 가 캐시돼 재시도 영구 차단 → 3건 RED.
  ④ claim 의미적 dedup 무력화(`dup=None`) → 재시도가 두 번째 누수 궤도 생성 → `test_claim_semantic_dedup_without_request_id` RED. 넷 다 복원 후 81 green.
- **deviation/한계(정직 표기)**:
  1. **D6 가드는 opt-in**(증분3 connect 와 동일 철학) — `finish`/`commit`/`connect` 무인자 호출은 write-orbit `state==HELD` 만 보던 종전 동작 유지(strict owner/fence 는 caller가 `(agent,fence)` 줄 때만). 물방울 계약상 권장 = 항상 `(agent,fence,bail_epoch)` 전달. 단 **bail_epoch 의 state 가드(RETIRED 차단)는 opt-in 아님** — agent가 등록돼 있고 RETIRED/ZOMBIE 면 bail_epoch 미제공이어도 차단(좀비 부활은 항상 막음).
  2. **flag_set 의 D6 가드는 부분** — 현 flags 테이블은 단순 LATCH(owner_agent/epoch 컬럼 없음, D3 범위). 그래서 flag_set 은 bail_epoch(좀비 차단)+request_id(멱등)만 적용하고 §D6 표의 "owner CAS·epoch CAS" 는 D3(EPHEMERAL 플래그) 증분으로 미룸. 정직: flag_set 은 아직 임의 caller가 LATCH 를 set 할 수 있다(소유 개념 자체가 D3 전엔 없음).
  3. **idempotency 테이블 GC 없음**(P2) — request_id 행이 무한 누적. 운영 GC(오래된 DONE 정리)는 P2.
  4. **connect 멱등 캐시는 비원자 2-스텝**(Phase C 성공 후 별도 `_cs()`에서 begin+finish_idem) — Phase C 와 캐시 기록 사이 크래시면 캐시 미기록이나 task 는 이미 MERGED 라 재시도가 already-merged(noop) 로 안전 수렴(의미적 멱등이 백스톱). 즉 request_id 캐시는 최적화, 의미적 멱등이 정확성 보장.
- **남은 부채**: P0 전부 닫힘(증분1~4). 미구현 = D3 플래그(EPHEMERAL/LATCH+wait+owner/epoch CAS) · D4 세마포어 · D5 배리어 · D12 read-set 코히런스 · D14 HA 입장 — 전부 설계만(P1/P2).

### ✅ 증분 6 — D3 플래그: EPHEMERAL(=lease) vs LATCH(영속·단조) + flag_wait register→poll — DONE
> 사용자 핵심 우려("작업중 플래그를 세운 놈이 죽으면 대기자 전원 영구 데드락")의 정면 해소(§1.2).
> 플래그를 두 종류로 분리: 소유 신호는 lease 라 §1.1 단일 회수 루틴이 자동으로 거두고,
> 단조 사실은 lease 가 아니라 producer 가 죽어도 살아남는다.

- **EPHEMERAL(=소유+TTL+heartbeat lease)** (`core.py` `_flag_set_ephemeral`):
  - `flag_set(key,value,agent,flag_type='EPHEMERAL',ttl=)` 가 받쳐주는 lease(`orbits.kind='flag_ephemeral'`, owned+TTL, `resource_key=key`)를 발급. owner 를 `upsert_agent` 로 등록(reclaim 이 찾을 수 있게).
  - **owner CAS**(§D6 보강): LIVE EPHEMERAL 은 같은 owner 만 재set, 타 agent `not flag owner` 거부. (증분5 deviation #2 "flag_set 의 owner 가드 미룸"을 EPHEMERAL 에 대해 닫음.)
  - **자동 clear/BROKEN(영구 hang 0)**: 보유자 사망의 **두 경로** 모두 단일 reclaim 루틴이 거둠 — (a) `bail`/좀비회수 → `_reclaim_agent_inline` 이 `flag_leases_owned_by` 를 거두며 `_break_ephemeral_flags_for_lease`(플래그 BROKEN + epoch +1 + 대기자 PRODUCER_DEAD 기상) + lease EXPIRE; (b) lease TTL 만료 → `_sweep_inline` 의 `due_flag_leases` 가 같은 break 경로. **agent_ttl 없이도 lease TTL 만으로 풀린다.**
  - `flag_clear(key,agent)`(owner 만, 정상 종료): lease 해제 + status→CLEARED + epoch +1. LATCH 는 clear 불가.
  - `heartbeat(agent)` 한 번이 그 agent 의 모든 flag_ephemeral lease 를 연장(§1.2 — 건강한 producer 가 renew 깜빡해 자기 신호가 BROKEN 되지 않게).
- **LATCH(영속·단조)** (`core.py` `_flag_set_latch`): `done`(rank 1)<`merged`(rank 2). **하향 set 거부**('un-finish 불가'), 동값 재발행 멱등 no-op, 상향/신규 set. 소유 개념 없음(connect 의 `merged` latch 가 그대로 동작) · **회수 대상 아님**(producer 죽어도 사실 살아남음).
- **flag_wait register→poll**(서버 비블로킹, `core.py`): `flag_wait(key,want,timeout,agent)` — **timeout=None 거부**(영구 hang 방지). 즉시 SATISFIED/BROKEN(producer_dead) 또는 `waiter_id` 발급. `flag_wait_poll(waiter_id)` — SATISFIED/TIMEOUT/BROKEN/WAITING. **epoch 재검사**(value 아님)로 ABA/유령기상 안전. poll 내부 `_sweep_inline` 이 lease TTL 만료를 BROKEN 으로 반영. **만족 판정 = 정확 값일치 OR 단조 랭크 도달** → `=done` 대기자는 `merged`로도 만족(merged ⊃ done).
- **§3.H 의존 해제 = `=merged`**: `flag_wait(producer,'merged',...)` 대기자는 producer 가 `done`(finish)만 세운 단계에선 **계속 WAITING**, `merged`(응결)에서만 기상 — 이른 의존 해제가 입체 창을 재오픈하는 것을 차단. 테스트로 명시 검증.
- 스키마(additive·fresh-DB 친화 + 멱등 `_migrate`): `flags` 에 `flag_type`/`epoch`/`rank`/`status`/`owner_agent`/`lease_id`; 신규 `flag_waiters` 테이블(`_SCHEMA` CREATE IF NOT EXISTS — 기존 DB도 획득). store: `get_flag_row`/`upsert_flag`/`set_flag_status`/`ephemeral_flags_for_lease` + `add/get/set_flag_waiter`/`waiters_for_key` + `flag_leases_owned_by`/`due_flag_leases`.
- server/cli: `flag_set`(+`flag_type`/`ttl`)·신규 `flag_clear`/`flag_wait`/`flag_wait_poll` 노출.
- 테스트(**108 passed, 1 skipped**; 신규 27 = `test_d3_flags.py` + `gates/flag.yaml`): LATCH 단조(set/get·상향·**하향거부**·동값멱등·producer 사망에도 영속·clear 불가); EPHEMERAL(lease 생성·owner 필수·**owner CAS**·정상 clear·non-owner clear 거부·type 혼동 거부); wait(즉시만족·set 후 만족·**timeout 필수**·timeout 발화·**§3.H merged-not-done**·done은 merged로 만족·미지 waiter·**epoch ABA 안전**); **크래시/사망/오추방 실패경로**(producer bail→BROKEN+PRODUCER_DEAD 기상+lease EXPIRE / 좀비회수→BROKEN / **lease TTL 만료→BROKEN**(agent_ttl 없이) / heartbeat 가 lease 연장 / 회수된 좀비 flag_set 차단 / request_id 멱등 누수0); LTDD 트레이스(`flag_set[EPHEMERAL]→flag_broken[producer_dead]` 순서 도착).
- **변이검증(이빨 4건 실증, 복원함)**:
  ① `_break_ephemeral_flags_for_lease` 무력화(early `return`) → producer 가 죽어도 플래그가 LIVE 로 잔존, 대기자가 영영 PRODUCER_DEAD 못 받음(= 사용자가 짚은 영구 데드락) → bail/좀비/TTL-만료/LTDD **4건 RED**.
  ② 단조 하향거부 무력화 → `merged` 뒤 `done` 으로 un-finish 됨 → `test_latch_downgrade_rejected` RED.
  ③ owner CAS 무력화 → 타 agent 가 남의 EPHEMERAL 신호를 덮어씀 → `test_ephemeral_owner_cas` RED.
  ④ `timeout is None` 거부 무력화 → 영구 wait 등록 시도(deadline 계산이 TypeError) → `test_wait_timeout_required` RED. 넷 다 복원 후 108 green.
- **deviation/한계(정직 표기)**:
  1. **EPHEMERAL set 은 agent 를 자동 upsert** — 안 그러면 `bail`/좀비회수가 `agents` 행을 못 찾아 noop → 플래그 영구 잔존(바로 해소하려던 버그). 즉 flag_set(EPHEMERAL)은 owner 를 살아있는 agent 로 등록하는 부작용이 있다(의도된 계약).
  2. **`flag_set` 의 bail_epoch CAS 는 부분** — 회수된 좀비(RETIRED/ZOMBIE/BAILING) 차단은 `_check_alive` 로 항상 ON 이지만, EPHEMERAL 의 §D6 "epoch CAS" 는 **owner CAS + lease fence 만료**로 대체 강제(별도 flag epoch 를 변이마다 caller 가 들고 오게 하진 않음 — orbit lease 가 이미 fence/owner 가드를 받으므로 이중화 불필요). LATCH 는 소유 개념 자체가 없어(§D3) owner/epoch CAS 비대상.
  3. **CLEARED ≠ BROKEN**: 자발 clear 는 '사실이 더는 참 아님'(producer 정상 종료)이라 대기자를 PRODUCER_DEAD 로 깨우지 않는다 — want 가 다른 값이면 계속 WAITING(타임아웃까지). 오직 보유자 **사망**(reclaim/TTL)만 BROKEN/PRODUCER_DEAD. 영구 hang 은 사망 경로가 BROKEN 으로 닫으므로 없음.
  4. **periodic sweep 없음**(현 inline only, §7 미해결) — flag_ephemeral lease TTL 만료의 BROKEN 반영은 누군가 `flag_wait_poll`/`sweep`/`next_task`/`claim` 등 inline-sweep 동사를 호출할 때 일어난다. 대기자가 poll 하면 반드시 반영되므로(poll 이 sweep 함) 대기 측엔 영구 hang 없음. 백그라운드 tick 은 P2.
- **남은 부채**: P0 전부 닫힘(증분1~4). 미구현 = **D4 세마포어** · **D5 배리어** · D12 read-set 코히런스 · D14 HA 입장 — 전부 설계만(P1/P2). (D3 = 증분6, D6 잔여+D9 = 증분5, P0-1~11 = 증분1~4.)

### ✅ 증분 7 — D4 크래시 안전 세마포어: permit=lease, 가용 = max − count(ACTIVE) — DONE
> 빌드 슬롯류 자원의 크래시 안전 배정. 정수 카운터의 고전 버그(보유자가 죽을 때마다 새서
> 결국 0=영구 정지)를 "permit=lease"로 원천 차단 — 죽은 보유자의 permit 이 EXPIRED 되면
> `가용 = max − count(ACTIVE)` 가 *구조적으로* 복구된다(누수 0). §1.2 의 세마포어 적용.

- **permit = owned+TTL+fenced LEASE**(`core.py`/`store.py`, §0.2 4프리미티브 투영):
  - `orbits.kind='sem_permit'`, `resource_key=sem_id`, fence 부여(소유+fenced). 일반 궤도(`kind='orbit'`)와
    분리돼 입체검사/promote/sweep 의 경로궤도 쿼리에 안 섞인다(기존 `kind='orbit'` 필터가 그대로 보호).
  - `semaphores` 레지스트리 테이블(`sem_id`,`max_permits`) — lease 아닌 설정. `sem_declare` 멱등 등록(max 증가 시 대기자 promote).
  - `sem_waiters` 테이블(register→poll, 서버 비블로킹) + 단조 전역 `next_seq()`(meta `seq`, fence 와 분리한 FIFO 티켓).
- **신규 동사**: `sem_declare(name,max)` · `acquire(agent,sem,ttl,no_wait,priority)` · `acquire_poll(waiter_id)` · `sem_release(permit,fence)` · `sem_status(sem)`(관측용). server/cli 동반 노출.
- **초과배정 불가(§D4)**: `acquire` 의 check-then-grant 가 `_cs()`(D1 임계구역) 안에서 원자 —
  두 acquirer 가 동시에 N-1 슬롯을 보고 둘 다 N+1번째를 부여하는 레이스 차단. `가용 = max − count_active_permits(sem)`.
- **멱등 reuse(§D9)**: 이미 ACTIVE permit 을 쥔 agent 의 재acquire 는 같은 permit 반환(재발급 안 함) — MCP 재시도 누수 0. `request_id` 멱등(`_idem`, 성공만 캐시)도 병행.
- **no-overtaking(§D7)**: `_has_earlier_waiter` — 가용 슬롯이 있어도 자기보다 먼저 줄선(우선순위 DESC → FIFO) 대기자가 있으면 양보(작은 acquire 스트림이 head 대기자를 굶기는 writer-starvation 방지). 슬롯 복구 시 `_promote_sem_waiters` 가 줄선 순서대로 부여.
- **reclaim 단일루틴 통합(§1.1, §G)**: 죽은 보유자의 sem_permit 을 `_reclaim_agent_inline` 이 EXPIRE → 슬롯 복구(bail/좀비회수 둘 다 수렴) + 복구된 세마포어 promote. `_sweep_inline` 의 `due_sem_permits` 가 TTL 만료 permit 도 같은 복구 경로(agent_ttl 없이도). 대기 중이던 agent 가 죽으면 그 대기 등록을 CANCELLED. `heartbeat` 한 번이 자기 sem_permit 도 연장(§G — 궤도/permit 비대칭 만료가 빌드 슬롯 이중배정을 부르는 것 방지).
- **fence/owner 거부(§D6)**: `sem_release` 는 `_check_owner`(owner∧fence) — 남의 permit 해제·재부여후 낡은 fence 해제 불가(P0-3 유형). 이미 RELEASED/EXPIRED 면 멱등 OK. `_check_alive`(bail_epoch)로 회수된 좀비의 acquire/release 차단.
- 스키마(additive·fresh-DB 친화 + `_SCHEMA` CREATE IF NOT EXISTS — 기존 DB도 획득): `semaphores`·`sem_waiters` 테이블, meta `seq`. orbits 컬럼 재사용(`kind`/`resource_key`/`expires_at`/`fence`) — 신규 ALTER 불필요. permit 상태 ACTIVE/RELEASED/EXPIRED 는 orbit FSM(HELD/RELEASED/EXPIRED)에 그대로 투영(FSM 변경 0).
- 테스트(**130 passed, 1 skipped**; 신규 22 = `test_d4_semaphore.py` + `gates/semaphore.yaml`):
  정상(declare/acquire·미지 sem 거부·용량 불초과·가용=count·멱등 reuse·request_id 멱등) · **초과배정 불가**(8 동시 acquire, max=3 → 정확히 3 ACTIVE) · no-overtaking(head 우선·우선순위 정렬·**빈슬롯+대기자 territory 이빨**) · fence/owner 거부(non-owner·stale-fence·멱등 release) · **크래시/사망/오추방 실패경로**(bail→슬롯복구+대기자 기상 / 좀비회수→복구 / **permit TTL 만료→복구**(agent_ttl 없이) / heartbeat 가 permit 연장 / 회수된 좀비 acquire 차단 / 대기자 사망→CANCELLED+슬롯 다음 대기자로 / 대기 TIMEOUT) · LTDD 트레이스(`sem_acquired→sem_permit_reclaimed` 순서 도착).
- **변이검증(이빨 4건 실증, 복원함)**:
  ① 용량 가드 무력화(`avail>=1` 분기 항상 참) → 초과배정·큐 점프 → **7건 RED**(용량불초과·**8-동시 초과배정**·no-overtaking·우선순위·대기자기상·대기자취소·타임아웃).
  ② 멱등 reuse 무력화(`existing` 분기 skip) → 재acquire 가 둘째 누수 permit 생성 → `test_idempotent_reuse` RED.
  ③ sem_permit reclaim 무력화(`sem_permits_owned_by`→`[]`) → 죽은 보유자가 슬롯 영구 누수(=정수 카운터 고전 버그) → **4건 RED**(bail 슬롯복구·대기자기상·좀비회수·LTDD).
  ④ no-overtaking 무력화(`_has_earlier_waiter`→False) → 빈슬롯+대기자 상태에서 새 acquire 가 head 를 가로챔 → `test_fresh_acquire_yields_to_queued_waiter` RED. 넷 다 복원 후 130 green.
- **deviation/한계(정직 표기)**:
  1. **promote 는 eager(release/sweep/declare 마다 즉시 head 부여)** — 그래서 '빈 슬롯 + 대기자' 상태가 정상 경로엔 거의 안 생기고, `_has_earlier_waiter` 가드는 *defense-in-depth*(직접 acquire 의 큐 점프 차단)다. 그 이빨은 territory 테스트(store 직접 상태 주입)로 실증. eager-promote 자체가 1차 기아 방지.
  2. **대기 deadline = ttl**(별도 wait-timeout 인자 없음) — `acquire` 의 `ttl` 이 'permit 보유 TTL'이자 '대기 타임아웃'을 겸한다. 명세(§D4 의사코드)는 `ttl`만 받으므로 따랐다. 더 세분이 필요하면 P2(별도 `wait_timeout`).
  3. **periodic sweep 없음**(증분6과 동일, §7 미해결) — sem_permit TTL 만료의 슬롯 복구는 누군가 inline-sweep 동사(acquire/acquire_poll/sem_status/sweep 등)를 호출할 때 일어난다. 대기자가 poll 하면 반드시 반영되므로 대기 측엔 영구 hang 없음. 백그라운드 tick 은 P2.
  4. **permit 은 fence 를 받지만 strict-fence 재검증 동사는 release 뿐** — acquire/reuse 는 owner 기준(같은 agent). renew 전용 동사는 안 만들고 heartbeat 로 TTL 연장(§D2 의 'heartbeat 한 번이 모든 hb_bound lease 갱신' 패턴). permit 개별 renew 가 필요하면 P2.
- **남은 부채**: P0 전부 닫힘(증분1~4). 미구현 = **D5 배리어**(세대-스탬프+BROKEN) · D12 read-set 코히런스 · D14 HA 입장 — 전부 설계만(P1/P2). (D4 = 증분7, D3 = 증분6, D6 잔여+D9 = 증분5 에서 닫힘.)

### ✅ 증분 8 — D5 크래시 안전 배리어: 세대-스탬프 응결 랑데부 + BROKEN 종단 — DONE
> 응결 랑데부(여러 task 가 한꺼번에 응결해야 하는 경우)의 크래시 안전 동기화. 좌석은 lease,
> 참가자가 도착 전(또는 도착 후) 죽으면 배리어가 **BROKEN** 되어 도착해 있던 전원이 에러로
> 기상한다(Java BrokenBarrierException / Python Barrier.abort 시맨틱). **영구 hang 불가**(§1.2/§D5).

- **배리어 = 세대-스탬프 FSM**(`fsm.py` BARRIER_STATES/TRANSITIONS): `ARMED → TRIPPING → TRIPPED
  → CONSUMED ⊕ (ARMED|TRIPPING) → BROKEN`. `advance("barrier", …)` 로 합법성 검증(orbit/task 와 동형).
- **멤버십 = task 집합(요구된 수정)**(`core.py`): agent 수가 아니라 task 집합. reclaim 으로 task 가
  requeue 되거나 write-lease 가 거둬지면(=참가자 사망) N 재계산/break/shrink. 참가자 생존 =
  write-orbit(lease) 생존(§0 모델). `_party_alive` = (a) task 없음/ABORTED, (b) HELD write-orbit 없음
  (lease 만료/해제), (c) 도착 후 fence 가 도착 시점과 달라짐(ABA) 중 하나면 사망.
- **신규 동사**: `barrier_declare(name,task_ids,kind,policy,timeout)` · `barrier_arrive(name,agent,task,fence)` ·
  `barrier_abort(name,agent)` · `barrier_status(name)`. server/cli 동반 노출.
- **응결은 내부 `_barrier_connect_one(task, expected_fence)` 로 trip — 공개 `connect()` 재호출 금지(§D5
  검증기 적발)**: 공개 connect 의 Phase A 는 `_sweep_inline` 을 부르는데, 그 sweep 가 방금 배리어가
  검증한(만료 임박) 궤도를 트립 직전 재진입 만료시켜 fenced_out 으로 깰 수 있다. 그래서 트립은
  **sweep 없는 Phase A'**(`_barrier_connect_phase_a`: write-orbit HELD ∧ fence==expected_fence 재검증 +
  write-set 감사 + merge_token 획득 + pin) + **공유 Phase B**(락밖 merge) + **Phase C**(merge_sha 기록 →
  MERGED → 해제 → 토큰반납) 를 직접 돈다. merge 는 락 밖(§3.B 한 계약 재사용).
- **break/shrink(policy)**: `break`(기본) = 참가자 사망/타임아웃 시 전원 깸. `shrink` = 죽은 멤버를
  빼고 N 재계산 후 진행 — **단 그 멤버에 의존하는 task 가 없을 때만**(`_task_dependents`); 의존자가
  있으면 shrink 금지 → break(의존자가 미응결 base 위에 빌드하는 것 차단).
- **trip 구동(§3.B 분할)**: `barrier_arrive` Phase A(락) = 도착 기록 + `_barrier_eval(can_trip=True)` →
  전원 도착이면 fill(ARMED→TRIPPING) + 트립 plan(결정적 task_id 순서) 반환. 그 후 `_barrier_trip`
  (락 밖)이 plan 의 각 task 를 `_barrier_connect_one` 으로 응결 → 전부 성공이면 TRIPPED, 하나라도
  실패면 BROKEN. **sweep/reclaim/status 의 eval 은 `can_trip=False`**(break/shrink 만; fill 하면 TRIPPING
  이 driver 없이 고아) — 그들은 사망/타임아웃 반영 전용.
- **reclaim 단일루틴 통합(§1.1/§3.D)**: `_reclaim_agent_inline` 이 회수하는 agent 의 **모든** task 가 든
  활성 배리어를 모아 재평가 — write-lease 가 이미 해제됐으므로 task 가 requeue 되든(IN_ORBIT) 안 되든
  (이미 DONE) 참가자 사망이다 → break/shrink. bail/좀비회수 둘 다 수렴.
- **타임아웃(영구 hang 방지)**: declare 의 `timeout` → deadline. deadline 지났는데 미도착 있으면
  `_barrier_eval` 이 break('timeout'). 누군가 sweep(poll/status/arrive)하면 반영되므로 대기 측 hang 0.
- **세대 재무장**: 같은 이름을 다시 declare 하면 다음 generation 으로 재무장(이전 세대가 종단일 때).
  활성(ARMED/TRIPPING) 인스턴스는 재declare 거부(이중 무장 방지). generation 스탬프가 옛 세대의 유령
  도착을 막는다(`barrier_parties` PK = (barrier_id, generation, task_id)).
- **fence/owner 거부(§D6)**: `barrier_arrive` 는 `_check_alive`(회수된 좀비 차단) + arrive 시점
  write-orbit fence capture; caller 가 `fence` 를 주면 `fence==cap` 재검증(stale=fenced_out, 도착 표시 안 함).
- 스키마(신규 테이블 — fresh-DB·기존 DB 모두 `CREATE IF NOT EXISTS` 로 획득, ALTER 불필요): `barriers`
  (name,generation,state,policy,deadline_at, UNIQUE(name,generation)) · `barrier_parties`(arrived,arrive_fence).
  store: `add/get/set_barrier`·`barrier_by_name`·`add/get/set/del_barrier_party`·`barrier_parties`·
  `barriers_with_task`·`all_barriers`.
- 테스트(**153 passed, 1 skipped**; 신규 23 = `test_d5_barrier.py` + `gates/barrier.yaml`):
  정상(declare/arm·미지 배리어·비멤버 거부·부분도착 대기·전원도착→trip→MERGED(DB-only **및** git 백엔드:
  통합에 양쪽 파일+clean index+토큰누수0)·request_id 멱등) · fence/owner 거부(stale-fence·lease 거둬짐→break·
  회수된 좀비 arrive 차단) · **크래시/사망/오추방 실패경로**(참가자 도착 **전** 사망→BROKEN+전원 기상 /
  도착 **후** 사망→BROKEN / **좀비회수**(heartbeat 끊김)→BROKEN / **타임아웃**→BROKEN / abort→BROKEN+늦은
  도착도 BROKEN) · policy(shrink 죽은멤버 제거(의존자 없을때)·shrink 의존자 있으면 break) · 세대 재무장
  (BROKEN 후 gen+1·활성 재declare 거부) · **§D5 핵심**(트립 중 `_sweep_inline` 0회 — 검증한 궤도 재진입
  만료 방지) · LTDD 트레이스(`barrier_declared→barrier_broken[participant_dead]` 순서 도착).
- **변이검증(이빨 5건 실증, 복원함)**:
  ① `_party_alive`→항상 True(사망 미검출) → 도착전/후 사망·좀비회수·shrink·LTDD **7건 RED**(영구 hang).
  ② 타임아웃 break 가드 무력화 → deadline 지나도 ARMED 유지 → `test_timeout_breaks_barrier` RED.
  ③ 트립을 공개 `connect()` 로 회귀(Phase A 가 `_sweep_inline` 부름) → `test_trip_phase_a_does_not_sweep` RED.
  ④ shrink 의존자 가드 무력화(의존자 있어도 shrink) → `test_shrink_blocked_by_dependent_breaks` RED.
  ⑤ arrive 의 `fence==cap` 거부 무력화 → stale-fence 도착이 통과 → `test_arrive_stale_fence_rejected` RED.
  다섯 다 복원 후 153 green.
- **deviation/한계(정직 표기)**:
  1. **트립 plan 의 merge 는 결정적이되 sequential**(task_id 순서로 한 번에 한 task 응결). 각 응결은
     merge_token(repo-wide max=1)으로 직렬화되므로 동시 트립이 통합 index 를 오염시키지 않는다(§D11 재사용).
     trip 도중 하나가 실패(fence stale/merge 충돌)하면 이미 응결된 것은 그대로 두고(전진) 배리어를 BROKEN
     으로 — 부분 트립 시 §3.D 처럼 생존자 rollback 까진 안 한다(이미 MERGED 는 단조 사실이라 되돌릴 수 없음;
     배리어 BROKEN 신호로 호출자에게 반쪽 적용을 알린다). 정직: "전부-아니면-전무" 원자 트립은 아니다 —
     merge 의 비가역성 때문(D8 도 개별 task 단위로 git 진실과 조정). 응결 순서 결정성 + merge_token 으로
     index 오염은 막지만, k번째에서 깨지면 1..k-1 은 응결됨.
  2. **periodic sweep 없음**(증분6/7 과 동일, §7 미해결) — 타임아웃/사망의 BROKEN 반영은 누군가
     inline-sweep 동사(barrier_arrive/barrier_status/sweep/next_task 등)를 호출할 때 일어난다. 대기 참가자가
     status/arrive 하면 반드시 반영되므로 영구 hang 없음. 백그라운드 tick 은 P2.
  3. **CONSUMED 전이는 정의만**(FSM 에 trip→consume 있음) — 결과 수거 동사는 미구현(현재 TRIPPED 가
     사실상 종단; barrier_status 로 관측). 필요해지면 P2.
  4. **§3.D 재기동 복구의 배리어-bound CONNECTING 단위 처리는 미구현** — `_recover()` 는 여전히 task 를
     개별로 git 진실과 조정한다(증분3). TRIPPING 중 크래시한 배리어를 *단위* 로(반쪽 트립→BROKEN+생존자
     rollback) 복구하는 §3.D 의 수정은 이 증분 범위 밖(트립은 단일 프로세스에 묶여 재기동을 가로지르기
     어렵고, 개별 task 복구가 안전 수렴은 함 — 단 BROKEN 신호 없이 반쪽 MERGED 가 될 수 있다는 §3.D 의
     함정은 정직히 미해결로 남긴다). P1/P2.
- **남은 부채**: P0 전부 닫힘(증분1~4). 미구현 = D12 read-set 코히런스 · D14 HA 입장 · §3.D 배리어
  재기동 단위복구 — 전부 설계만(P1/P2). (D5 = 증분8, D4 = 증분7, D3 = 증분6, D6 잔여+D9 = 증분5 에서 닫힘.)

### ✅ inc9 — D12 read-set 코히런스 + D14 코디네이터 singleton/HA 입장 (증분9)

**구현(D12 — 유령 읽기 차단).**
- **통합 generation 추적**: `meta.integration_gen` 응결 1건마다 +1(단일문, 읽고-쓰기 갭 없음).
  새 `merge_log(gen→write-globs, task)` 테이블에 그 gen 에 통합으로 들어간 write-globs 를 기록.
- **task 차원 read 동기화**: read claim 은 그 task 의 `read_synced_gen`(현 gen)을 박는다. **궤도
  생명과 분리** — read↔write 는 배타적이라 producer 가 그 영역을 쓰려면 consumer 가 read 궤도를
  release 해야 하는데, 그래도 `read_synced_gen` 이 task 에 남아 코히런스를 추적한다.
- **connect 게이트(`_ghost_reads`)**: consumer 의 connect Phase A 가 `read_synced_gen` *이후* 의
  merge_log 중 자기 선언 reads 와 `sets_overlap` 하는 게 있으면 = 옛 base 위에 조용히 빌드(머지는
  성공하되 로직 틀림) → `read_stale` 로 거부(merge_token 잡기 *전*). live read 궤도가 있으면
  `_mark_stale_reads` 가 `stale=1` + D3 `LATCH read_stale:<orbit>` 플래그/이벤트로도 신호.
- **회복(`read_refresh(task, agent, fence)`)**: rebase/재독 후 task 의 read-set 을 현 gen 으로
  재앵커(+살아있는 read 궤도 stale 해제·신호 CLEARED). connect 와 동일한 소유+fence 가드(그 task
  의 write-orbit 을 쥔 caller 만). MCP 툴 + CLI `read-refresh` 노출, `request_id` 멱등.

**구현(D14 — 단일 인스턴스 강제).**
- **DB 리더-lease**(`meta.leader_lease` JSON: coordinator_id/epoch/last_heartbeat/ttl). 기동 시
  `_acquire_leadership()` 가 `_cs`(BEGIN IMMEDIATE) 안에서 CAS 획득. 살아있는 다른 리더(heartbeat 가
  **incumbent 가 선언한 TTL** 안)면 `CoordinatorConflict` 거부 = actor 둘(=writer 둘) 차단.
- **죽은 리더 takeover**: heartbeat TTL 초과 시 epoch +1 로 fence 하며 takeover. `coordinator_heartbeat()`
  keepalive(권장 ttl/3), `resign()` graceful(즉시 takeover 허용).
- **leader-fence**: `_cs()` 가 트랜잭션 연 직후 `_assert_leader()` — takeover 된 좀비 리더(epoch/id
  불일치)의 *모든* 변이를 `CoordinatorConflict` 로 차단(획득 중에는 skip).
- **`:memory:` 디폴트 금지**: 영속 DB 필수(재기동마다 fence/leader_epoch 0 리셋→낡은 토큰 충돌).
  단위테스트만 `allow_memory_db=True` 명시 opt-in. 기존 in-memory 단위테스트(test_omd/test_d7)는
  그 플래그로 전환, 멀티프로세스 테스트(test_concurrency)는 D14 거부 계약으로 갱신.

**테스트(신규 19 — 정상+크래시/사망/오추방+fence/owner+변이검증).**
- `test_d12_read_coherence.py`(10): gen 앵커 · 유령읽기→connect 차단 · read_refresh→통과 ·
  비겹침 무차단 · read 없는 task 무관 · D3 플래그 신호 · **사망 consumer 자동 회수+부활차단** ·
  fence/owner 거부 · 멱등 · 변이검증.
- `test_d14_ha_admission.py`(9): `:memory:` 금지 · 첫 리더 획득 · **둘째 live 거부** ·
  **죽은 리더 takeover** · **takeover 된 좀비 리더 변이 차단** · heartbeat 유지 · resign 즉시 takeover ·
  좀비 heartbeat fence · 변이검증.

**변이검증(직접 수행, 복원 확인).**
- D12 connect ghost-read 가드(`_ghost_reads`/`stale_orbits`)를 `[]` 로 무력화 → 유령 읽기인데도
  consumer 가 MERGED 됨 → D12 테스트 5 RED(변이검증 테스트 포함). 복원 후 green.
- D14 admission(`alive=False` 강제, incumbent 항상 죽은 것처럼) → 둘째 코디네이터가 기동 성공
  (writer 둘) → D14 테스트 3 RED. 복원 후 green.
- D14 leader-fence(`_assert_leader` no-op) → takeover 된 좀비 리더가 변이/heartbeat 성공 → 3 RED.
  복원 후 green.
- 전체: inc8 까지 153 passed → **172 passed, 1 skipped**(신규 19 전부 포함, 기존 0 회귀).

**잔여(정직히).**
1. **D12 신호는 task 차원 게이트가 주(主), live read-궤도 stale 표시는 보조**다. read↔write 배타성
   때문에 *연속 점유* 중 live read 궤도가 producer 응결과 겹치는 일은 드물어, `_mark_stale_reads` 가
   실제로 fire 하는 주 경로는 `release_read=False`(읽기 궤도 유지) 케이스다. 정상 물방울 흐름(읽고
   release)은 `read_synced_gen` 기반 merge_log 검사가 잡는다.
2. **무효화 정책은 "전 consumer 강제 rebase"**(§7 미해결 중 강한 쪽)를 택했다 — connect 를 막아
   강제한다. "알림만"(soft) 옵션은 미구현.
3. **D14 는 단일 인스턴스 강제(거부)** 만 — 리더-lease **페일오버**(통합 머지 조정까지)는 §7 대로
   범위 밖. takeover 는 "죽은 리더 자리를 새 리더가 인계" 까지(진행중 작업의 무중단 인계 아님).
4. **leader heartbeat 자동 주기 미구현** — `coordinator_heartbeat()` 동사만 노출. 운영 시 서버가
   ttl/3 주기로 호출해야(현 server.py 는 호출 루프 없음 — 단일 프로세스 데모는 takeover 없어 무해,
   장수 HA 운영엔 주기 호출 추가 필요). 정직히 P1.
5. `_ghost_reads` 의 글로브 overlap 은 `sets_overlap`(보수적 — 거짓-양성 가능, soundness 우선).
   거짓-양성이면 불필요한 rebase 1회를 강제할 뿐 분열은 절대 안 남(안전측 실패).

### ✅ 증분 10 — P2 shared 레인: hot 공유파일 3-way 응결 — DONE

FEEDBACK P2 + 현장실측(consumer_b user~200: adoption 0%·hot 30파일, env.py/modbus.py/business_logic.py
실충돌 파일이 그대로 hot 상위) 응답. disjoint 는 그대로 1급시민 — **hot 파일만** 별도 레인.

- **선언**: `declare(task, shared=[...])` (`tasks.shared` 컬럼) — next_task 가 shared HELD 와의
  겹침은 허용(배타 write/read HELD 와 겹치면 여전히 대기).
- **궤도**: `claim(agent, paths, mode="shared")` — shared↔shared 동시 HELD 공존(직렬화 마찰 제거),
  shared↔write/read 는 여전히 충돌(배타 의미 보존). `WRITE_MODES=("write","shared")` 로 write-set
  감사(§D10)/fence(D6)/해제/배리어 경로에서 write 동급.
- **응결**: CLOUD CONNECT 의 git 3-way 가 다른 hunk 편집을 자동 병합. **같은 hunk 진짜 충돌 =
  정상사건**: `reason="shared_conflict"` + `retryable` + rebase 힌트, DONE 롤백(CONNECTING 좌초/
  경보 아님 — P3 부분 해소). shared 궤도 없는 task 의 충돌은 기존 '구조적 불가=경보' 의미론 불변.
- 가드: `tests/test_p2_shared_lane.py` 5종(공존/배타보존/automerge/shared_conflict/경보 음성컨트롤).

### ✅ 증분 11 — §3.D 배리어-bound 재기동 단위복구 + TRIPPED→CONSUMED 수거 — DONE

증분8 deviation 3·4(정직 표기 부채)를 닫는다.

- **`_barrier_recover()`** (`_recover()` 말미, task-단위 조정 *후*): TRIPPING 잔해를 단위로 조정 —
  전 멤버 MERGED(git 진실) → **TRIPPED 전진수정**; 일부만 MERGED → **BROKEN
  (`coordinator_crash_partial_trip`)** fail-loud. "BROKEN 신호 없이 반쪽 MERGED"(§3.D 함정) 폐쇄.
  MERGED 는 단조 사실로 유지(증분8 deviation 1과 동일 계약), 미응결 task 는 task-단위 복구가
  재시도 가능 상태로 되돌림. ARMED/종단 배리어는 불가침.
- **`barrier_consume(name, agent)`** (MCP 동반 노출): TRIPPED→CONSUMED 종단 + 멤버별 merge_sha
  수거. 비-TRIPPED 거부(수거할 결과 없음), CONSUMED 재호출은 멱등 noop(결과 재동봉) — 같은
  세대 이중 소비를 FSM 이 잡는다.
- 가드: `tests/test_p4_barrier_restart.py` 5종(전진수정/부분트립 BROKEN/ARMED 무해/수거+멱등/비-TRIPPED 거부).
  적합성 `barrier_restart_recovery` 를 must=True 로 승격(회귀가드).

### ✅ 증분 12 — D14 멀티프로세스 HA integration 실측 (P6 실측 공백 폐쇄) — DONE

FEEDBACK §P6 "D14 는 단일프로세스 테스트만 — 멀티프로세스/파티션 integration 실측 부재" 응답.
코드 변경 없음(측정 증분): 기존 D14 기제가 **실제 OS 프로세스 경계**에서 서는지 실측 —
`tests/test_p6_multiproc_ha.py` 3종, 실 subprocess 드라이버(stdin/stdout 1줄-응답 프로토콜):

- **INV-P6-1 admission**: 살아있는 리더 옆 2호 *프로세스* 기동 = CoordinatorConflict 거부(rc 3).
- **INV-P6-2 crash takeover**: 리더 SIGKILL(진짜 크래시, resign 없음) → TTL 경과 → 새 프로세스
  takeover, epoch 단조 +1.
- **INV-P6-3 GC-pause fence-out**(Kleppmann): 리더 SIGSTOP → TTL 경과 → takeover → SIGCONT 로
  깨어난 좀비 리더의 heartbeat/claim 전부 FENCED, 새 리더 변이 정상 — split-brain 이중쓰기 봉쇄가
  프로세스 경계를 넘어 실증. (SQLite WAL + BEGIN IMMEDIATE 의 cross-process 직렬화 확인.)

**P6 잔여의 처분(정직)**: 단일 coordinator+SQLite 는 여전히 SPOF 이나 이는 §7 결정("단일 인스턴스
강제 + `:memory:` 금지")의 *의도된* 설계 — 페일오버는 위 takeover 로 성립(수동/재기동 기반).
`transitions` 라이브러리 유지 공백 리스크는 미해소로 남는다(교체는 별도 증분).

### ⬜ 다음 증분 후보 (설계는 CONCURRENCY 완료, 구현 대기)
D13 git/FS 장애 분류 · D14 leader heartbeat 자동주기/페일오버 · periodic sweep(§7).
(P0-1~P0-11 = 증분1~4, D6 잔여+D9 = 증분5, D3 = 증분6, D4 = 증분7, D5 = 증분8, D12+D14 = 증분9,
P2 shared 레인 = 증분10, §3.D 배리어 재기동+CONSUMED = 증분11, D14 멀티프로세스 실측 = 증분12 에서 닫힘.)

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
