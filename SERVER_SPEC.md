# OMD Server — 데이터 모델 & 상태머신 스펙

OMD 군단장(코디네이터) 백엔드의 데이터 모델과 상태머신. 코어 불변식 = **SINGULON**
(서로소=입체 write-set ⇒ 무충돌 응결 ⇒ 분열=0). [`CONCEPT.md`](./CONCEPT.md) 참조.

> 본 문서는 *정상 경로* 데이터모델·FSM. 동시 호출·긴급 탈출·크래시·분단의 **비정상 경로 정밀 설계**(통합 lease 기반·전 동사 fencing·split-phase connect·크래시 복구·스키마/FSM/verb 델타)는 **[`CONCURRENCY.md`](./CONCURRENCY.md)** 에 있다. 아래 모델은 그 경화 설계의 출발점이며, CONCURRENCY가 컬럼/상태/동사를 확장한다.

---

## 1. 데이터 모델 (엔티티)

`P`=영속화(crash 복구 대상), `D`=파생(인메모리 계산).

### Cloud (Run) — OMC 인스턴스 `P`
한 번의 병렬 개발 실행 전체. = 하나의 구름.
```
cloud_id, repo, base_commit, state, created_at,
integration_branch, max_parallel (동시 운행 상한)
```

### Task — 작업 단위 `P`
```
task_id, cloud_id, name, spec(설명),
write_set: glob[],  read_set: glob[],     # 선언된 궤도
depends_on: task_id[],
state, agent_id?, worktree?, branch?, priority,
created_at, claimed_at?, done_at?, merged_at?
```

### Orbit (Lease) — 경로집합 임대 `P`
시스템의 핵심. = 물방울의 궤도.
```
orbit_id, cloud_id, task_id, agent_id,
pathspec: glob[],  mode: read|write,
state, ttl_seconds, expires_at, granted_at?, renewed_at?, released_at?,
reason
```

### Agent (물방울) — `P`
```
agent_id, cloud_id, name, program, model,
worktree, branch, state, current_task_id?,
last_heartbeat, spawned_at
```

### Flag / Event — 신호 `P`
```
key, value, set_by(agent_id), set_at        # 예: "task42"="done"
```

### Barrier — 응결/동기화 지점 `P`
```
barrier_id, cloud_id, name, parties(N),
arrived: agent_id[], state
```

### WaitForGraph — 데드락 감지용 `D`
PENDING orbit → 그 경로를 HELD 중인 orbit 들로 엣지. 사이클 = 데드락.
(저장 안 함; orbit 테이블에서 매 grant 시도 시 계산.)

---

## 2. 상태머신

### 2.1 Orbit (Lease) FSM — 가장 중요

```
                    request (disjoint?)
   (new) ───────────────┬───────────────► HELD ──renew──► HELD
                        │ 충돌                │
                        ▼                     ├── release ──► RELEASED
                     PENDING ──grant(충돌해소)─┘                │
                        │                     └── expire(TTL) ─► EXPIRED
                  --no-wait │ or queue-timeout                  │
                        ▼                          (RELEASED/EXPIRED 시
                     DENIED                         PENDING 큐 재평가)
```

| 상태 | 의미 |
|---|---|
| `PENDING` | 요청됨, 충돌로 대기 큐에 있음 |
| `HELD` | 궤도 점유 중(활성 lease) |
| `RELEASED` | 정상 반납 |
| `EXPIRED` | TTL 만료 → 자동 회수 |
| `DENIED` | `--no-wait`거나 큐 타임아웃 |

| 이벤트 | 전이 | Guard | Side-effect |
|---|---|---|---|
| `request` | new→HELD | **disjoint(pathspec, mode) vs 모든 HELD** | expires_at=now+ttl |
| `request` | new→PENDING | 충돌 존재 & wait 허용 | wait-for 엣지 추가; **사이클이면 DENIED** |
| `grant` | PENDING→HELD | 충돌 해소됨(선행 release/expire) | FIFO+우선순위로 승격 |
| `renew` | HELD→HELD | 소유 agent 일치 | expires_at 갱신 |
| `release` | HELD→RELEASED | 소유 agent 일치 | **PENDING 큐 재평가** |
| `expire` | HELD→EXPIRED | now ≥ expires_at & renew 없음 | 회수 + 큐 재평가 + agent ZOMBIE 의심 |

> **disjoint 규칙**: write↔write 겹침 = 충돌. write↔read 겹침 = 충돌. read↔read = 공존 OK.
> 겹침 = glob 교집합(§4). 이 Guard가 SINGULON 불변식의 실집행 지점.

### 2.2 Task FSM

```
PENDING ──deps done & 배정가능──► READY ──claim──► CLAIMED ──start──► IN_ORBIT
   ▲                                                                     │ finish(commit+flag done)
   │ requeue                                                             ▼
ABORTED ◄── abort/timeout ── (any) ── connect ── DONE ──connect──► CONNECTING ──merged──► MERGED
   │                                                                     │ conflict(불변식 깨짐=버그)
   └─ BLOCKED ◄── dep 미완 ── PENDING                                     └─► ABORTED→requeue
```

| 상태 | 의미 |
|---|---|
| `PENDING` | 큐에 있음 |
| `BLOCKED` | 의존 task 미완 |
| `READY` | deps 충족 + write_set 서로소 배정 가능 |
| `CLAIMED` | agent 배정됨, worktree 준비 중 |
| `IN_ORBIT` | 개발 진행(궤도 HELD) |
| `DONE` | 커밋 완료, 응결 대기 |
| `CONNECTING` | CLOUD CONNECT(merge) 진행 |
| `MERGED` | 통합 완료(궤도 release) |
| `ABORTED` | 실패/타임아웃 → requeue 가능 |

| 이벤트 | 전이 | Guard |
|---|---|---|
| `evaluate` | PENDING→READY | deps 전부 MERGED/DONE **&&** write_set이 활성 HELD와 서로소 배정 가능 |
| `evaluate` | PENDING→BLOCKED | deps 미완 |
| `claim` | READY→CLAIMED | agent IDLE |
| `start` | CLAIMED→IN_ORBIT | worktree 생성 + write_set 궤도 전부 HELD |
| `finish` | IN_ORBIT→DONE | 커밋 존재 + `flag <task>=done` |
| `connect` | DONE→CONNECTING | 응결 순서 도달(barrier/스케줄) |
| `merged` | CONNECTING→MERGED | merge 무충돌 | 
| `conflict` | CONNECTING→ABORTED | (발생 시 = 불변식 위반 = 버그 경보) |
| `abort` | any→ABORTED | 타임아웃/명시 abort → orbit 회수 후 requeue |

### 2.3 Agent (물방울) FSM
```
SPAWNED → IDLE → ASSIGNED → WORKING → IDLE … → RETIRED
                                  │ heartbeat_timeout
                                  ▼
                               ZOMBIE → (orbit 회수 + task requeue) → RETIRED
```
heartbeat가 TTL 내 안 오면 ZOMBIE → 보유 orbit 전부 EXPIRED 처리 + 해당 task ABORTED→requeue.

### 2.4 Barrier FSM
```
ARMED ──arrive×N──► TRIPPED ──(대기자 일괄 해제)──► CONSUMED
```
`arrive(agent)`: arrived++; `len(arrived)==parties` → TRIPPED.

---

## 3. SINGULON 불변식 강제 지점 (2곳)

1. **Task READY 평가** (static): write_set이 활성 HELD orbit들과 서로소일 때만 READY → 동시 운행하는 task들은 항상 서로소(입체).
2. **Orbit grant** (dynamic): 선언 안 한 경로의 write 시도는 HELD orbit 없음 → 거부.

둘 다 통과하면 **CONNECTING→MERGED 시 충돌이 구조적으로 불가능**. `conflict` 전이가 실제로 발생하면 = 불변식 구현 버그 → 경보.

---

## 4. glob 교집합(입체 판정) & 영속화

- **교집합(입체 판정)**: `omd_server/disjoint.py` 구현 완료 — 세그먼트('/')별 패턴-교집합. `**`=0+세그먼트 흡수, `*`=세그먼트내 0+문자, `?`=1문자, 문자클래스 `[...]`는 보수적 True. **soundness 우선**(false-negative 0). 예: `src/*.py` ∩ `src/auth/**` = ∅(정확히 서로소), `src/**` ∩ `src/auth/x.py` = 충돌.
- **영속화**: 모든 `P` 엔티티를 SQLite(또는 Postgres) state 컬럼으로. crash 복구 = 재기동 시 HELD/IN_ORBIT 재구성 + 만료 sweeper 재가동.
- **만료 sweeper**: 주기적으로 `now ≥ expires_at` HELD orbit → expire. (Agent Mail `FILE_RESERVATIONS_CLEANUP_INTERVAL` 방식.)

---

## 5. 상태머신/lease OSS — 검증 결과 & OMD 적합도

> deep-research(task `wlgj8e126`, 23 소스·25 클레임 검증, 23 confirmed). **결론: OMD 본분(강제형 glob write-set lease+분열0)을 통째로 주는 시판 도구는 없다.** 아래는 OMD 적합도 verdict.

### A. FSM/statechart 라이브러리 (Orbit/Task/Agent/Barrier 4 FSM 구현)
| 라이브러리 | lic / 활동 | 핵심 | OMD verdict |
|---|---|---|---|
| **`transitions`** (pytransitions, Py) | MIT, ~6.5k★, 유지 중(2025-09 push, ~9개월 공백) | HierarchicalMachine(중첩)+AsyncMachine(async)+pickle 스냅샷 | **★ 채택.** 단 pickle은 *전체 머신 거친 스냅샷*(트랜잭션 로그 아님), `add_state_features`면 pickle 불가, AsyncMachine `queued=True` 권장 → **state는 DB 컬럼+전이로그로 별도 영속화** |
| `qmuntal/stateless` (Go) | BSD-2, v1.8.0(2026-02) | `NewStateMachineWithExternalStorage`로 DB-row 외부영속 | 깔끔하나 **Go = 스택 불일치** |
| `python-statemachine` | — | (SCXML·내장영속 주장 **반증됨 0-3**) | current_state 외부 영속 직접 해야 |
| `sismic`(SCXML)·`automat` / XState(TS)·Spring(Java) | — | statechart 정식/시각화 | 스택 불일치 or 과함 |

### B. Durable workflow 엔진 — OMD엔 대체로 과함
- **Temporal** = OMD엔 **오버킬**(외부 서버·운영비용).
- **DBOS Transact**(Py) ★ = 더 가벼운 stack-정합 대안 — **Postgres 체크포인트**(워크플로가 재기동 시 마지막 완료 스텝부터 자동 재개), 인프라=Postgres만. 단 복구 코디네이터 *Conductor는 완전 OSS 아님*.
- **판정: durable 엔진은 선택사항.** DB-backed FSM로 충분 → 처음엔 도입 안 함. 장기 크래시-내성이 정말 필요해지면 DBOS.

### C. Lease/TTL substrate (Orbit lease 백엔드)
- **etcd lease** ★ — TTL grant + `LeaseKeepAlive` 스트림 renew + 미갱신 시 자동만료(붙은 키 삭제) → **좀비/lease 회수에 이상적**. 단 **완전한 락 아님**: 물리시간 TTL을 서버·클라 양쪽 시계로 재 → 서버가 revoke했는데 클라가 점유 주장 가능 → 상호배제는 **revision 번호(=fencing token) 검증(Txn)** 필요(etcd 문서가 Kleppmann 인용).
- Consul session = 유사 TTL이나 무효화가 **하한**(최대 ~2배 지연). ZooKeeper ephemeral, Redis. **전부 single-key — glob/range overlap 없음.**
- **자체 SQLite `expires_ts`+sweeper**(Agent Mail 방식) = 가장 단순, 동일 hazard.

### ⚠ 정합성(fencing) — 모든 순수 TTL lease의 함정
GC pause/네트워크 지연으로 **lease가 조용히 만료된 뒤에도 클라가 작업**할 수 있음(Kleppmann; HBase 버그). 해법 = **단조증가 fencing token**을 획득 시 발급, **저장소 측에서 옛 토큰 거부**. Redlock은 fencing 없음 → 정합성-임계 락에 부적합.
→ **OMD 적용**: 각 Orbit에 fencing token(획득 revision) 부여. **CLOUD CONNECT(merge) 게이트가 fencing 집행 지점** — 작업 중 lease가 만료됐던 물방울의 응결은 *현재 유효 holder가 아니면 거부*. (lease-grant 시점 disjoint 강제 + merge 시점 fencing 재검증 = 이중 안전.)

### ✅ GAP 확정 — OMD가 직접 만들어야 하는 것
시판 락/lease는 전부 **single-key(혹은 단순 prefix) TTL**뿐. **선언적 glob-overlap / write-set 교집합 leasing을 네이티브로 주는 건 없음**(etcd Txn=per-key, Spanner serializable 포함). 가장 가까운 선행 = **PostgreSQL SSI predicate locking**(SIREAD 인덱스-레인지 락 + granularity 승격 ≈ glob write-set 합치기) — 단 **사후 abort(SQLSTATE 40001 재시도)**이고 *접근기반*이지 *선언적-glob*이 아님.
→ **OMD가 자체 구현**: ① glob 교집합(입체 판정) ② wait-for 그래프(데드락) ③ 서로소 스케줄링. 참조 모델 = PG SSI predicate lock(단 OMD는 *사전 강제*로 변형).

### 추천 OMD 스택 (검증 반영)
1. **FSM** = `transitions`(HSM+Async) 4개 — **state는 SQLite 컬럼+전이로그로 별도 영속**(pickle 스냅샷 의존 금지).
2. **Orbit lease** = **SQLite `expires_ts`+sweeper로 시작**(Agent Mail 검증), 운영 확장 시 etcd lease로 승격. **+ fencing token(merge 게이트 집행).**
3. **영속화** = SQLite(→ 필요 시 Postgres) state 컬럼.
4. **durable 엔진 없음**(초기) — 필요해지면 DBOS Transact.
5. **자체 빌드(핵심 IP)** = glob-교집합·wait-for·서로소 스케줄러. (시판 없음 = OMD 차별점의 근거.)
