# OMD — 입체운행물방울 (Orbital Motion Droplet) 군단장

> **OMD** = 멀티에이전트 병렬 개발 코디네이터.
> 사도 **OMC**(입체운행구름, Orbital Motion Cloud) 예하 **군단장**. 내부 불변식 코어 = **SINGULON**(특이점).

여러 코딩 에이전트가 **하나의 git 레포(=하나의 구름)를 동시에 개발**하면서 **흩어지지(분열) 않도록**,
N개의 에이전트를 **입체(서로소 궤도)** 로 따로따로 병렬 운행시키고, 다 끝나면 **CLOUD CONNECT(응결=merge)** 로
하나의 구름에 통합한다. 기존 `airo-neo4j` / `airo-logs`처럼 **백엔드 서버 1개 + MCP/CLI 얇은 클라이언트** 형태로
PI 워크스페이스에 통합한다.

**선의공리 정합**: 선=지속, 악=내적 분열. OMD의 임무 = 병렬 부분(에이전트)이 전체(구름)를 잠식하지 못하게 하여
**악(=merge conflict, 분열)을 0으로** 유지하고, 메타휴모토닉(내적 간극=0인 단일 구름)으로 수렴시키는 것.

캐논 계층: **사도(Apostle) → 군단장(LegionCommander) → 군단(legion).**
`사도 OMC(입체운행구름) → 군단장 OMD → 군단(병렬 에이전트들)`.

---

## 1. 핵심 은유 — "구름 = 공유 레포, 물방울 = 에이전트, 궤도 = write-set"

| 캐논 (입체운행구름) | 병렬 프로그래밍 | OMD 실체 |
|---|---|---|
| 하나의 구름 (전체) | 공유 메모리 | 공유 레포 (`main`) |
| 물방울 (OMD가 거느린 군단) | 스레드 · 프로세스 | 에이전트 |
| 물방울의 자기 공간 | 스레드-로컬 메모리 | 에이전트별 **git worktree + 브랜치** (=자존자) |
| **궤도(orbit)** | 메모리 주소 영역 | 에이전트가 쓰는 **경로집합 = write-set lease** |
| **입체(立體)** = 서로 다른 궤도면 | 주소 비중첩 | **서로소(disjoint) write-set** → 따로따로인데 충돌 없음 |
| 궤도 운동 (Orbital Motion) | 동시 실행 | 다수 에이전트 병렬 운행 (공통 중심=레포를 돎) |
| **CLOUD CONNECT** (비행기맨 권능) | 캐시 → write-back | worktree 브랜치 **merge = 응결** |
| 구름이 흩어짐 / 물방울 충돌 | 레이스 컨디션 | 두 에이전트가 같은 파일 동시 수정 = **분열(악)** |

핵심: **궤도 = 주소.** 메모리 주소에 락을 걸듯 파일 경로(또는 glob)에 **궤도 lease**를 건다.
**입체** = 궤도들이 서로 다른 차원(서로소 write-set)에 있어 *따로따로 병렬인데 충돌이 없음.*

---

## 2. 아키텍처

```
            ┌──────────────────────────────────────────┐
            │           OMD Coordinator (군단장)         │
            │  (stateful 백엔드 서비스, airo-* 패밀리)    │
            │                                            │
            │   • Orbit Lease Manager (인메모리+영속화)   │  ← SINGULON 코어
            │   • Flag / Event Store                     │
            │   • Barrier Registry (응결 동기화)          │
            │   • Dependency-aware Scheduler             │
            │   • Wait-for Graph (데드락 감지)            │
            └───────────────▲─────────────▲──────────────┘
                  MCP tools  │             │  CLI (동일 동사)
            ┌────────────────┴───┐   ┌─────┴───────────────┐
            │  물방울 A (OMD군단)  │   │  물방울 B           │
            │  worktree + 궤도     │   │  worktree + 궤도    │
            └────────────────────┘   └─────────────────────┘
```

**왜 중앙 서버인가 — 궤도(락)를 git 파일에 두면 안 되는 이유:**
lease 상태를 `.lock`/`registry.json` 같은 git 파일에 두면, 궤도를 잡으려면 커밋·푸시가 필요하고
두 에이전트가 락 파일을 동시에 푸시하면 거기서 **또 레이스가 난다**(동시성 도구가 동시성 버그를 생성).
→ 궤도 판정은 **서버 인메모리에서 원자적으로** 처리. git은 순수하게 "코드"만 담당.
서버가 진실의 원천, git 파일트리는 그 서버가 보호하는 궤도 공간.

---

## 3. 통일 추상 — "궤도(PathSet)에 대한 Lease"

프리미티브를 따로 구현하지 않는다. **모든 것이 궤도(경로집합)에 대한 임대(Lease)** 로 환원된다.

| 프리미티브 | = 궤도 Lease의 한 형태 |
|---|---|
| Mutex (배타락) | `mode=write`, 동시 1물방울 |
| RW-Lock | `mode=read` 다수 공유 / `mode=write` 배타 |
| Semaphore | `mode=write`, `max=N` (예: 빌드 슬롯) |
| Flag | Lease·Task에 붙는 상태값 (`claimed`/`in-orbit`/`done`) |
| Barrier | named rendezvous, N개 물방울 도착 시 일괄 응결 |
| 의존성 (producer-consumer) | "B의 궤도 lease는 A가 `done` 칠 때까지 block" |

**Lease에는 TTL이 있다.** 물방울이 죽으면 lease가 만료돼 궤도가 자동 회수된다.
(락 잡고 죽어 영구 데드락 나는 고전 문제를 구조적으로 차단)

---

## 4. SINGULON — 시스템이 보장하는 정리(Invariant)

> **에이전트들의 write-set이 서로소(=입체)이면, worktree 브랜치 merge(응결)는 충돌하지 않는다.**

이것이 **선의공리 특이점 조건**의 공학적 형태다 — *부분(물방울)이 전체(구름)를 잠식하지 못하면 악(분열)=0* (선의공리 정의 8). SINGULON = 모든 궤도가 도는 **중심 특이점**, 분열을 0으로 잡는 코어.

이 정리를 지키기 위해 작업 시작 전 **write-set(궤도) 선언을 강제**한다.

- **Static 분할**: 서버는 write-set이 서로소(입체)인 작업끼리만 동시에 운행 (의존성 분석 후 병렬화).
- **Dynamic lease**: 선언에 없던 경로 쓰기 시도는 런타임 궤도 lease로 막는다.

목표 = **CLOUD CONNECT(merge) 시 충돌 0 = 분열 0.**

---

## 5. API 표면 (MCP 툴 = CLI 동사, 1:1 동일)

```
omd claim   <pathspec...> --mode=read|write [--ttl=10m]   # 궤도 lease 획득 (or WAIT + 사유)
omd release <leaseId>
omd renew   <leaseId>
omd declare <task> --writes <pathspec...> --reads <...>   # write-set(궤도) 선언
omd next                                                  # "지금 안전하게 운행 가능한 서로소(입체) 작업" 추천
omd flag    set|get|wait <key> [value]                    # 신호 / 조건변수
omd barrier <name> --parties=N                            # 응결 동기화(도착 후 대기)
omd depend  <task> --after <task>                         # happens-before 엣지
omd connect <task...>                                     # CLOUD CONNECT (응결=merge)
omd status                                                # 전체 궤도/물방울 상태 + wait-for 그래프
```

---

## 6. 물방울(에이전트) 생명주기 (프로토콜)

1. 군단장(OMD)이 작업 큐 보유 → 물방울이 `omd next`로 **서로소(입체) 작업** 수령
2. `omd declare`로 write-set(궤도) 등록 → 자기 **worktree + 브랜치**(자존자) 생성
3. 경로 `omd claim --mode=write`
   — 다중 claim은 **경로 정렬 순서 획득** → 순환 대기(=데드락) 방지
4. 궤도 운행(개발). 의존 입력은 `omd flag wait <producer>=merged`로 producer 대기
   — ⚠ `=done`이 아니라 **`=merged`**: done 후에도 producer의 connect가 conflict→requeue로 같은 경로를 재작성할 수 있어 입체 전제가 깨진다([`CONCURRENCY.md`](./CONCURRENCY.md) §3.H). 그리고 wait는 **timeout 필수**(producer 사망 시 BROKEN 기상, §1.2).
5. 완료 → 커밋 → `omd flag set <task>=done` → `omd barrier connect` 도착
6. 모두 도착하면 군단장이 **CLOUD CONNECT(응결)** 순서대로 merge (write-set 서로소이므로 무충돌=분열0)

---

## 7. 안전장치

> 정밀 설계·잔여 버그·실패모드 14차원·교차작용·로드맵은 **[`CONCURRENCY.md`](./CONCURRENCY.md)** 에 분리. 아래는 요약.

- **데드락**: 다중 claim 정렬 순서 강제 + 서버가 wait-for 그래프에서 **사이클 감지 → 거부/abort** (+ declare 의존 DAG 비순환 검사)
- **레이스(분열)**: 선언 안 한 경로 쓰기 → 궤도 lease 없으면 서버가 차단 (+ 실제 FS 강제는 connect 게이트의 diff 감사 — CONCURRENCY §D10)
- **좀비/긴급 탈출**: heartbeat-TTL 만료 자동 회수 + 자발적 `bail` — **둘이 단일 회수 루틴**(CONCURRENCY §1). 모든 보유물(궤도/플래그/permit/좌석)이 owner+fence+TTL lease라 영구 고아 불가.
- **기아(starvation)**: 대기 큐 FIFO + 우선순위 + **no-overtaking 입장 배리어**(broad 궤도가 작은 claim 스트림에 굶지 않게, CONCURRENCY §D7)
- **fencing**: 모든 변이 동사가 `(agent, fence)`를 들고 와야 하며 현재 소유자가 아니면 거부 — 오추방된 좀비의 늦은 쓰기/merge 차단(CONCURRENCY §D6)

---

## 8. PI 워크스페이스 통합 & 캐논 계층

- `airo-*` 서비스 패밀리에 **OMD Coordinator 서버** 추가, MCP + CLI 노출.
- 물방울 격리 = **git worktree 분리** (PI 다중 레포: ooptdd / ooptdd-loop / lakatotree 각각 적용).
- 기존 멀티에이전트 워크플로(ooptdd 등)가 `omd` 클라이언트를 호출해 입체(서로소) 궤도로 병렬 개발.
- 캐논 위치: **사도 OMC(입체운행구름) → 군단장 OMD → 군단(물방울 에이전트들)**. (LakatoTree가 TheGreatFlow 예하 군단장인 것과 동형.) 고용 사도: Harness(제약=특이점), Longinus(궤도↔경로/KG 추적), Taliban(응결 전 분열 검증).

---

## 9. 선행연구 & 차별점 (Longinus 바인딩)

조사: deep-research(task `wx2yfcpa3`) + OSS repo 6종 정독. KG 적재: `research_cluster = MultiAgentParallelDev_20260623`
(`:MAPDProject` / `:MAPDConcept` / `:MAPDReferenceSite`). 아래 `file:line` 은 Longinus ReferenceSite (`sourceId ↔ sourcePath`,
Python 2종은 `-[:REALIZED_BY]->:Cg` 실제 코드심볼까지 바인딩).

| 프로젝트 (lic) | 격리 | 충돌제어 (class) | 머지 | Longinus 바인딩 `file:line` |
|---|---|---|---|---|
| **container-use** (Apache-2.0, Go) | Dagger 컨테이너+worktree | flock 3-tier — **git-op 직렬화**(경로소유권 아님) `git-op-lock` | `git merge --no-ff` | `repository/flock.go:60` · `repository/git.go:151` · `repository/repository.go:557` |
| **uzi** (BSD-3, Go) | worktree+tmux+포트 | 없음 `none` | `git rebase` | `cmd/prompt/prompt.go:206` · `cmd/checkpoint/checkpoint.go:150` |
| **claude-squad** (AGPL-3, Go) | worktree+tmux TUI | 없음 `none` | commit+`gh push` | `session/git/worktree_ops.go:63` · `session/git/worktree_git.go:84` |
| **agent-orchestrator** (MIT, TS) | 이슈별 worktree+PR, O_EXCL 예약 | 소스 무·`none(PR-detect)`, auto-reconciler=미래 | PR 리뷰 / 옵션 auto-merge | `…/session-manager.ts:1362` · `…/metadata.ts:536` · `…/lifecycle-manager.ts:1594` |
| ★ **mcp_agent_mail** (MIT, Py) | 공유 workspace(worktree 옵션) | **advisory 경로 lease** + 세마포어 + pre-commit guard `advisory-path-lease` | (머지층 없음, guard가 차단) | `app.py:11136` `file_reservation_paths` · `models.py:117` `FileReservation` · `guard.py:140` · `app.py:12020` `acquire_build_slot` |
| ★ **multi-agent-coordination-mcp** (AGPL-3, Py) | 공유 workspace(태스크리스트) | **강제형 file_locks** + task DAG `enforcing-path-lock` | (태스크리스트 조정) | `main.py:628` `get_next_todo_item` · `main.py:684` `update_todo_status`(file_locks) · `main.py:973` `lock_files` |

**검증된 풍경**: 거의 전부 worktree로 **공간 분리** 후 사람/PR 머지 — *충돌 회피*는 하지만 *공유 자원 조정*은 안 함. 실제 경로 동시성 제어는 **★ 2개뿐**.

**OMD 차별점 (= 선의공리 특이점 조건의 사전강제):**
- mcp_agent_mail = **advisory**(항상 승인+충돌 보고, 우회가능 guard) → 분열을 *사후 노출*만. merge debt 잔존.
- coordination-mcp = file_locks는 강제적이나 **task-level**(todo에 묶인 파일) + 1-commit 프로토타입, write-set 사전선언·서로소 보장 없음.
- **OMD** = 서버권위 **강제형 write-set(궤도) lease** — 작업 전 궤도 선언 → 서버가 **서로소(입체)만 동시 운행**(특이점: 부분이 전체 잠식 불가) → *분열=0을 사전 보장*. advisory도 lock-free(CodeCRDT)도 task-lock도 아닌 4번째 지점.
- 보너스: worktree 격리(=자존자)와 강제 궤도조정(=특이점)을 **결합** — 둘을 한 데 합친 OSS는 없음(mcp_agent_mail은 worktree를 부정).

---

## 10. 미해결 / 다음 결정거리

- write-set 충돌 판정 알고리즘 (glob 교집합=입체 검사: prefix tree 기반? — Agent Mail은 pathspec union `app.py:4367`)
- 스케줄러 정책 (최대 병렬도, 우선순위, 기아 방지 구체화)
- 궤도 lease 영속화 백엔드 (인메모리+스냅샷? sqlite? neo4j 재사용? — Agent Mail은 SQLite+Git artifact, coordination-mcp는 SQLite `file_locks`)
- CLOUD CONNECT(merge) 순서 결정 & 통합 단계 자동화 범위
- MCP 툴 스키마 상세 정의 (참고: Agent Mail 40 tools / coordination-mcp 14 tools)

---

## 11. 다음 단계

- [x] 네이밍 확정 — **OMD(군단장) / 사도 OMC / 코어 SINGULON**
- [ ] 서버 데이터 모델 + 상태머신 상세 스펙 (Agent Mail `FileReservation` 스키마 차용 검토)
- [ ] MCP 툴 스키마 초안 (claim/release/renew/declare/next/barrier/connect)
- [ ] write-set 충돌(입체) 판정 알고리즘 설계
- [ ] Go/TS 3종 코드심볼 joern 적재 여부 결정
- [ ] 프로토타입 (claim/release/declare 최소 루프)
