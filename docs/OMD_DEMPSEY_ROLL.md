# OMD 댐프시롤 (OMD Dempsey Roll) — 운영 규율 스펙

> **status: PRELIMINARY / `VerdictPending`** (2026-07-11)  
> 사용자 창작 프레임 + 5-렌즈 워크플로우 + 나생문 homology 검증 산출.  
> “댐프시롤” 명명 = 사용자 정전 후보(미확정). 실질(서로소-궤도 포화 규율) = 검증됨.  
> Longinus 바인딩 대상 = OMD MCP 서버 driver loop
> (`declare→next→start→claim→commit→finish→connect`).

## 0. 한 줄 정의

**뎀프시 위브(슬립=방어 ∧ 로드=공격이 한 동작)를 위상(topology)으로 컴파일한 극한값.**
write-set 서로소(disjointness)라는 단 하나의 불변식이, 에이전트가 격리 worktree에서
편집하는 그 한 행위를 동시에 **충돌 불가(슬립)**이자 **깨끗한 머지 장전(로드)**으로
만든다. 슬립은 타이밍을 놓칠 수 없다 — 상대도 시간축도 없고 오직 위상만 있으니까.

## 1. 정직 판정 (나생문 homology) — 최고가치 산출물

렌즈 5개, 매핑 30여 개 중 **`mechanism`(같은 인과 기작) 등급 = 0개.** 다리는 전부
`structural`/`behavioral`/`naming-only`다.

- **이름의 “궤도/관성(Orbital Motion)” = 제일 약함 = `naming-only`.**
  - 진짜 궤도: 각운동량 보존, 엔진 꺼도 영원히 관성으로 돎(Noether, 공짜).
  - OMD: 스케줄러가 매 tick 일을 편다(`next`/`heartbeat`/`sweep`). 코디네이터를
    죽이면 한 tick 만에 정지한다. driven·dissipative, NOT inertial.
  - 뎀프시 파워 = 보존 운동량(mv, 줄). OMD 파워 = 처리량(tasks/초, 회계 수치).
    **connect에서 보존·전달되는 물리량 0.**
- 결론: **“운행 자체가 무기” = 설계 강조로는 참, 물리로는 거짓.** 이 정직함이
  개념의 척추다.

## 2. 진짜 load-bearing 코어 (극한값 통찰)

뎀프시의 “슬립·로드가 한 동작”은 상대에 맞춘 **반응(reactive) 기술 → 놓칠 수
있음**(잇포 카운터 지점). OMD는 그 붕괴를 **정적 불변식**으로 굳힌다.

| | 뎀프시 위브 | OMD |
|---|---|---|
| 슬립(방어) | 흔들어 피함 (타이밍 필요) | 서로소 리스 → 형제 write **애초 착지 불가** |
| 로드(공격) | 같은 흔들림이 다음 펀치 장전 | 같은 격리 편집이 **깨끗한 머지 장전** |
| 붕괴 | 한 동작=슬립+로드 (타이밍 존재) | 한 불변식=슬립+로드 (**타이밍 없음, 위상뿐**) |

**→ OMD = 위브의 극한 케이스.** 슬립-앤-로드에서 시간을 제거해 write-set 격자에
컴파일한다. 그래서 load-bearing 불변식이 **에이전트 → 코디네이터로 이사**한다.
에이전트 정체성·개수가 인과적으로 무관한 이유(히드라의 근거)는 분산시스템 사실이지
권투 사실이 아니다.

## 3. “좌우·개수 안 중요” = 암달 법칙 유도

사용자 세 정정 = 암달 법칙의 비형식적 유도. 각 정정이 non-load-bearing 자유도 하나씩
삭제한다.

- **“좌우 안 중요”** → 커밋 순서 삭제. 서로소 write는 교환가능 →
  **confluence(Church-Rosser)**, 머지 결과 순서 무관.
- **“팔은 히드라, 개수 무관”** → effector 다중성 삭제. 포화 N* 넘으면
  S(N)=1/(f+(1−f)/N) → **1/f**. 팔은 점근적 무의미. “개수가 안 중요”는 강함이 아니라
  fence-bound 진단 신호.
- **“기본적으로 OMD”** → bolt-on 삭제.

셋 다 지우고 남는 유일 변수 = 직렬 merge-fence 서비스 속도 c. 빨라지려면 팔 늘리기가
아니라 **connect를 싸게(c↓)** 해야 한다.

## 4. 케플러 규칙 — 물리에서 건진 유일한 것 (설계 지침)

- **근일점 = connect** — 빠르고 짧고 단독 점유(fence).
- **원일점 = 격리 편집** — 느리고 길고 다중 점유(전원 병렬).

> **원일점 호 : 근일점 체류 = 병렬 가속 천장(암달 상한).**  
> connect가 살찌면 = 물방울이 길어진 근일점에 줄서기 = 궤도 정지.

**규칙: 편집(원일점) 길게·격리 가능하게, connect(근일점) 얇게·빠르게·fence로.**

## 5. 어떻게 카운터당하나 + 방어

**fence는 펀치 착지점 ∧ 예측된 나딜(카운터 지점) — 같은 구조.** 방어 = 힘 추가가
아니라 **나딜 체류를 0으로** 만드는 것이다.

1. **자기충돌** — 물방울 동시 종료 → connect로 쇄도 → 최대 부하 직렬화 → 붕괴.  
   방어: **perihelion 스태거(엇갈려 착지) + connect O(1) 유지.**
2. **fence에서 일하기** — connect가 실제 머지 충돌 해결 = 없던 카운터 창 자작.  
   철칙: **DO NO WORK IN THE FENCE.** connect=검증된 fast-forward만, 모든 충돌은
   상류 declare에서 예방.
3. **보이지 않는 카운터 (최대 맹점, 권투엔 없음)** — **서로소 write-set은 바이트 합성은
   보장하지만 의미 합성은 절대 보장하지 않는다.** 텍스트상 안 겹치는 파일도 합쳐
   컴파일이 깨질 수 있다. fence를 완벽 통과하고 하류에서 터진다.  
   유일 방어: **Contract dual**(재배맨 complement 정전: “병렬분해는 인터페이스 계약 없이
   compose 불가”) **+ Longinus 바인딩으로 경계면 커플링 표면화.**
4. deps 순환 데드락 / sweep friendly-fire(느린≠죽음) = 일반 동시성 병리
   (`naming-only`). 방어: DAG 검증 / fencing-token(epoch) — 좀비 stale write는 fence에서
   거부 → 히드라 재생을 안전하게 만듦.

## 6. 실제로 던지는 법 (operator, 실 verb)

```text
0. about()                          # 먼저 정의할 것 없음. 오리엔트만.
── FAN-OUT (한 번) ──
1. 변경을 pairwise-disjoint write-set으로 분할.
   각 단위: declare(task, writes=[globs], deps=[...])
   진짜 hot 파일만 shared=[globs] (P2 레인, 3-way 머지+retry 감수)
── PER DROPLET (무제한 pull-pool, 전원 병렬) ──
2. next(agent) → 서로소 READY task
3. start(task, agent) → 격리 worktree
4. claim(agent, paths, task) → HELD=go / PENDING=declare 비서로소 / DENIED=데드락
5. heartbeat(agent, ttl) 1회       # slow≠dead 선언
6. [원일점] worktree 안에서만 편집 — 길게·독립적·lock-free
7. commit(task, msg) → 8. finish(task)
── PERIHELION (짧게 + 스태거) ──
9. connect(task, fence=…) → 착지. fence 직렬화라 구조상 절대 contend 안 함.
   유일한 일: 절대 IDLE 없이(항상 DONE 대기) + 절대 JAM 없이(9를 엇갈려)
── LIVENESS ──
10. sweep()/bail() — 죽은 팔 리스 회수→requeue→next 재-pull (= 히드라 재생)
── BARRIER (예외만) ──
11. barrier_* 는 진짜 원자적 랑데부일 때만. 평소 롤에서는 안티-이디엄.
```

## 한 문장 재요약

**OMD 댐프시롤 = “슬립=로드”를 write-set 서로소라는 위상 불변식으로 컴파일해 타이밍을
제거한 위브의 극한값. 힘은 궤도 관성이 아니라(그건 이름값) 그 불변식이 만드는
confluence·hydra·Amdahl-포화에 있고, 유일 병목이자 유일 카운터는 직렬 merge-fence다 —
규칙은 “fence에서 일하지 마라, perihelion을 엇갈려라, 계약으로 의미의 합성을 지켜라.”**

## OPEN / 다음 verdict 대기

- [x] 영구 위치 후보를 OMD 저장소에 보존 — 문서 위치는 확정하되 정전성은 미확정.
- [ ] “댐프시롤” 명명 `CANONICAL` 격상 여부.
- [ ] Contract dual(#5-3) enforcement를 OMD declare-time 게이트로 배선할지.

