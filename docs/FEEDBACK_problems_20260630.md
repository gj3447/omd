# OMD 현재 문제점 피드백 (2026-06-30)

> operator 요청 "현재 omd 문제점 피드백". 실증 기반(코드+설계문서+실채택 조사 + 28-에이전트 audit).
> 계기: 같은 날 consumer_b consumer_b 가 **OMD 를 우회**해 공유 `user` 직접커밋 → 로컬 +17 divergence 발생.
> ⚠️ 일부 audit 주장 교정: "auto_push 미테스트"는 **틀림** — `tests/test_auto_push.py` 3 passed (audit가 venv 없어 실행 못 함). 본 문서의 P/등급은 실행검증 반영본.

---

## 🔴 P1 — 채택 안 됨 = 보장이 사실상 무의미 (가장 큰 실문제)

OMD 의 "분열=0 사전보장"은 **에이전트가 opt-in 해야만** 성립한다(자발적·advisory). 그런데 **우회 감지·차단 메커니즘이 0** 이다.

**실증 (오늘 consumer_b)**:
- consumer_b/consumer_a 코드·워크플로에 OMD 호출 **0건**, OMD worktree 디렉토리(`*-omd-worktrees`/`*-omd-integration`) **0개** → 작업이 공유 dir 직행.
- `omd_mcp.db` = **6 task·Jun 28 에서 멈춤** → 오늘 daily 작업은 OMD 안 탐.
- 결과: 세션들이 공유 `user` 직접커밋 → 로컬 +17 미푸시 divergence(우리가 손으로 정리).
- CONCEPT §1 "merge 충돌=0 사전보장" 은 **협력적 세계(선의)** 가정. 한 명만 우회해도 깨짐.

**왜 중요**: *코드는 옳은데 아무도 안 쓴다.* OMD 의 모든 가치가 "에이전트가 declare 한다"는 전제에 100% 의존하는데 그 전제가 현장에서 안 지켜진다.

**제안**:
1. **기본 경로화** — 에이전트 부팅 훅이 자동으로 `declare`+worktree 격리(opt-out 이 아니라 opt-in 을 뒤집기). "그냥 쓰면 OMD 안에서" 되게.
2. **우회 fail-loud** — 통합 브랜치 `pre-push`/CI 게이트가 *OMD merge-trailer(`CLOUD CONNECT <task>`) 없는* 커밋을 경고/거부. (오늘 우리가 consumer_b `.git/hooks/post-commit` 으로 만든 divergence-nudge 가 이 빈약판.)
3. **discoverability** — repo CLAUDE.md/README 에 "이 repo 다중세션 작업은 OMD 필수" 를 hard prerequisite 로.

---

## 🔴 P2 — hot 공유파일에 구조적으로 약함 (이 코드베이스의 핵심 미스매치)

disjoint write-set 모델 + **위반 강제**(`core.py:1248 connect_rejected reason=writeset_violation`, 감사 `_writeset_audit` core.py:660). 그래서 `constants/env.py`·`business_logic.py`·`constants/modbus.py` 처럼 **여러 task 가 동시에 건드려야 하는 중앙 파일**은:
- ① 한 궤도만 잡아 **직렬화**(그 파일 만지는 작업 병렬도 ≈ 1), 또는
- ② 안 claim 하고 건드리면 **connect 거부**(writeset_violation).

**실증**: 오늘 divergence 의 충돌 파일이 정확히 `env.py`/`modbus.py`(여러 세션이 PLC/env 동시수정). = OMD 가 가장 약한 케이스. **공유파일 동시편집(3-way merge/CRDT) 경로가 없음** — disjoint 만 1급시민.

**제안**: "shared/hot" glob 등급 도입 — 그 경로는 disjoint 강제 대신 **연결 시 3-way merge 허용**(충돌나면 그때만 fail). 또는 hot 파일 전용 **빠른 직렬 레인**(claim→tiny edit→즉시 release). 중앙설정 파일을 위한 1급 패턴이 필요.

---

## 🟠 P3 — 충돌을 "버그/경보"로 취급 (graceful 복구 아님)

`SERVER_SPEC.md:156` "CONNECTING→MERGED 시 충돌은 **구조적 불가**, 실제 나면 = 불변식 구현 버그 = 경보". 즉 disjoint 가정이 깨지면(glob char-class over-report·공유파일·write-set 밖 쓰기) **복구가 아니라 rollback+alarm**. 실코드베이스에서 충돌은 정상사건인데 OMD 는 "있으면 안 되는 일"로 본다 → P2 와 맞물려 운영 마찰.

---

## 🟠 P4 — 명세는 있는데 미구현 (스펙↔구현 GAP)

프로젝트 *자체* 가 인정한 갭(설계문서 인용):
- **D12 read-set 코히런스**: `CONCURRENCY §D12:478` "commit 감사는 **자문(non-blocking)**" — phantom read(소비자가 옛 base 로 머지) 차단이 advisory. 미게이트.
- **세마포어/permit**: `CONCURRENCY:172` "명세엔 있으나 **미구현**".
- **periodic sweep 없음**: `CONCURRENCY §D3:538 / §D4:569` — 만료 lease 회수가 **inline only**(동사 호출 때만). 유휴 후 첫 호출에서 sweep 몰림 → latency 스파이크.
- **idempotency 테이블 GC 없음**: `CONCURRENCY §D9:508` "request_id 행 무한 누적".
- **배리어-bound 재기동 복구**: `CONCURRENCY §D5` "§3.D 배리어 단위 복구 **미구현**(P1/P2)" — 크래시 시 배리어 부분 트립.
- **durable FSM**: `SERVER_SPEC:183` "처음엔 도입 안 함" — 크래시-내성 deferred.
- **D14 멀티-코디네이터 HA**: 단일프로세스 테스트만, 멀티프로세스/파티션 integration 테스트 부재.

→ 대부분 "P1/P2 부채"로 *문서엔 정직히* 표기됨. 문제는 이게 **production-ready 아님을 사용자가 모르고 쓸 수 있다**는 것.

---

## 🟡 P5 — 안전하지 않은 기본값 + verb 마찰

- **fence opt-in (unsafe default)**: `CONCURRENCY §D6:506` "D6 가드는 opt-in, strict owner/fence 는 caller 가 `(agent,fence)` 줄 때만". 에이전트가 안 주면 stale lease 재검증 없이 통과 가능. → 기본을 strict 로(하위호환 깨더라도) 또는 프로토콜이 항상 동봉.
- **commit 은 advisory, connect 만 enforce**: write-set 위반을 commit 때 경고만(`commit_writeset_warning`)·connect 에서 거부. 에이전트가 경고 무시하면 늦게 깨짐. → `--strict-writeset` commit-time 거부 옵션.
- **7~8 verb 시퀀스 + 망각-스트랜드**: declare→next→claim→start→commit→**finish**→**connect**. `finish` 빼면 task 영원히 IN_ORBIT(궤도 미해제→기아), `connect` 빼면 worktree 에 묶임(미통합). → **`complete_task()` = finish+connect(+push)** 원샷 wrapper 로 happy-path 단순화. (오늘 고친 **L3 auto_push** 가 이 계열 — connect 가 remote sync 까지 하게 함, `3cb47a7`.)

---

## 🟡 P6 — 단일점/의존성 리스크

- 단일 coordinator+leader(`enforce_single_coordinator`)·**SQLite** store·repo-wide `merge_token` = SPOF.
- `transitions`(pytransitions) 라이브러리 ~9개월 유지 공백(`SERVER_SPEC:175`).

---

## ✅ 오늘 닫은 것 (참고)
- **connect→remote auto-push 갭**: connect 가 통합 브랜치를 로컬만 전진시키고 origin push 안 하던 것 → `Coordinator(auto_push="origin")`/env `OMD_AUTO_PUSH` 추가(`gitio.push_integration`+`_connect_phase_b`, fail-soft). **OMD `3cb47a7`**, `test_auto_push.py` **3 passed** + 기존 connect·git **14 passed**.

---

## 🎯 한 줄 결론
OMD 의 **P0(원자성·fence·disjoint 수학)는 견고**하다. 그러나 **(P1) 안 쓰이고 (P2) 공유파일에 약하고 (P4) 안전기능 다수가 design-only** 라 *현장 production 신뢰성*은 낮다. 우선순위는 명백히 **P1(채택·우회차단) → P2(hot 공유파일) → P4 GAP 게이팅** 순. P0 가 아무리 정교해도 P1 이 안 풀리면(아무도 안 쓰면) 전부 무의미하다.
