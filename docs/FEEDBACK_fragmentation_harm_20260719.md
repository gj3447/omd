# OMD 피드백 — 채택실패가 현실화한 대형 파편화 피해 (2026-07-19)

> operator 지시(2026-07-19): "OMD 이 제품 때문에 시간낭비 오지게 했다. OMD repo에 피드백 박아라."
> [`FEEDBACK_problems_20260630.md`](./FEEDBACK_problems_20260630.md) **P1(채택 안 됨 = 보장 무의미)의 3주 후 후속.** 그 예측이 확정됐고, 이제 *net-negative(순피해)* 로 넘어갔다.
> 실증 = 2026-07-19 3DLAB 두 프로덕션 레포(3d_vision_jg_bpc · prismv2) git 실측(읽기전용 하네스).

---

## 🔴 P0 (신규·최상위) — "mandate-but-unadopted" = 능동적 오도(誤導). OMD가 파편화를 *막은 게 아니라 유발*했다.

06-30 P1은 "코드는 옳은데 아무도 안 쓴다"였다. 3주 뒤 실측은 그보다 나쁘다: **아무도 안 쓰는데 하류 repo의 CLAUDE.md는 여전히 OMD를 통합 정본경로로 강제**한다.

**실증 — `3d_vision_jg_bpc/CLAUDE.md:103~107`**: "kjra 통합의 정본 경로는 **OMD 코디네이터**다 … 코디네이터 클론 = `/data/kjra/PROJECT/3D/jgbpc-omd` … 드라이버 루프 declare→claim→complete_task auto-push … `tools/kjra_sync.sh` … pre-push OMD 게이트." → 신규/컨텍스트-끊긴 에이전트가 이 지시를 읽고 (a) OMD ceremony 시도 → 과부하 → 포기, 또는 (b) 우회해 자체 브랜치 분기. **어느 쪽이든 OMD가 막겠다던 바로 그 파편화로 귀결**한다.

**결과 (2026-07-19 실측):**
- **jg_bpc**: 로컬 브랜치 **18개**, worktree **8개**, 서로 경쟁하는 "kjra-*" 통합 브랜치 **5개+** (`kjra-consolidate` · `kjra-clean-integ` · `kjra-fold` · `kjra-consolidate-fix` · `_converge_kjra_reinspection`). "분열 = 0, 하나의 구름(CLOUD CONNECT)" 약속의 **정반대**. 한 세션 통째로 수렴에 소비하고도 미완(측정 시점 계속 새 브랜치 생성 중).
- **prismv2**: 서로 부분만 겹치는 **13개 발산 계보** (codex/* · deploy/* · agent/* · label24 · snapshot …). 어느 하나도 나머지를 포함 안 함 → **수동 trunk-based(비-OMD) 통합**으로 정리(OMD 전혀 안 씀, 결과 파편화 0·GREEN).

**핵심**: mandate 되었지만 미채택인 도구는 **없는 것보다 나쁘다** — *권위 있는 오도*이기 때문. 06-30 P1의 처방("우회 fail-loud / 기본경로화")은 3주간 미반영됐고, 그 사이 피해가 실현됐다.

## 🔴 P0b — 근본 미스매치 재확인: OMD는 CLOBBER는 막아도 FRAGMENTATION은 못 막는다 (오히려 조장)

OMD의 1급 시민 = disjoint write-set lease(동시에 같은 파일 쓰기 = clobber 방지). 그러나 operator의 실제 고통은 clobber가 아니라 **오래 사는 발산 브랜치의 난립(fragmentation)**이었다. OMD는 이걸 다루는 primitive가 없을 뿐 아니라, **task마다 declare→claim→next→start→commit→finish→connect ceremony**를 요구해 **에이전트가 우회 → 장수 브랜치로 도망**가게 만든다 → 파편화를 *증가*시킨다.

실제로 이긴 패턴은 훨씬 단순했다: **kjra 단일 브랜치 + 잦은 소(小)커밋 + 당일 머지(trunk-based)**. ceremony 0. 이걸로 prismv2를 GREEN(파편화 0)으로 정리했다.

## 🔴 P0c — 파편화 줄이겠다는 도구가 *스스로 디렉토리 난립*을 만든다 (아이러니)

2026-07-19 실측: OMD가 스폰한 클론/워크트리/integration 디렉토리 **12개**:
`jgbpc-omd` · `jgbpc-omd-omd-integration` · `jgbpc-omd-omd-worktrees` · `jgbpc-omd-worktrees` · `omd-wt-p3ux` · `lakatotree-wt-omdengine` · `omd-wt` · `sqcedit-omd-integration` …
**`jgbpc-omd-omd-integration` / `jgbpc-omd-omd-worktrees` = 이중-omd(omd-omd) 이름** — 설정이 재귀·혼란에 빠졌다는 물증. 게다가 jg_bpc CLAUDE.md는 "이 현장 체크아웃엔 kjra가 물려있어 **OMD in-place 바인딩 불가**(같은 브랜치 이중 worktree를 git이 거부) → 별도 코디네이터 클론 `/data/kjra/PROJECT/3D/jgbpc-omd`"라고 명시한다. 즉 **OMD의 모델이 git 자신의 worktree 모델과 싸워** 별도 클론을 강제하고, 그 클론이 또 drift한다. 파편화를 줄이는 도구가 파편화의 *공급원*이 됐다.

## 🔴 P0d — 자기 DB가 stale = 채택 0의 직접 물증 (06-30과 동일 패턴 3주 연속)

`omd_mcp.db` 마지막 수정 = **2026-07-14 10:16**(측정 시점 5일 전). 07-15~19의 daily 작업(jg_bpc 18브랜치·prismv2 13계보 통합)은 **OMD를 한 번도 안 탐**. 06-30 피드백의 "omd_mcp.db = 6 task, Jun 28에서 멈춤"과 **정확히 같은 패턴**. 도구의 심장(task/lease DB)이 3주째 정지 상태.

## 🔴 P0e — 핵심 IP가 *잘못된 문제*를 푼다

OMD의 핵심 IP = "서버권위 강제형 disjoint write-set lease"(동시에 같은 파일 쓰기=clobber 방지, `core.py:1248 writeset_violation`). 그러나 2026-07 현장에서 관측된 실패는 **clobber가 단 한 번도 아니었다** — 전부 **오래 사는 발산 브랜치(fragmentation)**였다. lease는 fragmentation에 **무력**하다. 즉 방대한 IP가 병목이 아닌 문제를 정밀하게 푼다. (06-30 P2 "hot 공유파일에 구조적 약함"의 심화판: 실수요는 disjoint 격리가 아니라 *합치기*였다.)

## 🔴 P0f — 복잡도/효과 역전 + 3주간 실블로커 방치

- **투입**: 35 MCP 툴 + TLA+ 3스펙 + Hypothesis stateful 2종 + SINGULON 2지점 불변식 + "선의공리 특이점" + 은유층(입체운행물방울/OMC/군단장/특이점). 방대한 개념·구현·검증 표면.
- **대안**: trunk-based(kjra 단일 브랜치 + 잦은 소커밋 + 당일 머지)는 **도구 0개**로 같은 문제를 해결 — prismv2를 실제로 GREEN(파편화 0)으로 정리한 방법이 이것.
- **방향 오배치**: 06-30이 최상위 문제로 "채택 0"을 짚고 구체 처방(기본경로화·우회 fail-loud)을 줬는데, 3주간 그건 미반영이고 machinery만 늘었다(`OMD_SCHEDULER_REDESIGN_20260715.md`·`OMD_DEMPSEY_ROLL.md`). **아무도 안 쓰는 도구에 3겹 검증·스케줄러 재설계를 투자**하는 동안, 실블로커(채택)는 그대로 → 예측된 피해(파편화)가 그 사이 실현.

## 🟠 P0g — 은유층이 사용 전 해독비용을 만든다

"입체운행물방울 / Orbital Motion Droplet / 사도 OMC / 군단장 / SINGULON(특이점) / 선의공리 / 응결(CLOUD CONNECT)" — 도구를 쓰기 전에 은유를 먼저 해독해야 verb(declare/claim/connect)의 의미가 잡힌다. 신규/컨텍스트-끊긴 에이전트에겐 이게 그대로 진입 마찰이고, "그냥 git 브랜치 하나" 대비 인지비용이 크다. (기능이 아니라 **채택 마찰** 관점의 지적.)

## 🎯 operator 판정 & 권고

**판정 (2026-07-19)**: OMD 채택 여전히 0%, "오버헤드만 크고 효과 없음", **순피해(시간낭비)**. 06-30 이후 개선 미반영.

**권고 (우선순위):**
1. **[최상위·즉시] 하류 repo CLAUDE.md/AGENTS.md에서 OMD mandate 전량 제거.** jg_bpc `CLAUDE.md:103~107` + `tools/kjra_sync.sh` 참조 + pre-push OMD 게이트 + `*-omd`/`*-omd-worktrees`/`*-omd-integration` 클론 안내 — 전부. 이것이 파편화의 *능동 원인*이므로 최고 레버리지 수정.
2. **재-mandate 금지.** OMD를 유지하려면 그것이 이기는 **단 하나의 케이스**(진짜 동시 같은-파일 다중 쓰기)로만 좁히고, 통합/브랜치 정본경로로는 **절대** 강제하지 말 것.
3. **기본 = trunk-based.** 배포 표면이 2개 repo(jg_bpc · prismv2)뿐이고 각각 kjra 단일 브랜치면 충분. 06-30 P1의 "기본경로화 / 우회 fail-loud"를 여전히 안 할 거라면, 이 워크플로에서 OMD를 **deprecate**하라.

**한 줄 결론**: 06-30의 "코드는 옳은데 아무도 안 쓴다"가 확정 + 이제 **하류 mandate가 파편화를 능동 유발**한다. 코드 품질과 무관하게, 이 워크플로에서 OMD의 순효과는 음(−)이다. **mandate부터 제거하라.**
