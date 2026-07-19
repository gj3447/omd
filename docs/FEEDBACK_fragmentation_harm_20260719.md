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

## 🎯 operator 판정 & 권고

**판정 (2026-07-19)**: OMD 채택 여전히 0%, "오버헤드만 크고 효과 없음", **순피해(시간낭비)**. 06-30 이후 개선 미반영.

**권고 (우선순위):**
1. **[최상위·즉시] 하류 repo CLAUDE.md/AGENTS.md에서 OMD mandate 전량 제거.** jg_bpc `CLAUDE.md:103~107` + `tools/kjra_sync.sh` 참조 + pre-push OMD 게이트 + `*-omd`/`*-omd-worktrees`/`*-omd-integration` 클론 안내 — 전부. 이것이 파편화의 *능동 원인*이므로 최고 레버리지 수정.
2. **재-mandate 금지.** OMD를 유지하려면 그것이 이기는 **단 하나의 케이스**(진짜 동시 같은-파일 다중 쓰기)로만 좁히고, 통합/브랜치 정본경로로는 **절대** 강제하지 말 것.
3. **기본 = trunk-based.** 배포 표면이 2개 repo(jg_bpc · prismv2)뿐이고 각각 kjra 단일 브랜치면 충분. 06-30 P1의 "기본경로화 / 우회 fail-loud"를 여전히 안 할 거라면, 이 워크플로에서 OMD를 **deprecate**하라.

**한 줄 결론**: 06-30의 "코드는 옳은데 아무도 안 쓴다"가 확정 + 이제 **하류 mandate가 파편화를 능동 유발**한다. 코드 품질과 무관하게, 이 워크플로에서 OMD의 순효과는 음(−)이다. **mandate부터 제거하라.**
