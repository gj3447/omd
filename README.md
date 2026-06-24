# OMD — 입체운행물방울 (Orbital Motion Droplet)

멀티에이전트 **병렬 개발 코디네이터**. 사도 **OMC**(입체운행구름, Orbital Motion Cloud) 예하 **군단장**. 내부 불변식 코어 = **SINGULON**(특이점).

N개의 코딩 에이전트(물방울)를 **입체(서로소 write-set 궤도)** 로 따로따로 병렬 운행시키고, **분열(merge conflict)=0** 을 *사전* 보장한 뒤(선의공리 특이점 조건: 부분이 전체를 잠식 못 함), **CLOUD CONNECT(응결=merge)** 로 하나의 구름에 통합한다.

서버권위 **강제형 write-set lease**(advisory도 lock-free도 아닌 4번째 지점)가 핵심 IP — git worktree 격리(=자존자)와 강제 경로조정(=특이점)을 결합한다.

## 문서
- [`CONCEPT.md`](./CONCEPT.md) — 컨셉·은유·아키텍처·선행연구 & 차별점(Longinus 바인딩)
- [`SERVER_SPEC.md`](./SERVER_SPEC.md) — 데이터 모델·상태머신(Orbit/Task/Agent/Barrier)·SINGULON 불변식·OSS 검증(ABC)·추천 스택
- [`CONCURRENCY.md`](./CONCURRENCY.md) — **동시성·실패모드 정밀 설계** (긴급 탈출·고아 lease/플래그·데드락/기아·크래시 복구·14차원 + 교차작용 A–H + P0/P1/P2 로드맵)

## 캐논 계층
`사도 OMC(입체운행구름) → 군단장 OMD → 군단(병렬 에이전트 물방울들)`

## 상태
**동작 (49 tests green, P0 11/11 닫힘).** 구현됨: 입체 glob 교집합 · SQLite lease+fence · Orbit/Task FSM · SINGULON 2지점 강제 · 실물 git worktree+CLOUD CONNECT(merge)+fencing · 좀비 회수 · 데드락 wait-for 사이클 감지 · 우선순위 promote · FastMCP 13툴 · CLI.

> ✅ **동시성 P0 11/11 전부 닫힘 (2026-06-24, 49 tests green).** 증분1 P0-1 claim TOCTOU·P0-2 fence중복(D1 `BEGIN IMMEDIATE` 임계구역) / 증분2 P0-3 release 소유+fence·P0-7 `agent_ttl`=90s 회수ON·P0-8 reclaim 브랜치삭제·P0-9 CONNECTING 회수 / **증분3 P0-4 connect fence-captured(ABA)·P0-5 통합브랜치 명시 checkout(동시merge는 D1로 직렬화)·P0-6 `_recover()` 크래시복구·P0-10 의존 사이클 게이트·P0-11 write-set FS 강제(connect diff 감사)**. 전수 분석·기제·로드맵은 [`CONCURRENCY.md`](./CONCURRENCY.md) §5.1.
>
> ⚠ **남은 frontier (P1·정직).** 프리미티브 크래시안전 — D3 플래그(EPHEMERAL/LATCH+wait)·D4 세마포어·D5 배리어·D6 잔여(finish/commit 소유+fence·bail_epoch) + 성능(split-phase connect: 긴 merge 동안 DB writer 락 점유 해소). HA/싱글톤(D14)은 범위 밖.

## Quickstart
```bash
pip install -e .            # 코어 + transitions   (서버: -e '.[server]')
pytest -q                   # 49 passed

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
omd = Coordinator(repo="/path/to/repo")       # 실물 git 연동
omd.declare("A", writes=["a/**"]); omd.next_task("agA")
s = omd.start("A", "agA")                      # 물방울 worktree 발사
# ... s["worktree"] 에서 작업 ...
omd.commit("A", "feat: a"); omd.finish("A")
omd.connect("A")                               # CLOUD CONNECT = 실제 merge (fencing 검증)
```
