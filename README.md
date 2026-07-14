# OMD — 입체운행물방울 (Orbital Motion Droplet)

멀티에이전트 **병렬 개발 코디네이터**. 사도 **OMC**(입체운행구름, Orbital Motion Cloud) 예하 **군단장**. 내부 불변식 코어 = **SINGULON**(특이점).

N개의 코딩 에이전트(물방울)를 **입체(서로소 write-set 궤도)** 로 따로따로 병렬 운행시키고, **분열(merge conflict)=0** 을 *사전* 보장한 뒤(선의공리 특이점 조건: 부분이 전체를 잠식 못 함), **CLOUD CONNECT(응결=merge)** 로 하나의 구름에 통합한다.

서버권위 **강제형 write-set lease**(advisory도 lock-free도 아닌 4번째 지점)가 핵심 IP — git worktree 격리(=자존자)와 강제 경로조정(=특이점)을 결합한다.

## 문서
- [`CONCEPT.md`](./CONCEPT.md) — 컨셉·은유·아키텍처·선행연구 & 차별점(Longinus 바인딩)
- [`SERVER_SPEC.md`](./SERVER_SPEC.md) — 데이터 모델·상태머신(Orbit/Task/Agent/Barrier)·SINGULON 불변식·OSS 검증(ABC)·추천 스택
- [`CONCURRENCY.md`](./CONCURRENCY.md) — **동시성·실패모드 정밀 설계** (긴급 탈출·고아 lease/플래그·데드락/기아·크래시 복구·14차원 + 교차작용 A–H + P0/P1/P2 로드맵)
- [`docs/ENGINE_V2.md`](./docs/ENGINE_V2.md) — **v2 functional core + SQLite CAS/outbox + lease-only profile** 설계·불변식·검증·이행 경계

## Engine v2 (experimental)

`omd_server.v2`는 기존 `Coordinator`를 수정하거나 live lease를 옮기지 않는 별도 엔진입니다.
canonical `ResourceId`, 원자 claim-set, mandatory `FenceVector`, strict idempotency,
순수 `decide(state, command, now)`, SQLite WAL/revision CAS/transactional outbox를 먼저 닫고,
서버 발급 session epoch와 runtime-owned 만료 supervisor를 결합했습니다. Git·merge·task
lifecycle이 아예 없는 lease-only MCP surface로만 canary 합니다.

```bash
OMD_V2_DOMAIN_ID=my-workspace OMD_V2_REPO_ID=my-repo \
omd-v2-lease
```

v2 mutation 도구에는 호출자가 principal을 넣는 인자가 없습니다. stdio 시작 시 SQLite
atomic upsert/increment로 `(client, agent)`의 epoch를 발급·결속합니다. 기본 identity는
transport마다 고유하고, `OMD_V2_AGENT_ID`를 고정하면 해당 agent에서 파생된 client도
고정되어 재시작 시 옛 프로세스를 fencing 합니다. request ID 범위는
`(domain, client)` 전체이므로 stable agent의 재시작 전체와 client를 명시적으로 공유하는
모든 caller에서 전역 고유 request ID를 써야 합니다. 기본 DB는 cwd가 아니라
`~/.local/state/omd/v2/lease.db`(XDG/환경변수 override 가능)를 사용합니다. 이는 로컬
stdio+파일 권한 신뢰 경계이며, DB 파일을 직접 수정할 수 있는 로컬 공격자에 대한 인증
시스템은 아닙니다.

상세 계약과 v1→v2 drain/shadow 순서는 [`docs/ENGINE_V2.md`](./docs/ENGINE_V2.md)를 참조하세요.

## 캐논 계층
`사도 OMC(입체운행구름) → 군단장 OMD → 군단(병렬 에이전트 물방울들)`

## 상태
**프로토타입 동작 — 3겹 검증(green).** ① **`pytest`** (full dev+server+LTDD 환경에서 397 passed, 그중 v2 104; 선택적 deps 부재 시 server/LTDD 테스트는 skip) · ② **TLA+ 모델 체크** (`spec/*.tla` 3종 — leader·lease·connect — CI `tla` 잡에서 TLC) · ③ **Hypothesis stateful** (lease/fence 코어를 무작위 연산열로 흔드는 2종 — in-memory + 영속 SQLite/WAL 재시작 내구성). 구현됨: 입체 glob 교집합 · SQLite lease+fence · Orbit/Task FSM · SINGULON 2지점 강제 · 실물 git worktree+CLOUD CONNECT(merge)+fencing · 좀비 회수 · 데드락 wait-for 사이클 감지 · 우선순위 promote · legacy FastMCP 35툴 + v2 lease-only 6툴 · CLI · **P0 동시성 11/11 + D1–D14 하드닝**.

> ⚠ **검증 범위(정직한 표기).** TLA+ 모델은 **bounded·abstract** 다 — 작은 상수 공간(예: `Tasks = {t1, t2}`)에서만 망라 탐색하고, git split-phase·실시간·크래시 타이밍을 추상화한다. Hypothesis stateful 은 **단일 프로세스 in-proc** 모델(실제 멀티프로세스/멀티노드 레이스 그 자체가 아니라 코어 불변식의 모델). 즉 "model check + stateful + pytest green" 은 *설계 수준* 보증이지 분산 배포의 전수 보증은 아니다. 동시성·실패모드 전수 분석과 로드맵은 [`CONCURRENCY.md`](./CONCURRENCY.md). (예: 좀비 회수는 `agent_ttl` 을 켜야 동작, 기본 비활성.)

## Quickstart
```bash
pip install -e .            # 코어 + transitions   (서버: -e '.[server]')
pytest -q                   # full dev+server+LTDD 환경: 397 passed

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
