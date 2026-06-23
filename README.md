# OMD — 입체운행물방울 (Orbital Motion Droplet)

멀티에이전트 **병렬 개발 코디네이터**. 사도 **OMC**(입체운행구름, Orbital Motion Cloud) 예하 **군단장**. 내부 불변식 코어 = **SINGULON**(특이점).

N개의 코딩 에이전트(물방울)를 **입체(서로소 write-set 궤도)** 로 따로따로 병렬 운행시키고, **분열(merge conflict)=0** 을 *사전* 보장한 뒤(선의공리 특이점 조건: 부분이 전체를 잠식 못 함), **CLOUD CONNECT(응결=merge)** 로 하나의 구름에 통합한다.

서버권위 **강제형 write-set lease**(advisory도 lock-free도 아닌 4번째 지점)가 핵심 IP — git worktree 격리(=자존자)와 강제 경로조정(=특이점)을 결합한다.

## 문서
- [`CONCEPT.md`](./CONCEPT.md) — 컨셉·은유·아키텍처·선행연구 & 차별점(Longinus 바인딩)
- [`SERVER_SPEC.md`](./SERVER_SPEC.md) — 데이터 모델·상태머신(Orbit/Task/Agent/Barrier)·SINGULON 불변식·OSS 검증(ABC)·추천 스택

## 캐논 계층
`사도 OMC(입체운행구름) → 군단장 OMD → 군단(병렬 에이전트 물방울들)`

## 상태
**프로토타입 동작 (19 tests green).** 구현됨: 입체 glob 교집합 · SQLite lease+fence · Orbit/Task FSM · SINGULON 2지점 강제 · 실물 git worktree+CLOUD CONNECT(merge)+fencing · 좀비 회수 · 데드락 wait-for 사이클 감지 · 우선순위 promote · FastMCP 13툴 · CLI.

## Quickstart
```bash
pip install -e .            # 코어 + transitions   (서버: -e '.[server]')
pytest -q                   # 19 passed

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
