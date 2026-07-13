"""Coordinator 공유 상수 — core 와 sync-primitive mixin(_flags/_sems/_barriers)이 함께 쓰는
값을 한 곳에(순환 import 회피용 최소 모듈, apt-cleanup Q7 2026-07-13)."""
from __future__ import annotations

# P2 shared 레인: write-동급 궤도 mode. "shared" = hot 공유파일 전용 — 같은 경로에 shared↔shared
# 동시 HELD 를 허용하고(직렬화 마찰 제거) 응결은 git 3-way 에 맡긴다. 진짜 충돌(같은 hunk)은
# connect 에서 shared_conflict(정상사건·retryable)로 표면화. write-set 감사/fence/해제 경로에선
# write 와 동급으로 취급돼 disjoint(write) 궤도의 배타 의미론은 불변.
WRITE_MODES = ("write", "shared")

# Phase B(락밖 merge) 응결 pin 유예. 이 시간 동안 sweep/reclaim 이 진행중 merge 를 건드리지 않는다.
MERGE_PIN_GRACE_S = 60.0

# D3 단조 LATCH 랭크(§D3): done(1) < merged(2). 하향 set 은 거부, 동값 재발행은 멱등 no-op.
# 0 = 랭크 없는 일반 LATCH(임의 값, 단조검사 안 함). 의존 해제는 =merged 에 건다(§3.H).
LATCH_RANK = {"done": 1, "merged": 2}
