"""재귀 도그푸드 회귀가드: OMD 가 자기 instrumentation 작업의 병렬-dev 를 분열 없이 조율함을 고정.

scripts/dogfood_parallel_dev.py — 이 세션 산출물(PROM 13 guard + programme + OOPTDD spec/driver/test)을
실 Coordinator 로 claim → 입체면 전부 grant(SINGULON Δ분열=0), 같은 파일(OOPTDD 5 req=core.py)은 직렬.
"""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scripts"))

from dogfood_parallel_dev import REQ_WRITESETS, WORK_ITEMS, parallel_batches, receipt  # noqa: E402


def test_session_artifacts_are_disjoint_singulon_parallel():
    """이 세션 산출물은 write-set 서로소(입체) → 실 Coordinator 가 전부 병렬 grant, 0 충돌, 단조 fence."""
    r = receipt()
    assert r["work_all_disjoint"] is True
    assert r["work_max_parallel"] == r["work_items"] == len(WORK_ITEMS)
    d = r["coordinator_drive"]
    assert d["granted"] == d["claimed"] == len(WORK_ITEMS), d
    assert d["pending"] == 0, d
    assert d["unique_fences"] is True, d
    assert d["synthetic_overlap_serialized"] is True, "겹치는 작업은 직렬화(orbit_pending)돼야"


def test_same_file_requirements_are_serialized():
    """OOPTDD 5 requirement 은 전부 core.py 편집 → OMD 가 정직히 직렬화(병렬 불가)."""
    r = receipt()
    assert r["req_serial_same_file"] is True
    assert r["req_max_parallel"] == 1, "같은 파일을 병렬로 묶으면 분열 위험 — OMD 가 직렬화해야"
    assert len(parallel_batches(REQ_WRITESETS)) == len(REQ_WRITESETS)
