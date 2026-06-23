"""D1 동시성 불변식 — 직접 territory 검사 (ooptdd METHODOLOGY 원칙 7: µs 레이스는 log-free zone).

SINGULON: 겹치는(비입체) write-set은 동시에 둘 다 HELD일 수 없다(분열=0).
fence는 단조·전역 유일. 이 둘은 트레이스가 아니라 상태 불변식으로 직접 검증한다.
"""

import threading

import pytest

from omd_server import Coordinator
from omd_server.store import Store


# ---- store.tx() 원자성 ----
def test_tx_atomic_rollback(tmp_path):
    """tx 안의 실패는 fence 증가 + orbit 삽입을 통째로 롤백한다(부분쓰기 없음)."""
    s = Store(str(tmp_path / "x.db"))
    f0 = s.current_fence()
    with pytest.raises(RuntimeError):
        with s.tx():
            fence = s.next_fence()
            s.add_orbit(task_id=None, agent_id="a", pathspec=["p/**"],
                        mode="write", state="HELD", fence=fence)
            raise RuntimeError("boom")
    assert s.held_orbits() == []          # orbit 롤백
    assert s.current_fence() == f0         # fence 소모 안 됨


def test_tx_commit_persists(tmp_path):
    s = Store(str(tmp_path / "x.db"))
    with s.tx():
        fence = s.next_fence()
        oid = s.add_orbit(task_id=None, agent_id="a", pathspec=["p/**"],
                          mode="write", state="HELD", fence=fence)
    assert s.get_orbit(oid)["state"] == "HELD"
    assert s.current_fence() == 1


def test_duplicate_fence_rejected_by_unique_index(tmp_path):
    """P0-2 백스톱: 코드 회귀로 같은 fence를 두 번 발급하면 UNIQUE 인덱스가 fail-closed."""
    import sqlite3
    s = Store(str(tmp_path / "x.db"))
    s.add_orbit(task_id=None, agent_id="a", pathspec=["p/**"], mode="write",
                state="HELD", fence=5)
    with pytest.raises(sqlite3.IntegrityError):
        s.add_orbit(task_id=None, agent_id="b", pathspec=["q/**"], mode="write",
                    state="HELD", fence=5)            # 중복 fence → 거부(조용한 손상 방지)


def _run_concurrent(fn, n):
    barrier = threading.Barrier(n)
    out: list = [None] * n

    def w(i):
        barrier.wait()        # 모든 스레드를 동시에 출발시켜 레이스 창을 최대화
        out[i] = fn(i)

    ts = [threading.Thread(target=w, args=(i,)) for i in range(n)]
    for t in ts:
        t.start()
    for t in ts:
        t.join()
    return out


def test_concurrent_overlapping_claims_single_grant(tmp_path):
    """N 물방울이 동시에 같은 경로 write claim → 정확히 1개만 HELD (P0-1 TOCTOU 닫힘)."""
    omd = Coordinator(db_path=str(tmp_path / "omd.db"))
    n = 16
    out = _run_concurrent(lambda i: omd.claim(f"ag{i}", ["src/shared/**"], "write"), n)
    states = [r["state"] for r in out]
    assert states.count("HELD") == 1, states          # 분열=0: 겹치는 HELD 둘 불가
    assert states.count("PENDING") == n - 1


def test_concurrent_disjoint_claims_unique_monotonic_fences(tmp_path):
    """N 물방울이 동시에 서로소 경로 claim → 전부 HELD + fence 전부 유일·연속 (P0-2 닫힘)."""
    omd = Coordinator(db_path=str(tmp_path / "omd.db"))
    n = 24
    out = _run_concurrent(lambda i: omd.claim(f"ag{i}", [f"d{i}/**"], "write"), n)
    assert all(r["state"] == "HELD" for r in out)
    fences = sorted(r["fence"] for r in out)
    assert len(set(fences)) == n                       # 중복 fence 없음
    assert fences == list(range(1, n + 1))             # 단조·연속(1..N)


def test_two_coordinators_one_db_no_double_grant(tmp_path):
    """공유 lock 없는 두 코디네이터(=멀티프로세스 모형)가 한 파일 DB에 동시 claim →
    BEGIN IMMEDIATE(WAL)가 writer를 직렬화 → 정확히 1개만 HELD."""
    db = str(tmp_path / "shared.db")
    A = Coordinator(db_path=db)
    B = Coordinator(db_path=db)        # 별 인스턴스 = 별 연결 + 별 RLock(공유 안 함)
    barrier = threading.Barrier(2)
    out: dict = {}

    def w(name, co):
        barrier.wait()
        out[name] = co.claim(f"ag-{name}", ["x/**"], "write")

    t1 = threading.Thread(target=w, args=("A", A))
    t2 = threading.Thread(target=w, args=("B", B))
    t1.start(); t2.start(); t1.join(); t2.join()
    held = [r for r in out.values() if r["state"] == "HELD"]
    assert len(held) == 1, out
