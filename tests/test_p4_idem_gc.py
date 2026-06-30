"""P4 — 멱등(idempotency) 캐시 GC 변환-불변식. 만료 DONE 삭제·INFLIGHT/최근 보존·replay 유지."""
import time

from omd_server import Coordinator


def _backdate(store, rid, *, completed_at=None, created_at=None):
    store.db.execute(
        "UPDATE idempotency SET completed_at=COALESCE(?,completed_at),"
        " created_at=COALESCE(?,created_at) WHERE request_id=?",
        (completed_at, created_at, rid))


def test_old_done_collected(tmp_path):
    """happy: idem_ttl 지난 DONE 행은 sweep 후 0개."""
    omd = Coordinator(db_path=str(tmp_path / "o.db"), idem_ttl=3600.0)
    omd.claim("agA", ["a/**"], "write", request_id="r1")
    assert omd.store.get_idem("r1") is not None        # 성공 응답 DONE 캐시됨
    _backdate(omd.store, "r1", completed_at=time.time() - 7200)  # ttl 2배 과거
    omd.sweep()
    assert omd.store.get_idem("r1") is None             # 만료 DONE 삭제(누적 차단)


def test_inflight_never_collected(tmp_path):
    """edge: INFLIGHT(completed_at NULL)은 created_at 가 아무리 오래돼도 절대 삭제 안 됨."""
    omd = Coordinator(db_path=str(tmp_path / "o.db"), idem_ttl=3600.0)
    omd.store.begin_idem("inflight1", "agA", "claim", "h")
    _backdate(omd.store, "inflight1", created_at=time.time() - 99999)
    omd.sweep()
    assert omd.store.get_idem("inflight1") is not None  # 진행중 멱등 윈도우 보존


def test_recent_done_preserved_and_replay(tmp_path):
    """edge: ttl 이내 DONE 보존 → 같은 request_id replay 가 캐시 적중(본문 재실행 안 함)."""
    omd = Coordinator(db_path=str(tmp_path / "o.db"), idem_ttl=3600.0)
    r1 = omd.claim("agA", ["a/**"], "write", request_id="r2")
    omd.sweep()
    assert omd.store.get_idem("r2") is not None
    r2 = omd.claim("agA", ["a/**"], "write", request_id="r2")  # replay
    assert r2.get("replayed") is True                          # 캐시 적중(본문 재실행 안 함)
    assert {k: v for k, v in r2.items() if k != "replayed"} == r1   # 본문 동일


def test_gc_disabled_when_ttl_none(tmp_path):
    """idem_ttl=None → GC off(기존동작). 오래된 DONE 도 보존."""
    omd = Coordinator(db_path=str(tmp_path / "o.db"), idem_ttl=None)
    omd.claim("agA", ["a/**"], "write", request_id="r3")
    _backdate(omd.store, "r3", completed_at=time.time() - 99999)
    omd.sweep()
    assert omd.store.get_idem("r3") is not None
