"""P0-10 — task 의존 사이클 검출. declare가 사이클을 만들면 거부(영구 BLOCKED 방지)."""
import pytest
from omd_server import Coordinator


def test_self_dep_rejected(tmp_path):
    omd = Coordinator(db_path=str(tmp_path / "o.db"))
    with pytest.raises(ValueError):
        omd.declare("Z", writes=["z/**"], deps=["Z"])


def test_two_cycle_rejected(tmp_path):
    omd = Coordinator(db_path=str(tmp_path / "o.db"))
    omd.declare("A", writes=["a/**"], deps=["B"])   # forward dep ok (B 아직 없음)
    with pytest.raises(ValueError):
        omd.declare("B", writes=["b/**"], deps=["A"])   # closing edge → cycle
    assert omd.store.get_task("B") is None              # 거부 → 미생성


def test_three_cycle_rejected(tmp_path):
    omd = Coordinator(db_path=str(tmp_path / "o.db"))
    omd.declare("A", writes=["a/**"], deps=["B"])
    omd.declare("B", writes=["b/**"], deps=["C"])
    with pytest.raises(ValueError):
        omd.declare("C", writes=["c/**"], deps=["A"])


def test_valid_chain_ok(tmp_path):
    omd = Coordinator(db_path=str(tmp_path / "o.db"))
    omd.declare("X", writes=["x/**"])
    omd.declare("Y", writes=["y/**"], deps=["X"])       # DAG, no cycle
    assert omd.store.get_task("Y")["state"] == "PENDING"
