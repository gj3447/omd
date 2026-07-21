"""음성오라클 (dev123 teeth 증명): _promote_resolve_task 를 무력화하면 승격 테스트가 RED 여야 한다.
하네스는 이 명령이 exit != 0(=테스트 실패)이면 'teeth 존재'로 판정한다. 즉 승격 코드가 load-bearing
임을(무력화하면 실제로 깨짐을) mutation 으로 실증 — fake-green 방지."""
import sys, pytest
from omd_server import core

core.Coordinator._promote_resolve_task = lambda self, t, cf: None  # 승격 무력화(mutation)
sys.exit(pytest.main([
    "-q", "-p", "no:cacheprovider",
    "tests/test_p3_o3_resolve_task.py::test_exclusive_conflict_promotes_resolve_task",
]))
