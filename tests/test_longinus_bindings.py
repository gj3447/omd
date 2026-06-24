"""Longinus(omd) 바인딩 drift 가드 — 매 커밋 L4(심볼소멸)/L6(시그니처변경) 감지.
omd_server.longinus 정본에 위임(자족, lakatos 의존 0)."""
from omd_server import longinus as L


def test_all_bindings_symbol_resolves():
    """L4: 모든 바인딩 심볼이 현 소스에서 해소돼야(소멸/리네임 없음)."""
    r = L.audit()
    assert not r["l4_drift"], f"L4 심볼소멸: {r['l4_drift']}"


def test_all_bindings_def_line_sha_unchanged():
    """L6: def-line 시그니처 sha 가 baseline 과 일치(의도된 변경이면 매니페스트 재베이스라인)."""
    r = L.audit()
    assert not r["l6_drift"], f"L6 시그니처변경: {r['l6_drift']}"


def test_audit_ok_and_nonempty():
    r = L.audit()
    assert r["ok"] and r["total"] >= 17 and r["passed"] == r["total"]


def test_report_renders():
    assert "Longinus(omd)" in L.report()
