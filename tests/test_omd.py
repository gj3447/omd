"""OMD 코어 불변식 테스트 — SINGULON(분열0) 강제 검증."""

import time

import pytest

from omd_server import Coordinator
from omd_server.disjoint import globs_overlap, sets_overlap, glob_prefix


# ---- 입체(disjoint) 판정 ----
def test_glob_prefix():
    assert glob_prefix("src/auth/**") == "src/auth"
    assert glob_prefix("src/*.py") == "src"
    assert glob_prefix("src/a/login.py") == "src/a/login.py"
    assert glob_prefix("*.py") == ""  # 첫 세그먼트 와일드카드 → 전체


def test_overlap_nested_conflicts():
    assert globs_overlap("src/a/**", "src/a/util.py") is True   # 조상-자손
    assert globs_overlap("src/a/**", "src/a/**") is True


def test_overlap_siblings_disjoint():
    assert globs_overlap("src/a/**", "src/b/**") is False
    assert sets_overlap(["src/a/**", "docs/**"], ["src/b/**", "test/**"]) is False


def test_overlap_precise_segments():
    # 정밀: src/*.py(직접 자식만) 와 src/auth/**(서브트리)는 서로소
    assert globs_overlap("src/*.py", "src/auth/x.py") is False
    assert globs_overlap("src/**", "src/auth/x.py") is True
    # 세그먼트 깊이 다르면 서로소
    assert globs_overlap("*.py", "src/a.py") is False
    # 재귀 와일드카드는 깊이 가로질러 겹침
    assert globs_overlap("**/*.py", "src/a.py") is True
    # 디렉토리 선언(trailing /) = 서브트리
    assert globs_overlap("src/auth/", "src/auth/login.py") is True


def test_overlap_charclass_conservative():
    # 문자클래스는 안전하게 overlap=True (false-negative 금지)
    assert globs_overlap("src/[ab].py", "src/c.py") is True


# ---- claim: 입체면 둘 다 HELD, 겹치면 두번째 PENDING ----
def test_disjoint_claims_both_held():
    omd = Coordinator()
    a = omd.claim("agentA", ["src/a/**"], "write")
    b = omd.claim("agentB", ["src/b/**"], "write")
    assert a["state"] == "HELD" and b["state"] == "HELD"
    assert a["fence"] < b["fence"]  # 단조 증가


def test_overlapping_write_claim_waits():
    omd = Coordinator()
    omd.claim("agentA", ["src/a/**"], "write")
    c = omd.claim("agentC", ["src/a/login.py"], "write")
    assert c["state"] == "PENDING"
    assert c["conflicts"]


def test_read_read_coexists_but_write_blocks_read():
    omd = Coordinator()
    omd.claim("r1", ["src/a/**"], "read")
    assert omd.claim("r2", ["src/a/**"], "read")["state"] == "HELD"
    assert omd.claim("w1", ["src/a/x.py"], "write")["state"] == "PENDING"


# ---- TTL 만료 → 자동 회수 → 대기중 promote ----
def test_expire_reclaims_and_promotes():
    omd = Coordinator()
    held = omd.claim("agentA", ["src/a/**"], "write", ttl=0.05)
    waiting = omd.claim("agentC", ["src/a/**"], "write")
    assert waiting["state"] == "PENDING"
    time.sleep(0.08)
    omd.sweep()  # held 만료 → waiting promote
    assert omd.store.get_orbit(held["orbit_id"])["state"] == "EXPIRED"
    assert omd.store.get_orbit(waiting["orbit_id"])["state"] == "HELD"


# ---- fencing: 작업 중 lease 만료되면 connect 거부 ----
def test_connect_rejects_stale_lease():
    omd = Coordinator()
    omd.declare("T", writes=["src/a/**"])
    omd.next_task("agentA")
    omd.claim("agentA", ["src/a/**"], "write", ttl=0.05, task_id="T")
    omd.start("T", "agentA")
    omd.finish("T")
    time.sleep(0.08)
    res = omd.connect("T")  # lease 만료됨 → stale fence 거부
    assert res["ok"] is False and "stale" in res["reason"]


def test_connect_succeeds_when_lease_valid():
    omd = Coordinator()
    omd.declare("T", writes=["src/a/**"])
    omd.next_task("agentA")
    omd.claim("agentA", ["src/a/**"], "write", ttl=600, task_id="T")
    omd.start("T", "agentA")
    omd.finish("T")
    res = omd.connect("T")
    assert res["ok"] is True and res["state"] == "MERGED"
    assert omd.store.get_task("T")["state"] == "MERGED"


# ---- next_task: 활성 궤도와 겹치는 작업은 건너뛰고 서로소만 ----
def test_next_task_skips_overlapping():
    omd = Coordinator()
    omd.declare("T1", writes=["src/a/**"])
    omd.declare("T2", writes=["src/a/util.py"])   # T1과 겹침
    omd.declare("T3", writes=["src/b/**"])         # 서로소
    omd.claim("agentA", ["src/a/**"], "write", task_id="T1")
    picked = omd.next_task("agentB")
    assert picked["task_id"] == "T3"


def test_deadlock_denied():
    omd = Coordinator()
    omd.claim("A", ["a/**"], "write")          # A가 a 점유
    omd.claim("B", ["b/**"], "write")          # B가 b 점유
    r1 = omd.claim("A", ["b/**"], "write")     # A는 B를 대기
    assert r1["state"] == "PENDING"
    r2 = omd.claim("B", ["a/**"], "write")     # B는 A를 대기 → 사이클
    assert r2["state"] == "DENIED" and r2.get("deadlock")


def test_promote_priority_order():
    omd = Coordinator()
    h = omd.claim("H", ["a/**"], "write")
    lo = omd.claim("LO", ["a/**"], "write", priority=1)
    hi = omd.claim("HI", ["a/**"], "write", priority=5)
    assert lo["state"] == "PENDING" and hi["state"] == "PENDING"
    omd.release(h["orbit_id"])                  # promote: 높은 우선순위 먼저
    assert omd.store.get_orbit(hi["orbit_id"])["state"] == "HELD"
    assert omd.store.get_orbit(lo["orbit_id"])["state"] == "PENDING"


def test_zombie_reclaim_requeues():
    omd = Coordinator(agent_ttl=0.05)
    omd.declare("T", writes=["a/**"])
    omd.next_task("agA")
    omd.claim("agA", ["a/**"], task_id="T")
    omd.start("T", "agA")
    assert omd.store.get_task("T")["state"] == "IN_ORBIT"
    time.sleep(0.08)
    res = omd.reclaim_zombies()
    assert "agA" in res["reclaimed"]
    assert omd.store.get_task("T")["state"] == "PENDING"      # 작업 requeue
    r = omd.claim("agB", ["a/**"], task_id="T")               # 회수된 궤도 재획득
    assert r["state"] == "HELD"


def test_next_task_respects_deps():
    omd = Coordinator()
    omd.declare("base", writes=["src/base/**"])
    omd.declare("dependent", writes=["src/x/**"], deps=["base"])
    # base 미완 → dependent 건너뜀, base 반환
    assert omd.next_task("a")["task_id"] == "base"
