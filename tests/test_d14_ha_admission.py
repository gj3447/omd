"""증분9 — D14 코디네이터 singleton / HA 입장.

§D14 의 핵심: D1 의 in-process actor 직렬화는 *프로세스당*이다. 운영자가 HA 로 코디네이터 2개를
한 DB 에 띄우면 actor 불변식이 조용히 무효(actor 둘 = writer 둘)이고 통합 머지(락 밖)가 무조정.

기제(증분9):
  - DB 리더-lease: 기동 시 leader_lease 를 CAS 획득. 살아있는 다른 리더(heartbeat 가 TTL 안)가
    있으면 CoordinatorConflict 로 **거부**(단일 인스턴스 강제).
  - 죽은 리더(heartbeat TTL 초과)는 takeover 가능 — epoch +1 로 fence. takeover 후 옛 리더가
    깨어나 변이하려 하면 _cs() 의 leader-fence(_assert_leader)가 CoordinatorConflict 로 차단.
  - `:memory:` 디폴트 금지 — 재기동마다 fence/leader_epoch 가 0 으로 리셋(낡은 토큰 충돌).
    영속 DB 필수. 단위테스트만 allow_memory_db=True 로 명시 opt-in.

정상경로 + 크래시(죽은 리더 takeover) + 오추방(takeover 된 좀비 리더의 변이 차단) + 거부.
"""

import sqlite3

import pytest

from omd_server import Coordinator
from omd_server.core import CoordinatorConflict
from omd_server.store import Store, UnsupportedSchemaVersion


def _db(tmp_path):
    return str(tmp_path / "omd.db")


def _db_authority_snapshot(db_path):
    with sqlite3.connect(db_path) as db:
        return {
            "schema_version": db.execute("PRAGMA schema_version").fetchone()[0],
            "meta": db.execute(
                "SELECT key,value FROM meta ORDER BY key"
            ).fetchall(),
            "schema": db.execute(
                "SELECT type,name,sql FROM sqlite_master "
                "WHERE name NOT LIKE 'sqlite_%' ORDER BY type,name"
            ).fetchall(),
        }


# ---------- 1) :memory: 디폴트 금지 (영속 DB 강제) ----------
def test_memory_db_default_is_forbidden():
    with pytest.raises(ValueError, match="persistent DB"):
        Coordinator()                       # 디폴트 :memory: → 거부
    with pytest.raises(ValueError):
        Coordinator(db_path=":memory:")     # 명시 :memory: 도 거부(opt-in 없으면)
    # 단위테스트 opt-in 은 허용.
    assert Coordinator(allow_memory_db=True).leader_epoch == 1


# ---------- 2) 첫 코디네이터가 리더 lease 획득 ----------
def test_first_coordinator_becomes_leader(tmp_path):
    omd = Coordinator(db_path=_db(tmp_path), coordinator_id="c1")
    assert omd.leader_epoch == 1
    L = omd.store.get_leader()
    assert L["coordinator_id"] == "c1" and L["epoch"] == 1


# ---------- 3) 살아있는 리더가 있으면 둘째 기동 거부 (단일 인스턴스 강제) ----------
def test_second_live_coordinator_is_refused(tmp_path):
    db = _db(tmp_path)
    a = Coordinator(db_path=db, coordinator_id="a")
    with pytest.raises(CoordinatorConflict, match="single-instance"):
        Coordinator(db_path=db, coordinator_id="b")     # a 가 살아있음 → 거부
    # a 는 정상 동작.
    assert a.claim("ag", ["x/**"], "write")["state"] == "HELD"


def test_current_schema_peer_is_rejected_without_running_migration(
    tmp_path, monkeypatch
):
    db = _db(tmp_path)
    Coordinator(db_path=db, coordinator_id="a")
    before = _db_authority_snapshot(db)
    calls = []

    def forbidden_initialize(self):
        calls.append(self)
        raise AssertionError("current-schema peer attempted migration")

    monkeypatch.setattr(Store, "initialize", forbidden_initialize)
    with pytest.raises(CoordinatorConflict, match="single-instance"):
        Coordinator(db_path=db, coordinator_id="b")

    assert calls == []
    assert _db_authority_snapshot(db) == before


def test_pending_migration_cannot_run_before_live_leader_rejection(
    tmp_path, monkeypatch
):
    db = _db(tmp_path)
    leader = Coordinator(db_path=db, coordinator_id="a")
    with leader.store.tx():
        leader.store.db.execute("DELETE FROM meta WHERE key='schema_version'")
    before = _db_authority_snapshot(db)
    calls = []

    def forbidden_initialize(self):
        calls.append(self)
        raise AssertionError("migration ran before leader admission")

    monkeypatch.setattr(Store, "initialize", forbidden_initialize)
    with pytest.raises(CoordinatorConflict, match="single-instance"):
        Coordinator(db_path=db, coordinator_id="b")

    assert calls == []
    assert _db_authority_snapshot(db) == before


def test_leadership_disabled_peer_still_cannot_migrate_under_live_leader(
    tmp_path, monkeypatch
):
    db = _db(tmp_path)
    leader = Coordinator(db_path=db, coordinator_id="a")
    with leader.store.tx():
        leader.store.db.execute("DELETE FROM meta WHERE key='schema_version'")
    before = _db_authority_snapshot(db)
    calls = []

    def forbidden_initialize(self):
        calls.append(self)
        raise AssertionError("leadership-disabled peer migrated under live leader")

    monkeypatch.setattr(Store, "initialize", forbidden_initialize)
    with pytest.raises(CoordinatorConflict, match="single-instance"):
        Coordinator(
            db_path=db, coordinator_id="b", enforce_single_coordinator=False
        )

    assert calls == []
    assert _db_authority_snapshot(db) == before


def test_reused_coordinator_label_cannot_take_over_live_instance(tmp_path):
    db = _db(tmp_path)
    leader = Coordinator(db_path=db, coordinator_id="same-label")
    before = _db_authority_snapshot(db)

    with pytest.raises(CoordinatorConflict, match="single-instance"):
        Coordinator(db_path=db, coordinator_id="same-label")

    assert leader.leader_epoch == 1
    assert _db_authority_snapshot(db) == before


def test_unknown_schema_version_is_rejected_without_mutation(tmp_path):
    db = _db(tmp_path)
    original = Coordinator(db_path=db, coordinator_id="original")
    original.resign()
    with original.store.tx():
        original.store.set_meta("schema_version", "omd/future-999")
    before = _db_authority_snapshot(db)

    with pytest.raises(UnsupportedSchemaVersion, match="unsupported OMD schema"):
        Coordinator(db_path=db, coordinator_id="probe")

    assert _db_authority_snapshot(db) == before


def test_post_migration_activation_failure_is_retryable_and_releases_leader(
    tmp_path, monkeypatch
):
    db = _db(tmp_path)
    real_migrate = Store._migrate

    def migrate_then_fail(self):
        real_migrate(self)
        raise RuntimeError("injected post-migration activation failure")

    monkeypatch.setattr(Store, "_migrate", migrate_then_fail)
    with pytest.raises(RuntimeError, match="injected post-migration"):
        Coordinator(db_path=db, coordinator_id="failed-start")

    with sqlite3.connect(db) as raw:
        assert raw.execute(
            "SELECT value FROM meta WHERE key='schema_version'"
        ).fetchone() is None
        leader = raw.execute(
            "SELECT value FROM meta WHERE key='leader_lease'"
        ).fetchone()[0]
    assert '"last_heartbeat": 0' in leader

    monkeypatch.setattr(Store, "_migrate", real_migrate)
    recovered = Coordinator(db_path=db, coordinator_id="recovered-start")
    assert recovered.store.schema_current() is True


def test_pending_migration_cannot_cross_live_split_effect(
    tmp_path, monkeypatch
):
    db = _db(tmp_path)
    holder = Coordinator(
        db_path=db, coordinator_id="holder", enforce_single_coordinator=False
    )
    with holder.store.tx():
        holder.store.db.execute("DELETE FROM meta WHERE key='schema_version'")
    before = _db_authority_snapshot(db)
    calls = []

    def forbidden_initialize(self):
        calls.append(self)
        raise AssertionError("migration crossed a live split effect")

    monkeypatch.setattr(Store, "initialize", forbidden_initialize)
    with holder._connect_effect(blocking=True) as acquired:
        assert acquired is True
        with pytest.raises(RuntimeError, match="exclusive split-effect authority"):
            Coordinator(
                db_path=db, coordinator_id="probe",
                enforce_single_coordinator=False,
            )

    assert calls == []
    assert _db_authority_snapshot(db) == before


# ---------- 4) 죽은 리더는 takeover (크래시 → 새 코디네이터 입장) ----------
def test_dead_leader_can_be_taken_over(tmp_path):
    db = _db(tmp_path)
    # 아주 짧은 TTL 로 a 를 띄움 → heartbeat 안 하면 즉시 '죽은 리더'로 보임.
    a = Coordinator(db_path=db, coordinator_id="a", leader_ttl=0.0)
    assert a.leader_epoch == 1
    # b 가 같은 DB 로 기동 — a 의 lease 가 (TTL=0 이라) 죽은 것으로 보여 takeover.
    b = Coordinator(db_path=db, coordinator_id="b", leader_ttl=30.0)
    assert b.leader_epoch == 2, "takeover 는 epoch +1 로 fence"
    assert b.store.get_leader()["coordinator_id"] == "b"
    # b 는 정상 변이.
    assert b.claim("ag", ["x/**"], "write")["state"] == "HELD"


# ---------- 5) 오추방: takeover 된 좀비 리더의 변이는 차단 (writer 둘 방지) ----------
def test_taken_over_zombie_leader_cannot_mutate(tmp_path):
    db = _db(tmp_path)
    a = Coordinator(db_path=db, coordinator_id="a", leader_ttl=0.0)   # 곧 죽은 것처럼 보임
    b = Coordinator(db_path=db, coordinator_id="b", leader_ttl=30.0)  # takeover (epoch 2)
    assert b.leader_epoch == 2
    # a 가 GC-pause 에서 깨어나 변이 시도 → leader-fence(_assert_leader)가 차단.
    with pytest.raises(CoordinatorConflict, match="no longer leader"):
        a.claim("ag-a", ["y/**"], "write")
    # b 는 멀쩡히 변이(리더는 정확히 하나).
    assert b.claim("ag-b", ["y/**"], "write")["state"] == "HELD"


# ---------- 6) heartbeat 가 lease 를 살아있게 유지 → takeover 거부 ----------
def test_heartbeat_keeps_lease_alive(tmp_path):
    db = _db(tmp_path)
    a = Coordinator(db_path=db, coordinator_id="a", leader_ttl=30.0)
    hb = a.coordinator_heartbeat()
    assert hb["ok"] and hb["epoch"] == 1
    # heartbeat 직후라 살아있음 → 둘째 기동 거부.
    with pytest.raises(CoordinatorConflict):
        Coordinator(db_path=db, coordinator_id="b", leader_ttl=30.0)


# ---------- 7) resign 후 즉시 takeover (graceful shutdown) ----------
def test_resign_allows_immediate_takeover(tmp_path):
    db = _db(tmp_path)
    a = Coordinator(db_path=db, coordinator_id="a", leader_ttl=30.0)
    assert a.resign()["ok"]
    # a 가 사임 → TTL 대기 없이 b 가 즉시 takeover.
    b = Coordinator(db_path=db, coordinator_id="b", leader_ttl=30.0)
    assert b.leader_epoch == 2
    # 사임한 a 는 더 이상 변이 못 함(좀비 리더).
    with pytest.raises(CoordinatorConflict):
        a.coordinator_heartbeat()
    with pytest.raises(CoordinatorConflict):
        a.claim("resigned-agent", ["resigned/**"])
    assert a.resign()["noop"] is True


# ---------- 8) 좀비 리더가 heartbeat 로 부활 못 함 (epoch fence) ----------
def test_zombie_leader_heartbeat_is_fenced(tmp_path):
    db = _db(tmp_path)
    a = Coordinator(db_path=db, coordinator_id="a", leader_ttl=0.0)
    b = Coordinator(db_path=db, coordinator_id="b", leader_ttl=30.0)   # takeover, epoch 2
    # a 가 heartbeat 로 자기 lease 를 되살리려 해도 → _assert_leader 가 epoch 불일치로 차단.
    with pytest.raises(CoordinatorConflict, match="no longer leader"):
        a.coordinator_heartbeat()
    # b 의 lease 는 온전(epoch 2, b 소유).
    L = b.store.get_leader()
    assert L["coordinator_id"] == "b" and L["epoch"] == 2


# ============ 변이검증(mutation check) — 가드 무력화하면 RED ============
def test_MUTATION_single_instance_enforced(tmp_path):
    """가드 존재성: 한 DB 에 살아있는 리더가 있으면 둘째 코디네이터는 절대 leader_epoch 를
    못 얻는다. _acquire_leadership 의 살아있는-리더 거부 분기를 무력화하면 둘째가 기동에
    성공해(writer 둘) 이 단언이 깨진다(RED)."""
    db = _db(tmp_path)
    a = Coordinator(db_path=db, coordinator_id="a", leader_ttl=30.0)
    raised = False
    try:
        Coordinator(db_path=db, coordinator_id="b", leader_ttl=30.0)
    except CoordinatorConflict:
        raised = True
    assert raised, "살아있는 리더가 있는 DB 에 둘째 코디네이터가 기동되면 actor 둘 = writer 둘!"
    # 리더는 정확히 하나(a).
    assert a.store.get_leader()["coordinator_id"] == "a"
