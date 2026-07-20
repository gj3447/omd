"""Production bindings for semantic admission lifecycle maintenance events."""

from __future__ import annotations

from omd_server import Coordinator
from omd_server.admission_contract import project_legacy, step


OWNER_EVENTS = {
    "RENEW",
    "RELEASE",
    "WAIT_OWNER_RECLAIMED",
    "LEASE_OWNER_RECLAIMED",
}
FENCE_EVENTS = {"RENEW", "RELEASE", "LEASE_EXPIRED", "LEASE_OWNER_RECLAIMED"}
EXPECTED = {
    "RENEW": ("HELD", "HELD"),
    "RELEASE": ("RELEASED", "RELEASED"),
    "LEASE_EXPIRED": ("EXPIRED", "EXPIRED"),
    "WAIT_OWNER_RECLAIMED": ("CANCELLED", "DENIED"),
    "LEASE_OWNER_RECLAIMED": ("EXPIRED", "EXPIRED"),
}


def _assert_semantic_projection(omd, row, event_type):
    identity = omd._admission_identity(row)
    payload = {
        "repository_id": identity["repository_id"],
        "request_id": identity["request_id"],
        "orbit_id": identity["orbit_id"],
        "request_generation": identity["request_generation"],
        "actor": omd.coordinator_id,
        "authority_snapshot_hash": row["authority_snapshot_hash"],
        "event_id": f"replay-{event_type.lower()}",
    }
    if event_type in OWNER_EVENTS:
        payload.update(
            owner_agent=identity["owner_agent"],
            bail_epoch=identity["bail_epoch"],
        )
    if event_type in FENCE_EVENTS:
        payload["fence"] = row["fence"]
    if event_type == "RENEW":
        payload["lease_deadline"] = row["expires_at"]
    elif event_type == "LEASE_EXPIRED":
        payload["observed_at"] = row["released_at"]
    elif event_type == "WAIT_OWNER_RECLAIMED":
        payload["no_lease_fence"] = 0

    source_state = "PENDING" if event_type == "WAIT_OWNER_RECLAIMED" else "HELD"
    context = {**identity, "state": source_state}
    if row["fence"] is not None:
        context["fence"] = row["fence"]
    if event_type == "LEASE_EXPIRED":
        context["lease_deadline"] = row["expires_at"]
    reduced = step(
        context,
        event_type,
        payload,
        trusted_authority_snapshot_hash=row["authority_snapshot_hash"],
    )
    semantic_state, legacy_state = EXPECTED[event_type]
    assert reduced.accepted and reduced.context["state"] == semantic_state
    assert project_legacy(reduced.context["state"], reduced.context).state == legacy_state


def test_public_renew_and_release_persist_semantic_lifecycle_projection(tmp_path):
    omd = Coordinator(str(tmp_path / "omd.db"), agent_ttl=None)
    held = omd.claim("owner", ["src/**"], request_id="claim-owner")

    renewed = omd.renew(
        held["orbit_id"],
        "owner",
        held["fence"],
        ttl=30,
        request_id="renew-owner",
        bail_epoch=held["bail_epoch"],
    )
    assert renewed["ok"] is True
    row = omd.store.get_orbit(held["orbit_id"])
    assert row["state"] == "HELD" and row["decision_type"] == "RENEW"
    assert row["decision_id"] is None
    _assert_semantic_projection(omd, row, "RENEW")

    released = omd.release(
        held["orbit_id"],
        "owner",
        held["fence"],
        request_id="release-owner",
        bail_epoch=held["bail_epoch"],
    )
    assert released["ok"] is True
    row = omd.store.get_orbit(held["orbit_id"])
    assert row["state"] == "RELEASED" and row["decision_type"] == "RELEASE"
    assert row["released_at"] is not None and row["decision_id"] is None
    _assert_semantic_projection(omd, row, "RELEASE")
    omd.close()


def test_due_lease_expiry_is_reduced_before_legacy_projection(tmp_path):
    omd = Coordinator(str(tmp_path / "omd.db"), agent_ttl=None)
    held = omd.claim("owner", ["src/**"], request_id="expiring-claim")
    with omd.store.tx():
        omd.store.set_orbit(held["orbit_id"], expires_at=1.0)
    omd.sweep()

    row = omd.store.get_orbit(held["orbit_id"])
    assert row["state"] == "EXPIRED"
    assert row["decision_type"] == "LEASE_EXPIRED"
    assert row["terminal_reason"] == "lease_expired"
    _assert_semantic_projection(omd, row, "LEASE_EXPIRED")
    omd.close()


def test_owner_reclaim_binds_pending_and_held_rows_to_distinct_events(tmp_path):
    omd = Coordinator(str(tmp_path / "omd.db"), agent_ttl=None)
    omd.claim("blocker", ["a/**"])
    pending = omd.claim("victim", ["a/**"], request_id="victim-wait")
    held = omd.claim("victim", ["b/**"], request_id="victim-held")
    assert pending["state"] == "PENDING" and held["state"] == "HELD"

    reclaimed = omd.bail("victim", request_id="victim-bail")
    assert reclaimed["agent"] == "victim"
    pending_row = omd.store.get_orbit(pending["orbit_id"])
    held_row = omd.store.get_orbit(held["orbit_id"])
    assert (pending_row["state"], pending_row["decision_type"]) == (
        "DENIED", "WAIT_OWNER_RECLAIMED"
    )
    assert pending_row["terminal_reason"] == "wait_owner_reclaimed"
    assert (held_row["state"], held_row["decision_type"]) == (
        "EXPIRED", "LEASE_OWNER_RECLAIMED"
    )
    assert held_row["terminal_reason"] == "lease_owner_reclaimed"
    _assert_semantic_projection(omd, pending_row, "WAIT_OWNER_RECLAIMED")
    _assert_semantic_projection(omd, held_row, "LEASE_OWNER_RECLAIMED")
    omd.close()


def test_connect_internal_release_uses_same_semantic_binding(tmp_path):
    omd = Coordinator(str(tmp_path / "omd.db"), agent_ttl=None)
    omd.declare("task", writes=["src/**"])
    omd.next_task("owner")
    held = omd.claim(
        "owner", ["src/**"], task_id="task", request_id="task-claim"
    )
    omd.start("task", "owner")
    omd.finish("task")
    connected = omd.connect("task")
    assert connected["ok"] is True

    row = omd.store.get_orbit(held["orbit_id"])
    assert row["state"] == "RELEASED" and row["decision_type"] == "RELEASE"
    _assert_semantic_projection(omd, row, "RELEASE")
    omd.close()


def test_reducer_rejection_rolls_back_renewal_projection(tmp_path, monkeypatch):
    omd = Coordinator(str(tmp_path / "omd.db"), agent_ttl=None)
    held = omd.claim("owner", ["src/**"])
    before = omd.store.get_orbit(held["orbit_id"])

    def reject(*args, **kwargs):
        raise RuntimeError("synthetic reducer rejection")

    monkeypatch.setattr(omd, "_assert_admission_projection", reject)
    try:
        omd.renew(held["orbit_id"], "owner", held["fence"], ttl=30)
    except RuntimeError as exc:
        assert "synthetic reducer rejection" in str(exc)
    else:  # pragma: no cover - fail loudly if production stops invoking the reducer
        raise AssertionError("renew bypassed the semantic reducer")
    after = omd.store.get_orbit(held["orbit_id"])
    assert after["expires_at"] == before["expires_at"]
    assert after["decision_type"] == before["decision_type"]
    omd.close()
