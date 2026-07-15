"""CLI ↔ Coordinator surface parity and graceful leader handoff."""

from __future__ import annotations

import json

import pytest

from omd_server import cli


class SpyCoordinator:
    instances: list["SpyCoordinator"] = []
    fail_method: str | None = None

    def __init__(self, db_path):
        self.db_path = db_path
        self.calls = []
        self.resigned = False
        self.closed = False
        self.__class__.instances.append(self)

    def __getattr__(self, method):
        def call(*args, **kwargs):
            self.calls.append((method, args, kwargs))
            if method == self.fail_method:
                raise RuntimeError("boom")
            return {"method": method}

        return call

    def resign(self):
        self.resigned = True
        return {"ok": True}

    def close(self):
        self.closed = True


@pytest.fixture
def spy(monkeypatch):
    SpyCoordinator.instances = []
    SpyCoordinator.fail_method = None
    monkeypatch.setattr(cli, "Coordinator", SpyCoordinator)
    return SpyCoordinator


def _invoke(spy, capsys, *argv):
    cli.main(["--db", "test.db", *argv])
    instance = spy.instances[-1]
    payload = json.loads(capsys.readouterr().out)
    assert instance.db_path == "test.db"
    assert instance.resigned is True
    assert instance.closed is True
    return instance.calls[-1], payload


def test_declare_shared_and_task_conditions_are_forwarded(spy, capsys):
    call, payload = _invoke(
        spy, capsys, "declare", "T", "--name", "task", "--writes", "src/a.py",
        "--reads", "src/in.py", "--shared", "constants/env.py", "--deps", "D",
        "--priority", "7",
    )
    assert call == (
        "declare",
        ("T",),
        {
            "name": "task",
            "writes": ["src/a.py"],
            "reads": ["src/in.py"],
            "deps": ["D"],
            "priority": 7,
            "shared": ["constants/env.py"],
        },
    )
    assert payload == {"method": "declare"}

    call, _ = _invoke(spy, capsys, "task-conditions", "T")
    assert call == ("task_conditions", ("T",), {})


def test_begin_forwards_full_onboarding_contract(spy, capsys):
    call, _ = _invoke(
        spy, capsys, "begin", "T", "worker", "--writes", "src/a.py", "src/b.py",
        "--reads", "schema.json", "--shared", "constants/env.py", "--deps", "D1", "D2",
        "--priority", "4", "--name", "onboard", "--ttl", "900",
        "--liveness-ttl", "3600", "--request-id", "req-begin", "--bail-epoch", "3",
    )
    assert call == (
        "begin",
        ("T", "worker", ["src/a.py", "src/b.py"]),
        {
            "reads": ["schema.json"],
            "shared": ["constants/env.py"],
            "deps": ["D1", "D2"],
            "priority": 4,
            "name": "onboard",
            "ttl": 900.0,
            "liveness_ttl": 3600.0,
            "request_id": "req-begin",
            "bail_epoch": 3,
        },
    )


@pytest.mark.parametrize(
    ("argv", "expected"),
    [
        (
            ["connect", "T", "--agent", "worker", "--fence", "9", "--push", "origin",
             "--request-id", "req-c", "--bail-epoch", "2"],
            ("connect", ("T", "worker", 9),
             {"push": "origin", "request_id": "req-c", "bail_epoch": 2}),
        ),
        (
            ["complete-task", "T", "--agent", "worker", "--fence", "9", "--push", "origin",
             "--request-id", "req-done", "--bail-epoch", "2"],
            ("complete_task", ("T", None, "worker", 9),
             {"push": "origin", "request_id": "req-done", "bail_epoch": 2}),
        ),
        (
            ["complete-task", "T", "ship it"],
            ("complete_task", ("T", "ship it", None, None),
             {"push": None, "request_id": None, "bail_epoch": None}),
        ),
        (
            ["cancel", "T", "--reason", "obsolete", "--request-id", "req-x"],
            ("cancel", ("T",), {"reason": "obsolete", "request_id": "req-x"}),
        ),
        (
            ["cancel-wait", "orb-1", "worker", "4", "--bail-epoch", "2",
             "--request-id", "req-cancel-wait"],
            ("cancel_wait", ("orb-1", "worker", 4),
             {"bail_epoch": 2, "request_id": "req-cancel-wait"}),
        ),
        (
            ["barrier-consume", "ready", "--agent", "worker", "--request-id", "req-b",
             "--bail-epoch", "6"],
            ("barrier_consume", ("ready", "worker"),
             {"request_id": "req-b", "bail_epoch": 6}),
        ),
        (
            ["heartbeat", "worker", "--ttl", "1800"],
            ("heartbeat", ("worker",), {"ttl": 1800.0}),
        ),
    ],
)
def test_missing_cli_surfaces_forward_all_options(spy, capsys, argv, expected):
    call, _ = _invoke(spy, capsys, *argv)
    assert call == expected


def test_cleanup_runs_when_command_raises(spy):
    spy.fail_method = "status"
    with pytest.raises(RuntimeError, match="boom"):
        cli.main(["--db", "test.db", "status"])
    instance = spy.instances[-1]
    assert instance.resigned is True
    assert instance.closed is True


def test_sequential_real_cli_invocations_do_not_wait_for_leader_ttl(tmp_path, capsys):
    db = str(tmp_path / "omd.db")
    cli.main(["--db", db, "status"])
    cli.main(["--db", db, "status"])
    # Without graceful resign, the second Coordinator constructor raises a live-leader conflict.
    assert capsys.readouterr().out.count('"tasks"') == 2
