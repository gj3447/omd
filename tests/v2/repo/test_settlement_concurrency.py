from __future__ import annotations

import threading

import pytest

from omd_server.v2.repo import IntegrateRequest, RepoSagaService, SQLiteRepoSagaStore, SagaStatus
from omd_server.v2.resource import AccessMode, SelectorKind

from .conftest import FakeAuthority, RepoFixture, claim, make_fence


class SimulatedCrash(BaseException):
    pass


class BarrierAuthority(FakeAuthority):
    def __init__(self, claims):
        super().__init__(claims)
        self.barrier = threading.Barrier(2)

    def settle(self, reservation_id: str, outcome: str) -> None:
        self.barrier.wait(timeout=5)
        super().settle(reservation_id, outcome)


def test_concurrent_applied_settlement_returns_one_idempotent_receipt(
    repo_fixture: RepoFixture,
) -> None:
    claims = (claim(path="src", mode=AccessMode.WRITE, selector=SelectorKind.SUBTREE),)
    authority = BarrierAuthority(claims)
    integrate_request = IntegrateRequest(
        protocol_version=1,
        operation_id="op-concurrent-settle",
        domain_id="symposium",
        client_id="client",
        request_id="request-concurrent-settle",
        repo_id="repo",
        claim_id="claim-1",
        fence=make_fence(claims),
        source_oid=repo_fixture.source_oid,
        read_base_oid=repo_fixture.base_oid,
    )

    def crash(point: str) -> None:
        if point == "after_applied_recorded":
            raise SimulatedCrash

    store = SQLiteRepoSagaStore(repo_fixture.state / "repo-sagas.db")
    with pytest.raises(SimulatedCrash):
        RepoSagaService(
            registry=repo_fixture.registry,
            store=store,
            authority=authority,
            fault_injector=crash,
        ).integrate(integrate_request)
    assert store.load("op-concurrent-settle").status is SagaStatus.APPLIED

    engine = RepoSagaService(
        registry=repo_fixture.registry,
        store=store,
        authority=authority,
    )
    results: list[object] = [None, None]

    def publish(index: int) -> None:
        try:
            results[index] = engine.publish("op-concurrent-settle")
        except BaseException as exc:
            results[index] = exc

    workers = [threading.Thread(target=publish, args=(index,)) for index in range(2)]
    for worker in workers:
        worker.start()
    for worker in workers:
        worker.join(timeout=10)

    assert all(not worker.is_alive() for worker in workers)
    assert all(getattr(result, "status", None) is SagaStatus.RECEIPTED for result in results)
    assert store.load("op-concurrent-settle").status is SagaStatus.RECEIPTED
    assert authority.settled == [("reservation:op-concurrent-settle", "applied")]
