"""Terminal outcome and durable authority-settlement behavior."""

from __future__ import annotations

from typing import Callable

from .contracts import MutationAuthority, SagaRecord, SagaStatus
from .errors import RepoConfigurationError
from .store import SQLiteRepoSagaStore


class SagaSettlementMixin:
    """Shared settlement shell for a service with store/authority dependencies."""

    store: SQLiteRepoSagaStore
    authority: MutationAuthority
    _fault: Callable[[str], None]

    def _terminal(
        self,
        record: SagaRecord,
        status: SagaStatus,
        code: str,
        detail: str,
    ) -> SagaRecord:
        terminal = self.store.record_terminal(
            record.operation_id,
            expected=(record.status,),
            status=status,
            error_code=code,
            detail=detail or code,
        )
        if status is SagaStatus.IN_DOUBT:
            return terminal
        return self._settle_terminal(terminal)

    def _quarantine_existing(
        self, operation_id: str, error: RepoConfigurationError
    ) -> SagaRecord:
        try:
            record = self.store.load(operation_id)
        except KeyError:
            raise error
        if record.status.terminal:
            return self._settle_terminal(record)
        if record.status is SagaStatus.APPLIED:
            return self._settle_applied(record)
        return self._terminal(
            record,
            SagaStatus.IN_DOUBT,
            "LIVE_REPOSITORY_DRIFT",
            str(error),
        )

    def _settle_terminal(self, record: SagaRecord) -> SagaRecord:
        if (
            record.settled
            or record.reservation_id is None
            or not record.status.automatically_settleable
        ):
            return record
        try:
            self.authority.settle(record.reservation_id, record.status.value)
            self._fault("after_authority_settle_before_receipt")
            return self.store.record_terminal_settled(record.operation_id)
        except Exception:
            return self.store.load(record.operation_id)

    def _settle_applied(self, record: SagaRecord) -> SagaRecord:
        if record.status is SagaStatus.RECEIPTED:
            return record
        if record.reservation_id is None:
            return self._terminal(
                record,
                SagaStatus.IN_DOUBT,
                "MISSING_RESERVATION",
                "applied saga has no reservation receipt",
            )
        try:
            self.authority.settle(record.reservation_id, "applied")
            self._fault("after_authority_settle_before_receipt")
        except Exception:
            return record
        return self.store.record_receipted(record.operation_id)
