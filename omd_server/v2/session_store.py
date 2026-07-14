"""Persistent transport-session registration for the SQLite adapter."""

from __future__ import annotations

from dataclasses import replace
from typing import Callable

from .codec import encode_effect, encode_event, encode_state
from .kernel import assert_invariants, effects_for_events, evolve, maintenance_events
from .model import Principal
from .store_types import RevisionConflict


class SessionRegistryMixin:
    """Issue epochs and linearize rollover maintenance in one writer lock."""

    def register_session(
        self,
        *,
        domain_id: str,
        client_id: str,
        agent_id: str,
        registered_at_ms: int | None = None,
        clock_ms: Callable[[], int] | None = None,
    ) -> Principal:
        """Issue the next persistent epoch for one transport-bound agent.

        A clock callable is sampled only after ``BEGIN IMMEDIATE`` owns the
        writer lock. Tests and deterministic callers may instead supply an
        explicit timestamp, but production runtimes should pass ``clock_ms``.
        """

        if (
            not client_id
            or not agent_id
            or "\x00" in client_id
            or "\x00" in agent_id
            or (registered_at_ms is None) == (clock_ms is None)
        ):
            raise ValueError("invalid session registration")
        if registered_at_ms is not None and (
            type(registered_at_ms) is not int or registered_at_ms < 0
        ):
            raise ValueError("invalid session registration")

        connection = self._connect()
        try:
            with self._transaction(connection, write=True):
                snapshot = self._load_domain(connection, domain_id)
                sampled_now_ms = (
                    clock_ms() if clock_ms is not None else registered_at_ms
                )
                if type(sampled_now_ms) is not int or sampled_now_ms < 0:
                    raise ValueError("clock must return a nonnegative integer")
                resolved_now_ms = max(snapshot.state.last_now_ms, sampled_now_ms)

                row = connection.execute(
                    """
                    INSERT INTO sessions(
                        domain_id, client_id, agent_id,
                        current_epoch, registered_at_ms
                    ) VALUES (?, ?, ?, 1, ?)
                    ON CONFLICT(domain_id, client_id, agent_id) DO UPDATE SET
                        current_epoch = current_epoch + 1,
                        registered_at_ms = excluded.registered_at_ms
                    RETURNING current_epoch
                    """,
                    (domain_id, client_id, agent_id, resolved_now_ms),
                ).fetchone()
                assert row is not None
                principal = Principal(client_id, agent_id, int(row[0]))
                sessions = dict(snapshot.state.session_epochs)
                sessions[(client_id, agent_id)] = principal.session_epoch
                session_state = replace(snapshot.state, session_epochs=sessions)
                events = maintenance_events(session_state, resolved_now_ms)
                if not events:
                    assert_invariants(session_state)
                    return principal

                revision = snapshot.revision + 1
                next_state = evolve(session_state, events)
                assert_invariants(next_state)
                for seq, event in enumerate(events):
                    kind, payload = encode_event(event)
                    connection.execute(
                        """
                        INSERT INTO domain_events(
                            domain_id, revision, seq, event_kind, payload_json
                        ) VALUES (?, ?, ?, ?, ?)
                        """,
                        (domain_id, revision, seq, kind, payload),
                    )
                for seq, effect in enumerate(effects_for_events(events)):
                    effect_kind, effect_payload = encode_effect(effect)
                    connection.execute(
                        """
                        INSERT INTO outbox(
                            domain_id, revision, seq, effect_kind, payload_json,
                            status, attempts, created_at_ms
                        ) VALUES (?, ?, ?, ?, ?, 'pending', 0, ?)
                        """,
                        (
                            domain_id,
                            revision,
                            seq,
                            effect_kind,
                            effect_payload,
                            resolved_now_ms,
                        ),
                    )
                updated = connection.execute(
                    """
                    UPDATE domains
                    SET revision=?, state_json=?, updated_at_ms=?
                    WHERE domain_id=? AND revision=?
                    RETURNING revision
                    """,
                    (
                        revision,
                        encode_state(next_state),
                        resolved_now_ms,
                        domain_id,
                        snapshot.revision,
                    ),
                ).fetchone()
                if updated is None:
                    raise RevisionConflict(snapshot.revision, -1)
                return principal
        finally:
            connection.close()
