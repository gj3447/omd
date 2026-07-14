"""Versioned SQLite schema for the OMD v2 local coordination store."""

SCHEMA_VERSION = 3

SCHEMA = """
CREATE TABLE IF NOT EXISTS schema_meta (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
) STRICT;

CREATE TABLE IF NOT EXISTS domains (
    domain_id TEXT PRIMARY KEY CHECK(length(domain_id) > 0),
    revision INTEGER NOT NULL CHECK(revision >= 0),
    codec_version INTEGER NOT NULL CHECK(codec_version = 2),
    state_json TEXT NOT NULL,
    created_at_ms INTEGER NOT NULL CHECK(created_at_ms >= 0),
    updated_at_ms INTEGER NOT NULL CHECK(updated_at_ms >= 0)
) STRICT;

CREATE TABLE IF NOT EXISTS idempotency_keys (
    domain_id TEXT NOT NULL REFERENCES domains(domain_id) ON DELETE CASCADE,
    client_id TEXT NOT NULL CHECK(length(client_id) > 0),
    request_id TEXT NOT NULL CHECK(length(request_id) > 0),
    fingerprint TEXT NOT NULL CHECK(length(fingerprint) = 64),
    operation_id TEXT NOT NULL CHECK(length(operation_id) > 0),
    claim_id TEXT,
    frozen_error_json TEXT,
    recorded_revision INTEGER NOT NULL CHECK(recorded_revision > 0),
    PRIMARY KEY(domain_id, client_id, request_id)
) STRICT;

CREATE TABLE IF NOT EXISTS sessions (
    domain_id TEXT NOT NULL REFERENCES domains(domain_id) ON DELETE CASCADE,
    client_id TEXT NOT NULL CHECK(length(client_id) > 0),
    agent_id TEXT NOT NULL CHECK(length(agent_id) > 0),
    current_epoch INTEGER NOT NULL CHECK(current_epoch >= 1),
    registered_at_ms INTEGER NOT NULL CHECK(registered_at_ms >= 0),
    PRIMARY KEY(domain_id, client_id, agent_id)
) STRICT;

CREATE TABLE IF NOT EXISTS domain_events (
    domain_id TEXT NOT NULL REFERENCES domains(domain_id) ON DELETE CASCADE,
    revision INTEGER NOT NULL CHECK(revision > 0),
    seq INTEGER NOT NULL CHECK(seq >= 0),
    event_kind TEXT NOT NULL CHECK(length(event_kind) > 0),
    payload_json TEXT NOT NULL,
    PRIMARY KEY(domain_id, revision, seq)
) STRICT;

CREATE TABLE IF NOT EXISTS outbox (
    domain_id TEXT NOT NULL REFERENCES domains(domain_id) ON DELETE CASCADE,
    revision INTEGER NOT NULL CHECK(revision > 0),
    seq INTEGER NOT NULL CHECK(seq >= 0),
    effect_kind TEXT NOT NULL CHECK(length(effect_kind) > 0),
    payload_json TEXT NOT NULL,
    status TEXT NOT NULL CHECK(status IN ('pending', 'delivered', 'failed')),
    attempts INTEGER NOT NULL DEFAULT 0 CHECK(attempts >= 0),
    created_at_ms INTEGER NOT NULL CHECK(created_at_ms >= 0),
    delivered_at_ms INTEGER,
    PRIMARY KEY(domain_id, revision, seq)
) STRICT;

CREATE INDEX IF NOT EXISTS idx_outbox_pending
    ON outbox(domain_id, status, revision, seq);
"""
