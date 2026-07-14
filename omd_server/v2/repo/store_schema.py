"""Versioned STRICT SQLite schema for the isolated repo-saga database."""

from .contracts import SagaStatus


SCHEMA_VERSION = 1
VALID_STATUSES = ",".join(f"'{item.value}'" for item in SagaStatus)
SCHEMA = f"""
CREATE TABLE IF NOT EXISTS repo_schema_meta (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
) STRICT;

CREATE TABLE IF NOT EXISTS repo_sagas (
    operation_id TEXT PRIMARY KEY CHECK(length(operation_id) > 0),
    domain_id TEXT NOT NULL CHECK(length(domain_id) > 0),
    client_id TEXT NOT NULL CHECK(length(client_id) > 0),
    request_id TEXT NOT NULL CHECK(length(request_id) > 0),
    fingerprint TEXT NOT NULL CHECK(length(fingerprint) = 64),
    repo_id TEXT NOT NULL CHECK(length(repo_id) > 0),
    repository_identity TEXT NOT NULL CHECK(length(repository_identity) = 64),
    claim_id TEXT NOT NULL CHECK(length(claim_id) > 0),
    fence_json TEXT NOT NULL,
    fence_digest TEXT NOT NULL CHECK(length(fence_digest) = 64),
    source_oid TEXT NOT NULL CHECK(length(source_oid) IN (40, 64)),
    read_base_oid TEXT NOT NULL CHECK(length(read_base_oid) IN (40, 64)),
    target_ref TEXT NOT NULL CHECK(target_ref LIKE 'refs/heads/%'),
    expected_target_oid TEXT NOT NULL CHECK(length(expected_target_oid) IN (40, 64)),
    author_name TEXT NOT NULL,
    author_email TEXT NOT NULL,
    author_date TEXT NOT NULL,
    committer_name TEXT NOT NULL,
    committer_email TEXT NOT NULL,
    committer_date TEXT NOT NULL,
    message TEXT NOT NULL,
    status TEXT NOT NULL CHECK(status IN ({VALID_STATUSES})),
    revision INTEGER NOT NULL CHECK(revision >= 1),
    reservation_id TEXT,
    authority_claims_digest TEXT CHECK(
        authority_claims_digest IS NULL OR length(authority_claims_digest) = 64
    ),
    tree_oid TEXT CHECK(tree_oid IS NULL OR length(tree_oid) IN (40, 64)),
    candidate_oid TEXT CHECK(candidate_oid IS NULL OR length(candidate_oid) IN (40, 64)),
    receipt_kind TEXT,
    error_code TEXT,
    error_detail TEXT,
    settled INTEGER NOT NULL DEFAULT 0 CHECK(settled IN (0, 1)),
    created_at_ms INTEGER NOT NULL CHECK(created_at_ms >= 0),
    updated_at_ms INTEGER NOT NULL CHECK(updated_at_ms >= created_at_ms),
    UNIQUE(domain_id, client_id, request_id)
) STRICT;

CREATE TABLE IF NOT EXISTS repo_saga_events (
    operation_id TEXT NOT NULL REFERENCES repo_sagas(operation_id) ON DELETE CASCADE,
    revision INTEGER NOT NULL CHECK(revision >= 1),
    event_kind TEXT NOT NULL CHECK(length(event_kind) > 0),
    status TEXT NOT NULL CHECK(status IN ({VALID_STATUSES})),
    detail TEXT,
    created_at_ms INTEGER NOT NULL CHECK(created_at_ms >= 0),
    PRIMARY KEY(operation_id, revision)
) STRICT;
"""
