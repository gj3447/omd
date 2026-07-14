"""FenceVector construction and integrity digest."""

from __future__ import annotations

import hashlib
import json

from .model import ClaimRecord, FenceEntry, FenceVector, Principal
from .resource import claim_spec_key


def fence_digest(
    claim_id: str, owner: Principal, entries: tuple[FenceEntry, ...]
) -> str:
    payload = {
        "claim_id": claim_id,
        "owner": [owner.client_id, owner.agent_id, owner.session_epoch],
        "entries": [
            [
                entry.resource.domain_id,
                entry.resource.repo_id,
                list(entry.resource.segments),
                entry.resource.selector.value,
                entry.grant_epoch,
            ]
            for entry in entries
        ],
    }
    encoded = json.dumps(
        payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def make_fence(record: ClaimRecord, grant_epoch: int) -> FenceVector:
    entries = tuple(
        FenceEntry(spec.resource, grant_epoch)
        for spec in sorted(record.claims, key=claim_spec_key)
    )
    return FenceVector(
        claim_id=record.claim_id,
        owner=record.owner,
        entries=entries,
        vector_digest=fence_digest(record.claim_id, record.owner, entries),
    )
