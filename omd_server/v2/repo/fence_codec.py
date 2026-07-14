"""Strict JSON codec for the complete durable FenceVector handoff."""

from __future__ import annotations

import json

from ..fencing import fence_digest
from ..model import FenceEntry, FenceVector, Principal
from ..resource import ResourceId, SelectorKind


def encode_fence(fence: FenceVector) -> str:
    payload = {
        "claim_id": fence.claim_id,
        "owner": {
            "client_id": fence.owner.client_id,
            "agent_id": fence.owner.agent_id,
            "session_epoch": fence.owner.session_epoch,
        },
        "entries": [
            {
                "domain_id": item.resource.domain_id,
                "repo_id": item.resource.repo_id,
                "segments": list(item.resource.segments),
                "selector": item.resource.selector.value,
                "grant_epoch": item.grant_epoch,
            }
            for item in fence.entries
        ],
        "vector_digest": fence.vector_digest,
    }
    return json.dumps(
        payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False
    )


def decode_fence(raw: str) -> FenceVector:
    payload = json.loads(raw)
    if not isinstance(payload, dict) or set(payload) != {
        "claim_id",
        "owner",
        "entries",
        "vector_digest",
    }:
        raise ValueError("invalid fence codec shape")
    owner_raw = payload["owner"]
    entries_raw = payload["entries"]
    if not isinstance(owner_raw, dict) or set(owner_raw) != {
        "client_id",
        "agent_id",
        "session_epoch",
    }:
        raise ValueError("invalid fence owner")
    if not isinstance(entries_raw, list) or not entries_raw:
        raise ValueError("invalid fence entries")
    owner = Principal(
        client_id=owner_raw["client_id"],
        agent_id=owner_raw["agent_id"],
        session_epoch=owner_raw["session_epoch"],
    )
    entries: list[FenceEntry] = []
    for item in entries_raw:
        if not isinstance(item, dict) or set(item) != {
            "domain_id",
            "repo_id",
            "segments",
            "selector",
            "grant_epoch",
        }:
            raise ValueError("invalid fence entry")
        resource = ResourceId(
            domain_id=item["domain_id"],
            repo_id=item["repo_id"],
            segments=tuple(item["segments"]),
            selector=SelectorKind(item["selector"]),
        )
        entries.append(FenceEntry(resource, item["grant_epoch"]))
    result = FenceVector(
        claim_id=payload["claim_id"],
        owner=owner,
        entries=tuple(entries),
        vector_digest=payload["vector_digest"],
    )
    if fence_digest(result.claim_id, result.owner, result.entries) != result.vector_digest:
        raise ValueError("persisted fence integrity mismatch")
    return result
