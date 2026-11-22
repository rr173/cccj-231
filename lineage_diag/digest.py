"""Deterministic canonical hashing used for schema/sample consistency.

The hashing rules are part of the tool contract: the same logical schema or
sample set must always produce the same digest regardless of JSON key order.
"""

from __future__ import annotations

import hashlib
import json
from typing import Any, Mapping, Sequence


def canonical_json(payload: Any) -> str:
    """Serialize *payload* with sorted keys and stable separators."""
    return json.dumps(payload, sort_keys=True, ensure_ascii=False,
                      separators=(",", ":"), default=str)


def sha256_text(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def digest_payload(payload: Any) -> str:
    """SHA-256 of the canonical JSON representation of *payload*."""
    return sha256_text(canonical_json(payload))


def schema_digest(columns: Sequence[Mapping[str, Any]]) -> str:
    """Digest of an ordered package schema.

    Only column name and declared type participate in the schema digest;
    sample summaries are compared separately because they describe observed
    data rather than the declared contract.
    """
    normalized = [
        {"name": str(c["name"]), "type": str(c["type"])}
        for c in columns
    ]
    return digest_payload({"schema": normalized})


def sample_digest(samples: Mapping[str, Any]) -> str:
    """Digest of one output column's recorded sample summary."""
    return digest_payload({"sample": samples})
