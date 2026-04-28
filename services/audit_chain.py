from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from typing import Any, Iterable, Mapping

from .base import DatabaseConnection, ValidationError, fetch_all


def canonical_json(payload: Mapping[str, Any]) -> str:
    return json.dumps(dict(payload), default=str, sort_keys=True, separators=(",", ":"))


def chain_hash(previous_hash: str, payload: Mapping[str, Any]) -> str:
    material = f"{previous_hash}:{canonical_json(payload)}"
    return hashlib.sha256(material.encode("utf-8")).hexdigest()


@dataclass(frozen=True)
class ChainRecord:
    entity_type: str
    entity_id: str
    sequence: int
    previous_hash: str
    record_hash: str


class AuditChainService:
    ROOT_HASH = "0" * 64

    def __init__(self, db: DatabaseConnection):
        self.db = db

    def latest_hash(self, entity_type: str, entity_id: str) -> str:
        cursor = self.db.execute(
            """
            SELECT record_hash
            FROM audit_chain
            WHERE entity_type = ? AND entity_id = ?
            ORDER BY sequence DESC
            LIMIT 1
            """,
            (entity_type, entity_id),
        )
        rows = fetch_all(cursor)
        return str(rows[0]["record_hash"]) if rows else self.ROOT_HASH

    def append(self, entity_type: str, entity_id: str, payload: Mapping[str, Any]) -> ChainRecord:
        previous = self.latest_hash(entity_type, entity_id)
        record_hash = chain_hash(previous, payload)
        cursor = self.db.execute(
            """
            SELECT COALESCE(MAX(sequence), 0) + 1 AS next_sequence
            FROM audit_chain
            WHERE entity_type = ? AND entity_id = ?
            """,
            (entity_type, entity_id),
        )
        sequence = int(fetch_all(cursor)[0]["next_sequence"])
        self.db.execute(
            """
            INSERT INTO audit_chain (
                entity_type, entity_id, sequence, previous_hash, record_hash, payload_json
            ) VALUES (?, ?, ?, ?, ?, ?)
            """,
            (entity_type, entity_id, sequence, previous, record_hash, canonical_json(payload)),
        )
        return ChainRecord(entity_type, entity_id, sequence, previous, record_hash)

    def verify(self, records: Iterable[Mapping[str, Any]]) -> None:
        previous = self.ROOT_HASH
        for record in records:
            payload = json.loads(str(record["payload_json"]))
            expected = chain_hash(previous, payload)
            if record["previous_hash"] != previous or record["record_hash"] != expected:
                raise ValidationError("Audit chain verification failed")
            previous = str(record["record_hash"])

