"""Durable event ledger: the append-only source of truth every consumer reads from.

``ingest()`` enforces ``UNIQUE(source, source_dedup_key)``: re-ingesting the
same (source, source_dedup_key) is a no-op that returns the row already on
disk rather than inserting a duplicate or raising, so callers (e.g. the
Kanban adapter, or a webhook retry) can ingest at-least-once without
duplicating side effects.
"""

from __future__ import annotations

import json
import sqlite3
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from orchestrator.audit import AuditLog
from orchestrator.db import transaction


def _utcnow() -> str:
    return datetime.now(timezone.utc).isoformat()


@dataclass(frozen=True)
class EventRecord:
    seq: int
    event_id: str
    source: str
    source_dedup_key: str
    entity_id: str
    causal_id: str | None
    event_type: str
    payload: dict[str, Any]
    occurred_at: str
    received_at: str

    @classmethod
    def _from_row(cls, row: sqlite3.Row) -> "EventRecord":
        return cls(
            seq=row["seq"],
            event_id=row["event_id"],
            source=row["source"],
            source_dedup_key=row["source_dedup_key"],
            entity_id=row["entity_id"],
            causal_id=row["causal_id"],
            event_type=row["event_type"],
            payload=json.loads(row["payload"]),
            occurred_at=row["occurred_at"],
            received_at=row["received_at"],
        )


class EventLedger:
    def __init__(self, conn: sqlite3.Connection):
        self._conn = conn

    def ingest(
        self,
        *,
        source: str,
        source_dedup_key: str,
        entity_id: str,
        event_type: str,
        payload: dict[str, Any],
        occurred_at: str | None = None,
        causal_id: str | None = None,
        now: str | None = None,
    ) -> EventRecord:
        """Append a new event, or return the existing one for a duplicate key.

        Duplicate ingest (same source + source_dedup_key) is detected and
        short-circuited *before* any insert, inside the same transaction, so
        it never runs a second time's audit entry or touches the ledger.
        """
        received_at = now or _utcnow()
        with transaction(self._conn) as conn:
            existing = conn.execute(
                "SELECT * FROM events WHERE source = ? AND source_dedup_key = ?",
                (source, source_dedup_key),
            ).fetchone()
            if existing is not None:
                return EventRecord._from_row(existing)

            event_id = str(uuid.uuid4())
            conn.execute(
                """
                INSERT INTO events (
                    event_id, source, source_dedup_key, entity_id, causal_id,
                    event_type, payload, occurred_at, received_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    event_id,
                    source,
                    source_dedup_key,
                    entity_id,
                    causal_id,
                    event_type,
                    json.dumps(payload),
                    occurred_at or received_at,
                    received_at,
                ),
            )
            AuditLog(conn).record(
                actor="ledger",
                action="event.ingested",
                subject_id=event_id,
                detail={"source": source, "source_dedup_key": source_dedup_key, "event_type": event_type},
                now=received_at,
            )
            seq = conn.execute("SELECT seq FROM events WHERE event_id = ?", (event_id,)).fetchone()[0]
            return EventRecord(
                seq=seq,
                event_id=event_id,
                source=source,
                source_dedup_key=source_dedup_key,
                entity_id=entity_id,
                causal_id=causal_id,
                event_type=event_type,
                payload=payload,
                occurred_at=occurred_at or received_at,
                received_at=received_at,
            )

    def get(self, event_id: str) -> EventRecord | None:
        row = self._conn.execute("SELECT * FROM events WHERE event_id = ?", (event_id,)).fetchone()
        return EventRecord._from_row(row) if row is not None else None

    def get_by_dedup_key(self, source: str, source_dedup_key: str) -> EventRecord | None:
        row = self._conn.execute(
            "SELECT * FROM events WHERE source = ? AND source_dedup_key = ?",
            (source, source_dedup_key),
        ).fetchone()
        return EventRecord._from_row(row) if row is not None else None

    def count(self) -> int:
        return self._conn.execute("SELECT COUNT(*) FROM events").fetchone()[0]
