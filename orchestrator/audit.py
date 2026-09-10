"""Append-only audit log.

Every state-changing call elsewhere in this package (ingest, claim, ack,
outbox enqueue/dispatch/fail, policy rejection) records one row here in the
same transaction as the change it's auditing. There is deliberately no
update/delete method — ``audit_log_no_update``/``audit_log_no_delete``
triggers (schema.py) enforce that at the DB level too.

``record`` redacts every caller-controlled persisted field centrally (via
``orchestrator.health.redact``) before it is ever written, rather than
trusting every call site elsewhere in the package to remember to redact its
own caller-controlled fields — a caller that redacts anyway just gets a
no-op re-redaction.
"""

from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any


def _utcnow() -> str:
    return datetime.now(timezone.utc).isoformat()


@dataclass(frozen=True)
class AuditEntry:
    id: int
    recorded_at: str
    actor: str
    action: str
    subject_id: str | None
    detail: dict[str, Any] | None

    @classmethod
    def _from_row(cls, row: sqlite3.Row) -> "AuditEntry":
        return cls(
            id=row["id"],
            recorded_at=row["recorded_at"],
            actor=row["actor"],
            action=row["action"],
            subject_id=row["subject_id"],
            detail=json.loads(row["detail"]) if row["detail"] is not None else None,
        )


class AuditLog:
    def __init__(self, conn: sqlite3.Connection):
        self._conn = conn

    def record(
        self,
        *,
        actor: str,
        action: str,
        subject_id: str | None = None,
        detail: dict[str, Any] | None = None,
        now: str | None = None,
    ) -> AuditEntry:
        # Deferred import: health.py imports AuditEntry/AuditLog from this
        # module at module load time, so importing health.py back at *this*
        # module's load time would be circular. By the time record() actually
        # runs, both modules have finished loading and this is a normal cache
        # hit (same pattern as schema.py's deferred import of db.py).
        from orchestrator.health import redact

        recorded_at = now or _utcnow()
        redacted_actor = redact(actor)
        redacted_action = redact(action)
        redacted_subject_id = redact(subject_id) if subject_id is not None else None
        redacted_detail = redact(detail) if detail is not None else None
        cur = self._conn.execute(
            """
            INSERT INTO audit_log (recorded_at, actor, action, subject_id, detail)
            VALUES (?, ?, ?, ?, ?)
            """,
            (
                recorded_at,
                redacted_actor,
                redacted_action,
                redacted_subject_id,
                json.dumps(redacted_detail) if redacted_detail is not None else None,
            ),
        )
        return AuditEntry(
            id=cur.lastrowid,
            recorded_at=recorded_at,
            actor=redacted_actor,
            action=redacted_action,
            subject_id=redacted_subject_id,
            detail=redacted_detail,
        )

    def list_for_subject(self, subject_id: str) -> list[AuditEntry]:
        rows = self._conn.execute(
            "SELECT * FROM audit_log WHERE subject_id = ? ORDER BY id ASC",
            (subject_id,),
        ).fetchall()
        return [AuditEntry._from_row(r) for r in rows]

    def all(self) -> list[AuditEntry]:
        rows = self._conn.execute("SELECT * FROM audit_log ORDER BY id ASC").fetchall()
        return [AuditEntry._from_row(r) for r in rows]
