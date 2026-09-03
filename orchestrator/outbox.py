"""Transactional outbox: durable, idempotent record of actions an orchestrator wants taken.

This package never executes an outbox action — the execution side (whatever
adapter eventually dispatches "post a comment", "update a task", etc.) is out
of scope here. ``Outbox`` only guarantees the write side is durable and
idempotent: ``enqueue()`` is safe to call twice with the same
``idempotency_key`` (e.g. the caller crashed after the DB write but before
confirming, and retries) without creating a second row or resetting an
in-flight/terminal status.

Every enqueue is policy-checked first (policy.py) — merges, releases, and
High/Critical gate approvals are rejected before they ever touch the table.
"""

from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from orchestrator.audit import AuditLog
from orchestrator.db import transaction
from orchestrator.policy import PolicyViolation, check_outbox_action

VALID_STATUSES = frozenset({"pending", "dispatched", "failed"})


def _utcnow() -> str:
    return datetime.now(timezone.utc).isoformat()


@dataclass(frozen=True)
class OutboxRecord:
    idempotency_key: str
    consumer: str
    action_type: str
    payload: dict[str, Any]
    status: str
    causal_event_id: str | None
    created_at: str
    updated_at: str

    @classmethod
    def _from_row(cls, row: sqlite3.Row) -> "OutboxRecord":
        return cls(
            idempotency_key=row["idempotency_key"],
            consumer=row["consumer"],
            action_type=row["action_type"],
            payload=json.loads(row["payload"]),
            status=row["status"],
            causal_event_id=row["causal_event_id"],
            created_at=row["created_at"],
            updated_at=row["updated_at"],
        )


class Outbox:
    def __init__(self, conn: sqlite3.Connection):
        self._conn = conn

    def enqueue(
        self,
        *,
        idempotency_key: str,
        consumer: str,
        action_type: str,
        payload: dict[str, Any],
        causal_event_id: str | None = None,
        now: str | None = None,
    ) -> OutboxRecord:
        """Durably record an action to take, or return the existing record for a retried key.

        Raises ``PolicyViolation`` — without writing anything — if the action
        is forbidden by ``policy.check_outbox_action``.
        """
        moment = now or _utcnow()

        existing = self._conn.execute(
            "SELECT * FROM outbox WHERE idempotency_key = ?", (idempotency_key,)
        ).fetchone()
        if existing is not None:
            return OutboxRecord._from_row(existing)

        try:
            check_outbox_action(consumer=consumer, action_type=action_type, payload=payload)
        except PolicyViolation as exc:
            # A separate transaction from the insert path below: the audit
            # entry for a rejection must survive even though nothing else
            # about this call is written.
            with transaction(self._conn) as conn:
                AuditLog(conn).record(
                    actor=consumer,
                    action="outbox.rejected",
                    subject_id=idempotency_key,
                    detail={"action_type": action_type, "reason": str(exc)},
                    now=moment,
                )
            raise

        with transaction(self._conn) as conn:
            # Re-check inside the transaction (BEGIN IMMEDIATE has the write
            # lock by now) in case a concurrent enqueue raced us to this key
            # between the read above and here.
            existing = conn.execute(
                "SELECT * FROM outbox WHERE idempotency_key = ?", (idempotency_key,)
            ).fetchone()
            if existing is not None:
                return OutboxRecord._from_row(existing)

            conn.execute(
                """
                INSERT INTO outbox
                    (idempotency_key, consumer, action_type, payload, status, causal_event_id, created_at, updated_at)
                VALUES (?, ?, ?, ?, 'pending', ?, ?, ?)
                """,
                (idempotency_key, consumer, action_type, json.dumps(payload), causal_event_id, moment, moment),
            )
            AuditLog(conn).record(
                actor=consumer,
                action="outbox.enqueued",
                subject_id=idempotency_key,
                detail={"action_type": action_type, "causal_event_id": causal_event_id},
                now=moment,
            )
            return OutboxRecord(
                idempotency_key=idempotency_key,
                consumer=consumer,
                action_type=action_type,
                payload=payload,
                status="pending",
                causal_event_id=causal_event_id,
                created_at=moment,
                updated_at=moment,
            )

    def mark_dispatched(self, idempotency_key: str, *, now: str | None = None) -> OutboxRecord:
        return self._set_status(idempotency_key, "dispatched", now=now)

    def mark_failed(self, idempotency_key: str, *, reason: str | None = None, now: str | None = None) -> OutboxRecord:
        return self._set_status(idempotency_key, "failed", reason=reason, now=now)

    def _set_status(
        self, idempotency_key: str, status: str, *, reason: str | None = None, now: str | None = None
    ) -> OutboxRecord:
        assert status in VALID_STATUSES
        moment = now or _utcnow()
        with transaction(self._conn) as conn:
            row = conn.execute("SELECT * FROM outbox WHERE idempotency_key = ?", (idempotency_key,)).fetchone()
            if row is None:
                raise KeyError(f"no outbox action with idempotency_key {idempotency_key!r}")
            if row["status"] == status:
                return OutboxRecord._from_row(row)

            conn.execute(
                "UPDATE outbox SET status = ?, updated_at = ? WHERE idempotency_key = ?",
                (status, moment, idempotency_key),
            )
            AuditLog(conn).record(
                actor=row["consumer"],
                action=f"outbox.{status}",
                subject_id=idempotency_key,
                detail={"reason": reason} if reason else None,
                now=moment,
            )
            updated = conn.execute("SELECT * FROM outbox WHERE idempotency_key = ?", (idempotency_key,)).fetchone()
            return OutboxRecord._from_row(updated)

    def get(self, idempotency_key: str) -> OutboxRecord | None:
        row = self._conn.execute("SELECT * FROM outbox WHERE idempotency_key = ?", (idempotency_key,)).fetchone()
        return OutboxRecord._from_row(row) if row is not None else None
