"""Durable Human Owner evidence: immutable grants a Category B action may consume.

A grant records exactly what a Human Owner approved — its ``scope``,
``subject``, ``action_type``, and ``payload_digest`` — once. Nothing in this
package ever updates those fields after insert; ``schema.py``'s
``human_owner_grants_core_immutable`` trigger makes that a DB-level
guarantee, not just an application convention (mirrors ``audit_log``'s and
``events``' own append-only triggers). ``payload_digest`` is the canonical
digest of the exact payload a Human Owner approved: without it, a grant for
e.g. "comment on pr-42" would authorize *any* comment body, not just the one
actually approved.

The only mutation a grant ever undergoes is a single one-way transition from
unconsumed to consumed, recording the idempotency key of the action intent
that consumed it — and as of schema.py's v6 migration, that transition is
never made by application code at all. Inserting a row into
``action_intents`` is the *only* way a grant is consumed: schema.py's
``action_intents_consumes_grant`` trigger derives ``consumed_at`` /
``consumed_by_idempotency_key`` from that insert automatically, and its
``human_owner_grants_consumption_immutable`` trigger rejects any UPDATE of
those columns that isn't that exact, derived transition. This module
deliberately has no function that updates ``human_owner_grants`` directly
(the removed ``consume_grant_in_transaction`` did, and trusted callers to
always pair it with an ``action_intents`` insert in the same transaction —
a convention, not a guarantee); see ``action_gateway.py``'s
``_record_intent`` for the real consumption path.
"""

from __future__ import annotations

import hashlib
import json
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from orchestrator.audit import AuditLog
from orchestrator.db import transaction


def _utcnow() -> str:
    return datetime.now(timezone.utc).isoformat()


def canonical_payload_digest(payload: dict[str, Any]) -> str:
    """A stable digest of ``payload``'s exact contents.

    ``json.dumps(..., sort_keys=True)`` gives the same bytes for the same
    logical payload regardless of key order, so two callers building the
    "same" payload dict independently still land on the same digest. Used to
    bind a Human Owner grant to the exact payload it approved (see module
    docstring) — a mismatch means the payload actually being dispatched is
    not the one a human signed off on.
    """
    canonical = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


class GrantAlreadyConsumedError(ValueError):
    """Raised when a grant already consumed by a different idempotency key is consumed again."""


@dataclass(frozen=True)
class GrantRecord:
    grant_id: str
    scope: str
    subject: str
    action_type: str
    payload_digest: str
    granted_by: str
    granted_at: str
    consumed_at: str | None
    consumed_by_idempotency_key: str | None

    @classmethod
    def _from_row(cls, row: sqlite3.Row) -> "GrantRecord":
        return cls(
            grant_id=row["grant_id"],
            scope=row["scope"],
            subject=row["subject"],
            action_type=row["action_type"],
            payload_digest=row["payload_digest"],
            granted_by=row["granted_by"],
            granted_at=row["granted_at"],
            consumed_at=row["consumed_at"],
            consumed_by_idempotency_key=row["consumed_by_idempotency_key"],
        )


class HumanOwnerGrants:
    def __init__(self, conn: sqlite3.Connection):
        self._conn = conn

    def record(
        self,
        *,
        grant_id: str,
        scope: str,
        subject: str,
        action_type: str,
        payload_digest: str,
        granted_by: str,
        now: str | None = None,
    ) -> GrantRecord:
        moment = now or _utcnow()
        with transaction(self._conn) as conn:
            conn.execute(
                """
                INSERT INTO human_owner_grants
                    (grant_id, scope, subject, action_type, payload_digest, granted_by, granted_at,
                     consumed_at, consumed_by_idempotency_key)
                VALUES (?, ?, ?, ?, ?, ?, ?, NULL, NULL)
                """,
                (grant_id, scope, subject, action_type, payload_digest, granted_by, moment),
            )
            AuditLog(conn).record(
                actor=granted_by,
                action="grant.recorded",
                subject_id=grant_id,
                detail={"scope": scope, "subject": subject, "action_type": action_type},
                now=moment,
            )
        return self.get(grant_id)

    def get(self, grant_id: str) -> GrantRecord | None:
        row = self._conn.execute("SELECT * FROM human_owner_grants WHERE grant_id = ?", (grant_id,)).fetchone()
        return GrantRecord._from_row(row) if row is not None else None

    # Deliberately no standalone ``consume()``, and no other method that
    # updates ``human_owner_grants`` at all: the only way a grant is ever
    # consumed is inserting the ``action_intents`` row it authorizes
    # (action_gateway._record_intent) — schema.py's v6 triggers derive the
    # consumption from that insert and reject any other write to those
    # columns. A convenience wrapper that consumed a grant on its own, with
    # no intent row required, would be exactly the orphan-consumption gap
    # that violates Category B's atomic consume-plus-intent guarantee.
