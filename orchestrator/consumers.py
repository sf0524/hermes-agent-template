"""Per-consumer inbox: crash-safe claim/lease/ack over the event ledger.

Two named consumers exist: ``runtime_orchestrator`` and ``lead_orchestrator``.
Each maintains its own claim state per event (``consumer_inbox``) and a
watermark cursor (``consumer_cursor``) of the highest seq acked with no gaps
below it. Delivery is at-least-once: a claim holds a time-boxed lease: if the
holder crashes before acking, the lease expires and the same event is handed
out again (with ``attempt_count`` incremented) rather than lost or blocked
forever.

Every claim and reclaim mints a fresh opaque ``claim_token``, and ``ack()``
requires it. This closes the stale-ack race a bare lease-expiry check leaves
open: without a token, a worker whose lease just expired can still ack right
as (or after) a second worker reclaims the same event, silently confirming
work neither worker's outcome can be trusted for. With a token, that ack's
token no longer matches the row (the reclaim overwrote it) and is rejected.
"""

from __future__ import annotations

import json
import secrets
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

from orchestrator.audit import AuditLog
from orchestrator.db import transaction
from orchestrator.ledger import EventRecord

CONSUMER_NAMES = frozenset({"runtime_orchestrator", "lead_orchestrator"})

DEFAULT_LEASE_SECONDS = 60


def _utcnow() -> str:
    return datetime.now(timezone.utc).isoformat()


def _add_seconds(iso_ts: str, seconds: float) -> str:
    return (datetime.fromisoformat(iso_ts) + timedelta(seconds=seconds)).isoformat()


def _new_claim_token() -> str:
    return secrets.token_hex(16)


class UnknownConsumerError(ValueError):
    pass


class StaleClaimError(ValueError):
    """Raised when an ack's claim token no longer matches the current claim.

    Means the lease this ack is for has since expired and been reclaimed
    (possibly by another worker), or the caller never held a valid claim at
    all — either way the ack must not be honoured, since it no longer
    corresponds to the work currently owned.
    """


@dataclass(frozen=True)
class Claim:
    consumer: str
    seq: int
    event_id: str
    status: str
    attempt_count: int
    claimed_at: str
    lease_expires_at: str
    acked_at: str | None
    claim_token: str


class Inbox:
    def __init__(self, conn: sqlite3.Connection, consumer: str):
        if consumer not in CONSUMER_NAMES:
            raise UnknownConsumerError(f"unknown consumer {consumer!r}; expected one of {sorted(CONSUMER_NAMES)}")
        self._conn = conn
        self.consumer = consumer

    def claim_next(
        self, *, lease_seconds: float = DEFAULT_LEASE_SECONDS, now: str | None = None
    ) -> tuple[EventRecord, Claim] | None:
        """Claim the next event due for this consumer, or None if caught up.

        Preference order: (1) a previously claimed event whose lease has
        expired — a crash/retry case, reclaimed with ``attempt_count`` bumped
        — before (2) the oldest event never yet delivered to this consumer.
        Events currently claimed under a live lease, or already acked, are
        skipped. Idempotent to call repeatedly: re-claiming the same expired
        lease just extends it, it never double-delivers concurrently live work.
        """
        moment = now or _utcnow()
        with transaction(self._conn) as conn:
            expired = conn.execute(
                """
                SELECT * FROM consumer_inbox
                WHERE consumer = ? AND status = 'claimed' AND lease_expires_at < ?
                ORDER BY seq ASC LIMIT 1
                """,
                (self.consumer, moment),
            ).fetchone()
            if expired is not None:
                seq = expired["seq"]
                attempt_count = expired["attempt_count"] + 1
                lease_expires_at = _add_seconds(moment, lease_seconds)
                # A fresh token invalidates whatever token the previous
                # holder was given: if that holder is still alive and acks
                # late with its old token, the token mismatch in ack() is
                # what makes the stale ack fail instead of silently
                # confirming work someone else has since been re-handed.
                claim_token = _new_claim_token()
                conn.execute(
                    """
                    UPDATE consumer_inbox
                    SET attempt_count = ?, claimed_at = ?, lease_expires_at = ?, claim_token = ?
                    WHERE consumer = ? AND seq = ?
                    """,
                    (attempt_count, moment, lease_expires_at, claim_token, self.consumer, seq),
                )
                event_row = conn.execute("SELECT * FROM events WHERE seq = ?", (seq,)).fetchone()
                AuditLog(conn).record(
                    actor=self.consumer,
                    action="event.reclaimed",
                    subject_id=expired["event_id"],
                    detail={"attempt_count": attempt_count},
                    now=moment,
                )
                return _event_from_row(event_row), Claim(
                    consumer=self.consumer,
                    seq=seq,
                    event_id=expired["event_id"],
                    status="claimed",
                    attempt_count=attempt_count,
                    claimed_at=moment,
                    lease_expires_at=lease_expires_at,
                    acked_at=None,
                    claim_token=claim_token,
                )

            candidate = conn.execute(
                """
                SELECT e.* FROM events e
                WHERE NOT EXISTS (
                    SELECT 1 FROM consumer_inbox ci
                    WHERE ci.consumer = ? AND ci.seq = e.seq
                )
                ORDER BY e.seq ASC LIMIT 1
                """,
                (self.consumer,),
            ).fetchone()
            if candidate is None:
                return None

            lease_expires_at = _add_seconds(moment, lease_seconds)
            claim_token = _new_claim_token()
            conn.execute(
                """
                INSERT INTO consumer_inbox
                    (consumer, seq, event_id, status, attempt_count, claimed_at, lease_expires_at, acked_at, claim_token)
                VALUES (?, ?, ?, 'claimed', 1, ?, ?, NULL, ?)
                """,
                (self.consumer, candidate["seq"], candidate["event_id"], moment, lease_expires_at, claim_token),
            )
            AuditLog(conn).record(
                actor=self.consumer,
                action="event.claimed",
                subject_id=candidate["event_id"],
                detail={"attempt_count": 1},
                now=moment,
            )
            return _event_from_row(candidate), Claim(
                consumer=self.consumer,
                seq=candidate["seq"],
                event_id=candidate["event_id"],
                status="claimed",
                attempt_count=1,
                claimed_at=moment,
                lease_expires_at=lease_expires_at,
                acked_at=None,
                claim_token=claim_token,
            )

    def ack(self, event_id: str, claim_token: str, *, now: str | None = None) -> Claim:
        """Mark an event processed by this consumer, then advance the cursor.

        ``claim_token`` must match the token returned by the ``claim_next()``
        call this ack is for. Every claim and reclaim mints a fresh token, so
        a worker whose lease expired and was reclaimed by someone else holds
        a now-stale token: its ack is rejected (``StaleClaimError``) instead
        of acking work it no longer owns.

        Idempotent for a *matching* token: acking an already-acked event with
        the same token that acked it returns the existing claim unchanged
        rather than erroring, so a retried ack (e.g. the caller crashed right
        after the DB commit but before its own ack-confirmation) never fails.
        A retry with a different (stale) token is rejected even though the
        event is already acked, since that token was never the one that
        actually did it.
        """
        moment = now or _utcnow()
        with transaction(self._conn) as conn:
            row = conn.execute(
                "SELECT * FROM consumer_inbox WHERE consumer = ? AND event_id = ?",
                (self.consumer, event_id),
            ).fetchone()
            if row is None:
                raise ValueError(f"{event_id!r} was never claimed by consumer {self.consumer!r}")

            if row["status"] == "acked":
                if row["claim_token"] != claim_token:
                    raise StaleClaimError(
                        f"ack for {event_id!r} used a stale claim token; it was already acked under a "
                        "different claim"
                    )
                return _claim_from_row(row)

            cursor = conn.execute(
                """
                UPDATE consumer_inbox
                SET status = 'acked', acked_at = ?
                WHERE consumer = ? AND event_id = ? AND claim_token = ? AND status = 'claimed'
                """,
                (moment, self.consumer, event_id, claim_token),
            )
            if cursor.rowcount == 0:
                # status was 'claimed' a moment ago (checked above) but the
                # token doesn't match the row currently on disk: the lease
                # was reclaimed out from under this caller between its claim
                # and this ack.
                raise StaleClaimError(
                    f"ack for {event_id!r} used a stale claim token; the lease has since been reclaimed"
                )

            AuditLog(conn).record(
                actor=self.consumer,
                action="event.acked",
                subject_id=event_id,
                detail={"attempt_count": row["attempt_count"]},
                now=moment,
            )
            self._advance_cursor(conn)
            updated = conn.execute(
                "SELECT * FROM consumer_inbox WHERE consumer = ? AND seq = ?",
                (self.consumer, row["seq"]),
            ).fetchone()
            return _claim_from_row(updated)

    def _advance_cursor(self, conn: sqlite3.Connection) -> None:
        cursor_row = conn.execute(
            "SELECT acked_through_seq FROM consumer_cursor WHERE consumer = ?", (self.consumer,)
        ).fetchone()
        acked_through = cursor_row["acked_through_seq"] if cursor_row is not None else 0

        while True:
            nxt = conn.execute(
                "SELECT status FROM consumer_inbox WHERE consumer = ? AND seq = ?",
                (self.consumer, acked_through + 1),
            ).fetchone()
            if nxt is None or nxt["status"] != "acked":
                break
            acked_through += 1

        if cursor_row is None:
            conn.execute(
                "INSERT INTO consumer_cursor (consumer, acked_through_seq) VALUES (?, ?)",
                (self.consumer, acked_through),
            )
        else:
            conn.execute(
                "UPDATE consumer_cursor SET acked_through_seq = ? WHERE consumer = ?",
                (acked_through, self.consumer),
            )

    def cursor(self) -> int:
        """Highest seq acked with no gap below it — the durable resume point."""
        row = self._conn.execute(
            "SELECT acked_through_seq FROM consumer_cursor WHERE consumer = ?", (self.consumer,)
        ).fetchone()
        return row["acked_through_seq"] if row is not None else 0

    def get_claim(self, event_id: str) -> Claim | None:
        row = self._conn.execute(
            "SELECT * FROM consumer_inbox WHERE consumer = ? AND event_id = ?",
            (self.consumer, event_id),
        ).fetchone()
        return _claim_from_row(row) if row is not None else None


def _event_from_row(row: sqlite3.Row) -> EventRecord:
    return EventRecord(
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


def _claim_from_row(row: sqlite3.Row) -> Claim:
    return Claim(
        consumer=row["consumer"],
        seq=row["seq"],
        event_id=row["event_id"],
        status=row["status"],
        attempt_count=row["attempt_count"],
        claimed_at=row["claimed_at"],
        lease_expires_at=row["lease_expires_at"],
        acked_at=row["acked_at"],
        claim_token=row["claim_token"],
    )
