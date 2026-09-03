import tempfile
import unittest
from pathlib import Path

from orchestrator.consumers import Inbox, StaleClaimError, UnknownConsumerError
from orchestrator.db import connect
from orchestrator.ledger import EventLedger


class InboxTests(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.db_path = Path(self._tmp.name) / "state.db"
        self.conn = connect(self.db_path)
        self.addCleanup(self.conn.close)
        self.ledger = EventLedger(self.conn)
        for i in range(3):
            self.ledger.ingest(
                source="kanban", source_dedup_key=f"evt-{i}", entity_id=f"task-{i}",
                event_type="t", payload={"i": i},
            )

    def test_unknown_consumer_name_is_rejected(self):
        with self.assertRaises(UnknownConsumerError):
            Inbox(self.conn, "not_a_real_consumer")

    def test_claim_next_is_fifo_and_exhausts(self):
        inbox = Inbox(self.conn, "runtime_orchestrator")
        seen = []
        for _ in range(3):
            claimed = inbox.claim_next()
            self.assertIsNotNone(claimed)
            event, claim = claimed
            seen.append(event.payload["i"])
            self.assertEqual(claim.status, "claimed")
            self.assertEqual(claim.attempt_count, 1)
        self.assertEqual(seen, [0, 1, 2])
        self.assertIsNone(inbox.claim_next())  # nothing left un-delivered

    def test_two_consumers_are_independent(self):
        runtime = Inbox(self.conn, "runtime_orchestrator")
        lead = Inbox(self.conn, "lead_orchestrator")
        r_event, _ = runtime.claim_next()
        l_event, _ = lead.claim_next()
        self.assertEqual(r_event.payload["i"], 0)
        self.assertEqual(l_event.payload["i"], 0)  # each consumer sees the full stream

    def test_ack_requires_a_prior_claim(self):
        inbox = Inbox(self.conn, "runtime_orchestrator")
        with self.assertRaises(ValueError):
            inbox.ack("some-event-id-never-claimed", "irrelevant-token")

    def test_ack_with_wrong_token_is_rejected(self):
        inbox = Inbox(self.conn, "runtime_orchestrator")
        event, claim = inbox.claim_next()
        with self.assertRaises(StaleClaimError):
            inbox.ack(event.event_id, "not-the-real-token")
        # rejected ack must not have acked the event
        self.assertEqual(inbox.get_claim(event.event_id).status, "claimed")

    def test_ack_is_idempotent(self):
        inbox = Inbox(self.conn, "runtime_orchestrator")
        event, claim = inbox.claim_next()
        first = inbox.ack(event.event_id, claim.claim_token, now="2026-01-01T00:00:10+00:00")
        second = inbox.ack(event.event_id, claim.claim_token, now="2026-01-01T00:00:20+00:00")
        self.assertEqual(first.acked_at, second.acked_at)  # second ack is a no-op, not a re-timestamp
        self.assertEqual(second.status, "acked")

    def test_ack_retry_with_a_stale_token_after_already_acked_is_rejected(self):
        inbox = Inbox(self.conn, "runtime_orchestrator")
        event, claim = inbox.claim_next(now="2026-01-01T00:00:00+00:00")
        inbox.ack(event.event_id, claim.claim_token, now="2026-01-01T00:00:10+00:00")
        with self.assertRaises(StaleClaimError):
            inbox.ack(event.event_id, "some-other-token", now="2026-01-01T00:00:20+00:00")

    def test_cursor_advances_only_contiguously(self):
        inbox = Inbox(self.conn, "runtime_orchestrator")
        e0, c0 = inbox.claim_next()
        e1, c1 = inbox.claim_next()
        e2, c2 = inbox.claim_next()
        self.assertEqual(inbox.cursor(), 0)

        inbox.ack(e1.event_id, c1.claim_token)  # ack out of order — gap at seq 1 (e0) blocks the watermark
        self.assertEqual(inbox.cursor(), 0)

        inbox.ack(e0.event_id, c0.claim_token)  # fills the gap: cursor jumps past both acked events
        self.assertEqual(inbox.cursor(), e1.seq)

        inbox.ack(e2.event_id, c2.claim_token)
        self.assertEqual(inbox.cursor(), e2.seq)

    def test_cursor_and_claim_survive_a_reconnect(self):
        """Simulates a process crash/restart: a fresh connection to the same file
        must see the exact same claim/cursor state the old connection left behind."""
        inbox = Inbox(self.conn, "runtime_orchestrator")
        e0, claim0 = inbox.claim_next()
        inbox.ack(e0.event_id, claim0.claim_token)
        self.conn.close()

        reconnected = connect(self.db_path)
        self.addCleanup(reconnected.close)
        resumed = Inbox(reconnected, "runtime_orchestrator")
        self.assertEqual(resumed.cursor(), e0.seq)
        resumed_claim = resumed.get_claim(e0.event_id)
        self.assertEqual(resumed_claim.status, "acked")

        # Unclaimed backlog (seq 1, 2) must still be claimable after reconnect.
        e1, _ = resumed.claim_next()
        self.assertEqual(e1.payload["i"], 1)

    def test_unacked_claim_survives_a_crash_and_is_not_lost(self):
        inbox = Inbox(self.conn, "runtime_orchestrator")
        event, _ = inbox.claim_next(now="2026-01-01T00:00:00+00:00")  # claimed, never acked
        self.conn.close()

        reconnected = connect(self.db_path)
        self.addCleanup(reconnected.close)
        resumed = Inbox(reconnected, "runtime_orchestrator")
        claim = resumed.get_claim(event.event_id)
        self.assertEqual(claim.status, "claimed")
        self.assertEqual(claim.attempt_count, 1)

    def test_lease_expiry_allows_idempotent_retry_without_double_claim(self):
        inbox = Inbox(self.conn, "runtime_orchestrator")
        event, claim = inbox.claim_next(lease_seconds=30, now="2026-01-01T00:00:00+00:00")

        # Before the lease expires, claim_next must not hand the same event to
        # anyone else, or reclaim it itself — it looks caught up.
        still_leased = inbox.claim_next(now="2026-01-01T00:00:10+00:00")
        self.assertIsNotNone(still_leased)
        self.assertEqual(still_leased[0].payload["i"], 1)  # moved on to the next *new* event, not a re-claim

        # After the lease expires, the original event is reclaimed (retry),
        # with attempt_count bumped — proving at-least-once delivery survives
        # a crash between claim and ack.
        reclaimed_event, reclaimed_claim = inbox.claim_next(now="2026-01-01T00:00:40+00:00")
        self.assertEqual(reclaimed_event.event_id, event.event_id)
        self.assertEqual(reclaimed_claim.attempt_count, 2)
        self.assertNotEqual(reclaimed_claim.claim_token, claim.claim_token)  # reclaim mints a fresh token

        # Acking after the reclaim, with the reclaim's own token, is the
        # normal, successful path out.
        acked = inbox.ack(event.event_id, reclaimed_claim.claim_token, now="2026-01-01T00:00:41+00:00")
        self.assertEqual(acked.status, "acked")
        self.assertEqual(acked.attempt_count, 2)

    def test_stale_worker_ack_after_reclaim_is_rejected_not_applied(self):
        """True two-connection reproduction of the stale-ack race: worker A
        claims an event and holds a lease; the lease expires; worker B (a
        second, independent DB connection — simulating a second process)
        reclaims the same event; worker A, unaware its lease has moved on,
        finally tries to ack with its now-stale token. That ack must fail
        rather than silently confirming work worker A no longer owns, and
        must not disturb worker B's now-current claim."""
        worker_a = Inbox(self.conn, "runtime_orchestrator")
        event, claim_a = worker_a.claim_next(lease_seconds=30, now="2026-01-01T00:00:00+00:00")

        other_conn = connect(self.db_path)
        self.addCleanup(other_conn.close)
        worker_b = Inbox(other_conn, "runtime_orchestrator")
        reclaimed_event, claim_b = worker_b.claim_next(now="2026-01-01T00:01:00+00:00")  # lease long expired
        self.assertEqual(reclaimed_event.event_id, event.event_id)
        self.assertNotEqual(claim_b.claim_token, claim_a.claim_token)

        with self.assertRaises(StaleClaimError):
            worker_a.ack(event.event_id, claim_a.claim_token, now="2026-01-01T00:01:05+00:00")

        # Worker B's claim must be untouched by worker A's failed, stale ack.
        current = worker_b.get_claim(event.event_id)
        self.assertEqual(current.status, "claimed")
        self.assertEqual(current.claim_token, claim_b.claim_token)

        # The rightful holder's ack, with the current token, still succeeds.
        acked = worker_b.ack(event.event_id, claim_b.claim_token, now="2026-01-01T00:01:06+00:00")
        self.assertEqual(acked.status, "acked")


if __name__ == "__main__":
    unittest.main()
