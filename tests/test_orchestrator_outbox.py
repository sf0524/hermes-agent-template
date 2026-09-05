import tempfile
import unittest
from pathlib import Path

from orchestrator.db import connect
from orchestrator.outbox import Outbox
from orchestrator.policy import PolicyViolation


class OutboxTests(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.conn = connect(Path(self._tmp.name) / "state.db")
        self.addCleanup(self.conn.close)
        self.outbox = Outbox(self.conn)

    def test_enqueue_creates_a_pending_action(self):
        record = self.outbox.enqueue(
            idempotency_key="k1",
            consumer="runtime_orchestrator",
            action_type="post_comment",
            payload={"body": "hi"},
        )
        self.assertEqual(record.status, "pending")
        self.assertEqual(record.payload, {"body": "hi"})

    def test_enqueue_is_idempotent_on_the_key(self):
        first = self.outbox.enqueue(
            idempotency_key="k1", consumer="runtime_orchestrator",
            action_type="post_comment", payload={"body": "hi"},
        )
        second = self.outbox.enqueue(
            idempotency_key="k1", consumer="runtime_orchestrator",
            action_type="post_comment", payload={"body": "a different body"},  # ignored
        )
        self.assertEqual(first.created_at, second.created_at)
        self.assertEqual(second.payload, {"body": "hi"})
        rows = self.conn.execute("SELECT COUNT(*) FROM outbox").fetchone()[0]
        self.assertEqual(rows, 1)

    def test_mark_dispatched_and_failed_update_status(self):
        self.outbox.enqueue(
            idempotency_key="k1", consumer="runtime_orchestrator",
            action_type="post_comment", payload={},
        )
        dispatched = self.outbox.mark_dispatched("k1")
        self.assertEqual(dispatched.status, "dispatched")

        self.outbox.enqueue(
            idempotency_key="k2", consumer="runtime_orchestrator",
            action_type="post_comment", payload={},
        )
        failed = self.outbox.mark_failed("k2", reason="upstream 500")
        self.assertEqual(failed.status, "failed")

    def test_mark_dispatched_unknown_key_raises(self):
        with self.assertRaises(KeyError):
            self.outbox.mark_dispatched("does-not-exist")

    def test_get_returns_none_for_unknown_key(self):
        self.assertIsNone(self.outbox.get("nope"))

    def test_forbidden_action_is_rejected_and_never_written(self):
        with self.assertRaises(PolicyViolation):
            self.outbox.enqueue(
                idempotency_key="merge-1", consumer="runtime_orchestrator",
                action_type="merge", payload={},
            )
        self.assertIsNone(self.outbox.get("merge-1"))
        rejected = self.conn.execute(
            "SELECT COUNT(*) FROM audit_log WHERE action = 'outbox.rejected'"
        ).fetchone()[0]
        self.assertEqual(rejected, 1)


if __name__ == "__main__":
    unittest.main()
