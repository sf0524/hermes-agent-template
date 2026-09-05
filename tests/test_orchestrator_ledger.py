import tempfile
import unittest
from pathlib import Path

from orchestrator.db import connect
from orchestrator.ledger import EventLedger


class EventLedgerTests(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.conn = connect(Path(self._tmp.name) / "state.db")
        self.addCleanup(self.conn.close)
        self.ledger = EventLedger(self.conn)

    def test_ingest_appends_a_new_event(self):
        record = self.ledger.ingest(
            source="kanban",
            source_dedup_key="evt-1",
            entity_id="task-42",
            event_type="kanban.task.created",
            payload={"title": "Do the thing"},
        )
        self.assertEqual(record.source, "kanban")
        self.assertEqual(record.source_dedup_key, "evt-1")
        self.assertEqual(record.entity_id, "task-42")
        self.assertEqual(record.payload, {"title": "Do the thing"})
        self.assertIsNone(record.causal_id)
        self.assertEqual(record.occurred_at, record.received_at)
        self.assertEqual(self.ledger.count(), 1)

    def test_duplicate_ingest_returns_existing_event_without_side_effects(self):
        first = self.ledger.ingest(
            source="kanban", source_dedup_key="evt-1", entity_id="task-42",
            event_type="kanban.task.created", payload={"n": 1},
        )
        second = self.ledger.ingest(
            source="kanban", source_dedup_key="evt-1", entity_id="task-42",
            event_type="kanban.task.created", payload={"n": 999},  # different payload — must be ignored
        )
        self.assertEqual(first.event_id, second.event_id)
        self.assertEqual(second.payload, {"n": 1})  # original wins, no overwrite
        self.assertEqual(self.ledger.count(), 1)

        audit_rows = self.conn.execute(
            "SELECT COUNT(*) FROM audit_log WHERE action = 'event.ingested'"
        ).fetchone()[0]
        self.assertEqual(audit_rows, 1)

    def test_dedup_key_is_scoped_per_source(self):
        a = self.ledger.ingest(
            source="kanban", source_dedup_key="1", entity_id="task-1",
            event_type="t", payload={},
        )
        b = self.ledger.ingest(
            source="other-source", source_dedup_key="1", entity_id="task-1",
            event_type="t", payload={},
        )
        self.assertNotEqual(a.event_id, b.event_id)
        self.assertEqual(self.ledger.count(), 2)

    def test_get_and_get_by_dedup_key(self):
        record = self.ledger.ingest(
            source="kanban", source_dedup_key="evt-9", entity_id="task-9",
            event_type="t", payload={}, causal_id="evt-parent",
        )
        self.assertEqual(self.ledger.get(record.event_id), record)
        self.assertEqual(self.ledger.get_by_dedup_key("kanban", "evt-9"), record)
        self.assertIsNone(self.ledger.get("does-not-exist"))
        self.assertEqual(record.causal_id, "evt-parent")

    def test_occurred_at_can_differ_from_received_at(self):
        record = self.ledger.ingest(
            source="kanban", source_dedup_key="evt-2", entity_id="task-2",
            event_type="t", payload={}, occurred_at="2020-01-01T00:00:00+00:00",
            now="2026-01-01T00:00:00+00:00",
        )
        self.assertEqual(record.occurred_at, "2020-01-01T00:00:00+00:00")
        self.assertEqual(record.received_at, "2026-01-01T00:00:00+00:00")


if __name__ == "__main__":
    unittest.main()
