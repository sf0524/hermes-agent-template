import tempfile
import unittest
from pathlib import Path

from orchestrator.db import connect
from orchestrator.kanban_adapter import ingest_kanban_task_events, normalize_kanban_task_event
from orchestrator.ledger import EventLedger


class NormalizeKanbanTaskEventTests(unittest.TestCase):
    def test_normalizes_primary_field_spellings(self):
        raw = {
            "id": 101,
            "task_id": "task-7",
            "type": "task.status_changed",
            "created_at": "2026-01-01T00:00:00+00:00",
            "board_id": "board-1",
        }
        normalized = normalize_kanban_task_event(raw)
        self.assertEqual(normalized["source"], "kanban")
        self.assertEqual(normalized["source_dedup_key"], "101")
        self.assertEqual(normalized["entity_id"], "task-7")
        self.assertEqual(normalized["event_type"], "kanban.task.status_changed")
        self.assertEqual(normalized["occurred_at"], "2026-01-01T00:00:00+00:00")
        self.assertIsNone(normalized["causal_id"])
        self.assertEqual(normalized["payload"], raw)  # full raw record preserved

    def test_normalizes_alternate_field_spellings(self):
        raw = {
            "event_id": "evt-abc",
            "entity_id": "task-9",
            "event_type": "task.created",
            "timestamp": "2026-02-02T00:00:00+00:00",
            "parent_event_id": "evt-parent",
        }
        normalized = normalize_kanban_task_event(raw)
        self.assertEqual(normalized["source_dedup_key"], "evt-abc")
        self.assertEqual(normalized["entity_id"], "task-9")
        self.assertEqual(normalized["event_type"], "kanban.task.created")
        self.assertEqual(normalized["occurred_at"], "2026-02-02T00:00:00+00:00")
        self.assertEqual(normalized["causal_id"], "evt-parent")

    def test_event_type_already_prefixed_is_not_double_prefixed(self):
        raw = {"id": 1, "task_id": "t", "type": "kanban.task.created"}
        normalized = normalize_kanban_task_event(raw)
        self.assertEqual(normalized["event_type"], "kanban.task.created")

    def test_missing_id_raises(self):
        with self.assertRaises(ValueError):
            normalize_kanban_task_event({"task_id": "t", "type": "task.created"})

    def test_missing_task_id_raises(self):
        with self.assertRaises(ValueError):
            normalize_kanban_task_event({"id": 1, "type": "task.created"})

    def test_missing_type_raises(self):
        with self.assertRaises(ValueError):
            normalize_kanban_task_event({"id": 1, "task_id": "t"})


class IngestKanbanTaskEventsTests(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.db_path = Path(self._tmp.name) / "state.db"
        self.conn = connect(self.db_path)
        self.addCleanup(self.conn.close)
        self.ledger = EventLedger(self.conn)
        self.batch = [
            {"id": 1, "task_id": "t-1", "type": "task.created"},
            {"id": 2, "task_id": "t-1", "type": "task.status_changed"},
            {"id": 3, "task_id": "t-2", "type": "task.created"},
        ]

    def test_boot_ingest_persists_the_whole_batch(self):
        records = ingest_kanban_task_events(self.ledger, self.batch)
        self.assertEqual(len(records), 3)
        self.assertEqual(self.ledger.count(), 3)

    def test_reset_replay_of_the_same_batch_is_idempotent(self):
        ingest_kanban_task_events(self.ledger, self.batch)
        ingest_kanban_task_events(self.ledger, self.batch)  # simulate a boot/reset replay
        self.assertEqual(self.ledger.count(), 3)

    def test_ingested_events_persist_across_a_reconnect(self):
        """Reset/replay after a process restart must see prior kanban events as
        already-ingested, not re-append them under a fresh connection."""
        ingest_kanban_task_events(self.ledger, self.batch)
        self.conn.close()

        reconnected = connect(self.db_path)
        self.addCleanup(reconnected.close)
        resumed_ledger = EventLedger(reconnected)
        self.assertEqual(resumed_ledger.count(), 3)

        ingest_kanban_task_events(resumed_ledger, self.batch)  # boot/reset entrypoint again
        self.assertEqual(resumed_ledger.count(), 3)  # still no duplicates

        record = resumed_ledger.get_by_dedup_key("kanban", "1")
        self.assertIsNotNone(record)
        self.assertEqual(record.entity_id, "t-1")


if __name__ == "__main__":
    unittest.main()
