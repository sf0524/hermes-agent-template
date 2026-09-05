import tempfile
import unittest
from pathlib import Path

from orchestrator.control_plane.db import connect, transaction
from orchestrator.control_plane.ledger import ObservationLedger, ObservationOutcome, evidence_hash


class ObservationLedgerTests(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.db_path = Path(self._tmp.name) / "control-plane.db"
        self.conn = connect(self.db_path)
        self.addCleanup(self.conn.close)

    def _record(self, ledger, **overrides):
        kwargs = dict(
            source="kanban",
            source_id="1",
            entity_id="task-1",
            kind="task.created",
            payload="{}",
            occurred_at="2026-01-01T00:00:00+00:00",
            policy_version="policy-v1",
            actor="observer",
            correlation_id="corr-1",
        )
        kwargs.update(overrides)
        return ledger.record(**kwargs)

    def test_first_observation_appends(self):
        with transaction(self.conn) as conn:
            ledger = ObservationLedger(conn)
            result = self._record(ledger)
        self.assertEqual(result.outcome, ObservationOutcome.APPENDED)
        self.assertEqual(ObservationLedger(self.conn).count(), 1)

    def test_exact_replay_is_a_noop_duplicate(self):
        with transaction(self.conn) as conn:
            ledger = ObservationLedger(conn)
            first = self._record(ledger)
        with transaction(self.conn) as conn:
            ledger = ObservationLedger(conn)
            second = self._record(ledger)  # identical payload/fields
        self.assertEqual(second.outcome, ObservationOutcome.DUPLICATE)
        self.assertEqual(second.observation.observation_id, first.observation.observation_id)
        self.assertEqual(ObservationLedger(self.conn).count(), 1)

    def test_conflicting_same_source_id_is_quarantined_not_overwritten(self):
        with transaction(self.conn) as conn:
            ledger = ObservationLedger(conn)
            first = self._record(ledger)
        with transaction(self.conn) as conn:
            ledger = ObservationLedger(conn)
            second = self._record(ledger, kind="task.status_changed", payload='{"changed": true}')
        self.assertEqual(second.outcome, ObservationOutcome.QUARANTINED)
        self.assertIsNotNone(second.conflict)
        # original observation is untouched
        self.assertEqual(ObservationLedger(self.conn).count(), 1)
        stored = ObservationLedger(self.conn).get_by_source_id("kanban", "1")
        self.assertEqual(stored.observation_id, first.observation.observation_id)
        self.assertEqual(stored.kind, "task.created")
        # conflict recorded as its own evidence row
        conflicts = ObservationLedger(self.conn).list_conflicts()
        self.assertEqual(len(conflicts), 1)
        self.assertEqual(conflicts[0].existing_observation_id, first.observation.observation_id)
        self.assertEqual(conflicts[0].conflicting_payload, '{"changed": true}')

    def test_different_source_id_appends_independently(self):
        with transaction(self.conn) as conn:
            ledger = ObservationLedger(conn)
            self._record(ledger, source_id="1")
            self._record(ledger, source_id="2", entity_id="task-2")
        self.assertEqual(ObservationLedger(self.conn).count(), 2)

    def test_evidence_hash_is_stable_for_identical_fields(self):
        h1 = evidence_hash(
            source="kanban", source_id="1", entity_id="t-1", run_id=None,
            kind="task.created", payload="{}", occurred_at="2026-01-01T00:00:00+00:00",
        )
        h2 = evidence_hash(
            source="kanban", source_id="1", entity_id="t-1", run_id=None,
            kind="task.created", payload="{}", occurred_at="2026-01-01T00:00:00+00:00",
        )
        self.assertEqual(h1, h2)

    def test_evidence_hash_changes_when_payload_changes(self):
        h1 = evidence_hash(
            source="kanban", source_id="1", entity_id="t-1", run_id=None,
            kind="task.created", payload="{}", occurred_at="2026-01-01T00:00:00+00:00",
        )
        h2 = evidence_hash(
            source="kanban", source_id="1", entity_id="t-1", run_id=None,
            kind="task.created", payload='{"x": 1}', occurred_at="2026-01-01T00:00:00+00:00",
        )
        self.assertNotEqual(h1, h2)

    def test_recorded_observation_carries_policy_actor_correlation_and_hash(self):
        with transaction(self.conn) as conn:
            ledger = ObservationLedger(conn)
            result = self._record(ledger, policy_version="policy-v9", actor="shadow-observer", correlation_id="corr-xyz")
        obs = result.observation
        self.assertEqual(obs.policy_version, "policy-v9")
        self.assertEqual(obs.actor, "shadow-observer")
        self.assertEqual(obs.correlation_id, "corr-xyz")
        self.assertTrue(obs.evidence_hash)


if __name__ == "__main__":
    unittest.main()
