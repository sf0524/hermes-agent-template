import tempfile
import threading
import unittest
from pathlib import Path

from orchestrator.control_plane.capabilities import (
    SEED_CAPABILITIES,
    VALID_STATUSES,
    CapabilityRegistry,
    seed_default_capabilities,
)
from orchestrator.control_plane.db import connect


class CapabilityRegistryTests(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.conn = connect(Path(self._tmp.name) / "control-plane.db")
        self.addCleanup(self.conn.close)
        self.registry = CapabilityRegistry(self.conn)

    def test_record_defaults_to_inventory_status(self):
        record = self.registry.record(capability_id="claude-sonnet-5/high", cli="claude", model="claude-sonnet-5", effort="high")
        self.assertEqual(record.status, "inventory")

    def test_rejects_status_outside_inventory_and_unverified(self):
        with self.assertRaises(ValueError):
            self.registry.record(
                capability_id="claude-sonnet-5/high", cli="claude", model="claude-sonnet-5",
                effort="high", status="approved",
            )

    def test_valid_statuses_never_include_approved_or_dispatch_like_terms(self):
        self.assertEqual(VALID_STATUSES, frozenset({"inventory", "unverified"}))

    def test_re_recording_updates_probe_evidence_without_duplicating_row(self):
        self.registry.record(capability_id="claude-sonnet-5/high", cli="claude", model="claude-sonnet-5", effort="high")
        self.registry.record(
            capability_id="claude-sonnet-5/high", cli="claude", model="claude-sonnet-5", effort="high",
            status="unverified", probe_evidence={"probe": "auth-check", "result": "expired"},
        )
        all_records = self.registry.list_all()
        self.assertEqual(len(all_records), 1)
        self.assertEqual(all_records[0].status, "unverified")
        self.assertEqual(all_records[0].probe_evidence, {"probe": "auth-check", "result": "expired"})

    def test_get_returns_none_for_unknown_capability(self):
        self.assertIsNone(self.registry.get("nonexistent/high"))

    def test_seed_default_capabilities_records_documented_identities(self):
        seed_default_capabilities(self.registry)
        recorded_ids = {r.capability_id for r in self.registry.list_all()}
        expected_ids = {seed["capability_id"] for seed in SEED_CAPABILITIES}
        self.assertEqual(recorded_ids, expected_ids)
        self.assertIn("claude-fable-5-1/high", recorded_ids)
        self.assertIn("claude-sonnet-5/high", recorded_ids)
        self.assertIn("claude-opus-5/xhigh", recorded_ids)
        self.assertIn("codex-terra/high", recorded_ids)
        self.assertIn("codex-terra/xhigh", recorded_ids)
        self.assertIn("codex-sol/high", recorded_ids)

    def test_seed_is_idempotent(self):
        seed_default_capabilities(self.registry)
        seed_default_capabilities(self.registry)
        self.assertEqual(len(self.registry.list_all()), len(SEED_CAPABILITIES))

    def test_codex_sol_high_is_recorded_with_counter_review_role(self):
        seed_default_capabilities(self.registry)
        record = self.registry.get("codex-sol/high")
        self.assertEqual(record.role, "counter-review")

    def test_all_seeded_entries_are_inventory_status(self):
        seed_default_capabilities(self.registry)
        for record in self.registry.list_all():
            self.assertEqual(record.status, "inventory")


class ReseedAtomicityTests(unittest.TestCase):
    """Finding 5: reseeding must be a genuinely atomic, seed-only INSERT
    (ON CONFLICT DO NOTHING) rather than a check-then-act read-then-record —
    the old approach's own record() call unconditionally upserts (overwriting
    status/probe_evidence back to bare inventory defaults), so a real probe
    result that commits after the reseed's read but before the reseed's own
    write would be silently clobbered under the old approach."""

    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.db_path = Path(self._tmp.name) / "control-plane.db"

    def test_reseed_write_blocked_behind_a_concurrent_probe_commit_does_not_clobber_it(self):
        # sqlite3 connections can only be used from the thread that created
        # them, so the reseed's connection must be opened *inside* its own
        # thread. Pre-migrating first means that connect() call never itself
        # contends for conn_a's lock. Two events force a deterministic
        # ordering without any sleep: the reseed doesn't start its own work
        # until conn_a is confirmed to hold the write lock, and conn_a's
        # write lock is held continuously from BEGIN IMMEDIATE until its own
        # probe write commits — so regardless of exact thread scheduling,
        # the reseed's write can only ever complete strictly after the
        # probe's commit.
        target = SEED_CAPABILITIES[0]
        connect(self.db_path).close()

        conn_b_ready = threading.Event()
        conn_a_locked = threading.Event()
        result = {}
        errors = []

        def run_reseed():
            conn_b = connect(self.db_path)
            try:
                conn_b_ready.set()
                conn_a_locked.wait(timeout=5)
                result["records"] = seed_default_capabilities(CapabilityRegistry(conn_b))
            except Exception as exc:  # noqa: BLE001 - surfaced via errors below
                errors.append(exc)
            finally:
                conn_b.close()

        t = threading.Thread(target=run_reseed)
        t.start()
        self.assertTrue(conn_b_ready.wait(timeout=5), "reseed connection never opened")

        conn_a = connect(self.db_path)
        self.addCleanup(conn_a.close)
        conn_a.execute("BEGIN IMMEDIATE")
        conn_a_locked.set()

        # Commit a real probe result while still holding the write lock —
        # the reseed's own write cannot proceed until this commits.
        CapabilityRegistry(conn_a).record(
            capability_id=target["capability_id"],
            cli=target["cli"],
            model=target["model"],
            effort=target["effort"],
            status="unverified",
            probe_evidence={"probe": "auth-check", "result": "ok"},
        )
        conn_a.execute("COMMIT")

        t.join(timeout=10)
        for exc in errors:
            raise exc

        verify_conn = connect(self.db_path)
        self.addCleanup(verify_conn.close)
        record = CapabilityRegistry(verify_conn).get(target["capability_id"])
        self.assertEqual(record.status, "unverified")
        self.assertEqual(record.probe_evidence, {"probe": "auth-check", "result": "ok"})


class NoDispatchSurfaceTests(unittest.TestCase):
    """Structural proof that CapabilityRegistry cannot produce a command or side effect."""

    def test_registry_exposes_no_execution_or_dispatch_methods(self):
        forbidden_substrings = ("dispatch", "execute", "run", "approve", "launch", "spawn", "invoke")
        public_methods = [
            name
            for name in dir(CapabilityRegistry)
            if not name.startswith("_") and callable(getattr(CapabilityRegistry, name))
        ]
        self.assertEqual(set(public_methods), {"record", "get", "list_all", "seed_if_missing"})
        for name in public_methods:
            for forbidden in forbidden_substrings:
                self.assertNotIn(forbidden, name.lower())


if __name__ == "__main__":
    unittest.main()
