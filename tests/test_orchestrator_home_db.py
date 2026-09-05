import os
import sqlite3
import tempfile
import threading
import unittest
from pathlib import Path
from unittest import mock

from orchestrator import schema
from orchestrator.db import connect, transaction
from orchestrator.home import InvalidHermesHomeError, get_hermes_home, orchestrator_state_db_path


class GetHermesHomeTests(unittest.TestCase):
    def test_defaults_to_dot_hermes_under_home(self):
        with mock.patch.dict(os.environ, {}, clear=False):
            os.environ.pop("HERMES_HOME", None)
            self.assertEqual(get_hermes_home(), Path.home() / ".hermes")

    def test_honours_hermes_home_env_var(self):
        with mock.patch.dict(os.environ, {"HERMES_HOME": "/tmp/some-profile"}):
            self.assertEqual(get_hermes_home(), Path("/tmp/some-profile"))

    def test_state_db_path_is_namespaced_under_orchestrator(self):
        with mock.patch.dict(os.environ, {"HERMES_HOME": "/tmp/some-profile"}):
            self.assertEqual(
                orchestrator_state_db_path(), Path("/tmp/some-profile") / "orchestrator" / "state.db"
            )

    def test_empty_hermes_home_falls_back_to_default_rather_than_cwd(self):
        with mock.patch.dict(os.environ, {"HERMES_HOME": ""}):
            self.assertEqual(get_hermes_home(), Path.home() / ".hermes")

    def test_relative_hermes_home_is_rejected_not_resolved_against_cwd(self):
        with mock.patch.dict(os.environ, {"HERMES_HOME": "relative/profile"}):
            with self.assertRaises(InvalidHermesHomeError):
                get_hermes_home()

    def test_relative_hermes_home_never_produces_a_relative_state_db_path(self):
        with mock.patch.dict(os.environ, {"HERMES_HOME": "relative/profile"}):
            with self.assertRaises(InvalidHermesHomeError):
                orchestrator_state_db_path()


class ConnectAndMigrateTests(unittest.TestCase):
    def test_connect_creates_parent_dir_and_schema(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "nested" / "state.db"
            conn = connect(db_path)
            try:
                self.assertTrue(db_path.exists())
                tables = {
                    row[0]
                    for row in conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'")
                }
                self.assertTrue(
                    {"events", "consumer_inbox", "consumer_cursor", "audit_log", "outbox", "schema_migrations"}
                    <= tables
                )
            finally:
                conn.close()

    def test_connect_is_idempotent_across_processes(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "state.db"
            conn1 = connect(db_path)
            conn1.close()
            # Simulate a second process/boot re-running migrations against the
            # same file — must not raise and must not duplicate migration rows.
            conn2 = connect(db_path)
            try:
                versions = [
                    row[0] for row in conn2.execute("SELECT version FROM schema_migrations ORDER BY version")
                ]
                self.assertEqual(versions, sorted(set(versions)))
            finally:
                conn2.close()

    def test_transaction_commits_on_success(self):
        with tempfile.TemporaryDirectory() as tmp:
            conn = connect(Path(tmp) / "state.db")
            try:
                with transaction(conn) as c:
                    c.execute(
                        "INSERT INTO audit_log (recorded_at, actor, action) VALUES (?, ?, ?)",
                        ("2026-01-01T00:00:00+00:00", "test", "unit.test"),
                    )
                count = conn.execute("SELECT COUNT(*) FROM audit_log").fetchone()[0]
                self.assertEqual(count, 1)
            finally:
                conn.close()

    def test_transaction_rolls_back_on_exception(self):
        with tempfile.TemporaryDirectory() as tmp:
            conn = connect(Path(tmp) / "state.db")
            try:
                with self.assertRaises(RuntimeError):
                    with transaction(conn) as c:
                        c.execute(
                            "INSERT INTO audit_log (recorded_at, actor, action) VALUES (?, ?, ?)",
                            ("2026-01-01T00:00:00+00:00", "test", "unit.test"),
                        )
                        raise RuntimeError("boom")
                count = conn.execute("SELECT COUNT(*) FROM audit_log").fetchone()[0]
                self.assertEqual(count, 0)
            finally:
                conn.close()

    def test_audit_log_is_append_only_at_db_level(self):
        with tempfile.TemporaryDirectory() as tmp:
            conn = connect(Path(tmp) / "state.db")
            try:
                conn.execute(
                    "INSERT INTO audit_log (recorded_at, actor, action) VALUES (?, ?, ?)",
                    ("2026-01-01T00:00:00+00:00", "test", "unit.test"),
                )
                with self.assertRaises(sqlite3.IntegrityError):
                    conn.execute("UPDATE audit_log SET action = 'tampered' WHERE id = 1")
                with self.assertRaises(sqlite3.IntegrityError):
                    conn.execute("DELETE FROM audit_log WHERE id = 1")
            finally:
                conn.close()

    def test_events_is_append_only_at_db_level(self):
        with tempfile.TemporaryDirectory() as tmp:
            conn = connect(Path(tmp) / "state.db")
            try:
                conn.execute(
                    """
                    INSERT INTO events
                        (event_id, source, source_dedup_key, entity_id, event_type, payload, occurred_at, received_at)
                    VALUES ('evt-1', 'kanban', 'k1', 'task-1', 't', '{}', '2026-01-01T00:00:00+00:00', '2026-01-01T00:00:00+00:00')
                    """
                )
                with self.assertRaises(sqlite3.IntegrityError):
                    conn.execute("UPDATE events SET event_type = 'tampered' WHERE event_id = 'evt-1'")
                with self.assertRaises(sqlite3.IntegrityError):
                    conn.execute("DELETE FROM events WHERE event_id = 'evt-1'")
            finally:
                conn.close()

    def test_consumer_inbox_enforces_foreign_key_to_events(self):
        with tempfile.TemporaryDirectory() as tmp:
            conn = connect(Path(tmp) / "state.db")
            try:
                with self.assertRaises(sqlite3.IntegrityError):
                    conn.execute(
                        """
                        INSERT INTO consumer_inbox
                            (consumer, seq, event_id, status, attempt_count, claimed_at, lease_expires_at, claim_token)
                        VALUES ('runtime_orchestrator', 999, 'no-such-event', 'claimed', 1,
                                '2026-01-01T00:00:00+00:00', '2026-01-01T00:01:00+00:00', 'tok')
                        """
                    )
            finally:
                conn.close()

    def test_consumer_inbox_accepts_a_valid_event_reference(self):
        with tempfile.TemporaryDirectory() as tmp:
            conn = connect(Path(tmp) / "state.db")
            try:
                conn.execute(
                    """
                    INSERT INTO events
                        (event_id, source, source_dedup_key, entity_id, event_type, payload, occurred_at, received_at)
                    VALUES ('evt-1', 'kanban', 'k1', 'task-1', 't', '{}', '2026-01-01T00:00:00+00:00', '2026-01-01T00:00:00+00:00')
                    """
                )
                conn.execute(
                    """
                    INSERT INTO consumer_inbox
                        (consumer, seq, event_id, status, attempt_count, claimed_at, lease_expires_at, claim_token)
                    VALUES ('runtime_orchestrator', 1, 'evt-1', 'claimed', 1,
                            '2026-01-01T00:00:00+00:00', '2026-01-01T00:01:00+00:00', 'tok')
                    """
                )  # must not raise
                count = conn.execute("SELECT COUNT(*) FROM consumer_inbox").fetchone()[0]
                self.assertEqual(count, 1)
            finally:
                conn.close()

    def test_consumer_inbox_rejects_a_seq_and_event_id_that_are_each_individually_valid_but_mismatched(self):
        """seq and event_id must name the *same* row in events. A single-column
        foreign key on event_id alone can't catch a row whose seq points at one
        real event and whose event_id points at a different real event — the
        composite foreign key on (seq, event_id) must."""
        with tempfile.TemporaryDirectory() as tmp:
            conn = connect(Path(tmp) / "state.db")
            try:
                conn.execute(
                    """
                    INSERT INTO events
                        (event_id, source, source_dedup_key, entity_id, event_type, payload, occurred_at, received_at)
                    VALUES ('evt-1', 'kanban', 'k1', 'task-1', 't', '{}', '2026-01-01T00:00:00+00:00', '2026-01-01T00:00:00+00:00')
                    """
                )
                conn.execute(
                    """
                    INSERT INTO events
                        (event_id, source, source_dedup_key, entity_id, event_type, payload, occurred_at, received_at)
                    VALUES ('evt-2', 'kanban', 'k2', 'task-2', 't', '{}', '2026-01-01T00:00:01+00:00', '2026-01-01T00:00:01+00:00')
                    """
                )
                # seq=1 is a real event's seq (evt-1) and 'evt-2' is a real
                # event's id — each individually valid — but seq 1 is not
                # evt-2's seq, so the pair must be rejected.
                with self.assertRaises(sqlite3.IntegrityError):
                    conn.execute(
                        """
                        INSERT INTO consumer_inbox
                            (consumer, seq, event_id, status, attempt_count, claimed_at, lease_expires_at, claim_token)
                        VALUES ('runtime_orchestrator', 1, 'evt-2', 'claimed', 1,
                                '2026-01-01T00:00:00+00:00', '2026-01-01T00:01:00+00:00', 'tok')
                        """
                    )
            finally:
                conn.close()


class ConcurrentBootTests(unittest.TestCase):
    def test_concurrent_fresh_boot_does_not_crash_or_duplicate_migrations(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "nested" / "state.db"
            errors = []
            barrier = threading.Barrier(8)

            def _boot():
                barrier.wait()
                try:
                    conn = connect(db_path)
                    conn.close()
                except Exception as exc:  # pragma: no cover - assertion below is the real check
                    errors.append(exc)

            threads = [threading.Thread(target=_boot) for _ in range(8)]
            for t in threads:
                t.start()
            for t in threads:
                t.join()

            self.assertEqual(errors, [])

            verify = connect(db_path)
            try:
                versions = [
                    row[0] for row in verify.execute("SELECT version FROM schema_migrations ORDER BY version")
                ]
                self.assertEqual(versions, [1, 2, 3, 4])
            finally:
                verify.close()

    def test_concurrent_upgrade_applies_a_new_migration_exactly_once(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "state.db"
            bootstrap = connect(db_path)
            bootstrap.close()

            extra_version = 9001
            extra_statements = ["CREATE TABLE IF NOT EXISTS _concurrent_upgrade_probe (id INTEGER PRIMARY KEY)"]
            patched_migrations = list(schema.MIGRATIONS) + [(extra_version, extra_statements)]

            errors = []
            barrier = threading.Barrier(8)

            def _upgrade():
                barrier.wait()
                try:
                    conn = connect(db_path)
                    conn.close()
                except Exception as exc:  # pragma: no cover - assertion below is the real check
                    errors.append(exc)

            with mock.patch.object(schema, "MIGRATIONS", patched_migrations):
                threads = [threading.Thread(target=_upgrade) for _ in range(8)]
                for t in threads:
                    t.start()
                for t in threads:
                    t.join()

            self.assertEqual(errors, [])

            verify = connect(db_path)
            try:
                count = verify.execute(
                    "SELECT COUNT(*) FROM schema_migrations WHERE version = ?", (extra_version,)
                ).fetchone()[0]
                self.assertEqual(count, 1)  # exactly one row, not one per racing connection
                probe_exists = verify.execute(
                    "SELECT name FROM sqlite_master WHERE type = 'table' AND name = '_concurrent_upgrade_probe'"
                ).fetchone()
                self.assertIsNotNone(probe_exists)
            finally:
                verify.close()


if __name__ == "__main__":
    unittest.main()
