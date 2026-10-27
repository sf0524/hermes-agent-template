"""Read-only old named-profile delivery ledger advisory for the Hermes upgrade."""

from __future__ import annotations

import contextlib
import importlib.util
import io
import sqlite3
import sys
import tempfile
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location(
    "check_pending_deliveries", ROOT / "template_helpers" / "check_pending_deliveries.py"
)
assert SPEC and SPEC.loader
preflight = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = preflight
SPEC.loader.exec_module(preflight)


SCHEMA = """CREATE TABLE delivery_obligations (
    obligation_id TEXT PRIMARY KEY,
    session_key TEXT, platform TEXT, chat_id TEXT, thread_id TEXT,
    content TEXT, state TEXT, attempts INTEGER, created_at REAL, updated_at REAL,
    owner_pid INTEGER, owner_started_at INTEGER, last_error TEXT
)"""


class DeliveryPreflightTests(unittest.TestCase):
    def setUp(self):
        tmp = tempfile.TemporaryDirectory(prefix="hermes-delivery-check-")
        self.addCleanup(tmp.cleanup)
        self.home = Path(tmp.name) / ".hermes"
        self.home.mkdir()
        (self.home / "profiles").mkdir()

    def profile(self, name="work") -> Path:
        path = self.home / "profiles" / name
        path.mkdir()
        return path

    def db(self, profile: Path) -> Path:
        db = profile / "state.db"
        with contextlib.closing(sqlite3.connect(db)) as conn:
            with conn:
                conn.execute(SCHEMA)
        return db

    def row(self, db: Path, state: str, id: str) -> None:
        with contextlib.closing(sqlite3.connect(db)) as conn:
            with conn:
                conn.execute(
                    "INSERT INTO delivery_obligations (obligation_id, content, state) VALUES (?, ?, ?)",
                    (id, "SECRET reply content and API key", state),
                )

    def run_check(self):
        output = io.StringIO()
        with contextlib.redirect_stdout(output):
            code = preflight.main(["--hermes-home", str(self.home)])
        return code, output.getvalue()

    def test_absent_db_and_absent_table_are_clear_and_do_not_create_db(self):
        no_db = self.profile("empty")
        no_table = self.profile("work")
        with contextlib.closing(sqlite3.connect(no_table / "state.db")) as conn:
            with conn:
                conn.execute("CREATE TABLE unrelated (value TEXT)")
        code, output = self.run_check()
        self.assertEqual(code, 0)
        self.assertIn("CLEAR profile=empty", output)
        self.assertIn("CLEAR profile=work", output)
        self.assertFalse((no_db / "state.db").exists())

    def test_zero_and_terminal_rows_are_clear(self):
        db = self.db(self.profile())
        self.assertEqual(self.run_check()[0], 0)
        self.row(db, "delivered", "one")
        self.row(db, "abandoned", "two")
        self.assertEqual(self.run_check()[0], 0)

    def test_all_nonterminal_states_are_counted_without_reading_message_content(self):
        db = self.db(self.profile())
        for index, state in enumerate(("pending", "pending", "attempting", "failed")):
            self.row(db, state, str(index))
        self.row(db, "delivered", "terminal")
        code, output = self.run_check()
        self.assertEqual(code, 2)
        self.assertIn("profile=work total=4 pending=2 attempting=1 failed=1", output)
        self.assertIn("may already have been sent", output)
        self.assertNotIn("SECRET", output)

    def test_uncheckpointed_wal_is_visible_without_changing_database_or_wal(self):
        profile = self.profile()
        db = profile / "state.db"
        writer = sqlite3.connect(db)
        self.addCleanup(writer.close)
        writer.execute("PRAGMA journal_mode=WAL")
        writer.execute(SCHEMA)
        writer.execute(
            "INSERT INTO delivery_obligations (obligation_id, content, state) VALUES ('one', 'SECRET', 'pending')"
        )
        writer.commit()
        wal = profile / "state.db-wal"
        self.assertTrue(wal.exists())
        db_before, wal_before = db.read_bytes(), wal.read_bytes()
        code, output = self.run_check()
        self.assertEqual(code, 2)
        self.assertIn("pending=1", output)
        self.assertEqual(db.read_bytes(), db_before)
        self.assertEqual(wal.read_bytes(), wal_before)

    def test_corrupt_db_and_unrecognized_schema_are_unknown(self):
        corrupt = self.profile("corrupt")
        (corrupt / "state.db").write_bytes(b"not a SQLite database")
        malformed = self.profile("malformed")
        with contextlib.closing(sqlite3.connect(malformed / "state.db")) as conn:
            with conn:
                conn.execute("CREATE TABLE delivery_obligations (state TEXT)")
        code, output = self.run_check()
        self.assertEqual(code, 3)
        self.assertIn("UNKNOWN profile=corrupt", output)
        self.assertIn("UNKNOWN profile=malformed", output)
        self.assertNotIn("not a SQLite database", output)

    def test_locked_db_is_unknown(self):
        db = self.db(self.profile())
        locker = sqlite3.connect(db)
        self.addCleanup(locker.close)
        locker.execute("BEGIN EXCLUSIVE")
        code, output = self.run_check()
        self.assertEqual(code, 3)
        self.assertIn("UNKNOWN profile=work", output)

    def test_unknown_state_is_unknown_even_if_another_row_is_pending(self):
        db = self.db(self.profile())
        self.row(db, "pending", "one")
        self.row(db, "SECRET unexpected state", "two")
        code, output = self.run_check()
        self.assertEqual(code, 3)
        self.assertIn("UNKNOWN profile=work", output)
        self.assertNotIn("SECRET", output)

    def test_symlinked_profile_and_database_are_unknown_without_following(self):
        outside = self.home.parent / "outside"
        outside.mkdir()
        (self.home / "profiles" / "linked").symlink_to(outside, target_is_directory=True)
        code, output = self.run_check()
        self.assertEqual(code, 3)
        self.assertIn("UNKNOWN profile=linked: profile directory is linked", output)

        (self.home / "profiles" / "linked").unlink()
        profile = self.profile("work")
        (profile / "state.db").symlink_to(outside / "state.db")
        code, output = self.run_check()
        self.assertEqual(code, 3)
        self.assertIn("UNKNOWN profile=work", output)
        self.assertFalse((outside / "state.db").exists())

    def test_orphan_wal_and_symlinked_sidecar_are_unknown(self):
        profile = self.profile("work")
        (profile / "state.db-wal").write_bytes(b"orphan")
        self.assertEqual(self.run_check()[0], 3)
        (profile / "state.db-wal").unlink()
        self.db(profile)
        (profile / "state.db-wal").symlink_to(self.home / ".env")
        self.assertEqual(self.run_check()[0], 3)

    def test_unknown_precedes_pending_in_exit_status_but_reports_both(self):
        db = self.db(self.profile("active"))
        self.row(db, "failed", "one")
        (self.profile("broken") / "state.db").write_bytes(b"corrupt")
        code, output = self.run_check()
        self.assertEqual(code, 3)
        self.assertIn("PENDING profile=active", output)
        self.assertIn("UNKNOWN profile=broken", output)

    def test_deleted_profile_is_not_treated_as_live(self):
        db = self.db(self.profile("retired"))
        self.row(db, "pending", "old")
        tombstones = self.home / "profiles" / ".deleted"
        tombstones.mkdir()
        (tombstones / "retired").write_text("deleted\n")

        code, output = self.run_check()

        self.assertEqual(code, 0)
        self.assertNotIn("retired", output)

    def test_linked_tombstones_directory_cannot_hide_old_rows(self):
        db = self.db(self.profile("work"))
        self.row(db, "pending", "old")
        (self.home / "profiles" / ".deleted").symlink_to(self.home, target_is_directory=True)

        code, output = self.run_check()

        self.assertEqual(code, 3)
        self.assertIn("UNKNOWN", output)

    def test_symlink_error_does_not_hide_other_profiles_pending_rows(self):
        db = self.db(self.profile("active"))
        self.row(db, "pending", "one")
        (self.home / "profiles" / "linked").symlink_to(self.home, target_is_directory=True)
        code, output = self.run_check()
        self.assertEqual(code, 3)
        self.assertIn("PENDING profile=active", output)
        self.assertIn("UNKNOWN profile=linked", output)


if __name__ == "__main__":
    unittest.main()
