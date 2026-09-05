import os
import sqlite3
import tempfile
import unittest
from pathlib import Path

from orchestrator.control_plane.db import RelativePersistentDbPathError, connect
from orchestrator.control_plane.schema import (
    MIGRATIONS,
    REQUIRED_TABLE_COLUMNS,
    REQUIRED_TRIGGERS,
    SchemaVerificationError,
    evaluate_schema,
    latest_schema_version,
    verify_schema,
)


class FreshBootTests(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.db_path = Path(self._tmp.name) / "control-plane.db"

    def test_fresh_boot_creates_all_expected_tables(self):
        conn = connect(self.db_path)
        self.addCleanup(conn.close)
        tables = {
            row["name"]
            for row in conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'").fetchall()
        }
        expected = {
            "observations",
            "source_cursors",
            "duplicate_conflicts",
            "capability_inventory",
            "recovery_checkpoints",
            "schema_migrations",
        }
        self.assertTrue(expected.issubset(tables))

    def test_fresh_boot_records_latest_schema_version(self):
        conn = connect(self.db_path)
        self.addCleanup(conn.close)
        applied = {row["version"] for row in conn.execute("SELECT version FROM schema_migrations").fetchall()}
        self.assertIn(latest_schema_version(), applied)

    def test_repeat_boot_is_idempotent(self):
        conn1 = connect(self.db_path)
        conn1.close()
        conn2 = connect(self.db_path)  # second boot against the same file
        self.addCleanup(conn2.close)
        applied = conn2.execute("SELECT COUNT(*) FROM schema_migrations").fetchone()[0]
        self.assertEqual(applied, len(conn2.execute("SELECT DISTINCT version FROM schema_migrations").fetchall()))

    def test_repeat_connect_on_same_open_connection_does_not_error(self):
        # run_migrations() runs again on every connect(); calling it twice
        # in-process against a live connection must not raise or duplicate rows.
        conn = connect(self.db_path)
        self.addCleanup(conn.close)
        from orchestrator.control_plane.schema import run_migrations

        run_migrations(conn)  # second application, same connection
        applied = conn.execute("SELECT COUNT(*) FROM schema_migrations").fetchone()[0]
        from orchestrator.control_plane.schema import MIGRATIONS

        self.assertEqual(applied, len(MIGRATIONS))


class RelativePersistentPathRejectionTests(unittest.TestCase):
    """Finding 1: connect() must reject every relative persistent path, no DB created."""

    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self._original_cwd = os.getcwd()
        os.chdir(self._tmp.name)
        self.addCleanup(os.chdir, self._original_cwd)

    def test_relative_path_is_rejected_and_creates_no_db(self):
        with self.assertRaises(RelativePersistentDbPathError):
            connect("relative-control.db")
        # Adversarial proof, not just "raises": nothing was ever created,
        # neither at the relative name in cwd nor anywhere else in the tmp dir.
        self.assertFalse((Path(self._tmp.name) / "relative-control.db").exists())
        self.assertEqual(list(Path(self._tmp.name).iterdir()), [])

    def test_relative_path_with_subdirectory_is_rejected_and_creates_no_db(self):
        with self.assertRaises(RelativePersistentDbPathError):
            connect("nested/relative-control.db")
        self.assertFalse((Path(self._tmp.name) / "nested").exists())

    def test_bare_memory_string_is_rejected_through_public_connect(self):
        # ":memory:" must not be reachable through the public connect() path
        # at all — only the internal/test-only helper may open one.
        with self.assertRaises(RelativePersistentDbPathError):
            connect(":memory:")

    def test_internal_in_memory_helper_still_works_for_tests(self):
        from orchestrator.control_plane.db import _connect_in_memory_for_tests

        conn = _connect_in_memory_for_tests()
        try:
            applied = {row[0] for row in conn.execute("SELECT version FROM schema_migrations").fetchall()}
            self.assertIn(latest_schema_version(), applied)
        finally:
            conn.close()


class MigrationSquashTests(unittest.TestCase):
    """The original schema is squashed; later security-only migrations stay additive.

    In particular, no ALTER TABLE backfills pre-existing duplicate_conflicts
    rows with fabricated empty values, while the rowid REPLACE guard can be
    safely added to already-created state DBs.
    """

    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.db_path = Path(self._tmp.name) / "control-plane.db"

    def test_migration_history_has_only_the_base_schema_and_additive_security_guard(self):
        self.assertEqual([version for version, _ in MIGRATIONS], [1, 2])

    def test_duplicate_conflicts_conflict_detail_columns_have_no_fabricated_default(self):
        conn = connect(self.db_path)
        self.addCleanup(conn.close)
        columns = {row["name"]: row["dflt_value"] for row in conn.execute("PRAGMA table_info(duplicate_conflicts)")}
        for name in ("conflicting_entity_id", "conflicting_kind", "conflicting_occurred_at"):
            self.assertIsNone(
                columns[name],
                f"{name} must have no DEFAULT — every writer must supply a real value, "
                "never a silently fabricated empty string",
            )

    def test_duplicate_conflicts_conflict_detail_columns_are_present_from_creation(self):
        conn = connect(self.db_path)
        self.addCleanup(conn.close)
        columns = {row["name"] for row in conn.execute("PRAGMA table_info(duplicate_conflicts)")}
        expected = {"conflicting_entity_id", "conflicting_run_id", "conflicting_kind", "conflicting_occurred_at"}
        self.assertTrue(expected.issubset(columns))

    def test_omitting_a_conflict_detail_column_on_insert_is_rejected(self):
        # Proves there is no DEFAULT standing in for a real value: an insert
        # that omits a required conflict-detail column must fail outright,
        # not silently substitute ''.
        conn = connect(self.db_path)
        self.addCleanup(conn.close)
        with self.assertRaises(sqlite3.IntegrityError):
            conn.execute(
                """
                INSERT INTO duplicate_conflicts
                    (source, source_id, existing_observation_id, existing_evidence_hash,
                     conflicting_run_id, conflicting_payload, conflicting_evidence_hash, detected_at)
                VALUES ('kanban', '1', 'obs-1', 'hash-1', NULL, '{}', 'hash-2', 'now')
                """
            )


class AppendOnlyEnforcementTests(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.db_path = Path(self._tmp.name) / "control-plane.db"
        self.conn = connect(self.db_path)
        self.addCleanup(self.conn.close)
        self.conn.execute(
            """
            INSERT INTO observations (
                observation_id, source, source_id, entity_id, run_id, kind, payload,
                occurred_at, observed_at, policy_version, actor, correlation_id, evidence_hash
            ) VALUES ('obs-1', 'kanban', '1', 't-1', NULL, 'task.created', '{}', 'now', 'now', 'v1', 'observer', 'corr-1', 'hash-1')
            """
        )

    def test_direct_update_on_observations_is_rejected(self):
        with self.assertRaises(sqlite3.IntegrityError):
            self.conn.execute("UPDATE observations SET kind = 'tampered' WHERE observation_id = 'obs-1'")

    def test_direct_delete_on_observations_is_rejected(self):
        with self.assertRaises(sqlite3.IntegrityError):
            self.conn.execute("DELETE FROM observations WHERE observation_id = 'obs-1'")

    def test_direct_update_on_recovery_checkpoints_is_rejected(self):
        self.conn.execute(
            """
            INSERT INTO recovery_checkpoints
                (checkpoint_at, source, cursor_value, observation_count, schema_version, integrity_hash)
            VALUES ('now', 'kanban', '1', 1, 1, 'hash')
            """
        )
        with self.assertRaises(sqlite3.IntegrityError):
            self.conn.execute("UPDATE recovery_checkpoints SET cursor_value = '999' WHERE source = 'kanban'")

    def test_direct_delete_on_recovery_checkpoints_is_rejected(self):
        self.conn.execute(
            """
            INSERT INTO recovery_checkpoints
                (checkpoint_at, source, cursor_value, observation_count, schema_version, integrity_hash)
            VALUES ('now', 'kanban', '1', 1, 1, 'hash')
            """
        )
        with self.assertRaises(sqlite3.IntegrityError):
            self.conn.execute("DELETE FROM recovery_checkpoints WHERE source = 'kanban'")

    def test_direct_update_on_duplicate_conflicts_is_rejected(self):
        self.conn.execute(
            """
            INSERT INTO duplicate_conflicts
                (source, source_id, existing_observation_id, existing_evidence_hash,
                 conflicting_entity_id, conflicting_kind, conflicting_payload,
                 conflicting_occurred_at, conflicting_evidence_hash, detected_at)
            VALUES ('kanban', '1', 'obs-1', 'hash-1', 't-1', 'task.created', '{}', 'now', 'hash-2', 'now')
            """
        )
        with self.assertRaises(sqlite3.IntegrityError):
            self.conn.execute("UPDATE duplicate_conflicts SET source_id = '999' WHERE source = 'kanban'")

    def test_direct_delete_on_duplicate_conflicts_is_rejected(self):
        self.conn.execute(
            """
            INSERT INTO duplicate_conflicts
                (source, source_id, existing_observation_id, existing_evidence_hash,
                 conflicting_entity_id, conflicting_kind, conflicting_payload,
                 conflicting_occurred_at, conflicting_evidence_hash, detected_at)
            VALUES ('kanban', '1', 'obs-1', 'hash-1', 't-1', 'task.created', '{}', 'now', 'hash-2', 'now')
            """
        )
        with self.assertRaises(sqlite3.IntegrityError):
            self.conn.execute("DELETE FROM duplicate_conflicts WHERE source = 'kanban'")

    def test_insert_or_replace_on_observations_is_rejected(self):
        # Default SQLite (recursive_triggers=OFF) lets "INSERT OR REPLACE"
        # silently delete-then-reinsert a conflicting row without ever firing
        # the BEFORE DELETE trigger. connect() must force recursive_triggers
        # ON so this stays blocked like any other delete.
        with self.assertRaises(sqlite3.IntegrityError):
            self.conn.execute(
                """
                INSERT OR REPLACE INTO observations (
                    observation_id, source, source_id, entity_id, run_id, kind, payload,
                    occurred_at, observed_at, policy_version, actor, correlation_id, evidence_hash
                ) VALUES ('obs-1', 'kanban', '1', 't-1', NULL, 'task.tampered', '{}', 'now', 'now', 'v1', 'observer', 'corr-1', 'hash-tampered')
                """
            )
        row = self.conn.execute("SELECT kind FROM observations WHERE observation_id = 'obs-1'").fetchone()
        self.assertEqual(row["kind"], "task.created")

    def test_insert_or_replace_on_recovery_checkpoints_is_rejected(self):
        self.conn.execute(
            """
            INSERT INTO recovery_checkpoints
                (id, checkpoint_at, source, cursor_value, observation_count, schema_version, integrity_hash)
            VALUES (1, 'now', 'kanban', '1', 1, 1, 'hash')
            """
        )
        with self.assertRaises(sqlite3.IntegrityError):
            self.conn.execute(
                """
                INSERT OR REPLACE INTO recovery_checkpoints
                    (id, checkpoint_at, source, cursor_value, observation_count, schema_version, integrity_hash)
                VALUES (1, 'now', 'kanban', '999', 1, 1, 'hash-tampered')
                """
            )

    def test_insert_or_replace_on_duplicate_conflicts_is_rejected(self):
        self.conn.execute(
            """
            INSERT INTO duplicate_conflicts
                (id, source, source_id, existing_observation_id, existing_evidence_hash,
                 conflicting_entity_id, conflicting_kind, conflicting_payload,
                 conflicting_occurred_at, conflicting_evidence_hash, detected_at)
            VALUES (1, 'kanban', '1', 'obs-1', 'hash-1', 't-1', 'task.created', '{}', 'now', 'hash-2', 'now')
            """
        )
        with self.assertRaises(sqlite3.IntegrityError):
            self.conn.execute(
                """
                INSERT OR REPLACE INTO duplicate_conflicts
                    (id, source, source_id, existing_observation_id, existing_evidence_hash,
                     conflicting_entity_id, conflicting_kind, conflicting_payload,
                     conflicting_occurred_at, conflicting_evidence_hash, detected_at)
                VALUES (1, 'kanban', '999', 'obs-1', 'hash-1', 't-1', 'task.created', '{}', 'now', 'hash-2', 'now')
                """
            )


class RawConnectionReplaceProofTests(unittest.TestCase):
    """Finding 1: append-only guards must hold even from a brand-new raw
    sqlite3 connection that never went through connect() and therefore never
    had recursive_triggers turned on — recursive_triggers is a per-connection
    pragma, not a property of the DB file, so the guard must be enforced at
    the schema level (BEFORE INSERT triggers keyed on each identity column,
    which fire regardless of recursive_triggers since they intercept the
    INSERT itself, not the implicit delete REPLACE performs) rather than
    relying on every future connection remembering to set that pragma."""

    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.db_path = Path(self._tmp.name) / "control-plane.db"
        connect(self.db_path).close()  # create + migrate via the real path
        self.raw = sqlite3.connect(str(self.db_path))
        self.addCleanup(self.raw.close)
        self.raw.execute("PRAGMA recursive_triggers = OFF")
        self.raw.row_factory = sqlite3.Row
        self.raw.execute(
            """
            INSERT INTO observations (
                observation_id, source, source_id, entity_id, run_id, kind, payload,
                occurred_at, observed_at, policy_version, actor, correlation_id, evidence_hash
            ) VALUES ('obs-1', 'kanban', '1', 't-1', NULL, 'task.created', '{}', 'now', 'now', 'v1', 'observer', 'corr-1', 'hash-1')
            """
        )
        self.raw.execute(
            """
            INSERT INTO recovery_checkpoints
                (id, checkpoint_at, source, cursor_value, observation_count, schema_version, integrity_hash)
            VALUES (1, 'now', 'kanban', '1', 1, 1, 'hash')
            """
        )
        self.raw.execute(
            """
            INSERT INTO duplicate_conflicts
                (id, source, source_id, existing_observation_id, existing_evidence_hash,
                 conflicting_entity_id, conflicting_kind, conflicting_payload,
                 conflicting_occurred_at, conflicting_evidence_hash, detected_at)
            VALUES (1, 'kanban', '1', 'obs-1', 'hash-1', 't-1', 'task.created', '{}', 'now', 'hash-2', 'now')
            """
        )
        self.raw.execute(
            """
            INSERT INTO source_identity (rowid, source, canonical_path, device, inode, recorded_at)
            VALUES (41, 'kanban', '/trusted/kanban.db', 10, 20, 'now')
            """
        )
        self.raw.commit()

    def test_recursive_triggers_pragma_confirmed_off(self):
        # Sanity check the adversarial premise: this raw connection really
        # does have recursive_triggers off (SQLite's own default).
        self.assertEqual(self.raw.execute("PRAGMA recursive_triggers").fetchone()[0], 0)

    def test_replace_by_observation_id_is_rejected_on_raw_connection(self):
        with self.assertRaises(sqlite3.IntegrityError):
            self.raw.execute(
                """
                INSERT OR REPLACE INTO observations (
                    observation_id, source, source_id, entity_id, run_id, kind, payload,
                    occurred_at, observed_at, policy_version, actor, correlation_id, evidence_hash
                ) VALUES ('obs-1', 'kanban', '1', 't-1', NULL, 'task.tampered', '{}', 'now', 'now', 'v1', 'observer', 'corr-1', 'hash-tampered')
                """
            )
        row = self.raw.execute("SELECT kind FROM observations WHERE observation_id = 'obs-1'").fetchone()
        self.assertEqual(row["kind"], "task.created")

    def test_replace_by_source_source_id_is_rejected_on_raw_connection(self):
        # Same logical row, targeted via the OTHER unique identity
        # ((source, source_id)) rather than observation_id.
        with self.assertRaises(sqlite3.IntegrityError):
            self.raw.execute(
                """
                INSERT OR REPLACE INTO observations (
                    observation_id, source, source_id, entity_id, run_id, kind, payload,
                    occurred_at, observed_at, policy_version, actor, correlation_id, evidence_hash
                ) VALUES ('obs-evil', 'kanban', '1', 't-1', NULL, 'task.tampered', '{}', 'now', 'now', 'v1', 'observer', 'corr-1', 'hash-tampered')
                """
            )
        row = self.raw.execute("SELECT observation_id, kind FROM observations WHERE source = 'kanban' AND source_id = '1'").fetchone()
        self.assertEqual(row["observation_id"], "obs-1")
        self.assertEqual(row["kind"], "task.created")

    def test_replace_by_seq_is_rejected_on_raw_connection(self):
        seq = self.raw.execute("SELECT seq FROM observations WHERE observation_id = 'obs-1'").fetchone()[0]
        with self.assertRaises(sqlite3.IntegrityError):
            self.raw.execute(
                f"""
                INSERT OR REPLACE INTO observations (
                    seq, observation_id, source, source_id, entity_id, run_id, kind, payload,
                    occurred_at, observed_at, policy_version, actor, correlation_id, evidence_hash
                ) VALUES ({seq}, 'obs-evil', 'kanban', '999', 't-1', NULL, 'task.tampered', '{{}}', 'now', 'now', 'v1', 'observer', 'corr-1', 'hash-tampered')
                """
            )
        row = self.raw.execute("SELECT observation_id FROM observations WHERE seq = ?", (seq,)).fetchone()
        self.assertEqual(row["observation_id"], "obs-1")

    def test_replace_recovery_checkpoints_by_id_is_rejected_on_raw_connection(self):
        with self.assertRaises(sqlite3.IntegrityError):
            self.raw.execute(
                """
                INSERT OR REPLACE INTO recovery_checkpoints
                    (id, checkpoint_at, source, cursor_value, observation_count, schema_version, integrity_hash)
                VALUES (1, 'now', 'kanban', '999', 1, 1, 'hash-tampered')
                """
            )
        row = self.raw.execute("SELECT cursor_value FROM recovery_checkpoints WHERE id = 1").fetchone()
        self.assertEqual(row["cursor_value"], "1")

    def test_replace_duplicate_conflicts_by_id_is_rejected_on_raw_connection(self):
        with self.assertRaises(sqlite3.IntegrityError):
            self.raw.execute(
                """
                INSERT OR REPLACE INTO duplicate_conflicts
                    (id, source, source_id, existing_observation_id, existing_evidence_hash,
                     conflicting_entity_id, conflicting_kind, conflicting_payload,
                     conflicting_occurred_at, conflicting_evidence_hash, detected_at)
                VALUES (1, 'kanban', '999', 'obs-1', 'hash-1', 't-1', 'task.created', '{}', 'now', 'hash-2', 'now')
                """
            )
        row = self.raw.execute("SELECT source_id FROM duplicate_conflicts WHERE id = 1").fetchone()
        self.assertEqual(row["source_id"], "1")

    def test_replace_source_identity_by_hidden_rowid_is_rejected_on_raw_connection(self):
        # source_identity's TEXT primary key is not its hidden rowid.  A raw
        # connection with recursive triggers disabled can target that second
        # identity directly; its append-only guard must stop REPLACE before
        # SQLite performs the implicit delete.
        with self.assertRaises(sqlite3.IntegrityError):
            self.raw.execute(
                """
                INSERT OR REPLACE INTO source_identity
                    (rowid, source, canonical_path, device, inode, recorded_at)
                VALUES (41, 'evil-source', '/attacker/kanban.db', 99, 99, 'later')
                """
            )
        row = self.raw.execute(
            "SELECT rowid, source, canonical_path, device, inode FROM source_identity WHERE rowid = 41"
        ).fetchone()
        self.assertEqual(tuple(row), (41, "kanban", "/trusted/kanban.db", 10, 20))

    def test_legitimate_fresh_insert_without_replace_still_works_on_raw_connection(self):
        # The guard must never block an ordinary, non-conflicting append.
        self.raw.execute(
            """
            INSERT INTO observations (
                observation_id, source, source_id, entity_id, run_id, kind, payload,
                occurred_at, observed_at, policy_version, actor, correlation_id, evidence_hash
            ) VALUES ('obs-2', 'kanban', '2', 't-2', NULL, 'task.created', '{}', 'now', 'now', 'v1', 'observer', 'corr-2', 'hash-2')
            """
        )
        row = self.raw.execute("SELECT observation_id FROM observations WHERE source_id = '2'").fetchone()
        self.assertEqual(row["observation_id"], "obs-2")


class TriggerFingerprintAndTableContractTests(unittest.TestCase):
    """Finding 2: verification must recognize a same-name no-op trigger as a
    failure, and validate each required column's declared type/constraint
    (not merely that the column name exists)."""

    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.db_path = Path(self._tmp.name) / "control-plane.db"
        self.conn = connect(self.db_path)
        self.addCleanup(self.conn.close)

    def test_healthy_fresh_db_verifies_clean(self):
        result = verify_schema(self.conn)
        self.assertEqual(result.integrity_check, "ok")

    def test_same_name_no_op_trigger_is_detected(self):
        # A trigger that exists under the exact required name, and even
        # fires on the right event, but whose body doesn't actually enforce
        # append-only (a no-op) — passing a name-only existence check while
        # providing zero real protection.
        self.conn.execute("DROP TRIGGER observations_no_update")
        self.conn.execute(
            """
            CREATE TRIGGER observations_no_update
            BEFORE UPDATE ON observations
            BEGIN
                SELECT 1;
            END
            """
        )
        with self.assertRaises(SchemaVerificationError):
            evaluate_schema(self.conn)

    def test_table_contract_column_type_mismatch_is_detected(self):
        # source_cursors carries no per-row triggers, isolating this case to
        # purely a column-type contract violation.
        self.conn.execute("DROP TABLE source_cursors")
        self.conn.execute(
            "CREATE TABLE source_cursors (source TEXT PRIMARY KEY, cursor_value INTEGER NOT NULL, updated_at TEXT NOT NULL)"
        )
        with self.assertRaises(SchemaVerificationError):
            evaluate_schema(self.conn)

    def test_table_contract_notnull_mismatch_is_detected(self):
        self.conn.execute("DROP TABLE source_cursors")
        self.conn.execute(
            "CREATE TABLE source_cursors (source TEXT PRIMARY KEY, cursor_value TEXT, updated_at TEXT NOT NULL)"
        )
        with self.assertRaises(SchemaVerificationError):
            evaluate_schema(self.conn)

    def test_source_cursors_has_consumed_digest_column_with_correct_contract(self):
        # Codex re-review Finding C: the durable cursor bookmark must carry a
        # consumed-row-prefix digest alongside cursor_value, not just the bare
        # cursor integer, or an in-place tamper of an already-consumed row
        # earlier than the cursor's anchor row is undetectable.
        columns = {row["name"]: row for row in self.conn.execute("PRAGMA table_info(source_cursors)")}
        self.assertIn("consumed_digest", columns)
        self.assertEqual(columns["consumed_digest"]["type"].upper(), "TEXT")
        self.assertEqual(columns["consumed_digest"]["notnull"], 1)

    def test_healthy_db_trigger_fingerprints_cover_every_required_trigger(self):
        result = verify_schema(self.conn)
        fingerprint_names = {entry[0] for entry in result.trigger_fingerprints}
        self.assertEqual(fingerprint_names, set(REQUIRED_TRIGGERS))


class TriggerFingerprintSpoofTests(unittest.TestCase):
    """Codex re-review Finding B: trigger verification must compare complete
    normalized trigger definitions (target table, BEFORE timing/event,
    WHEN clause, and the literal RAISE(ABORT, ...) body) rather than an
    expected-string substring match against the raw trigger SQL text."""

    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.db_path = Path(self._tmp.name) / "control-plane.db"
        self.conn = connect(self.db_path)
        self.addCleanup(self.conn.close)

    def test_select_spoof_trigger_with_decoy_message_text_is_detected(self):
        # The exact append-only message text is present in the trigger body,
        # but only as an inert string literal a no-op SELECT ... WHERE 0
        # discards — it never calls RAISE at all. A substring-only check on
        # the raw trigger SQL is fooled by this; a real parse is not.
        self.conn.execute("DROP TRIGGER observations_no_update")
        self.conn.execute(
            """
            CREATE TRIGGER observations_no_update
            BEFORE UPDATE ON observations
            BEGIN
                SELECT 'observations is append-only: UPDATE forbidden' WHERE 0;
            END
            """
        )
        with self.assertRaises(SchemaVerificationError):
            evaluate_schema(self.conn)

    def test_wrong_target_table_trigger_is_detected(self):
        # Same trigger name and the exact correct RAISE(ABORT) message, but
        # bound to a different table than the one it must actually guard.
        self.conn.execute("DROP TRIGGER observations_no_update")
        self.conn.execute(
            """
            CREATE TRIGGER observations_no_update
            BEFORE UPDATE ON duplicate_conflicts
            BEGIN
                SELECT RAISE(ABORT, 'observations is append-only: UPDATE forbidden');
            END
            """
        )
        with self.assertRaises(SchemaVerificationError):
            evaluate_schema(self.conn)

    def test_wrong_event_trigger_is_detected(self):
        # Fires BEFORE INSERT instead of BEFORE UPDATE under a WHEN clause
        # that never evaluates true — never actually blocks the UPDATE this
        # trigger name claims to guard.
        self.conn.execute("DROP TRIGGER observations_no_update")
        self.conn.execute(
            """
            CREATE TRIGGER observations_no_update
            BEFORE INSERT ON observations
            WHEN 0
            BEGIN
                SELECT RAISE(ABORT, 'observations is append-only: UPDATE forbidden');
            END
            """
        )
        with self.assertRaises(SchemaVerificationError):
            evaluate_schema(self.conn)

    def test_unexpected_when_clause_narrowing_enforcement_is_detected(self):
        # This trigger must fire unconditionally before every UPDATE; a WHEN
        # clause that narrows it lets a crafted UPDATE dodge enforcement.
        self.conn.execute("DROP TRIGGER observations_no_update")
        self.conn.execute(
            """
            CREATE TRIGGER observations_no_update
            BEFORE UPDATE ON observations
            WHEN NEW.kind != 'task.created'
            BEGIN
                SELECT RAISE(ABORT, 'observations is append-only: UPDATE forbidden');
            END
            """
        )
        with self.assertRaises(SchemaVerificationError):
            evaluate_schema(self.conn)

    def test_missing_required_when_clause_is_detected(self):
        # observations_no_replace_by_seq must only fire under its documented
        # WHEN guard; dropping the WHEN clause entirely changes what this
        # trigger actually enforces even though timing/event/table/message
        # all still look right.
        self.conn.execute("DROP TRIGGER observations_no_replace_by_seq")
        self.conn.execute(
            """
            CREATE TRIGGER observations_no_replace_by_seq
            BEFORE INSERT ON observations
            BEGIN
                SELECT RAISE(ABORT, 'observations is append-only: INSERT OR REPLACE forbidden (seq)');
            END
            """
        )
        with self.assertRaises(SchemaVerificationError):
            evaluate_schema(self.conn)


class TableStructuralContractTests(unittest.TestCase):
    """Codex re-review: the full table structural contract must validate
    primary key, UNIQUE, and AUTOINCREMENT/rowid identity semantics -- not
    just column names/types/NOT NULL -- so a table rebuilt with identical
    columns but none of its identity constraints is rejected before any
    control-plane write relies on those constraints (e.g. a tail append
    relying on observations' UNIQUE(source, source_id) dedup key)."""

    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.db_path = Path(self._tmp.name) / "control-plane.db"
        self.conn = connect(self.db_path)
        self.addCleanup(self.conn.close)

    def _recreate_required_triggers_for(self, table: str) -> None:
        # DROP TABLE cascades to auto-drop that table's triggers; recreate
        # them verbatim from MIGRATIONS so these tests isolate the
        # structural (PK/UNIQUE/AUTOINCREMENT) contract from the
        # already-covered trigger-fingerprint contract.
        for _version, statements in MIGRATIONS:
            for stmt in statements:
                if "CREATE TRIGGER" in stmt.upper() and f" ON {table}\n" in stmt:
                    self.conn.execute(stmt)

    def test_observations_rebuilt_without_identity_constraints_is_rejected(self):
        self.conn.execute("DROP TABLE observations")
        self.conn.execute(
            """
            CREATE TABLE observations (
                seq             INTEGER,
                observation_id  TEXT NOT NULL,
                source          TEXT NOT NULL,
                source_id       TEXT NOT NULL,
                entity_id       TEXT NOT NULL,
                run_id          TEXT,
                kind            TEXT NOT NULL,
                payload         TEXT NOT NULL,
                occurred_at     TEXT NOT NULL,
                observed_at     TEXT NOT NULL,
                policy_version  TEXT NOT NULL,
                actor           TEXT NOT NULL,
                correlation_id  TEXT NOT NULL,
                evidence_hash   TEXT NOT NULL
            )
            """
        )
        self._recreate_required_triggers_for("observations")
        with self.assertRaises(SchemaVerificationError):
            evaluate_schema(self.conn)

    def test_observations_missing_autoincrement_is_rejected(self):
        self.conn.execute("DROP TABLE observations")
        self.conn.execute(
            """
            CREATE TABLE observations (
                seq             INTEGER PRIMARY KEY,
                observation_id  TEXT NOT NULL UNIQUE,
                source          TEXT NOT NULL,
                source_id       TEXT NOT NULL,
                entity_id       TEXT NOT NULL,
                run_id          TEXT,
                kind            TEXT NOT NULL,
                payload         TEXT NOT NULL,
                occurred_at     TEXT NOT NULL,
                observed_at     TEXT NOT NULL,
                policy_version  TEXT NOT NULL,
                actor           TEXT NOT NULL,
                correlation_id  TEXT NOT NULL,
                evidence_hash   TEXT NOT NULL,
                UNIQUE(source, source_id)
            )
            """
        )
        self._recreate_required_triggers_for("observations")
        with self.assertRaises(SchemaVerificationError):
            evaluate_schema(self.conn)

    def test_observations_missing_dedup_unique_constraint_is_rejected(self):
        # Same PK/AUTOINCREMENT and the observation_id UNIQUE, but the
        # UNIQUE(source, source_id) dedup key itself is dropped -- the exact
        # constraint ledger.py's replay-vs-quarantine logic depends on being
        # DB-enforced, not merely application-checked.
        self.conn.execute("DROP TABLE observations")
        self.conn.execute(
            """
            CREATE TABLE observations (
                seq             INTEGER PRIMARY KEY AUTOINCREMENT,
                observation_id  TEXT NOT NULL UNIQUE,
                source          TEXT NOT NULL,
                source_id       TEXT NOT NULL,
                entity_id       TEXT NOT NULL,
                run_id          TEXT,
                kind            TEXT NOT NULL,
                payload         TEXT NOT NULL,
                occurred_at     TEXT NOT NULL,
                observed_at     TEXT NOT NULL,
                policy_version  TEXT NOT NULL,
                actor           TEXT NOT NULL,
                correlation_id  TEXT NOT NULL,
                evidence_hash   TEXT NOT NULL
            )
            """
        )
        self._recreate_required_triggers_for("observations")
        with self.assertRaises(SchemaVerificationError):
            evaluate_schema(self.conn)

    def test_observation_id_unique_collation_change_is_rejected(self):
        """A UNIQUE column with NOCASE is not the binary UNIQUE contract.

        The old structural fingerprint retained only each unique index's
        column names, so this reconstruction looked identical despite
        changing duplicate semantics for every observation id.
        """
        self.conn.execute("DROP TABLE observations")
        self.conn.execute(
            """
            CREATE TABLE observations (
                seq             INTEGER PRIMARY KEY AUTOINCREMENT,
                observation_id  TEXT NOT NULL COLLATE NOCASE UNIQUE,
                source          TEXT NOT NULL,
                source_id       TEXT NOT NULL,
                entity_id       TEXT NOT NULL,
                run_id          TEXT,
                kind            TEXT NOT NULL,
                payload         TEXT NOT NULL,
                occurred_at     TEXT NOT NULL,
                observed_at     TEXT NOT NULL,
                policy_version  TEXT NOT NULL,
                actor           TEXT NOT NULL,
                correlation_id  TEXT NOT NULL,
                evidence_hash   TEXT NOT NULL,
                UNIQUE(source, source_id)
            )
            """
        )
        self._recreate_required_triggers_for("observations")
        with self.assertRaises(SchemaVerificationError):
            evaluate_schema(self.conn)

    def test_source_cursors_missing_primary_key_is_rejected(self):
        # Column-level contract (type/NOT NULL) is preserved exactly --
        # 'source' stays a nullable TEXT column, matching the quirky-but-
        # documented PRAGMA table_info behavior for a real PRIMARY KEY
        # column -- so only the missing PRIMARY KEY constraint itself can
        # trip this test, isolating it from the pre-existing column
        # contract check.
        self.conn.execute("DROP TABLE source_cursors")
        self.conn.execute(
            "CREATE TABLE source_cursors (source TEXT, cursor_value TEXT NOT NULL, "
            "consumed_digest TEXT NOT NULL, updated_at TEXT NOT NULL)"
        )
        with self.assertRaises(SchemaVerificationError):
            evaluate_schema(self.conn)

    def test_capability_inventory_check_and_default_semantics_change_is_rejected(self):
        # Every column, PK, AUTOINCREMENT setting, and UNIQUE constraint is
        # preserved.  Only the status CHECK/default authorization semantics
        # have been weakened -- a partial structural fingerprint misses this
        # rebuild entirely.
        self.conn.execute("DROP TABLE capability_inventory")
        self.conn.execute(
            """
            CREATE TABLE capability_inventory (
                id                INTEGER PRIMARY KEY AUTOINCREMENT,
                capability_id     TEXT NOT NULL UNIQUE,
                cli               TEXT NOT NULL,
                model             TEXT NOT NULL,
                effort            TEXT,
                role              TEXT,
                status            TEXT NOT NULL CHECK(status IN ('inventory', 'unverified', 'approved')) DEFAULT 'approved',
                probe_evidence    TEXT,
                recorded_at       TEXT NOT NULL,
                updated_at        TEXT NOT NULL
            )
            """
        )
        with self.assertRaises(SchemaVerificationError):
            evaluate_schema(self.conn)

    def test_healthy_db_structural_fingerprints_cover_every_required_table(self):
        result = verify_schema(self.conn)
        fingerprint_tables = {entry[0] for entry in result.table_fingerprints}
        self.assertEqual(fingerprint_tables, set(REQUIRED_TABLE_COLUMNS))

    def test_healthy_fresh_db_still_verifies_clean(self):
        result = verify_schema(self.conn)
        self.assertEqual(result.integrity_check, "ok")


class UnexpectedTriggerRejectionTests(unittest.TestCase):
    """Control-plane attestation must reject, not silently omit, rogue triggers."""

    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.conn = connect(Path(self._tmp.name) / "control-plane.db")
        self.addCleanup(self.conn.close)

    def test_unexpected_write_capable_trigger_is_rejected(self):
        self.conn.execute(
            """
            CREATE TRIGGER rogue_checkpoint_cursor_reset
            AFTER INSERT ON recovery_checkpoints
            BEGIN
                UPDATE source_cursors SET cursor_value = '0';
            END
            """
        )
        with self.assertRaises(SchemaVerificationError):
            evaluate_schema(self.conn)

    def test_unexpected_temp_trigger_on_control_plane_table_is_rejected(self):
        self.conn.execute(
            """
            CREATE TEMP TRIGGER temp_rogue_checkpoint_cursor_reset
            AFTER INSERT ON recovery_checkpoints
            BEGIN
                UPDATE source_cursors SET cursor_value = '0';
            END
            """
        )
        with self.assertRaises(SchemaVerificationError):
            verify_schema(self.conn)


if __name__ == "__main__":
    unittest.main()
