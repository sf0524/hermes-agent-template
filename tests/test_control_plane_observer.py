import sqlite3
import tempfile
import unittest
from pathlib import Path

from orchestrator.control_plane.db import connect
from orchestrator.control_plane.kanban_tail import KanbanTailAdapter, SourceContractError
from orchestrator.control_plane.ledger import ObservationLedger
from orchestrator.control_plane.observer import (
    CheckpointVerificationError,
    ShadowModeRequiredError,
    ShadowObserver,
    checkpoint_integrity_hash,
)
from orchestrator.control_plane.schema import SchemaVerificationError


def _make_source_db(path: Path, rows: list[tuple]) -> None:
    conn = sqlite3.connect(str(path))
    try:
        conn.execute(
            """
            CREATE TABLE task_events (
                id INTEGER PRIMARY KEY,
                task_id TEXT NOT NULL,
                run_id TEXT,
                kind TEXT NOT NULL,
                payload TEXT NOT NULL,
                created_at TEXT NOT NULL
            )
            """
        )
        conn.executemany(
            "INSERT INTO task_events (id, task_id, run_id, kind, payload, created_at) VALUES (?, ?, ?, ?, ?, ?)",
            rows,
        )
        conn.commit()
    finally:
        conn.close()


def _append_source_rows(path: Path, rows: list[tuple]) -> None:
    conn = sqlite3.connect(str(path))
    try:
        conn.executemany(
            "INSERT INTO task_events (id, task_id, run_id, kind, payload, created_at) VALUES (?, ?, ?, ?, ?, ?)",
            rows,
        )
        conn.commit()
    finally:
        conn.close()


def _control_table_rows(conn: sqlite3.Connection) -> dict[str, tuple[tuple, ...]]:
    """Capture every durable control-plane table to prove a rejected run wrote nothing."""
    tables = (
        "schema_migrations",
        "observations",
        "source_cursors",
        "duplicate_conflicts",
        "capability_inventory",
        "recovery_checkpoints",
        "source_identity",
    )
    return {
        table: tuple(tuple(row) for row in conn.execute(f"SELECT * FROM {table} ORDER BY rowid"))
        for table in tables
    }


class ShadowModeRejectionTests(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.source_db_path = Path(self._tmp.name) / "kanban.db"
        _make_source_db(self.source_db_path, rows=[])
        self.control_conn = connect(Path(self._tmp.name) / "control-plane.db")
        self.addCleanup(self.control_conn.close)

    def test_shadow_mode_is_accepted(self):
        observer = ShadowObserver(self.source_db_path, self.control_conn, mode="shadow")
        self.assertEqual(observer.mode, "shadow")

    def test_active_mode_is_rejected(self):
        with self.assertRaises(ShadowModeRequiredError):
            ShadowObserver(self.source_db_path, self.control_conn, mode="active")

    def test_dispatch_mode_is_rejected(self):
        with self.assertRaises(ShadowModeRequiredError):
            ShadowObserver(self.source_db_path, self.control_conn, mode="dispatch")

    def test_empty_mode_is_rejected(self):
        with self.assertRaises(ShadowModeRequiredError):
            ShadowObserver(self.source_db_path, self.control_conn, mode="")


class RestartRecoveryTests(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.source_db_path = Path(self._tmp.name) / "kanban.db"
        self.control_db_path = Path(self._tmp.name) / "control-plane.db"
        _make_source_db(
            self.source_db_path,
            rows=[
                (1, "task-1", None, "task.created", "{}", "2026-01-01T00:00:00+00:00"),
                (2, "task-1", None, "task.status_changed", "{}", "2026-01-01T00:01:00+00:00"),
            ],
        )

    def test_run_once_writes_a_recovery_checkpoint(self):
        conn = connect(self.control_db_path)
        self.addCleanup(conn.close)
        observer = ShadowObserver(self.source_db_path, conn, mode="shadow")
        result = observer.run_once(policy_version="v1", actor="observer", correlation_id="corr-1")
        self.assertEqual(result.checkpoint.cursor_value, "2")
        self.assertEqual(result.checkpoint.observation_count, 2)
        self.assertTrue(result.checkpoint.integrity_hash)
        self.assertEqual(observer.latest_checkpoint().id, result.checkpoint.id)

    def test_restart_resumes_from_durable_cursor_without_reprocessing(self):
        conn = connect(self.control_db_path)
        observer = ShadowObserver(self.source_db_path, conn, mode="shadow")
        observer.run_once(policy_version="v1", actor="observer", correlation_id="corr-1")
        self.assertEqual(ObservationLedger(conn).count(), 2)
        conn.close()  # simulate process kill

        # New process, new connection, new ShadowObserver instance — same DB files.
        restarted_conn = connect(self.control_db_path)
        self.addCleanup(restarted_conn.close)
        restarted_observer = ShadowObserver(self.source_db_path, restarted_conn, mode="shadow")
        self.assertEqual(restarted_observer.current_cursor(), 2)

        result = restarted_observer.run_once(policy_version="v1", actor="observer", correlation_id="corr-2")
        self.assertEqual(result.tail_result.rows_read, 0)  # nothing new yet, no reprocessing
        self.assertEqual(ObservationLedger(restarted_conn).count(), 2)

    def test_dropped_notification_equivalent_is_recovered_by_tailing(self):
        # "Dropped notification" is simulated by never having any push in the
        # first place: the observer only ever learns of new rows by tailing
        # past its durable cursor, so a row that arrived while the process
        # was down is still picked up on the next run_once with no special case.
        conn = connect(self.control_db_path)
        observer = ShadowObserver(self.source_db_path, conn, mode="shadow")
        observer.run_once(policy_version="v1", actor="observer", correlation_id="corr-1")
        conn.close()  # process "down" while a new event arrives

        _append_source_rows(
            self.source_db_path,
            rows=[(3, "task-2", None, "task.created", "{}", "2026-01-01T00:02:00+00:00")],
        )

        restarted_conn = connect(self.control_db_path)
        self.addCleanup(restarted_conn.close)
        restarted_observer = ShadowObserver(self.source_db_path, restarted_conn, mode="shadow")
        result = restarted_observer.run_once(policy_version="v1", actor="observer", correlation_id="corr-2")
        self.assertEqual(result.tail_result.rows_read, 1)
        self.assertEqual(result.tail_result.appended, 1)
        self.assertEqual(ObservationLedger(restarted_conn).count(), 3)

    def test_run_once_has_zero_side_effects_outside_control_plane_db(self):
        conn = connect(self.control_db_path)
        self.addCleanup(conn.close)
        observer = ShadowObserver(self.source_db_path, conn, mode="shadow")
        before = self.source_db_path.read_bytes()
        observer.run_once(policy_version="v1", actor="observer", correlation_id="corr-1")
        after = self.source_db_path.read_bytes()
        self.assertEqual(before, after)


class ControlSchemaValidatedBeforeTailingTests(unittest.TestCase):
    """Codex re-review Finding B: control schema validation must happen
    before run_once invokes tailing — a broken control-plane schema must be
    rejected before any observations/cursor/checkpoint write, not caught
    only after the tail cycle already wrote through a compromised schema."""

    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.source_db_path = Path(self._tmp.name) / "kanban.db"
        _make_source_db(
            self.source_db_path,
            rows=[(1, "task-1", None, "task.created", "{}", "2026-01-01T00:00:00+00:00")],
        )
        self.control_db_path = Path(self._tmp.name) / "control-plane.db"
        self.control_conn = connect(self.control_db_path)
        self.addCleanup(self.control_conn.close)

    def test_broken_schema_is_rejected_before_any_tailing_write(self):
        # Spoof a same-name no-op trigger (the same "SELECT-spoof" shape
        # covered in test_control_plane_schema_db.py) before run_once ever
        # gets a chance to tail.
        self.control_conn.execute("DROP TRIGGER observations_no_update")
        self.control_conn.execute(
            """
            CREATE TRIGGER observations_no_update
            BEFORE UPDATE ON observations
            BEGIN
                SELECT 1;
            END
            """
        )
        observer = ShadowObserver(self.source_db_path, self.control_conn, mode="shadow")
        with self.assertRaises(SchemaVerificationError):
            observer.run_once(policy_version="v1", actor="observer", correlation_id="corr-1")

        self.assertEqual(ObservationLedger(self.control_conn).count(), 0)
        cursor_count = self.control_conn.execute("SELECT COUNT(*) FROM source_cursors").fetchone()[0]
        self.assertEqual(cursor_count, 0)
        checkpoint_count = self.control_conn.execute("SELECT COUNT(*) FROM recovery_checkpoints").fetchone()[0]
        self.assertEqual(checkpoint_count, 0)
        identity_count = self.control_conn.execute("SELECT COUNT(*) FROM source_identity").fetchone()[0]
        self.assertEqual(identity_count, 0)


class SourceContractRowValidationBeforeAnyWriteTests(unittest.TestCase):
    """Codex re-review: a source row that fails contract validation (e.g. a
    BLOB stored in a declared TEXT NOT NULL column -- SQLite's TEXT
    affinity only converts INTEGER/REAL literals to text, a BLOB is stored
    as-is) must be rejected before ShadowObserver.run_once performs ANY
    control-plane write -- including the one-time append-only
    source_identity insert, which previously happened before the fetched
    batch's rows were validated against the row-level contract."""

    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.source_db_path = Path(self._tmp.name) / "kanban.db"
        self.control_db_path = Path(self._tmp.name) / "control-plane.db"
        self.control_conn = connect(self.control_db_path)
        self.addCleanup(self.control_conn.close)

        conn = sqlite3.connect(str(self.source_db_path))
        try:
            conn.execute(
                "CREATE TABLE task_events (id INTEGER PRIMARY KEY, task_id TEXT NOT NULL, run_id TEXT, "
                "kind TEXT NOT NULL, payload TEXT NOT NULL, created_at TEXT NOT NULL)"
            )
            conn.execute(
                "INSERT INTO task_events (id, task_id, run_id, kind, payload, created_at) "
                "VALUES (1, 't-1', NULL, 'task.created', X'0011', 'now')"
            )
            conn.commit()
        finally:
            conn.close()

    def test_invalid_payload_row_rejected_before_any_control_plane_write(self):
        observer = ShadowObserver(self.source_db_path, self.control_conn, mode="shadow")
        with self.assertRaises(SourceContractError):
            observer.run_once(policy_version="v1", actor="observer", correlation_id="corr-1")

        self.assertEqual(ObservationLedger(self.control_conn).count(), 0)
        for table in ("source_identity", "source_cursors", "recovery_checkpoints"):
            count = self.control_conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            self.assertEqual(count, 0, f"{table} must remain empty on a pre-write contract rejection")

    def test_malformed_row_beyond_current_batch_is_rejected_before_any_control_plane_write(self):
        """Preflight must stream the whole source, not only the first batch.

        If row 2 is malformed and batch_size is one, accepting row 1 would
        already write an identity, observation, cursor, and checkpoint before
        row 2 was ever inspected.  That is not an atomic source validation.
        """
        self.source_db_path.unlink()
        conn = sqlite3.connect(str(self.source_db_path))
        try:
            conn.execute(
                "CREATE TABLE task_events (id INTEGER PRIMARY KEY, task_id TEXT NOT NULL, run_id TEXT, "
                "kind TEXT NOT NULL, payload TEXT NOT NULL, created_at TEXT NOT NULL)"
            )
            conn.execute(
                "INSERT INTO task_events (id, task_id, run_id, kind, payload, created_at) "
                "VALUES (1, 't-1', NULL, 'task.created', '{}', 'now')"
            )
            conn.execute(
                "INSERT INTO task_events (id, task_id, run_id, kind, payload, created_at) "
                "VALUES (2, 't-2', NULL, 'task.created', X'0011', 'now')"
            )
            conn.commit()
        finally:
            conn.close()

        observer = ShadowObserver(self.source_db_path, self.control_conn, mode="shadow", batch_size=1)
        with self.assertRaises(SourceContractError):
            observer.run_once(policy_version="v1", actor="observer", correlation_id="corr-1")

        self.assertEqual(ObservationLedger(self.control_conn).count(), 0)
        for table in ("source_identity", "source_cursors", "recovery_checkpoints"):
            count = self.control_conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            self.assertEqual(count, 0, f"{table} must remain empty on a full-source preflight rejection")


class CheckpointIntegrityHashFieldCoverageTests(unittest.TestCase):
    """Codex re-review Finding A: the canonical checkpoint hash must cover
    every persisted attested checkpoint field, including id and
    checkpoint_at (both silently excluded by the previous hash)."""

    def _base_kwargs(self):
        return dict(
            id=1,
            checkpoint_at="2026-01-01T00:00:00+00:00",
            source="kanban",
            cursor_value="5",
            observation_count=5,
            schema_version=1,
            applied_migrations=(1,),
            trigger_fingerprints=(
                ("observations_no_update", "observations", "BEFORE", "UPDATE", None, "SELECT RAISE(ABORT, 'x');"),
            ),
            table_fingerprints=(
                (
                    "observations",
                    ("seq",),
                    True,
                    (
                        ("u", (("observation_id", "BINARY", "ASC"),), False, None),
                        (
                            "u",
                            (("source", "BINARY", "ASC"), ("source_id", "BINARY", "ASC")),
                            False,
                            None,
                        ),
                    ),
                ),
            ),
            table_contract_fingerprints=(
                (
                    "observations",
                    "CREATE TABLE observations(seq INTEGER PRIMARY KEY AUTOINCREMENT,observation_id TEXT NOT NULL UNIQUE)",
                ),
            ),
            integrity_check="ok",
        )

    def test_identical_inputs_produce_identical_hash(self):
        self.assertEqual(
            checkpoint_integrity_hash(**self._base_kwargs()),
            checkpoint_integrity_hash(**self._base_kwargs()),
        )

    def test_changing_id_changes_hash(self):
        base = checkpoint_integrity_hash(**self._base_kwargs())
        kwargs = self._base_kwargs()
        kwargs["id"] = 2
        self.assertNotEqual(base, checkpoint_integrity_hash(**kwargs))

    def test_changing_checkpoint_at_changes_hash(self):
        base = checkpoint_integrity_hash(**self._base_kwargs())
        kwargs = self._base_kwargs()
        kwargs["checkpoint_at"] = "2026-01-01T00:00:01+00:00"
        self.assertNotEqual(base, checkpoint_integrity_hash(**kwargs))

    def test_changing_source_changes_hash(self):
        base = checkpoint_integrity_hash(**self._base_kwargs())
        kwargs = self._base_kwargs()
        kwargs["source"] = "other-source"
        self.assertNotEqual(base, checkpoint_integrity_hash(**kwargs))

    def test_changing_cursor_value_changes_hash(self):
        base = checkpoint_integrity_hash(**self._base_kwargs())
        kwargs = self._base_kwargs()
        kwargs["cursor_value"] = "6"
        self.assertNotEqual(base, checkpoint_integrity_hash(**kwargs))

    def test_changing_observation_count_changes_hash(self):
        base = checkpoint_integrity_hash(**self._base_kwargs())
        kwargs = self._base_kwargs()
        kwargs["observation_count"] = 6
        self.assertNotEqual(base, checkpoint_integrity_hash(**kwargs))

    def test_changing_schema_version_changes_hash(self):
        base = checkpoint_integrity_hash(**self._base_kwargs())
        kwargs = self._base_kwargs()
        kwargs["schema_version"] = 2
        self.assertNotEqual(base, checkpoint_integrity_hash(**kwargs))

    def test_changing_applied_migrations_changes_hash(self):
        base = checkpoint_integrity_hash(**self._base_kwargs())
        kwargs = self._base_kwargs()
        kwargs["applied_migrations"] = (1, 2)
        self.assertNotEqual(base, checkpoint_integrity_hash(**kwargs))

    def test_changing_trigger_fingerprints_changes_hash(self):
        base = checkpoint_integrity_hash(**self._base_kwargs())
        kwargs = self._base_kwargs()
        kwargs["trigger_fingerprints"] = (
            ("observations_no_update", "observations", "BEFORE", "UPDATE", None, "SELECT RAISE(ABORT, 'y');"),
        )
        self.assertNotEqual(base, checkpoint_integrity_hash(**kwargs))

    def test_changing_integrity_check_changes_hash(self):
        base = checkpoint_integrity_hash(**self._base_kwargs())
        kwargs = self._base_kwargs()
        kwargs["integrity_check"] = "not ok"
        self.assertNotEqual(base, checkpoint_integrity_hash(**kwargs))

    def test_changing_table_fingerprints_changes_hash(self):
        # Codex re-review: a table rebuilt with identical columns but no
        # PRIMARY KEY/UNIQUE/AUTOINCREMENT constraints must change the
        # checkpoint hash too, not just live verify_schema() output.
        base = checkpoint_integrity_hash(**self._base_kwargs())
        kwargs = self._base_kwargs()
        kwargs["table_fingerprints"] = (("observations", (), False, ()),)
        self.assertNotEqual(base, checkpoint_integrity_hash(**kwargs))

    def test_changing_unique_index_collation_changes_hash(self):
        base = checkpoint_integrity_hash(**self._base_kwargs())
        kwargs = self._base_kwargs()
        kwargs["table_fingerprints"] = (
            (
                "observations",
                ("seq",),
                True,
                (
                    ("u", (("observation_id", "NOCASE", "ASC"),), False, None),
                    (
                        "u",
                        (("source", "BINARY", "ASC"), ("source_id", "BINARY", "ASC")),
                        False,
                        None,
                    ),
                ),
            ),
        )
        self.assertNotEqual(base, checkpoint_integrity_hash(**kwargs))

    def test_changing_exact_table_contract_changes_hash(self):
        # The checkpoint must attest to the complete canonical CREATE TABLE
        # contract too: these two definitions keep the identity indexes but
        # differ in the status CHECK/default authorization semantics.
        base_kwargs = self._base_kwargs()
        base_kwargs["table_contract_fingerprints"] = (
            (
                "capability_inventory",
                "CREATE TABLE capability_inventory(status TEXT NOT NULL CHECK(status IN ('inventory','unverified')) DEFAULT 'inventory')",
            ),
        )
        base = checkpoint_integrity_hash(**base_kwargs)
        changed_kwargs = dict(base_kwargs)
        changed_kwargs["table_contract_fingerprints"] = (
            (
                "capability_inventory",
                "CREATE TABLE capability_inventory(status TEXT NOT NULL CHECK(status IN ('inventory','unverified','approved')) DEFAULT 'approved')",
            ),
        )
        self.assertNotEqual(base, checkpoint_integrity_hash(**changed_kwargs))


class CheckpointIntegrityVerificationTests(unittest.TestCase):
    """Codex re-review Finding A: verify_recovery_state must recompute the
    checkpoint hash and compare with hmac.compare_digest, raising a real
    CheckpointVerificationError on mismatch."""

    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.source_db_path = Path(self._tmp.name) / "kanban.db"
        _make_source_db(
            self.source_db_path,
            rows=[(1, "task-1", None, "task.created", "{}", "2026-01-01T00:00:00+00:00")],
        )
        self.control_db_path = Path(self._tmp.name) / "control-plane.db"
        self.control_conn = connect(self.control_db_path)
        self.addCleanup(self.control_conn.close)

    def test_verify_recovery_state_passes_for_untampered_checkpoint(self):
        observer = ShadowObserver(self.source_db_path, self.control_conn, mode="shadow")
        observer.run_once(policy_version="v1", actor="observer", correlation_id="corr-1")
        observer.verify_recovery_state()  # must not raise

    def test_verify_recovery_state_detects_forged_integrity_hash(self):
        observer = ShadowObserver(self.source_db_path, self.control_conn, mode="shadow")
        result = observer.run_once(policy_version="v1", actor="observer", correlation_id="corr-1")

        # recovery_checkpoints is append-only (UPDATE/DELETE/REPLACE are all
        # rejected by trigger), so simulate a forged/corrupted checkpoint the
        # way a compromised writer or storage corruption would: insert a
        # *new* row whose integrity_hash does not match what
        # checkpoint_integrity_hash() would compute for its own fields.
        self.control_conn.execute(
            """
            INSERT INTO recovery_checkpoints
                (id, checkpoint_at, source, cursor_value, observation_count, schema_version, integrity_hash)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (result.checkpoint.id + 1, "2026-01-01T00:00:00+00:00", "kanban", "1", 1, 1, "0" * 64),
        )
        with self.assertRaises(CheckpointVerificationError):
            observer.verify_recovery_state()

    def test_run_once_rejects_a_forged_latest_checkpoint_before_any_control_plane_write(self):
        """A new checkpoint must not hide the forged one that preceded it."""
        observer = ShadowObserver(self.source_db_path, self.control_conn, mode="shadow")
        result = observer.run_once(policy_version="v1", actor="observer", correlation_id="corr-1")
        _append_source_rows(
            self.source_db_path,
            rows=[(2, "task-2", None, "task.created", "{}", "2026-01-01T00:01:00+00:00")],
        )
        self.control_conn.execute(
            """
            INSERT INTO recovery_checkpoints
                (id, checkpoint_at, source, cursor_value, observation_count, schema_version, integrity_hash)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (result.checkpoint.id + 1, "2026-01-01T00:00:30+00:00", "kanban", "1", 1, 1, "0" * 64),
        )
        before = _control_table_rows(self.control_conn)

        with self.assertRaises(CheckpointVerificationError):
            observer.run_once(policy_version="v1", actor="observer", correlation_id="corr-2")

        self.assertEqual(_control_table_rows(self.control_conn), before)

    def test_run_once_rejects_unattested_prior_source_state_before_any_control_plane_write(self):
        """State that has no checkpoint is not a fresh source and cannot be resumed."""
        from orchestrator.control_plane.kanban_tail import _seed_consumed_digest

        self.control_conn.execute(
            """
            INSERT INTO source_cursors (source, cursor_value, consumed_digest, updated_at)
            VALUES ('kanban', '0', ?, 'now')
            """,
            (_seed_consumed_digest("kanban"),),
        )
        observer = ShadowObserver(self.source_db_path, self.control_conn, mode="shadow")
        before = _control_table_rows(self.control_conn)

        with self.assertRaises(CheckpointVerificationError):
            observer.run_once(policy_version="v1", actor="observer", correlation_id="corr-1")

        self.assertEqual(_control_table_rows(self.control_conn), before)

    def test_verify_recovery_state_is_read_only_even_when_it_raises(self):
        observer = ShadowObserver(self.source_db_path, self.control_conn, mode="shadow")
        observer.run_once(policy_version="v1", actor="observer", correlation_id="corr-1")
        before = ObservationLedger(self.control_conn).count()
        self.control_conn.execute(
            """
            INSERT INTO recovery_checkpoints
                (id, checkpoint_at, source, cursor_value, observation_count, schema_version, integrity_hash)
            VALUES (999, '2026-01-01T00:00:00+00:00', 'kanban', '1', 1, 1, ?)
            """,
            ("f" * 64,),
        )
        with self.assertRaises(CheckpointVerificationError):
            observer.verify_recovery_state()
        self.assertEqual(ObservationLedger(self.control_conn).count(), before)

    def test_checkpoint_source_locator_tamper_is_not_treated_as_no_checkpoint(self):
        """A tailed source needs a checkpoint under the same source key.

        Simulate privileged/storage tamper by temporarily bypassing the
        append-only UPDATE trigger, then restore its exact protection before
        verification.  A lookup by the original locator must fail closed;
        it must not mistake the relabeled checkpoint for a fresh source.
        """
        observer = ShadowObserver(self.source_db_path, self.control_conn, mode="shadow")
        observer.run_once(policy_version="v1", actor="observer", correlation_id="corr-1")
        before_observations = ObservationLedger(self.control_conn).count()
        before_cursor = observer.current_cursor()

        self.control_conn.execute("DROP TRIGGER recovery_checkpoints_no_update")
        try:
            self.control_conn.execute(
                "UPDATE recovery_checkpoints SET source = 'tampered-source-locator' WHERE source = 'kanban'"
            )
        finally:
            self.control_conn.execute(
                """
                CREATE TRIGGER recovery_checkpoints_no_update
                BEFORE UPDATE ON recovery_checkpoints
                BEGIN
                    SELECT RAISE(ABORT, 'recovery_checkpoints is append-only: UPDATE forbidden');
                END
                """
            )

        with self.assertRaises(CheckpointVerificationError):
            observer.verify_recovery_state()
        self.assertEqual(ObservationLedger(self.control_conn).count(), before_observations)
        self.assertEqual(observer.current_cursor(), before_cursor)

    def test_missing_checkpoint_rejects_each_kind_of_source_owned_state(self):
        """No source-owned durable state may be relabelled as a fresh source."""
        inserts = {
            "source_identity": """
                INSERT INTO source_identity (source, canonical_path, device, inode, recorded_at)
                VALUES ('kanban', '/trusted/kanban.db', 1, 2, 'now')
            """,
            "source_cursors": """
                INSERT INTO source_cursors (source, cursor_value, consumed_digest, updated_at)
                VALUES ('kanban', '1', 'digest', 'now')
            """,
            "observations": """
                INSERT INTO observations (
                    observation_id, source, source_id, entity_id, run_id, kind, payload,
                    occurred_at, observed_at, policy_version, actor, correlation_id, evidence_hash
                ) VALUES ('obs-1', 'kanban', '1', 'task-1', NULL, 'task.created', '{}',
                    'now', 'now', 'v1', 'observer', 'corr', 'hash')
            """,
            "duplicate_conflicts": """
                INSERT INTO duplicate_conflicts (
                    source, source_id, existing_observation_id, existing_evidence_hash,
                    conflicting_entity_id, conflicting_run_id, conflicting_kind,
                    conflicting_payload, conflicting_occurred_at, conflicting_evidence_hash, detected_at
                ) VALUES ('kanban', '1', 'obs-1', 'existing-hash', 'task-1', NULL, 'task.created',
                    '{}', 'now', 'conflict-hash', 'now')
            """,
        }
        for table, statement in inserts.items():
            with self.subTest(table=table):
                isolated_conn = connect(Path(self._tmp.name) / f"{table}-control-plane.db")
                self.addCleanup(isolated_conn.close)
                observer = ShadowObserver(self.source_db_path, isolated_conn, mode="shadow")
                isolated_conn.execute(statement)
                with self.assertRaises(CheckpointVerificationError):
                    observer.verify_recovery_state()


class UnexpectedTriggerObserverRejectionTests(unittest.TestCase):
    """A trigger outside the approved control-plane schema stops all paths."""

    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.source_db_path = Path(self._tmp.name) / "kanban.db"
        _make_source_db(
            self.source_db_path,
            rows=[(1, "task-1", None, "task.created", "{}", "2026-01-01T00:00:00+00:00")],
        )
        self.control_conn = connect(Path(self._tmp.name) / "control-plane.db")
        self.addCleanup(self.control_conn.close)
        self.control_conn.execute(
            """
            CREATE TRIGGER rogue_checkpoint_cursor_reset
            AFTER INSERT ON recovery_checkpoints
            BEGIN
                UPDATE source_cursors SET cursor_value = '0';
            END
            """
        )

    def test_rogue_checkpoint_trigger_prevents_run_before_tailing(self):
        observer = ShadowObserver(self.source_db_path, self.control_conn, mode="shadow")
        with self.assertRaises(SchemaVerificationError):
            observer.run_once(policy_version="v1", actor="observer", correlation_id="corr")
        self.assertEqual(ObservationLedger(self.control_conn).count(), 0)
        self.assertIsNone(
            self.control_conn.execute(
                "SELECT cursor_value FROM source_cursors WHERE source = 'kanban'"
            ).fetchone()
        )

    def test_rogue_checkpoint_trigger_prevents_recovery_verification(self):
        observer = ShadowObserver(self.source_db_path, self.control_conn, mode="shadow")
        with self.assertRaises(SchemaVerificationError):
            observer.verify_recovery_state()


class TemporaryTriggerObserverRejectionTests(unittest.TestCase):
    """Connection-local TEMP triggers are control-plane side effects too."""

    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.source_db_path = Path(self._tmp.name) / "kanban.db"
        _make_source_db(
            self.source_db_path,
            rows=[(1, "task-1", None, "task.created", "{}", "2026-01-01T00:00:00+00:00")],
        )
        self.control_conn = connect(Path(self._tmp.name) / "control-plane.db")
        self.addCleanup(self.control_conn.close)
        self.control_conn.execute(
            """
            CREATE TEMP TRIGGER temp_rogue_checkpoint_cursor_reset
            AFTER INSERT ON recovery_checkpoints
            BEGIN
                UPDATE source_cursors SET cursor_value = '0';
            END
            """
        )

    def test_temp_checkpoint_trigger_blocks_recovery_verification_and_run_without_persistence(self):
        observer = ShadowObserver(self.source_db_path, self.control_conn, mode="shadow")
        before = _control_table_rows(self.control_conn)

        with self.assertRaises(SchemaVerificationError):
            observer.verify_recovery_state()
        self.assertEqual(_control_table_rows(self.control_conn), before)

        with self.assertRaises(SchemaVerificationError):
            observer.run_once(policy_version="v1", actor="observer", correlation_id="corr")
        self.assertEqual(_control_table_rows(self.control_conn), before)


if __name__ == "__main__":
    unittest.main()
