import hashlib
import os
import sqlite3
import stat
import tempfile
import threading
import unittest
from pathlib import Path
from unittest import mock

from orchestrator.control_plane.db import connect
from orchestrator.control_plane.kanban_tail import (
    InvalidBatchSizeError,
    KanbanTailAdapter,
    RelativeSourceDbPathError,
    SourceContractError,
    SourceIdentityMismatchError,
    SourcePathSecurityError,
)
from orchestrator.control_plane.ledger import ObservationLedger


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
        # An unrelated table, to prove the tail adapter never touches
        # anything outside task_events's exact verified columns.
        conn.execute("CREATE TABLE secrets (token TEXT NOT NULL)")
        conn.execute("INSERT INTO secrets (token) VALUES ('do-not-touch')")
        conn.executemany(
            "INSERT INTO task_events (id, task_id, run_id, kind, payload, created_at) VALUES (?, ?, ?, ?, ?, ?)",
            rows,
        )
        conn.commit()
    finally:
        conn.close()


def _file_hash(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


class KanbanTailAdapterTests(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.source_db_path = Path(self._tmp.name) / "kanban.db"
        self.control_db_path = Path(self._tmp.name) / "control-plane.db"
        _make_source_db(
            self.source_db_path,
            rows=[
                (1, "task-1", None, "task.created", '{"a": 1}', "2026-01-01T00:00:00+00:00"),
                (2, "task-1", "run-1", "task.status_changed", '{"a": 2}', "2026-01-01T00:01:00+00:00"),
                (3, "task-2", None, "task.created", '{"a": 3}', "2026-01-01T00:02:00+00:00"),
            ],
        )
        self.control_conn = connect(self.control_db_path)
        self.addCleanup(self.control_conn.close)

    def test_rejects_relative_source_db_path(self):
        with self.assertRaises(RelativeSourceDbPathError):
            KanbanTailAdapter("relative/kanban.db", self.control_conn)

    def test_group_writable_source_directory_is_rejected_before_sqlite_opens_it(self):
        unsafe_dir = Path(self._tmp.name) / "unsafe-source"
        unsafe_dir.mkdir()
        unsafe_path = unsafe_dir / "kanban.db"
        _make_source_db(unsafe_path, rows=[])
        unsafe_dir.chmod(0o770)
        self.addCleanup(unsafe_dir.chmod, 0o700)
        adapter = KanbanTailAdapter(unsafe_path, self.control_conn)
        with self.assertRaises(SourcePathSecurityError):
            adapter.tail_once(policy_version="v1", actor="observer", correlation_id="corr-1")

    def test_symlinked_source_file_is_rejected_before_sqlite_opens_it(self):
        target_path = Path(self._tmp.name) / "actual-kanban.db"
        _make_source_db(target_path, rows=[])
        symlink_path = Path(self._tmp.name) / "kanban-link.db"
        symlink_path.symlink_to(target_path)
        adapter = KanbanTailAdapter(symlink_path, self.control_conn)
        with self.assertRaises(SourcePathSecurityError):
            adapter.tail_once(policy_version="v1", actor="observer", correlation_id="corr-1")

    def test_symlinked_wal_sidecar_is_rejected_before_sqlite_opens_the_source(self):
        """The normal SQLite URI must not follow an attacker-controlled -wal."""
        target_path = Path(self._tmp.name) / "attacker-wal"
        target_path.write_bytes(b"not a sqlite WAL")
        wal_path = Path(f"{self.source_db_path}-wal")
        wal_path.symlink_to(target_path)
        adapter = KanbanTailAdapter(self.source_db_path, self.control_conn)
        with self.assertRaises(SourcePathSecurityError):
            adapter.tail_once(policy_version="v1", actor="observer", correlation_id="corr-1")

    def test_rejects_untrusted_0755_ancestor_beneath_sticky_tmp_before_open_or_control_writes(self):
        """A non-writable-looking directory can still be renamed by its owner.

        This is the pathname-swap case the old group/world-writable-only
        check missed: an attacker-owned 0755 directory underneath the normal
        root-owned sticky /tmp can replace its root-owned 0644 child.  The
        source must be refused before SQLite is asked to open it and before
        any control-plane evidence is persisted.
        """
        if os.geteuid() != 0:
            self.skipTest("this concrete UID-65534 ownership regression requires chown privilege")
        tmp_root = Path(tempfile.gettempdir())
        tmp_mode = tmp_root.stat().st_mode
        if not (tmp_mode & stat.S_ISVTX) or tmp_root.stat().st_uid != 0:
            self.skipTest("requires the conventional root-owned sticky /tmp ancestor")

        untrusted_ancestor = Path(self._tmp.name) / "uid-65534-owned-0755"
        untrusted_ancestor.mkdir(mode=0o755)
        os.chown(untrusted_ancestor, 65534, 65534)
        source_path = untrusted_ancestor / "kanban.db"
        _make_source_db(source_path, rows=[(1, "task-1", None, "task.created", "{}", "now")])
        source_path.chmod(0o644)
        self.assertEqual(source_path.stat().st_uid, 0, "regression requires a root-owned source file")
        self.assertEqual(untrusted_ancestor.stat().st_uid, 65534)
        self.assertEqual(stat.S_IMODE(untrusted_ancestor.stat().st_mode), 0o755)

        adapter = KanbanTailAdapter(source_path, self.control_conn)
        with mock.patch("orchestrator.control_plane.kanban_tail.sqlite3.connect") as source_connect:
            with self.assertRaises(SourcePathSecurityError):
                adapter.tail_once(policy_version="v1", actor="observer", correlation_id="corr-1")
        source_connect.assert_not_called()
        for table in ("source_identity", "observations", "source_cursors", "recovery_checkpoints"):
            self.assertEqual(
                self.control_conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0],
                0,
                f"{table} must remain empty when source pathname trust validation rejects",
            )

    def test_direct_readonly_open_observes_a_live_wal_snapshot(self):
        """The accepted source connection must use SQLite's real WAL sidecar."""
        writer = sqlite3.connect(str(self.source_db_path))
        try:
            self.assertEqual(writer.execute("PRAGMA journal_mode = WAL").fetchone()[0].lower(), "wal")
            writer.execute(
                "INSERT INTO task_events (id, task_id, run_id, kind, payload, created_at) VALUES "
                "(4, 'task-3', NULL, 'task.created', '{}', '2026-01-01T00:03:00+00:00')"
            )
            writer.commit()
            self.assertTrue(Path(f"{self.source_db_path}-wal").exists())
            adapter = KanbanTailAdapter(self.source_db_path, self.control_conn)
            result = adapter.tail_once(policy_version="v1", actor="observer", correlation_id="corr-1")
            self.assertEqual(result.rows_read, 4)
            self.assertEqual(ObservationLedger(self.control_conn).count(), 4)
        finally:
            writer.close()

    def test_tails_a_bounded_batch_in_order(self):
        adapter = KanbanTailAdapter(self.source_db_path, self.control_conn, batch_size=2)
        result = adapter.tail_once(policy_version="v1", actor="observer", correlation_id="corr-1")
        self.assertEqual(result.rows_read, 2)
        self.assertEqual(result.appended, 2)
        self.assertEqual(result.cursor_before, 0)
        self.assertEqual(result.cursor_after, 2)

        result2 = adapter.tail_once(policy_version="v1", actor="observer", correlation_id="corr-1")
        self.assertEqual(result2.rows_read, 1)
        self.assertEqual(result2.appended, 1)
        self.assertEqual(result2.cursor_before, 2)
        self.assertEqual(result2.cursor_after, 3)

        self.assertEqual(ObservationLedger(self.control_conn).count(), 3)

    def test_empty_batch_when_nothing_past_cursor_performs_no_write(self):
        adapter = KanbanTailAdapter(self.source_db_path, self.control_conn)
        adapter.tail_once(policy_version="v1", actor="observer", correlation_id="corr-1")
        before = adapter.current_cursor()
        result = adapter.tail_once(policy_version="v1", actor="observer", correlation_id="corr-1")
        self.assertEqual(result.rows_read, 0)
        self.assertEqual(result.cursor_after, before)

    def test_exact_replay_of_already_seen_rows_dedups(self):
        from orchestrator.control_plane.kanban_tail import _seed_consumed_digest

        adapter = KanbanTailAdapter(self.source_db_path, self.control_conn)
        adapter.tail_once(policy_version="v1", actor="observer", correlation_id="corr-1")
        # Simulate recovery from an earlier checkpoint / cursor reset by
        # directly resetting the durable cursor (and its paired
        # consumed-prefix digest, which must always describe the same
        # prefix the cursor claims), then re-tailing the same source rows
        # unchanged.
        with self.control_conn:
            self.control_conn.execute("BEGIN IMMEDIATE")
            self.control_conn.execute(
                "UPDATE source_cursors SET cursor_value = '0', consumed_digest = ? WHERE source = 'kanban'",
                (_seed_consumed_digest("kanban"),),
            )
            self.control_conn.execute("COMMIT")
        replay = adapter.tail_once(policy_version="v1", actor="observer", correlation_id="corr-1")
        self.assertEqual(replay.rows_read, 3)
        self.assertEqual(replay.appended, 0)
        self.assertEqual(replay.duplicates, 3)
        self.assertEqual(replay.quarantined, 0)
        self.assertEqual(ObservationLedger(self.control_conn).count(), 3)

    def test_conflicting_duplicate_same_source_id_is_quarantined(self):
        from orchestrator.control_plane.kanban_tail import _seed_consumed_digest

        adapter = KanbanTailAdapter(self.source_db_path, self.control_conn)
        adapter.tail_once(policy_version="v1", actor="observer", correlation_id="corr-1")

        # Mutate the source row's content under the same id (a hostile or
        # buggy source rewriting history) and reset the cursor (and its
        # paired consumed-prefix digest) to re-tail it. Resetting only to
        # id=0 (not replaying through the tampered id=1 row) is deliberate:
        # this test targets ledger-level quarantine behavior on replay, not
        # the consumed-prefix tamper detection covered separately.
        source_conn = sqlite3.connect(str(self.source_db_path))
        try:
            source_conn.execute(
                "UPDATE task_events SET kind = 'task.tampered', payload = '{\"a\": 999}' WHERE id = 1"
            )
            source_conn.commit()
        finally:
            source_conn.close()
        with self.control_conn:
            self.control_conn.execute("BEGIN IMMEDIATE")
            self.control_conn.execute(
                "UPDATE source_cursors SET cursor_value = '0', consumed_digest = ? WHERE source = 'kanban'",
                (_seed_consumed_digest("kanban"),),
            )
            self.control_conn.execute("COMMIT")

        replay = adapter.tail_once(policy_version="v1", actor="observer", correlation_id="corr-1")
        self.assertEqual(replay.quarantined, 1)
        self.assertEqual(replay.duplicates, 2)
        conflicts = ObservationLedger(self.control_conn).list_conflicts()
        self.assertEqual(len(conflicts), 1)
        self.assertEqual(conflicts[0].source_id, "1")
        # original observation for source_id=1 is untouched
        original = ObservationLedger(self.control_conn).get_by_source_id("kanban", "1")
        self.assertEqual(original.kind, "task.created")

    def test_batch_and_cursor_advance_atomically(self):
        adapter = KanbanTailAdapter(self.source_db_path, self.control_conn, batch_size=2)
        adapter.tail_once(policy_version="v1", actor="observer", correlation_id="corr-1")
        row = self.control_conn.execute(
            "SELECT cursor_value FROM source_cursors WHERE source = 'kanban'"
        ).fetchone()
        self.assertEqual(row["cursor_value"], "2")
        self.assertEqual(ObservationLedger(self.control_conn).count(), 2)

    def test_never_writes_to_source_db_readonly_open_rejects_write(self):
        adapter = KanbanTailAdapter(self.source_db_path, self.control_conn)
        readonly_conn = adapter._open_source_readonly().conn
        try:
            with self.assertRaises(sqlite3.OperationalError):
                readonly_conn.execute("INSERT INTO task_events (task_id, kind, payload, created_at) VALUES ('x','x','{}','now')")
        finally:
            readonly_conn.close()

    def test_source_db_remains_byte_identical_after_tailing(self):
        before_hash = _file_hash(self.source_db_path)
        adapter = KanbanTailAdapter(self.source_db_path, self.control_conn)
        adapter.tail_once(policy_version="v1", actor="observer", correlation_id="corr-1")
        after_hash = _file_hash(self.source_db_path)
        self.assertEqual(before_hash, after_hash)

    def test_only_verified_columns_are_read_secrets_table_untouched(self):
        adapter = KanbanTailAdapter(self.source_db_path, self.control_conn)
        result = adapter.tail_once(policy_version="v1", actor="observer", correlation_id="corr-1")
        self.assertEqual(result.appended, 3)
        # the secrets table's content never appears anywhere in recorded payloads
        rows = self.control_conn.execute("SELECT payload FROM observations").fetchall()
        for row in rows:
            self.assertNotIn("do-not-touch", row["payload"])


class HostileSourcePathTests(unittest.TestCase):
    """Finding 1: the source URI must survive filenames containing ?, #, %."""

    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.control_conn = connect(Path(self._tmp.name) / "control-plane.db")
        self.addCleanup(self.control_conn.close)

    def _hostile_source(self, filename: str) -> Path:
        path = Path(self._tmp.name) / filename
        _make_source_db(
            path,
            rows=[(1, "task-1", None, "task.created", "{}", "2026-01-01T00:00:00+00:00")],
        )
        return path

    def test_question_mark_in_filename_does_not_inject_uri_params(self):
        # Naively f-stringed into "file:{path}?mode=ro", a stray "?" here
        # would itself start the URI's query string, and "mode=rw" after it
        # would override the read-only mode entirely.
        path = self._hostile_source("kanban?mode=rw&x=1.db")
        adapter = KanbanTailAdapter(path, self.control_conn)
        result = adapter.tail_once(policy_version="v1", actor="observer", correlation_id="corr-1")
        self.assertEqual(result.appended, 1)
        readonly_conn = adapter._open_source_readonly().conn
        try:
            with self.assertRaises(sqlite3.OperationalError):
                readonly_conn.execute(
                    "INSERT INTO task_events (task_id, kind, payload, created_at) VALUES ('x','x','{}','now')"
                )
        finally:
            readonly_conn.close()

    def test_hash_in_filename_does_not_get_parsed_as_fragment(self):
        path = self._hostile_source("kanban#frag.db")
        adapter = KanbanTailAdapter(path, self.control_conn)
        result = adapter.tail_once(policy_version="v1", actor="observer", correlation_id="corr-1")
        self.assertEqual(result.appended, 1)

    def test_percent_in_filename_does_not_get_percent_decoded(self):
        path = self._hostile_source("kan%25ban.db")
        adapter = KanbanTailAdapter(path, self.control_conn)
        result = adapter.tail_once(policy_version="v1", actor="observer", correlation_id="corr-1")
        self.assertEqual(result.appended, 1)

    def test_readonly_connection_has_query_only_pragma_set(self):
        path = self._hostile_source("normal.db")
        adapter = KanbanTailAdapter(path, self.control_conn)
        conn = adapter._open_source_readonly().conn
        try:
            self.assertEqual(conn.execute("PRAGMA query_only").fetchone()[0], 1)
        finally:
            conn.close()

    def test_write_attempt_against_hostile_named_source_is_rejected(self):
        path = self._hostile_source("evil?mode=rwc#x.db")
        adapter = KanbanTailAdapter(path, self.control_conn)
        conn = adapter._open_source_readonly().conn
        try:
            with self.assertRaises(sqlite3.OperationalError):
                conn.execute("DELETE FROM task_events")
        finally:
            conn.close()


class BatchSizeValidationTests(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.control_conn = connect(Path(self._tmp.name) / "control-plane.db")
        self.addCleanup(self.control_conn.close)
        self.source_db_path = Path(self._tmp.name) / "kanban.db"
        _make_source_db(self.source_db_path, rows=[])

    def test_zero_batch_size_is_rejected(self):
        with self.assertRaises(InvalidBatchSizeError):
            KanbanTailAdapter(self.source_db_path, self.control_conn, batch_size=0)

    def test_negative_batch_size_is_rejected(self):
        # SQLite treats a negative LIMIT as "no limit" — silently unbounded,
        # not merely invalid.
        with self.assertRaises(InvalidBatchSizeError):
            KanbanTailAdapter(self.source_db_path, self.control_conn, batch_size=-1)

    def test_oversized_batch_size_is_rejected(self):
        with self.assertRaises(InvalidBatchSizeError):
            KanbanTailAdapter(self.source_db_path, self.control_conn, batch_size=10_000_001)

    def test_non_integer_batch_size_is_rejected(self):
        with self.assertRaises(InvalidBatchSizeError):
            KanbanTailAdapter(self.source_db_path, self.control_conn, batch_size="500")

    def test_boolean_batch_size_is_rejected(self):
        with self.assertRaises(InvalidBatchSizeError):
            KanbanTailAdapter(self.source_db_path, self.control_conn, batch_size=True)

    def test_minimum_and_maximum_batch_sizes_are_accepted(self):
        KanbanTailAdapter(self.source_db_path, self.control_conn, batch_size=1)
        KanbanTailAdapter(self.source_db_path, self.control_conn, batch_size=10_000)


class SourceContractValidationTests(unittest.TestCase):
    """Finding 7: fail closed on a source table that doesn't match the exact contract."""

    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.control_conn = connect(Path(self._tmp.name) / "control-plane.db")
        self.addCleanup(self.control_conn.close)
        self.source_db_path = Path(self._tmp.name) / "kanban.db"

    def test_missing_task_events_table_is_rejected(self):
        conn = sqlite3.connect(str(self.source_db_path))
        conn.execute("CREATE TABLE not_task_events (id INTEGER PRIMARY KEY)")
        conn.commit()
        conn.close()
        adapter = KanbanTailAdapter(self.source_db_path, self.control_conn)
        with self.assertRaises(SourceContractError):
            adapter.tail_once(policy_version="v1", actor="observer", correlation_id="corr-1")

    def test_missing_required_column_is_rejected(self):
        conn = sqlite3.connect(str(self.source_db_path))
        conn.execute(
            "CREATE TABLE task_events (id INTEGER PRIMARY KEY, task_id TEXT NOT NULL, kind TEXT NOT NULL, "
            "payload TEXT NOT NULL, created_at TEXT NOT NULL)"  # run_id missing entirely
        )
        conn.commit()
        conn.close()
        adapter = KanbanTailAdapter(self.source_db_path, self.control_conn)
        with self.assertRaises(SourceContractError):
            adapter.tail_once(policy_version="v1", actor="observer", correlation_id="corr-1")

    def test_id_not_integer_primary_key_is_rejected(self):
        conn = sqlite3.connect(str(self.source_db_path))
        conn.execute(
            "CREATE TABLE task_events (id TEXT, task_id TEXT NOT NULL, run_id TEXT, kind TEXT NOT NULL, "
            "payload TEXT NOT NULL, created_at TEXT NOT NULL)"
        )
        conn.commit()
        conn.close()
        adapter = KanbanTailAdapter(self.source_db_path, self.control_conn)
        with self.assertRaises(SourceContractError):
            adapter.tail_once(policy_version="v1", actor="observer", correlation_id="corr-1")

    def test_nullable_required_text_column_is_rejected(self):
        conn = sqlite3.connect(str(self.source_db_path))
        conn.execute(
            # kind is nullable rather than NOT NULL — violates the contract
            # even though it happens to be TEXT.
            "CREATE TABLE task_events (id INTEGER PRIMARY KEY, task_id TEXT NOT NULL, run_id TEXT, kind TEXT, "
            "payload TEXT NOT NULL, created_at TEXT NOT NULL)"
        )
        conn.commit()
        conn.close()
        adapter = KanbanTailAdapter(self.source_db_path, self.control_conn)
        with self.assertRaises(SourceContractError):
            adapter.tail_once(policy_version="v1", actor="observer", correlation_id="corr-1")

    def test_valid_contract_with_extra_columns_is_accepted(self):
        conn = sqlite3.connect(str(self.source_db_path))
        conn.execute(
            "CREATE TABLE task_events (id INTEGER PRIMARY KEY, task_id TEXT NOT NULL, run_id TEXT, "
            "kind TEXT NOT NULL, payload TEXT NOT NULL, created_at TEXT NOT NULL, extra TEXT)"
        )
        conn.execute(
            "INSERT INTO task_events (id, task_id, run_id, kind, payload, created_at, extra) "
            "VALUES (1, 't-1', NULL, 'task.created', '{}', 'now', 'unused')"
        )
        conn.commit()
        conn.close()
        adapter = KanbanTailAdapter(self.source_db_path, self.control_conn)
        result = adapter.tail_once(policy_version="v1", actor="observer", correlation_id="corr-1")
        self.assertEqual(result.appended, 1)

    def test_run_id_declared_integer_is_rejected(self):
        # run_id is documented/relied-on as nullable TEXT; a source that
        # declares it INTEGER must fail closed rather than being silently
        # str()-coerced downstream.
        conn = sqlite3.connect(str(self.source_db_path))
        conn.execute(
            "CREATE TABLE task_events (id INTEGER PRIMARY KEY, task_id TEXT NOT NULL, run_id INTEGER, "
            "kind TEXT NOT NULL, payload TEXT NOT NULL, created_at TEXT NOT NULL)"
        )
        conn.commit()
        conn.close()
        adapter = KanbanTailAdapter(self.source_db_path, self.control_conn)
        with self.assertRaises(SourceContractError):
            adapter.tail_once(policy_version="v1", actor="observer", correlation_id="corr-1")

    def test_run_id_declared_not_null_is_rejected(self):
        # run_id must stay nullable: a NOT NULL source declaration would mean
        # a genuinely-absent run identity could never be represented, silently
        # forcing every event to carry a (possibly fabricated) run_id.
        conn = sqlite3.connect(str(self.source_db_path))
        conn.execute(
            "CREATE TABLE task_events (id INTEGER PRIMARY KEY, task_id TEXT NOT NULL, run_id TEXT NOT NULL, "
            "kind TEXT NOT NULL, payload TEXT NOT NULL, created_at TEXT NOT NULL)"
        )
        conn.commit()
        conn.close()
        adapter = KanbanTailAdapter(self.source_db_path, self.control_conn)
        with self.assertRaises(SourceContractError):
            adapter.tail_once(policy_version="v1", actor="observer", correlation_id="corr-1")

    def test_null_value_in_notnull_text_column_is_rejected_without_coercion(self):
        # A schema can declare NOT NULL but a hand-edited/corrupted DB file
        # could still surface a NULL at the storage layer despite it; whether
        # or not that's reachable via the driver, the row-validation contract
        # itself must reject it outright rather than silently coercing it
        # into the string "None" via str(None).
        from orchestrator.control_plane import kanban_tail as kt

        with self.assertRaises(SourceContractError):
            kt._validate_row((1, "t-1", None, "task.created", None, "now"))

    def test_non_string_id_is_rejected(self):
        from orchestrator.control_plane import kanban_tail as kt

        with self.assertRaises(SourceContractError):
            kt._validate_row(("1", "t-1", None, "task.created", "{}", "now"))

    def test_id_declared_primary_key_desc_is_rejected(self):
        # "INTEGER PRIMARY KEY DESC" disables the rowid-alias optimization:
        # id becomes an ordinary, independently-assigned column that still
        # enforces uniqueness via an auxiliary index, but is no longer the
        # same storage cell as the table's actual rowid. PRAGMA table_info
        # reports an identical pk=1/type=INTEGER either way, so only the
        # query planner's actual access path tells the two apart.
        conn = sqlite3.connect(str(self.source_db_path))
        conn.execute(
            "CREATE TABLE task_events (id INTEGER PRIMARY KEY DESC, task_id TEXT NOT NULL, run_id TEXT, "
            "kind TEXT NOT NULL, payload TEXT NOT NULL, created_at TEXT NOT NULL)"
        )
        conn.execute(
            "INSERT INTO task_events (id, task_id, run_id, kind, payload, created_at) VALUES "
            "(1, 't-1', NULL, 'task.created', '{}', 'now')"
        )
        conn.commit()
        conn.close()
        adapter = KanbanTailAdapter(self.source_db_path, self.control_conn)
        with self.assertRaises(SourceContractError):
            adapter.tail_once(policy_version="v1", actor="observer", correlation_id="corr-1")

    def test_genuine_rowid_alias_does_not_depend_on_query_plan_wording(self):
        """Planner prose is not SQLite's contract for rowid-alias identity."""
        from orchestrator.control_plane.kanban_tail import _verify_source_contract

        class _PlanCursor:
            def fetchall(self):
                # Equivalent planner diagnostics vary between SQLite releases.
                # This valid rowid access description deliberately omits the
                # non-contractual "INTEGER PRIMARY KEY" wording.
                return [(0, 0, 0, "SEARCH task_events USING rowid=?")]

        class _ConnectionWithAlternatePlanWording:
            def __init__(self, delegate):
                self._delegate = delegate

            def execute(self, sql, parameters=()):
                if sql.startswith("EXPLAIN QUERY PLAN"):
                    return _PlanCursor()
                return self._delegate.execute(sql, parameters)

        source = sqlite3.connect(str(self.source_db_path))
        try:
            source.execute(
                "CREATE TABLE task_events (id INTEGER PRIMARY KEY, task_id TEXT NOT NULL, run_id TEXT, "
                "kind TEXT NOT NULL, payload TEXT NOT NULL, created_at TEXT NOT NULL)"
            )
            _verify_source_contract(_ConnectionWithAlternatePlanWording(source))
        finally:
            source.close()


class SourceIdentityMismatchTests(unittest.TestCase):
    """Finding 3: the durable cursor is bound to verified source-instance identity."""

    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.source_db_path = Path(self._tmp.name) / "kanban.db"
        self.backup_path = Path(self._tmp.name) / "kanban-backup.db"
        self.control_conn = connect(Path(self._tmp.name) / "control-plane.db")
        self.addCleanup(self.control_conn.close)
        _make_source_db(
            self.source_db_path,
            rows=[
                (1, "task-1", None, "task.created", '{"a": 1}', "2026-01-01T00:00:00+00:00"),
                (2, "task-1", "run-1", "task.status_changed", '{"a": 2}', "2026-01-01T00:01:00+00:00"),
            ],
        )

    def _snapshot_backup(self) -> None:
        source_conn = sqlite3.connect(str(self.source_db_path))
        try:
            backup_conn = sqlite3.connect(str(self.backup_path))
            try:
                source_conn.backup(backup_conn)
            finally:
                backup_conn.close()
        finally:
            source_conn.close()

    def test_restore_to_earlier_backup_fails_closed_not_silently_skipped(self):
        adapter = KanbanTailAdapter(self.source_db_path, self.control_conn)
        adapter.tail_once(policy_version="v1", actor="observer", correlation_id="corr-1")
        self.assertEqual(adapter.current_cursor(), 2)

        # Take a backup *before* the row the cursor currently points at ever
        # existed (an earlier state of the source), then simulate a restore
        # by copying that backup back over the live source file.
        source_conn = sqlite3.connect(str(self.source_db_path))
        try:
            source_conn.execute("DELETE FROM task_events WHERE id = 2")
            source_conn.commit()
        finally:
            source_conn.close()
        self._snapshot_backup()
        self.source_db_path.write_bytes(self.backup_path.read_bytes())

        with self.assertRaises(SourceIdentityMismatchError):
            adapter.tail_once(policy_version="v1", actor="observer", correlation_id="corr-2")
        # Fail closed: cursor and observation count are untouched, not
        # silently left stuck at 2 while future tails quietly no-op forever.
        self.assertEqual(adapter.current_cursor(), 2)
        self.assertEqual(ObservationLedger(self.control_conn).count(), 2)

    def test_content_change_at_cursor_row_under_same_id_fails_closed(self):
        adapter = KanbanTailAdapter(self.source_db_path, self.control_conn)
        adapter.tail_once(policy_version="v1", actor="observer", correlation_id="corr-1")

        source_conn = sqlite3.connect(str(self.source_db_path))
        try:
            source_conn.execute(
                "UPDATE task_events SET payload = '{\"a\": 999}' WHERE id = 2"
            )
            source_conn.commit()
        finally:
            source_conn.close()

        with self.assertRaises(SourceIdentityMismatchError):
            adapter.tail_once(policy_version="v1", actor="observer", correlation_id="corr-2")

    def test_normal_growth_does_not_trigger_identity_mismatch(self):
        adapter = KanbanTailAdapter(self.source_db_path, self.control_conn)
        adapter.tail_once(policy_version="v1", actor="observer", correlation_id="corr-1")

        _append_source_rows_for_identity_test(
            self.source_db_path,
            rows=[(3, "task-2", None, "task.created", "{}", "2026-01-01T00:02:00+00:00")],
        )
        result = adapter.tail_once(policy_version="v1", actor="observer", correlation_id="corr-2")
        self.assertEqual(result.appended, 1)
        self.assertEqual(adapter.current_cursor(), 3)

    def test_wholesale_replacement_with_unrelated_db_fails_closed(self):
        adapter = KanbanTailAdapter(self.source_db_path, self.control_conn)
        adapter.tail_once(policy_version="v1", actor="observer", correlation_id="corr-1")
        self.assertEqual(adapter.current_cursor(), 2)

        # Replace the entire file with an unrelated but schema-valid source
        # DB (different row content under the same ids), via an atomic
        # rename so the adapter is really tailing a different underlying
        # file rather than a mutation of the one it already saw.
        replacement_path = Path(self._tmp.name) / "kanban-replacement.db"
        _make_source_db(
            replacement_path,
            rows=[
                (1, "task-X", None, "task.created", '{"different": true}', "2099-01-01T00:00:00+00:00"),
                (2, "task-X", None, "task.created", '{"different": true}', "2099-01-01T00:00:01+00:00"),
            ],
        )
        os.replace(replacement_path, self.source_db_path)

        with self.assertRaises(SourceIdentityMismatchError):
            adapter.tail_once(policy_version="v1", actor="observer", correlation_id="corr-2")
        # Fail closed: no new observations or cursor progression from the
        # replacement source's rows.
        self.assertEqual(adapter.current_cursor(), 2)
        self.assertEqual(ObservationLedger(self.control_conn).count(), 2)

    def test_first_tail_records_source_instance_identity(self):
        adapter = KanbanTailAdapter(self.source_db_path, self.control_conn)
        adapter.tail_once(policy_version="v1", actor="observer", correlation_id="corr-1")
        row = self.control_conn.execute(
            "SELECT canonical_path, device, inode FROM source_identity WHERE source = 'kanban'"
        ).fetchone()
        self.assertIsNotNone(row)
        st = self.source_db_path.resolve().stat()
        self.assertEqual(row["canonical_path"], str(self.source_db_path.resolve()))
        self.assertEqual(row["device"], st.st_dev)
        self.assertEqual(row["inode"], st.st_ino)

    def test_atomic_replacement_preserving_cursor_anchor_content_is_detected(self):
        # A replacement file engineered so the row at the cursor anchor is
        # byte-for-byte identical to what was already observed there (so the
        # content-hash anchor check in _verify_source_identity alone would
        # NOT catch it) but earlier history (row id=1) differs and the file
        # itself is a distinct physical instance (atomic rename => new
        # inode). Only the instance-identity (device/inode) check catches
        # this.
        adapter = KanbanTailAdapter(self.source_db_path, self.control_conn)
        adapter.tail_once(policy_version="v1", actor="observer", correlation_id="corr-1")
        self.assertEqual(adapter.current_cursor(), 2)
        anchor_row = sqlite3.connect(str(self.source_db_path)).execute(
            "SELECT id, task_id, run_id, kind, payload, created_at FROM task_events WHERE id = 2"
        ).fetchone()

        replacement_path = Path(self._tmp.name) / "kanban-anchor-preserved.db"
        _make_source_db(
            replacement_path,
            rows=[
                # Earlier history tampered...
                (1, "task-TAMPERED", None, "task.created", '{"tampered": true}', "2099-01-01T00:00:00+00:00"),
                # ...but the exact anchor row content is preserved.
                anchor_row,
            ],
        )
        os.replace(replacement_path, self.source_db_path)

        with self.assertRaises(SourceIdentityMismatchError):
            adapter.tail_once(policy_version="v1", actor="observer", correlation_id="corr-2")
        self.assertEqual(adapter.current_cursor(), 2)
        self.assertEqual(ObservationLedger(self.control_conn).count(), 2)

    def test_restart_against_the_same_unreplaced_file_does_not_trigger_mismatch(self):
        adapter = KanbanTailAdapter(self.source_db_path, self.control_conn)
        adapter.tail_once(policy_version="v1", actor="observer", correlation_id="corr-1")

        _append_source_rows_for_identity_test(
            self.source_db_path,
            rows=[(3, "task-2", None, "task.created", "{}", "2026-01-01T00:02:00+00:00")],
        )
        # A brand-new adapter instance, same connections/files — simulates a
        # process restart against the exact same (unreplaced) source file.
        restarted_adapter = KanbanTailAdapter(self.source_db_path, self.control_conn)
        result = restarted_adapter.tail_once(policy_version="v1", actor="observer", correlation_id="corr-2")
        self.assertEqual(result.appended, 1)
        self.assertEqual(restarted_adapter.current_cursor(), 3)


class SourceContractPreWriteRowValidationTests(unittest.TestCase):
    """Codex re-review: an invalid row (schema-valid columns/types, but a
    stored value that violates the row-level contract) must be rejected
    before ANY control-plane persistence -- including the one-time
    append-only source_identity write, not merely before the observation
    ledger write. A TEXT-affinity column does not reject a BLOB value at
    insert time (only INTEGER/REAL literals get converted to TEXT; a BLOB
    is stored as-is), so a schema-valid task_events table can still hold a
    row whose payload (or another required TEXT column) is not text."""

    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.control_conn = connect(Path(self._tmp.name) / "control-plane.db")
        self.addCleanup(self.control_conn.close)
        self.source_db_path = Path(self._tmp.name) / "kanban.db"

    def _assert_all_control_tables_are_empty(self):
        for table in ("source_identity", "observations", "source_cursors", "recovery_checkpoints"):
            count = self.control_conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            self.assertEqual(count, 0, f"{table} must remain empty on a pre-write contract rejection")

    def test_invalid_non_text_payload_row_rejected_before_any_control_plane_write(self):
        conn = sqlite3.connect(str(self.source_db_path))
        conn.execute(
            "CREATE TABLE task_events (id INTEGER PRIMARY KEY, task_id TEXT NOT NULL, run_id TEXT, "
            "kind TEXT NOT NULL, payload TEXT NOT NULL, created_at TEXT NOT NULL)"
        )
        conn.execute(
            "INSERT INTO task_events (id, task_id, run_id, kind, payload, created_at) "
            "VALUES (1, 't-1', NULL, 'task.created', X'0011', 'now')"
        )
        conn.commit()
        conn.close()
        adapter = KanbanTailAdapter(self.source_db_path, self.control_conn)
        with self.assertRaises(SourceContractError):
            adapter.tail_once(policy_version="v1", actor="observer", correlation_id="corr-1")
        self._assert_all_control_tables_are_empty()

    def test_invalid_non_text_required_column_row_rejected_before_any_control_plane_write(self):
        conn = sqlite3.connect(str(self.source_db_path))
        conn.execute(
            "CREATE TABLE task_events (id INTEGER PRIMARY KEY, task_id TEXT NOT NULL, run_id TEXT, "
            "kind TEXT NOT NULL, payload TEXT NOT NULL, created_at TEXT NOT NULL)"
        )
        conn.execute(
            "INSERT INTO task_events (id, task_id, run_id, kind, payload, created_at) "
            "VALUES (1, X'0011', NULL, 'task.created', '{}', 'now')"
        )
        conn.commit()
        conn.close()
        adapter = KanbanTailAdapter(self.source_db_path, self.control_conn)
        with self.assertRaises(SourceContractError):
            adapter.tail_once(policy_version="v1", actor="observer", correlation_id="corr-1")
        self._assert_all_control_tables_are_empty()

    def test_invalid_row_within_batch_window_is_caught_before_identity_write(self):
        # The invalid row (id=2) isn't the first row in the table; the
        # preflight validation must still cover the whole bounded batch this
        # cycle will actually read, ahead of the identity write, not just
        # whichever row happens to be read first.
        conn = sqlite3.connect(str(self.source_db_path))
        conn.execute(
            "CREATE TABLE task_events (id INTEGER PRIMARY KEY, task_id TEXT NOT NULL, run_id TEXT, "
            "kind TEXT NOT NULL, payload TEXT NOT NULL, created_at TEXT NOT NULL)"
        )
        conn.execute(
            "INSERT INTO task_events (id, task_id, run_id, kind, payload, created_at) VALUES "
            "(1, 't-1', NULL, 'task.created', '{}', 'now')"
        )
        conn.execute(
            "INSERT INTO task_events (id, task_id, run_id, kind, payload, created_at) VALUES "
            "(2, 't-1', NULL, 'task.created', X'0011', 'now')"
        )
        conn.commit()
        conn.close()
        adapter = KanbanTailAdapter(self.source_db_path, self.control_conn, batch_size=10)
        with self.assertRaises(SourceContractError):
            adapter.tail_once(policy_version="v1", actor="observer", correlation_id="corr-1")
        self._assert_all_control_tables_are_empty()


class WithoutRowidCompositePrimaryKeyRejectionTests(unittest.TestCase):
    """Codex re-review: 'id INTEGER PRIMARY KEY' must be the sole rowid
    alias. A WITHOUT ROWID table -- composite primary key or not -- does not
    give 'id' rowid-alias semantics, so id values are only guaranteed
    unique in combination with the rest of the declared primary key (or, for
    a plain WITHOUT ROWID single-column key, lose the ordinary rowid table's
    dedicated integer-key fast path this module's cursor/identity logic is
    built against) -- not globally unique or rowid-backed the way the
    cursor and identity-verification logic (keyed purely on id) both
    assume. Must be rejected before ANY control-plane write."""

    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.control_conn = connect(Path(self._tmp.name) / "control-plane.db")
        self.addCleanup(self.control_conn.close)
        self.source_db_path = Path(self._tmp.name) / "kanban.db"

    def _assert_all_control_tables_are_empty(self):
        for table in ("source_identity", "observations", "source_cursors", "recovery_checkpoints"):
            count = self.control_conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            self.assertEqual(count, 0, f"{table} must remain empty on a pre-write contract rejection")

    def test_without_rowid_composite_primary_key_schema_is_rejected(self):
        conn = sqlite3.connect(str(self.source_db_path))
        conn.execute(
            """
            CREATE TABLE task_events (
                id INTEGER NOT NULL,
                task_id TEXT NOT NULL,
                run_id TEXT,
                kind TEXT NOT NULL,
                payload TEXT NOT NULL,
                created_at TEXT NOT NULL,
                PRIMARY KEY (id, task_id)
            ) WITHOUT ROWID
            """
        )
        # id is NOT globally unique here -- only the (id, task_id) pair is.
        conn.execute(
            "INSERT INTO task_events (id, task_id, run_id, kind, payload, created_at) VALUES "
            "(1, 't-1', NULL, 'task.created', '{}', 'now')"
        )
        conn.execute(
            "INSERT INTO task_events (id, task_id, run_id, kind, payload, created_at) VALUES "
            "(1, 't-2', NULL, 'task.created', '{}', 'now')"
        )
        conn.commit()
        conn.close()
        adapter = KanbanTailAdapter(self.source_db_path, self.control_conn, batch_size=1)
        with self.assertRaises(SourceContractError):
            adapter.tail_once(policy_version="v1", actor="observer", correlation_id="corr-1")
        self._assert_all_control_tables_are_empty()

    def test_without_rowid_single_column_primary_key_schema_is_rejected(self):
        conn = sqlite3.connect(str(self.source_db_path))
        conn.execute(
            """
            CREATE TABLE task_events (
                id INTEGER PRIMARY KEY,
                task_id TEXT NOT NULL,
                run_id TEXT,
                kind TEXT NOT NULL,
                payload TEXT NOT NULL,
                created_at TEXT NOT NULL
            ) WITHOUT ROWID
            """
        )
        conn.execute(
            "INSERT INTO task_events (id, task_id, run_id, kind, payload, created_at) VALUES "
            "(1, 't-1', NULL, 'task.created', '{}', 'now')"
        )
        conn.commit()
        conn.close()
        adapter = KanbanTailAdapter(self.source_db_path, self.control_conn)
        with self.assertRaises(SourceContractError):
            adapter.tail_once(policy_version="v1", actor="observer", correlation_id="corr-1")
        self._assert_all_control_tables_are_empty()


class MalformedZeroOrNegativeIdRowTests(unittest.TestCase):
    """Codex re-review: a malformed row at id<=0 is invisible to the normal
    'id > cursor' batch scan (the cursor starts at 0), and previously also
    invisible to the consumed-prefix digest recompute (which short-circuited
    entirely when upto_id<=0) -- so it could sit in the source table
    forever, undetected, while the one-time append-only source_identity
    write (and any other control-plane write) still went ahead. Preflight
    must validate every row relevant to the entire source history --
    including ids <= cursor and id=0/negative rows -- before ANY
    control-plane write."""

    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.control_conn = connect(Path(self._tmp.name) / "control-plane.db")
        self.addCleanup(self.control_conn.close)
        self.source_db_path = Path(self._tmp.name) / "kanban.db"

    def _assert_all_control_tables_are_empty(self):
        for table in ("source_identity", "observations", "source_cursors", "recovery_checkpoints"):
            count = self.control_conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            self.assertEqual(count, 0, f"{table} must remain empty on a pre-write contract rejection")

    def test_malformed_zero_id_row_rejected_before_any_control_plane_write(self):
        conn = sqlite3.connect(str(self.source_db_path))
        conn.execute(
            "CREATE TABLE task_events (id INTEGER PRIMARY KEY, task_id TEXT NOT NULL, run_id TEXT, "
            "kind TEXT NOT NULL, payload TEXT NOT NULL, created_at TEXT NOT NULL)"
        )
        conn.execute(
            "INSERT INTO task_events (id, task_id, run_id, kind, payload, created_at) VALUES "
            "(0, 't-1', NULL, 'task.created', X'0011', 'now')"
        )
        conn.execute(
            "INSERT INTO task_events (id, task_id, run_id, kind, payload, created_at) VALUES "
            "(1, 't-1', NULL, 'task.created', '{}', 'now')"
        )
        conn.commit()
        conn.close()
        adapter = KanbanTailAdapter(self.source_db_path, self.control_conn)
        with self.assertRaises(SourceContractError):
            adapter.tail_once(policy_version="v1", actor="observer", correlation_id="corr-1")
        self._assert_all_control_tables_are_empty()

    def test_malformed_negative_id_row_rejected_before_any_control_plane_write(self):
        conn = sqlite3.connect(str(self.source_db_path))
        conn.execute(
            "CREATE TABLE task_events (id INTEGER PRIMARY KEY, task_id TEXT NOT NULL, run_id TEXT, "
            "kind TEXT NOT NULL, payload TEXT NOT NULL, created_at TEXT NOT NULL)"
        )
        conn.execute(
            "INSERT INTO task_events (id, task_id, run_id, kind, payload, created_at) VALUES "
            "(-1, X'DEAD', NULL, 'task.created', '{}', 'now')"
        )
        conn.execute(
            "INSERT INTO task_events (id, task_id, run_id, kind, payload, created_at) VALUES "
            "(1, 't-1', NULL, 'task.created', '{}', 'now')"
        )
        conn.commit()
        conn.close()
        adapter = KanbanTailAdapter(self.source_db_path, self.control_conn)
        with self.assertRaises(SourceContractError):
            adapter.tail_once(policy_version="v1", actor="observer", correlation_id="corr-1")
        self._assert_all_control_tables_are_empty()


@unittest.skip(
    "Obsoleted by the source-path contract: /proc/self/fd is not a descriptor-stable SQLite source "
    "and is deliberately unsupported; SourcePathSecurityError and WAL regressions cover the supported path."
)
class SourcePhysicalIdentityTOCTOUTests(unittest.TestCase):
    """Historical reproducers for the invalid descriptor-stability claim.

    Kept as non-executing evidence of why this adapter now validates a safe
    source lookup path and opens SQLite's normal URI instead of claiming that
    a ``/proc/self/fd`` name is a stable SQLite database handle.

    The old claim was: physical source identity must be tied to the exact
    OS file descriptor sqlite3 opened (fstat of the fd found via
    /proc/self/fd), not a re-stat of the source pathname before/after
    opening. A pathname re-stat only observes whatever is at the path at
    the moment each stat call runs, so an attacker who swaps the file
    immediately before connect() and swaps the original back immediately
    after it returns defeats a pre/post pathname-stat check entirely
    (independently reproduced against a prior implementation: no
    exception, the swapped-in row appended, and the *original* file's
    identity durably recorded even though the swapped-in file's bytes
    were what got read). fd-based identity cannot be fooled this way: it
    reflects the literal file the connection is bound to for its whole
    lifetime, immune to anything that happens at the path afterward."""

    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.control_conn = connect(Path(self._tmp.name) / "control-plane.db")
        self.addCleanup(self.control_conn.close)
        self.source_db_path = Path(self._tmp.name) / "kanban.db"
        _make_source_db(
            self.source_db_path,
            rows=[(1, "task-1", None, "task.created", "{}", "2026-01-01T00:00:00+00:00")],
        )

    def test_file_swapped_after_open_reads_only_the_original_fd_then_swap_is_caught_next_cycle(self):
        # The already-open connection's fd keeps reading the ORIGINAL
        # file's bytes for its entire lifetime (ordinary POSIX semantics),
        # regardless of what gets swapped in at the path mid-cycle -- so
        # this cycle's read is genuinely unaffected and must not be
        # aborted for no reason. Physical identity is bound to that same
        # stable fd (fstat, not a path restat), so it is recorded
        # correctly too: the original file's identity, not the swapped-in
        # one, since the swapped-in file's bytes were never actually read.
        # The now-swapped file left sitting at the path is instead caught
        # on the *next* cycle, when a fresh connection opens it and its
        # content no longer matches durable evidence for the row(s) already
        # consumed -- so a tamper is always caught, just not necessarily on
        # the exact cycle during which the swap occurred.
        adapter = KanbanTailAdapter(self.source_db_path, self.control_conn)
        original_fetch = adapter._fetch_and_validate_batch

        def _swap_then_fetch(source_conn, cursor_before):
            replacement_path = Path(self._tmp.name) / "kanban-swapped.db"
            _make_source_db(
                replacement_path,
                rows=[(1, "task-EVIL", None, "task.created", "{}", "2026-01-01T00:00:00+00:00")],
            )
            os.replace(replacement_path, self.source_db_path)
            return original_fetch(source_conn, cursor_before)

        adapter._fetch_and_validate_batch = _swap_then_fetch
        result = adapter.tail_once(policy_version="v1", actor="observer", correlation_id="corr-1")
        self.assertEqual(result.appended, 1)
        observed = ObservationLedger(self.control_conn).get_by_source_id("kanban", "1")
        self.assertEqual(observed.entity_id, "task-1")  # the original, not "task-EVIL"

        # A second, independent adapter/cycle opens the now-permanently
        # swapped-in file at the same path -- must fail closed rather than
        # silently accepting the swapped content or the swap being invisible.
        adapter2 = KanbanTailAdapter(self.source_db_path, self.control_conn)
        with self.assertRaises(SourceIdentityMismatchError):
            adapter2.tail_once(policy_version="v1", actor="observer", correlation_id="corr-2")
        self.assertEqual(ObservationLedger(self.control_conn).count(), 1)

    def test_swap_and_revert_bracketing_the_open_call_cannot_bind_the_wrong_identity(self):
        # The narrowest possible window: the attacker swaps the file at the
        # source path to a different physical file (new inode) immediately
        # *before* sqlite3.connect() performs its internal open() syscall,
        # then swaps the ORIGINAL file straight back into place immediately
        # *after* that connect() call returns -- so a pre/post *stat of the
        # pathname* (taken outside the connect() call) sees no change at
        # all, even though the connection just opened, and will read for
        # its entire lifetime, the malicious file's bytes. Only identity
        # derived from the actual fd sqlite opened (not a re-stat of the
        # mutable path) can catch this. Reproduces the exact failure mode
        # independently confirmed against this module before any fix: no
        # exception, the malicious row appended to observations, and
        # source_identity recording the pre-swap (original) file's
        # (device, inode) -- not the file whose bytes were actually read.
        adapter = KanbanTailAdapter(self.source_db_path, self.control_conn)
        real_connect = sqlite3.connect
        swap_dir = Path(self._tmp.name)
        malicious_path = swap_dir / "malicious.db"
        original_backup = swap_dir / "orig_backup.db"
        state: dict = {}

        def _wrapped_connect(dsn, *args, **kwargs):
            if not (isinstance(dsn, str) and dsn.startswith("file:")):
                return real_connect(dsn, *args, **kwargs)
            _make_source_db(
                malicious_path,
                rows=[(1, "task-EVIL", None, "task.created", "{}", "2026-01-01T00:00:00+00:00")],
            )
            state["malicious_stat"] = malicious_path.stat()
            os.replace(self.source_db_path, original_backup)
            os.replace(malicious_path, self.source_db_path)
            conn = real_connect(dsn, *args, **kwargs)
            os.replace(self.source_db_path, swap_dir / "malicious_leftover.db")
            os.replace(original_backup, self.source_db_path)
            return conn

        with mock.patch("orchestrator.control_plane.kanban_tail.sqlite3.connect", _wrapped_connect):
            try:
                adapter.tail_once(policy_version="v1", actor="observer", correlation_id="corr-1")
            except SourceIdentityMismatchError:
                pass
            else:
                observed = ObservationLedger(self.control_conn).get_by_source_id("kanban", "1")
                identity_row = self.control_conn.execute(
                    "SELECT device, inode FROM source_identity WHERE source = 'kanban'"
                ).fetchone()
                if observed is not None and observed.entity_id == "task-EVIL":
                    # Data actually consumed came from the malicious file:
                    # the durably recorded identity must reflect that same
                    # file, never the pre-swap original's (device, inode).
                    self.assertIsNotNone(identity_row)
                    self.assertEqual(
                        (identity_row["device"], identity_row["inode"]),
                        (state["malicious_stat"].st_dev, state["malicious_stat"].st_ino),
                        "malicious content was consumed but source_identity recorded a different "
                        "file's identity -- the durable binding does not reflect what was actually read",
                    )

    def test_decoy_descriptor_cannot_replace_the_opened_database_identity(self):
        """The identity probe must name the same inode SQLite reads.

        Merely scanning /proc/self/fd after connect is insufficient: during a
        swap-and-revert an unrelated fd opened on the restored pathname can
        be the only exact pathname match, while SQLite remains attached to
        the swapped-out (now deleted) database.  The test makes that state
        deterministic; success is allowed only when the recorded identity is
        the malicious database that was actually read, never the decoy.
        """
        adapter = KanbanTailAdapter(self.source_db_path, self.control_conn)
        real_connect = sqlite3.connect
        swap_dir = Path(self._tmp.name)
        malicious_path = swap_dir / "malicious-decoy.db"
        original_backup = swap_dir / "orig-decoy-backup.db"
        state: dict = {"original_stat": self.source_db_path.stat()}

        def _wrapped_connect(dsn, *args, **kwargs):
            if not (isinstance(dsn, str) and dsn.startswith("file:")):
                return real_connect(dsn, *args, **kwargs)
            _make_source_db(
                malicious_path,
                rows=[(1, "task-EVIL", None, "task.created", "{}", "2026-01-01T00:00:00+00:00")],
            )
            state["malicious_stat"] = malicious_path.stat()
            os.replace(self.source_db_path, original_backup)
            os.replace(malicious_path, self.source_db_path)
            conn = real_connect(dsn, *args, **kwargs)
            os.replace(self.source_db_path, swap_dir / "malicious-decoy-leftover.db")
            os.replace(original_backup, self.source_db_path)
            # This fd points at the restored original path.  A post-connect
            # pathname/fd scan must not mistake it for SQLite's fd above.
            state["decoy_fd"] = os.open(self.source_db_path, os.O_RDONLY)
            return conn

        try:
            with mock.patch("orchestrator.control_plane.kanban_tail.sqlite3.connect", _wrapped_connect):
                try:
                    adapter.tail_once(policy_version="v1", actor="observer", correlation_id="corr-1")
                except SourceIdentityMismatchError:
                    # Failing closed is also safe; no decoy identity may have
                    # been committed along with observations/cursor state.
                    self.assertEqual(ObservationLedger(self.control_conn).count(), 0)
                    self.assertEqual(
                        self.control_conn.execute("SELECT COUNT(*) FROM source_identity").fetchone()[0], 0
                    )
                else:
                    observed = ObservationLedger(self.control_conn).get_by_source_id("kanban", "1")
                    identity_row = self.control_conn.execute(
                        "SELECT device, inode FROM source_identity WHERE source = 'kanban'"
                    ).fetchone()
                    expected_stat = (
                        state["malicious_stat"] if observed.entity_id == "task-EVIL" else state["original_stat"]
                    )
                    self.assertIn(observed.entity_id, {"task-1", "task-EVIL"})
                    self.assertEqual(
                        (identity_row["device"], identity_row["inode"]),
                        (expected_stat.st_dev, expected_stat.st_ino),
                    )
        finally:
            if "decoy_fd" in state:
                os.close(state["decoy_fd"])


class SourceIdentityRaceConditionTests(unittest.TestCase):
    """Codex re-review: source identity first-registration must atomically
    insert-or-verify. If a concurrent connection wins the identity INSERT
    (e.g. under a different canonical path -- a hard-link alias) between
    this connection's pre-check read and its own write transaction, the
    conditional 'INSERT ... WHERE NOT EXISTS' becomes a silent no-op --
    durable identity must be reread inside the same transaction and a
    mismatch rejected, not treated as a successful registration."""

    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.control_db_path = Path(self._tmp.name) / "control-plane.db"
        self.control_conn = connect(self.control_db_path)
        self.addCleanup(self.control_conn.close)
        self.source_db_path = Path(self._tmp.name) / "kanban.db"
        _make_source_db(
            self.source_db_path,
            rows=[(1, "task-1", None, "task.created", "{}", "2026-01-01T00:00:00+00:00")],
        )

    def test_concurrent_winner_with_different_canonical_path_is_rejected(self):
        adapter = KanbanTailAdapter(self.source_db_path, self.control_conn)

        def _pre_check_with_concurrent_winner():
            # Simulate a second connection winning the identity INSERT for
            # this source between this connection's own pre-check and its
            # own write transaction -- deterministically, rather than
            # relying on real thread timing.
            racing_conn = connect(self.control_db_path)
            try:
                racing_conn.execute(
                    "INSERT INTO source_identity (source, canonical_path, device, inode, recorded_at) "
                    "VALUES ('kanban', '/tmp/hard-link-alias/kanban.db', 999999, 888888, "
                    "'2026-01-01T00:00:00+00:00')"
                )
            finally:
                racing_conn.close()
            return None  # this connection's own pre-check still saw "no row yet"

        adapter._read_existing_source_identity = _pre_check_with_concurrent_winner
        with self.assertRaises(SourceIdentityMismatchError):
            adapter.tail_once(policy_version="v1", actor="observer", correlation_id="corr-1")

        # The racing connection's row remains durable (append-only); this
        # connection's own observed identity must never be silently accepted
        # over it after its own INSERT no-oped.
        row = self.control_conn.execute(
            "SELECT canonical_path FROM source_identity WHERE source = 'kanban'"
        ).fetchone()
        self.assertEqual(row["canonical_path"], "/tmp/hard-link-alias/kanban.db")
        self.assertEqual(ObservationLedger(self.control_conn).count(), 0)

    def test_real_concurrent_hard_link_first_registration_is_insert_or_verify(self):
        """Both contenders see no identity before either obtains the write lock.

        The winner may register either hard-link locator, but the loser must
        re-read that persistent row in its transaction and fail -- it cannot
        treat its no-op conditional INSERT as successful registration.
        """
        alias_path = Path(self._tmp.name) / "kanban-hard-link.db"
        os.link(self.source_db_path, alias_path)
        precheck_barrier = threading.Barrier(2)
        errors: list[BaseException] = []
        results = []
        real_precheck = KanbanTailAdapter._read_existing_source_identity

        def synchronized_precheck(adapter_self):
            row = real_precheck(adapter_self)
            precheck_barrier.wait(timeout=10)
            return row

        def run(source_path: Path, correlation_id: str):
            conn = connect(self.control_db_path)
            try:
                adapter = KanbanTailAdapter(source_path, conn)
                results.append(
                    adapter.tail_once(policy_version="v1", actor="observer", correlation_id=correlation_id)
                )
            except BaseException as exc:  # Assert exact outcomes below.
                errors.append(exc)
            finally:
                conn.close()

        with mock.patch.object(KanbanTailAdapter, "_read_existing_source_identity", synchronized_precheck):
            first = threading.Thread(target=run, args=(self.source_db_path, "corr-original"))
            second = threading.Thread(target=run, args=(alias_path, "corr-alias"))
            first.start()
            second.start()
            first.join(timeout=15)
            second.join(timeout=15)

        self.assertFalse(first.is_alive(), "original-path contender did not finish")
        self.assertFalse(second.is_alive(), "hard-link contender did not finish")
        self.assertEqual(len(results), 1)
        self.assertEqual(len(errors), 1)
        self.assertIsInstance(errors[0], SourceIdentityMismatchError)
        identity = self.control_conn.execute(
            "SELECT canonical_path, device, inode FROM source_identity WHERE source = 'kanban'"
        ).fetchone()
        self.assertIn(identity["canonical_path"], {str(self.source_db_path), str(alias_path)})
        self.assertEqual((identity["device"], identity["inode"]), (self.source_db_path.stat().st_dev, self.source_db_path.stat().st_ino))
        self.assertEqual(ObservationLedger(self.control_conn).count(), 1)


def _append_source_rows_for_identity_test(path: Path, rows: list[tuple]) -> None:
    conn = sqlite3.connect(str(path))
    try:
        conn.executemany(
            "INSERT INTO task_events (id, task_id, run_id, kind, payload, created_at) VALUES (?, ?, ?, ?, ?, ?)",
            rows,
        )
        conn.commit()
    finally:
        conn.close()


class SourceContractOrderingTests(unittest.TestCase):
    """Codex re-review Finding B: source-contract validation must happen
    before source identity (or any other control-plane write)."""

    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.control_conn = connect(Path(self._tmp.name) / "control-plane.db")
        self.addCleanup(self.control_conn.close)
        self.source_db_path = Path(self._tmp.name) / "kanban.db"

    def test_malformed_source_contract_is_rejected_before_identity_write(self):
        conn = sqlite3.connect(str(self.source_db_path))
        conn.execute(
            # run_id column missing entirely: violates the source contract.
            "CREATE TABLE task_events (id INTEGER PRIMARY KEY, task_id TEXT NOT NULL, kind TEXT NOT NULL, "
            "payload TEXT NOT NULL, created_at TEXT NOT NULL)"
        )
        conn.commit()
        conn.close()
        adapter = KanbanTailAdapter(self.source_db_path, self.control_conn)
        with self.assertRaises(SourceContractError):
            adapter.tail_once(policy_version="v1", actor="observer", correlation_id="corr-1")

        # Fail closed BEFORE any control-plane write: physical source identity
        # must never be recorded for a source that never passed the contract
        # check, since source_identity is append-only and could never be
        # corrected afterward.
        identity_count = self.control_conn.execute("SELECT COUNT(*) FROM source_identity").fetchone()[0]
        self.assertEqual(identity_count, 0)
        self.assertEqual(ObservationLedger(self.control_conn).count(), 0)
        cursor_count = self.control_conn.execute("SELECT COUNT(*) FROM source_cursors").fetchone()[0]
        self.assertEqual(cursor_count, 0)

    def test_malformed_run_id_row_value_is_rejected_without_coercion(self):
        from orchestrator.control_plane import kanban_tail as kt

        with self.assertRaises(SourceContractError):
            kt._validate_row((1, "t-1", 42, "task.created", "{}", "now"))


class ConsumedPrefixDigestTests(unittest.TestCase):
    """Codex re-review Finding C: physical device/inode identity plus a
    single anchor-row content check is insufficient — an in-place overwrite
    (same inode) of an already-consumed row *earlier* than the cursor's
    anchor row must also be detected, by recomputing a deterministic digest
    of the whole consumed row prefix from the opened readonly source."""

    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.source_db_path = Path(self._tmp.name) / "kanban.db"
        self.control_conn = connect(Path(self._tmp.name) / "control-plane.db")
        self.addCleanup(self.control_conn.close)
        _make_source_db(
            self.source_db_path,
            rows=[
                (1, "task-1", None, "task.created", '{"a": 1}', "2026-01-01T00:00:00+00:00"),
                (2, "task-1", "run-1", "task.status_changed", '{"a": 2}', "2026-01-01T00:01:00+00:00"),
            ],
        )

    def test_same_inode_earlier_row_tamper_with_anchor_preserved_is_detected(self):
        adapter = KanbanTailAdapter(self.source_db_path, self.control_conn)
        adapter.tail_once(policy_version="v1", actor="observer", correlation_id="corr-1")
        self.assertEqual(adapter.current_cursor(), 2)

        # In-place tamper (same file, same inode) of an EARLIER already-
        # consumed row (id=1), leaving the anchor row (id=2, the current
        # cursor) byte-for-byte unchanged — an anchor-only check alone
        # cannot catch this.
        source_conn = sqlite3.connect(str(self.source_db_path))
        try:
            source_conn.execute(
                "UPDATE task_events SET payload = '{\"tampered\": true}' WHERE id = 1"
            )
            source_conn.commit()
        finally:
            source_conn.close()

        with self.assertRaises(SourceIdentityMismatchError):
            adapter.tail_once(policy_version="v1", actor="observer", correlation_id="corr-2")
        # Fail closed: cursor and observation count untouched.
        self.assertEqual(adapter.current_cursor(), 2)
        self.assertEqual(ObservationLedger(self.control_conn).count(), 2)

    def test_deleted_earlier_row_with_anchor_preserved_is_detected(self):
        adapter = KanbanTailAdapter(self.source_db_path, self.control_conn)
        adapter.tail_once(policy_version="v1", actor="observer", correlation_id="corr-1")

        source_conn = sqlite3.connect(str(self.source_db_path))
        try:
            source_conn.execute("DELETE FROM task_events WHERE id = 1")
            source_conn.commit()
        finally:
            source_conn.close()

        with self.assertRaises(SourceIdentityMismatchError):
            adapter.tail_once(policy_version="v1", actor="observer", correlation_id="corr-2")
        self.assertEqual(ObservationLedger(self.control_conn).count(), 2)

    def test_hard_link_path_change_with_identical_inode_is_detected(self):
        adapter = KanbanTailAdapter(self.source_db_path, self.control_conn)
        adapter.tail_once(policy_version="v1", actor="observer", correlation_id="corr-1")

        alias_path = Path(self._tmp.name) / "kanban-alias.db"
        os.link(self.source_db_path, alias_path)
        aliased_adapter = KanbanTailAdapter(alias_path, self.control_conn)
        with self.assertRaises(SourceIdentityMismatchError):
            aliased_adapter.tail_once(policy_version="v1", actor="observer", correlation_id="corr-2")
        self.assertEqual(ObservationLedger(self.control_conn).count(), 2)

    def test_consumed_prefix_digest_recomputes_correctly_across_many_batches(self):
        # Functional proof the streaming/iterative recompute is correct over
        # more rows than a single batch, not just a 1-2 row toy case.
        rows = [
            (i, f"task-{i}", None, "task.created", "{}", f"2026-01-01T01:{i:02d}:00+00:00")
            for i in range(3, 41)
        ]
        _append_source_rows_for_identity_test(self.source_db_path, rows=rows)
        adapter = KanbanTailAdapter(self.source_db_path, self.control_conn, batch_size=5)
        for i in range(10):
            adapter.tail_once(policy_version="v1", actor="observer", correlation_id=f"corr-{i}")
        self.assertEqual(adapter.current_cursor(), 40)
        self.assertEqual(ObservationLedger(self.control_conn).count(), 40)

        # One more no-op cycle: the just-recomputed full 40-row prefix digest
        # must still verify cleanly against durable evidence.
        result = adapter.tail_once(policy_version="v1", actor="observer", correlation_id="corr-final")
        self.assertEqual(result.rows_read, 0)


class MonotonicCursorRaceTests(unittest.TestCase):
    """Finding 8: cursor advances must stay monotonic even under a genuine two-connection race."""

    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.control_db_path = Path(self._tmp.name) / "control-plane.db"
        self.source_db_path = Path(self._tmp.name) / "kanban.db"
        _make_source_db(
            self.source_db_path,
            rows=[
                (i, f"task-{i}", None, "task.created", "{}", f"2026-01-01T00:{i:02d}:00+00:00")
                for i in range(1, 11)
            ],
        )

    def test_two_real_connections_racing_never_regress_the_cursor(self):
        # sqlite3 connections are only usable from the thread that created
        # them (absent check_same_thread=False, which this codebase doesn't
        # use); each racing writer must open — and close — its own
        # connection from inside its own thread, or the "race" would just
        # raise ProgrammingError before ever touching the database.
        barrier = threading.Barrier(2)
        errors: list[Exception] = []

        def run(actor, batch_size):
            conn = connect(self.control_db_path)
            try:
                adapter = KanbanTailAdapter(self.source_db_path, conn, batch_size=batch_size)
                barrier.wait(timeout=5)
                adapter.tail_once(policy_version="v1", actor=actor, correlation_id="corr")
            except Exception as exc:  # noqa: BLE001 - captured for the assertion below
                errors.append(exc)
            finally:
                conn.close()

        t_a = threading.Thread(target=run, args=("observer-a", 10))
        t_b = threading.Thread(target=run, args=("observer-b", 3))
        t_a.start()
        t_b.start()
        t_a.join(timeout=10)
        t_b.join(timeout=10)

        # Racing writers under BEGIN IMMEDIATE serialize rather than corrupt;
        # a busy/locked retry is an acceptable outcome, silent cursor
        # regression is not.
        for exc in errors:
            if not isinstance(exc, sqlite3.OperationalError):
                raise exc

        verify_conn = connect(self.control_db_path)
        try:
            final_cursor = int(
                verify_conn.execute(
                    "SELECT cursor_value FROM source_cursors WHERE source = 'kanban'"
                ).fetchone()[0]
            )
        finally:
            verify_conn.close()
        self.assertGreaterEqual(final_cursor, 3)  # never regresses below either writer's own progress

    def test_set_cursor_upsert_never_regresses_within_one_connection(self):
        conn = connect(self.control_db_path)
        self.addCleanup(conn.close)
        adapter = KanbanTailAdapter(self.source_db_path, conn, batch_size=10)
        adapter.tail_once(policy_version="v1", actor="observer", correlation_id="corr-1")
        self.assertEqual(adapter.current_cursor(), 10)

        # Directly attempt to move the cursor backward the same way a stale
        # racing writer's upsert would; the monotonic CASE must refuse it.
        from orchestrator.control_plane.db import transaction

        with transaction(conn):
            adapter._set_cursor(conn, 3, now="2026-01-01T00:00:00+00:00", consumed_digest="bogus")
        self.assertEqual(adapter.current_cursor(), 10)


class ConcurrentNoOpTailingTests(unittest.TestCase):
    """Finding 3: a genuinely empty batch (no rows past this connection's own
    cursor_before) must still report the cursor as it stands *after* a
    concurrent writer's commit, not the possibly-stale value this connection
    captured before its (empty) read — and must not itself perform any
    write, so source_cursors.updated_at is left exactly as the concurrent
    writer set it."""

    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.source_db_path = Path(self._tmp.name) / "kanban.db"
        self.control_db_path = Path(self._tmp.name) / "control-plane.db"
        _make_source_db(self.source_db_path, rows=[])

    def test_reread_persisted_cursor_reflects_concurrent_advance_without_writing(self):
        conn_b = connect(self.control_db_path)
        self.addCleanup(conn_b.close)
        adapter_b = KanbanTailAdapter(self.source_db_path, conn_b, source="kanban")

        real_get_cursor = KanbanTailAdapter._get_cursor
        call_count = {"n": 0}
        outer = self

        def racing_get_cursor(adapter_self):
            call_count["n"] += 1
            if call_count["n"] == 2:
                # Between adapter_b's first cursor read (which found the
                # batch empty) and this reread, a fully independent
                # connection races in: a new row appears in the source and
                # gets tailed+committed through its own adapter, advancing
                # the durable cursor and updated_at.
                _append_source_rows_for_identity_test(
                    outer.source_db_path,
                    rows=[(1, "task-1", None, "task.created", "{}", "2026-01-01T00:00:00+00:00")],
                )
                conn_a = connect(outer.control_db_path)
                try:
                    KanbanTailAdapter(outer.source_db_path, conn_a, source="kanban").tail_once(
                        policy_version="v1",
                        actor="observer-a",
                        correlation_id="corr-a",
                        now="2026-01-01T00:05:00+00:00",
                    )
                finally:
                    conn_a.close()
            return real_get_cursor(adapter_self)

        with mock.patch.object(KanbanTailAdapter, "_get_cursor", racing_get_cursor):
            result = adapter_b.tail_once(
                policy_version="v1", actor="observer-b", correlation_id="corr-b", now="2026-01-01T00:00:01+00:00"
            )

        self.assertEqual(result.rows_read, 0)
        self.assertEqual(result.cursor_before, 0)
        # Reflects connection A's concurrent commit, not the stale value
        # captured before A raced in.
        self.assertEqual(result.cursor_after, 1)

        row = conn_b.execute(
            "SELECT cursor_value, updated_at FROM source_cursors WHERE source = 'kanban'"
        ).fetchone()
        self.assertEqual(row["cursor_value"], "1")
        # Set only by connection A's real write; adapter_b's no-op path never
        # touched it.
        self.assertEqual(row["updated_at"], "2026-01-01T00:05:00+00:00")


class FailureInjectionTests(unittest.TestCase):
    """Finding 8: a mid-batch failure must roll back atomically, no partial cursor/observation state."""

    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.source_db_path = Path(self._tmp.name) / "kanban.db"
        self.control_conn = connect(Path(self._tmp.name) / "control-plane.db")
        self.addCleanup(self.control_conn.close)
        _make_source_db(
            self.source_db_path,
            rows=[
                (1, "task-1", None, "task.created", "{}", "2026-01-01T00:00:00+00:00"),
                (2, "task-1", None, "task.status_changed", "{}", "2026-01-01T00:01:00+00:00"),
                (3, "task-2", None, "task.created", "{}", "2026-01-01T00:02:00+00:00"),
            ],
        )

    def test_failure_after_partial_ledger_writes_rolls_back_whole_batch(self):
        adapter = KanbanTailAdapter(self.source_db_path, self.control_conn, batch_size=10)

        real_record = ObservationLedger.record
        call_count = {"n": 0}

        def flaky_record(self, *args, **kwargs):
            call_count["n"] += 1
            if call_count["n"] == 3:
                raise RuntimeError("injected failure after 2 successful writes")
            return real_record(self, *args, **kwargs)

        with mock.patch.object(ObservationLedger, "record", flaky_record):
            with self.assertRaises(RuntimeError):
                adapter.tail_once(policy_version="v1", actor="observer", correlation_id="corr-1")

        # Whole-batch rollback: none of the 2 rows that succeeded before the
        # injected failure were left committed, and the cursor never moved.
        self.assertEqual(ObservationLedger(self.control_conn).count(), 0)
        self.assertEqual(adapter.current_cursor(), 0)

    def test_retry_after_injected_failure_recovers_cleanly(self):
        adapter = KanbanTailAdapter(self.source_db_path, self.control_conn, batch_size=10)

        real_record = ObservationLedger.record
        call_count = {"n": 0}

        def fail_once(self, *args, **kwargs):
            call_count["n"] += 1
            if call_count["n"] == 2:
                raise RuntimeError("injected failure")
            return real_record(self, *args, **kwargs)

        with mock.patch.object(ObservationLedger, "record", fail_once):
            with self.assertRaises(RuntimeError):
                adapter.tail_once(policy_version="v1", actor="observer", correlation_id="corr-1")

        result = adapter.tail_once(policy_version="v1", actor="observer", correlation_id="corr-2")
        self.assertEqual(result.appended, 3)
        self.assertEqual(adapter.current_cursor(), 3)


if __name__ == "__main__":
    unittest.main()
