import json
import os
import sqlite3
import tempfile
import threading
import unittest
from pathlib import Path
from unittest import mock

from orchestrator import schema
from orchestrator.audit import AuditLog
from orchestrator.db import connect, transaction
from orchestrator.health import REDACTED_VALUE
from orchestrator.home import InvalidHermesHomeError, get_hermes_home, orchestrator_state_db_path
from orchestrator.human_owner import canonical_payload_digest


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
                self.assertEqual(versions, [1, 2, 3, 4, 5, 6, 7])
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


class V6LegacyGrantIntentIntegrityTests(unittest.TestCase):
    """Exercises schema.py's v6 preflight (``_v6_legacy_integrity_guard``)
    against pre-v6 databases -- data that predates the v6 triggers and so
    could never have been produced under them. The two existing checks catch
    an orphan-consumed grant and a grant referenced by more than one
    ``action_intents`` row; these tests cover the two gaps review flagged:
    an ``action_intents`` row naming a grant that's still unconsumed, and a
    legacy consumed pair whose ``human_owner_grants.consumed_at`` doesn't
    match the referencing ``action_intents.created_at``."""

    def _build_pre_v6_db(self, tmp, populate) -> Path:
        """Build a DB with only migrations 1-5 applied and ``populate`` run
        against it, all while migration 6 is patched out of existence -- so
        the legacy rows it inserts are genuinely pre-v6 (no v6 trigger has
        ever touched them), not rows that merely look that way because they
        were inserted directly after v6's triggers were already live."""
        db_path = Path(tmp) / "state.db"
        pre_v6_migrations = [m for m in schema.MIGRATIONS if m[0] < 6]
        with mock.patch.object(schema, "MIGRATIONS", pre_v6_migrations):
            conn = connect(db_path)
            try:
                with transaction(conn) as c:
                    populate(c)
            finally:
                conn.close()
        return db_path

    def _insert_legacy_grant(self, conn, *, grant_id, consumed_at=None, consumed_by_idempotency_key=None):
        conn.execute(
            """
            INSERT INTO human_owner_grants
                (grant_id, scope, subject, action_type, payload_digest, granted_by, granted_at,
                 consumed_at, consumed_by_idempotency_key)
            VALUES (?, 'github:acme/widgets', 'pr-1', 'github_comment', ?, 'owner@example.com',
                    '2026-01-01T00:00:00+00:00', ?, ?)
            """,
            (grant_id, canonical_payload_digest({}), consumed_at, consumed_by_idempotency_key),
        )

    def _insert_legacy_intent(self, conn, *, idempotency_key, grant_id, created_at):
        conn.execute(
            """
            INSERT INTO action_intents
                (idempotency_key, consumer, action_type, target_scope, subject, payload, grant_id,
                 status, causal_event_id, created_at, updated_at, dispatched_at)
            VALUES (?, 'runtime_orchestrator', 'github_comment', 'github:acme/widgets', 'pr-1', '{}', ?,
                    'dispatched', NULL, ?, ?, ?)
            """,
            (idempotency_key, grant_id, created_at, created_at, created_at),
        )

    def test_legacy_intent_referencing_a_still_unconsumed_grant_is_rejected(self):
        """Before v6, an application bug (or a hand-rolled fixture) could
        insert an ``action_intents`` row without ever flipping the grant's
        ``consumed_at`` -- the two-step convention v6 replaces. Because
        ``action_intents_consumes_grant`` only fires on new inserts, this
        legacy row would never get backfilled, so migrating it forward as-is
        would leave an intent permanently pointing at an unconsumed grant."""
        with tempfile.TemporaryDirectory() as tmp:

            def populate(c):
                self._insert_legacy_grant(c, grant_id="g1")
                self._insert_legacy_intent(
                    c, idempotency_key="intent-1", grant_id="g1", created_at="2026-01-01T00:00:00+00:00"
                )

            db_path = self._build_pre_v6_db(tmp, populate)

            with self.assertRaises(schema.LegacyIntegrityViolation):
                connect(db_path)

    def test_legacy_consumed_pair_with_mismatched_timestamps_is_rejected(self):
        """A legacy grant/intent pair whose idempotency keys line up (so the
        existing orphan check is satisfied) but whose ``consumed_at`` doesn't
        equal the referencing intent's ``created_at`` could never have been
        produced by the v6 trigger, which derives one from the other -- it
        must be rejected, not locked in as if it had been."""
        with tempfile.TemporaryDirectory() as tmp:

            def populate(c):
                self._insert_legacy_grant(
                    c,
                    grant_id="g1",
                    consumed_at="2026-01-01T00:00:05+00:00",
                    consumed_by_idempotency_key="intent-1",
                )
                self._insert_legacy_intent(
                    c, idempotency_key="intent-1", grant_id="g1", created_at="2026-01-01T00:00:00+00:00"
                )

            db_path = self._build_pre_v6_db(tmp, populate)

            with self.assertRaises(schema.LegacyIntegrityViolation):
                connect(db_path)

    def test_valid_legacy_consumed_pair_migrates_deterministically(self):
        """A legacy pair that *does* satisfy every v6 invariant -- exactly
        one intent, matching idempotency key, matching timestamp -- must
        still migrate cleanly. The preflight must reject only genuine
        violations, not weaken or over-reject valid history."""
        with tempfile.TemporaryDirectory() as tmp:

            def populate(c):
                self._insert_legacy_grant(
                    c,
                    grant_id="g1",
                    consumed_at="2026-01-01T00:00:00+00:00",
                    consumed_by_idempotency_key="intent-1",
                )
                self._insert_legacy_intent(
                    c, idempotency_key="intent-1", grant_id="g1", created_at="2026-01-01T00:00:00+00:00"
                )

            db_path = self._build_pre_v6_db(tmp, populate)

            migrated = connect(db_path)
            try:
                versions = [
                    row[0] for row in migrated.execute("SELECT version FROM schema_migrations ORDER BY version")
                ]
                self.assertIn(6, versions)
            finally:
                migrated.close()


class V7LegacyGrantIntentCoordinateIntegrityTests(unittest.TestCase):
    """Exercises schema.py's v7 preflight against pre-v7 (v6-era) databases.

    v6's ``action_intents_requires_fresh_grant`` trigger only checked that
    *some* unconsumed ``human_owner_grants`` row existed for ``grant_id`` --
    it never checked that the grant's own scope/subject/action_type/payload
    actually matched the inserted intent. So under v6 alone (before v7's
    rewritten trigger closes this), an old intent/grant pair with mismatched
    coordinates could be inserted and consumed. Migrating such a pair forward
    to v7 must be rejected, not silently locked in as reconcilable/
    dispatchable history."""

    def _build_pre_v7_db(self, tmp, populate) -> Path:
        """Build a DB with only migrations 1-6 applied (v6's coordinate-blind
        trigger is live; v7's coordinate-checking one is not), then run
        ``populate`` against it -- so the legacy pair it inserts is genuinely
        pre-v7, produced under v6's own rules, not merely shaped to look that
        way after v7 already existed."""
        db_path = Path(tmp) / "state.db"
        pre_v7_migrations = [m for m in schema.MIGRATIONS if m[0] < 7]
        with mock.patch.object(schema, "MIGRATIONS", pre_v7_migrations):
            conn = connect(db_path)
            try:
                with transaction(conn) as c:
                    populate(c)
            finally:
                conn.close()
        return db_path

    def _insert_legacy_grant(
        self,
        conn,
        *,
        grant_id,
        scope="github:acme/widgets",
        subject="pr-42",
        action_type="github_comment",
        payload_digest=None,
    ):
        conn.execute(
            """
            INSERT INTO human_owner_grants
                (grant_id, scope, subject, action_type, payload_digest, granted_by, granted_at,
                 consumed_at, consumed_by_idempotency_key)
            VALUES (?, ?, ?, ?, ?, 'owner@example.com', '2026-01-01T00:00:00+00:00', NULL, NULL)
            """,
            (grant_id, scope, subject, action_type, payload_digest or canonical_payload_digest({"body": "hi"})),
        )

    def _insert_legacy_intent_under_v6(
        self,
        conn,
        *,
        idempotency_key,
        grant_id,
        action_type="github_comment",
        target_scope="github:acme/widgets",
        subject="pr-42",
        payload='{"body": "hi"}',
        created_at="2026-01-01T00:00:00+00:00",
    ):
        # Under v6 alone this insert succeeds regardless of whether these
        # coordinates match the named grant's -- only v7's rewritten trigger
        # checks that. v6's own trigger (still live here, since this DB only
        # has migrations 1-6 applied) also consumes the grant as a side
        # effect, exactly as it would for a legitimate pair.
        conn.execute(
            """
            INSERT INTO action_intents
                (idempotency_key, consumer, action_type, target_scope, subject, payload, grant_id,
                 status, causal_event_id, created_at, updated_at, dispatched_at)
            VALUES (?, 'test-consumer', ?, ?, ?, ?, ?, 'dispatched', NULL, ?, ?, ?)
            """,
            (idempotency_key, action_type, target_scope, subject, payload, grant_id, created_at, created_at, created_at),
        )

    def test_legacy_pair_with_mismatched_action_type_is_rejected(self):
        with tempfile.TemporaryDirectory() as tmp:

            def populate(c):
                self._insert_legacy_grant(c, grant_id="g1", action_type="github_comment")
                self._insert_legacy_intent_under_v6(c, idempotency_key="intent-1", grant_id="g1", action_type="github_add_label")

            db_path = self._build_pre_v7_db(tmp, populate)

            with self.assertRaises(schema.LegacyIntegrityViolation):
                connect(db_path)

    def test_legacy_pair_with_mismatched_target_scope_vs_grant_scope_is_rejected(self):
        with tempfile.TemporaryDirectory() as tmp:

            def populate(c):
                self._insert_legacy_grant(c, grant_id="g1", scope="github:acme/widgets")
                self._insert_legacy_intent_under_v6(c, idempotency_key="intent-1", grant_id="g1", target_scope="github:other/repo")

            db_path = self._build_pre_v7_db(tmp, populate)

            with self.assertRaises(schema.LegacyIntegrityViolation):
                connect(db_path)

    def test_legacy_pair_with_mismatched_subject_is_rejected(self):
        with tempfile.TemporaryDirectory() as tmp:

            def populate(c):
                self._insert_legacy_grant(c, grant_id="g1", subject="pr-42")
                self._insert_legacy_intent_under_v6(c, idempotency_key="intent-1", grant_id="g1", subject="pr-999")

            db_path = self._build_pre_v7_db(tmp, populate)

            with self.assertRaises(schema.LegacyIntegrityViolation):
                connect(db_path)

    def test_legacy_pair_with_mismatched_payload_digest_is_rejected(self):
        with tempfile.TemporaryDirectory() as tmp:

            def populate(c):
                self._insert_legacy_grant(c, grant_id="g1", payload_digest=canonical_payload_digest({"body": "hi"}))
                self._insert_legacy_intent_under_v6(
                    c,
                    idempotency_key="intent-1",
                    grant_id="g1",
                    payload='{"body": "a completely different, never-approved body"}',
                )

            db_path = self._build_pre_v7_db(tmp, populate)

            with self.assertRaises(schema.LegacyIntegrityViolation):
                connect(db_path)

    def test_valid_legacy_pair_migrates_deterministically(self):
        """A legacy pair whose coordinates genuinely match the grant it
        consumed -- exactly what v6 was supposed to produce even without v7's
        stricter trigger -- must still migrate cleanly to v7."""
        with tempfile.TemporaryDirectory() as tmp:

            def populate(c):
                self._insert_legacy_grant(c, grant_id="g1")
                self._insert_legacy_intent_under_v6(c, idempotency_key="intent-1", grant_id="g1")

            db_path = self._build_pre_v7_db(tmp, populate)

            migrated = connect(db_path)
            try:
                versions = [
                    row[0] for row in migrated.execute("SELECT version FROM schema_migrations ORDER BY version")
                ]
                self.assertIn(7, versions)
            finally:
                migrated.close()


class ActionIntentsGrantBindingTests(unittest.TestCase):
    """schema.py's v7 ``action_intents_requires_fresh_grant`` trigger
    structurally binds every inserted ``action_intents`` row to the
    ``human_owner_grants`` row it names: ``action_type``, ``target_scope``
    (mapped against the grant's ``scope``), ``subject``, and the payload's
    canonical digest must all equal the grant's immutable fields before the
    insert may consume it. Before v7, the trigger only checked that *some*
    unconsumed grant existed for ``grant_id`` -- a raw insert naming a real,
    unconsumed grant but different coordinates or payload could slip past the
    DB and, via ``action_intents_consumes_grant``, consume a grant it was
    never actually approved against."""

    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.conn = connect(Path(self._tmp.name) / "state.db")
        self.addCleanup(self.conn.close)
        self.conn.execute(
            """
            INSERT INTO human_owner_grants
                (grant_id, scope, subject, action_type, payload_digest, granted_by, granted_at,
                 consumed_at, consumed_by_idempotency_key)
            VALUES ('g1', 'github:acme/widgets', 'pr-42', 'github_comment', ?, 'owner@example.com',
                    '2026-01-01T00:00:00+00:00', NULL, NULL)
            """,
            (canonical_payload_digest({"body": "hi"}),),
        )

    def _insert_intent(
        self,
        *,
        action_type="github_comment",
        target_scope="github:acme/widgets",
        subject="pr-42",
        payload='{"body": "hi"}',
        grant_id="g1",
        idempotency_key="intent-1",
    ):
        self.conn.execute(
            """
            INSERT INTO action_intents
                (idempotency_key, consumer, action_type, target_scope, subject, payload, grant_id,
                 status, causal_event_id, created_at, updated_at, dispatched_at)
            VALUES (?, 'test-consumer', ?, ?, ?, ?, ?, 'pending', NULL,
                    '2026-01-01T00:00:00+00:00', '2026-01-01T00:00:00+00:00', NULL)
            """,
            (idempotency_key, action_type, target_scope, subject, payload, grant_id),
        )

    def _assert_grant_unconsumed(self):
        grant = self.conn.execute(
            "SELECT consumed_at, consumed_by_idempotency_key FROM human_owner_grants WHERE grant_id = 'g1'"
        ).fetchone()
        self.assertIsNone(grant["consumed_at"])
        self.assertIsNone(grant["consumed_by_idempotency_key"])

    def test_matching_coordinates_and_payload_consume_the_grant(self):
        self._insert_intent()
        grant = self.conn.execute(
            "SELECT consumed_at, consumed_by_idempotency_key FROM human_owner_grants WHERE grant_id = 'g1'"
        ).fetchone()
        self.assertIsNotNone(grant["consumed_at"])
        self.assertEqual(grant["consumed_by_idempotency_key"], "intent-1")

    def test_mismatched_action_type_is_rejected_and_does_not_consume_grant(self):
        with self.assertRaises(sqlite3.IntegrityError):
            self._insert_intent(action_type="github_add_label")
        self._assert_grant_unconsumed()

    def test_mismatched_target_scope_is_rejected_and_does_not_consume_grant(self):
        with self.assertRaises(sqlite3.IntegrityError):
            self._insert_intent(target_scope="github:other/repo")
        self._assert_grant_unconsumed()

    def test_mismatched_subject_is_rejected_and_does_not_consume_grant(self):
        with self.assertRaises(sqlite3.IntegrityError):
            self._insert_intent(subject="pr-999")
        self._assert_grant_unconsumed()

    def test_mismatched_payload_is_rejected_and_does_not_consume_grant(self):
        """Same coordinates as the approved grant, but a payload whose digest
        doesn't match what was actually approved -- the DB-level counterpart
        of ``test_a_grant_for_one_payload_does_not_authorize_a_different_payload``
        in test_orchestrator_action_gateway.py."""
        with self.assertRaises(sqlite3.IntegrityError):
            self._insert_intent(payload='{"body": "a completely different, never-approved body"}')
        self._assert_grant_unconsumed()

    def test_unknown_grant_id_is_rejected(self):
        with self.assertRaises(sqlite3.IntegrityError):
            self._insert_intent(grant_id="no-such-grant")
        self._assert_grant_unconsumed()


class ActionIntentsIdentityImmutableTests(unittest.TestCase):
    """schema.py's v7 ``action_intents_identity_immutable`` trigger locks down
    every remaining request-identity column of ``action_intents`` after
    insert -- ``consumer``, ``action_type``, ``target_scope``, ``subject``,
    ``payload``, ``causal_event_id``, ``created_at`` -- complementing v6's
    existing ``grant_id``/``idempotency_key`` immutability triggers (both
    regression-checked here too). ``status``/``updated_at``/``dispatched_at``
    stay lifecycle-writable, per action_gateway.py's claim/finish UPDATEs."""

    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.conn = connect(Path(self._tmp.name) / "state.db")
        self.addCleanup(self.conn.close)
        self.conn.execute(
            """
            INSERT INTO human_owner_grants
                (grant_id, scope, subject, action_type, payload_digest, granted_by, granted_at,
                 consumed_at, consumed_by_idempotency_key)
            VALUES ('g1', 'github:acme/widgets', 'pr-42', 'github_comment', ?, 'owner@example.com',
                    '2026-01-01T00:00:00+00:00', NULL, NULL)
            """,
            (canonical_payload_digest({"body": "hi"}),),
        )
        self.conn.execute(
            """
            INSERT INTO human_owner_grants
                (grant_id, scope, subject, action_type, payload_digest, granted_by, granted_at,
                 consumed_at, consumed_by_idempotency_key)
            VALUES ('g2', 'github:acme/widgets', 'pr-42', 'github_comment', ?, 'owner@example.com',
                    '2026-01-01T00:00:00+00:00', NULL, NULL)
            """,
            (canonical_payload_digest({"body": "hi"}),),
        )
        self.conn.execute(
            """
            INSERT INTO action_intents
                (idempotency_key, consumer, action_type, target_scope, subject, payload, grant_id,
                 status, causal_event_id, created_at, updated_at, dispatched_at)
            VALUES ('intent-1', 'test-consumer', 'github_comment', 'github:acme/widgets', 'pr-42',
                    '{"body": "hi"}', 'g1', 'pending', 'evt-1', '2026-01-01T00:00:00+00:00',
                    '2026-01-01T00:00:00+00:00', NULL)
            """
        )

    def test_consumer_is_immutable(self):
        with self.assertRaises(sqlite3.IntegrityError):
            self.conn.execute(
                "UPDATE action_intents SET consumer = 'someone-else' WHERE idempotency_key = 'intent-1'"
            )

    def test_action_type_is_immutable(self):
        with self.assertRaises(sqlite3.IntegrityError):
            self.conn.execute(
                "UPDATE action_intents SET action_type = 'github_add_label' WHERE idempotency_key = 'intent-1'"
            )

    def test_target_scope_is_immutable(self):
        with self.assertRaises(sqlite3.IntegrityError):
            self.conn.execute(
                "UPDATE action_intents SET target_scope = 'github:other/repo' WHERE idempotency_key = 'intent-1'"
            )

    def test_subject_is_immutable(self):
        with self.assertRaises(sqlite3.IntegrityError):
            self.conn.execute(
                "UPDATE action_intents SET subject = 'pr-999' WHERE idempotency_key = 'intent-1'"
            )

    def test_payload_is_immutable(self):
        with self.assertRaises(sqlite3.IntegrityError):
            self.conn.execute(
                'UPDATE action_intents SET payload = \'{"body": "different"}\' WHERE idempotency_key = \'intent-1\''
            )

    def test_grant_id_is_immutable(self):
        with self.assertRaises(sqlite3.IntegrityError):
            self.conn.execute("UPDATE action_intents SET grant_id = 'g2' WHERE idempotency_key = 'intent-1'")

    def test_idempotency_key_is_immutable(self):
        with self.assertRaises(sqlite3.IntegrityError):
            self.conn.execute(
                "UPDATE action_intents SET idempotency_key = 'intent-2' WHERE idempotency_key = 'intent-1'"
            )

    def test_causal_event_id_is_immutable(self):
        with self.assertRaises(sqlite3.IntegrityError):
            self.conn.execute(
                "UPDATE action_intents SET causal_event_id = 'evt-2' WHERE idempotency_key = 'intent-1'"
            )

    def test_created_at_is_immutable(self):
        with self.assertRaises(sqlite3.IntegrityError):
            self.conn.execute(
                "UPDATE action_intents SET created_at = '2026-01-02T00:00:00+00:00' WHERE idempotency_key = 'intent-1'"
            )

    def test_status_remains_lifecycle_writable(self):
        self.conn.execute(
            "UPDATE action_intents SET status = 'dispatching', updated_at = '2026-01-01T00:00:01+00:00' "
            "WHERE idempotency_key = 'intent-1'"
        )
        row = self.conn.execute(
            "SELECT status, updated_at FROM action_intents WHERE idempotency_key = 'intent-1'"
        ).fetchone()
        self.assertEqual(row["status"], "dispatching")
        self.assertEqual(row["updated_at"], "2026-01-01T00:00:01+00:00")

    def test_dispatched_at_remains_lifecycle_writable(self):
        self.conn.execute(
            "UPDATE action_intents SET status = 'dispatched', dispatched_at = '2026-01-01T00:00:02+00:00' "
            "WHERE idempotency_key = 'intent-1'"
        )
        row = self.conn.execute(
            "SELECT dispatched_at FROM action_intents WHERE idempotency_key = 'intent-1'"
        ).fetchone()
        self.assertEqual(row["dispatched_at"], "2026-01-01T00:00:02+00:00")


class AuditLogRedactionTests(unittest.TestCase):
    """AuditLog.record is the single place every state-changing call in this
    package persists its ``detail`` -- review flagged that several callers
    (human_owner.py's grant.recorded, action_gateway.py's scope-mismatch and
    intent-recorded paths) pass caller-controlled fields straight through
    unredacted. These tests exercise redaction centrally, in ``record``
    itself, rather than trusting every call site to remember to redact."""

    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.conn = connect(Path(self._tmp.name) / "state.db")
        self.addCleanup(self.conn.close)
        self.audit = AuditLog(self.conn)

    def test_record_redacts_bearer_token_nested_inside_detail(self):
        token = "nested-bearer-secret-123"
        entry = self.audit.record(
            actor="test",
            action="unit.test",
            detail={"nested": {"header": f"Authorization: Bearer {token}"}, "safe": "keep-me"},
        )
        detail_str = json.dumps(entry.detail)
        self.assertNotIn(token, detail_str)
        self.assertIn(REDACTED_VALUE, detail_str)
        self.assertEqual(entry.detail["safe"], "keep-me")

    def test_record_redacts_url_userinfo_and_dsn_inside_a_list(self):
        secret = "hunter2"
        entry = self.audit.record(
            actor="test",
            action="unit.test",
            detail={"connections": [f"postgres://dbuser:{secret}@db.internal:5432/app", "not-a-url"]},
        )
        detail_str = json.dumps(entry.detail)
        self.assertNotIn(secret, detail_str)
        self.assertIn("not-a-url", detail_str)

    def test_record_redacts_query_string_secret(self):
        secret = "query-secret-abc"
        entry = self.audit.record(
            actor="test",
            action="unit.test",
            detail={"target_scope": f"github:acme/widgets?token={secret}"},
        )
        detail_str = json.dumps(entry.detail)
        self.assertNotIn(secret, detail_str)
        self.assertIn(REDACTED_VALUE, detail_str)

    def test_redaction_is_applied_to_what_is_actually_persisted_not_just_returned(self):
        secret = "persisted-secret-xyz"
        self.audit.record(actor="test", action="unit.test", detail={"token": secret})
        row = self.conn.execute("SELECT detail FROM audit_log ORDER BY id DESC LIMIT 1").fetchone()
        self.assertNotIn(secret, row["detail"])

    def test_record_redacts_caller_controlled_scalar_fields_before_persisting(self):
        actor_secret = "actor-secret-123"
        action_secret = "action-secret-456"
        subject_secret = "subject-secret-789"
        entry = self.audit.record(
            actor=f"operator Authorization: Bearer {actor_secret}",
            action=f"unit.test?token={action_secret}",
            subject_id=f"subject?token={subject_secret}",
            detail={"nested": {"token": "detail-secret"}, "safe": "keep-me"},
        )
        row = self.conn.execute(
            "SELECT actor, action, subject_id, detail FROM audit_log WHERE id = ?", (entry.id,)
        ).fetchone()
        persisted = json.dumps(dict(row))
        for secret in (actor_secret, action_secret, subject_secret, "detail-secret"):
            self.assertNotIn(secret, persisted)
        self.assertIn(REDACTED_VALUE, persisted)
        self.assertEqual(entry.detail["safe"], "keep-me")

    def test_nonsecret_detail_is_preserved_exactly(self):
        entry = self.audit.record(
            actor="test", action="unit.test", detail={"action_type": "github_comment", "count": 3}
        )
        self.assertEqual(entry.detail, {"action_type": "github_comment", "count": 3})


if __name__ == "__main__":
    unittest.main()
