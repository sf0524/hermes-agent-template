import json
import sqlite3
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path

from orchestrator.audit import AuditLog
from orchestrator.db import connect, transaction
from orchestrator.health import REDACTED_VALUE
from orchestrator.human_owner import HumanOwnerGrants, canonical_payload_digest


class HumanOwnerGrantsTests(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.db_path = Path(self._tmp.name) / "state.db"
        self.conn = connect(self.db_path)
        self.addCleanup(self.conn.close)
        self.grants = HumanOwnerGrants(self.conn)

    def _consume_via_intent(self, grant_id, *, idempotency_key, now=None):
        """The only way a grant is ever consumed as of schema.py's v6
        migration: inserting a matching ``action_intents`` row. The
        ``action_intents_consumes_grant`` AFTER INSERT trigger derives
        ``consumed_at``/``consumed_by_idempotency_key`` from that row
        automatically -- see action_gateway.py's ``_record_intent`` for the
        real production path this mirrors at the schema level."""
        moment = now or datetime.now(timezone.utc).isoformat()
        grant = self.grants.get(grant_id)
        with transaction(self.conn) as conn:
            conn.execute(
                """
                INSERT INTO action_intents
                    (idempotency_key, consumer, action_type, target_scope, subject, payload, grant_id,
                     status, causal_event_id, created_at, updated_at, dispatched_at)
                VALUES (?, 'test-consumer', ?, ?, ?, '{}', ?, 'pending', NULL, ?, ?, NULL)
                """,
                (idempotency_key, grant.action_type, grant.scope, grant.subject, grant_id, moment, moment),
            )
        return self.grants.get(grant_id)

    def test_record_grant_is_unconsumed_on_creation(self):
        grant = self.grants.record(
            grant_id="g1",
            scope="github:acme/widgets",
            subject="pr-42",
            action_type="github_comment",
            payload_digest=canonical_payload_digest({}),
            granted_by="owner@example.com",
        )
        self.assertEqual(grant.scope, "github:acme/widgets")
        self.assertEqual(grant.subject, "pr-42")
        self.assertIsNone(grant.consumed_at)
        self.assertIsNone(grant.consumed_by_idempotency_key)

    def test_get_unknown_grant_returns_none(self):
        self.assertIsNone(self.grants.get("nope"))

    def test_scope_is_immutable_at_the_db_level(self):
        self.grants.record(
            grant_id="g1", scope="github:acme/widgets", subject="pr-42",
            action_type="github_comment", payload_digest=canonical_payload_digest({}),
            granted_by="owner@example.com",
        )
        with self.assertRaises(sqlite3.DatabaseError):
            self.conn.execute(
                "UPDATE human_owner_grants SET scope = 'github:other/repo' WHERE grant_id = 'g1'"
            )

    def test_subject_is_immutable_at_the_db_level(self):
        self.grants.record(
            grant_id="g1", scope="github:acme/widgets", subject="pr-42",
            action_type="github_comment", payload_digest=canonical_payload_digest({}),
            granted_by="owner@example.com",
        )
        with self.assertRaises(sqlite3.DatabaseError):
            self.conn.execute(
                "UPDATE human_owner_grants SET subject = 'pr-999' WHERE grant_id = 'g1'"
            )

    def test_action_type_is_immutable_at_the_db_level(self):
        self.grants.record(
            grant_id="g1", scope="github:acme/widgets", subject="pr-42",
            action_type="github_comment", payload_digest=canonical_payload_digest({}),
            granted_by="owner@example.com",
        )
        with self.assertRaises(sqlite3.DatabaseError):
            self.conn.execute(
                "UPDATE human_owner_grants SET action_type = 'github_merge' WHERE grant_id = 'g1'"
            )

    def test_delete_is_forbidden_at_the_db_level(self):
        self.grants.record(
            grant_id="g1", scope="github:acme/widgets", subject="pr-42",
            action_type="github_comment", payload_digest=canonical_payload_digest({}),
            granted_by="owner@example.com",
        )
        with self.assertRaises(sqlite3.DatabaseError):
            self.conn.execute("DELETE FROM human_owner_grants WHERE grant_id = 'g1'")

    def test_insert_derived_consumption_marks_grant_consumed(self):
        """Inserting the action_intents row is itself what consumes the
        grant -- there is no separate consume step to forget or get out of
        sync."""
        self.grants.record(
            grant_id="g1", scope="github:acme/widgets", subject="pr-42",
            action_type="github_comment", payload_digest=canonical_payload_digest({}),
            granted_by="owner@example.com",
        )
        consumed = self._consume_via_intent("g1", idempotency_key="intent-1", now="2026-01-01T00:00:00+00:00")
        self.assertEqual(consumed.consumed_at, "2026-01-01T00:00:00+00:00")
        self.assertEqual(consumed.consumed_by_idempotency_key, "intent-1")

    def test_raw_update_cannot_orphan_consume_a_grant(self):
        """The orphan-consumption gap this migration closes: directly
        UPDATE-ing human_owner_grants' consumption columns, with no
        corresponding action_intents row, must be rejected at the DB level
        -- not just discouraged by convention."""
        self.grants.record(
            grant_id="g1", scope="github:acme/widgets", subject="pr-42",
            action_type="github_comment", payload_digest=canonical_payload_digest({}),
            granted_by="owner@example.com",
        )
        with self.assertRaises(sqlite3.DatabaseError):
            self.conn.execute(
                "UPDATE human_owner_grants SET consumed_at = ?, consumed_by_idempotency_key = ? "
                "WHERE grant_id = 'g1'",
                ("2026-01-01T00:00:00+00:00", "no-such-intent"),
            )
        grant = self.grants.get("g1")
        self.assertIsNone(grant.consumed_at)
        self.assertIsNone(grant.consumed_by_idempotency_key)

    def test_consumption_fields_are_immutable_once_set(self):
        self.grants.record(
            grant_id="g1", scope="github:acme/widgets", subject="pr-42",
            action_type="github_comment", payload_digest=canonical_payload_digest({}),
            granted_by="owner@example.com",
        )
        self._consume_via_intent("g1", idempotency_key="intent-1", now="2026-01-01T00:00:00+00:00")
        with self.assertRaises(sqlite3.DatabaseError):
            self.conn.execute(
                "UPDATE human_owner_grants SET consumed_by_idempotency_key = 'intent-2' WHERE grant_id = 'g1'"
            )
        with self.assertRaises(sqlite3.DatabaseError):
            self.conn.execute(
                "UPDATE human_owner_grants SET consumed_at = NULL, consumed_by_idempotency_key = NULL "
                "WHERE grant_id = 'g1'"
            )
        grant = self.grants.get("g1")
        self.assertEqual(grant.consumed_by_idempotency_key, "intent-1")

    def test_second_intent_for_an_already_consumed_grant_is_rejected(self):
        self.grants.record(
            grant_id="g1", scope="github:acme/widgets", subject="pr-42",
            action_type="github_comment", payload_digest=canonical_payload_digest({}),
            granted_by="owner@example.com",
        )
        self._consume_via_intent("g1", idempotency_key="intent-1")
        with self.assertRaises(sqlite3.DatabaseError):
            self._consume_via_intent("g1", idempotency_key="intent-2")
        grant = self.grants.get("g1")
        self.assertEqual(grant.consumed_by_idempotency_key, "intent-1")

    def test_grants_expose_no_standalone_consume_method(self):
        """Consuming a grant must always atomically persist the exact action
        intent it authorizes (see action_gateway._record_intent). A public
        HumanOwnerGrants.consume() that only flips consumed_at /
        consumed_by_idempotency_key with no corresponding action_intents row
        is exactly the orphan-consumption gap flagged in review: it must not
        be reachable on the public class at all."""
        self.grants.record(
            grant_id="g1", scope="github:acme/widgets", subject="pr-42",
            action_type="github_comment", payload_digest=canonical_payload_digest({}),
            granted_by="owner@example.com",
        )
        self.assertFalse(
            hasattr(self.grants, "consume"),
            "HumanOwnerGrants must not expose an API path that consumes a "
            "grant without atomically recording the action intent it "
            "authorizes",
        )

    def test_grant_and_consume_survive_a_reconnect(self):
        self.grants.record(
            grant_id="g1", scope="github:acme/widgets", subject="pr-42",
            action_type="github_comment", payload_digest=canonical_payload_digest({}),
            granted_by="owner@example.com",
        )
        self._consume_via_intent("g1", idempotency_key="intent-1")
        self.conn.close()

        reconnected = connect(self.db_path)
        self.addCleanup(reconnected.close)
        resumed = HumanOwnerGrants(reconnected)
        grant = resumed.get("g1")
        self.assertIsNotNone(grant.consumed_at)
        self.assertEqual(grant.consumed_by_idempotency_key, "intent-1")


    def test_grant_recorded_audit_redacts_a_secret_pasted_into_scope(self):
        """scope/subject/action_type are caller-supplied free text -- review
        flagged that ``grant.recorded`` persists them verbatim into the
        durable audit log. A secret pasted into one of them (e.g. a token
        tacked onto a scope string) must never survive into the audit
        trail."""
        token = "grant-scope-secret-789"
        self.grants.record(
            grant_id="g1",
            scope=f"github:acme/widgets?token={token}",
            subject="pr-42",
            action_type="github_comment",
            payload_digest=canonical_payload_digest({}),
            granted_by="owner@example.com",
        )
        recorded = [e for e in AuditLog(self.conn).all() if e.action == "grant.recorded"]
        self.assertEqual(len(recorded), 1)
        detail_str = json.dumps(recorded[0].detail)
        self.assertNotIn(token, detail_str)
        self.assertIn(REDACTED_VALUE, detail_str)


if __name__ == "__main__":
    unittest.main()
