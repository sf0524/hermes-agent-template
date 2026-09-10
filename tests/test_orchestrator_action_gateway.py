import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from orchestrator.action_gateway import (
    CATEGORY_A_ACTION_TYPES,
    CATEGORY_B_ACTION_SCOPES,
    ActionGateway,
    CategoryAForbiddenError,
    GitHubActionInterface,
    GrantMismatchError,
    IdempotencyKeyConflictError,
    RailwayActionInterface,
    TransportDisabledError,
    UnknownActionTypeError,
    default_transport,
)
from orchestrator.audit import AuditLog
from orchestrator.db import connect, transaction
from orchestrator.health import REDACTED_VALUE
from orchestrator.human_owner import (
    GrantAlreadyConsumedError,
    HumanOwnerGrants,
    canonical_payload_digest,
)


class _SpyTransport:
    def __init__(self):
        self.calls = []

    def __call__(self, intent):
        self.calls.append(intent)


class _TransportPatchMixin:
    def use_transport(self, transport):
        patcher = patch("orchestrator.action_gateway.default_transport", transport)
        patcher.start()
        self.addCleanup(patcher.stop)


class ActionGatewayCategoryATests(_TransportPatchMixin, unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.conn = connect(Path(self._tmp.name) / "state.db")
        self.addCleanup(self.conn.close)
        self.grants = HumanOwnerGrants(self.conn)
        self.transport = _SpyTransport()
        self.use_transport(self.transport)
        self.gateway = ActionGateway(self.conn, grants=self.grants)

    def test_every_category_a_action_type_is_always_rejected(self):
        for action_type in sorted(CATEGORY_A_ACTION_TYPES):
            with self.subTest(action_type=action_type):
                with self.assertRaises(CategoryAForbiddenError):
                    self.gateway.dispatch(
                        idempotency_key=f"cat-a-{action_type}",
                        consumer="runtime_orchestrator",
                        action_type=action_type,
                        target_scope="github:acme/widgets",
                        subject="pr-1",
                        payload={},
                        grant_id="whatever",
                    )
        self.assertEqual(self.transport.calls, [])

    def test_category_a_is_rejected_even_with_a_real_matching_grant(self):
        self.grants.record(
            grant_id="g1", scope="github:acme/widgets", subject="pr-1",
            action_type="kanban_task_write", payload_digest=canonical_payload_digest({}),
            granted_by="owner@example.com",
        )
        with self.assertRaises(CategoryAForbiddenError):
            self.gateway.dispatch(
                idempotency_key="cat-a-with-grant",
                consumer="runtime_orchestrator",
                action_type="kanban_task_write",
                target_scope="github:acme/widgets",
                subject="pr-1",
                payload={},
                grant_id="g1",
            )
        self.assertIsNone(self.gateway.get("cat-a-with-grant"))
        self.assertEqual(self.transport.calls, [])
        grant = self.grants.get("g1")
        self.assertIsNone(grant.consumed_at)  # never touched

    def test_category_a_rejection_is_audited(self):
        with self.assertRaises(CategoryAForbiddenError):
            self.gateway.dispatch(
                idempotency_key="cat-a-audit",
                consumer="runtime_orchestrator",
                action_type="override_role",
                target_scope="github:acme/widgets",
                subject="pr-1",
                payload={},
                grant_id="whatever",
            )
        rejected = [e for e in AuditLog(self.conn).all() if e.action == "action.category_a_rejected"]
        self.assertEqual(len(rejected), 1)


class ActionGatewayDefaultDenyTests(_TransportPatchMixin, unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.conn = connect(Path(self._tmp.name) / "state.db")
        self.addCleanup(self.conn.close)
        self.grants = HumanOwnerGrants(self.conn)
        self.transport = _SpyTransport()
        self.use_transport(self.transport)
        self.gateway = ActionGateway(self.conn, grants=self.grants)

    def test_unknown_action_type_is_rejected_fail_closed(self):
        for action_type in ("github_merge_pr", "railway_deploy", "railway_release", "totally_unheard_of"):
            with self.subTest(action_type=action_type):
                with self.assertRaises(UnknownActionTypeError):
                    self.gateway.dispatch(
                        idempotency_key=f"unk-{action_type}",
                        consumer="runtime_orchestrator",
                        action_type=action_type,
                        target_scope="github:acme/widgets",
                        subject="pr-1",
                        payload={},
                        grant_id="whatever",
                    )
        self.assertEqual(self.transport.calls, [])

    def test_non_string_action_type_is_rejected_fail_closed(self):
        for bogus in (None, 123, ["x"]):
            with self.subTest(bogus=bogus):
                with self.assertRaises(UnknownActionTypeError):
                    self.gateway.dispatch(
                        idempotency_key="bogus",
                        consumer="runtime_orchestrator",
                        action_type=bogus,
                        target_scope="github:acme/widgets",
                        subject="pr-1",
                        payload={},
                        grant_id="whatever",
                    )

    def test_scope_prefix_mismatch_is_rejected(self):
        self.grants.record(
            grant_id="g1", scope="railway:acme/prod", subject="svc-1",
            action_type="github_comment", payload_digest=canonical_payload_digest({}),
            granted_by="owner@example.com",
        )
        with self.assertRaises(GrantMismatchError):
            self.gateway.dispatch(
                idempotency_key="scope-mismatch",
                consumer="runtime_orchestrator",
                action_type="github_comment",
                target_scope="railway:acme/prod",  # github_comment must be scoped "github:*"
                subject="svc-1",
                payload={},
                grant_id="g1",
            )


class ActionGatewayCategoryBTests(_TransportPatchMixin, unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.db_path = Path(self._tmp.name) / "state.db"
        self.conn = connect(self.db_path)
        self.addCleanup(self.conn.close)
        self.grants = HumanOwnerGrants(self.conn)
        self.transport = _SpyTransport()
        self.use_transport(self.transport)
        self.gateway = ActionGateway(self.conn, grants=self.grants)
        self.grants.record(
            grant_id="g1", scope="github:acme/widgets", subject="pr-42",
            action_type="github_comment", payload_digest=canonical_payload_digest({"body": "hi"}),
            granted_by="owner@example.com",
        )

    def test_dispatch_without_a_grant_is_rejected(self):
        with self.assertRaises(GrantMismatchError):
            self.gateway.dispatch(
                idempotency_key="no-grant",
                consumer="runtime_orchestrator",
                action_type="github_comment",
                target_scope="github:acme/widgets",
                subject="pr-42",
                payload={"body": "hi"},
                grant_id="does-not-exist",
            )
        self.assertEqual(self.transport.calls, [])

    def test_dispatch_with_subject_mismatch_is_rejected(self):
        with self.assertRaises(GrantMismatchError):
            self.gateway.dispatch(
                idempotency_key="subject-mismatch",
                consumer="runtime_orchestrator",
                action_type="github_comment",
                target_scope="github:acme/widgets",
                subject="pr-999",  # grant is for pr-42
                payload={"body": "hi"},
                grant_id="g1",
            )
        self.assertIsNone(self.grants.get("g1").consumed_at)
        self.assertEqual(self.transport.calls, [])

    def test_a_grant_for_one_payload_does_not_authorize_a_different_payload(self):
        """A grant naming only scope/subject/action_type is a blank check for
        any payload body -- Codex's finding was that a broad 'github_comment
        on pr-42' grant could be used to post an arbitrary comment body never
        actually approved by the Human Owner. The grant's authorization must
        be exact for the payload actually sent, not just its coordinates."""
        with self.assertRaises(GrantMismatchError):
            self.gateway.dispatch(
                idempotency_key="payload-mismatch",
                consumer="runtime_orchestrator",
                action_type="github_comment",
                target_scope="github:acme/widgets",
                subject="pr-42",
                payload={"body": "a completely different, never-approved body"},
                grant_id="g1",
            )
        self.assertEqual(self.transport.calls, [])
        self.assertIsNone(self.grants.get("g1").consumed_at)

    def test_dispatch_with_action_type_mismatch_is_rejected(self):
        with self.assertRaises(GrantMismatchError):
            self.gateway.dispatch(
                idempotency_key="type-mismatch",
                consumer="runtime_orchestrator",
                action_type="github_add_label",  # grant is for github_comment
                target_scope="github:acme/widgets",
                subject="pr-42",
                payload={},
                grant_id="g1",
            )

    def test_dispatching_a_new_key_against_an_already_consumed_grant_is_rejected(self):
        """Once g1 has been consumed by one action intent, a second dispatch
        reusing a *different* idempotency key must not be able to consume it
        again -- even though scope/subject/action_type/payload still match
        exactly what the grant approved. There is no lower-level bypass left
        that could leave a grant consumed with no action_intents row (the
        old consume_grant_in_transaction() did; it has been removed), so
        _record_intent's own already-consumed check is what has to catch
        this."""
        self.gateway.dispatch(
            idempotency_key="intent-1",
            consumer="runtime_orchestrator",
            action_type="github_comment",
            target_scope="github:acme/widgets",
            subject="pr-42",
            payload={"body": "hi"},
            grant_id="g1",
        )
        with self.assertRaises(GrantAlreadyConsumedError):
            self.gateway.dispatch(
                idempotency_key="intent-2",
                consumer="runtime_orchestrator",
                action_type="github_comment",
                target_scope="github:acme/widgets",
                subject="pr-42",
                payload={"body": "hi"},
                grant_id="g1",
            )
        self.assertEqual(len(self.transport.calls), 1)
        self.assertIsNone(self.gateway.get("intent-2"))

    def test_happy_path_consumes_grant_and_calls_transport_exactly_once(self):
        record = self.gateway.dispatch(
            idempotency_key="intent-1",
            consumer="runtime_orchestrator",
            action_type="github_comment",
            target_scope="github:acme/widgets",
            subject="pr-42",
            payload={"body": "hi"},
            grant_id="g1",
        )
        self.assertEqual(record.status, "dispatched")
        self.assertEqual(len(self.transport.calls), 1)
        self.assertEqual(self.transport.calls[0].idempotency_key, "intent-1")

        grant = self.grants.get("g1")
        self.assertIsNotNone(grant.consumed_at)
        self.assertEqual(grant.consumed_by_idempotency_key, "intent-1")

    def test_repeated_dispatch_with_the_same_key_calls_transport_at_most_once(self):
        first = self.gateway.dispatch(
            idempotency_key="intent-1", consumer="runtime_orchestrator", action_type="github_comment",
            target_scope="github:acme/widgets", subject="pr-42", payload={"body": "hi"}, grant_id="g1",
        )
        second = self.gateway.dispatch(
            idempotency_key="intent-1", consumer="runtime_orchestrator", action_type="github_comment",
            target_scope="github:acme/widgets", subject="pr-42", payload={"body": "hi"}, grant_id="g1",
        )
        self.assertEqual(first.dispatched_at, second.dispatched_at)
        self.assertEqual(len(self.transport.calls), 1)

    def test_conflicting_replay_with_mismatched_field_is_rejected_fail_closed(self):
        """Repeating an idempotency key is only safe if every immutable field
        of the request matches byte-for-byte the original dispatch. A replay
        that changes any field -- consumer, action_type, target_scope,
        subject, payload, grant_id, or causal_event_id -- must fail closed
        with a dedicated conflict error instead of silently reusing (or,
        worse, re-authorizing under a different grant) the original intent."""
        base_kwargs = dict(
            idempotency_key="intent-1", consumer="runtime_orchestrator", action_type="github_comment",
            target_scope="github:acme/widgets", subject="pr-42", payload={"body": "hi"}, grant_id="g1",
            causal_event_id="evt-1",
        )
        self.gateway.dispatch(**base_kwargs)
        self.assertEqual(len(self.transport.calls), 1)

        self.grants.record(
            grant_id="g2", scope="github:acme/widgets", subject="pr-42",
            action_type="github_comment", granted_by="owner@example.com",
            payload_digest=canonical_payload_digest({"body": "hi"}),
        )

        conflicting_overrides = [
            {"consumer": "someone_else"},
            {"action_type": "github_add_label"},
            {"target_scope": "github:acme/other-repo"},
            {"subject": "pr-999"},
            {"payload": {"body": "a different, never-approved body"}},
            {"grant_id": "g2"},
            {"causal_event_id": "evt-2"},
        ]
        for override in conflicting_overrides:
            with self.subTest(override=override):
                with self.assertRaises(IdempotencyKeyConflictError):
                    self.gateway.dispatch(**{**base_kwargs, **override})

        # None of the conflicting replays touched transport or consumed g2.
        self.assertEqual(len(self.transport.calls), 1)
        self.assertIsNone(self.grants.get("g2").consumed_at)

    def test_second_grant_cannot_be_consumed_for_an_already_dispatched_key(self):
        """A replay reusing the same key but naming a different grant_id is a
        request-identity conflict, not a safe no-op: it must fail closed and
        never touch the second grant, rather than silently keep dispatching
        under the original grant."""
        self.gateway.dispatch(
            idempotency_key="intent-1", consumer="runtime_orchestrator", action_type="github_comment",
            target_scope="github:acme/widgets", subject="pr-42", payload={"body": "hi"}, grant_id="g1",
        )
        self.grants.record(
            grant_id="g2", scope="github:acme/widgets", subject="pr-42",
            action_type="github_comment", granted_by="owner@example.com",
            payload_digest=canonical_payload_digest({"body": "hi"}),
        )
        with self.assertRaises(IdempotencyKeyConflictError):
            self.gateway.dispatch(
                idempotency_key="intent-1", consumer="runtime_orchestrator", action_type="github_comment",
                target_scope="github:acme/widgets", subject="pr-42", payload={"body": "hi"}, grant_id="g2",
            )
        self.assertIsNone(self.grants.get("g2").consumed_at)

    def test_default_shipped_transport_is_disabled(self):
        gateway = ActionGateway(self.conn, grants=self.grants)  # no transport override: shipped default
        with patch("orchestrator.action_gateway.default_transport", default_transport):
            with self.assertRaises(TransportDisabledError):
                gateway.dispatch(
                    idempotency_key="shipped-1", consumer="runtime_orchestrator", action_type="github_comment",
                    target_scope="github:acme/widgets", subject="pr-42", payload={"body": "hi"}, grant_id="g1",
                )
        record = gateway.get("shipped-1")
        self.assertEqual(record.status, "failed")
        # grant was durably consumed before the disabled transport was ever reached
        self.assertIsNotNone(self.grants.get("g1").consumed_at)

    def test_public_gateway_constructor_rejects_arbitrary_transport_injection(self):
        """Production Category B dispatch must have no public path to a real
        external transport; tests exercise the disabled stub by patching it at
        its module seam instead of passing a callable through this API."""
        with self.assertRaises(TypeError):
            ActionGateway(self.conn, grants=self.grants, transport=_SpyTransport())

    def test_intent_and_grant_consumption_are_audited(self):
        self.gateway.dispatch(
            idempotency_key="intent-1", consumer="runtime_orchestrator", action_type="github_comment",
            target_scope="github:acme/widgets", subject="pr-42", payload={"body": "hi"}, grant_id="g1",
        )
        actions = [e.action for e in AuditLog(self.conn).all()]
        self.assertIn("action.intent_recorded", actions)
        self.assertIn("action.dispatched", actions)


class ActionGatewayRecordIntentRaceTests(_TransportPatchMixin, unittest.TestCase):
    """Exercises _record_intent's in-lock recheck directly: a second caller
    that raced dispatch()'s pre-check (both saw no existing row) and reached
    _record_intent after a concurrent winner already inserted the row under
    the write lock."""

    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.conn = connect(Path(self._tmp.name) / "state.db")
        self.addCleanup(self.conn.close)
        self.grants = HumanOwnerGrants(self.conn)
        self.transport = _SpyTransport()
        self.use_transport(self.transport)
        self.gateway = ActionGateway(self.conn, grants=self.grants)
        self.grants.record(
            grant_id="g1", scope="github:acme/widgets", subject="pr-42",
            action_type="github_comment", payload_digest=canonical_payload_digest({"body": "hi"}),
            granted_by="owner@example.com",
        )
        self.grants.record(
            grant_id="g2", scope="github:acme/widgets", subject="pr-42",
            action_type="github_comment", payload_digest=canonical_payload_digest({"body": "bye"}),
            granted_by="owner@example.com",
        )

    def test_concurrent_insert_race_with_mismatched_request_is_rejected(self):
        """The loser of the race must not silently receive the winner's row
        as if it were its own -- it must fail closed with the same
        IdempotencyKeyConflictError dispatch()'s pre-check raises, and must
        never touch its own (different) grant."""
        self.gateway._record_intent(
            idempotency_key="intent-1", consumer="runtime_orchestrator", action_type="github_comment",
            target_scope="github:acme/widgets", subject="pr-42", payload={"body": "hi"}, grant_id="g1",
            causal_event_id=None, now="2026-01-01T00:00:00+00:00",
        )
        with self.assertRaises(IdempotencyKeyConflictError):
            self.gateway._record_intent(
                idempotency_key="intent-1", consumer="runtime_orchestrator", action_type="github_comment",
                target_scope="github:acme/widgets", subject="pr-42", payload={"body": "bye"}, grant_id="g2",
                causal_event_id=None, now="2026-01-01T00:00:01+00:00",
            )
        self.assertIsNone(self.grants.get("g2").consumed_at)

    def test_concurrent_insert_race_with_exact_match_returns_existing_row(self):
        """The loser's request is byte-for-byte identical to what actually
        got inserted (a true concurrent duplicate, not a mismatch) -- it
        must get back the winner's row rather than erroring."""
        first = self.gateway._record_intent(
            idempotency_key="intent-1", consumer="runtime_orchestrator", action_type="github_comment",
            target_scope="github:acme/widgets", subject="pr-42", payload={"body": "hi"}, grant_id="g1",
            causal_event_id=None, now="2026-01-01T00:00:00+00:00",
        )
        second = self.gateway._record_intent(
            idempotency_key="intent-1", consumer="runtime_orchestrator", action_type="github_comment",
            target_scope="github:acme/widgets", subject="pr-42", payload={"body": "hi"}, grant_id="g1",
            causal_event_id=None, now="2026-01-01T00:00:01+00:00",
        )
        self.assertEqual(first.created_at, second.created_at)
        self.assertEqual(second.grant_id, "g1")


class ActionGatewayRestartReconciliationTests(_TransportPatchMixin, unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.db_path = Path(self._tmp.name) / "state.db"
        self.conn = connect(self.db_path)
        self.addCleanup(self.conn.close)
        self.grants = HumanOwnerGrants(self.conn)
        self.grants.record(
            grant_id="g1", scope="github:acme/widgets", subject="pr-42",
            action_type="github_comment", granted_by="owner@example.com",
            payload_digest=canonical_payload_digest({"body": "hi"}),
        )

    def test_a_crash_before_the_stub_call_leaves_a_pending_intent_that_reconcile_resumes(self):
        """Simulates a process that committed the atomic consume+intent
        transaction and then crashed before ever attempting the transport
        call: the intent is durably 'pending' and the grant durably
        consumed, exactly as ``_record_intent`` would leave them. A fresh
        process/connection must resume it without re-consuming the grant.
        The insert below is itself what durably consumes g1 -- schema.py's
        v6 action_intents_consumes_grant trigger derives it from the row,
        exactly as _record_intent's own insert would have."""
        self.conn.execute(
            """
            INSERT INTO action_intents
                (idempotency_key, consumer, action_type, target_scope, subject, payload, grant_id,
                 status, causal_event_id, created_at, updated_at, dispatched_at)
            VALUES ('intent-1', 'runtime_orchestrator', 'github_comment', 'github:acme/widgets', 'pr-42',
                    '{"body": "hi"}', 'g1', 'pending', NULL, '2026-01-01T00:00:00+00:00',
                    '2026-01-01T00:00:00+00:00', NULL)
            """
        )
        self.conn.close()

        reconnected = connect(self.db_path)
        self.addCleanup(reconnected.close)
        spy = _SpyTransport()
        self.use_transport(spy)
        resumed_gateway = ActionGateway(reconnected)
        results = resumed_gateway.reconcile_pending()
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0].status, "dispatched")
        self.assertEqual(len(spy.calls), 1)

        resumed_grants = HumanOwnerGrants(reconnected)
        self.assertEqual(resumed_grants.get("g1").consumed_by_idempotency_key, "intent-1")

    def test_a_crash_after_claim_leaves_a_dispatching_intent_that_reconcile_never_retries(self):
        """Simulates a process that committed the atomic claim (pending ->
        dispatching) and then crashed strictly between that commit and the
        transport call's own outcome being recorded. Whether transport was
        ever attempted -- let alone whether it succeeded -- is unknown, so
        reconcile must never call transport again for this row; it must
        preserve it for explicit manual recovery and surface it, both in the
        reconcile result and as a durable audit event."""
        self.conn.execute(
            """
            INSERT INTO action_intents
                (idempotency_key, consumer, action_type, target_scope, subject, payload, grant_id,
                 status, causal_event_id, created_at, updated_at, dispatched_at)
            VALUES ('intent-1', 'runtime_orchestrator', 'github_comment', 'github:acme/widgets', 'pr-42',
                    '{"body": "hi"}', 'g1', 'dispatching', NULL, '2026-01-01T00:00:00+00:00',
                    '2026-01-01T00:00:00+00:00', NULL)
            """
        )
        self.conn.close()

        reconnected = connect(self.db_path)
        self.addCleanup(reconnected.close)
        spy = _SpyTransport()
        self.use_transport(spy)
        resumed_gateway = ActionGateway(reconnected)
        results = resumed_gateway.reconcile_pending()

        self.assertEqual(len(spy.calls), 0)
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0].idempotency_key, "intent-1")
        self.assertEqual(results[0].status, "dispatching")

        still_dispatching = resumed_gateway.get("intent-1")
        self.assertEqual(still_dispatching.status, "dispatching")

        recovery_events = [e for e in AuditLog(reconnected).all() if e.action == "action.recovery_required"]
        self.assertEqual(len(recovery_events), 1)
        self.assertEqual(recovery_events[0].subject_id, "intent-1")
        self.assertEqual(recovery_events[0].detail.get("reason"), "ambiguous_dispatch_outcome")

    def test_reconcile_pending_never_recalls_an_already_dispatched_intent(self):
        spy = _SpyTransport()
        self.use_transport(spy)
        gateway = ActionGateway(self.conn, grants=self.grants)
        gateway.dispatch(
            idempotency_key="intent-1", consumer="runtime_orchestrator", action_type="github_comment",
            target_scope="github:acme/widgets", subject="pr-42", payload={"body": "hi"}, grant_id="g1",
        )
        self.assertEqual(len(spy.calls), 1)
        results = gateway.reconcile_pending()
        self.assertEqual(results, [])
        self.assertEqual(len(spy.calls), 1)  # not called again

    def test_reconcile_pending_is_exclusive_across_two_connections(self):
        """Simulates two processes reconciling concurrently: once one of them
        claims and dispatches the pending intent, the other must not call
        transport for it too."""
        self.use_transport(lambda intent: None)
        gateway_a = ActionGateway(self.conn, grants=self.grants)
        gateway_a.dispatch(
            idempotency_key="intent-1", consumer="runtime_orchestrator", action_type="github_comment",
            target_scope="github:acme/widgets", subject="pr-42", payload={"body": "hi"}, grant_id="g1",
        )
        # Force it back to pending to simulate a crash right after the intent
        # was durably recorded but before the (successful) dispatch above was
        # ever attempted by *this* reconciliation race.
        self.conn.execute("UPDATE action_intents SET status = 'pending' WHERE idempotency_key = 'intent-1'")

        other_conn = connect(self.db_path)
        self.addCleanup(other_conn.close)
        spy_a, spy_b = _SpyTransport(), _SpyTransport()
        reconciler_a = ActionGateway(self.conn)
        reconciler_b = ActionGateway(other_conn)

        with patch("orchestrator.action_gateway.default_transport", spy_a):
            reconciler_a.reconcile_pending()
        with patch("orchestrator.action_gateway.default_transport", spy_b):
            reconciler_b.reconcile_pending()  # must see it already dispatched

        self.assertEqual(len(spy_a.calls) + len(spy_b.calls), 1)


class GitHubActionInterfaceTests(_TransportPatchMixin, unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.conn = connect(Path(self._tmp.name) / "state.db")
        self.addCleanup(self.conn.close)
        self.grants = HumanOwnerGrants(self.conn)
        self.transport = _SpyTransport()
        self.use_transport(self.transport)
        self.gateway = ActionGateway(self.conn, grants=self.grants)
        self.github = GitHubActionInterface(self.gateway)

    def test_happy_path_scopes_to_the_named_repo(self):
        self.grants.record(
            grant_id="g1", scope="github:acme/widgets", subject="issue-7",
            action_type="github_comment", granted_by="owner@example.com",
            payload_digest=canonical_payload_digest({"body": "hi"}),
        )
        record = self.github.dispatch(
            idempotency_key="gh-1", consumer="lead_orchestrator", action_type="github_comment",
            repo="acme/widgets", subject="issue-7", payload={"body": "hi"}, grant_id="g1",
        )
        self.assertEqual(record.status, "dispatched")
        self.assertEqual(record.target_scope, "github:acme/widgets")

    def test_a_grant_for_a_different_repo_is_rejected(self):
        self.grants.record(
            grant_id="g1", scope="github:acme/other-repo", subject="issue-7",
            action_type="github_comment", granted_by="owner@example.com",
            payload_digest=canonical_payload_digest({}),
        )
        with self.assertRaises(GrantMismatchError):
            self.github.dispatch(
                idempotency_key="gh-2", consumer="lead_orchestrator", action_type="github_comment",
                repo="acme/widgets", subject="issue-7", payload={}, grant_id="g1",
            )

    def test_release_style_action_types_are_not_offered(self):
        for action_type in ("github_merge_pr", "github_force_push", "github_delete_branch"):
            with self.subTest(action_type=action_type):
                with self.assertRaises(UnknownActionTypeError):
                    self.github.dispatch(
                        idempotency_key=f"gh-{action_type}", consumer="lead_orchestrator", action_type=action_type,
                        repo="acme/widgets", subject="issue-7", payload={}, grant_id="whatever",
                    )
        self.assertEqual(self.transport.calls, [])


class RailwayActionInterfaceTests(_TransportPatchMixin, unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.conn = connect(Path(self._tmp.name) / "state.db")
        self.addCleanup(self.conn.close)
        self.grants = HumanOwnerGrants(self.conn)
        self.transport = _SpyTransport()
        self.use_transport(self.transport)
        self.gateway = ActionGateway(self.conn, grants=self.grants)
        self.railway = RailwayActionInterface(self.gateway)

    def test_happy_path_restart_service_scopes_to_the_named_project(self):
        self.grants.record(
            grant_id="g1", scope="railway:acme/prod", subject="svc-web",
            action_type="railway_restart_service", granted_by="owner@example.com",
            payload_digest=canonical_payload_digest({}),
        )
        record = self.railway.dispatch(
            idempotency_key="rw-1", consumer="runtime_orchestrator", action_type="railway_restart_service",
            project="acme/prod", subject="svc-web", payload={}, grant_id="g1",
        )
        self.assertEqual(record.status, "dispatched")

    def test_deploy_and_release_action_types_are_never_offered(self):
        for action_type in ("railway_deploy", "railway_release", "railway_promote"):
            with self.subTest(action_type=action_type):
                with self.assertRaises(UnknownActionTypeError):
                    self.railway.dispatch(
                        idempotency_key=f"rw-{action_type}", consumer="runtime_orchestrator", action_type=action_type,
                        project="acme/prod", subject="svc-web", payload={}, grant_id="whatever",
                    )
        self.assertEqual(self.transport.calls, [])
        for action_type in ("railway_deploy", "railway_release", "railway_promote"):
            self.assertNotIn(action_type, CATEGORY_B_ACTION_SCOPES)


class ActionGatewayAuditRedactionTests(_TransportPatchMixin, unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.conn = connect(Path(self._tmp.name) / "state.db")
        self.addCleanup(self.conn.close)
        self.grants = HumanOwnerGrants(self.conn)
        self.transport = _SpyTransport()
        self.use_transport(self.transport)

    def test_unknown_action_rejection_audit_redacts_bearer_token(self):
        """A caller-supplied action_type string is untrusted input -- it may
        carry a secret (e.g. a copy-pasted Authorization header) even though
        it's also invalid. The default-deny rejection audit must never
        persist that secret verbatim."""
        token = "abc123reallysecrettoken"
        action_type = f"totally_unheard_of Authorization: Bearer {token}"
        transport = _SpyTransport()
        gateway = ActionGateway(self.conn, grants=self.grants)

        with self.assertRaises(UnknownActionTypeError):
            gateway.dispatch(
                idempotency_key="unk-token",
                consumer="runtime_orchestrator",
                action_type=action_type,
                target_scope="github:acme/widgets",
                subject="pr-1",
                payload={},
                grant_id="whatever",
            )

        rejected = [e for e in AuditLog(self.conn).all() if e.action == "action.rejected"]
        self.assertEqual(len(rejected), 1)
        detail_str = json.dumps(rejected[0].detail)
        self.assertNotIn(token, detail_str)
        self.assertIn(REDACTED_VALUE, detail_str)

    def test_transport_failure_audit_redacts_bearer_token(self):
        """A transport failure's exception message is untrusted (it may echo
        back request headers, e.g. from an HTTP client's error string) and
        must never be persisted verbatim into the durable audit log."""
        token = "xyz789reallysecrettoken"

        def failing_transport(intent):
            raise RuntimeError(f"transport failed: Authorization: Bearer {token}")

        self.grants.record(
            grant_id="g1", scope="github:acme/widgets", subject="pr-42",
            action_type="github_comment", payload_digest=canonical_payload_digest({"body": "hi"}),
            granted_by="owner@example.com",
        )
        gateway = ActionGateway(self.conn, grants=self.grants)
        with patch("orchestrator.action_gateway.default_transport", failing_transport):
            with self.assertRaises(RuntimeError):
                gateway.dispatch(
                    idempotency_key="transport-fail",
                    consumer="runtime_orchestrator",
                    action_type="github_comment",
                    target_scope="github:acme/widgets",
                    subject="pr-42",
                    payload={"body": "hi"},
                    grant_id="g1",
                )

        failed = [e for e in AuditLog(self.conn).all() if e.action == "action.failed"]
        self.assertEqual(len(failed), 1)
        detail_str = json.dumps(failed[0].detail)
        self.assertNotIn(token, detail_str)
        self.assertIn(REDACTED_VALUE, detail_str)

    def test_scope_prefix_mismatch_audit_redacts_secret_in_target_scope(self):
        """target_scope is caller-controlled and reaches the audit log even
        on a rejection -- review flagged that the scope-mismatch path
        persists it verbatim (unlike the two paths above, which already call
        ``redact`` explicitly)."""
        token = "scope-mismatch-secret-123"
        gateway = ActionGateway(self.conn, grants=self.grants)

        with self.assertRaises(GrantMismatchError):
            gateway.dispatch(
                idempotency_key="scope-secret",
                consumer="runtime_orchestrator",
                action_type="github_comment",
                target_scope=f"railway:acme/prod?token={token}",  # github_comment must be scoped "github:*"
                subject="pr-1",
                payload={},
                grant_id="whatever",
            )

        rejected = [e for e in AuditLog(self.conn).all() if e.action == "action.rejected"]
        self.assertEqual(len(rejected), 1)
        detail_str = json.dumps(rejected[0].detail)
        self.assertNotIn(token, detail_str)
        self.assertIn(REDACTED_VALUE, detail_str)

    def test_intent_recorded_audit_redacts_secret_in_target_scope(self):
        """The successful-path ``action.intent_recorded`` audit entry also
        persists ``target_scope`` verbatim -- a secret pasted into it (the
        same caller-controlled field as the mismatch case above) must be
        redacted on the happy path too, not just on rejection."""
        token = "intent-recorded-secret-456"
        scope = f"github:acme/widgets?token={token}"
        self.grants.record(
            grant_id="g1", scope=scope, subject="pr-42",
            action_type="github_comment", payload_digest=canonical_payload_digest({"body": "hi"}),
            granted_by="owner@example.com",
        )
        gateway = ActionGateway(self.conn, grants=self.grants)

        gateway.dispatch(
            idempotency_key="intent-secret",
            consumer="runtime_orchestrator",
            action_type="github_comment",
            target_scope=scope,
            subject="pr-42",
            payload={"body": "hi"},
            grant_id="g1",
        )

        recorded = [e for e in AuditLog(self.conn).all() if e.action == "action.intent_recorded"]
        self.assertEqual(len(recorded), 1)
        detail_str = json.dumps(recorded[0].detail)
        self.assertNotIn(token, detail_str)
        self.assertIn(REDACTED_VALUE, detail_str)


if __name__ == "__main__":
    unittest.main()
