import unittest

from orchestrator.consumers import CONSUMER_NAMES
from orchestrator.policy import ALLOWED_ACTION_TYPES, ALLOWED_GATE_SEVERITIES, PolicyViolation, check_outbox_action


class PolicyGuardTests(unittest.TestCase):
    def test_allowed_action_types_are_accepted_for_every_consumer(self):
        for consumer in sorted(CONSUMER_NAMES):
            for action_type in sorted(ALLOWED_ACTION_TYPES):
                with self.subTest(consumer=consumer, action_type=action_type):
                    payload = {"severity": "low"} if action_type == "approve_gate" else {}
                    check_outbox_action(consumer=consumer, action_type=action_type, payload=payload)  # must not raise

    def test_unknown_action_type_is_rejected_fail_closed(self):
        for consumer in sorted(CONSUMER_NAMES):
            for action_type in (
                "merge",
                "release",
                "release_production",
                "force_push",
                "delete_branch",
                "delete_repository",
                "rotate_production_secret",
                "totally_unheard_of_action",
            ):
                with self.subTest(consumer=consumer, action_type=action_type):
                    with self.assertRaises(PolicyViolation):
                        check_outbox_action(consumer=consumer, action_type=action_type, payload={})

    def test_merge_and_release_are_not_in_the_allowlist(self):
        self.assertNotIn("merge", ALLOWED_ACTION_TYPES)
        self.assertNotIn("release", ALLOWED_ACTION_TYPES)
        self.assertNotIn("release_production", ALLOWED_ACTION_TYPES)

    def test_action_type_case_and_whitespace_variants_cannot_bypass_the_allowlist(self):
        for variant in ("Merge", "MERGE", " merge ", "merge ", "Release_Production", "RELEASE_PRODUCTION"):
            with self.subTest(variant=variant):
                with self.assertRaises(PolicyViolation):
                    check_outbox_action(consumer="runtime_orchestrator", action_type=variant, payload={})

    def test_action_type_case_and_whitespace_variants_of_an_allowed_action_still_normalize_in(self):
        for variant in ("Post_Comment", "POST_COMMENT", " post_comment ", "post_comment "):
            with self.subTest(variant=variant):
                check_outbox_action(consumer="runtime_orchestrator", action_type=variant, payload={})  # must not raise

    def test_non_string_action_type_is_rejected_fail_closed(self):
        for bogus in (None, 123, ["merge"], {"type": "merge"}):
            with self.subTest(bogus=bogus):
                with self.assertRaises(PolicyViolation):
                    check_outbox_action(consumer="runtime_orchestrator", action_type=bogus, payload={})

    def test_blank_action_type_is_rejected(self):
        for blank in ("", "   "):
            with self.subTest(blank=blank):
                with self.assertRaises(PolicyViolation):
                    check_outbox_action(consumer="runtime_orchestrator", action_type=blank, payload={})

    def test_critical_gate_approval_has_no_route_for_either_consumer(self):
        for consumer in sorted(CONSUMER_NAMES):
            with self.subTest(consumer=consumer):
                with self.assertRaises(PolicyViolation):
                    check_outbox_action(
                        consumer=consumer, action_type="approve_gate", payload={"severity": "critical"}
                    )

    def test_high_gate_approval_is_forbidden_for_either_consumer(self):
        for consumer in sorted(CONSUMER_NAMES):
            with self.subTest(consumer=consumer):
                with self.assertRaises(PolicyViolation):
                    check_outbox_action(
                        consumer=consumer, action_type="approve_gate", payload={"severity": "high"}
                    )

    def test_low_and_medium_gate_approval_are_not_blocked_by_this_guard(self):
        for severity in sorted(ALLOWED_GATE_SEVERITIES):
            for consumer in sorted(CONSUMER_NAMES):
                with self.subTest(consumer=consumer, severity=severity):
                    check_outbox_action(
                        consumer=consumer, action_type="approve_gate", payload={"severity": severity}
                    )  # must not raise

    def test_severity_check_is_case_insensitive(self):
        with self.assertRaises(PolicyViolation):
            check_outbox_action(
                consumer="runtime_orchestrator", action_type="approve_gate", payload={"severity": "CRITICAL"}
            )

    def test_severity_whitespace_cannot_bypass_the_gate_block(self):
        for variant in ("Critical ", " critical", "critical\t", "CRITICAL\n", " High ", "HIGH "):
            with self.subTest(variant=variant):
                with self.assertRaises(PolicyViolation):
                    check_outbox_action(
                        consumer="runtime_orchestrator", action_type="approve_gate", payload={"severity": variant}
                    )

    def test_severity_whitespace_and_case_normalize_in_for_allowed_severities(self):
        for variant in (" low ", "Low", "LOW", "medium\t", " Medium"):
            with self.subTest(variant=variant):
                check_outbox_action(
                    consumer="runtime_orchestrator", action_type="approve_gate", payload={"severity": variant}
                )  # must not raise

    def test_unknown_gate_severity_is_rejected_fail_closed(self):
        for bogus in ("", "   ", "urgent", None, 7):
            with self.subTest(bogus=bogus):
                with self.assertRaises(PolicyViolation):
                    check_outbox_action(
                        consumer="runtime_orchestrator", action_type="approve_gate", payload={"severity": bogus}
                    )

    def test_ordinary_actions_are_allowed(self):
        check_outbox_action(
            consumer="lead_orchestrator", action_type="post_comment", payload={"body": "hi"}
        )  # must not raise

    def test_payload_marked_irreversible_is_rejected_even_for_an_allowed_action_type(self):
        for consumer in sorted(CONSUMER_NAMES):
            with self.subTest(consumer=consumer):
                with self.assertRaises(PolicyViolation):
                    check_outbox_action(
                        consumer=consumer,
                        action_type="post_comment",
                        payload={"irreversible": True},
                    )

    def test_payload_irreversible_flag_is_truthy_and_string_aware(self):
        for value in (True, "true", "True", "1", "yes"):
            with self.subTest(value=value):
                with self.assertRaises(PolicyViolation):
                    check_outbox_action(
                        consumer="runtime_orchestrator", action_type="post_comment", payload={"irreversible": value}
                    )

    def test_payload_irreversible_false_or_absent_is_not_blocked_by_this_guard(self):
        check_outbox_action(
            consumer="runtime_orchestrator", action_type="post_comment", payload={"irreversible": False}
        )  # must not raise
        check_outbox_action(
            consumer="runtime_orchestrator", action_type="post_comment", payload={}
        )  # must not raise

    def test_payload_irreversible_fails_closed_for_every_present_value_except_literal_false(self):
        # Only the explicit boolean False may mark a payload as not-irreversible.
        # Every other present value -- truthy or falsy by Python's own rules,
        # a recognized "true-ish" string or not -- must be rejected fail-closed.
        # This closes the "on" bypass: "on" is not in {"true", "1", "yes"}, so
        # the old string-only truthiness check let it slip through as allowed.
        non_false_values = (
            "on",
            "arbitrary-string",
            "false",
            "False",
            "FALSE",
            "",
            None,
            0,
            1,
            {"nested": True},
            [],
            [False],
            True,
        )
        for value in non_false_values:
            with self.subTest(value=value):
                with self.assertRaises(PolicyViolation):
                    check_outbox_action(
                        consumer="runtime_orchestrator",
                        action_type="post_comment",
                        payload={"irreversible": value},
                    )

    def test_payload_irreversible_literal_false_is_the_only_accepted_non_irreversible_value(self):
        check_outbox_action(
            consumer="runtime_orchestrator", action_type="post_comment", payload={"irreversible": False}
        )  # must not raise


if __name__ == "__main__":
    unittest.main()
