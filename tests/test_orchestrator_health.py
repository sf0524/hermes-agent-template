import tempfile
import unittest
from pathlib import Path

from orchestrator.audit import AuditLog
from orchestrator.db import connect
from orchestrator.health import record_health_evidence, redact


class RedactTests(unittest.TestCase):
    def test_redacts_sensitive_top_level_keys(self):
        redacted = redact({"token": "abc123", "status": "ok"})
        self.assertEqual(redacted["token"], "***REDACTED***")
        self.assertEqual(redacted["status"], "ok")

    def test_redacts_sensitive_keys_case_insensitively_and_by_substring(self):
        raw = {
            "API_KEY": "sk-live-xyz",
            "Authorization": "Bearer xyz",
            "user_password": "hunter2",
            "github_secret": "s3cr3t",
            "credential_blob": "abcd",
        }
        redacted = redact(raw)
        for key in raw:
            self.assertEqual(redacted[key], "***REDACTED***", key)

    def test_redacts_nested_dicts_and_lists(self):
        raw = {
            "component": "github",
            "nested": {"access_token": "abc", "ok": True},
            "items": [{"password": "x"}, {"safe": "y"}],
        }
        redacted = redact(raw)
        self.assertEqual(redacted["nested"]["access_token"], "***REDACTED***")
        self.assertTrue(redacted["nested"]["ok"])
        self.assertEqual(redacted["items"][0]["password"], "***REDACTED***")
        self.assertEqual(redacted["items"][1]["safe"], "y")

    def test_non_sensitive_values_pass_through_unchanged(self):
        raw = {"component": "kanban", "status": "ok", "latency_ms": 12, "ok": True, "count": None}
        self.assertEqual(redact(raw), raw)

    def test_redact_does_not_mutate_the_input(self):
        raw = {"token": "abc"}
        redact(raw)
        self.assertEqual(raw["token"], "abc")

    def test_redacts_strings_nested_inside_a_tuple_and_preserves_tuple_shape(self):
        raw = {"headers": ("Bearer abc.def.ghi", "status: ok")}
        redacted = redact(raw)
        self.assertIsInstance(redacted["headers"], tuple)
        self.assertNotIn("abc.def.ghi", redacted["headers"][0])
        self.assertIn("***REDACTED***", redacted["headers"][0])
        self.assertEqual(redacted["headers"][1], "status: ok")

    def test_redacts_access_token_query_param(self):
        secret = "access-token-secret-456"
        redacted = redact({"url": f"https://api.example.com/health?access_token={secret}&status=ok"})
        self.assertNotIn(secret, redacted["url"])
        self.assertIn("***REDACTED***", redacted["url"])
        self.assertIn("status=ok", redacted["url"])

    def test_redacts_authorization_header_with_token_scheme(self):
        secret = "auth-token-scheme-secret"
        redacted = redact({"header": f"Authorization: token {secret}"})
        self.assertNotIn(secret, redacted["header"])
        self.assertIn("***REDACTED***", redacted["header"])
        self.assertIn("Authorization:", redacted["header"])

    def test_redacts_url_username_only_userinfo(self):
        secret = "ghp_usernameonlysecret"
        redacted = redact({"url": f"https://{secret}@github.com/acme/widgets"})
        self.assertNotIn(secret, redacted["url"])
        self.assertIn("***REDACTED***", redacted["url"])
        self.assertIn("github.com/acme/widgets", redacted["url"])

    def test_redacts_secrets_embedded_in_string_values(self):
        raw = {
            "dsn": "postgres://dbuser:hunter2@db.example.com:5432/kanban",
            "url": "https://api.example.com/health?token=sk-live-xyz&status=ok",
            "header": "Bearer abc.def.ghi",
            "note": "component is healthy",
        }
        redacted = redact(raw)
        self.assertNotIn("hunter2", redacted["dsn"])
        self.assertIn("dbuser", redacted["dsn"])
        self.assertIn("db.example.com", redacted["dsn"])
        self.assertIn("***REDACTED***", redacted["dsn"])
        self.assertNotIn("sk-live-xyz", redacted["url"])
        self.assertIn("status=ok", redacted["url"])
        self.assertIn("***REDACTED***", redacted["url"])
        self.assertNotIn("abc.def.ghi", redacted["header"])
        self.assertIn("***REDACTED***", redacted["header"])
        self.assertEqual(redacted["note"], "component is healthy")


class RecordHealthEvidenceTests(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.db_path = Path(self._tmp.name) / "state.db"
        self.conn = connect(self.db_path)
        self.addCleanup(self.conn.close)

    def test_records_one_audit_entry_with_redacted_detail(self):
        entry = record_health_evidence(
            self.conn,
            components={
                "state_db": {"status": "ok"},
                "kanban_source": {"status": "ok", "auth_token": "shh"},
            },
        )
        self.assertEqual(entry.action, "health.snapshot")
        self.assertEqual(entry.detail["components"]["kanban_source"]["auth_token"], "***REDACTED***")
        self.assertEqual(entry.detail["components"]["state_db"]["status"], "ok")

    def test_evidence_is_durable_and_queryable_via_audit_log(self):
        record_health_evidence(self.conn, components={"state_db": {"status": "ok"}})
        entries = AuditLog(self.conn).all()
        health_entries = [e for e in entries if e.action == "health.snapshot"]
        self.assertEqual(len(health_entries), 1)

    def test_overall_status_is_derived_and_present(self):
        entry = record_health_evidence(
            self.conn,
            components={"a": {"status": "ok"}, "b": {"status": "degraded"}},
        )
        self.assertEqual(entry.detail["overall_status"], "degraded")

    def test_overall_status_ok_when_all_components_ok(self):
        entry = record_health_evidence(
            self.conn,
            components={"a": {"status": "ok"}, "b": {"status": "ok"}},
        )
        self.assertEqual(entry.detail["overall_status"], "ok")


if __name__ == "__main__":
    unittest.main()
