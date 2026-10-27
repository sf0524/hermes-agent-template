"""Upgrade contracts; run with the pinned Hermes source on PYTHONPATH or in-image."""
from __future__ import annotations

import asyncio
import io
import json
import os
import shlex
from pathlib import Path
import sys
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock, Mock, patch
import zipfile

from starlette.datastructures import UploadFile
from starlette.requests import Request

from test_v2026_9_24_config_safety import ConfigSafetyFixture


class UpgradeTests(ConfigSafetyFixture, unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        super().setUp()
        self.real_guard = self.server.guard
        self.server.guard = lambda _: None
        self.server._verified_root_gateway_pid = Mock(return_value=None)

    async def test_seeded_save_removes_obsolete_model_fields_with_real_writer(self):
        # The target writer rejects omitted paths unless atomic_config_replace
        # is used. A mocked writer would miss the fresh-setup regression.
        self.server._write_config_atomic = self.real_writer
        config = self.home / "config.yaml"
        config.write_text("_config_version: 50\nmodel:\n  provider: auto\n  base_url: ''\ncustom_section:\n  keep: yes\nmcp_servers:\n  example:\n    enabled: false\n")
        self.server.write_config_yaml({"MINIMAX_API_KEY": "synthetic-key", "LLM_MODEL": "MiniMax-M2.7"})
        value = self.server._read_config_for_write(config)
        self.assertNotIn("base_url", value["model"])
        self.assertTrue(value["custom_section"]["keep"])
        self.assertFalse(value["mcp_servers"]["example"]["enabled"])

    async def test_duplicate_yaml_does_not_replace_credentials(self):
        config = self.home / "config.yaml"
        before = "model: {default: first}\nmodel: {default: second}\n"
        config.write_text(before)
        self.server.ENV_FILE.write_text("MINIMAX_API_KEY=old\n")

        class Save:
            async def json(self):
                return {"vars": {"MINIMAX_API_KEY": "new", "LLM_MODEL": "new"}}

        response = await self.server.api_config_put(Save())
        self.assertEqual(response.status_code, 500)
        self.assertEqual(config.read_text(), before)
        self.assertEqual(self.server.ENV_FILE.read_text(), "MINIMAX_API_KEY=old\n")

    async def test_dependency_apply_preserves_stopped_gateway_and_configuration(self):
        config = self.home / "config.yaml"
        config.write_text("model: {default: synthetic}\n")
        self.server.ENV_FILE.write_text("MINIMAX_API_KEY=synthetic\nLLM_MODEL=synthetic\n")
        self.server.ESTOP_FILE.write_text("paused")
        before = {p: p.read_bytes() for p in (config, self.server.ENV_FILE, self.server.ESTOP_FILE)}
        self.server.gw.stop = AsyncMock()
        self.server.gw.start = AsyncMock()
        self.server.dash.stop = AsyncMock()
        self.server.dash.start = AsyncMock()
        self.assertEqual((await self.server.api_dependencies_apply(object())).status_code, 200)
        self.server.gw.start.assert_not_awaited()
        self.server.dash.stop.assert_awaited_once()
        self.server.dash.start.assert_awaited_once()
        self.assertEqual({p: p.read_bytes() for p in before}, before)

    async def test_dependency_apply_running_gateway_does_not_rewrite_config(self):
        self.server.gw.state = "running"
        self.server.gw.proc = SimpleNamespace(returncode=None)
        self.server.gw.stop = AsyncMock()
        self.server.gw.start = AsyncMock()
        self.server.dash.stop = AsyncMock()
        self.server.dash.start = AsyncMock()
        with patch.object(self.server, "is_config_complete", return_value=True):
            await self.server.api_dependencies_apply(object())
        self.server.gw.start.assert_awaited_once_with(write_config=False)

    async def test_reload_waits_for_config_lifecycle_and_refuses_active_backup(self):
        self.server.gw.stop = AsyncMock()
        self.server.dash.stop = AsyncMock()
        self.server.dash.start = AsyncMock()
        async with self.server.lifecycle_lock:
            action = asyncio.create_task(self.server.api_dependencies_apply(object()))
            await asyncio.sleep(0)
            self.server.gw.stop.assert_not_awaited()
        await action
        self.server.gw.stop.assert_awaited_once()
        async with self.server.backup_lock:
            response = await self.server.api_dependencies_apply(object())
        self.assertEqual(response.status_code, 409)

    async def test_failed_preparation_keeps_both_children_stopped(self):
        self.server._prepare_hermes = AsyncMock(return_value=False)
        self.server.dash.start = AsyncMock()
        self.server.gw.start = AsyncMock()
        await self.server._initialize_hermes("restore")
        self.server._prepare_hermes.assert_awaited_once_with("restore")
        self.server.dash.start.assert_not_awaited()
        self.server.gw.start.assert_not_awaited()

    async def test_restore_returns_pending_and_prepares_before_restarting(self):
        incoming = io.BytesIO()
        with zipfile.ZipFile(incoming, "w") as zf:
            zf.writestr("config.yaml", "_config_version: 46\n")
        upload = UploadFile(filename="backup.zip", file=io.BytesIO(incoming.getvalue()))

        class Restore:
            async def form(self):
                return {"file": upload}

        async def cli(*args, **kwargs):
            if args[0] == "backup":
                with zipfile.ZipFile(args[2], "w") as zf:
                    zf.writestr("config.yaml", "_config_version: 50\n")
            return 0, "ok"

        self.server._run_hermes_cli = cli
        self.server.gw.stop = AsyncMock()
        self.server.dash.stop = AsyncMock()
        self.server._prepare_hermes = AsyncMock(return_value=False)
        self.server.dash.start = AsyncMock()
        self.server.gw.start = AsyncMock()
        response = await self.server.api_backup_restore(Restore())
        self.assertEqual(response.status_code, 200)
        self.assertTrue(json.loads(response.body)["preparing"])
        self.assertEqual(self.server.preparation["state"], "running")
        await self.server._preparation_task
        self.server._prepare_hermes.assert_awaited_once_with("restore")
        self.server.dash.start.assert_not_awaited()
        self.server.gw.start.assert_not_awaited()
        self.assertEqual(len(list(self.server.BACKUP_DIR.glob("pre-restore-*.zip"))), 1)

    async def test_real_preparation_timeout_stops_owned_child(self):
        helper = self.home / "template_helpers" / "prepare_hermes.py"
        helper.parent.mkdir()
        marker = helper.with_name("heartbeat")
        helper.write_text("import time\nfrom pathlib import Path\nwhile True:\n Path(__file__).with_name('heartbeat').write_text(str(time.time()))\n time.sleep(.02)\n")
        with patch.object(self.server, "__file__", str(self.home / "server.py")), \
                patch.object(self.server, "PREPARATION_TIMEOUT", .5), \
                patch.dict(os.environ, {"HERMES_PYTHON": sys.executable}):
            self.assertFalse(await self.server._prepare_hermes("startup"))
        self.assertEqual(self.server.preparation["state"], "failed")
        before = marker.read_text()
        await asyncio.sleep(.05)
        self.assertEqual(marker.read_text(), before)

    async def test_preparation_blocks_mutations_but_health_and_setup_remain_available(self):
        self.server.preparation["state"] = "running"
        request = Request({"type": "http", "method": "PUT", "path": "/setup/api/config", "headers": []})
        with patch.object(self.server, "_is_authenticated", return_value=True):
            self.assertEqual(self.real_guard(request).status_code, 503)
        health = await self.server.route_health(object())
        self.assertEqual(health.status_code, 200)
        self.assertEqual(json.loads(health.body)["preparation"], "running")

    async def test_config_read_does_not_wait_for_network_bound_preparation(self):
        self.server.preparation["state"] = "running"
        async with self.server.cfg_lock:
            response = await asyncio.wait_for(self.server.api_config_get(object()), timeout=.1)
        self.assertEqual(response.status_code, 503)

    async def test_safety_backup_progress_is_visible_and_failure_restores_prior_state(self):
        self.server.gw.stop = AsyncMock()
        self.server.dash.stop = AsyncMock()

        async def fail_backup(*args, **kwargs):
            self.assertEqual(args[0], "backup")
            self.assertEqual(self.server.preparation["phase"], "safety_backup")
            response = await asyncio.wait_for(self.server.api_config_get(object()), timeout=.1)
            self.assertEqual(response.status_code, 503)
            return 1, "synthetic backup failure"

        self.server._run_hermes_cli = fail_backup
        for state, requires_restore in (("ready", False), ("failed", True)):
            self.server.preparation.update(state=state, phase="complete" if state == "ready" else "failed",
                                           requires_restore=requires_restore)
            before = dict(self.server.preparation)
            incoming = io.BytesIO()
            with zipfile.ZipFile(incoming, "w") as archive:
                archive.writestr("config.yaml", "_config_version: 46\n")
            upload = UploadFile(filename="synthetic.zip", file=io.BytesIO(incoming.getvalue()))

            class Restore:
                async def form(self):
                    return {"file": upload}

            response = await self.server.api_backup_restore(Restore())
            self.assertEqual(response.status_code, 500)
            self.assertEqual(self.server.preparation, before)
        self.server.gw.stop.assert_not_awaited()
        self.server.dash.stop.assert_not_awaited()

    async def test_queued_start_cannot_bypass_restored_state_preparation(self):
        request = Request({"type": "http", "method": "POST", "path": "/setup/api/gateway/start", "headers": []})
        self.server.guard = self.real_guard
        self.server.gw.start = AsyncMock()
        with patch.object(self.server, "_is_authenticated", return_value=True):
            async with self.server.lifecycle_lock:
                action = asyncio.create_task(self.server.api_gw_start(request))
                await asyncio.sleep(0)
                self.server.preparation["state"] = "running"
            response = await action
        self.assertEqual(response.status_code, 503)
        self.server.gw.start.assert_not_awaited()

    async def test_email_pin_notice_uses_native_config_or_env_without_duplicate_field(self):
        (self.home / "config.yaml").write_text("platforms:\n  email:\n    address: synthetic@example.invalid\n")
        self.assertEqual(self.server._email_authserv_missing(), ["default"])
        self.server.ENV_FILE.write_text("EMAIL_AUTHSERV_ID=synthetic.invalid\n")
        self.assertEqual(self.server._email_authserv_missing(), [])
        self.assertNotIn("EMAIL_AUTHSERV_ID", {item[0] for item in self.server.ENV_VARS})

    async def test_email_pin_notice_follows_native_nested_and_extra_precedence(self):
        config = self.home / "config.yaml"
        config.write_text("gateway:\n  platforms:\n    email:\n      address: synthetic@example.invalid\n      authserv_id: wrong.invalid\n      extra:\n        authserv_id: correct.invalid\n")
        self.assertEqual(self.server._email_authserv_missing(), [])
        config.write_text("gateway:\n  email:\n    address: synthetic@example.invalid\n")
        self.assertEqual(self.server._email_authserv_missing(), ["default"])
        config.write_text("{}\n")
        (self.home / "gateway.json").write_text(json.dumps({"platforms": {"email": {"extra": {"address": "synthetic@example.invalid"}}}}))
        self.assertEqual(self.server._email_authserv_missing(), ["default"])

    async def test_gateway_spawn_failure_is_not_reported_as_success(self):
        self.server.write_config_yaml = lambda _: None
        with patch.object(self.server.asyncio, "create_subprocess_exec", side_effect=FileNotFoundError("synthetic missing binary")):
            response = await self.server.api_gw_start(object())
        self.assertEqual(response.status_code, 503)
        self.assertEqual(self.server.gw.state, "error")

    async def test_failed_restore_allows_safety_restore_but_not_start_or_retry(self):
        self.server.preparation.update(state="failed", requires_restore=True)
        with patch.object(self.server, "_is_authenticated", return_value=True):
            for path, allowed in (("/setup/api/backup/restore", True), ("/setup/api/gateway/start", False)):
                request = Request({"type": "http", "method": "POST", "path": path, "headers": []})
                response = self.real_guard(request)
                self.assertEqual(response is None, allowed)
        response = await self.server.api_preparation_retry(object())
        self.assertEqual(response.status_code, 409)

    async def test_initializer_reports_dashboard_spawn_failure(self):
        self.server._prepare_hermes = AsyncMock(return_value=True)
        self.server.dash.start = AsyncMock(return_value=False)
        self.server.gw.start = AsyncMock()
        await self.server._initialize_hermes()
        self.assertEqual(self.server.preparation["state"], "failed")
        self.server.gw.start.assert_not_awaited()

    async def test_interrupted_import_gate_survives_fresh_server_import(self):
        import importlib.util
        self.server.BACKUP_DIR.mkdir()
        self.server._write_restore_state({"phase": "importing", "snapshot": "pre-restore-123-abcd.zip"})
        spec = importlib.util.spec_from_file_location("server_after_interrupted_restore", Path(__file__).parents[1] / "server.py")
        fresh = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(fresh)
        fresh._prepare_hermes = AsyncMock()
        fresh.dash.start = AsyncMock()
        fresh.gw.start = AsyncMock()
        await fresh._initialize_hermes()
        self.assertTrue(fresh.preparation["requires_restore"])
        self.assertEqual(fresh.preparation["state"], "failed")
        fresh._prepare_hermes.assert_not_awaited()
        fresh.dash.start.assert_not_awaited()
        fresh.gw.start.assert_not_awaited()

    async def test_successful_import_marker_resumes_migration_before_children(self):
        self.server.BACKUP_DIR.mkdir()
        self.server._write_restore_state({"phase": "imported", "snapshot": "pre-restore-123-abcd.zip"})
        self.server._prepare_hermes = AsyncMock(return_value=True)

        async def start():
            self.assertIsNone(self.server._restore_state())

        self.server.dash.start = start
        with patch.object(self.server, "is_config_complete", return_value=False):
            await self.server._initialize_hermes()
        self.server._prepare_hermes.assert_awaited_once_with("restore")
        self.assertIsNone(self.server._restore_state())

    async def test_backup_scope_keeps_user_tools_and_generated_databases(self):
        excluded = self.server._in_excluded_root_dir
        for name in ("installs/pkgs/tool.db", "tools/python/tool.db", "profiles/work/installs/tool.db"):
            self.assertTrue(excluded(Path(name)), name)
        for name in ("skills/example/tools/user.db", "cache/generated/data.db", "profiles/work/cache/generated/data.db"):
            self.assertFalse(excluded(Path(name)), name)

    async def test_operator_retry_command_quotes_provider_from_imported_config(self):
        from template_helpers import prepare_hermes
        events = []
        name = "unknown; echo SYNTHETIC_SHELL_TEXT"
        with patch("hermes_cli.left_core_migration.recover_at_startup"), \
                patch("hermes_cli.left_core_migration.LEFT_CORE", ()), \
                patch("hermes_cli.memory_provider_migration.configured_provider", return_value=name), \
                patch("hermes_cli.memory_provider_migration.provider_present", return_value=False), \
                patch("hermes_cli.memory_provider_migration.recover_at_startup"), \
                patch.object(prepare_hermes, "emit", side_effect=lambda phase, message, **fields: events.append(fields)):
            prepare_hermes.prepare_integrations(self.home)
        command = events[-1]["integration"]["retry_command"]
        self.assertEqual(shlex.split(command)[-1], name)
        self.assertEqual(len(shlex.split(command)), 6)


if __name__ == "__main__":
    unittest.main()
