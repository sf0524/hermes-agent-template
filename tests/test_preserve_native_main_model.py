"""Gateway start / admin saves must not reset a model chosen in native Hermes."""

from __future__ import annotations

import unittest
from unittest.mock import AsyncMock, Mock, patch

import hermes_yaml as yaml

import importlib.util
from pathlib import Path

_spec = importlib.util.spec_from_file_location(
    "_config_safety_fixture",
    Path(__file__).resolve().parent / "test_v2026_9_24_config_safety.py",
)
_module = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_module)
ConfigSafetyFixture = _module.ConfigSafetyFixture


class _Request:
    def __init__(self, body):
        self._body = body

    async def json(self):
        return self._body


class PreserveNativeMainModelTests(ConfigSafetyFixture, unittest.IsolatedAsyncioTestCase):
    def _config(self):
        return yaml.safe_load((self.home / "config.yaml").read_text(encoding="utf-8"))

    def _write_config(self, cfg):
        (self.home / "config.yaml").write_text(yaml.safe_dump(cfg, sort_keys=False), encoding="utf-8")

    def _write_env(self, text):
        (self.home / ".env").write_text(text, encoding="utf-8")

    async def test_gateway_start_keeps_native_ui_model_over_stale_env(self):
        self._write_env("OPENROUTER_API_KEY=k\nLLM_MODEL=stale-env-model\n")
        # Native UI choice written after .env, with unrelated content alongside.
        self._write_config({
            "model": {"default": "native-choice", "provider": "openrouter"},
            "mcp_servers": {"x": {"command": "y"}},
        })
        proc = Mock(returncode=None)
        with patch.object(self.server.asyncio, "create_subprocess_exec", AsyncMock(return_value=proc)) as spawn, \
             patch.object(self.server.asyncio, "create_task", lambda c: c.close()):
            self.assertTrue(await self.server.gw.start())
        spawn.assert_awaited_once()
        cfg = self._config()
        self.assertEqual(cfg["model"]["default"], "native-choice")
        self.assertEqual(cfg["model"]["provider"], "openrouter")
        self.assertEqual(cfg["mcp_servers"], {"x": {"command": "y"}})

    async def test_absent_yaml_model_is_seeded_from_env(self):
        self._write_env("OPENROUTER_API_KEY=k\nLLM_MODEL=seed-model\n")
        self._write_config({"model": {"default": "", "provider": "openrouter"}})
        self.server.write_config_yaml(self.server.read_env(self.server.ENV_FILE), preserve_model=True)
        self.assertEqual(self._config()["model"]["default"], "seed-model")
        self.assertEqual(self._config()["model"]["provider"], "openrouter")

    async def test_no_config_file_is_seeded_from_env(self):
        self.server.write_config_yaml({"LLM_MODEL": "seed-model"}, preserve_model=True)
        self.assertEqual(self._config()["model"]["default"], "seed-model")

    async def test_preserve_keeps_custom_endpoint_but_strips_stale_builtin_endpoint(self):
        self._write_config({"model": {"default": "m", "provider": "custom", "base_url": "http://x/v1"}})
        self.server.write_config_yaml({"LLM_MODEL": "env"}, preserve_model=True)
        self.assertEqual(self._config()["model"]["base_url"], "http://x/v1")
        self._write_config({"model": {"default": "m", "provider": "openrouter", "base_url": "http://stale"}})
        self.server.write_config_yaml({"LLM_MODEL": "env"}, preserve_model=True)
        self.assertNotIn("base_url", self._config()["model"])
        self.assertEqual(self._config()["model"]["default"], "m")

    async def test_admin_save_without_model_change_preserves_native_model(self):
        self.server.guard = lambda _: None
        self._write_env("OPENROUTER_API_KEY=k\nLLM_MODEL=stale-env-model\n")
        self._write_config({"model": {"default": "native-choice", "provider": "openrouter"}})
        resp = await self.server.api_config_put(_Request({
            "vars": {"LLM_MODEL": "stale-env-model", "OPENROUTER_API_KEY": "k"}, "_restart": False}))
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(self._config()["model"]["default"], "native-choice")

    async def test_explicit_provider_selection_same_env_model_overrides_native_yaml(self):
        self.server.guard = lambda _: None
        self._write_env("OPENROUTER_API_KEY=k\nLLM_MODEL=same-model\n")
        self._write_config({"model": {"default": "native-choice", "provider": "openrouter"}})
        with patch.object(self.server, "set_active_model_via_hermes", AsyncMock(return_value=None)):
            resp = await self.server.api_config_put(_Request({
                "vars": {"LLM_MODEL": "same-model", "OPENROUTER_API_KEY": "k"},
                "_active_provider_key": "OPENROUTER_API_KEY", "_restart": False}))
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(self._config()["model"]["default"], "same-model")

    async def test_active_provider_key_without_model_var_preserves_native_yaml(self):
        self.server.guard = lambda _: None
        self._write_env("OPENROUTER_API_KEY=k\nLLM_MODEL=same-model\n")
        self._write_config({"model": {"default": "native-choice", "provider": "openrouter"}})
        with patch.object(self.server, "set_active_model_via_hermes", AsyncMock(return_value=None)):
            resp = await self.server.api_config_put(_Request({
                "vars": {"OPENROUTER_API_KEY": "k"},
                "_active_provider_key": "OPENROUTER_API_KEY", "_restart": False}))
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(self._config()["model"]["default"], "native-choice")

    def _fake_native_pin(self):
        """Stand-in for hermes' POST /api/model/set that really rewrites the pair.

        Writes into the fixture config only (never a live home), so a replayed
        pin is visible in config.yaml instead of hiding behind a bare mock.
        """
        calls = []

        async def pin(provider_id, model, *, base_url="", api_key=""):
            calls.append((provider_id, model))
            cfg = self._config()
            cfg["model"] = {**cfg.get("model", {}), "provider": provider_id, "default": model}
            self._write_config(cfg)
            return None

        return calls, pin

    async def test_provider_key_without_model_var_does_not_replay_stale_env_pin(self):
        self.server.guard = lambda _: None
        self._write_env("OPENROUTER_API_KEY=k\nLLM_MODEL=stale-env-model\n")
        self._write_config({"model": {"default": "gpt-6.1-sol", "provider": "openai-codex"}})
        calls, pin = self._fake_native_pin()
        with patch.object(self.server, "set_active_model_via_hermes", pin):
            resp = await self.server.api_config_put(_Request({
                "vars": {"OPENROUTER_API_KEY": "k"},
                "_active_provider_key": "OPENROUTER_API_KEY", "_restart": False}))
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(calls, [])
        model = self._config()["model"]
        self.assertEqual((model["provider"], model["default"]), ("openai-codex", "gpt-6.1-sol"))

    async def test_admin_explicit_model_change_updates_yaml(self):
        self.server.guard = lambda _: None
        self._write_env("OPENROUTER_API_KEY=k\nLLM_MODEL=old-env\n")
        self._write_config({"model": {"default": "native-choice", "provider": "openrouter"},
                            "mcp_servers": {"x": {}}})
        resp = await self.server.api_config_put(_Request({
            "vars": {"LLM_MODEL": "admin-new", "OPENROUTER_API_KEY": "k"}, "_restart": False}))
        self.assertEqual(resp.status_code, 200)
        cfg = self._config()
        self.assertEqual(cfg["model"]["default"], "admin-new")
        self.assertEqual(cfg["model"]["provider"], "openrouter")
        self.assertEqual(cfg["mcp_servers"], {"x": {}})

    async def test_reset_clears_model_block_and_keeps_unrelated(self):
        self.server.guard = lambda _: None
        self.server.gw.stop = AsyncMock()
        self._write_env("OPENROUTER_API_KEY=k\nLLM_MODEL=m\n")
        self._write_config({"model": {"default": "native-choice", "provider": "openrouter",
                                      "base_url": "http://stale"},
                            "mcp_servers": {"x": {}}})
        resp = await self.server.api_config_reset(object())
        self.assertEqual(resp.status_code, 200)
        cfg = self._config()
        self.assertEqual(cfg["model"], {"default": ""})
        self.assertEqual(cfg["mcp_servers"], {"x": {}})

    async def test_default_write_remains_env_authoritative(self):
        self._write_config({"model": {"default": "native-choice", "provider": "openrouter"}})
        self.server.write_config_yaml({"LLM_MODEL": "env-model"})
        self.assertEqual(self._config()["model"]["default"], "env-model")


if __name__ == "__main__":
    unittest.main()
