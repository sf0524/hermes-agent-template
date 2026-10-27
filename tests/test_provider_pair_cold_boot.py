"""An explicit provider/model pair survives ordinary startup and saves.

Scope of the "cold boot" here: real ``prepare_hermes.seed_home`` and the real
upstream ``migrate_hermes_configs.migrate`` hook (when the Hermes image is
installed) run against an isolated temp home, then the real
``Gateway.start()`` / ``write_config_yaml`` path runs with only the
``hermes gateway run`` subprocess mocked. This is NOT a container cold boot:
no entrypoint, supervisor, dashboard, or live Hermes process is involved, and
the dashboard's ``/api/model/set`` pin is replaced by a fake that really
rewrites the fixture config so a replayed pin is observable.
"""

from __future__ import annotations

import importlib
import importlib.util
import json
import os
import sys
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, Mock, patch

import hermes_yaml as yaml

ROOT = Path(__file__).resolve().parents[1]
HERMES_REPO = Path(os.environ.get("HERMES_REPO_ROOT", "/opt/hermes/repo"))

_spec = importlib.util.spec_from_file_location(
    "_config_safety_fixture_cold_boot", ROOT / "tests" / "test_v2026_9_24_config_safety.py"
)
_module = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_module)
ConfigSafetyFixture = _module.ConfigSafetyFixture


def _load(name: str):
    # Import as a package member so helpers' relative imports resolve at call time.
    sys.path.insert(0, str(ROOT))
    try:
        return importlib.import_module(f"template_helpers.{name}")
    finally:
        sys.path.remove(str(ROOT))


class _Request:
    def __init__(self, body):
        self._body = body

    async def json(self):
        return self._body


# (provider id, model, env var that makes the provider "configured")
NATIVE_PAIRS = [
    ("openai-codex", "gpt-6.1-sol", None),
    ("gemini", "gemini-3-pro-preview", "GEMINI_API_KEY"),
    ("deepseek", "deepseek-v4-pro", "DEEPSEEK_API_KEY"),
]


class ProviderPairColdBootTests(ConfigSafetyFixture, unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        super().setUp()
        self.server.guard = lambda _: None
        self.pin_calls, self.fake_pin = self._fake_native_pin()
        patcher = patch.object(self.server, "set_active_model_via_hermes", self.fake_pin)
        patcher.start()
        self.addCleanup(patcher.stop)

    # -- helpers ---------------------------------------------------------
    def _config(self):
        return yaml.safe_load((self.home / "config.yaml").read_text(encoding="utf-8"))

    def _pair(self):
        model = self._config()["model"]
        return model.get("provider"), model.get("default")

    def _write_config(self, cfg, *, version=None):
        text = yaml.safe_dump(cfg, sort_keys=False)
        if version is not None:
            text = f"_config_version: {version}\n" + text
        (self.home / "config.yaml").write_text(text, encoding="utf-8")

    def _write_env(self, **values):
        (self.home / ".env").write_text(
            "".join(f"{k}={v}\n" for k, v in values.items()), encoding="utf-8"
        )

    def _fake_native_pin(self):
        calls = []

        async def pin(provider_id, model, *, base_url="", api_key=""):
            calls.append((provider_id, model))
            cfg = self._config()
            cfg["model"] = {**cfg.get("model", {}), "provider": provider_id, "default": model}
            (self.home / "config.yaml").write_text(
                yaml.safe_dump(cfg, sort_keys=False), encoding="utf-8"
            )
            return None

        return calls, pin

    def _seed_and_migrate(self):
        """Real seed_home + real upstream migrator (skipped without the image)."""
        _load("prepare_hermes").seed_home(self.home, HERMES_REPO)
        migrator = HERMES_REPO / "scripts" / "docker_config_migrate.py"
        if not migrator.is_file():
            return False
        migrate_mod = _load("migrate_hermes_configs")
        env = {"PATH": os.environ.get("PATH", "") or "/usr/bin:/bin",
               "HERMES_REPO_ROOT": str(HERMES_REPO), "PYTHONPATH": str(HERMES_REPO)}
        with patch.dict(os.environ, env):
            migrate_mod.migrate(self.home, migrator)
        return True

    async def _start_gateway(self):
        """Real Gateway.start(); only the hermes subprocess is mocked."""
        self.server.gw.proc = None
        proc = Mock(returncode=None)
        with patch.object(self.server.asyncio, "create_subprocess_exec", AsyncMock(return_value=proc)) as spawn, \
             patch.object(self.server.asyncio, "create_task", lambda c: c.close()):
            self.assertTrue(await self.server.gw.start())
        spawn.assert_awaited_once()
        self.server.gw.proc = None

    async def _save(self, vars_, active_key=None):
        body = {"vars": vars_, "_restart": False}
        if active_key is not None:
            body["_active_provider_key"] = active_key
        resp = await self.server.api_config_put(_Request(body))
        self.assertEqual(resp.status_code, 200)

    # -- tests -----------------------------------------------------------
    async def test_explicit_native_pair_survives_every_ordinary_stage(self):
        for provider, model, key in NATIVE_PAIRS:
            with self.subTest(provider=provider):
                self.pin_calls.clear()
                (self.home / "config.yaml").unlink(missing_ok=True)
                (self.home / ".env").unlink(missing_ok=True)
                want = (provider, model)
                # The saved pair is the sacred state: OpenRouter + Gemini keys
                # are present and .env carries a stale model that must not win.
                self._write_config(
                    {"model": {"default": model, "provider": provider},
                     "mcp_servers": {"x": {"command": "y"}}},
                    version=45,
                )
                self._write_env(OPENROUTER_API_KEY="dummy-or", GEMINI_API_KEY="dummy-gem",
                                LLM_MODEL="stale-env-model")
                auth = self.home / "auth.json"
                auth.unlink(missing_ok=True)
                if provider == "openai-codex":
                    # Native ChatGPT OAuth providers only start with stored grants.
                    auth.write_text(json.dumps({"credential_pool": {"openai-codex": [
                        {"access_token": "dummy-access", "refresh_token": "dummy-refresh"}]}}),
                        encoding="utf-8")

                migrated = self._seed_and_migrate()
                self.assertEqual(self._pair(), want, "after seed_home + migrate")
                self.assertEqual(self._config()["mcp_servers"], {"x": {"command": "y"}})
                if not migrated:
                    self.skipTest("Hermes image migrator not installed; migrate stage unverified")

                await self._start_gateway()
                self.assertEqual(self._pair(), want, "after gateway start")
                await self._start_gateway()  # restart replay reads the same stale .env
                self.assertEqual(self._pair(), want, "after restart replay")

                # Ordinary saves: no provider pick, and a provider key saved
                # with the UI flag but WITHOUT LLM_MODEL (stale .env fallback).
                await self._save({"OPENROUTER_API_KEY": "dummy-or"})
                self.assertEqual(self._pair(), want, "after save without provider pick")
                await self._save({"OPENROUTER_API_KEY": "dummy-or"}, "OPENROUTER_API_KEY")
                self.assertEqual(self._pair(), want, "after provider key save without LLM_MODEL")
                await self._save({"GEMINI_API_KEY": "dummy-gem"}, "GEMINI_API_KEY")
                self.assertEqual(self._pair(), want, "after gemini key save without LLM_MODEL")
                await self._start_gateway()
                self.assertEqual(self._pair(), want, "after save then restart")
                self.assertEqual(self.pin_calls, [], "ordinary startup/save must not pin")

    async def test_first_setup_seeds_from_env_and_pins_explicit_selection(self):
        # Fresh volume: no config.yaml, no .env.
        _load("prepare_hermes").seed_home(self.home, HERMES_REPO)
        self.assertTrue((self.home / "config.yaml").is_file())
        self.assertEqual((self.home / ".env").read_text(encoding="utf-8"), "")
        await self._save({"OPENROUTER_API_KEY": "dummy-or", "LLM_MODEL": "vendor/first-model"},
                         "OPENROUTER_API_KEY")
        self.assertEqual(self.pin_calls, [("openrouter", "vendor/first-model")])
        self.assertEqual(self._pair(), ("openrouter", "vendor/first-model"))
        # Subsequent ordinary boot keeps what first setup produced.
        await self._start_gateway()
        self.assertEqual(self._pair(), ("openrouter", "vendor/first-model"))
        self.assertEqual(len(self.pin_calls), 1)

    async def test_first_start_without_config_seeds_model_from_env(self):
        self._write_env(OPENROUTER_API_KEY="dummy-or", LLM_MODEL="seed-model")
        await self._start_gateway()
        self.assertEqual(self._config()["model"]["default"], "seed-model")
        self.assertEqual(self.pin_calls, [])

    async def test_intentional_provider_switch_pins_new_pair(self):
        self._write_config({"model": {"default": "gpt-6.1-sol", "provider": "openai-codex"}})
        self._write_env(OPENROUTER_API_KEY="dummy-or", GEMINI_API_KEY="dummy-gem",
                        LLM_MODEL="stale-env-model")
        await self._save({"GEMINI_API_KEY": "dummy-gem", "LLM_MODEL": "gemini-3-pro-preview"},
                         "GEMINI_API_KEY")
        self.assertEqual(self.pin_calls, [("gemini", "gemini-3-pro-preview")])
        self.assertEqual(self._pair(), ("gemini", "gemini-3-pro-preview"))
        # ...and the new explicit pair is itself preserved by the next boot.
        await self._start_gateway()
        self.assertEqual(self._pair(), ("gemini", "gemini-3-pro-preview"))

    async def test_same_model_value_with_provider_pick_still_pins(self):
        # The UI re-picking a provider while LLM_MODEL equals the stale .env
        # value is an explicit selection, not a stale replay.
        self._write_config({"model": {"default": "gpt-6.1-sol", "provider": "openai-codex"}})
        self._write_env(OPENROUTER_API_KEY="dummy-or", LLM_MODEL="same-model")
        await self._save({"OPENROUTER_API_KEY": "dummy-or", "LLM_MODEL": "same-model"},
                         "OPENROUTER_API_KEY")
        self.assertEqual(self.pin_calls, [("openrouter", "same-model")])
        self.assertEqual(self._pair(), ("openrouter", "same-model"))

    async def test_reset_empties_model_block(self):
        self._write_config({"model": {"default": "gpt-6.1-sol", "provider": "openai-codex"},
                            "mcp_servers": {"x": {}}})
        self._write_env(OPENROUTER_API_KEY="dummy-or", LLM_MODEL="stale-env-model")
        self.server.gw.stop = AsyncMock()
        resp = await self.server.api_config_reset(object())
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(self._config()["model"], {"default": ""})
        self.assertEqual(self._config()["mcp_servers"], {"x": {}})
        self.assertFalse((self.home / ".env").exists())
        self.assertEqual(self.pin_calls, [])


if __name__ == "__main__":
    unittest.main()
