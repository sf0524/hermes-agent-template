"""Startup migration routing tests; Docker also runs the real upstream hook tests."""

from __future__ import annotations

import importlib.util
import os
import stat
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch


ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location(
    "template_config_migrator", ROOT / "template_helpers" / "migrate_hermes_configs.py"
)
assert SPEC and SPEC.loader
migration = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = migration
SPEC.loader.exec_module(migration)


class MigrationFixture:
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix="hermes-config-migration-")
        self.addCleanup(self.tmp.cleanup)
        self.home = (Path(self.tmp.name) / ".hermes").resolve()
        self.home.mkdir()

    def profile(self, name: str, config: str | None = None) -> Path:
        home = self.home / "profiles" / name
        home.mkdir(parents=True)
        if config is not None:
            (home / "config.yaml").write_text(config, encoding="utf-8")
        return home

    def fake_upstream_hook(self) -> Path:
        """A subprocess stub for routing and hook failure modes, not schema QA."""
        upstream = Path(self.tmp.name) / "fake-upstream"
        scripts = upstream / "scripts"
        scripts.mkdir(parents=True, exist_ok=True)
        (upstream / "cli-config.yaml.example").write_text("_config_version: 46\n")
        path = scripts / "fake-docker-config-migrate.py"
        path.write_text(
            "import os, sys\n"
            "from pathlib import Path\n"
            "home = Path(os.environ['HERMES_HOME'])\n"
            "config = home / 'config.yaml'\n"
            "with Path(os.environ['TEST_MIGRATION_LOG']).open('a') as log:\n"
            "    log.write(str(home) + '\\n')\n"
            "if os.environ.get('TEST_ENV_LOG'):\n"
            "    Path(os.environ['TEST_ENV_LOG']).write_text(repr(os.environ.get('LLM_MODEL')))\n"
            "if os.environ.get('TEST_SOUL_PATHS'):\n"
            "    for filename in os.environ['TEST_SOUL_PATHS'].split(os.pathsep):\n"
            "        soul = Path(filename)\n"
            "        if not list((soul.parent / 'backups' / 'config').glob('SOUL.md.pre-docker-migrate.*')):\n"
            "            sys.exit(10)\n"
            "        soul.write_text('hook changed SOUL\\n')\n"
            "backups = list((home / 'backups' / 'config').glob('config.yaml.pre-docker-migrate.*'))\n"
            "if not backups:\n"
            "    sys.exit(9)\n"
            "if home.name == os.environ.get('TEST_FAIL_HOME'):\n"
            "    sys.exit(7)\n"
            "behavior = os.environ.get('TEST_FAKE_BEHAVIOR', '')\n"
            "if behavior == 'skip_all':\n"
            "    sys.exit(0)\n"
            "raw = config.read_text()\n"
            "raw = '\\n'.join(line for line in raw.splitlines() if not line.startswith('_config_version:')) + '\\n'\n"
            "if behavior != 'skip_mcp':\n"
            "    raw = raw.replace('disabled: true', 'enabled: false')\n"
            "config.write_text('_config_version: 46\\n' + raw)\n"
            "if behavior == 'delete_backup':\n"
            "    backups[0].unlink()\n",
            encoding="utf-8",
        )
        return path

class StartupMigrationTests(MigrationFixture, unittest.TestCase):
    def test_fresh_root_and_real_named_profiles_only(self):
        (self.home / "config.yaml").write_text("_config_version: 46\n", encoding="utf-8")
        work = self.profile("work", "_config_version: 45\n")
        self.profile("ghost")
        self.profile("Invalid", "_config_version: 45\n")
        (self.home / "profiles" / "notes").write_text("not a profile\n")
        deleted = self.profile("retired", "_config_version: 45\n")
        tombstones = self.home / "profiles" / ".deleted"
        tombstones.mkdir()
        (tombstones / deleted.name).write_text("deleted\n", encoding="utf-8")

        self.assertEqual(migration.config_homes(self.home), [self.home, work])
        hook = self.fake_upstream_hook()
        log = Path(self.tmp.name) / "calls.txt"
        with patch.dict(os.environ, {"TEST_MIGRATION_LOG": str(log)}):
            migration.migrate(self.home, hook)
        self.assertEqual(log.read_text().splitlines(), [str(work)])

    def test_unversioned_and_v45_profiles_are_both_routed(self):
        (self.home / "config.yaml").write_text(
            "mcp_servers:\n  legacy:\n    disabled: true\n", encoding="utf-8"
        )
        work = self.profile(
            "work", "_config_version: 45\nmcp_servers:\n  legacy:\n    disabled: true\n"
        )
        self.assertEqual(migration.config_homes(self.home), [self.home, work])
        hook = self.fake_upstream_hook()
        log = Path(self.tmp.name) / "calls.txt"
        with patch.dict(os.environ, {"TEST_MIGRATION_LOG": str(log)}):
            migration.migrate(self.home, hook)
        self.assertEqual(log.read_text().splitlines(), [str(self.home), str(work)])
        for home in (self.home, work):
            config = (home / "config.yaml").read_text()
            self.assertIn("_config_version: 46", config)
            self.assertIn("enabled: false", config)
            self.assertNotIn("disabled: true", config)
            self.assertEqual(
                len(list((home / "backups" / "config").glob("config.yaml.pre-docker-migrate.*"))),
                1,
            )

    def test_malformed_config_is_not_modified_by_wrapper_and_other_home_is_checked(self):
        invalid = "mcp_servers: [\n"
        (self.home / "config.yaml").write_text(invalid, encoding="utf-8")
        work = self.profile("work", "_config_version: 45\n")
        hook = self.fake_upstream_hook()
        log = Path(self.tmp.name) / "calls.txt"
        with patch.dict(os.environ, {"TEST_MIGRATION_LOG": str(log)}):
            migration.migrate(self.home, hook)
        self.assertEqual((self.home / "config.yaml").read_text(), invalid)
        self.assertEqual(log.read_text().splitlines(), [str(work)])

    def test_second_boot_checks_the_same_homes_without_wrapper_writes(self):
        original = "_config_version: 45\n"
        (self.home / "config.yaml").write_text(original, encoding="utf-8")
        hook = self.fake_upstream_hook()
        log = Path(self.tmp.name) / "calls.txt"
        with patch.dict(os.environ, {"TEST_MIGRATION_LOG": str(log)}):
            migration.migrate(self.home, hook)
            migration.migrate(self.home, hook)
        self.assertIn("_config_version: 46", (self.home / "config.yaml").read_text())
        self.assertEqual(log.read_text().splitlines(), [str(self.home)])

    def test_failure_stops_before_later_profiles(self):
        (self.home / "config.yaml").write_text("_config_version: 45\n")
        self.profile("alpha", "_config_version: 45\n")
        self.profile("beta", "_config_version: 45\n")
        hook = self.fake_upstream_hook()
        log = Path(self.tmp.name) / "calls.txt"
        with patch.dict(
            os.environ,
            {"TEST_MIGRATION_LOG": str(log), "TEST_FAIL_HOME": "alpha"},
        ):
            with self.assertRaisesRegex(RuntimeError, "stopped gateway startup"):
                migration.migrate(self.home, hook)
        self.assertEqual(
            log.read_text().splitlines(),
            [str(self.home), str(self.home / "profiles" / "alpha")],
        )
        self.assertEqual(
            (self.home / "profiles" / "alpha" / "config.yaml").read_text(),
            "_config_version: 45\n",
        )
        self.assertEqual((self.home / "config.yaml").read_text(), "_config_version: 45\n")

    def test_v40_cross_home_soul_changes_and_later_failure_restore_whole_volume(self):
        root_config = self.home / "config.yaml"
        root_config.write_text("_config_version: 40\n", encoding="utf-8")
        root_soul = self.home / "SOUL.md"
        root_soul.write_text("root before\n", encoding="utf-8")
        alpha = self.profile("alpha", "_config_version: 40\n")
        alpha_soul = alpha / "SOUL.md"
        alpha_soul.write_text("alpha before\n", encoding="utf-8")
        # This profile has no config.yaml, but upstream's v41 roster includes
        # it because SOUL.md itself is a profile identity marker.
        notes = self.profile("notes")
        notes_soul = notes / "SOUL.md"
        notes_soul.write_text("notes before\n", encoding="utf-8")
        (self.home / ".env").write_text("ROOT_KEY=before\n", encoding="utf-8")
        hook = self.fake_upstream_hook()
        log = Path(self.tmp.name) / "calls.txt"
        with patch.dict(os.environ, {
            "TEST_MIGRATION_LOG": str(log),
            "TEST_FAIL_HOME": "alpha",
            "TEST_SOUL_PATHS": os.pathsep.join(map(str, (root_soul, alpha_soul, notes_soul))),
        }):
            with self.assertRaisesRegex(RuntimeError, "restored pre-migration files"):
                migration.migrate(self.home, hook)

        self.assertEqual(log.read_text().splitlines(), [str(self.home), str(alpha)])
        self.assertEqual(root_config.read_text(), "_config_version: 40\n")
        self.assertEqual((alpha / "config.yaml").read_text(), "_config_version: 40\n")
        self.assertEqual((self.home / ".env").read_text(), "ROOT_KEY=before\n")
        for soul, original in (
            (root_soul, "root before\n"),
            (alpha_soul, "alpha before\n"),
            (notes_soul, "notes before\n"),
        ):
            self.assertEqual(soul.read_text(), original)
            backups = list((soul.parent / "backups" / "config").glob("SOUL.md.pre-docker-migrate.*"))
            self.assertEqual(len(backups), 1)
            self.assertEqual(backups[0].read_text(), original)
            self.assertEqual(stat.S_IMODE(backups[0].stat().st_mode), 0o600)

    def test_v40_soul_symlink_in_profile_without_config_blocks_all_hooks(self):
        config = self.home / "config.yaml"
        config.write_text("_config_version: 40\n", encoding="utf-8")
        outside = Path(self.tmp.name) / "outside-soul.md"
        outside.write_text("outside before\n", encoding="utf-8")
        no_config_profile = self.profile("notes")
        (no_config_profile / "SOUL.md").symlink_to(outside)
        hook = self.fake_upstream_hook()
        log = Path(self.tmp.name) / "calls.txt"
        with patch.dict(os.environ, {"TEST_MIGRATION_LOG": str(log)}):
            with self.assertRaisesRegex(RuntimeError, "non-regular Hermes file"):
                migration.migrate(self.home, hook)
        self.assertFalse(log.exists())
        self.assertEqual(config.read_text(), "_config_version: 40\n")
        self.assertEqual(outside.read_text(), "outside before\n")

    def test_future_version_in_later_profile_blocks_before_first_hook(self):
        root_config = self.home / "config.yaml"
        root_config.write_text("_config_version: 45\n", encoding="utf-8")
        self.profile("work", "_config_version: 47\n")
        hook = self.fake_upstream_hook()
        log = Path(self.tmp.name) / "calls.txt"
        with patch.dict(os.environ, {"TEST_MIGRATION_LOG": str(log)}):
            with self.assertRaisesRegex(RuntimeError, "newer than this Hermes image"):
                migration.migrate(self.home, hook)
        self.assertFalse(log.exists())
        self.assertEqual(root_config.read_text(), "_config_version: 45\n")

    def test_successful_hook_that_skips_mcp_conversion_is_rolled_back(self):
        original = "_config_version: 45\nmcp_servers:\n  old:\n    disabled: true\n"
        config = self.home / "config.yaml"
        config.write_text(original)
        hook = self.fake_upstream_hook()
        log = Path(self.tmp.name) / "calls.txt"
        with patch.dict(
            os.environ,
            {"TEST_MIGRATION_LOG": str(log), "TEST_FAKE_BEHAVIOR": "skip_mcp"},
        ):
            with self.assertRaisesRegex(RuntimeError, "was not migrated"):
                migration.migrate(self.home, hook)
        self.assertEqual(config.read_text(), original)
        self.assertEqual(log.read_text().splitlines(), [str(self.home)])

    def test_successful_hook_that_skips_version_change_is_rolled_back(self):
        original = "_config_version: 45\n"
        config = self.home / "config.yaml"
        config.write_text(original)
        hook = self.fake_upstream_hook()
        log = Path(self.tmp.name) / "calls.txt"
        with patch.dict(
            os.environ,
            {"TEST_MIGRATION_LOG": str(log), "TEST_FAKE_BEHAVIOR": "skip_all"},
        ):
            with self.assertRaisesRegex(RuntimeError, "did not reach 46"):
                migration.migrate(self.home, hook)
        self.assertEqual(config.read_text(), original)

    def test_hook_cannot_succeed_after_removing_pre_migration_backup(self):
        original = "_config_version: 45\n"
        config = self.home / "config.yaml"
        config.write_text(original)
        hook = self.fake_upstream_hook()
        log = Path(self.tmp.name) / "calls.txt"
        with patch.dict(
            os.environ,
            {"TEST_MIGRATION_LOG": str(log), "TEST_FAKE_BEHAVIOR": "delete_backup"},
        ):
            with self.assertRaisesRegex(RuntimeError, "backup vanished"):
                migration.migrate(self.home, hook)
        self.assertEqual(config.read_text(), original)

    def test_migration_never_starts_without_safe_backup_directory(self):
        config = self.home / "config.yaml"
        config.write_text("_config_version: 45\n")
        outside = Path(self.tmp.name) / "outside-backups"
        outside.mkdir()
        (self.home / "backups").symlink_to(outside, target_is_directory=True)
        hook = self.fake_upstream_hook()
        log = Path(self.tmp.name) / "calls.txt"
        with patch.dict(os.environ, {"TEST_MIGRATION_LOG": str(log)}):
            with self.assertRaisesRegex(RuntimeError, "non-directory"):
                migration.migrate(self.home, hook)
        self.assertEqual(config.read_text(), "_config_version: 45\n")
        self.assertFalse(log.exists())

    def test_below_support_floor_is_preserved_with_a_warning(self):
        original = "_config_version: 11\nmcp_servers:\n  old:\n    disabled: true\n"
        config = self.home / "config.yaml"
        config.write_text(original)
        self.assertEqual(migration._parsed_config(config)[1], 11)
        hook = self.fake_upstream_hook()
        log = Path(self.tmp.name) / "calls.txt"
        with patch.dict(os.environ, {"TEST_MIGRATION_LOG": str(log)}):
            migration.migrate(self.home, hook)
        self.assertEqual(config.read_text(), original)
        self.assertFalse(log.exists())

    def test_newer_volume_schema_refuses_downgrade_without_migration(self):
        original = "_config_version: 47\nmodel:\n  default: keep-me\n"
        config = self.home / "config.yaml"
        config.write_text(original, encoding="utf-8")
        hook = self.fake_upstream_hook()
        log = Path(self.tmp.name) / "calls.txt"
        with patch.dict(os.environ, {"TEST_MIGRATION_LOG": str(log)}):
            with self.assertRaisesRegex(RuntimeError, "newer than this Hermes image"):
                migration.migrate(self.home, hook)
        self.assertEqual(config.read_text(encoding="utf-8"), original)
        self.assertFalse(log.exists())

    def test_v12_migration_masks_only_upstream_obsolete_model_cleanup(self):
        (self.home / "config.yaml").write_text("_config_version: 12\n")
        (self.home / ".env").write_text("LLM_MODEL=volume-model\n")
        hook = self.fake_upstream_hook()
        log = Path(self.tmp.name) / "calls.txt"
        env_log = Path(self.tmp.name) / "env.txt"
        with patch.dict(os.environ, {
            "TEST_MIGRATION_LOG": str(log), "TEST_ENV_LOG": str(env_log),
            "LLM_MODEL": "railway-model",
        }):
            migration.migrate(self.home, hook)
        self.assertEqual(env_log.read_text(), "''")
        self.assertEqual((self.home / ".env").read_text(), "LLM_MODEL=volume-model\n")
        self.assertEqual(log.read_text().splitlines(), [str(self.home)])

    def test_symlinked_profile_or_config_is_rejected_before_any_migration(self):
        (self.home / "config.yaml").write_text("_config_version: 45\n")
        outside = Path(self.tmp.name) / "outside"
        outside.mkdir()
        (outside / "config.yaml").write_text("_config_version: 45\n")
        (self.home / "profiles").mkdir()
        (self.home / "profiles" / "linked").symlink_to(outside, target_is_directory=True)
        with self.assertRaisesRegex(RuntimeError, "non-directory"):
            migration.config_homes(self.home)

        (self.home / "profiles" / "linked").unlink()
        work = self.profile("work")
        (work / "config.yaml").symlink_to(outside / "config.yaml")
        with self.assertRaisesRegex(RuntimeError, "non-regular"):
            migration.config_homes(self.home)


@unittest.skipUnless(
    migration.UPSTREAM_MIGRATOR.is_file(),
    "runs against the installed Hermes hook in the local Docker image",
)
class RealUpstreamHookTests(MigrationFixture, unittest.TestCase):
    """Exercise actual backup, migration, malformed-file, and idempotence behavior."""

    def test_real_hook_migrates_root_and_named_profiles_once(self):
        root_config = self.home / "config.yaml"
        root_config.write_text(
            "_config_version: 45\nmcp_servers:\n  old:\n    disabled: true\n",
            encoding="utf-8",
        )
        (self.home / ".env").write_text("ROOT_KEY=value\n", encoding="utf-8")
        work = self.profile("work", "mcp_servers:\n  old:\n    disabled: true\n")
        (work / ".env").write_text("WORK_KEY=value\n", encoding="utf-8")

        migration.migrate(self.home)
        for home in (self.home, work):
            config = (home / "config.yaml").read_text(encoding="utf-8")
            self.assertIn(f"_config_version: {migration._target_config_version(migration.UPSTREAM_MIGRATOR)}", config)
            self.assertIn("enabled: false", config)
            self.assertNotIn("disabled: true", config)
            backups = list((home / "backups" / "config").glob("config.yaml.pre-docker-migrate.*"))
            self.assertEqual(len(backups), 1)
            env_backups = list((home / "backups" / "config").glob(".env.pre-docker-migrate.*"))
            self.assertEqual(len(env_backups), 1)

        first = [root_config.read_bytes(), (work / "config.yaml").read_bytes()]
        migration.migrate(self.home)
        self.assertEqual(
            first, [root_config.read_bytes(), (work / "config.yaml").read_bytes()]
        )

    def test_real_hook_leaves_malformed_home_untouched_and_migrates_other_home(self):
        bad = self.home / "config.yaml"
        bad.write_text("mcp_servers: [\n", encoding="utf-8")
        work = self.profile("work", "_config_version: 45\n")
        migration.migrate(self.home)
        self.assertEqual(bad.read_text(), "mcp_servers: [\n")
        self.assertIn(f"_config_version: {migration._target_config_version(migration.UPSTREAM_MIGRATOR)}", (work / "config.yaml").read_text())

    def test_real_hook_preserves_template_model_env_from_v12(self):
        (self.home / "config.yaml").write_text("_config_version: 12\nmodel:\n  default: config-model\n")
        (self.home / ".env").write_text("LLM_MODEL=volume-model\n")
        migration.migrate(self.home)
        self.assertIn("LLM_MODEL=volume-model", (self.home / ".env").read_text())

        work = self.profile("work", "_config_version: 12\nmodel:\n  default: config-model\n")
        with patch.dict(os.environ, {"LLM_MODEL": "railway-model"}):
            migration.migrate(self.home)
        self.assertFalse((work / ".env").exists())

    def test_real_v41_hook_covers_soul_in_profile_without_config(self):
        (self.home / "config.yaml").write_text("_config_version: 40\n")
        no_config_profile = self.profile("notes")
        soul = no_config_profile / "SOUL.md"
        original = (
            "# Persona\n\n"
            "## Messaging other agents\n"
            "old protocol\n\n"
            "## Keep this section\n"
            "user content\n"
        )
        soul.write_text(original, encoding="utf-8")
        migration.migrate(self.home)
        migrated = soul.read_text(encoding="utf-8")
        self.assertNotIn("## Messaging other agents", migrated)
        self.assertIn("## Keep this section", migrated)
        backups = list((no_config_profile / "backups" / "config").glob("SOUL.md.pre-docker-migrate.*"))
        self.assertEqual(len(backups), 1)
        self.assertEqual(backups[0].read_text(encoding="utf-8"), original)
        self.assertEqual(stat.S_IMODE(backups[0].stat().st_mode), 0o600)


if __name__ == "__main__":
    unittest.main()
