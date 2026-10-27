#!/usr/bin/env python3
"""Run Hermes' Docker config migration for the root and live named profiles.

The upstream Docker hook migrates only one HERMES_HOME. This template starts a
single gateway from the root home, which can also serve named profiles, so a
redeploy must migrate each persisted profile before that gateway starts.
"""

from __future__ import annotations

import os
import re
import stat
import subprocess
import sys
import tempfile
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

import hermes_yaml as yaml


UPSTREAM_MIGRATOR = Path(os.environ.get("HERMES_REPO_ROOT", "/opt/hermes/repo")) / "scripts/docker_config_migrate.py"
SUPPORT_FLOOR_VERSION = 12  # upstream hermes_cli.config_migrations
SOUL_REWRITE_VERSION = 41  # upstream _migrate_to_41 touches the entire profile roster
# Matches hermes_constants.PROFILE_ID_RE in v0.22.2. Keep in step on bumps.
PROFILE_ID_RE = re.compile(r"^[a-z0-9][a-z0-9_-]{0,63}$")


@dataclass(frozen=True)
class Snapshot:
    path: Path
    contents: bytes
    mode: int
    backup: Path


@dataclass(frozen=True)
class MigrationJob:
    home: Path
    config: dict
    stamp: int | None


def _read_regular(path: Path) -> tuple[bytes, int]:
    """Read a regular file through O_NOFOLLOW, including its permission mode."""
    fd = os.open(path, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0))
    try:
        info = os.fstat(fd)
        if not stat.S_ISREG(info.st_mode):
            raise RuntimeError(f"refusing non-regular Hermes file: {path}")
        with os.fdopen(fd, "rb", closefd=False) as stream:
            return stream.read(), stat.S_IMODE(info.st_mode)
    finally:
        os.close(fd)


def _parse_config_bytes(raw: bytes, path: Path) -> tuple[dict | None, int | None]:
    """Return a valid mapping and its stamp, or warn and leave bad YAML alone."""
    try:
        config = yaml.safe_load(raw.decode("utf-8"))
    except (UnicodeError, yaml.YAMLError) as exc:
        print(
            f"[template-config-migrate] WARNING: cannot parse {path}: {exc}; "
            "leaving config.yaml untouched",
            file=sys.stderr,
        )
        return None, None
    if config is None:
        config = {}
    if not isinstance(config, dict):
        print(
            f"[template-config-migrate] WARNING: {path} must contain a YAML mapping; "
            "leaving config.yaml untouched",
            file=sys.stderr,
        )
        return None, None
    if "_config_version" not in config:
        return config, None
    value = config["_config_version"]
    if isinstance(value, bool):
        return config, 0
    try:
        return config, max(0, int(value))
    except (TypeError, ValueError):
        return config, 0


def _parsed_config(path: Path) -> tuple[dict | None, int | None]:
    return _parse_config_bytes(_read_regular(path)[0], path)


IMAGE_EXECUTION_ENV_KEYS = (
    "HERMES_RUNTIME_DIR", "HERMES_PYTHON", "PYTHONPATH", "PYTHONHOME", "VIRTUAL_ENV",
)


def validate_profile_execution_environment(home: Path) -> None:
    """Refuse incompatible stored interpreter settings without rewriting the file.

    Native dotenv can reload with override=True after spawn. Restoring the child
    env alone cannot establish image ownership. This parser only reads dotenv;
    it does not hydrate external secret sources or execute profile code.
    """
    from agent.secret_scope import load_env_file

    path = home / ".env"
    if not _file_exists_without_following_links(path):
        return
    _read_regular(path)  # native load_env_file intentionally treats unreadable as empty
    values = load_env_file(path)
    conflicts = [
        key for key in IMAGE_EXECUTION_ENV_KEYS
        if values.get(key) not in (None, "") and values[key] != os.environ.get(key)
    ]
    if conflicts:
        raise RuntimeError(
            f"{path} contains conflicting runtime overrides: {', '.join(conflicts)}. "
            "The file was preserved. Remove or update these settings to match this "
            "image using an operator volume edit, then retry preparation."
        )


def profile_environment(home: Path) -> dict[str, str]:
    """Use the target home's secrets, never a launch profile's inherited keys."""
    validate_profile_execution_environment(home)
    from tools.environments.local import served_profile_child_env

    env = served_profile_child_env(
        base=os.environ, target_home=home, inherit_credentials=True,
    )
    for key in IMAGE_EXECUTION_ENV_KEYS:
        if key in os.environ:
            env[key] = os.environ[key]
        else:
            env.pop(key, None)
    env["HERMES_HOME"] = str(home)
    env["HERMES_SUPERVISED_CHILD"] = "1"
    env["HERMES_NONINTERACTIVE"] = "1"
    # This legacy target contains Python 3.12 extensions. PM owns the 3.14
    # dependency environment; retain the old bytes without making them importable.
    env.pop("HERMES_LAZY_INSTALL_TARGET", None)
    return env


def _target_config_version(migrator: Path) -> int:
    """Read the schema stamp shipped with the same Hermes checkout as the hook."""
    example = migrator.parent.parent / "cli-config.yaml.example"
    if not _file_exists_without_following_links(example):
        raise RuntimeError(f"Hermes config schema example is missing: {example}")
    data, version = _parsed_config(example)
    if data is None or version is None or version < 1:
        raise RuntimeError(f"Hermes config schema example lacks a valid version: {example}")
    return version


def _legacy_disabled_names(config: dict) -> set[str]:
    def disabled(value: object) -> bool:
        # Mirrors tools_config._parse_enabled_flag(value, default=False) in
        # v2026.9.24, which the 45→46 upstream migration itself uses.
        if isinstance(value, (bool, int)):
            return bool(value)
        if isinstance(value, str):
            return value.strip().lower() in {"true", "1", "yes", "on"}
        return False

    servers = config.get("mcp_servers")
    if not isinstance(servers, dict):
        return set()
    return {
        name for name, entry in servers.items()
        if isinstance(name, str) and isinstance(entry, dict) and disabled(entry.get("disabled"))
    }


def _backup_file(path: Path, backup_dir: Path) -> Snapshot:
    contents, mode = _read_regular(path)
    prefix = f"{path.name}.pre-docker-migrate."
    # Reuse only the newest matching secure snapshot after a failed attempt.
    # Upstream may prune older snapshots, so an older match is not sufficient.
    candidates = sorted(
        (item for item in backup_dir.iterdir() if item.name.startswith(prefix)),
        key=lambda item: item.name,
        reverse=True,
    )
    if candidates:
        backed_up, backup_mode = _read_regular(candidates[0])
        if backed_up == contents and backup_mode & 0o077 == 0:
            return Snapshot(path, contents, mode, candidates[0])

    name = prefix + datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S") + f"-{uuid.uuid4().hex[:8]}"
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0)
    dir_flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_NOFOLLOW", 0)
    dir_fd = os.open(backup_dir, dir_flags)
    try:
        fd = os.open(name, flags, 0o600, dir_fd=dir_fd)
        try:
            with os.fdopen(fd, "wb", closefd=False) as stream:
                stream.write(contents)
                stream.flush()
            os.fsync(fd)
        finally:
            os.close(fd)
        os.fsync(dir_fd)
    finally:
        os.close(dir_fd)
    backup = backup_dir / name
    if _read_regular(backup)[0] != contents:
        raise RuntimeError(f"pre-migration backup verification failed: {backup}")
    return Snapshot(path, contents, mode, backup)


def _ensure_backup_dir(home: Path) -> Path:
    backup_root = home / "backups"
    backup_dir = backup_root / "config"
    for directory in (backup_root, backup_dir):
        created = False
        try:
            directory.mkdir(mode=0o700)
            created = True
        except FileExistsError:
            pass
        if not _directory_if_present(directory):
            raise RuntimeError(f"cannot create migration backup directory: {directory}")
        if created:
            parent_fd = os.open(
                directory.parent,
                os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_NOFOLLOW", 0),
            )
            try:
                os.fsync(parent_fd)
            finally:
                os.close(parent_fd)
    return backup_dir


def _restore_snapshot(snapshot: Snapshot) -> None:
    """Replace a changed file with the pre-migration bytes held in memory."""
    fd, temporary = tempfile.mkstemp(prefix=f".{snapshot.path.name}.restore-", dir=snapshot.path.parent)
    try:
        os.fchmod(fd, snapshot.mode)
        with os.fdopen(fd, "wb") as stream:
            stream.write(snapshot.contents)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, snapshot.path)
        directory_fd = os.open(
            snapshot.path.parent,
            os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_NOFOLLOW", 0),
        )
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


def _file_exists_without_following_links(path: Path) -> bool:
    """Return whether a regular file exists, rejecting links and special files."""
    try:
        mode = path.lstat().st_mode
    except FileNotFoundError:
        return False
    if not stat.S_ISREG(mode):
        raise RuntimeError(f"refusing non-regular Hermes file: {path}")
    return True


def _directory_if_present(path: Path, *, allow_regular_file: bool = False) -> bool:
    """Return whether a directory exists without following a symlink."""
    try:
        mode = path.lstat().st_mode
    except FileNotFoundError:
        return False
    if stat.S_ISDIR(mode):
        return True
    # Hermes itself ignores stray regular files in its optional profiles tree.
    if allow_regular_file and stat.S_ISREG(mode):
        return False
    raise RuntimeError(f"refusing non-directory Hermes path: {path}")


def config_homes(root: Path, *, include_env_only: bool = False) -> list[Path]:
    """Select real config homes, optionally including live dotenv-only homes.

    A config file itself is an upstream profile identity marker. A tombstoned
    profile is excluded even if a stale process recreated its directory.
    Enumeration is completed before any migration so an unsafe path cannot
    cause a half-migrated volume.
    """
    if not _directory_if_present(root):
        raise RuntimeError(f"Hermes home does not exist: {root}")
    root = root.resolve(strict=True)
    homes: list[Path] = []
    if _file_exists_without_following_links(root / "config.yaml") or (
        include_env_only and _file_exists_without_following_links(root / ".env")
    ):
        _file_exists_without_following_links(root / ".env")
        _directory_if_present(root / "backups")
        _directory_if_present(root / "backups" / "config")
        homes.append(root)

    profiles = root / "profiles"
    if not _directory_if_present(profiles, allow_regular_file=True):
        return homes
    deleted = profiles / ".deleted"
    has_deleted = _directory_if_present(deleted, allow_regular_file=True)
    for entry in sorted(profiles.iterdir()):
        name = entry.name
        if name == "default" or not PROFILE_ID_RE.fullmatch(name):
            continue
        if not _directory_if_present(entry, allow_regular_file=True):
            continue
        if has_deleted and (deleted / name).is_symlink():
            continue
        if has_deleted and (deleted / name).exists():
            continue
        if _file_exists_without_following_links(entry / "config.yaml") or (
            include_env_only and _file_exists_without_following_links(entry / ".env")
        ):
            _file_exists_without_following_links(entry / ".env")
            _directory_if_present(entry / "backups")
            _directory_if_present(entry / "backups" / "config")
            homes.append(entry)
    return homes


def _soul_paths(root: Path) -> list[Path]:
    """Preflight SOUL files in the v41 migration's entire live roster.

    Upstream's roster includes named profiles that have identity markers other
    than config.yaml, so config_homes() alone cannot cover this migration.
    Checking every candidate directory and SOUL path before starting a hook
    also keeps an upstream is_file()/write_text() from following a symlink.
    """
    paths = [root / "SOUL.md"]
    profiles = root / "profiles"
    if _directory_if_present(profiles, allow_regular_file=True):
        deleted = profiles / ".deleted"
        has_deleted = _directory_if_present(deleted, allow_regular_file=True)
        for entry in sorted(profiles.iterdir()):
            if entry.name == "default" or not PROFILE_ID_RE.fullmatch(entry.name):
                continue
            if not _directory_if_present(entry, allow_regular_file=True):
                continue
            if has_deleted and ((deleted / entry.name).is_symlink() or (deleted / entry.name).exists()):
                continue
            paths.append(entry / "SOUL.md")
    for path in paths:
        _file_exists_without_following_links(path)
    return paths


def _verify_backups(snapshots: list[Snapshot]) -> str | None:
    for snapshot in snapshots:
        try:
            backed_up, backup_mode = _read_regular(snapshot.backup)
        except (OSError, RuntimeError):
            return f"pre-migration backup vanished: {snapshot.backup}"
        if backed_up != snapshot.contents or backup_mode & 0o077:
            return f"pre-migration backup changed: {snapshot.backup}"
    return None


def _rollback(snapshots: list[Snapshot], absent_paths: list[Path]) -> str:
    errors: list[str] = []
    for snapshot in snapshots:
        try:
            _restore_snapshot(snapshot)
        except (OSError, RuntimeError) as exc:
            errors.append(f"{snapshot.path}: {exc}")
    for path in absent_paths:
        try:
            if _file_exists_without_following_links(path):
                path.unlink()
        except (OSError, RuntimeError) as exc:
            errors.append(f"{path}: {exc}")
    return (
        "rollback incomplete (" + "; ".join(errors) + ")"
        if errors else "restored pre-migration files"
    )


def migrate(root: Path, migrator: Path = UPSTREAM_MIGRATOR) -> None:
    if os.environ.get("HERMES_SKIP_CONFIG_MIGRATION", "").strip().lower() in {"1", "true", "yes", "on"}:
        print("[template-config-migrate] HERMES_SKIP_CONFIG_MIGRATION is set; skipping")
        return
    if not migrator.is_file():
        raise RuntimeError(f"Hermes Docker config migrator is missing: {migrator}")
    target_version = _target_config_version(migrator)
    homes = config_homes(root)
    # Plan and validate the complete volume before any upstream hook changes a
    # config. In particular, a later profile with a future schema must not
    # leave earlier profiles migrated by the time we detect it.
    planned: list[tuple[Path, dict, int | None, bytes]] = []
    needs_soul_snapshot = False
    for home in homes:
        print(f"[template-config-migrate] Checking {home}", flush=True)
        config_path = home / "config.yaml"
        original_config, _ = _read_regular(config_path)
        config, stamp = _parse_config_bytes(original_config, config_path)
        if config is None:
            continue
        if stamp is not None and stamp < SUPPORT_FLOOR_VERSION:
            print(
                f"[template-config-migrate] WARNING: {config_path} has schema version "
                f"{stamp}, below Hermes' automatic migration floor "
                f"{SUPPORT_FLOOR_VERSION}; leaving it untouched",
                file=sys.stderr,
            )
            continue
        if stamp is not None and stamp > target_version:
            raise RuntimeError(
                f"{config_path} has schema version {stamp}, newer than this Hermes image "
                f"supports ({target_version}); restore matching data or use a newer image"
            )
        if stamp is not None and stamp >= target_version:
            continue

        planned.append((home, config, stamp, original_config))
        # The 40→41 migration runs for explicit stamps only. It calls _roster()
        # on the root home and can edit every live profile's SOUL.md, including
        # those with no config.yaml of their own.
        if stamp is not None and stamp < SOUL_REWRITE_VERSION:
            needs_soul_snapshot = True

    all_snapshots: list[Snapshot] = []
    absent_paths: list[Path] = []
    jobs: list[MigrationJob] = []
    if needs_soul_snapshot:
        soul_snapshots: list[Snapshot] = []
        for soul_path in _soul_paths(root.resolve(strict=True)):
            if _file_exists_without_following_links(soul_path):
                soul_backup_dir = _ensure_backup_dir(soul_path.parent)
                soul_snapshots.append(_backup_file(soul_path, soul_backup_dir))
            else:
                absent_paths.append(soul_path)
        all_snapshots.extend(soul_snapshots)
        print(
            "[template-config-migrate] Pre-migration SOUL backups: "
            + (", ".join(str(item.backup) for item in soul_snapshots) or "none"),
            flush=True,
        )

    for home, config, stamp, original_config in planned:
        config_path = home / "config.yaml"
        # Upstream backup_config() can warn and return None on I/O failure, then
        # continue migrating. Require our own verified, durable snapshots first.
        backup_dir = _ensure_backup_dir(home)
        snapshots = [_backup_file(config_path, backup_dir)]
        if snapshots[0].contents != original_config:
            raise RuntimeError(f"config changed during migration preflight: {config_path}")
        env_path = home / ".env"
        had_env = _file_exists_without_following_links(env_path)
        if had_env:
            snapshots.append(_backup_file(env_path, backup_dir))
        else:
            absent_paths.append(env_path)
        all_snapshots.extend(snapshots)
        jobs.append(MigrationJob(home, config, stamp))
        print(
            "[template-config-migrate] Pre-migration backups: "
            + ", ".join(str(item.backup) for item in snapshots),
            flush=True,
        )

    for job in jobs:
        home, config, stamp = job.home, job.config, job.stamp
        config_path = home / "config.yaml"
        failure: str | None = None
        try:
            env = profile_environment(home)
            if stamp == 12:
                # Suppress only upstream's obsolete LLM_MODEL cleanup; this
                # template still uses the variable, including Railway values.
                env["LLM_MODEL"] = ""
            result = subprocess.run(
                [os.environ.get("HERMES_PYTHON", sys.executable), "-E", "-s", str(migrator)],
                env=env, stdout=sys.stderr, check=False,
            )
        except Exception as exc:
            failure = f"could not start hook: {exc}"
        if failure is None and result.returncode != 0:
            failure = f"hook exited {result.returncode}"
        if failure is None:
            try:
                migrated, new_stamp = _parsed_config(config_path)
            except (OSError, RuntimeError) as exc:
                migrated, new_stamp = None, None
                failure = f"cannot read migrated config: {exc}"
            if migrated is None or new_stamp != target_version:
                failure = failure or f"config version did not reach {target_version}"
            else:
                servers = migrated.get("mcp_servers")
                for name in _legacy_disabled_names(config):
                    entry = servers.get(name) if isinstance(servers, dict) else None
                    if not isinstance(entry, dict) or entry.get("enabled") is not False or "disabled" in entry:
                        failure = f"MCP server {name!r} was not migrated to enabled: false"
                        break
            if failure is None:
                failure = _verify_backups(all_snapshots)
        if failure is not None:
            restoration = _rollback(all_snapshots, absent_paths)
            raise RuntimeError(
                f"Hermes config migration failed for {home}: {failure}; "
                f"{restoration} and stopped gateway startup"
            )


def main() -> int:
    try:
        home = Path(os.environ["HERMES_HOME"])
        migrate(home)
    except (KeyError, OSError, RuntimeError) as exc:
        print(f"[template-config-migrate] ERROR: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
