#!/usr/bin/env python3
"""Prepare persistent Hermes state before the supervised children can start.

Run with the image-owned HERMES_PYTHON, not a selected writable generation. The
admin server owns the deadline and process group, so setup remains reachable
while PM acquires dependencies and a cancelled worker cannot leave children.
"""

from __future__ import annotations

import argparse
import contextlib
import json
import os
import shlex
import subprocess
import sys
from pathlib import Path


EVENT_STREAM = sys.stdout


def emit(phase: str, message: str, **fields: object) -> None:
    print(json.dumps({"phase": phase, "message": message, **fields}), file=EVENT_STREAM, flush=True)


def seed_home(home: Path, project: Path) -> None:
    """Seed missing files after journal recovery, without following links."""
    if __package__:
        from .migrate_hermes_configs import _file_exists_without_following_links, _read_regular
    else:  # The supervised worker runs directly with the image-owned interpreter.
        from migrate_hermes_configs import _file_exists_without_following_links, _read_regular

    for path in (home / "config.yaml", home / ".env"):
        if _file_exists_without_following_links(path):
            continue
        contents = _read_regular(project / "cli-config.yaml.example")[0] if path.name == "config.yaml" else b""
        try:
            fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0), 0o600)
        except FileExistsError:
            _file_exists_without_following_links(path)
            continue
        with os.fdopen(fd, "wb") as stream:
            stream.write(contents)
            stream.flush()
            os.fsync(stream.fileno())
        parent_fd = os.open(home, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_NOFOLLOW", 0))
        try:
            os.fsync(parent_fd)
        finally:
            os.close(parent_fd)


def _dependency_notice(integration: dict, pending: bool) -> None:
    """Source presence is not SDK readiness after a failed first selection."""
    if not pending or integration.get("status") != "present":
        return
    from pm.install import venv_is_current
    from pm.paths import repo_root

    if venv_is_current(project_root=repo_root()):
        return  # Native recovery earlier in this worker may have completed it.
    integration.update(
        status="dependencies_pending",
        message=(
            "Selected plugin source was preserved, but its dependency environment "
            "could not be prepared. In native Hermes Console, run hermes pm install venv, "
            "then Apply dependencies."
        ),
        retry_command="hermes pm install venv",
    )


def prepare_integrations(home: Path, *, dependencies_pending: bool = False) -> None:
    """Invoke native, policy-aware recovery in a fresh per-home process."""
    from hermes_cli import left_core_migration, memory_provider_migration
    from hermes_constants import profile_name_for_home

    profile = profile_name_for_home(home) or "default"

    def report(message: str) -> None:
        emit("integrations", message.strip(), profile=profile, warning="⚠" in message)

    # These hooks skip installed source, honor lazy-install policy and retry
    # backoff, and install only the catalog versions of selected former builtins.
    # They do not update an existing (possibly customized) user plugin.
    left_core_migration.recover_at_startup(say=report)
    for feature in left_core_migration.LEFT_CORE:
        if feature.in_use(home, process_env=True):
            present = left_core_migration.plugin_present(feature.plugin, home)
            integration = {
                "profile": profile, "name": feature.plugin,
                "status": "present" if present else "missing",
                "message": "Selected plugin source is present." if present else "Selected plugin is unavailable.",
            }
            if not present:
                integration["retry_command"] = shlex.join(["hermes", "-p", profile, "plugins", "install", feature.plugin])
            _dependency_notice(integration, dependencies_pending)
            emit("integrations", integration["message"], profile=profile, integration=integration)
    name = memory_provider_migration.configured_provider(home)
    if not name:
        return
    from agent.memory_provider import is_core_memory_provider
    if is_core_memory_provider(name):
        return
    if not memory_provider_migration.provider_present(name, home):
        memory_provider_migration.recover_at_startup(name, say=report)
    present = memory_provider_migration.provider_present(name, home)
    integration: dict[str, str] = {
        "profile": profile,
        "name": name,
        "status": "present" if present else "missing",
        "message": (
            "Selected memory provider source is present; its service and credentials still need to be valid."
            if present else "Selected memory provider is unavailable; external memory is off."
        ),
    }
    if not present:
        integration["retry_command"] = shlex.join(["hermes", "-p", profile, "plugins", "install", name])
    elif name == "hindsight":
        from plugins.memory import find_provider_dir
        source = find_provider_dir(name)
        # Inspect the retired import without executing user code. A new catalog
        # version is not itself a reason to overwrite a customized installation.
        if source is not None and any(
            path.is_file() and "from tools.lazy_deps import install_specs" in path.read_text(encoding="utf-8-sig")
            for path in (source / "setup.py", source / "embedded.py")
        ):
            integration["status"] = "needs_review"
            integration["message"] = (
                "Existing Hindsight source uses retired dependency installation helpers. "
                "Review its native plugin update before setup or dependency repair; "
                "stored memories and existing plugin source were not removed."
            )
    _dependency_notice(integration, dependencies_pending)
    emit("integrations", integration["message"], profile=profile, integration=integration)


def prepare(mode: str) -> None:
    home = Path(os.environ["HERMES_HOME"])
    project = Path(os.environ.get("HERMES_REPO_ROOT", "/opt/hermes/repo")).resolve(strict=True)
    integrations: list[dict] = []
    warnings: list[str] = []
    from hermes_cli.runtime_state import recover_publication, runtime_lock

    emit("recovery", "Checking interrupted dependency publication.")
    # Recover before config snapshots or migrations change journaled digests.
    # The server bounds this writer wait by terminating the worker's group.
    with runtime_lock(project, timeout=None):
        recover_publication(project)

    seed_home(home, project)
    if __package__:
        from .migrate_hermes_configs import (
            config_homes, migrate, profile_environment, validate_profile_execution_environment,
        )
    else:
        from migrate_hermes_configs import (
            config_homes, migrate, profile_environment, validate_profile_execution_environment,
        )
    # Validate the complete volume before PM reads profile selections or a
    # subprocess loads any target home's secrets.
    homes = config_homes(home)
    for target in config_homes(home, include_env_only=True):
        validate_profile_execution_environment(target)
    from pm.recovery import refresh_dependencies
    from pm.install import venv_is_current
    emit("dependencies", "Checking dependencies against this Hermes image.")
    result = refresh_dependencies(project)
    dependencies_pending = result == "fallback"
    if result == "base" and not venv_is_current(project_root=project):
        # Pre-PM volumes can have accepted installed plugins but no writable
        # facts yet. Native sync retains the shipped extras and existing source;
        # explicit=False keeps the operator's lazy-install policy authoritative.
        from pm.client import sync_venv

        emit("dependencies", "Preparing dependencies for existing selected plugins.")
        try:
            with contextlib.redirect_stdout(sys.stderr):
                sync_venv(None, explicit=False)
            if not venv_is_current(project_root=project):
                raise RuntimeError("Dependency inputs still do not match the selected environment.")
            result = "initialized"
        except Exception as exc:
            print(f"Initial dependency preparation failed: {type(exc).__name__}: {exc}", file=sys.stderr)
            dependencies_pending = True
            result = "base; dependencies pending"
    # A native sync normally compensates its own failure. Settle any remaining
    # journal under its lock BEFORE template snapshots/migration could give a
    # journaled config a third digest. A failed recovery stops preparation.
    with runtime_lock(project, timeout=None):
        recover_publication(project)
    emit("dependencies", f"Dependency preparation: {result}.", warning=dependencies_pending)
    if dependencies_pending:
        message = (
            "Selected plugin dependencies could not be prepared; installation may be "
            "disabled or unavailable. Existing plugin source and selections were retained. "
            "In native Hermes Console, run hermes pm install venv, then Apply dependencies."
        )
        warnings.append(message)
        emit("dependencies", message, warning=True)

    emit("deliveries", "Checking previous named-profile deliveries.")
    delivery = subprocess.run(
        [sys.executable, "-E", "-s", str(Path(__file__).with_name("check_pending_deliveries.py")),
         "--hermes-home", str(home)], stdout=sys.stderr, check=False,
    )
    if delivery.returncode == 2:
        message = "Some old named-profile replies may need a new request."
        warnings.append(message)
        emit("deliveries", message, warning=True)
    elif delivery.returncode:
        raise RuntimeError("Could not verify previous delivery ledgers; preparation stopped before migration.")

    emit("migration", "Migrating root and named-profile configurations safely.")
    with contextlib.redirect_stdout(sys.stderr):
        migrate(home)

    for target in homes:
        emit("integrations", "Checking selected integrations.", profile=target.name if target != home else "default")
        child = subprocess.Popen(
            [sys.executable, "-E", "-s", str(Path(__file__)), "--profile-home", str(target),
             *(["--dependencies-pending"] if dependencies_pending else [])],
            env=profile_environment(target), stdout=subprocess.PIPE, text=True,
            encoding="utf-8", errors="replace",
        )
        assert child.stdout is not None
        with child.stdout:
            for line in child.stdout:
                print(line, end="", flush=True)
                try:
                    event = json.loads(line)
                except ValueError:
                    continue
                if not isinstance(event, dict):
                    continue
                if event.get("warning"):
                    warnings.append(str(event.get("message", "")))
                if isinstance(event.get("integration"), dict):
                    integrations.append(event["integration"])
        if child.wait():
            message = f"Selected integrations could not be checked for {target.name if target != home else 'default'}; check the preparation logs."
            warnings.append(message)
            emit("integrations", message, warning=True)
    emit("complete", "Hermes preparation completed.", mode=mode, integrations=integrations, warnings=warnings)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mode", choices=("startup", "restore"), default="startup")
    parser.add_argument("--profile-home", type=Path, help=argparse.SUPPRESS)
    parser.add_argument("--dependencies-pending", action="store_true", help=argparse.SUPPRESS)
    args = parser.parse_args()
    try:
        if args.profile_home is not None:
            # Native helpers often print status; emit() retains the event stream.
            with contextlib.redirect_stdout(sys.stderr):
                prepare_integrations(args.profile_home, dependencies_pending=args.dependencies_pending)
        else:
            prepare(args.mode)
    except Exception as exc:
        emit("failed", str(exc), warning=True)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
