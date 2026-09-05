"""Resolve the Hermes home directory and the orchestrator state DB beneath it.

Mirrors server.py's own ``HERMES_HOME = os.environ.get("HERMES_HOME", str(Path.home() / ".hermes"))``
resolution so the orchestrator lands on the same profile-safe volume as every
other Hermes state DB (kanban.db, cron/executions.db, ...) instead of
inventing a second convention.
"""

from __future__ import annotations

import os
from pathlib import Path


class InvalidHermesHomeError(ValueError):
    """Raised when ``$HERMES_HOME`` is set to something unsafe to resolve."""


def get_hermes_home() -> Path:
    """The root Hermes state directory for this deployment/profile.

    Honours ``$HERMES_HOME``, falling back to ``~/.hermes`` — same rule
    server.py uses, so both processes always agree on one volume.

    An unset or empty ``$HERMES_HOME`` falls back to the default. A
    *non-empty but relative* value is rejected outright rather than resolved
    against the process's current working directory: a relative state root
    would silently move depending on where the process happens to be
    launched from, which for a durable DB location is a data-loss trap
    rather than a convenience worth supporting.
    """
    raw = os.environ.get("HERMES_HOME")
    if raw is None or raw.strip() == "":
        return Path.home() / ".hermes"
    path = Path(raw)
    if not path.is_absolute():
        raise InvalidHermesHomeError(
            f"HERMES_HOME must be an absolute path, got {raw!r}; refusing to resolve a relative "
            "path against the process's current working directory for a durable state root"
        )
    return path


def orchestrator_state_db_path() -> Path:
    """Default location of the orchestrator's durable SQLite state DB.

    ``$HERMES_HOME/orchestrator/state.db`` — namespaced under its own
    subdirectory so it backs up/restores alongside kanban.db and friends
    without colliding with them.
    """
    return get_hermes_home() / "orchestrator" / "state.db"
