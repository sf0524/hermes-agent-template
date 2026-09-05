"""Resolve the control-plane state DB path, independent of ``orchestrator.home``.

The durable ledger foundation (``orchestrator/home.py``) namespaces its state
DB under ``$HERMES_HOME``, which a worker process can and does modify at
runtime. The control plane's evidence trail must stay put regardless of that,
so it resolves from its own pair of variables instead of ``$HERMES_HOME``:

1. ``$ORCH_STATE_DB`` — an explicit absolute path to the DB file itself, if set.
2. Otherwise ``${HERMES_ROOT:-/data/.hermes}/orchestrator/control-plane.db``.

Both a non-empty ``$ORCH_STATE_DB`` and a non-empty ``$HERMES_ROOT`` are
rejected outright if relative, for the same reason ``orchestrator.home``
rejects a relative ``$HERMES_HOME``: a relative durable state root would
silently move depending on process launch directory, which is a data-loss
trap rather than a convenience.
"""

from __future__ import annotations

import os
from pathlib import Path

DEFAULT_HERMES_ROOT = Path("/data/.hermes")


class InvalidControlPlaneStateError(ValueError):
    """Raised when a control-plane state path is set to something unsafe to resolve."""


def validate_absolute_control_db_path(value: str, *, source: str) -> Path:
    """Centralized validation for any externally-supplied control-plane DB path.

    Used by both env-var resolution (``$ORCH_STATE_DB``/``$HERMES_ROOT``,
    below) and the CLI's ``--control-db`` flag, so the two can never drift:
    every external entry point rejects a relative path the same way, for the
    same reason a relative durable-state root is a data-loss trap rather than
    a convenience. ``:memory:`` is rejected too — it bypasses durable state
    entirely, so it stays reachable only by calling ``connect()`` directly
    (as tests do), never through an external input like the CLI.
    """
    if value.strip() == "":
        raise InvalidControlPlaneStateError(f"{source} must not be empty")
    if value == ":memory:":
        raise InvalidControlPlaneStateError(
            f"{source} does not accept ':memory:'; call connect() directly for an in-memory test DB, "
            "it is not a valid durable state target for external input"
        )
    path = Path(value)
    if not path.is_absolute():
        raise InvalidControlPlaneStateError(
            f"{source} must be an absolute path, got {value!r}; refusing to resolve a "
            "relative path against the process's current working directory for a durable state root"
        )
    return path


def control_plane_db_path() -> Path:
    """The control plane's durable SQLite state DB path.

    Honours ``$ORCH_STATE_DB`` first (the full file path, taken verbatim), then
    ``$HERMES_ROOT`` (a root directory, with ``orchestrator/control-plane.db``
    appended), then falls back to ``/data/.hermes/orchestrator/control-plane.db``.
    """
    explicit = os.environ.get("ORCH_STATE_DB")
    if explicit is not None and explicit.strip() != "":
        return validate_absolute_control_db_path(explicit, source="ORCH_STATE_DB")

    raw_root = os.environ.get("HERMES_ROOT")
    if raw_root is None or raw_root.strip() == "":
        root = DEFAULT_HERMES_ROOT
    else:
        root = validate_absolute_control_db_path(raw_root, source="HERMES_ROOT")

    return root / "orchestrator" / "control-plane.db"
