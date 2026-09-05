"""Shadow-only observation control plane: durable, read-only evidence recording.

This package is a Phase-1 safe slice (see
``docs/plans/2026-09-03-shadow-control-plane-recorder-plan.md``): it tails the
native Kanban ``task_events`` SQLite table read-only and records what it sees
as append-only evidence in its own DB, rooted independently of the
``orchestrator`` ledger foundation and of a worker-modified ``HERMES_HOME``.

Hard scope boundary, enforced by omission: no HTTP server, no webhook
ingestion, no outbox/dispatcher integration, no Kanban writes, no role
runners, no subprocess execution, no git/GitHub/Railway operations, no update
application, no owner decisions, no merge/release paths. Anything here that
looks like it could produce a command or a side effect is a bug — the
capability registry (``capabilities.py``) is inventory-only by design.
"""

from orchestrator.control_plane.capabilities import CapabilityRegistry, CapabilityRecord
from orchestrator.control_plane.db import connect, transaction
from orchestrator.control_plane.kanban_tail import KanbanTailAdapter, TailBatchResult
from orchestrator.control_plane.ledger import ObservationLedger, ObservationResult
from orchestrator.control_plane.observer import ShadowModeRequiredError, ShadowObserver
from orchestrator.control_plane.state import InvalidControlPlaneStateError, control_plane_db_path

__all__ = [
    "CapabilityRegistry",
    "CapabilityRecord",
    "connect",
    "transaction",
    "KanbanTailAdapter",
    "TailBatchResult",
    "ObservationLedger",
    "ObservationResult",
    "ShadowModeRequiredError",
    "ShadowObserver",
    "InvalidControlPlaneStateError",
    "control_plane_db_path",
]
