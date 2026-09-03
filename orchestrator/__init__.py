"""Durable, event-driven orchestration control-plane foundation.

This package is the first production foundation for Hermes' orchestration
control plane: a durable SQLite event ledger, per-consumer inbox
claim/lease/ack, an append-only audit log, and a transactional outbox with a
policy guard. It is deliberately NOT a polling loop — consumers claim work
from the ledger and ack it; nothing here spins on a timer.

Scope boundary: this package does not execute merges, releases, or any
irreversible action, and does not wire into the gateway loop. It provides the
storage/ingestion primitives and a Kanban task-event adapter that a future
gateway integration can call.
"""

from orchestrator.audit import AuditLog
from orchestrator.consumers import CONSUMER_NAMES, Inbox
from orchestrator.db import connect, transaction
from orchestrator.home import get_hermes_home, orchestrator_state_db_path
from orchestrator.kanban_adapter import (
    ingest_kanban_task_events,
    normalize_kanban_task_event,
)
from orchestrator.ledger import EventLedger, EventRecord
from orchestrator.outbox import Outbox, OutboxRecord
from orchestrator.policy import PolicyViolation, check_outbox_action

__all__ = [
    "AuditLog",
    "CONSUMER_NAMES",
    "Inbox",
    "connect",
    "transaction",
    "get_hermes_home",
    "orchestrator_state_db_path",
    "ingest_kanban_task_events",
    "normalize_kanban_task_event",
    "EventLedger",
    "EventRecord",
    "Outbox",
    "OutboxRecord",
    "PolicyViolation",
    "check_outbox_action",
]
