"""Adapter: native Hermes Kanban task-event records -> canonical ledger events.

Hermes' kanban plugin exposes its own task-event feed (kanban.db, and the
live ``/api/plugins/kanban/events`` websocket — see server.py's WS proxy
notes). This module does not depend on that plugin's code or DB directly; it
normalizes whatever record/payload shape it hands over into the canonical
event fields ``EventLedger.ingest`` expects, tolerating the handful of key
spellings a task-event record is plausibly shipped under so a plugin version
bump doesn't silently drop events.

``ingest_kanban_task_events`` is the boot/reset entrypoint: call it with a
batch of raw kanban task-event records (e.g. read from kanban.db at process
start, or replayed after a reset) and it ingests each one idempotently — safe
to call repeatedly over the same batch, since ``EventLedger.ingest`` dedups on
(source, source_dedup_key).
"""

from __future__ import annotations

from typing import Any, Iterable

from orchestrator.ledger import EventLedger, EventRecord

SOURCE = "kanban"


def _first(raw: dict[str, Any], *keys: str) -> Any:
    for key in keys:
        if key in raw and raw[key] is not None:
            return raw[key]
    return None


def normalize_kanban_task_event(raw: dict[str, Any]) -> dict[str, Any]:
    """Convert one native Kanban task-event record into ``EventLedger.ingest`` kwargs.

    Required in ``raw`` (under any of the listed spellings): a stable event
    id (-> ``source_dedup_key``), a task id (-> ``entity_id``), and an event
    type. Everything else is optional. The full raw record is preserved
    verbatim as the canonical event's payload — normalization only extracts
    routing/identity fields, it never drops data.
    """
    event_ref = _first(raw, "id", "event_id")
    task_id = _first(raw, "task_id", "entity_id")
    event_type = _first(raw, "type", "event_type")
    if event_ref is None:
        raise ValueError("kanban task-event record is missing an id/event_id")
    if task_id is None:
        raise ValueError("kanban task-event record is missing a task_id/entity_id")
    if event_type is None:
        raise ValueError("kanban task-event record is missing a type/event_type")

    occurred_at = _first(raw, "occurred_at", "created_at", "timestamp")
    causal_id = _first(raw, "causal_id", "parent_event_id")

    return {
        "source": SOURCE,
        "source_dedup_key": str(event_ref),
        "entity_id": str(task_id),
        "event_type": f"kanban.{event_type}" if not str(event_type).startswith("kanban.") else str(event_type),
        "payload": dict(raw),
        "occurred_at": str(occurred_at) if occurred_at is not None else None,
        "causal_id": str(causal_id) if causal_id is not None else None,
    }


def ingest_kanban_task_events(ledger: EventLedger, records: Iterable[dict[str, Any]]) -> list[EventRecord]:
    """Boot/reset entrypoint: normalize and ingest a batch of raw kanban task-event records.

    Idempotent and order-independent per record — re-running it over a batch
    that includes already-ingested records (e.g. Hermes cold-starts and
    replays its last N kanban events defensively) is a no-op for those and
    only appends the genuinely new ones.
    """
    return [ledger.ingest(**normalize_kanban_task_event(record)) for record in records]
