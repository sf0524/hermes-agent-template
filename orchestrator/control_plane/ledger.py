"""Append-only observation ledger: the control plane's source of evidence.

``record()`` enforces ``UNIQUE(source, source_id)`` the same way
``orchestrator.ledger.EventLedger.ingest`` enforces ``UNIQUE(source,
source_dedup_key)`` — but with one addition: because this ledger tails a
*source* system it does not own, a second sighting of the same
``(source, source_id)`` is not always a harmless retry. If its canonical
content matches what is already on disk, it is an exact replay and a no-op.
If it doesn't, the source row genuinely changed identity under a key that is
supposed to be immutable — recording that overwrite would corrupt the
append-only invariant, so it is quarantined into ``duplicate_conflicts``
instead, and the original observation is left untouched.
"""

from __future__ import annotations

import hashlib
import json
import sqlite3
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum

from orchestrator.control_plane.models import DuplicateConflict, Observation

# Bumping this changes every future evidence_hash() output. It exists so a
# future change to the canonical encoding is itself detectable/attributable
# rather than silently reinterpreting old hashes under a new scheme.
EVIDENCE_HASH_VERSION = 1


def _utcnow() -> str:
    return datetime.now(timezone.utc).isoformat()


def evidence_hash(
    *,
    source: str,
    source_id: str,
    entity_id: str,
    run_id: str | None,
    kind: str,
    payload: str,
    occurred_at: str,
) -> str:
    """Canonical hash of a source row's identity + content.

    Deliberately excludes anything the control plane itself assigns
    (observation_id, observed_at, policy_version, actor, correlation_id) — it
    hashes only fields that originate from the source system, so the same
    source row always hashes the same way regardless of when or by whom it
    was observed.

    Hashes a versioned, typed JSON encoding rather than a delimiter-joined
    string: a delimiter join collapses ``run_id=None`` and ``run_id=""``
    into the same canonical text (``... or ""``), which would let a source
    row that is genuinely absent an identity be hash-indistinguishable from
    one that deliberately carries an empty one. JSON's ``null`` keeps that
    distinction, and ``sort_keys``/compact separators make the encoding
    deterministic regardless of field-construction order.
    """
    canonical = {
        "v": EVIDENCE_HASH_VERSION,
        "source": source,
        "source_id": source_id,
        "entity_id": entity_id,
        "run_id": run_id,
        "kind": kind,
        "payload": payload,
        "occurred_at": occurred_at,
    }
    encoded = json.dumps(canonical, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


class ObservationOutcome(str, Enum):
    APPENDED = "appended"
    DUPLICATE = "duplicate"
    QUARANTINED = "quarantined"


@dataclass(frozen=True)
class ObservationResult:
    outcome: ObservationOutcome
    observation: Observation
    conflict: DuplicateConflict | None = None


class ObservationLedger:
    def __init__(self, conn: sqlite3.Connection):
        self._conn = conn

    def record(
        self,
        *,
        source: str,
        source_id: str,
        entity_id: str,
        kind: str,
        payload: str,
        occurred_at: str,
        policy_version: str,
        actor: str,
        correlation_id: str,
        run_id: str | None = None,
        now: str | None = None,
    ) -> ObservationResult:
        """Append one observation, or resolve a duplicate ``(source, source_id)``.

        Does not open its own transaction — callers wrap one or more
        ``record()`` calls in ``orchestrator.control_plane.db.transaction``
        themselves (see ``kanban_tail.py``, which commits a whole batch plus
        its cursor advance atomically).
        """
        observed_at = now or _utcnow()
        new_hash = evidence_hash(
            source=source,
            source_id=source_id,
            entity_id=entity_id,
            run_id=run_id,
            kind=kind,
            payload=payload,
            occurred_at=occurred_at,
        )

        existing_row = self._conn.execute(
            "SELECT * FROM observations WHERE source = ? AND source_id = ?",
            (source, source_id),
        ).fetchone()

        if existing_row is not None:
            existing = Observation._from_row(existing_row)
            if existing.evidence_hash == new_hash:
                return ObservationResult(outcome=ObservationOutcome.DUPLICATE, observation=existing)
            return ObservationResult(
                outcome=ObservationOutcome.QUARANTINED,
                observation=existing,
                conflict=self._quarantine(
                    source=source,
                    source_id=source_id,
                    existing=existing,
                    conflicting_entity_id=entity_id,
                    conflicting_run_id=run_id,
                    conflicting_kind=kind,
                    conflicting_payload=payload,
                    conflicting_occurred_at=occurred_at,
                    conflicting_hash=new_hash,
                    detected_at=observed_at,
                ),
            )

        observation_id = str(uuid.uuid4())
        self._conn.execute(
            """
            INSERT INTO observations (
                observation_id, source, source_id, entity_id, run_id, kind, payload,
                occurred_at, observed_at, policy_version, actor, correlation_id, evidence_hash
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                observation_id,
                source,
                source_id,
                entity_id,
                run_id,
                kind,
                payload,
                occurred_at,
                observed_at,
                policy_version,
                actor,
                correlation_id,
                new_hash,
            ),
        )
        inserted = self._conn.execute(
            "SELECT * FROM observations WHERE observation_id = ?", (observation_id,)
        ).fetchone()
        return ObservationResult(outcome=ObservationOutcome.APPENDED, observation=Observation._from_row(inserted))

    def _quarantine(
        self,
        *,
        source: str,
        source_id: str,
        existing: Observation,
        conflicting_entity_id: str,
        conflicting_run_id: str | None,
        conflicting_kind: str,
        conflicting_payload: str,
        conflicting_occurred_at: str,
        conflicting_hash: str,
        detected_at: str,
    ) -> DuplicateConflict:
        self._conn.execute(
            """
            INSERT INTO duplicate_conflicts (
                source, source_id, existing_observation_id, existing_evidence_hash,
                conflicting_entity_id, conflicting_run_id, conflicting_kind,
                conflicting_payload, conflicting_occurred_at,
                conflicting_evidence_hash, detected_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                source,
                source_id,
                existing.observation_id,
                existing.evidence_hash,
                conflicting_entity_id,
                conflicting_run_id,
                conflicting_kind,
                conflicting_payload,
                conflicting_occurred_at,
                conflicting_hash,
                detected_at,
            ),
        )
        row = self._conn.execute(
            "SELECT * FROM duplicate_conflicts WHERE rowid = last_insert_rowid()"
        ).fetchone()
        return DuplicateConflict._from_row(row)

    def get(self, observation_id: str) -> Observation | None:
        row = self._conn.execute(
            "SELECT * FROM observations WHERE observation_id = ?", (observation_id,)
        ).fetchone()
        return Observation._from_row(row) if row is not None else None

    def get_by_source_id(self, source: str, source_id: str) -> Observation | None:
        row = self._conn.execute(
            "SELECT * FROM observations WHERE source = ? AND source_id = ?",
            (source, source_id),
        ).fetchone()
        return Observation._from_row(row) if row is not None else None

    def count(self) -> int:
        return self._conn.execute("SELECT COUNT(*) FROM observations").fetchone()[0]

    def list_conflicts(self) -> list[DuplicateConflict]:
        rows = self._conn.execute("SELECT * FROM duplicate_conflicts ORDER BY id ASC").fetchall()
        return [DuplicateConflict._from_row(r) for r in rows]
