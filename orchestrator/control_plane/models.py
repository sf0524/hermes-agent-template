"""Dataclasses for the control plane's evidence rows.

Every model here is a read projection of a row already written by
``ledger.py``/``capabilities.py``/``observer.py`` — none of these carry
behavior, they only shape data for callers and tests.
"""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass


@dataclass(frozen=True)
class Observation:
    seq: int
    observation_id: str
    source: str
    source_id: str
    entity_id: str
    run_id: str | None
    kind: str
    payload: str
    occurred_at: str
    observed_at: str
    policy_version: str
    actor: str
    correlation_id: str
    evidence_hash: str

    @classmethod
    def _from_row(cls, row: sqlite3.Row) -> "Observation":
        return cls(
            seq=row["seq"],
            observation_id=row["observation_id"],
            source=row["source"],
            source_id=row["source_id"],
            entity_id=row["entity_id"],
            run_id=row["run_id"],
            kind=row["kind"],
            payload=row["payload"],
            occurred_at=row["occurred_at"],
            observed_at=row["observed_at"],
            policy_version=row["policy_version"],
            actor=row["actor"],
            correlation_id=row["correlation_id"],
            evidence_hash=row["evidence_hash"],
        )


@dataclass(frozen=True)
class DuplicateConflict:
    id: int
    source: str
    source_id: str
    existing_observation_id: str
    existing_evidence_hash: str
    conflicting_entity_id: str
    conflicting_run_id: str | None
    conflicting_kind: str
    conflicting_payload: str
    conflicting_occurred_at: str
    conflicting_evidence_hash: str
    detected_at: str

    @classmethod
    def _from_row(cls, row: sqlite3.Row) -> "DuplicateConflict":
        return cls(
            id=row["id"],
            source=row["source"],
            source_id=row["source_id"],
            existing_observation_id=row["existing_observation_id"],
            existing_evidence_hash=row["existing_evidence_hash"],
            conflicting_entity_id=row["conflicting_entity_id"],
            conflicting_run_id=row["conflicting_run_id"],
            conflicting_kind=row["conflicting_kind"],
            conflicting_payload=row["conflicting_payload"],
            conflicting_occurred_at=row["conflicting_occurred_at"],
            conflicting_evidence_hash=row["conflicting_evidence_hash"],
            detected_at=row["detected_at"],
        )


@dataclass(frozen=True)
class RecoveryCheckpoint:
    id: int
    checkpoint_at: str
    source: str
    cursor_value: str
    observation_count: int
    schema_version: int
    integrity_hash: str

    @classmethod
    def _from_row(cls, row: sqlite3.Row) -> "RecoveryCheckpoint":
        return cls(
            id=row["id"],
            checkpoint_at=row["checkpoint_at"],
            source=row["source"],
            cursor_value=row["cursor_value"],
            observation_count=row["observation_count"],
            schema_version=row["schema_version"],
            integrity_hash=row["integrity_hash"],
        )
