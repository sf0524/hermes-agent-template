"""Inventory-only capability registry.

Records what Claude/Codex model+effort (+role) identities the eventual role
runners would need, and whatever probe evidence has been gathered about them.
Every row's ``status`` is ``inventory`` or ``unverified`` — there is no
``approved`` status, and this class exposes no method that builds a command,
launches a process, or approves dispatch. A future phase that adds dispatch
must do so in a different module behind an explicit approval boundary; this
one stays a read/record-only catalogue.
"""

from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

VALID_STATUSES = frozenset({"inventory", "unverified"})


def _utcnow() -> str:
    return datetime.now(timezone.utc).isoformat()


@dataclass(frozen=True)
class CapabilityRecord:
    id: int
    capability_id: str
    cli: str
    model: str
    effort: str | None
    role: str | None
    status: str
    probe_evidence: dict[str, Any] | None
    recorded_at: str
    updated_at: str

    @classmethod
    def _from_row(cls, row: sqlite3.Row) -> "CapabilityRecord":
        return cls(
            id=row["id"],
            capability_id=row["capability_id"],
            cli=row["cli"],
            model=row["model"],
            effort=row["effort"],
            role=row["role"],
            status=row["status"],
            probe_evidence=json.loads(row["probe_evidence"]) if row["probe_evidence"] is not None else None,
            recorded_at=row["recorded_at"],
            updated_at=row["updated_at"],
        )


# Documented target identities from the control-plane spec's discovery scope
# (docs/specs/2026-09-03-autonomous-team-control-plane.md, REQ-05): the
# model/effort pairs a future role runner would need, recorded here purely as
# inventory ahead of any probe or dispatch capability.
SEED_CAPABILITIES: tuple[dict[str, Any], ...] = (
    {"capability_id": "claude-fable-5-1/high", "cli": "claude", "model": "claude-fable-5-1", "effort": "high", "role": None},
    {"capability_id": "claude-sonnet-5/high", "cli": "claude", "model": "claude-sonnet-5", "effort": "high", "role": None},
    {"capability_id": "claude-opus-5/xhigh", "cli": "claude", "model": "claude-opus-5", "effort": "xhigh", "role": None},
    {"capability_id": "codex-terra/high", "cli": "codex", "model": "codex-terra", "effort": "high", "role": None},
    {"capability_id": "codex-terra/xhigh", "cli": "codex", "model": "codex-terra", "effort": "xhigh", "role": None},
    {"capability_id": "codex-sol/high", "cli": "codex", "model": "codex-sol", "effort": "high", "role": "counter-review"},
)


class CapabilityRegistry:
    def __init__(self, conn: sqlite3.Connection):
        self._conn = conn

    def record(
        self,
        *,
        capability_id: str,
        cli: str,
        model: str,
        effort: str | None = None,
        role: str | None = None,
        status: str = "inventory",
        probe_evidence: dict[str, Any] | None = None,
        now: str | None = None,
    ) -> CapabilityRecord:
        """Insert or refresh one capability's inventory row.

        Not idempotent-by-no-op like the ledgers above: a re-probe legitimately
        updates ``probe_evidence``/``status``/``updated_at`` for the same
        ``capability_id`` (this table is a catalogue snapshot, not an
        append-only event log), so a second call with new evidence overwrites
        the row rather than being ignored.
        """
        if status not in VALID_STATUSES:
            raise ValueError(f"status must be one of {sorted(VALID_STATUSES)}, got {status!r}")
        moment = now or _utcnow()
        existing = self._conn.execute(
            "SELECT recorded_at FROM capability_inventory WHERE capability_id = ?",
            (capability_id,),
        ).fetchone()
        recorded_at = existing["recorded_at"] if existing is not None else moment
        self._conn.execute(
            """
            INSERT INTO capability_inventory
                (capability_id, cli, model, effort, role, status, probe_evidence, recorded_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(capability_id) DO UPDATE SET
                cli = excluded.cli,
                model = excluded.model,
                effort = excluded.effort,
                role = excluded.role,
                status = excluded.status,
                probe_evidence = excluded.probe_evidence,
                updated_at = excluded.updated_at
            """,
            (
                capability_id,
                cli,
                model,
                effort,
                role,
                status,
                json.dumps(probe_evidence) if probe_evidence is not None else None,
                recorded_at,
                moment,
            ),
        )
        row = self._conn.execute(
            "SELECT * FROM capability_inventory WHERE capability_id = ?", (capability_id,)
        ).fetchone()
        return CapabilityRecord._from_row(row)

    def seed_if_missing(
        self,
        *,
        capability_id: str,
        cli: str,
        model: str,
        effort: str | None = None,
        role: str | None = None,
        status: str = "inventory",
        now: str | None = None,
    ) -> CapabilityRecord:
        """Atomically insert a bare seed row only if one doesn't already exist.

        Unlike ``record()``, this never touches an existing row: no upsert,
        no ``ON CONFLICT ... DO UPDATE``. A single ``INSERT ... ON CONFLICT
        DO NOTHING`` is atomic at the SQLite engine level regardless of how
        concurrent callers interleave, so a real probe result committed by
        another connection — whether just before or just after this
        statement executes — can never be clobbered back to bare inventory
        defaults the way a read-then-conditionally-write (check-then-act)
        implementation could be.
        """
        if status not in VALID_STATUSES:
            raise ValueError(f"status must be one of {sorted(VALID_STATUSES)}, got {status!r}")
        moment = now or _utcnow()
        self._conn.execute(
            """
            INSERT INTO capability_inventory
                (capability_id, cli, model, effort, role, status, probe_evidence, recorded_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, NULL, ?, ?)
            ON CONFLICT(capability_id) DO NOTHING
            """,
            (capability_id, cli, model, effort, role, status, moment, moment),
        )
        return self.get(capability_id)

    def get(self, capability_id: str) -> CapabilityRecord | None:
        row = self._conn.execute(
            "SELECT * FROM capability_inventory WHERE capability_id = ?", (capability_id,)
        ).fetchone()
        return CapabilityRecord._from_row(row) if row is not None else None

    def list_all(self) -> list[CapabilityRecord]:
        rows = self._conn.execute("SELECT * FROM capability_inventory ORDER BY capability_id ASC").fetchall()
        return [CapabilityRecord._from_row(r) for r in rows]


def seed_default_capabilities(registry: CapabilityRegistry, *, now: str | None = None) -> list[CapabilityRecord]:
    """Record the documented target capability identities as inventory rows.

    Only inserts identities that are missing from the inventory. Re-seeding
    (e.g. on every restart) must never clobber a capability that has already
    been probed: an existing row's ``status``/``probe_evidence`` — including
    a failed or ``unverified`` probe result — is left completely untouched,
    not overwritten back to the bare ``inventory`` defaults. A real probe
    result is only ever recorded through ``CapabilityRegistry.record()``
    directly.
    """
    return [registry.seed_if_missing(**seed, now=now) for seed in SEED_CAPABILITIES]
