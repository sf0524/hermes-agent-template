"""Shadow-only observer lifecycle: tail a batch, then record a recovery checkpoint.

Only ``mode="shadow"`` exists. Any other mode — this slice has no active,
dispatch, or write mode to select — is rejected in the constructor, before any
DB or source access happens, so a caller can never talk this class into doing
more than read-only observation regardless of what string it passes.

Restart recovery relies on nothing but durable state already written by
``KanbanTailAdapter``/``ObservationLedger``: a fresh ``ShadowObserver`` bound
to the same control-plane DB resumes from the persisted cursor automatically.
``run_once`` additionally writes a ``recovery_checkpoints`` row after each
cycle as self-attested evidence of what the observer had consumed and held at
that point — a dropped notification from the source system is not a special
case here, since the next tail simply reads everything past the durable
cursor, notification or not.
"""

from __future__ import annotations

import hashlib
import hmac
import json
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

from orchestrator.control_plane.db import transaction
from orchestrator.control_plane.kanban_tail import DEFAULT_BATCH_SIZE, KanbanTailAdapter, TailBatchResult
from orchestrator.control_plane.ledger import ObservationLedger
from orchestrator.control_plane.models import RecoveryCheckpoint
from orchestrator.control_plane.schema import (
    SchemaVerificationResult,
    evaluate_schema,
)

ALLOWED_MODES = frozenset({"shadow"})

# Bumping this changes every future checkpoint_integrity_hash() output, the
# same way ledger.EVIDENCE_HASH_VERSION does for evidence_hash() — a future
# change to the canonical encoding is then itself detectable/attributable
# rather than silently reinterpreting old checkpoint hashes under a new
# scheme.
CHECKPOINT_HASH_VERSION = 2


class CheckpointVerificationError(RuntimeError):
    """Raised when a recorded checkpoint's integrity_hash no longer matches recomputed state.

    Distinct from ``SchemaVerificationError``: the schema can be perfectly
    valid while a specific checkpoint row's self-attestation still doesn't
    reproduce (e.g. its integrity_hash was forged/corrupted, or it was
    written against a different cursor/observation count than the one now
    on disk) — this is that narrower check.
    """


def checkpoint_integrity_hash(
    *,
    id: int,
    checkpoint_at: str,
    source: str,
    cursor_value: str,
    observation_count: int,
    schema_version: int,
    applied_migrations: tuple[int, ...],
    trigger_fingerprints: tuple[tuple[str, str, str, str, str | None, str], ...],
    table_fingerprints: tuple[
        tuple[str, tuple[str, ...], bool, tuple[tuple[str, tuple[tuple[str | None, str, str], ...], bool, str | None], ...]],
        ...,
    ],
    table_contract_fingerprints: tuple[tuple[str, str], ...],
    integrity_check: str,
) -> str:
    """The one canonical hash over every persisted attested checkpoint field.

    Covers every column ``recovery_checkpoints`` actually stores (``id``,
    ``checkpoint_at``, ``source``, ``cursor_value``, ``observation_count``,
    ``schema_version``) plus the schema/trigger/table fingerprint evidence
    that checkpoint attests to (``applied_migrations``, the complete
    normalized ``trigger_fingerprints`` from ``SchemaVerificationResult`` —
    not just trigger names —, ``table_fingerprints`` (PRIMARY
    KEY/AUTOINCREMENT/UNIQUE structural identity per required table), the
    exact migration-derived ``table_contract_fingerprints`` (complete
    canonical CREATE TABLE DDL, including CHECK/default/collation/foreign
    key/options), and ``integrity_check``). Both ``_write_checkpoint`` (build) and
    ``verify_recovery_state`` (recompute + compare) call this same function
    so the two can never drift apart.

    Uses a versioned, typed JSON encoding (``sort_keys``, compact
    separators) for the same reason ``ledger.evidence_hash`` does: a
    deterministic canonical serialization regardless of field-construction
    order, with no delimiter-join ambiguity between e.g. an empty string and
    a genuinely absent value.
    """
    canonical = {
        "v": CHECKPOINT_HASH_VERSION,
        "id": id,
        "checkpoint_at": checkpoint_at,
        "source": source,
        "cursor_value": cursor_value,
        "observation_count": observation_count,
        "schema_version": schema_version,
        "applied_migrations": list(applied_migrations),
        "trigger_fingerprints": [list(fp) for fp in trigger_fingerprints],
        "table_fingerprints": [
            [
                table,
                list(pk_columns),
                autoincrement,
                [
                    [origin, [list(column) for column in columns], partial, definition]
                    for origin, columns, partial, definition in unique_indexes
                ],
            ]
            for table, pk_columns, autoincrement, unique_indexes in table_fingerprints
        ],
        "table_contract_fingerprints": [list(fingerprint) for fingerprint in table_contract_fingerprints],
        "integrity_check": integrity_check,
    }
    encoded = json.dumps(canonical, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def _utcnow() -> str:
    return datetime.now(timezone.utc).isoformat()


class ShadowModeRequiredError(ValueError):
    """Raised when a mode other than ``shadow`` is requested.

    This slice has no active/dispatch observer mode to fall back to — there
    is nothing this error defers to, it is a hard refusal.
    """


@dataclass(frozen=True)
class ObservationCycleResult:
    tail_result: TailBatchResult
    checkpoint: RecoveryCheckpoint


class ShadowObserver:
    def __init__(
        self,
        source_db_path: Path | str,
        control_conn: sqlite3.Connection,
        *,
        mode: str = "shadow",
        source: str = "kanban",
        batch_size: int = DEFAULT_BATCH_SIZE,
    ):
        if mode not in ALLOWED_MODES:
            raise ShadowModeRequiredError(
                f"control-plane observer only supports mode='shadow' in this slice "
                f"(no active/dispatch mode exists); got {mode!r}"
            )
        self._mode = mode
        self._conn = control_conn
        self._source = source
        self._tail = KanbanTailAdapter(source_db_path, control_conn, source=source, batch_size=batch_size)

    @property
    def mode(self) -> str:
        return self._mode

    def run_once(
        self,
        *,
        policy_version: str,
        actor: str,
        correlation_id: str,
        now: str | None = None,
    ) -> ObservationCycleResult:
        """Verify prior recovery state, tail one bounded batch, and checkpoint it.

        Recovery-state verification happens *before* ``run_once`` invokes
        tailing at all.  It validates both the control-plane schema and this
        source's latest checkpoint (or refuses unattested source-owned
        state), so a broken/tampered schema or forged/inconsistent prior
        recovery state is rejected before the tail adapter — or anything
        else in this cycle — writes to ``observations``/
        ``duplicate_conflicts``/``source_cursors``/``source_identity``.
        ``_write_checkpoint`` below re-validates its fresh, consistent
        snapshot anyway (defense in depth, and because that snapshot is also
        what its evidence attests to).

        Zero side effects outside the control-plane DB: the only writes this
        performs are to ``observations``/``duplicate_conflicts``/
        ``source_cursors``/``source_identity`` (via the tail adapter) and
        ``recovery_checkpoints`` below — nothing here touches the source DB,
        dispatches anything, or calls out to any other system.
        """
        self.verify_recovery_state()
        moment = now or _utcnow()
        tail_result = self._tail.tail_once(
            policy_version=policy_version, actor=actor, correlation_id=correlation_id, now=moment
        )
        checkpoint = self._write_checkpoint(now=moment)
        return ObservationCycleResult(tail_result=tail_result, checkpoint=checkpoint)

    def _write_checkpoint(self, *, now: str) -> RecoveryCheckpoint:
        """Verify schema/triggers/migrations/integrity, then record a checkpoint of that state.

        Everything a checkpoint attests to — applied migrations, required
        trigger fingerprints, ``PRAGMA integrity_check``, the current cursor,
        and the current observation count — is read from one consistent
        snapshot: a single ``BEGIN IMMEDIATE`` transaction that runs the
        schema checks (``evaluate_schema``, caller-managed transaction) and
        the cursor/count reads before the checkpoint row itself is inserted
        and committed. A schema/trigger/migration/integrity problem raises
        ``SchemaVerificationError`` here and the whole transaction rolls back
        — no checkpoint is ever recorded against a schema this code can't
        trust.

        ``recovery_checkpoints`` is append-only (UPDATE is trigger-forbidden,
        see schema.py), so ``id`` must be known *before* the row is inserted
        in order to be covered by ``integrity_hash`` — there is no
        insert-then-fix-up-the-hash option. It is computed explicitly here
        (``MAX(id) + 1``, safe under this transaction's write lock) rather
        than left to AUTOINCREMENT, then supplied on the INSERT itself.
        """
        with transaction(self._conn) as conn:
            verification = evaluate_schema(conn)
            cursor = self._tail.current_cursor()
            observation_count = ObservationLedger(conn).count()
            schema_version = verification.applied_migrations[-1]
            next_id = conn.execute("SELECT COALESCE(MAX(id), 0) + 1 FROM recovery_checkpoints").fetchone()[0]
            integrity_hash = checkpoint_integrity_hash(
                id=next_id,
                checkpoint_at=now,
                source=self._source,
                cursor_value=str(cursor),
                observation_count=observation_count,
                schema_version=schema_version,
                applied_migrations=verification.applied_migrations,
                trigger_fingerprints=verification.trigger_fingerprints,
                table_fingerprints=verification.table_fingerprints,
                table_contract_fingerprints=verification.table_contract_fingerprints,
                integrity_check=verification.integrity_check,
            )
            conn.execute(
                """
                INSERT INTO recovery_checkpoints
                    (id, checkpoint_at, source, cursor_value, observation_count, schema_version, integrity_hash)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (next_id, now, self._source, str(cursor), observation_count, schema_version, integrity_hash),
            )
            row = conn.execute("SELECT * FROM recovery_checkpoints WHERE id = ?", (next_id,)).fetchone()
        return RecoveryCheckpoint._from_row(row)

    def latest_checkpoint(self) -> RecoveryCheckpoint | None:
        row = self._conn.execute(
            "SELECT * FROM recovery_checkpoints WHERE source = ? ORDER BY id DESC LIMIT 1",
            (self._source,),
        ).fetchone()
        return RecoveryCheckpoint._from_row(row) if row is not None else None

    def current_cursor(self) -> int:
        return self._tail.current_cursor()

    def verify_recovery_state(self) -> SchemaVerificationResult:
        """Read-only verification path: fails closed on any schema or checkpoint defect.

        Raises ``SchemaVerificationError`` if applied migrations don't match
        exactly what this code expects, a required table/column is missing,
        a required append-only trigger is missing, or ``PRAGMA
        integrity_check`` reports anything but ``ok``. Callers (e.g. a health
        check, or before trusting a restart-recovery resume) can call this
        independently of running a tail cycle.

        Additionally recomputes this source's latest checkpoint's
        ``integrity_hash`` via ``checkpoint_integrity_hash`` — using the
        checkpoint row's own persisted fields plus the schema/trigger
        fingerprint evidence just evaluated above, from the same consistent
        snapshot — and compares it against the durable ``integrity_hash``
        with ``hmac.compare_digest``. A mismatch means the checkpoint row's
        self-attestation no longer reproduces (e.g. a forged/corrupted
        ``integrity_hash``, or evidence written under a different hashing
        scheme) and raises ``CheckpointVerificationError``; entirely
        read-only, so a raise here leaves durable state untouched.

        A missing checkpoint row for ``self._source`` (``row is None``) is
        *not* automatically treated as "nothing to verify, so verification
        passes": that collapses two very different situations into one.
        Genuinely fresh — no durable row in *any* source-owned state table
        — is fine; there is nothing yet to attest to. But a source that has
        source identity, a durable cursor, observations, or quarantined
        duplicate evidence yet no matching checkpoint is exactly what a
        source-locator relabel/tamper can look like.  Looking only at the
        cursor collapses those cases into “fresh”; this checks every table
        whose rows are owned by ``source`` and fails closed instead.
        """
        self._conn.execute("BEGIN")
        try:
            verification = evaluate_schema(self._conn)
            row = self._conn.execute(
                "SELECT * FROM recovery_checkpoints WHERE source = ? ORDER BY id DESC LIMIT 1",
                (self._source,),
            ).fetchone()
            source_state_tables = (
                "source_identity",
                "source_cursors",
                "observations",
                "duplicate_conflicts",
            )
            state_row = self._conn.execute(
                """
                SELECT
                    EXISTS(SELECT 1 FROM source_identity WHERE source = ?) AS source_identity,
                    EXISTS(SELECT 1 FROM source_cursors WHERE source = ?) AS source_cursors,
                    EXISTS(SELECT 1 FROM observations WHERE source = ?) AS observations,
                    EXISTS(SELECT 1 FROM duplicate_conflicts WHERE source = ?) AS duplicate_conflicts
                """,
                (self._source, self._source, self._source, self._source),
            ).fetchone()
        finally:
            self._conn.execute("ROLLBACK")

        if row is None:
            state_tables = tuple(
                table for table, present in zip(source_state_tables, state_row) if present
            )
            if state_tables:
                raise CheckpointVerificationError(
                    f"source={self._source!r} has durable state in {state_tables!r} but no recovery checkpoint "
                    "is recorded for it under that same source key; refusing to treat unattested state (or a "
                    "checkpoint source-locator mismatch/tamper) as a successfully verified recovery state"
                )
            return verification

        checkpoint = RecoveryCheckpoint._from_row(row)
        recomputed = checkpoint_integrity_hash(
            id=checkpoint.id,
            checkpoint_at=checkpoint.checkpoint_at,
            source=checkpoint.source,
            cursor_value=checkpoint.cursor_value,
            observation_count=checkpoint.observation_count,
            schema_version=checkpoint.schema_version,
            applied_migrations=verification.applied_migrations,
            trigger_fingerprints=verification.trigger_fingerprints,
            table_fingerprints=verification.table_fingerprints,
            table_contract_fingerprints=verification.table_contract_fingerprints,
            integrity_check=verification.integrity_check,
        )
        if not hmac.compare_digest(recomputed, checkpoint.integrity_hash):
            raise CheckpointVerificationError(
                f"recovery checkpoint id={checkpoint.id} for source={self._source!r} integrity_hash "
                "no longer matches recomputed state; the checkpoint row or the schema/trigger "
                "evidence it attests to may have been tampered with"
            )
        return verification
