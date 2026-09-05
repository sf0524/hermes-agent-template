"""Idempotent schema for the control plane's durable SQLite state DB.

Same migration shape as ``orchestrator/schema.py``: each migration is a list
of DDL statements guarded by ``schema_migrations``, applied inside one writer
transaction (``BEGIN IMMEDIATE``) that rechecks the version is still unapplied
after acquiring the write lock, so two connections racing to migrate the same
fresh DB can never both insert the same ``schema_migrations`` row.

This schema is intentionally read/observe-only: every table here records
evidence about something that happened elsewhere (a Kanban task event, a
capability probe, a recovery checkpoint) — none of them are a queue of work
to execute, and there is no outbox/dispatch table in this package.
"""

from __future__ import annotations

import re
import sqlite3
from dataclasses import dataclass

# The one migration-tracking table's DDL, factored out as a constant since
# it's applied outside the normal MIGRATIONS list (before any versioned
# migration can run) but is still a real required table whose structural
# fingerprint (see TableStructuralFingerprint below) must be derived from
# this exact same DDL, not a hand-maintained second copy.
SCHEMA_MIGRATIONS_TABLE_DDL = """
CREATE TABLE IF NOT EXISTS schema_migrations (
    version     INTEGER PRIMARY KEY,
    applied_at  TEXT NOT NULL
)
"""

MIGRATIONS: list[tuple[int, list[str]]] = [
    (
        1,
        [
            # Append-only ledger of everything the shadow observer has seen.
            # UNIQUE(source, source_id) is the dedup key: a second observation
            # for the same source row is handled in ledger.py (exact replay is
            # a no-op, conflicting content is quarantined) rather than at the
            # DB level, so this constraint exists to make "one canonical row
            # per source event" a DB-enforced invariant too.
            """
            CREATE TABLE IF NOT EXISTS observations (
                seq             INTEGER PRIMARY KEY AUTOINCREMENT,
                observation_id  TEXT NOT NULL UNIQUE,
                source          TEXT NOT NULL,
                source_id       TEXT NOT NULL,
                entity_id       TEXT NOT NULL,
                run_id          TEXT,
                kind            TEXT NOT NULL,
                payload         TEXT NOT NULL,
                occurred_at     TEXT NOT NULL,
                observed_at     TEXT NOT NULL,
                policy_version  TEXT NOT NULL,
                actor           TEXT NOT NULL,
                correlation_id  TEXT NOT NULL,
                evidence_hash   TEXT NOT NULL,
                UNIQUE(source, source_id)
            )
            """,
            """
            CREATE TRIGGER IF NOT EXISTS observations_no_update
            BEFORE UPDATE ON observations
            BEGIN
                SELECT RAISE(ABORT, 'observations is append-only: UPDATE forbidden');
            END
            """,
            """
            CREATE TRIGGER IF NOT EXISTS observations_no_delete
            BEFORE DELETE ON observations
            BEGIN
                SELECT RAISE(ABORT, 'observations is append-only: DELETE forbidden');
            END
            """,
            # "INSERT OR REPLACE" (or any ON CONFLICT ... REPLACE upsert)
            # resolves a unique-constraint conflict by deleting the
            # pre-existing row and then inserting — an implicit delete that,
            # unlike a direct DELETE statement, only fires a BEFORE/AFTER
            # DELETE trigger when the connection has recursive_triggers
            # turned on. That pragma lives on the *connection*, not the DB
            # file, so a brand-new raw connection (default recursive_triggers
            # = OFF) would otherwise bypass the two triggers above entirely.
            # A BEFORE INSERT trigger has no such gap: it always fires as
            # part of processing the INSERT statement itself, before SQLite
            # ever gets to conflict resolution, so the pre-existing row is
            # still present to detect. One trigger per unique/primary
            # identity column set, since REPLACE can target any of them.
            """
            CREATE TRIGGER IF NOT EXISTS observations_no_replace_by_seq
            BEFORE INSERT ON observations
            WHEN NEW.seq IS NOT NULL AND EXISTS (SELECT 1 FROM observations WHERE seq = NEW.seq)
            BEGIN
                SELECT RAISE(ABORT, 'observations is append-only: INSERT OR REPLACE forbidden (seq)');
            END
            """,
            """
            CREATE TRIGGER IF NOT EXISTS observations_no_replace_by_observation_id
            BEFORE INSERT ON observations
            WHEN EXISTS (SELECT 1 FROM observations WHERE observation_id = NEW.observation_id)
            BEGIN
                SELECT RAISE(ABORT, 'observations is append-only: INSERT OR REPLACE forbidden (observation_id)');
            END
            """,
            """
            CREATE TRIGGER IF NOT EXISTS observations_no_replace_by_source_source_id
            BEFORE INSERT ON observations
            WHEN EXISTS (SELECT 1 FROM observations WHERE source = NEW.source AND source_id = NEW.source_id)
            BEGIN
                SELECT RAISE(ABORT, 'observations is append-only: INSERT OR REPLACE forbidden (source, source_id)');
            END
            """,
            # Durable per-source read cursor for the tail adapter. Mutable by
            # design (it is a bookmark, not evidence) — committed only after
            # the observations it covers have committed, in the same
            # transaction, so a crash between the two can never advance the
            # cursor past unrecorded evidence.
            # ``consumed_digest`` is a deterministic, streaming-computed digest
            # of every source row from id=1 through ``cursor_value`` (see
            # ``kanban_tail._fold_digest``/``_recompute_consumed_prefix_digest``).
            # Physical (device, inode) identity alone cannot detect an
            # in-place overwrite of an already-consumed row *earlier* than
            # the cursor's own anchor row — this digest lets the tail adapter
            # re-derive the whole consumed prefix from the opened readonly
            # source on every cycle and compare it against durable evidence,
            # catching that class of tamper regardless of which previously-
            # consumed row was altered.
            """
            CREATE TABLE IF NOT EXISTS source_cursors (
                source            TEXT PRIMARY KEY,
                cursor_value      TEXT NOT NULL,
                consumed_digest   TEXT NOT NULL,
                updated_at        TEXT NOT NULL
            )
            """,
            # Evidence of a source row whose (source, source_id) collided with
            # an already-recorded observation but carried different canonical
            # content — quarantined rather than silently dropped or allowed to
            # overwrite the original append-only row. The full conflicting
            # canonical record (entity_id/run_id/kind/payload/occurred_at) is
            # captured up front, as NOT NULL columns with no DEFAULT: a
            # conflict where entity_id/run_id/kind/occurred_at differ instead
            # of (or in addition to) payload is otherwise undiagnosable, and
            # every writer (``ledger.py``'s ``_quarantine``) already has real
            # values for all of them, so there is no legitimate "unknown" case
            # to default to. Not versioned as a later ALTER: that would silently
            # backfill pre-existing rows with a fabricated empty string, which
            # reads as "genuinely empty" rather than "not recorded".
            """
            CREATE TABLE IF NOT EXISTS duplicate_conflicts (
                id                          INTEGER PRIMARY KEY AUTOINCREMENT,
                source                      TEXT NOT NULL,
                source_id                   TEXT NOT NULL,
                existing_observation_id     TEXT NOT NULL,
                existing_evidence_hash      TEXT NOT NULL,
                conflicting_entity_id       TEXT NOT NULL,
                conflicting_run_id          TEXT,
                conflicting_kind            TEXT NOT NULL,
                conflicting_payload         TEXT NOT NULL,
                conflicting_occurred_at     TEXT NOT NULL,
                conflicting_evidence_hash   TEXT NOT NULL,
                detected_at                 TEXT NOT NULL
            )
            """,
            """
            CREATE TRIGGER IF NOT EXISTS duplicate_conflicts_no_update
            BEFORE UPDATE ON duplicate_conflicts
            BEGIN
                SELECT RAISE(ABORT, 'duplicate_conflicts is append-only: UPDATE forbidden');
            END
            """,
            """
            CREATE TRIGGER IF NOT EXISTS duplicate_conflicts_no_delete
            BEFORE DELETE ON duplicate_conflicts
            BEGIN
                SELECT RAISE(ABORT, 'duplicate_conflicts is append-only: DELETE forbidden');
            END
            """,
            """
            CREATE TRIGGER IF NOT EXISTS duplicate_conflicts_no_replace_by_id
            BEFORE INSERT ON duplicate_conflicts
            WHEN NEW.id IS NOT NULL AND EXISTS (SELECT 1 FROM duplicate_conflicts WHERE id = NEW.id)
            BEGIN
                SELECT RAISE(ABORT, 'duplicate_conflicts is append-only: INSERT OR REPLACE forbidden (id)');
            END
            """,
            # Inventory-only capability records: requested Claude/Codex
            # model+effort (+role) identities and their latest probe
            # evidence. Status is always 'inventory' or 'unverified' in this
            # slice — there is no 'approved' status and no dispatch method
            # anywhere in this package, so an inventory row can never become
            # an authorization to run anything.
            """
            CREATE TABLE IF NOT EXISTS capability_inventory (
                id                INTEGER PRIMARY KEY AUTOINCREMENT,
                capability_id     TEXT NOT NULL UNIQUE,
                cli               TEXT NOT NULL,
                model             TEXT NOT NULL,
                effort            TEXT,
                role              TEXT,
                status            TEXT NOT NULL CHECK(status IN ('inventory', 'unverified')) DEFAULT 'inventory',
                probe_evidence    TEXT,
                recorded_at       TEXT NOT NULL,
                updated_at        TEXT NOT NULL
            )
            """,
            # Append-only recovery checkpoints: a periodic self-attestation of
            # "as of this checkpoint, the observer had consumed cursor X and
            # held N observations under schema version S", used by the
            # restart-recovery check. Never mutated — a new checkpoint is
            # always a new row, so the checkpoint history itself is evidence.
            """
            CREATE TABLE IF NOT EXISTS recovery_checkpoints (
                id                  INTEGER PRIMARY KEY AUTOINCREMENT,
                checkpoint_at       TEXT NOT NULL,
                source              TEXT NOT NULL,
                cursor_value        TEXT NOT NULL,
                observation_count   INTEGER NOT NULL,
                schema_version      INTEGER NOT NULL,
                integrity_hash      TEXT NOT NULL
            )
            """,
            """
            CREATE TRIGGER IF NOT EXISTS recovery_checkpoints_no_update
            BEFORE UPDATE ON recovery_checkpoints
            BEGIN
                SELECT RAISE(ABORT, 'recovery_checkpoints is append-only: UPDATE forbidden');
            END
            """,
            """
            CREATE TRIGGER IF NOT EXISTS recovery_checkpoints_no_delete
            BEFORE DELETE ON recovery_checkpoints
            BEGIN
                SELECT RAISE(ABORT, 'recovery_checkpoints is append-only: DELETE forbidden');
            END
            """,
            """
            CREATE TRIGGER IF NOT EXISTS recovery_checkpoints_no_replace_by_id
            BEFORE INSERT ON recovery_checkpoints
            WHEN NEW.id IS NOT NULL AND EXISTS (SELECT 1 FROM recovery_checkpoints WHERE id = NEW.id)
            BEGIN
                SELECT RAISE(ABORT, 'recovery_checkpoints is append-only: INSERT OR REPLACE forbidden (id)');
            END
            """,
            # Stable identity of the *physical* source DB instance a cursor is
            # bound to (canonical resolved path + device/inode), independent
            # of any single row's content. A source swapped for a different
            # file that happens to replicate the exact row the durable cursor
            # currently anchors on would defeat a content-only anchor check;
            # this table lets the tail adapter detect that swap by physical
            # identity instead. Append-only for the same reason as the other
            # evidence tables: once recorded, an instance's identity must
            # never be silently reassigned out from under it.
            """
            CREATE TABLE IF NOT EXISTS source_identity (
                source          TEXT PRIMARY KEY,
                canonical_path  TEXT NOT NULL,
                device          INTEGER NOT NULL,
                inode           INTEGER NOT NULL,
                recorded_at     TEXT NOT NULL
            )
            """,
            """
            CREATE TRIGGER IF NOT EXISTS source_identity_no_update
            BEFORE UPDATE ON source_identity
            BEGIN
                SELECT RAISE(ABORT, 'source_identity is append-only: UPDATE forbidden');
            END
            """,
            """
            CREATE TRIGGER IF NOT EXISTS source_identity_no_delete
            BEFORE DELETE ON source_identity
            BEGIN
                SELECT RAISE(ABORT, 'source_identity is append-only: DELETE forbidden');
            END
            """,
            """
            CREATE TRIGGER IF NOT EXISTS source_identity_no_replace_by_source
            BEFORE INSERT ON source_identity
            WHEN EXISTS (SELECT 1 FROM source_identity WHERE source = NEW.source)
            BEGIN
                SELECT RAISE(ABORT, 'source_identity is append-only: INSERT OR REPLACE forbidden (source)');
            END
            """,
        ],
    ),
    # This is deliberately an additive migration rather than editing the
    # already-applied version-1 migration: installations with existing
    # evidence DBs must receive the new schema-level guard too, not only
    # freshly-created databases.  source_identity has a TEXT PRIMARY KEY but
    # remains an ordinary rowid table, so REPLACE can target its hidden rowid
    # and otherwise bypass the source-key guard on a raw connection whose
    # recursive_triggers pragma is OFF.
    (
        2,
        [
            """
            CREATE TRIGGER IF NOT EXISTS source_identity_no_replace_by_rowid
            BEFORE INSERT ON source_identity
            WHEN NEW.rowid IS NOT NULL AND EXISTS (SELECT 1 FROM source_identity WHERE rowid = NEW.rowid)
            BEGIN
                SELECT RAISE(ABORT, 'source_identity is append-only: INSERT OR REPLACE forbidden (rowid)');
            END
            """,
        ],
    ),
]

# Every trigger a checkpoint/verification pass must confirm still exists, on
# every append-only evidence table. If a fresh migration adds a new
# append-only table, its two triggers belong in this tuple too.
REQUIRED_TRIGGERS: tuple[str, ...] = (
    "observations_no_update",
    "observations_no_delete",
    "observations_no_replace_by_seq",
    "observations_no_replace_by_observation_id",
    "observations_no_replace_by_source_source_id",
    "duplicate_conflicts_no_update",
    "duplicate_conflicts_no_delete",
    "duplicate_conflicts_no_replace_by_id",
    "recovery_checkpoints_no_update",
    "recovery_checkpoints_no_delete",
    "recovery_checkpoints_no_replace_by_id",
    "source_identity_no_update",
    "source_identity_no_delete",
    "source_identity_no_replace_by_source",
    "source_identity_no_replace_by_rowid",
)

# The minimal column set each table must expose for the control plane to
# function; used by verify_schema() to fail closed on a malformed/partial
# schema rather than assuming migrations applied cleanly.
REQUIRED_TABLE_COLUMNS: dict[str, tuple[str, ...]] = {
    "observations": (
        "seq", "observation_id", "source", "source_id", "entity_id", "run_id", "kind", "payload",
        "occurred_at", "observed_at", "policy_version", "actor", "correlation_id", "evidence_hash",
    ),
    "source_cursors": ("source", "cursor_value", "consumed_digest", "updated_at"),
    "duplicate_conflicts": (
        "id", "source", "source_id", "existing_observation_id", "existing_evidence_hash",
        "conflicting_entity_id", "conflicting_run_id", "conflicting_kind",
        "conflicting_payload", "conflicting_occurred_at", "conflicting_evidence_hash", "detected_at",
    ),
    "capability_inventory": (
        "id", "capability_id", "cli", "model", "effort", "role", "status",
        "probe_evidence", "recorded_at", "updated_at",
    ),
    "recovery_checkpoints": (
        "id", "checkpoint_at", "source", "cursor_value", "observation_count", "schema_version", "integrity_hash",
    ),
    "source_identity": ("source", "canonical_path", "device", "inode", "recorded_at"),
    "schema_migrations": ("version", "applied_at"),
}


@dataclass(frozen=True)
class ColumnContract:
    """Declared type + NOT NULL requirement for one required column.

    ``column_type`` is compared against ``PRAGMA table_info``'s declared type
    case-insensitively (SQLite stores whatever type string the CREATE TABLE
    used, verbatim) — an affinity-compatible but differently-declared column
    (e.g. ``INTEGER`` swapped in for a declared ``TEXT`` primary/foreign key)
    would otherwise silently pass a name-only column check while changing
    what values that column actually accepts.
    """

    column_type: str
    not_null: bool


# Per-table column type/nullability contract, checked in addition to mere
# column-name presence (REQUIRED_TABLE_COLUMNS above): a column that exists
# under the right name but was recreated with the wrong declared type or a
# dropped NOT NULL is just as broken a contract as a missing column, and a
# name-only check would pass it silently. Only columns whose exact type/
# nullability is load-bearing for the code that reads them are listed here;
# columns not listed still get the presence check above.
REQUIRED_TABLE_CONTRACTS: dict[str, dict[str, ColumnContract]] = {
    "observations": {
        "seq": ColumnContract("INTEGER", not_null=False),
        "observation_id": ColumnContract("TEXT", not_null=True),
        "source": ColumnContract("TEXT", not_null=True),
        "source_id": ColumnContract("TEXT", not_null=True),
        "entity_id": ColumnContract("TEXT", not_null=True),
        "run_id": ColumnContract("TEXT", not_null=False),
        "kind": ColumnContract("TEXT", not_null=True),
        "payload": ColumnContract("TEXT", not_null=True),
        "occurred_at": ColumnContract("TEXT", not_null=True),
        "observed_at": ColumnContract("TEXT", not_null=True),
        "policy_version": ColumnContract("TEXT", not_null=True),
        "actor": ColumnContract("TEXT", not_null=True),
        "correlation_id": ColumnContract("TEXT", not_null=True),
        "evidence_hash": ColumnContract("TEXT", not_null=True),
    },
    "source_cursors": {
        "source": ColumnContract("TEXT", not_null=False),
        "cursor_value": ColumnContract("TEXT", not_null=True),
        "consumed_digest": ColumnContract("TEXT", not_null=True),
        "updated_at": ColumnContract("TEXT", not_null=True),
    },
    "duplicate_conflicts": {
        "id": ColumnContract("INTEGER", not_null=False),
        "source": ColumnContract("TEXT", not_null=True),
        "source_id": ColumnContract("TEXT", not_null=True),
        "existing_observation_id": ColumnContract("TEXT", not_null=True),
        "existing_evidence_hash": ColumnContract("TEXT", not_null=True),
        "conflicting_entity_id": ColumnContract("TEXT", not_null=True),
        "conflicting_run_id": ColumnContract("TEXT", not_null=False),
        "conflicting_kind": ColumnContract("TEXT", not_null=True),
        "conflicting_payload": ColumnContract("TEXT", not_null=True),
        "conflicting_occurred_at": ColumnContract("TEXT", not_null=True),
        "conflicting_evidence_hash": ColumnContract("TEXT", not_null=True),
        "detected_at": ColumnContract("TEXT", not_null=True),
    },
    "capability_inventory": {
        "id": ColumnContract("INTEGER", not_null=False),
        "capability_id": ColumnContract("TEXT", not_null=True),
        "cli": ColumnContract("TEXT", not_null=True),
        "model": ColumnContract("TEXT", not_null=True),
        "effort": ColumnContract("TEXT", not_null=False),
        "role": ColumnContract("TEXT", not_null=False),
        "status": ColumnContract("TEXT", not_null=True),
        "probe_evidence": ColumnContract("TEXT", not_null=False),
        "recorded_at": ColumnContract("TEXT", not_null=True),
        "updated_at": ColumnContract("TEXT", not_null=True),
    },
    "recovery_checkpoints": {
        "id": ColumnContract("INTEGER", not_null=False),
        "checkpoint_at": ColumnContract("TEXT", not_null=True),
        "source": ColumnContract("TEXT", not_null=True),
        "cursor_value": ColumnContract("TEXT", not_null=True),
        "observation_count": ColumnContract("INTEGER", not_null=True),
        "schema_version": ColumnContract("INTEGER", not_null=True),
        "integrity_hash": ColumnContract("TEXT", not_null=True),
    },
    "source_identity": {
        "source": ColumnContract("TEXT", not_null=False),
        "canonical_path": ColumnContract("TEXT", not_null=True),
        "device": ColumnContract("INTEGER", not_null=True),
        "inode": ColumnContract("INTEGER", not_null=True),
        "recorded_at": ColumnContract("TEXT", not_null=True),
    },
    "schema_migrations": {
        "version": ColumnContract("INTEGER", not_null=False),
        "applied_at": ColumnContract("TEXT", not_null=True),
    },
}

# Every append-only trigger's exact required body, keyed by trigger name.
# ``REQUIRED_TRIGGERS`` alone only proves *a* trigger with the right name
# fires on the right event — a same-name trigger whose body was swapped for
# a no-op (e.g. "SELECT 1") would still satisfy that name-only check while
# providing zero real protection. Comparing against ``sqlite_master.sql``
# (SQLite's own stored, semantically-normalized text of the CREATE TRIGGER
# statement) catches that: it must actually contain the RAISE(ABORT, ...)
# call this trigger exists to perform.
REQUIRED_TRIGGER_RAISE_FRAGMENTS: dict[str, str] = {
    "observations_no_update": "observations is append-only: UPDATE forbidden",
    "observations_no_delete": "observations is append-only: DELETE forbidden",
    "observations_no_replace_by_seq": "observations is append-only: INSERT OR REPLACE forbidden (seq)",
    "observations_no_replace_by_observation_id": "observations is append-only: INSERT OR REPLACE forbidden (observation_id)",
    "observations_no_replace_by_source_source_id": "observations is append-only: INSERT OR REPLACE forbidden (source, source_id)",
    "duplicate_conflicts_no_update": "duplicate_conflicts is append-only: UPDATE forbidden",
    "duplicate_conflicts_no_delete": "duplicate_conflicts is append-only: DELETE forbidden",
    "duplicate_conflicts_no_replace_by_id": "duplicate_conflicts is append-only: INSERT OR REPLACE forbidden (id)",
    "recovery_checkpoints_no_update": "recovery_checkpoints is append-only: UPDATE forbidden",
    "recovery_checkpoints_no_delete": "recovery_checkpoints is append-only: DELETE forbidden",
    "recovery_checkpoints_no_replace_by_id": "recovery_checkpoints is append-only: INSERT OR REPLACE forbidden (id)",
    "source_identity_no_update": "source_identity is append-only: UPDATE forbidden",
    "source_identity_no_delete": "source_identity is append-only: DELETE forbidden",
    "source_identity_no_replace_by_source": "source_identity is append-only: INSERT OR REPLACE forbidden (source)",
    "source_identity_no_replace_by_rowid": "source_identity is append-only: INSERT OR REPLACE forbidden (rowid)",
}
assert set(REQUIRED_TRIGGER_RAISE_FRAGMENTS) == set(REQUIRED_TRIGGERS)


def _normalize_sql_fragment(text: str) -> str:
    """Collapse insignificant whitespace so formatting differences (e.g. an
    extra newline SQLite's own storage happens to preserve verbatim from the
    original CREATE statement) never cause a false mismatch, while any real
    change to the fragment's content still changes the normalized text."""
    return re.sub(r"\s+", " ", text).strip()


# Parses a CREATE TRIGGER statement into its semantically load-bearing
# pieces: target table, timing/event, optional WHEN guard, and body. A
# same-name trigger recreated on a different table, under a different
# timing/event, with a widened/narrowed WHEN guard, or with a body that no
# longer actually calls RAISE(ABORT, ...) all change one of these captured
# groups — which is the point: verification below compares *structure*, not
# a raw-text substring, so a spoof can't hide behind matching whitespace or
# an unrelated decoy string that happens to contain the expected message.
_TRIGGER_DDL_RE = re.compile(
    r"CREATE\s+TRIGGER\s+(?:IF\s+NOT\s+EXISTS\s+)?(?P<name>\w+)\s+"
    r"(?P<timing>BEFORE|AFTER|INSTEAD\s+OF)\s+"
    r"(?P<event>INSERT|UPDATE|DELETE)\s+ON\s+(?P<table>\w+)\s+"
    r"(?:WHEN\s+(?P<when>.+?)\s+)?"
    r"BEGIN\s+(?P<body>.+?)\s*END\s*;?\s*\Z",
    re.IGNORECASE | re.DOTALL,
)

# The trigger body pattern this codebase relies on exclusively: a single
# statement that unconditionally raises. Anything else — including a SELECT
# that merely contains the expected message text as an inert string literal
# a WHERE clause discards ("SELECT-spoof") — fails this match and is treated
# as not actually enforcing append-only.
_RAISE_ABORT_RE = re.compile(
    r"\A\s*SELECT\s+RAISE\s*\(\s*ABORT\s*,\s*'((?:[^']|'')*)'\s*\)\s*;?\s*\Z",
    re.IGNORECASE | re.DOTALL,
)


@dataclass(frozen=True)
class TriggerFingerprint:
    """The complete normalized definition of one required append-only trigger."""

    name: str
    table: str
    timing: str
    event: str
    when: str | None
    body: str


def _parse_trigger_sql(name: str, sql: str) -> TriggerFingerprint | None:
    match = _TRIGGER_DDL_RE.match((sql or "").strip())
    if match is None:
        return None
    when = match.group("when")
    return TriggerFingerprint(
        name=name,
        table=match.group("table"),
        timing=_normalize_sql_fragment(match.group("timing")).upper(),
        event=match.group("event").upper(),
        when=_normalize_sql_fragment(when) if when else None,
        body=_normalize_sql_fragment(match.group("body")),
    )


def _trigger_raises_expected_abort(body: str, expected_message: str) -> bool:
    """True only if ``body`` is exactly ``SELECT RAISE(ABORT, '<expected_message>')``
    (whitespace-insensitive) — not merely a body that happens to contain the
    message text somewhere inside an inert statement."""
    match = _RAISE_ABORT_RE.match(body)
    if match is None:
        return False
    actual_message = match.group(1).replace("''", "'")
    return actual_message == expected_message


def _build_required_trigger_fingerprints() -> dict[str, TriggerFingerprint]:
    """Derive each required trigger's expected fingerprint from the exact DDL
    in ``MIGRATIONS`` — the single source of truth — rather than hand-
    maintaining a second, parallel description that could silently drift
    from what actually gets executed against a fresh database."""
    fingerprints: dict[str, TriggerFingerprint] = {}
    for _version, statements in MIGRATIONS:
        for statement in statements:
            if "CREATE TRIGGER" not in statement.upper():
                continue
            name_match = re.search(
                r"CREATE\s+TRIGGER\s+(?:IF\s+NOT\s+EXISTS\s+)?(\w+)", statement, re.IGNORECASE
            )
            if name_match is None or name_match.group(1) not in REQUIRED_TRIGGERS:
                continue
            parsed = _parse_trigger_sql(name_match.group(1), statement)
            if parsed is None:
                raise AssertionError(f"required trigger DDL for {name_match.group(1)!r} failed to self-parse")
            if not _trigger_raises_expected_abort(parsed.body, REQUIRED_TRIGGER_RAISE_FRAGMENTS[parsed.name]):
                raise AssertionError(
                    f"required trigger DDL for {parsed.name!r} does not match its own "
                    "REQUIRED_TRIGGER_RAISE_FRAGMENTS entry"
                )
            fingerprints[parsed.name] = parsed
    missing = set(REQUIRED_TRIGGERS) - set(fingerprints)
    if missing:
        raise AssertionError(f"MIGRATIONS is missing DDL for required triggers: {sorted(missing)}")
    return fingerprints


# Built once at import time by parsing MIGRATIONS itself, so this can never
# drift from what a fresh database actually has applied to it.
REQUIRED_TRIGGER_FINGERPRINTS: dict[str, TriggerFingerprint] = _build_required_trigger_fingerprints()


@dataclass(frozen=True)
class UniqueIndexFingerprint:
    """The semantics of one SQLite unique index, independent of its name.

    ``PRAGMA index_info`` exposes only the indexed column names.  That is
    not enough to preserve uniqueness semantics: ``TEXT UNIQUE`` and
    ``TEXT COLLATE NOCASE UNIQUE`` report the same column sequence while
    accepting different duplicate sets.  ``index_xinfo`` additionally
    reports each key column's collation and sort direction; ``index_list``
    supplies the origin/partial bits; and the normalized stored CREATE INDEX
    text preserves the exact partial-index predicate (and any expression
    syntax that PRAGMA cannot represent as a column name).
    """

    origin: str
    columns: tuple[tuple[str | None, str, str], ...]
    partial: bool
    definition: str | None


@dataclass(frozen=True)
class TableStructuralFingerprint:
    """A table's identity-bearing structural properties: which column(s)
    form its PRIMARY KEY (in declared order), whether that key is an
    AUTOINCREMENT rowid alias, and every UNIQUE index's complete semantics
    (including an index whose origin is ``'pk'``).

    Column presence/type/NOT NULL (``REQUIRED_TABLE_COLUMNS``/
    ``REQUIRED_TABLE_CONTRACTS`` above) says nothing about *identity*: a
    table rebuilt with byte-identical column names/types but no PRIMARY
    KEY/UNIQUE constraints at all would pass every one of those checks
    while silently losing the DB-enforced dedup/uniqueness invariants this
    package's ledger logic depends on (e.g. ``observations``'
    ``UNIQUE(source, source_id)``). This fingerprint makes that identity
    shape itself part of the verified (and checkpoint-hashed) contract.
    """

    table: str
    primary_key_columns: tuple[str, ...]
    autoincrement: bool
    unique_indexes: tuple[UniqueIndexFingerprint, ...]


@dataclass(frozen=True)
class TableContractFingerprint:
    """The complete canonical ``CREATE TABLE`` contract for one required table.

    The pre-existing structural fingerprint proves only identity semantics
    (PK/AUTOINCREMENT/UNIQUE).  It intentionally cannot see table-level or
    column-level details such as CHECK expressions, DEFAULT values,
    collations outside a UNIQUE index, foreign-key clauses, STRICT/WITHOUT
    ROWID options, or conflict clauses.  This fingerprint is derived from
    SQLite's stored table DDL after applying the migration DDL verbatim, then
    compared to the same canonicalization of live sqlite_master DDL.
    """

    table: str
    definition: str


def _canonicalize_table_ddl(sql: str) -> str:
    """Canonicalize SQLite table DDL without erasing semantic tokens.

    sqlite_master is the authoritative DDL source for both a reference
    database built from ``MIGRATIONS`` and a live database.  This small lexer
    removes only insignificant whitespace/comments and canonicalizes
    unquoted tokens' case; quoted literals/identifiers are retained verbatim.
    It therefore preserves the exact contract SQLite executes, including
    CHECK/default/collation/foreign-key/table-option syntax, without relying
    on a brittle hand-picked PRAGMA subset.
    """
    tokens: list[tuple[str, bool]] = []  # token text, can-touch-a-word flag
    index = 0
    length = len(sql or "")
    while index < length:
        char = sql[index]
        if char.isspace():
            index += 1
            continue
        if sql.startswith("--", index):
            newline = sql.find("\n", index + 2)
            index = length if newline == -1 else newline + 1
            continue
        if sql.startswith("/*", index):
            end = sql.find("*/", index + 2)
            if end == -1:
                return ""
            index = end + 2
            continue
        if char in "'\"`":
            quote = char
            end = index + 1
            while end < length:
                if sql[end] == quote:
                    if end + 1 < length and sql[end + 1] == quote:
                        end += 2
                        continue
                    end += 1
                    break
                end += 1
            else:
                return ""
            tokens.append((sql[index:end], True))
            index = end
            continue
        if char == "[":
            end = sql.find("]", index + 1)
            if end == -1:
                return ""
            tokens.append((sql[index : end + 1], True))
            index = end + 1
            continue
        if char.isalnum() or char in "_$":
            end = index + 1
            while end < length and (sql[end].isalnum() or sql[end] in "_$"):
                end += 1
            tokens.append((sql[index:end].upper(), True))
            index = end
            continue
        # Multi-character operators are one semantic token; preserving them
        # avoids accidentally canonicalizing e.g. '<>' into two unrelated
        # punctuation tokens in a future CHECK clause.
        operator = next(
            (candidate for candidate in ("->>", "||", "<=", ">=", "<>", "!=", "==", "<<", ">>") if sql.startswith(candidate, index)),
            None,
        )
        if operator is not None:
            tokens.append((operator, False))
            index += len(operator)
            continue
        tokens.append((char, False))
        index += 1

    rendered: list[str] = []
    prior_word = False
    for token, word in tokens:
        # Whitespace is required only between adjacent word-like tokens and
        # after a closing parenthesis before the next clause/table option.
        if rendered and word and (prior_word or rendered[-1] == ")"):
            rendered.append(" ")
        rendered.append(token)
        prior_word = word
    return "".join(rendered).rstrip(";")


def _table_contract_fingerprint(conn: sqlite3.Connection, table: str) -> TableContractFingerprint:
    row = conn.execute(
        "SELECT sql FROM sqlite_master WHERE type = 'table' AND name = ?", (table,)
    ).fetchone()
    definition = _canonicalize_table_ddl(row[0] if row is not None and row[0] is not None else "")
    return TableContractFingerprint(table=table, definition=definition)


def _table_structural_fingerprint(conn: sqlite3.Connection, table: str) -> TableStructuralFingerprint:
    """Derive ``table``'s structural fingerprint from live SQLite introspection.

    PRIMARY KEY columns come from ``PRAGMA table_info``'s ``pk`` field
    (1-based ordinal position within the key, 0 for non-key columns) sorted
    into declared order. AUTOINCREMENT has no PRAGMA-level signal at all —
    it only appears in the table's own stored CREATE TABLE text in
    ``sqlite_master`` — so it's detected there directly. UNIQUE index
    semantics come from ``PRAGMA index_list``/``index_xinfo``; primary-key
    autoindexes are retained as well because their origin, collation, and
    sort order are independent uniqueness evidence.
    """
    table_info = conn.execute(f"PRAGMA table_info({table})").fetchall()
    primary_key_columns = tuple(
        name for _pk_pos, name in sorted((row[5], row[1]) for row in table_info if row[5] != 0)
    )

    create_sql_row = conn.execute(
        "SELECT sql FROM sqlite_master WHERE type = 'table' AND name = ?", (table,)
    ).fetchone()
    create_sql = (create_sql_row[0] or "") if create_sql_row is not None else ""
    autoincrement = "AUTOINCREMENT" in create_sql.upper()

    unique_indexes: list[UniqueIndexFingerprint] = []
    for index_row in conn.execute(f"PRAGMA index_list({table})").fetchall():
        _seq, index_name, is_unique, origin, is_partial = index_row[:5]
        if not is_unique:
            continue
        index_xinfo = conn.execute(
            "PRAGMA index_xinfo(" + "'" + index_name.replace("'", "''") + "')"
        ).fetchall()
        # index_xinfo includes a non-key rowid payload row for ordinary
        # indexes.  Only key=1 rows participate in equality/uniqueness;
        # keep them in seqno order, including expression columns whose name
        # is NULL (the stored definition below then supplies their exact
        # syntax too).
        columns = tuple(
            (row[2], row[4], "DESC" if row[3] else "ASC")
            for row in sorted((row for row in index_xinfo if row[5]), key=lambda row: row[0])
        )
        index_sql_row = conn.execute(
            "SELECT sql FROM sqlite_master WHERE type = 'index' AND name = ?", (index_name,)
        ).fetchone()
        index_sql = index_sql_row[0] if index_sql_row is not None else None
        unique_indexes.append(
            UniqueIndexFingerprint(
                origin=origin,
                columns=columns,
                partial=bool(is_partial),
                # SQLite supplies NULL for automatic indexes.  Explicit
                # indexes retain the complete canonical CREATE INDEX DDL;
                # for a partial index that means its exact WHERE predicate
                # is attested rather than merely the partial flag.
                definition=_normalize_sql_fragment(index_sql) if index_sql is not None else None,
            )
        )

    return TableStructuralFingerprint(
        table=table,
        primary_key_columns=primary_key_columns,
        autoincrement=autoincrement,
        unique_indexes=tuple(sorted(unique_indexes, key=repr)),
    )


def _reference_schema_connection() -> sqlite3.Connection:
    """A private, ephemeral in-memory connection with every required
    table's DDL applied verbatim from ``MIGRATIONS`` (the single source of
    truth) plus ``schema_migrations``'s own DDL -- used only to derive the
    *expected* structural fingerprints via the exact same introspection
    ``evaluate_schema`` runs against a live DB, rather than hand-maintaining
    a second, parallel description that could silently drift.
    """
    conn = sqlite3.connect(":memory:")
    conn.execute(SCHEMA_MIGRATIONS_TABLE_DDL)
    for _version, statements in MIGRATIONS:
        for statement in statements:
            conn.execute(statement)
    return conn


def _build_required_table_structural_fingerprints() -> dict[str, TableStructuralFingerprint]:
    conn = _reference_schema_connection()
    try:
        return {table: _table_structural_fingerprint(conn, table) for table in REQUIRED_TABLE_COLUMNS}
    finally:
        conn.close()


def _build_required_table_contract_fingerprints() -> dict[str, TableContractFingerprint]:
    conn = _reference_schema_connection()
    try:
        return {table: _table_contract_fingerprint(conn, table) for table in REQUIRED_TABLE_COLUMNS}
    finally:
        conn.close()


# Built once at import time against a reference DB constructed purely from
# MIGRATIONS/SCHEMA_MIGRATIONS_TABLE_DDL, so this can never drift from what
# a fresh database actually has applied to it.
REQUIRED_TABLE_STRUCTURAL_FINGERPRINTS: dict[str, TableStructuralFingerprint] = (
    _build_required_table_structural_fingerprints()
)

# Like the structural fingerprints, this is derived by executing the exact
# migration DDL in a reference database, not copied into a parallel contract.
REQUIRED_TABLE_CONTRACT_FINGERPRINTS: dict[str, TableContractFingerprint] = (
    _build_required_table_contract_fingerprints()
)


class SchemaVerificationError(RuntimeError):
    """Raised when the control-plane DB's schema doesn't match what this code requires.

    Covers: applied migrations not matching exactly what ``MIGRATIONS``
    expects, a required table/column missing, structural/index semantics
    differing, a CHECK/default/collation/foreign-key/table-option DDL contract
    differing, a required append-only trigger missing or any unexpected
    trigger being present, or ``PRAGMA integrity_check`` reporting anything
    but ``ok``. Any of these is a fail-closed condition — a checkpoint or
    read path must refuse rather than proceed against a schema it can't
    trust.
    """


@dataclass(frozen=True)
class SchemaVerificationResult:
    applied_migrations: tuple[int, ...]
    triggers: tuple[str, ...]
    # Sorted-by-name (name, table, timing, event, when, body) tuples: the
    # complete normalized fingerprint evidence for every required trigger,
    # as actually found in sqlite_master at verification time. Consumed by
    # observer.checkpoint_integrity_hash() as part of a checkpoint's
    # schema/trigger fingerprint evidence.
    trigger_fingerprints: tuple[tuple[str, str, str, str, str | None, str], ...]
    # Sorted-by-table (table, primary_key_columns, autoincrement,
    # unique_indexes) tuples: the complete structural identity evidence
    # (PRIMARY KEY / AUTOINCREMENT / fully-described UNIQUE indexes) for every required table, as
    # actually found at verification time. Consumed by
    # observer.checkpoint_integrity_hash() the same way trigger_fingerprints
    # is, so a structural rebuild-without-identity-constraints attack is
    # covered by checkpoint integrity too, not just a live verify_schema()
    # call.
    table_fingerprints: tuple[
        tuple[str, tuple[str, ...], bool, tuple[tuple[str, tuple[tuple[str | None, str, str], ...], bool, str | None], ...]],
        ...,
    ]
    # Sorted-by-table (table, canonical CREATE TABLE definition) tuples:
    # complete migration-derived semantic table contracts, covering details
    # PRAGMA-only structural checks cannot represent.  Checkpoint hashes must
    # include these alongside the identity fingerprints above.
    table_contract_fingerprints: tuple[tuple[str, str], ...]
    integrity_check: str


def evaluate_schema(conn: sqlite3.Connection) -> SchemaVerificationResult:
    """Run every schema check against whatever transaction ``conn`` is already in.

    Caller-managed transaction: this does not open or close one itself, so a
    caller that needs *other* reads (e.g. cursor value, observation count)
    captured from the exact same consistent snapshot as these checks can wrap
    both in a single transaction and call this in the middle of it. Standalone
    callers should use ``verify_schema`` instead, which manages its own
    transaction. Raises ``SchemaVerificationError`` on the first mismatch.
    """
    applied = tuple(
        sorted(row[0] for row in conn.execute("SELECT version FROM schema_migrations").fetchall())
    )
    expected = tuple(version for version, _ in MIGRATIONS)
    if applied != expected:
        raise SchemaVerificationError(
            f"applied migrations {applied} do not match expected {expected}"
        )

    existing_tables = {
        row[0] for row in conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'").fetchall()
    }
    missing_tables = sorted(set(REQUIRED_TABLE_COLUMNS) - existing_tables)
    if missing_tables:
        raise SchemaVerificationError(f"missing required tables: {missing_tables}")

    for table, required_columns in REQUIRED_TABLE_COLUMNS.items():
        table_info = conn.execute(f"PRAGMA table_info({table})").fetchall()
        existing_columns = {row[1] for row in table_info}
        missing_columns = sorted(set(required_columns) - existing_columns)
        if missing_columns:
            raise SchemaVerificationError(f"table {table!r} missing required columns: {missing_columns}")

        contracts = REQUIRED_TABLE_CONTRACTS.get(table, {})
        columns_by_name = {row[1]: row for row in table_info}
        for column, contract in contracts.items():
            # PRAGMA table_info row shape: (cid, name, type, notnull, dflt_value, pk)
            _, _, declared_type, declared_not_null, _, _ = columns_by_name[column]
            if declared_type.strip().upper() != contract.column_type.upper():
                raise SchemaVerificationError(
                    f"table {table!r} column {column!r} has declared type {declared_type!r}, "
                    f"expected {contract.column_type!r}"
                )
            if bool(declared_not_null) != contract.not_null:
                raise SchemaVerificationError(
                    f"table {table!r} column {column!r} has NOT NULL={bool(declared_not_null)}, "
                    f"expected {contract.not_null}"
                )

    # The complete table DDL is a separate attested contract from the
    # identity-only structural fingerprint below.  Comparing canonical
    # sqlite_master definitions derived from a reference DB built directly
    # from MIGRATIONS catches a rebuild that preserves every visible column,
    # PK, and UNIQUE index while weakening CHECK/default/collation/foreign
    # key/table option semantics.
    table_contract_fingerprints: dict[str, TableContractFingerprint] = {}
    for table, expected_fp in REQUIRED_TABLE_CONTRACT_FINGERPRINTS.items():
        actual_fp = _table_contract_fingerprint(conn, table)
        if actual_fp.definition != expected_fp.definition:
            raise SchemaVerificationError(
                f"table {table!r} canonical CREATE TABLE contract differs from migration DDL; "
                f"got {actual_fp.definition!r}, expected {expected_fp.definition!r}"
            )
        table_contract_fingerprints[table] = actual_fp

    # Column name/type/NOT NULL alone says nothing about identity: a table
    # rebuilt with byte-identical columns but no PRIMARY KEY/UNIQUE/
    # AUTOINCREMENT constraints at all would pass every check above while
    # silently losing the DB-enforced dedup/uniqueness invariants this
    # package's ledger logic relies on. Compare the full structural
    # fingerprint (see TableStructuralFingerprint) against
    # REQUIRED_TABLE_STRUCTURAL_FINGERPRINTS for every required table.
    table_fingerprints: dict[str, TableStructuralFingerprint] = {}
    for table, expected_fp in REQUIRED_TABLE_STRUCTURAL_FINGERPRINTS.items():
        actual_fp = _table_structural_fingerprint(conn, table)
        if actual_fp.primary_key_columns != expected_fp.primary_key_columns:
            raise SchemaVerificationError(
                f"table {table!r} has PRIMARY KEY columns {actual_fp.primary_key_columns}, "
                f"expected {expected_fp.primary_key_columns} — possible identity-stripped table rebuild"
            )
        if actual_fp.autoincrement != expected_fp.autoincrement:
            raise SchemaVerificationError(
                f"table {table!r} has AUTOINCREMENT={actual_fp.autoincrement}, "
                f"expected {expected_fp.autoincrement} — possible identity-stripped table rebuild"
            )
        if actual_fp.unique_indexes != expected_fp.unique_indexes:
            raise SchemaVerificationError(
                f"table {table!r} has UNIQUE index semantics {actual_fp.unique_indexes}, "
                f"expected {expected_fp.unique_indexes} — possible identity/unique-semantics table rebuild"
            )
        table_fingerprints[table] = actual_fp

    existing_triggers = {
        row[0]: (row[1], row[2])  # name -> (tbl_name, sql)
        for row in conn.execute(
            "SELECT name, tbl_name, sql FROM sqlite_master WHERE type = 'trigger'"
        ).fetchall()
    }
    # TEMP triggers live only in this connection's temp schema, not in
    # sqlite_master.  They can still fire on a persistent control-plane
    # table, however, and can therefore alter the exact writes a caller is
    # trying to attest to.  Reject every such trigger rather than treating a
    # connection-local side effect as invisible to schema verification.
    temp_control_plane_triggers = sorted(
        row[0]
        for row in conn.execute(
            "SELECT name FROM sqlite_temp_master WHERE type = 'trigger' AND tbl_name IN "
            f"({', '.join('?' for _ in REQUIRED_TABLE_COLUMNS)})",
            tuple(REQUIRED_TABLE_COLUMNS),
        ).fetchall()
    )
    if temp_control_plane_triggers:
        raise SchemaVerificationError(
            f"unexpected TEMP control-plane triggers: {temp_control_plane_triggers}; refusing schema with "
            "connection-local un-attested trigger side effects"
        )
    missing_triggers = sorted(set(REQUIRED_TRIGGERS) - set(existing_triggers))
    if missing_triggers:
        raise SchemaVerificationError(f"missing required append-only triggers: {missing_triggers}")
    unexpected_triggers = sorted(set(existing_triggers) - set(REQUIRED_TRIGGERS))
    if unexpected_triggers:
        raise SchemaVerificationError(
            f"unexpected control-plane triggers: {unexpected_triggers}; refusing schema with un-attested "
            "trigger side effects"
        )

    # Compare the *complete normalized definition* of every required trigger
    # (target table, BEFORE timing/event, WHEN guard, and RAISE(ABORT) body)
    # against REQUIRED_TRIGGER_FINGERPRINTS, not merely an expected-string
    # substring against the raw trigger SQL. A same-name trigger recreated on
    # the wrong table, under the wrong timing/event, with a widened/dropped
    # WHEN guard, or whose body only contains the expected message text as
    # an inert decoy (a no-op SELECT that never calls RAISE) all fail here.
    trigger_fingerprints: dict[str, TriggerFingerprint] = {}
    for trigger, expected in REQUIRED_TRIGGER_FINGERPRINTS.items():
        tbl_name, trigger_sql = existing_triggers[trigger]
        if tbl_name != expected.table:
            raise SchemaVerificationError(
                f"trigger {trigger!r} is defined on table {tbl_name!r}, expected {expected.table!r} — "
                "possible wrong-target same-name trigger spoof"
            )
        parsed = _parse_trigger_sql(trigger, trigger_sql)
        if parsed is None:
            raise SchemaVerificationError(
                f"trigger {trigger!r} SQL could not be parsed for fingerprint verification: {trigger_sql!r}"
            )
        if (parsed.table, parsed.timing, parsed.event, parsed.when) != (
            expected.table,
            expected.timing,
            expected.event,
            expected.when,
        ):
            raise SchemaVerificationError(
                f"trigger {trigger!r} definition does not match its required fingerprint "
                f"(table/timing/event/WHEN mismatch: got table={parsed.table!r} timing={parsed.timing!r} "
                f"event={parsed.event!r} when={parsed.when!r}, expected table={expected.table!r} "
                f"timing={expected.timing!r} event={expected.event!r} when={expected.when!r}) — "
                "possible same-name spoofed trigger"
            )
        raise_fragment = REQUIRED_TRIGGER_RAISE_FRAGMENTS[trigger]
        if not _trigger_raises_expected_abort(parsed.body, raise_fragment):
            raise SchemaVerificationError(
                f"trigger {trigger!r} body does not enforce append-only via an unconditional "
                f"RAISE(ABORT, {raise_fragment!r}) call — possible SELECT-spoof no-op replacement"
            )
        trigger_fingerprints[trigger] = parsed

    integrity_rows = conn.execute("PRAGMA integrity_check").fetchall()
    integrity_result = integrity_rows[0][0] if integrity_rows else "unknown"
    if integrity_result != "ok":
        raise SchemaVerificationError(
            f"PRAGMA integrity_check failed: {[row[0] for row in integrity_rows]}"
        )

    return SchemaVerificationResult(
        applied_migrations=applied,
        triggers=tuple(sorted(existing_triggers)),
        trigger_fingerprints=tuple(
            (fp.name, fp.table, fp.timing, fp.event, fp.when, fp.body)
            for fp in (trigger_fingerprints[name] for name in sorted(trigger_fingerprints))
        ),
        table_fingerprints=tuple(
            (
                fp.table,
                fp.primary_key_columns,
                fp.autoincrement,
                tuple(
                    (index.origin, index.columns, index.partial, index.definition)
                    for index in fp.unique_indexes
                ),
            )
            for fp in (table_fingerprints[name] for name in sorted(table_fingerprints))
        ),
        table_contract_fingerprints=tuple(
            (fp.table, fp.definition)
            for fp in (table_contract_fingerprints[name] for name in sorted(table_contract_fingerprints))
        ),
        integrity_check=integrity_result,
    )


def verify_schema(conn: sqlite3.Connection) -> SchemaVerificationResult:
    """Verify migrations/tables/columns/triggers/integrity from one consistent snapshot.

    Wrapped in its own (deferred, non-``IMMEDIATE``) transaction so every
    check reads the same point-in-time snapshot rather than racing a
    concurrent writer between individual statements — this is a read-only
    pass, so it never needs the write lock ``BEGIN IMMEDIATE`` would take.
    Never returns a partial/best-effort result: ``evaluate_schema`` raises on
    the first thing that doesn't match.
    """
    conn.execute("BEGIN")
    try:
        return evaluate_schema(conn)
    finally:
        conn.execute("ROLLBACK")


def latest_schema_version() -> int:
    return MIGRATIONS[-1][0]


def run_migrations(conn: sqlite3.Connection) -> None:
    """Apply every migration in ``MIGRATIONS`` not yet recorded as applied.

    Safe to call on every ``connect()`` — the migrations table plus the
    per-version writer transaction (BEGIN IMMEDIATE, recheck, apply, record)
    make this idempotent and race-safe whether it's the first boot or the
    thousandth, or two connections booting at once.
    """
    # Deferred import: db.py imports run_migrations from this module at
    # module load time, so importing db.py back at *this* module's load time
    # would be circular. By the time run_migrations() actually runs (inside
    # connect(), after both modules have finished loading), the import below
    # is just a normal cache hit.
    from orchestrator.control_plane.db import retry_on_locked, transaction

    def _ensure_migrations_table() -> None:
        with transaction(conn) as c:
            c.execute(SCHEMA_MIGRATIONS_TABLE_DDL)

    retry_on_locked(_ensure_migrations_table)

    for version, statements in MIGRATIONS:

        def _apply(version=version, statements=statements) -> None:
            with transaction(conn) as c:
                already_applied = c.execute(
                    "SELECT 1 FROM schema_migrations WHERE version = ?", (version,)
                ).fetchone()
                if already_applied is not None:
                    return
                for statement in statements:
                    c.execute(statement)
                c.execute(
                    "INSERT INTO schema_migrations (version, applied_at) VALUES (?, datetime('now'))",
                    (version,),
                )

        retry_on_locked(_apply)
