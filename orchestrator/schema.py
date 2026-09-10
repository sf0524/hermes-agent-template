"""Idempotent schema for the orchestrator's durable SQLite state DB.

Each migration is a list of individual DDL statements guarded by
``schema_migrations``. Every migration is applied inside one writer
transaction (``BEGIN IMMEDIATE``) that rechecks the version is still unapplied
*after* acquiring the write lock, so two connections racing to migrate the
same fresh DB (two processes booting concurrently, or a test that calls
``connect()`` twice) can never both insert the same ``schema_migrations`` row
— one wins the lock, the other sees the version already recorded and skips.
"""

from __future__ import annotations

import hashlib
import json
import sqlite3
from typing import Callable

# (version, [sql statements]) pairs, applied in order — each version's
# statements run inside one writer transaction. Append new versions here —
# never edit an already-shipped entry, since that would desync deployments
# that already recorded it as applied.
MIGRATIONS: list[tuple[int, list[str]]] = [
    (
        1,
        [
            """
            CREATE TABLE IF NOT EXISTS events (
                seq              INTEGER PRIMARY KEY AUTOINCREMENT,
                event_id         TEXT NOT NULL UNIQUE,
                source           TEXT NOT NULL,
                source_dedup_key TEXT NOT NULL,
                entity_id        TEXT NOT NULL,
                causal_id        TEXT,
                event_type       TEXT NOT NULL,
                payload          TEXT NOT NULL,
                occurred_at      TEXT NOT NULL,
                received_at      TEXT NOT NULL,
                UNIQUE(source, source_dedup_key)
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS consumer_inbox (
                consumer         TEXT NOT NULL,
                seq              INTEGER NOT NULL,
                event_id         TEXT NOT NULL,
                status           TEXT NOT NULL CHECK(status IN ('claimed', 'acked')),
                attempt_count    INTEGER NOT NULL DEFAULT 1,
                claimed_at       TEXT NOT NULL,
                lease_expires_at TEXT NOT NULL,
                acked_at         TEXT,
                PRIMARY KEY (consumer, seq)
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS consumer_cursor (
                consumer         TEXT PRIMARY KEY,
                acked_through_seq INTEGER NOT NULL DEFAULT 0
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS audit_log (
                id           INTEGER PRIMARY KEY AUTOINCREMENT,
                recorded_at  TEXT NOT NULL,
                actor        TEXT NOT NULL,
                action       TEXT NOT NULL,
                subject_id   TEXT,
                detail       TEXT
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS outbox (
                idempotency_key  TEXT PRIMARY KEY,
                consumer         TEXT NOT NULL,
                action_type      TEXT NOT NULL,
                payload          TEXT NOT NULL,
                status           TEXT NOT NULL CHECK(status IN ('pending', 'dispatched', 'failed')) DEFAULT 'pending',
                causal_event_id  TEXT,
                created_at       TEXT NOT NULL,
                updated_at       TEXT NOT NULL
            )
            """,
        ],
    ),
    (
        2,
        [
            # audit_log is append-only by convention (no UPDATE/DELETE method
            # is exposed) and these triggers make that a DB-level guarantee
            # too, so a bug elsewhere in the process can't quietly rewrite
            # history.
            """
            CREATE TRIGGER IF NOT EXISTS audit_log_no_update
            BEFORE UPDATE ON audit_log
            BEGIN
                SELECT RAISE(ABORT, 'audit_log is append-only: UPDATE forbidden');
            END
            """,
            """
            CREATE TRIGGER IF NOT EXISTS audit_log_no_delete
            BEFORE DELETE ON audit_log
            BEGIN
                SELECT RAISE(ABORT, 'audit_log is append-only: DELETE forbidden');
            END
            """,
        ],
    ),
    (
        3,
        [
            # events is the ledger's source of truth and must be append-only
            # for the same reason audit_log is: nothing in this package ever
            # updates or deletes an event, and these triggers make that a
            # DB-level guarantee rather than just a convention.
            """
            CREATE TRIGGER IF NOT EXISTS events_no_update
            BEFORE UPDATE ON events
            BEGIN
                SELECT RAISE(ABORT, 'events is append-only: UPDATE forbidden');
            END
            """,
            """
            CREATE TRIGGER IF NOT EXISTS events_no_delete
            BEFORE DELETE ON events
            BEGIN
                SELECT RAISE(ABORT, 'events is append-only: DELETE forbidden');
            END
            """,
            # consumer_inbox is rebuilt (SQLite can't ALTER TABLE to add a
            # foreign key) to add:
            #   - claim_token: a fresh opaque token minted on every claim and
            #     reclaim, required by Inbox.ack so an ack from a lease that
            #     has since been reclaimed by someone else fails instead of
            #     acking work it no longer holds.
            #   - a foreign key to events(event_id), so an inbox row can
            #     never dangle. Only runs once (guarded by the version check
            #     above), so it doesn't need to be its own IF NOT EXISTS-safe
            #     no-op like the rest of this file.
            """
            CREATE TABLE consumer_inbox_v3 (
                consumer         TEXT NOT NULL,
                seq              INTEGER NOT NULL,
                event_id         TEXT NOT NULL,
                status           TEXT NOT NULL CHECK(status IN ('claimed', 'acked')),
                attempt_count    INTEGER NOT NULL DEFAULT 1,
                claimed_at       TEXT NOT NULL,
                lease_expires_at TEXT NOT NULL,
                acked_at         TEXT,
                claim_token      TEXT NOT NULL DEFAULT '',
                PRIMARY KEY (consumer, seq),
                FOREIGN KEY (event_id) REFERENCES events(event_id)
            )
            """,
            """
            INSERT INTO consumer_inbox_v3
                (consumer, seq, event_id, status, attempt_count, claimed_at, lease_expires_at, acked_at, claim_token)
            SELECT consumer, seq, event_id, status, attempt_count, claimed_at, lease_expires_at, acked_at, ''
            FROM consumer_inbox
            """,
            "DROP TABLE consumer_inbox",
            "ALTER TABLE consumer_inbox_v3 RENAME TO consumer_inbox",
        ],
    ),
    (
        4,
        [
            # events already has seq as its PK and event_id UNIQUE, but
            # nothing ties a specific (seq, event_id) *pair* together — the
            # v3 foreign key only checked that event_id existed somewhere in
            # events, not that consumer_inbox's own seq column named the same
            # row. A composite UNIQUE index is what lets a composite foreign
            # key reference (seq, event_id) as a unit below: SQLite requires
            # the referenced columns to be backed by a unique index over
            # exactly those columns, in that order.
            "CREATE UNIQUE INDEX IF NOT EXISTS events_seq_event_id_uindex ON events(seq, event_id)",
            # consumer_inbox is rebuilt again (SQLite can't ALTER TABLE to
            # change a foreign key) so its (seq, event_id) pair is checked
            # together against events, not event_id alone — a row whose seq
            # and event_id each individually reference *some* real event, but
            # not the *same* one, is now rejected at the DB level instead of
            # silently desyncing seq and event_id for that consumer's claim.
            """
            CREATE TABLE consumer_inbox_v4 (
                consumer         TEXT NOT NULL,
                seq              INTEGER NOT NULL,
                event_id         TEXT NOT NULL,
                status           TEXT NOT NULL CHECK(status IN ('claimed', 'acked')),
                attempt_count    INTEGER NOT NULL DEFAULT 1,
                claimed_at       TEXT NOT NULL,
                lease_expires_at TEXT NOT NULL,
                acked_at         TEXT,
                claim_token      TEXT NOT NULL DEFAULT '',
                PRIMARY KEY (consumer, seq),
                FOREIGN KEY (seq, event_id) REFERENCES events(seq, event_id)
            )
            """,
            """
            INSERT INTO consumer_inbox_v4
                (consumer, seq, event_id, status, attempt_count, claimed_at, lease_expires_at, acked_at, claim_token)
            SELECT consumer, seq, event_id, status, attempt_count, claimed_at, lease_expires_at, acked_at, claim_token
            FROM consumer_inbox
            """,
            "DROP TABLE consumer_inbox",
            "ALTER TABLE consumer_inbox_v4 RENAME TO consumer_inbox",
        ],
    ),
    (
        5,
        [
            # Durable Human Owner evidence: a grant records exactly the
            # scope/subject/action_type/payload_digest a Human Owner
            # approved, once — payload_digest is the canonical digest of the
            # exact payload approved, so a grant for "comment on pr-42"
            # cannot be replayed to authorize a different, never-approved
            # comment body. Nothing in this package ever updates
            # scope/subject/action_type/payload_digest/granted_by/granted_at
            # after insert — the trigger below makes that a DB-level
            # guarantee, not just a convention. consumed_at /
            # consumed_by_idempotency_key are the only columns ever updated,
            # exactly once (human_owner.py enforces the once-only transition
            # in application code, re-checked inside its own transaction).
            """
            CREATE TABLE IF NOT EXISTS human_owner_grants (
                grant_id                    TEXT PRIMARY KEY,
                scope                       TEXT NOT NULL,
                subject                     TEXT NOT NULL,
                action_type                 TEXT NOT NULL,
                payload_digest              TEXT NOT NULL,
                granted_by                  TEXT NOT NULL,
                granted_at                  TEXT NOT NULL,
                consumed_at                 TEXT,
                consumed_by_idempotency_key TEXT
            )
            """,
            """
            CREATE TRIGGER IF NOT EXISTS human_owner_grants_core_immutable
            BEFORE UPDATE ON human_owner_grants
            WHEN NEW.scope IS NOT OLD.scope
              OR NEW.subject IS NOT OLD.subject
              OR NEW.action_type IS NOT OLD.action_type
              OR NEW.payload_digest IS NOT OLD.payload_digest
              OR NEW.granted_by IS NOT OLD.granted_by
              OR NEW.granted_at IS NOT OLD.granted_at
            BEGIN
                SELECT RAISE(ABORT, 'human_owner_grants scope/subject/action_type/payload_digest/granted_by/granted_at are immutable');
            END
            """,
            """
            CREATE TRIGGER IF NOT EXISTS human_owner_grants_no_delete
            BEFORE DELETE ON human_owner_grants
            BEGIN
                SELECT RAISE(ABORT, 'human_owner_grants is append-only: DELETE forbidden');
            END
            """,
            # Target-scoped, default-deny action interface intents. Every row
            # names the exact Human Owner grant it durably consumed
            # (grant_id) — the foreign key means an intent can never
            # reference a grant that doesn't exist. Status starts 'pending'
            # (intent + grant-consumption already committed, transport not
            # yet attempted), moves to 'dispatching' only via an atomic
            # claim (UPDATE ... WHERE status = 'pending'), then to a
            # terminal 'dispatched' or 'failed'.
            """
            CREATE TABLE IF NOT EXISTS action_intents (
                idempotency_key TEXT PRIMARY KEY,
                consumer        TEXT NOT NULL,
                action_type     TEXT NOT NULL,
                target_scope    TEXT NOT NULL,
                subject         TEXT NOT NULL,
                payload         TEXT NOT NULL,
                grant_id        TEXT NOT NULL REFERENCES human_owner_grants(grant_id),
                status          TEXT NOT NULL CHECK(status IN ('pending', 'dispatching', 'dispatched', 'failed')) DEFAULT 'pending',
                causal_event_id TEXT,
                created_at      TEXT NOT NULL,
                updated_at      TEXT NOT NULL,
                dispatched_at   TEXT
            )
            """,
        ],
    ),
    (
        6,
        [
            # Grant-intent integrity, made structural rather than a
            # two-step application convention: a grant can be consumed
            # *only* by an action_intents row naming it, and that row can
            # only ever be inserted once per grant. Before v6,
            # human_owner.py's consume_grant_in_transaction() UPDATEd
            # human_owner_grants directly and application code was trusted
            # to always pair that UPDATE with an action_intents INSERT in
            # the same transaction -- a trusted convention, not a
            # guarantee. v6 removes that function entirely: the only path
            # left is inserting into action_intents, and the triggers below
            # derive the grant's consumption from that insert automatically
            # (see action_gateway.py's _record_intent).
            #
            # One grant authorizes at most one action_intents row --
            # without this, a grant already consumed by intent A could
            # still receive a second, unrelated intent B row referencing
            # the same grant_id.
            "CREATE UNIQUE INDEX IF NOT EXISTS action_intents_grant_id_uindex ON action_intents(grant_id)",
            # An action_intents row may only be inserted against a grant
            # that still exists and is not yet consumed -- the DB-level
            # counterpart of action_gateway._record_intent's own
            # scope/subject/action_type/payload_digest + freshness checks,
            # so the invariant holds even for a row inserted by something
            # other than the gateway (e.g. reconcile_pending's crash-replay
            # path, or a test fixture).
            """
            CREATE TRIGGER IF NOT EXISTS action_intents_requires_fresh_grant
            BEFORE INSERT ON action_intents
            BEGIN
                SELECT RAISE(ABORT, 'action_intents insert requires an existing, unconsumed human_owner_grants row')
                WHERE NOT EXISTS (
                    SELECT 1 FROM human_owner_grants
                    WHERE grant_id = NEW.grant_id AND consumed_at IS NULL
                );
            END
            """,
            # The insert that just passed the trigger above is itself the
            # sole evidence a grant was ever consumed: this derives
            # consumed_at/consumed_by_idempotency_key straight from the row
            # that was just inserted, in the same statement's transaction,
            # rather than trusting a second, separate UPDATE issued by
            # application code to actually happen.
            """
            CREATE TRIGGER IF NOT EXISTS action_intents_consumes_grant
            AFTER INSERT ON action_intents
            BEGIN
                UPDATE human_owner_grants
                SET consumed_at = NEW.created_at,
                    consumed_by_idempotency_key = NEW.idempotency_key
                WHERE grant_id = NEW.grant_id;
            END
            """,
            # human_owner_grants_core_immutable (v5) already locks down
            # scope/subject/action_type/payload_digest/granted_by/granted_at.
            # This locks down the two remaining columns: consumed_at and
            # consumed_by_idempotency_key may transition exactly once, from
            # NULL to a value, and *only* as the byproduct of the matching
            # action_intents row the trigger above just inserted -- the
            # EXISTS clause is what makes this the "except that derived
            # initial transition" carve-out rather than a blanket allow of
            # any NULL -> value write. A raw UPDATE that sets these columns
            # without a corresponding action_intents row (the orphan-
            # consumption gap this migration closes) has no matching row to
            # satisfy the EXISTS clause and is rejected. Any change once
            # already consumed -- including reverting back to NULL -- is
            # rejected unconditionally.
            """
            CREATE TRIGGER IF NOT EXISTS human_owner_grants_consumption_immutable
            BEFORE UPDATE OF consumed_at, consumed_by_idempotency_key ON human_owner_grants
            WHEN NOT (
                OLD.consumed_at IS NULL
                AND OLD.consumed_by_idempotency_key IS NULL
                AND NEW.consumed_at IS NOT NULL
                AND NEW.consumed_by_idempotency_key IS NOT NULL
                AND EXISTS (
                    SELECT 1 FROM action_intents
                    WHERE grant_id = OLD.grant_id
                      AND idempotency_key = NEW.consumed_by_idempotency_key
                      AND created_at = NEW.consumed_at
                )
            )
            BEGIN
                SELECT RAISE(ABORT, 'human_owner_grants consumption fields may only be set once, by the action_intents insert that derives them, and never changed after');
            END
            """,
            # action_intents is the durable record of exactly which intent
            # consumed which grant -- deleting a row would sever that
            # correspondence (a grant left looking consumed with no intent
            # to show for it) just as surely as an orphan-producing UPDATE
            # would.
            """
            CREATE TRIGGER IF NOT EXISTS action_intents_no_delete
            BEFORE DELETE ON action_intents
            BEGIN
                SELECT RAISE(ABORT, 'action_intents is append-only: DELETE forbidden');
            END
            """,
            # Reassigning grant_id or idempotency_key after insert would
            # let a row silently point at a different grant (or answer to a
            # different idempotency key) than the one its insert was
            # validated and derived against -- the same correspondence
            # break as a delete, just via UPDATE instead.
            """
            CREATE TRIGGER IF NOT EXISTS action_intents_grant_id_immutable
            BEFORE UPDATE OF grant_id ON action_intents
            WHEN NEW.grant_id IS NOT OLD.grant_id
            BEGIN
                SELECT RAISE(ABORT, 'action_intents.grant_id is immutable');
            END
            """,
            """
            CREATE TRIGGER IF NOT EXISTS action_intents_idempotency_key_immutable
            BEFORE UPDATE OF idempotency_key ON action_intents
            WHEN NEW.idempotency_key IS NOT OLD.idempotency_key
            BEGIN
                SELECT RAISE(ABORT, 'action_intents.idempotency_key is immutable');
            END
            """,
        ],
    ),
    (
        7,
        [
            # v6's action_intents_requires_fresh_grant only checked that
            # *some* unconsumed grant existed for NEW.grant_id -- it never
            # checked that the grant's own scope/subject/action_type/payload
            # actually match this intent. A raw INSERT naming a real,
            # unconsumed grant_id but different coordinates or payload could
            # therefore slip past the DB and (via action_intents_consumes_grant)
            # consume a grant it was never approved against -- the DB-level
            # counterpart of the gap action_gateway.py's _record_intent
            # already closes in application code (its own scope/subject/
            # action_type/payload_digest match, before this same INSERT).
            # Recreated (not IF NOT EXISTS) because SQLite has no ALTER
            # TRIGGER: the v6 body must be replaced, not left in place beside
            # a same-named new one.
            "DROP TRIGGER IF EXISTS action_intents_requires_fresh_grant",
            """
            CREATE TRIGGER action_intents_requires_fresh_grant
            BEFORE INSERT ON action_intents
            BEGIN
                SELECT RAISE(ABORT, 'action_intents insert requires an existing, unconsumed human_owner_grants row whose scope/subject/action_type/payload digest exactly match this intent')
                WHERE NOT EXISTS (
                    SELECT 1 FROM human_owner_grants
                    WHERE grant_id = NEW.grant_id
                      AND consumed_at IS NULL
                      AND scope = NEW.target_scope
                      AND subject = NEW.subject
                      AND action_type = NEW.action_type
                      AND payload_digest = action_intents_payload_digest(NEW.payload)
                );
            END
            """,
            # v6 already locked down grant_id and idempotency_key
            # individually. This locks down the rest of action_intents'
            # request-identity columns -- consumer, action_type,
            # target_scope, subject, payload, causal_event_id, created_at --
            # the same way: none of them may change after insert, since each
            # one was part of what the fresh-grant trigger above (and
            # action_gateway.py's own checks) validated at insert time.
            # status/updated_at/dispatched_at are deliberately excluded --
            # those are the lifecycle columns _claim_and_dispatch/_finish
            # legitimately write after insert.
            """
            CREATE TRIGGER IF NOT EXISTS action_intents_identity_immutable
            BEFORE UPDATE OF consumer, action_type, target_scope, subject, payload, causal_event_id, created_at
            ON action_intents
            WHEN NEW.consumer IS NOT OLD.consumer
              OR NEW.action_type IS NOT OLD.action_type
              OR NEW.target_scope IS NOT OLD.target_scope
              OR NEW.subject IS NOT OLD.subject
              OR NEW.payload IS NOT OLD.payload
              OR NEW.causal_event_id IS NOT OLD.causal_event_id
              OR NEW.created_at IS NOT OLD.created_at
            BEGIN
                SELECT RAISE(ABORT, 'action_intents consumer/action_type/target_scope/subject/payload/causal_event_id/created_at are immutable');
            END
            """,
        ],
    ),
]

# Guards keyed by the migration version they must run *before* -- inside the
# same per-version writer transaction as that version's own statements, after
# the "already applied?" recheck but before any DDL runs. Versions 6 and 7
# each turn a previously-conventional invariant into a structural, DB-enforced
# one, so each needs a guard that rejects pre-existing data the new trigger
# could never have produced:
#   - v6 turns "a consumed grant has a corresponding action_intents row" into
#     a structural invariant (see the v6 migration above), so any deployment
#     upgrading from v5 with data that already violates that invariant -- a
#     grant marked consumed with no matching intent row, left behind by the
#     old, merely-conventional consume_grant_in_transaction() two-step --
#     must fail the upgrade instead of silently locking in corrupt state.
#   - v7 turns "an action_intents row's coordinates exactly match the grant
#     it consumed" into a structural invariant. v6's own trigger never
#     checked that, so a deployment upgrading from v6 could already hold an
#     intent/grant pair with mismatched action_type/target_scope/subject/
#     payload -- that pair must fail the upgrade too, rather than becoming
#     reconcilable/dispatchable under a schema that now claims the binding
#     is guaranteed.
MIGRATION_PREFLIGHTS: dict[int, Callable[[sqlite3.Connection], None]] = {}


class LegacyIntegrityViolation(RuntimeError):
    """Raised when a migration's preflight guard finds pre-migration data that
    would violate the invariant that migration is about to make structural."""


def _v6_legacy_integrity_guard(conn: sqlite3.Connection) -> None:
    orphan = conn.execute(
        """
        SELECT g.grant_id FROM human_owner_grants g
        LEFT JOIN action_intents ai
          ON ai.grant_id = g.grant_id AND ai.idempotency_key = g.consumed_by_idempotency_key
        WHERE g.consumed_at IS NOT NULL AND ai.idempotency_key IS NULL
        LIMIT 1
        """
    ).fetchone()
    if orphan is not None:
        raise LegacyIntegrityViolation(
            f"human_owner_grants {orphan['grant_id']!r} is marked consumed but has no corresponding "
            "action_intents row -- refusing to apply migration 6 (grant-intent integrity) over this "
            "legacy orphan-consumption state"
        )
    duplicate = conn.execute(
        "SELECT grant_id FROM action_intents GROUP BY grant_id HAVING COUNT(*) > 1 LIMIT 1"
    ).fetchone()
    if duplicate is not None:
        raise LegacyIntegrityViolation(
            f"human_owner_grants {duplicate['grant_id']!r} is referenced by more than one action_intents "
            "row -- refusing to apply migration 6 (action_intents.grant_id uniqueness) over this legacy state"
        )
    # Before v6, action_intents rows were inserted by a two-step convention
    # (consume_grant_in_transaction() UPDATE + a separate INSERT) rather than
    # the AFTER INSERT trigger v6 introduces. Since that trigger only fires
    # on *new* inserts, a legacy row left pointing at a grant that was never
    # actually marked consumed would never get backfilled -- it would sail
    # through migration 6 and then be structurally unfixable, since every
    # remaining write path derives consumption from an insert that already
    # happened.
    unconsumed = conn.execute(
        """
        SELECT ai.grant_id FROM action_intents ai
        JOIN human_owner_grants g ON g.grant_id = ai.grant_id
        WHERE g.consumed_at IS NULL
        LIMIT 1
        """
    ).fetchone()
    if unconsumed is not None:
        raise LegacyIntegrityViolation(
            f"action_intents references human_owner_grants {unconsumed['grant_id']!r} which is not marked "
            "consumed -- refusing to apply migration 6 (grant-intent atomicity) over this legacy "
            "unconsumed-grant-with-intent state"
        )
    # A legacy pair could also have been consumed by the old two-step convention
    # with a *different* moment recorded for the UPDATE than the paired INSERT's
    # own created_at -- something the v6 trigger, which derives consumed_at from
    # the inserting row's created_at in the same statement, could never produce.
    # Migrating such a pair forward would lock in a legacy row lying about the
    # very derivation v6 is meant to guarantee structurally.
    mismatched = conn.execute(
        """
        SELECT ai.grant_id FROM action_intents ai
        JOIN human_owner_grants g
          ON g.grant_id = ai.grant_id AND g.consumed_by_idempotency_key = ai.idempotency_key
        WHERE g.consumed_at IS NOT NULL AND g.consumed_at IS NOT ai.created_at
        LIMIT 1
        """
    ).fetchone()
    if mismatched is not None:
        raise LegacyIntegrityViolation(
            f"human_owner_grants {mismatched['grant_id']!r} consumed_at does not match the created_at of "
            "the action_intents row that consumed it -- refusing to apply migration 6 (grant-intent "
            "atomicity) over this legacy mismatched-consumption-timestamp state"
        )


MIGRATION_PREFLIGHTS[6] = _v6_legacy_integrity_guard


def _v7_legacy_coordinate_integrity_guard(conn: sqlite3.Connection) -> None:
    """Before v7, ``action_intents_requires_fresh_grant`` (v6) only checked
    that *some* unconsumed ``human_owner_grants`` row existed for
    ``grant_id`` -- it never checked that the grant's own
    scope/subject/action_type/payload actually matched the inserted intent.
    So a deployment upgrading from v6 could already hold an intent/grant pair
    whose coordinates never matched -- v7's rewritten trigger only guards
    *future* inserts, so any such legacy pair would sail through migration 7
    and remain reconcilable/dispatchable under a schema that now claims this
    binding is guaranteed. Refuse the upgrade instead.
    """
    mismatched = conn.execute(
        """
        SELECT ai.idempotency_key, ai.grant_id FROM action_intents ai
        JOIN human_owner_grants g ON g.grant_id = ai.grant_id
        WHERE g.action_type IS NOT ai.action_type
           OR g.scope IS NOT ai.target_scope
           OR g.subject IS NOT ai.subject
           OR g.payload_digest IS NOT action_intents_payload_digest(ai.payload)
        LIMIT 1
        """
    ).fetchone()
    if mismatched is not None:
        raise LegacyIntegrityViolation(
            f"action_intents {mismatched['idempotency_key']!r} references human_owner_grants "
            f"{mismatched['grant_id']!r} whose scope/subject/action_type/payload_digest do not exactly "
            "match this intent's target_scope/subject/action_type/payload -- refusing to apply migration 7 "
            "(action_intents-grant coordinate binding) over this legacy mismatched-coordinate state"
        )


MIGRATION_PREFLIGHTS[7] = _v7_legacy_coordinate_integrity_guard


def _action_intents_payload_digest(payload_json: str) -> str:
    """SQL-callable mirror of ``human_owner.canonical_payload_digest``, so
    v7's ``action_intents_requires_fresh_grant`` trigger can compare a
    payload's digest against a ``human_owner_grants`` row from inside SQLite
    itself, without trusting application code to be the only place that
    check runs. Deliberately re-implemented here (same canonicalization:
    ``json.dumps(..., sort_keys=True, separators=(",", ":"))`` + sha256 hex)
    rather than imported from human_owner.py — that import would form a
    schema.py -> human_owner.py -> db.py -> schema.py cycle, since db.py
    already imports ``run_migrations`` from this module at load time.
    """
    parsed = json.loads(payload_json)
    canonical = json.dumps(parsed, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def run_migrations(conn: sqlite3.Connection) -> None:
    """Apply every migration in ``MIGRATIONS`` not yet recorded as applied.

    Safe to call on every ``connect()`` — the migrations table plus the
    per-version writer transaction (BEGIN IMMEDIATE, recheck, apply, record)
    make this idempotent and race-safe whether it's the first boot or the
    thousandth, or two connections booting at once.
    """
    # SQL functions are per-connection, not part of the persisted schema, so
    # this must run unconditionally on every call (not gated behind "is
    # version 7 already applied?") -- otherwise a connection opened after v7
    # was already recorded would hit action_intents_requires_fresh_grant's
    # call to this function with nothing registered.
    conn.create_function("action_intents_payload_digest", 1, _action_intents_payload_digest)

    # Deferred import: db.py imports run_migrations from this module at
    # module load time, so importing db.py back at *this* module's load time
    # would be circular. By the time run_migrations() actually runs (inside
    # connect(), after both modules have finished loading), the import below
    # is just a normal cache hit.
    from orchestrator.db import retry_on_locked, transaction

    def _ensure_migrations_table() -> None:
        with transaction(conn) as c:
            c.execute(
                """
                CREATE TABLE IF NOT EXISTS schema_migrations (
                    version     INTEGER PRIMARY KEY,
                    applied_at  TEXT NOT NULL
                )
                """
            )

    retry_on_locked(_ensure_migrations_table)

    for version, statements in MIGRATIONS:

        def _apply(version=version, statements=statements) -> None:
            with transaction(conn) as c:
                # Rechecked *after* BEGIN IMMEDIATE has the write lock, so a
                # concurrent connection that already applied and recorded
                # this version between our pre-lock read (none here — we
                # don't do one) and now is caught here instead of racing us
                # into a duplicate INSERT / IntegrityError on the PK.
                already_applied = c.execute(
                    "SELECT 1 FROM schema_migrations WHERE version = ?", (version,)
                ).fetchone()
                if already_applied is not None:
                    return
                preflight = MIGRATION_PREFLIGHTS.get(version)
                if preflight is not None:
                    preflight(c)
                for statement in statements:
                    c.execute(statement)
                c.execute(
                    "INSERT INTO schema_migrations (version, applied_at) VALUES (?, datetime('now'))",
                    (version,),
                )

        retry_on_locked(_apply)
