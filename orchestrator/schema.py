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

import sqlite3

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
]


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
                for statement in statements:
                    c.execute(statement)
                c.execute(
                    "INSERT INTO schema_migrations (version, applied_at) VALUES (?, datetime('now'))",
                    (version,),
                )

        retry_on_locked(_apply)
