"""Read-only tail adapter over the native Kanban ``task_events`` SQLite table.

Opens the source DB via SQLite's read-only URI mode (``mode=ro``) — the
sqlite3 driver itself refuses any write against that connection, so "never
writes to Kanban" is enforced by the open mode, not just by this module never
issuing a write statement. ``PRAGMA query_only = ON`` is set on top of that as
defense in depth. The source URI is built with ``Path.as_uri()``, which
percent-encodes filename characters such as ``?``, ``#``, and ``%`` before
the fixed ``mode=ro`` query parameter is appended.

Reads only the exact verified columns (``id, task_id, run_id, kind, payload,
created_at``) in ordered, bounded batches, and never touches any other table
or column in the source DB. Before every read, the source table's shape is
verified against the exact contract this module relies on (required columns,
``id`` as an INTEGER PRIMARY KEY, the NOT NULL TEXT columns actually NOT NULL
TEXT) and every fetched row is validated against that same contract — a
mismatch fails closed rather than being silently coerced with ``str()``.

The read cursor is durable state (``source_cursors``) in the *control-plane*
DB, not the source DB — it advances only after the whole batch's observations
have committed, in the same transaction, so a crash mid-batch always resumes
by re-reading (and safely re-deduping) rather than skipping unobserved rows.
The cursor is also bound to the source row it currently points at: before
reading anything new, that row's content is re-verified against the evidence
already recorded for it, so a source restored from an earlier backup or
otherwise replaced under the same row id is refused rather than silently
tailed as if nothing happened.

SQLite cannot consume a caller's file descriptor as a stable database handle:
``/proc/self/fd/N`` is still a filename SQLite reopens, and it also gives
SQLite the wrong sibling name for a live ``-wal`` file.  This adapter therefore
does not claim descriptor-stable source identity.  It supports only a
published source path with no symlink components and a full ownership/
permission trust chain from the filesystem root to the source and its SQLite
sidecars: every component is owned by the effective UID or root, and is not
group/world writable except for a trusted sticky system ancestor such as
``/tmp``.  The source's direct parent itself must not be writable by
group/other, because an attacker could otherwise inject an absent ``-wal`` or
``-journal`` sidecar.  Under that source contract an untrusted principal
cannot exchange the pathname or inject a symlink; an unsafe path is rejected
before SQLite opens it.  SQLite then opens the actual checked path normally,
so its standard read-only WAL behavior is preserved.  A trusted publisher
racing a publication with an open is detected when the path identity changes
across the open and is refused, but a hostile trusted publisher (or a same-UID
compromise) remains outside this contract.
"""

from __future__ import annotations

import hashlib
import hmac
import os
from os import stat_result
import sqlite3
import stat
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

from orchestrator.control_plane.db import transaction
from orchestrator.control_plane.ledger import ObservationLedger, ObservationOutcome, evidence_hash

DEFAULT_BATCH_SIZE = 500
MIN_BATCH_SIZE = 1
MAX_BATCH_SIZE = 10_000
SOURCE_COLUMNS = ("id", "task_id", "run_id", "kind", "payload", "created_at")
_REQUIRED_TEXT_NOT_NULL_COLUMNS = ("task_id", "kind", "payload", "created_at")

# Bumping this changes every future consumed-prefix digest output, the same
# way ledger.EVIDENCE_HASH_VERSION does for evidence_hash() — a future change
# to the folding scheme is then itself detectable rather than silently
# reinterpreting old digests under a new scheme.
_CONSUMED_DIGEST_VERSION = 1


def _utcnow() -> str:
    return datetime.now(timezone.utc).isoformat()


def _seed_consumed_digest(source: str) -> str:
    """The consumed-prefix digest of an empty (nothing yet consumed) history."""
    return hashlib.sha256(f"v{_CONSUMED_DIGEST_VERSION}:seed:{source}".encode("utf-8")).hexdigest()


def _fold_consumed_digest(prior_digest: str, row_evidence_hash: str) -> str:
    """Fold one more consumed row's evidence hash onto the running prefix digest.

    A simple hash chain: each row's contribution depends on every row folded
    in before it (in id order), so altering, deleting, or reordering any
    single previously-consumed row changes every digest folded after it —
    there is no way to tamper with row N without the chain diverging from
    durable evidence for every cursor position past N.
    """
    return hashlib.sha256(f"{prior_digest}:{row_evidence_hash}".encode("utf-8")).hexdigest()


class RelativeSourceDbPathError(ValueError):
    """Raised when the source Kanban DB path is not absolute."""


class InvalidBatchSizeError(ValueError):
    """Raised when a requested batch size is not a positive, capped integer."""


class SourceContractError(ValueError):
    """Raised when the source ``task_events`` table doesn't match the required read contract."""


class SourcePathSecurityError(ValueError):
    """Raised when a source path does not meet the immutable-lookup contract.

    The contract protects against a different, untrusted filesystem principal
    exchanging a source DB or substituting a symlink between validation and a
    SQLite open.  It deliberately does not pretend to protect against the
    trusted directory owner/publisher or a process already running as that
    same principal.
    """


class SourceIdentityMismatchError(RuntimeError):
    """Raised when the source DB no longer matches the identity the durable cursor is bound to.

    Signals a possible restore-from-backup, truncation, or wholesale
    replacement of the source instance: the row the cursor currently points
    at either no longer exists or no longer matches what was previously
    observed there. The tail refuses to proceed rather than silently
    resuming (and possibly skipping) against a different source generation.
    """


def _validate_batch_size(batch_size: int) -> int:
    if isinstance(batch_size, bool) or not isinstance(batch_size, int):
        raise InvalidBatchSizeError(f"batch_size must be an int, got {batch_size!r}")
    if not (MIN_BATCH_SIZE <= batch_size <= MAX_BATCH_SIZE):
        raise InvalidBatchSizeError(
            f"batch_size must be between {MIN_BATCH_SIZE} and {MAX_BATCH_SIZE}, got {batch_size!r}"
        )
    return batch_size


def _source_path_stat(path: Path) -> tuple[str, stat_result]:
    """Validate the source-location trust boundary without following symlinks.

    Every component is ``lstat``ed, so a final-file symlink and a symlinked
    ancestor both fail before SQLite sees a filename.  SQLite's companion
    ``-wal``, ``-shm``, and rollback ``-journal`` names are checked with the
    same final-file rules: a normal read-only URI must never be allowed to
    follow an unsafe sidecar after the main DB has passed validation.  A
    directory writable by group/other lets an untrusted principal rename a
    child or install a symlink; a sticky root/effective-UID-owned system
    ancestor is accepted only when the child it leads to is itself trusted,
    because POSIX then prevents another user from replacing that existing
    child (``/tmp`` is the conventional example).  Every directory and file
    in the lookup chain must be owned by the effective UID or root: an
    attacker-owned 0755 directory can still replace a root/effective-UID-owned
    source inside it.  The direct source parent cannot be group/world
    writable, even if sticky, because SQLite may open a currently absent
    ``-wal``, ``-shm``, or rollback ``-journal`` sidecar there.  The source
    and all present sidecars cannot be group/world writable.

    This deliberately describes a *publication boundary*, not an impossible
    claim that a process can defend a file against its own UID or a trusted
    administrator.  See the module docstring for the supported contract.
    """
    if ".." in path.parts:
        raise SourcePathSecurityError(
            f"source_db_path must not contain '..' components: {str(path)!r}"
        )

    trusted_uids = {0, os.geteuid()}

    def _require_trusted_owner(component: Path, component_stat: stat_result) -> None:
        if component_stat.st_uid not in trusted_uids:
            raise SourcePathSecurityError(
                f"source path component {str(component)!r} is owned by uid={component_stat.st_uid}, "
                f"not the effective uid={os.geteuid()} or root; its owner could exchange a source "
                "pathname component"
            )

    current = Path(path.anchor)
    current_stat = current.lstat()
    if not stat.S_ISDIR(current_stat.st_mode):
        raise SourcePathSecurityError(f"source path root is not a directory: {str(current)!r}")
    _require_trusted_owner(current, current_stat)

    components = path.parts[1:]
    if not components:
        raise SourcePathSecurityError(f"source_db_path must name a regular file, got root {str(path)!r}")

    for position, part in enumerate(components):
        # The containing directory controls whether this component can be
        # replaced during lookup.  A sticky group/world-writable ancestor
        # protects the already-existing next child only if both it and that
        # child are owned by root/the effective UID.  The source's immediate
        # parent additionally controls SQLite sidecar names, so it must not
        # be group/world writable at all: an attacker could create an absent
        # -wal/-shm/-journal between validation and SQLite's open.
        mode = stat.S_IMODE(current_stat.st_mode)
        group_or_world_writable = bool(mode & (stat.S_IWGRP | stat.S_IWOTH))
        is_source_parent = position == len(components) - 1
        if group_or_world_writable and (not (mode & stat.S_ISVTX) or is_source_parent):
            raise SourcePathSecurityError(
                f"source path parent {str(current)!r} is group/world writable"
                + (" (including the source parent, which can receive SQLite sidecars); " if is_source_parent else " without the sticky bit; ")
                + "an untrusted principal could exchange or inject a source component"
            )
        current = current / part
        try:
            current_stat = current.lstat()
        except FileNotFoundError as exc:
            raise SourcePathSecurityError(f"source path component does not exist: {str(current)!r}") from exc
        if stat.S_ISLNK(current_stat.st_mode):
            raise SourcePathSecurityError(
                f"source path contains a symbolic link at {str(current)!r}; refusing mutable lookup indirection"
            )
        _require_trusted_owner(current, current_stat)
        if position < len(components) - 1 and not stat.S_ISDIR(current_stat.st_mode):
            raise SourcePathSecurityError(f"source path component is not a directory: {str(current)!r}")

    if not stat.S_ISREG(current_stat.st_mode):
        raise SourcePathSecurityError(f"source_db_path must be a regular file: {str(path)!r}")
    if stat.S_IMODE(current_stat.st_mode) & (stat.S_IWGRP | stat.S_IWOTH):
        raise SourcePathSecurityError(
            f"source DB {str(path)!r} is group/world writable; an untrusted principal could rewrite it"
        )
    for suffix in ("-wal", "-shm", "-journal"):
        sidecar = Path(f"{path}{suffix}")
        try:
            sidecar_stat = sidecar.lstat()
        except FileNotFoundError:
            continue
        if stat.S_ISLNK(sidecar_stat.st_mode):
            raise SourcePathSecurityError(
                f"SQLite source sidecar {str(sidecar)!r} is a symbolic link; refusing mutable lookup indirection"
            )
        if not stat.S_ISREG(sidecar_stat.st_mode):
            raise SourcePathSecurityError(
                f"SQLite source sidecar {str(sidecar)!r} must be a regular file"
            )
        _require_trusted_owner(sidecar, sidecar_stat)
        if stat.S_IMODE(sidecar_stat.st_mode) & (stat.S_IWGRP | stat.S_IWOTH):
            raise SourcePathSecurityError(
                f"SQLite source sidecar {str(sidecar)!r} is group/world writable; an untrusted principal "
                "could rewrite source state"
            )
    # No component was symlinked, so lexical absolute spelling is also the
    # canonical path we record and later compare as identity evidence.
    return str(path), current_stat


def _verify_source_contract(source_conn: sqlite3.Connection) -> None:
    """Fail closed if ``task_events`` doesn't match the exact contract this module relies on."""
    columns = source_conn.execute("PRAGMA table_info(task_events)").fetchall()
    if not columns:
        raise SourceContractError("source table 'task_events' does not exist or has no columns")

    by_name = {row[1]: row for row in columns}  # PRAGMA table_info: cid, name, type, notnull, dflt_value, pk
    missing = [name for name in SOURCE_COLUMNS if name not in by_name]
    if missing:
        raise SourceContractError(f"source table 'task_events' is missing required columns: {missing}")

    # PRAGMA table_info's pk field is the column's 1-based position within
    # the primary key (0 for non-key columns), so more than one non-zero
    # entry means a composite primary key: 'id' would then only be unique
    # in combination with the rest of the key, not on its own -- the global
    # per-row uniqueness this module's cursor/identity logic assumes.
    pk_columns = [row for row in columns if row[5] != 0]
    if len(pk_columns) != 1:
        raise SourceContractError(
            "source table 'task_events' must have exactly one PRIMARY KEY column (the sole rowid alias "
            f"for 'id'); got a composite primary key over: {[row[1] for row in pk_columns]}"
        )

    id_col = by_name["id"]
    id_type, id_pk = id_col[2], id_col[5]
    if (id_type or "").upper() != "INTEGER" or id_pk != 1:
        raise SourceContractError(
            f"source table 'task_events'.id must be declared INTEGER PRIMARY KEY, got type={id_type!r} pk={id_pk!r}"
        )

    # A WITHOUT ROWID table has no rowid at all, so even a lone
    # "id INTEGER PRIMARY KEY" column loses the ordinary rowid-table
    # semantics (the dedicated integer-key rowid alias) this module's
    # cursor/identity logic is built against -- SELECT rowid fails outright
    # against such a table, which is the reliable way to detect it (SQLite
    # itself, not a parse of the stored CREATE TABLE text).
    try:
        source_conn.execute("SELECT rowid FROM task_events LIMIT 0")
    except sqlite3.OperationalError as exc:
        raise SourceContractError(
            "source table 'task_events' has no rowid (declared WITHOUT ROWID); 'id' must be an "
            f"ordinary rowid table's INTEGER PRIMARY KEY rowid alias: {exc}"
        ) from exc

    # ``INTEGER PRIMARY KEY DESC`` looks identical in table_info(), but it
    # is *not* a rowid alias: SQLite builds a physical ``origin='pk'``
    # autoindex for it.  A genuine INTEGER PRIMARY KEY rowid alias has no
    # separate primary-key index at all; the table b-tree itself is the
    # key.  This structural fact, combined with the successful rowid probe
    # above, proves the alias without depending on EXPLAIN QUERY PLAN's
    # human-readable diagnostic wording (which is not a stable API).
    pk_indexes = [
        row
        for row in source_conn.execute("PRAGMA index_list(task_events)").fetchall()
        if len(row) > 3 and row[3] == "pk"
    ]
    if pk_indexes:
        raise SourceContractError(
            "source table 'task_events'.id is declared INTEGER PRIMARY KEY but is not a genuine rowid "
            "alias: SQLite reports a separate primary-key index, as it does for an 'INTEGER PRIMARY KEY "
            "DESC' declaration; refusing a source whose id column is not backed by the single global rowid "
            f"this module relies on; primary-key indexes: {pk_indexes!r}"
        )

    for name in _REQUIRED_TEXT_NOT_NULL_COLUMNS:
        col = by_name[name]
        col_type, col_notnull = col[2], col[3]
        if (col_type or "").upper() != "TEXT" or col_notnull != 1:
            raise SourceContractError(
                f"source table 'task_events'.{name} must be declared TEXT NOT NULL, "
                f"got type={col_type!r} notnull={col_notnull!r}"
            )

    run_id_col = by_name["run_id"]
    run_id_type, run_id_notnull = run_id_col[2], run_id_col[3]
    if (run_id_type or "").upper() != "TEXT":
        raise SourceContractError(
            f"source table 'task_events'.run_id must be declared TEXT, got type={run_id_type!r}"
        )
    if run_id_notnull != 0:
        raise SourceContractError(
            f"source table 'task_events'.run_id must be nullable, got notnull={run_id_notnull!r}"
        )


def _validate_row(row: sqlite3.Row) -> tuple[int, str, str | None, str, str, str]:
    """Validate one fetched row against the source contract; never silently coerce."""
    source_id, task_id, run_id, kind, payload, created_at = row
    if not isinstance(source_id, int) or isinstance(source_id, bool):
        raise SourceContractError(f"task_events.id must be an integer, got {source_id!r}")
    for name, value in (("task_id", task_id), ("kind", kind), ("payload", payload), ("created_at", created_at)):
        if not isinstance(value, str):
            raise SourceContractError(f"task_events.{name} must be non-null text, got {value!r}")
    if run_id is not None and not isinstance(run_id, str):
        raise SourceContractError(f"task_events.run_id must be text or NULL, got {run_id!r}")
    return source_id, task_id, run_id, kind, payload, created_at


@dataclass(frozen=True)
class _OpenedSource:
    """The readonly source connection plus checked path identity evidence.

    This is not represented as an SQLite descriptor guarantee.  The source
    path contract removes untrusted pathname replacement from the supported
    environment, while identity sampled before and after SQLite opens the
    ordinary source URI detects a trusted publication that raced this open.
    """

    conn: sqlite3.Connection
    canonical_path: str
    device: int
    inode: int


@dataclass(frozen=True)
class TailBatchResult:
    rows_read: int
    appended: int
    duplicates: int
    quarantined: int
    cursor_before: int
    cursor_after: int


class KanbanTailAdapter:
    """Tails one source Kanban SQLite DB into the control-plane observation ledger."""

    def __init__(
        self,
        source_db_path: Path | str,
        control_conn: sqlite3.Connection,
        *,
        source: str = "kanban",
        batch_size: int = DEFAULT_BATCH_SIZE,
    ):
        path = Path(source_db_path)
        if not path.is_absolute():
            raise RelativeSourceDbPathError(
                f"source_db_path must be an absolute path, got {source_db_path!r}; refusing to resolve "
                "a relative path against the process's current working directory for a durable source"
            )
        self._source_db_path = path
        self._control_conn = control_conn
        self._source = source
        self._batch_size = _validate_batch_size(batch_size)

    def _open_source_readonly(self) -> _OpenedSource:
        """Open the actual checked source URI in SQLite read-only mode.

        ``/proc/self/fd/N`` is intentionally not used: SQLite reopens it as
        a path and derives sidecar names from that synthetic locator, so it
        cannot prove descriptor-stable identity or correctly consume a live
        WAL.  ``_source_path_stat`` first enforces the documented immutable
        lookup boundary; a second check after SQLite opens catches a source
        publisher changing the path during this narrow interval.
        """
        canonical_path, before = _source_path_stat(self._source_db_path)
        source_uri = self._source_db_path.as_uri() + "?mode=ro"
        try:
            conn = sqlite3.connect(source_uri, uri=True)
        except Exception:
            raise
        try:
            conn.execute("PRAGMA query_only = ON")
            _canonical_path_after, after = _source_path_stat(self._source_db_path)
            if (before.st_dev, before.st_ino) != (after.st_dev, after.st_ino):
                raise SourcePathSecurityError(
                    f"source DB {canonical_path!r} changed while SQLite was opening it; refusing an "
                    "unattestable source publication race"
                )
        except Exception:
            conn.close()
            raise
        return _OpenedSource(
            conn=conn,
            canonical_path=canonical_path,
            device=before.st_dev,
            inode=before.st_ino,
        )

    def current_cursor(self) -> int:
        """The durable cursor position: the highest source ``id`` already recorded."""
        return self._get_cursor()

    def _get_cursor(self) -> int:
        row = self._control_conn.execute(
            "SELECT cursor_value FROM source_cursors WHERE source = ?", (self._source,)
        ).fetchone()
        return int(row["cursor_value"]) if row is not None else 0

    def _get_consumed_digest(self) -> str:
        """The durable consumed-prefix digest for the current cursor position.

        A missing row (no tailing has happened yet for this source) is
        equivalent to the seed digest of an empty history — consistent with
        ``_get_cursor()`` treating a missing row as cursor position 0.
        """
        row = self._control_conn.execute(
            "SELECT consumed_digest FROM source_cursors WHERE source = ?", (self._source,)
        ).fetchone()
        return row["consumed_digest"] if row is not None else _seed_consumed_digest(self._source)

    def _set_cursor(self, conn: sqlite3.Connection, value: int, *, now: str, consumed_digest: str) -> tuple[int, str]:
        """Upsert the cursor + consumed-prefix digest together, never letting the
        cursor move backward, and return what's durable.

        Runs inside the same writer transaction as the batch it covers
        (``BEGIN IMMEDIATE`` already serializes concurrent writers), and the
        ``CASE`` compares against ``source_cursors.cursor_value`` as it
        stands *at conflict-resolution time* in that transaction — so even if
        two connections race to tail overlapping ranges, whichever commits
        second can never regress the cursor (or the digest that corresponds
        to it) set by the one that committed first. ``updated_at`` only moves
        alongside an actual forward cursor_value change — a losing writer's
        commit must not make the bookmark's timestamp look newer than the
        progress it actually represents. Returns the (cursor, digest) pair
        now durable in the row (which may reflect a concurrent writer's
        advance past ``value``), so a caller reports what's actually
        persisted rather than its own possibly-stale local computation.
        """
        conn.execute(
            """
            INSERT INTO source_cursors (source, cursor_value, consumed_digest, updated_at)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(source) DO UPDATE SET
                cursor_value = CASE
                    WHEN CAST(excluded.cursor_value AS INTEGER) > CAST(source_cursors.cursor_value AS INTEGER)
                    THEN excluded.cursor_value
                    ELSE source_cursors.cursor_value
                END,
                consumed_digest = CASE
                    WHEN CAST(excluded.cursor_value AS INTEGER) > CAST(source_cursors.cursor_value AS INTEGER)
                    THEN excluded.consumed_digest
                    ELSE source_cursors.consumed_digest
                END,
                updated_at = CASE
                    WHEN CAST(excluded.cursor_value AS INTEGER) > CAST(source_cursors.cursor_value AS INTEGER)
                    THEN excluded.updated_at
                    ELSE source_cursors.updated_at
                END
            """,
            (self._source, str(value), consumed_digest, now),
        )
        row = conn.execute(
            "SELECT cursor_value, consumed_digest FROM source_cursors WHERE source = ?", (self._source,)
        ).fetchone()
        return int(row["cursor_value"]), row["consumed_digest"]

    def _recompute_consumed_prefix_digest(self, source_conn: sqlite3.Connection, upto_id: int) -> str:
        """Validate the complete source and recompute its consumed prefix.

        Reads in ``self._batch_size``-bounded chunks via ``fetchmany`` rather
        than ``fetchall`` so memory stays bounded regardless of the source
        table's size.  Every row is validated before the caller can write to
        the control plane -- including rows at/before the durable cursor,
        id<=0 rows that a forward tail never reaches, and future rows beyond
        this cycle's bounded observation batch.  Only rows through
        ``upto_id`` are folded into the consumed digest, but no row is
        skipped for contract validation.

        Re-deriving that prefix from the source itself (rather than trusting
        an incremental cache) also detects that some already-consumed row
        earlier than ``upto_id`` was altered or removed in place, even though
        the file's physical identity and the cursor-anchor row are unchanged.
        """
        digest = _seed_consumed_digest(self._source)
        cursor = source_conn.execute(
            f"SELECT {', '.join(SOURCE_COLUMNS)} FROM task_events ORDER BY id ASC",
        )
        while True:
            chunk = cursor.fetchmany(self._batch_size)
            if not chunk:
                break
            for row in chunk:
                source_id, task_id, run_id, kind, payload, created_at = _validate_row(row)
                if source_id <= upto_id:
                    row_hash = evidence_hash(
                        source=self._source,
                        source_id=str(source_id),
                        entity_id=str(task_id),
                        run_id=run_id,
                        kind=kind,
                        payload=payload,
                        occurred_at=created_at,
                    )
                    digest = _fold_consumed_digest(digest, row_hash)
        return digest

    def _verify_consumed_prefix_digest(self, source_conn: sqlite3.Connection, upto_id: int) -> str:
        """Fail closed if the recomputed consumed-prefix digest no longer matches
        durable evidence; returns the verified digest for the caller to fold
        this cycle's new rows onto."""
        expected = self._get_consumed_digest()
        recomputed = self._recompute_consumed_prefix_digest(source_conn, upto_id)
        if not hmac.compare_digest(recomputed, expected):
            raise SourceIdentityMismatchError(
                f"source={self._source!r} consumed row history up to id={upto_id} no longer matches "
                "durable evidence (recomputed prefix digest mismatch); refusing to tail past a possible "
                "in-place tamper of an already-consumed row"
            )
        return recomputed

    def _verify_source_identity(self, source_conn: sqlite3.Connection, cursor_before: int) -> None:
        """Fail closed if the row the durable cursor points at no longer matches recorded evidence."""
        previous = ObservationLedger(self._control_conn).get_by_source_id(self._source, str(cursor_before))
        if previous is None:
            raise SourceIdentityMismatchError(
                f"cursor for source={self._source!r} points at source_id={cursor_before} but no matching "
                "observation is recorded in the control-plane ledger; refusing to tail an inconsistent cursor"
            )

        row = source_conn.execute(
            f"SELECT {', '.join(SOURCE_COLUMNS)} FROM task_events WHERE id = ?",
            (cursor_before,),
        ).fetchone()
        if row is None:
            raise SourceIdentityMismatchError(
                f"source={self._source!r} row id={cursor_before} (the current cursor position) is missing "
                "from the source DB; refusing to tail past a possible restore/replacement of the source"
            )

        source_id, task_id, run_id, kind, payload, created_at = _validate_row(row)
        current_hash = evidence_hash(
            source=self._source,
            source_id=str(source_id),
            entity_id=str(task_id),
            run_id=str(run_id) if run_id is not None else None,
            kind=str(kind),
            payload=str(payload),
            occurred_at=str(created_at),
        )
        if current_hash != previous.evidence_hash:
            raise SourceIdentityMismatchError(
                f"source={self._source!r} row id={cursor_before} content no longer matches what was "
                "previously observed there; refusing to tail past a possible restore/replacement of the source"
            )

    def _read_existing_source_identity(self) -> sqlite3.Row | None:
        return self._control_conn.execute(
            "SELECT canonical_path, device, inode FROM source_identity WHERE source = ?", (self._source,)
        ).fetchone()

    def _assert_identity_row_matches(
        self, row: sqlite3.Row, canonical_path: str, device: int, inode: int
    ) -> None:
        # Checked independently of (device, inode): two different paths can
        # share the same inode (a hard link), and a physical-identity check
        # alone would silently accept the observer being pointed at a
        # different path than the one durable evidence was recorded
        # against. Any path/hard-link change is treated as a mismatch
        # requiring fail-closed refusal, not a silent accept.
        if row["canonical_path"] != canonical_path:
            raise SourceIdentityMismatchError(
                f"source={self._source!r} canonical path changed from {row['canonical_path']!r} "
                f"to {canonical_path!r} (possible hard-link swap or path reconfiguration); refusing "
                "to tail a source whose identity path no longer matches durable evidence"
            )
        if row["device"] != device or row["inode"] != inode:
            raise SourceIdentityMismatchError(
                f"source={self._source!r} physical file identity changed (device/inode no longer "
                "match the previously recorded source file); refusing to tail a replaced source database"
            )

    def _verify_or_record_source_instance_identity(self, opened: _OpenedSource) -> None:
        """Fail closed if this source's physical file identity changed since it was first recorded.

        A source swapped for a different file that happens to replicate the
        exact row content the durable cursor currently anchors on (see
        ``_verify_source_identity``) would defeat a content-only check —
        this catches that by binding the source to its physical (device,
        inode) identity instead, independent of any single row's content.
        Uses the identity captured when this cycle's connection was opened
        (``opened``), not a fresh restat of the path, so this never records
        or verifies against a source other than the one actually read.

        Recorded once, on first use; a plain ``INSERT ... WHERE NOT EXISTS``
        rather than an upsert, since ``source_identity`` is append-only (see
        ``source_identity_no_replace_by_source`` in schema.py) — an upsert's
        ON CONFLICT clause would never even get a chance to run, because a
        BEFORE INSERT guard trigger fires (and would abort) before SQLite
        decides how to resolve the conflict.

        The pre-check read below (``_read_existing_source_identity``) runs
        outside any write transaction, so a concurrent connection can win
        the INSERT for this same source between that read and this
        transaction acquiring its write lock -- in which case this
        connection's own conditional INSERT is a silent no-op (the row
        this connection would have written already exists). Rereading the
        durable row *inside this same transaction*, unconditionally,
        catches that: whichever connection's INSERT actually won, every
        connection verifies the row that ended up durable against what it
        itself observed, and a mismatch (e.g. the winner recorded a
        different canonical path — a hard-link alias) is rejected rather
        than silently continued past.
        """
        canonical_path, device, inode = opened.canonical_path, opened.device, opened.inode
        existing = self._read_existing_source_identity()
        if existing is not None:
            self._assert_identity_row_matches(existing, canonical_path, device, inode)
            return
        with transaction(self._control_conn) as conn:
            conn.execute(
                """
                INSERT INTO source_identity (source, canonical_path, device, inode, recorded_at)
                SELECT ?, ?, ?, ?, ?
                WHERE NOT EXISTS (SELECT 1 FROM source_identity WHERE source = ?)
                """,
                (self._source, canonical_path, device, inode, _utcnow(), self._source),
            )
            durable = conn.execute(
                "SELECT canonical_path, device, inode FROM source_identity WHERE source = ?", (self._source,)
            ).fetchone()
            self._assert_identity_row_matches(durable, canonical_path, device, inode)

    def _fetch_and_validate_batch(
        self, source_conn: sqlite3.Connection, cursor_before: int
    ) -> list[tuple[int, str, str | None, str, str, str]]:
        """Preflight: fetch this cycle's bounded batch and validate every row
        against the row-level contract *before* the caller ever performs a
        control-plane write for it.

        The ``LIMIT`` in the query below already bounds the result set to at
        most ``self._batch_size`` rows, so this reads in ``fetchmany``
        chunks of that same size on top of it rather than a single
        ``fetchall``. The I/O tradeoff: for a batch already this small, that
        chunking buys negligible extra memory headroom over one bulk fetch
        (one round trip either way, since ``LIMIT`` caps it) -- its real
        value is consistency with ``_recompute_consumed_prefix_digest``,
        which streams a potentially much larger (unbounded-by-LIMIT) range
        of rows the same way, so both preflight paths share one bounded-read
        contract and neither can silently regress to holding an unbounded
        result set in memory if either range grows.

        Called after the complete streaming preflight above and strictly
        before ``_verify_or_record_source_instance_identity``.  The second
        validation is intentional defense in depth: all rows were checked
        first, and the bounded rows about to be written are checked again
        from the same source snapshot.
        """
        cursor = source_conn.execute(
            f"""
            SELECT {', '.join(SOURCE_COLUMNS)}
            FROM task_events
            WHERE id > ?
            ORDER BY id ASC
            LIMIT ?
            """,
            (cursor_before, self._batch_size),
        )
        rows: list[tuple[int, str, str | None, str, str, str]] = []
        while True:
            chunk = cursor.fetchmany(self._batch_size)
            if not chunk:
                break
            rows.extend(_validate_row(row) for row in chunk)
        return rows

    def tail_once(
        self,
        *,
        policy_version: str,
        actor: str,
        correlation_id: str,
        now: str | None = None,
    ) -> TailBatchResult:
        """Read one bounded batch past the durable cursor and record it as evidence.

        A no-op batch (no new rows) still returns a result but performs no
        write at all — no empty transaction, no cursor churn.
        """
        moment = now or _utcnow()
        cursor_before = self._get_cursor()

        opened = self._open_source_readonly()
        source_conn = opened.conn
        try:
            # Keep every schema, full-source preflight, bounded batch, and
            # cursor-anchor read in one readonly SQLite snapshot.  Otherwise
            # an external writer could alter a row after the full preflight
            # but before it is fetched into the batch that gets recorded.
            source_conn.execute("BEGIN")
            # Source-contract validation happens before source identity (or
            # any other control-plane write, including the append-only
            # source_identity row below): source_identity can never be
            # corrected once written, so a source that doesn't even match
            # the required read contract must never get an identity binding
            # recorded against it in the first place. This covers the
            # table's declared shape (columns/types/PK) only -- a
            # schema-valid table can still hold a row whose stored value
            # violates the row-level contract (e.g. a BLOB in a declared
            # TEXT NOT NULL column, which TEXT affinity does not coerce), so
            # the complete source is streamed and validated next, still
            # ahead of any write.
            _verify_source_contract(source_conn)

            # This single bounded-memory pass validates *every* task_events
            # row, not merely the consumed prefix or the next observation
            # batch.  It also returns the verified digest for the already-
            # consumed portion.  It always runs, even at cursor 0, before
            # the identity checks below that may do a one-time append-only
            # source_identity write.
            verified_prefix_digest = self._verify_consumed_prefix_digest(source_conn, cursor_before)
            rows = self._fetch_and_validate_batch(source_conn, cursor_before)

            # ``opened.device``/``opened.inode`` were sampled before and
            # after SQLite opened the ordinary checked path.  The source-path
            # contract excludes untrusted pathname replacement; a trusted
            # publisher racing the open is rejected by _open_source_readonly.
            # This is intentionally not represented as an FD-stability proof.
            self._verify_or_record_source_instance_identity(opened)

            if cursor_before > 0:
                self._verify_source_identity(source_conn, cursor_before)
        finally:
            source_conn.close()

        if not rows:
            # A genuinely empty batch performs no write of its own, but a
            # concurrent writer may have advanced the durable cursor between
            # this call's initial read (above) and now — reread it fresh
            # (a plain SELECT, not a write) so the reported cursor_after
            # reflects current durable truth rather than the value this
            # connection happened to capture before racing in.
            return TailBatchResult(
                rows_read=0,
                appended=0,
                duplicates=0,
                quarantined=0,
                cursor_before=cursor_before,
                cursor_after=self._get_cursor(),
            )

        appended = duplicates = quarantined = 0
        max_id = cursor_before
        running_digest = verified_prefix_digest
        with transaction(self._control_conn) as conn:
            ledger = ObservationLedger(conn)
            for source_id, task_id, run_id, kind, payload, created_at in rows:
                run_id_str = str(run_id) if run_id is not None else None
                result = ledger.record(
                    source=self._source,
                    source_id=str(source_id),
                    entity_id=str(task_id),
                    run_id=run_id_str,
                    kind=str(kind),
                    payload=str(payload),
                    occurred_at=str(created_at),
                    policy_version=policy_version,
                    actor=actor,
                    correlation_id=correlation_id,
                    now=moment,
                )
                if result.outcome is ObservationOutcome.APPENDED:
                    appended += 1
                elif result.outcome is ObservationOutcome.DUPLICATE:
                    duplicates += 1
                else:
                    quarantined += 1
                row_hash = evidence_hash(
                    source=self._source,
                    source_id=str(source_id),
                    entity_id=str(task_id),
                    run_id=run_id_str,
                    kind=str(kind),
                    payload=str(payload),
                    occurred_at=str(created_at),
                )
                running_digest = _fold_consumed_digest(running_digest, row_hash)
                max_id = max(max_id, int(source_id))

            durable_cursor, _durable_digest = self._set_cursor(
                conn, max_id, now=moment, consumed_digest=running_digest
            )

        return TailBatchResult(
            rows_read=len(rows),
            appended=appended,
            duplicates=duplicates,
            quarantined=quarantined,
            cursor_before=cursor_before,
            cursor_after=durable_cursor,
        )
