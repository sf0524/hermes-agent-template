"""Connection + explicit transaction helpers for the control-plane state DB.

Same shape as ``orchestrator/db.py``: ``isolation_level=None`` puts the
connection in autocommit mode so ``transaction()`` below is the only source of
BEGIN/COMMIT/ROLLBACK. Every write in this package goes through
``transaction()``.
"""

from __future__ import annotations

import random
import sqlite3
import time
from contextlib import contextmanager
from pathlib import Path

from orchestrator.control_plane.schema import run_migrations
from orchestrator.control_plane.state import control_plane_db_path

_LOCK_RETRY_ATTEMPTS = 25
_LOCK_RETRY_BASE_DELAY = 0.02


def _is_lock_contention(exc: sqlite3.OperationalError) -> bool:
    msg = str(exc).lower()
    return "locked" in msg or "busy" in msg


def retry_on_locked(fn):
    """Run ``fn()``, retrying with jittered backoff while SQLite reports contention."""
    last_exc: sqlite3.OperationalError | None = None
    for attempt in range(_LOCK_RETRY_ATTEMPTS):
        try:
            return fn()
        except sqlite3.OperationalError as exc:
            if not _is_lock_contention(exc):
                raise
            last_exc = exc
            time.sleep(_LOCK_RETRY_BASE_DELAY * (2 ** min(attempt, 6)) + random.uniform(0, _LOCK_RETRY_BASE_DELAY))
    raise last_exc


class RelativePersistentDbPathError(ValueError):
    """Raised when ``connect()`` is given a relative path for durable state.

    A relative path would silently resolve against whatever the process's
    current working directory happens to be at call time — a data-loss trap
    for durable state, the same reasoning ``state.py`` already applies to
    ``$ORCH_STATE_DB``/``$HERMES_ROOT`` and ``kanban_tail.py`` applies to the
    source DB path. ``:memory:`` is rejected here too: it is not a relative
    filesystem path, but the public ``connect()`` entry point a real
    caller/CLI would use must never silently hand back a non-durable
    connection. Tests that genuinely want an ephemeral in-memory DB use
    ``_connect_in_memory_for_tests`` instead.
    """


def connect(db_path: Path | str | None = None) -> sqlite3.Connection:
    """Open (creating if needed) the control-plane state DB and migrate it.

    Defaults to ``control_plane_db_path()``; pass an explicit *absolute* path
    (tests use this with a tmp file) to point elsewhere. A relative path —
    and ``:memory:`` — are rejected; see ``RelativePersistentDbPathError``.
    """
    path = Path(db_path) if db_path is not None else control_plane_db_path()
    if not path.is_absolute():
        raise RelativePersistentDbPathError(
            f"db_path must be an absolute path, got {db_path!r}; refusing to resolve a "
            "relative path against the process's current working directory for a durable state root"
        )
    return _connect_path(path)


def _connect_in_memory_for_tests() -> sqlite3.Connection:
    """Internal, test-only escape hatch for an ephemeral in-memory control-plane DB.

    Deliberately not reachable through the public ``connect()`` path — see
    ``RelativePersistentDbPathError`` — so no real caller/CLI can end up with
    a non-durable connection by passing ``:memory:`` through.
    """
    return _connect_path(Path(":memory:"))


def _connect_path(path: Path) -> sqlite3.Connection:
    if str(path) != ":memory:":
        path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(path), isolation_level=None, timeout=5.0)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA busy_timeout = 5000")
    conn.execute("PRAGMA foreign_keys = ON")
    # SQLite's REPLACE conflict-resolution algorithm deletes the pre-existing
    # conflicting row before inserting, but — unless recursive_triggers is on
    # — that implicit delete does NOT fire BEFORE/AFTER DELETE triggers. Every
    # append-only table's *_no_update/*_no_delete triggers would otherwise be
    # silently bypassable via "INSERT OR REPLACE" (or any ON CONFLICT REPLACE
    # upsert) under SQLite's own default. Forcing this on closes that gap.
    conn.execute("PRAGMA recursive_triggers = ON")
    if str(path) != ":memory:":
        retry_on_locked(lambda: conn.execute("PRAGMA journal_mode = WAL"))
    run_migrations(conn)
    return conn


@contextmanager
def transaction(conn: sqlite3.Connection):
    """Wrap a block of writes in a single crash-safe SQLite transaction.

    Uses BEGIN IMMEDIATE so the write lock is acquired up front rather than
    on the first write statement, which avoids SQLITE_BUSY surfacing deep
    inside a multi-statement block under concurrent access.
    """
    conn.execute("BEGIN IMMEDIATE")
    try:
        yield conn
        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise
