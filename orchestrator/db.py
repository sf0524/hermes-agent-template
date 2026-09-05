"""Connection + explicit transaction helpers for the orchestrator state DB.

``isolation_level=None`` puts the connection in autocommit mode so
transactions are exactly what ``transaction()`` below says they are — no
implicit BEGIN inserted by the sqlite3 module before writes, no surprise
commit boundaries. Every write in this package goes through ``transaction()``.
"""

from __future__ import annotations

import random
import sqlite3
import time
from contextlib import contextmanager
from pathlib import Path

from orchestrator.home import orchestrator_state_db_path
from orchestrator.schema import run_migrations

_LOCK_RETRY_ATTEMPTS = 25
_LOCK_RETRY_BASE_DELAY = 0.02


def _is_lock_contention(exc: sqlite3.OperationalError) -> bool:
    msg = str(exc).lower()
    return "locked" in msg or "busy" in msg


def retry_on_locked(fn):
    """Run ``fn()``, retrying with jittered backoff while SQLite reports contention.

    Two connections can open the same fresh DB file at the same time (e.g.
    two processes booting concurrently) and race on ``PRAGMA journal_mode =
    WAL`` or the first migration's write lock. SQLite's own ``busy_timeout``
    covers most of that, but under enough concurrency it can still give up
    and raise ``OperationalError`` rather than block forever — this retries
    that specific case instead of surfacing a boot-time crash.
    """
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


def connect(db_path: Path | str | None = None) -> sqlite3.Connection:
    """Open (creating if needed) the orchestrator state DB and migrate it.

    Defaults to ``get_hermes_home()/orchestrator/state.db``; pass an explicit
    path (tests use this, e.g. ``:memory:`` or a tmp file) to point elsewhere.
    """
    path = Path(db_path) if db_path is not None else orchestrator_state_db_path()
    if str(path) != ":memory:":
        path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(path), isolation_level=None, timeout=5.0)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA busy_timeout = 5000")
    conn.execute("PRAGMA foreign_keys = ON")
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
