#!/usr/bin/env python3
"""Read-only pre-upgrade check for v2026.9.21 named-profile replies.

The old gateway recorded a served profile's delivery obligations in that
profile's state.db. v2026.9.24 reads the launch home's state.db instead, so
nonterminal rows left in named homes will no longer be swept on startup.

Run against a stopped v2026.9.21 volume before upgrading. This check neither
claims nor moves rows: an ``attempting`` reply may already have reached the
platform, and copying it blindly could send a duplicate.

Exit 0: no named-profile nonterminal rows; 2: advisory outstanding rows;
3: a profile could not be checked, so the result is unknown. Exit 3 takes
precedence over 2, but both findings are printed.
"""

from __future__ import annotations

import argparse
import os
import re
import sqlite3
import stat
import sys
from contextlib import closing
from dataclasses import dataclass
from pathlib import Path


# hermes_constants.PROFILE_ID_RE in both v2026.9.21 and v2026.9.24.
PROFILE_ID_RE = re.compile(r"^[a-z0-9][a-z0-9_-]{0,63}$")
NONTERMINAL_STATES = ("pending", "attempting", "failed")
# v2026.9.21 gateway/delivery_ledger.py:_initialize_schema. The optional
# adapter_profile column was added to older ledgers on first use, so it is not
# required to read a pre-existing database.
REQUIRED_COLUMNS = frozenset({
    "obligation_id", "session_key", "platform", "chat_id", "thread_id",
    "content", "state", "attempts", "created_at", "updated_at",
    "owner_pid", "owner_started_at", "last_error",
})


class UnsafePathError(Exception):
    """A path might escape the volume or is not a regular Hermes file."""


@dataclass(frozen=True)
class ProfileCheck:
    profile: str
    counts: dict[str, int]
    error: str | None = None

    @property
    def outstanding(self) -> int:
        return sum(self.counts.get(state, 0) for state in NONTERMINAL_STATES)


def _kind(path: Path) -> str:
    """Classify one path without following links, including dangling links."""
    try:
        mode = path.lstat().st_mode
    except FileNotFoundError:
        return "missing"
    if stat.S_ISDIR(mode):
        return "directory"
    if stat.S_ISREG(mode):
        return "file"
    return "unsafe"


def _require_directory(path: Path) -> None:
    if _kind(path) != "directory":
        raise UnsafePathError("directory is missing, linked, or not a directory")


def _profile_dirs(home: Path) -> list[Path | ProfileCheck]:
    _require_directory(home)
    profiles = home / "profiles"
    kind = _kind(profiles)
    if kind == "missing":
        return []
    if kind != "directory":
        raise UnsafePathError("profiles directory is linked or not a directory")
    tombstones = profiles / ".deleted"
    tombstone_kind = _kind(tombstones)
    if tombstone_kind not in ("missing", "directory"):
        raise UnsafePathError("profile tombstones directory is linked or not a directory")
    # No upstream imports or SQL writes: a failed preflight must leave the
    # v2026.9.21 ledger and its WAL available for recovery.
    result = []
    for entry in sorted(profiles.iterdir()):
        if entry.name == "default" or not PROFILE_ID_RE.fullmatch(entry.name):
            continue
        if tombstone_kind == "directory" and _kind(tombstones / entry.name) != "missing":
            continue  # Hermes excludes deleted profiles from the live roster.
        kind = _kind(entry)
        if kind == "unsafe":
            result.append(ProfileCheck(entry.name, {}, "profile directory is linked or not a directory"))
        if kind == "directory":
            result.append(entry)
    return result


def _check_profile(profile_dir: Path) -> ProfileCheck:
    name = profile_dir.name
    db = profile_dir / "state.db"
    try:
        kind = _kind(db)
        if kind == "missing":
            # An orphan WAL/journal could contain uncheckpointed obligations.
            if any(_kind(profile_dir / ("state.db" + suffix)) != "missing"
                   for suffix in ("-wal", "-shm", "-journal")):
                raise UnsafePathError("state.db is missing but SQLite sidecars exist")
            return ProfileCheck(name, {})
        if kind != "file":
            raise UnsafePathError("state.db is linked or not a regular file")
        for suffix in ("-wal", "-shm", "-journal"):
            sidecar_kind = _kind(profile_dir / ("state.db" + suffix))
            if sidecar_kind not in ("missing", "file"):
                raise UnsafePathError("a SQLite sidecar is linked or not a regular file")

        # mode=ro sees committed WAL records. Do not use immutable=1: SQLite
        # would ignore an uncheckpointed WAL and falsely report zero rows.
        # query_only is a second guard against accidental future write SQL.
        with closing(sqlite3.connect(db.as_uri() + "?mode=ro", uri=True, timeout=0.25)) as conn:
            conn.execute("PRAGMA query_only=ON")
            row = conn.execute(
                "SELECT type FROM sqlite_master WHERE name='delivery_obligations'"
            ).fetchone()
            if row is None:
                return ProfileCheck(name, {})
            if row[0] != "table":
                raise ValueError("delivery_obligations is not a table")
            columns = {item[1] for item in conn.execute("PRAGMA table_info(delivery_obligations)")}
            if not REQUIRED_COLUMNS.issubset(columns):
                raise ValueError("delivery_obligations has an unrecognized schema")

            # Classify in SQL so arbitrary/corrupt state text never appears in
            # output, alongside content, chat IDs, tokens, or error details.
            rows = conn.execute(
                """SELECT CASE
                       WHEN state IN ('pending', 'attempting', 'failed', 'delivered', 'abandoned')
                         THEN state ELSE 'unknown' END AS category, COUNT(*)
                   FROM delivery_obligations GROUP BY category"""
            ).fetchall()
            counts = {str(state): int(count) for state, count in rows}
            if counts.get("unknown", 0):
                raise ValueError("delivery_obligations contains an unrecognized state")
            return ProfileCheck(name, counts)
    except (OSError, sqlite3.Error, UnsafePathError, ValueError) as exc:
        # The fixed messages above and SQLite exception type are enough to
        # direct investigation without printing SQL values or SQLite paths.
        if isinstance(exc, sqlite3.Error):
            reason = "state.db cannot be read consistently (locked or damaged)"
        elif isinstance(exc, OSError):
            reason = "state.db path cannot be inspected"
        else:
            reason = str(exc)
        return ProfileCheck(name, {}, reason)


def check(home: Path) -> tuple[list[ProfileCheck], str | None]:
    """Inspect every valid on-disk named profile; never mutate its SQLite DB."""
    try:
        home = home.expanduser().absolute()
        return [
            item if isinstance(item, ProfileCheck) else _check_profile(item)
            for item in _profile_dirs(home)
        ], None
    except (OSError, RuntimeError, UnsafePathError) as exc:
        # A directory-level failure means profiles may have been missed.
        return [], str(exc) if isinstance(exc, UnsafePathError) else "cannot list profile directories"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--hermes-home", type=Path,
        default=Path(os.environ.get("HERMES_HOME") or "/data/.hermes"),
        help="Root HERMES_HOME on the old persistent volume (default: env HERMES_HOME or /data/.hermes)",
    )
    args = parser.parse_args(argv)
    results, scan_error = check(args.hermes_home)
    if scan_error:
        print(f"UNKNOWN: {scan_error}; inspect the Hermes volume before upgrading.")
        return 3

    unknown = False
    outstanding = False
    for result in results:
        if result.error:
            unknown = True
            print(f"UNKNOWN profile={result.profile}: {result.error}")
        elif result.outstanding:
            outstanding = True
            detail = " ".join(
                f"{state}={result.counts.get(state, 0)}" for state in NONTERMINAL_STATES
            )
            print(f"PENDING profile={result.profile} total={result.outstanding} {detail}")
        else:
            print(f"CLEAR profile={result.profile} nonterminal=0")

    if unknown:
        print("Upgrade check incomplete. Stop the gateway, inspect the named-profile databases, and rerun this check.")
        return 3
    if outstanding:
        print(
            "Old named-profile replies may not be replayed after upgrade. For important replies, "
            "resolve them before redeploying; asking again creates a new turn that may repeat tools "
            "or actions. An attempting reply may already have been sent."
        )
        return 2
    print("No outstanding named-profile delivery obligations found; this ledger check is clear.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
