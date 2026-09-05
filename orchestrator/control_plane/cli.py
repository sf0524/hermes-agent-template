"""Shadow-only observer CLI: run one bounded tail-and-checkpoint cycle and exit.

Deliberately not a daemon and not a server: no listen socket, no webhook
route, no loop. Runs exactly one observation cycle against the control-plane
DB (resolved via ``ORCH_STATE_DB``/``HERMES_ROOT`` unless ``--control-db``
overrides it for a test) and prints a JSON summary to stdout.

``--mode`` only accepts ``shadow`` at the argument-parser level (so an
unsupported mode is rejected before any DB or source access happens, with
a non-zero exit and no partial output) and ``ShadowObserver`` enforces the
same restriction again itself, so the refusal holds even for a caller that
builds ``ShadowObserver`` directly instead of going through this CLI.
"""

from __future__ import annotations

import argparse
import json
import sys
from typing import Sequence

from orchestrator.control_plane.db import connect
from orchestrator.control_plane.kanban_tail import MAX_BATCH_SIZE, MIN_BATCH_SIZE
from orchestrator.control_plane.observer import ShadowObserver
from orchestrator.control_plane.state import control_plane_db_path, validate_absolute_control_db_path


def _batch_size_arg(value: str) -> int:
    """argparse ``type=`` for ``--batch-size``: rejected at parse time, before any DB/source access."""
    try:
        parsed = int(value)
    except ValueError:
        raise argparse.ArgumentTypeError(f"--batch-size must be an integer, got {value!r}")
    if not (MIN_BATCH_SIZE <= parsed <= MAX_BATCH_SIZE):
        raise argparse.ArgumentTypeError(
            f"--batch-size must be between {MIN_BATCH_SIZE} and {MAX_BATCH_SIZE}, got {parsed!r}"
        )
    return parsed


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="control-plane-observer",
        description=(
            "Shadow-only Kanban task_events observer: read-only, append-only evidence "
            "recording. No writes to Kanban, no dispatch, no side effects."
        ),
    )
    parser.add_argument(
        "--mode",
        required=True,
        choices=sorted(("shadow",)),
        help="Observer mode. Only 'shadow' exists in this slice.",
    )
    parser.add_argument("--source-db", required=True, help="Absolute path to the source Kanban SQLite DB.")
    parser.add_argument(
        "--control-db",
        default=None,
        help="Absolute path override for the control-plane state DB (defaults to the "
        "ORCH_STATE_DB/HERMES_ROOT resolution).",
    )
    parser.add_argument("--source-name", default="kanban", help="Source label recorded on each observation.")
    parser.add_argument("--policy-version", required=True)
    parser.add_argument("--actor", required=True, help="Principal recorded as this run's actor.")
    parser.add_argument("--correlation-id", required=True)
    parser.add_argument("--batch-size", type=_batch_size_arg, default=500)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_arg_parser()
    args = parser.parse_args(argv)

    if args.control_db is not None:
        control_db = str(validate_absolute_control_db_path(args.control_db, source="--control-db"))
    else:
        control_db = str(control_plane_db_path())
    conn = connect(control_db)
    try:
        observer = ShadowObserver(
            args.source_db,
            conn,
            mode=args.mode,
            source=args.source_name,
            batch_size=args.batch_size,
        )
        result = observer.run_once(
            policy_version=args.policy_version,
            actor=args.actor,
            correlation_id=args.correlation_id,
        )
    finally:
        conn.close()

    summary = {
        "mode": args.mode,
        "source": args.source_name,
        "rows_read": result.tail_result.rows_read,
        "appended": result.tail_result.appended,
        "duplicates": result.tail_result.duplicates,
        "quarantined": result.tail_result.quarantined,
        "cursor_before": result.tail_result.cursor_before,
        "cursor_after": result.tail_result.cursor_after,
        "checkpoint_id": result.checkpoint.id,
        "checkpoint_integrity_hash": result.checkpoint.integrity_hash,
    }
    print(json.dumps(summary))
    return 0


if __name__ == "__main__":
    sys.exit(main())
