import json
import sqlite3
import tempfile
import unittest
from pathlib import Path

from orchestrator.control_plane import cli
from orchestrator.control_plane.db import connect
from orchestrator.control_plane.ledger import ObservationLedger


def _make_source_db(path: Path, rows: list[tuple]) -> None:
    conn = sqlite3.connect(str(path))
    try:
        conn.execute(
            """
            CREATE TABLE task_events (
                id INTEGER PRIMARY KEY,
                task_id TEXT NOT NULL,
                run_id TEXT,
                kind TEXT NOT NULL,
                payload TEXT NOT NULL,
                created_at TEXT NOT NULL
            )
            """
        )
        conn.executemany(
            "INSERT INTO task_events (id, task_id, run_id, kind, payload, created_at) VALUES (?, ?, ?, ?, ?, ?)",
            rows,
        )
        conn.commit()
    finally:
        conn.close()


class CliShadowModeRefusalTests(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.source_db_path = Path(self._tmp.name) / "kanban.db"
        _make_source_db(self.source_db_path, rows=[])
        self.control_db_path = Path(self._tmp.name) / "control-plane.db"

    def _argv(self, mode: str) -> list[str]:
        return [
            "--mode", mode,
            "--source-db", str(self.source_db_path),
            "--control-db", str(self.control_db_path),
            "--policy-version", "v1",
            "--actor", "observer",
            "--correlation-id", "corr-1",
        ]

    def test_non_shadow_mode_is_rejected_at_parse_time(self):
        with self.assertRaises(SystemExit) as ctx:
            cli.main(self._argv("active"))
        self.assertNotEqual(ctx.exception.code, 0)
        # rejected before any control-plane DB is created
        self.assertFalse(self.control_db_path.exists())

    def test_dispatch_mode_is_rejected(self):
        with self.assertRaises(SystemExit):
            cli.main(self._argv("dispatch"))

    def test_missing_required_args_exits_nonzero(self):
        with self.assertRaises(SystemExit) as ctx:
            cli.main(["--mode", "shadow"])
        self.assertNotEqual(ctx.exception.code, 0)


class CliRunOnceTests(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.source_db_path = Path(self._tmp.name) / "kanban.db"
        _make_source_db(
            self.source_db_path,
            rows=[(1, "task-1", None, "task.created", "{}", "2026-01-01T00:00:00+00:00")],
        )
        self.control_db_path = Path(self._tmp.name) / "control-plane.db"

    def test_shadow_mode_run_prints_summary_and_records_observation(self):
        exit_code = cli.main(
            [
                "--mode", "shadow",
                "--source-db", str(self.source_db_path),
                "--control-db", str(self.control_db_path),
                "--policy-version", "v1",
                "--actor", "observer",
                "--correlation-id", "corr-1",
            ]
        )
        self.assertEqual(exit_code, 0)
        conn = connect(self.control_db_path)
        self.addCleanup(conn.close)
        self.assertEqual(ObservationLedger(conn).count(), 1)


if __name__ == "__main__":
    unittest.main()
