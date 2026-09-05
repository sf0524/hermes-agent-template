import os
import unittest
from pathlib import Path
from unittest import mock

from orchestrator.control_plane.state import (
    DEFAULT_HERMES_ROOT,
    InvalidControlPlaneStateError,
    control_plane_db_path,
)


def _clean_env():
    env = dict(os.environ)
    env.pop("ORCH_STATE_DB", None)
    env.pop("HERMES_ROOT", None)
    return env


class ControlPlaneDbPathTests(unittest.TestCase):
    def test_defaults_to_data_hermes_orchestrator_control_plane_db(self):
        with mock.patch.dict(os.environ, _clean_env(), clear=True):
            self.assertEqual(
                control_plane_db_path(), DEFAULT_HERMES_ROOT / "orchestrator" / "control-plane.db"
            )

    def test_honours_explicit_orch_state_db(self):
        env = _clean_env()
        env["ORCH_STATE_DB"] = "/tmp/somewhere/control-plane.db"
        with mock.patch.dict(os.environ, env, clear=True):
            self.assertEqual(control_plane_db_path(), Path("/tmp/somewhere/control-plane.db"))

    def test_orch_state_db_takes_priority_over_hermes_root(self):
        env = _clean_env()
        env["ORCH_STATE_DB"] = "/tmp/explicit/state.db"
        env["HERMES_ROOT"] = "/tmp/other-root"
        with mock.patch.dict(os.environ, env, clear=True):
            self.assertEqual(control_plane_db_path(), Path("/tmp/explicit/state.db"))

    def test_honours_hermes_root_env_var(self):
        env = _clean_env()
        env["HERMES_ROOT"] = "/tmp/some-root"
        with mock.patch.dict(os.environ, env, clear=True):
            self.assertEqual(
                control_plane_db_path(), Path("/tmp/some-root") / "orchestrator" / "control-plane.db"
            )

    def test_empty_orch_state_db_falls_back_to_hermes_root(self):
        env = _clean_env()
        env["ORCH_STATE_DB"] = ""
        env["HERMES_ROOT"] = "/tmp/fallback-root"
        with mock.patch.dict(os.environ, env, clear=True):
            self.assertEqual(
                control_plane_db_path(), Path("/tmp/fallback-root") / "orchestrator" / "control-plane.db"
            )

    def test_empty_hermes_root_falls_back_to_default(self):
        env = _clean_env()
        env["HERMES_ROOT"] = ""
        with mock.patch.dict(os.environ, env, clear=True):
            self.assertEqual(
                control_plane_db_path(), DEFAULT_HERMES_ROOT / "orchestrator" / "control-plane.db"
            )

    def test_relative_orch_state_db_is_rejected(self):
        env = _clean_env()
        env["ORCH_STATE_DB"] = "relative/control-plane.db"
        with mock.patch.dict(os.environ, env, clear=True):
            with self.assertRaises(InvalidControlPlaneStateError):
                control_plane_db_path()

    def test_relative_hermes_root_is_rejected(self):
        env = _clean_env()
        env["HERMES_ROOT"] = "relative/root"
        with mock.patch.dict(os.environ, env, clear=True):
            with self.assertRaises(InvalidControlPlaneStateError):
                control_plane_db_path()

    def test_relative_hermes_root_never_produces_a_relative_state_db_path(self):
        env = _clean_env()
        env["HERMES_ROOT"] = "relative/root"
        with mock.patch.dict(os.environ, env, clear=True):
            try:
                control_plane_db_path()
            except InvalidControlPlaneStateError:
                pass
            else:
                self.fail("expected InvalidControlPlaneStateError")


if __name__ == "__main__":
    unittest.main()
