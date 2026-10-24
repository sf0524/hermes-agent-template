"""Offline contracts for the image's pinned Node security overlay."""
from __future__ import annotations

from copy import deepcopy
import json
from pathlib import Path
import tempfile
import unittest

from build_hermes_image import NODE_SHA256, NODE_VERSION, pin_security_node


class NodeSecurityPinTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.lock = Path(self.tmp.name) / "lock.json"
        self.data = {"schema": 1, "packages": {
            "node": {"version": "26.7.0", "artifacts": {
                **{target: {"url": "old", "sha256": "old"} for target in NODE_SHA256},
                "linux-x64": {
                    "sha256": "982aa24dd8be4c889c6a8ab337ddff3b0896645b20f4239356e80552c16277ee",
                    "url": "https://nodejs.org/dist/v26.7.0/node-v26.7.0-linux-x64.tar.xz",
                },
                "linux-arm64-bionic": {"url": "termux-old", "sha256": "old"},
                "linux-arm64-musl": {"url": "unofficial-old", "sha256": "old"},
            }},
            "python": {"version": "unrelated", "artifacts": {"any": {"sha256": "untouched"}}},
        }}
        self.store()

    def store(self):
        self.lock.write_text(json.dumps(self.data, sort_keys=True) + "\n")

    def test_pins_official_platform_artifacts_only_without_other_package_drift(self):
        original_python = deepcopy(self.data["packages"]["python"])
        pin_security_node(self.lock)
        updated = json.loads(self.lock.read_text())
        self.assertEqual(updated["packages"]["python"], original_python)
        self.assertEqual(updated["packages"]["node"]["version"], NODE_VERSION)
        self.assertEqual(set(updated["packages"]["node"]["artifacts"]), set(NODE_SHA256))
        for target, sha in NODE_SHA256.items():
            artifact = updated["packages"]["node"]["artifacts"][target]
            self.assertEqual(artifact["sha256"], sha)
            platform = target.replace("win32-", "win-")
            suffix = "zip" if target.startswith("win32-") else "tar.xz"
            self.assertEqual(artifact["url"],
                             f"https://nodejs.org/dist/v{NODE_VERSION}/"
                             f"node-v{NODE_VERSION}-{platform}.{suffix}")
        self.assertEqual(updated["packages"]["node"]["artifacts"]["linux-x64"]["sha256"],
                         "6170b2d474e1c626f43cff3adc118df9fe3b573258a4fc3d850c8dbc6b079bbd")
        first = self.lock.read_bytes()
        pin_security_node(self.lock)
        self.assertEqual(self.lock.read_bytes(), first)

    def test_rejects_unexpected_source_version_without_writing(self):
        self.data["packages"]["node"]["version"] = "26.8.0"
        self.store()
        original = self.lock.read_bytes()
        with self.assertRaisesRegex(RuntimeError, "Unexpected Node pin"):
            pin_security_node(self.lock)
        self.assertEqual(self.lock.read_bytes(), original)

    def test_rejects_changed_source_checksum_without_writing(self):
        self.data["packages"]["node"]["artifacts"]["linux-x64"]["sha256"] = "wrong"
        self.store()
        original = self.lock.read_bytes()
        with self.assertRaisesRegex(RuntimeError, "Unexpected Node pin"):
            pin_security_node(self.lock)
        self.assertEqual(self.lock.read_bytes(), original)

    def test_rejects_unexpected_artifact_targets_without_writing(self):
        del self.data["packages"]["node"]["artifacts"]["darwin-arm64"]
        self.store()
        original = self.lock.read_bytes()
        with self.assertRaisesRegex(RuntimeError, "Unexpected Node pin"):
            pin_security_node(self.lock)
        self.assertEqual(self.lock.read_bytes(), original)

    def test_rejects_wrong_existing_fixed_row_without_writing(self):
        pin_security_node(self.lock)
        self.data = json.loads(self.lock.read_text())
        self.data["packages"]["node"]["artifacts"]["linux-x64"]["sha256"] = "wrong"
        self.store()
        original = self.lock.read_bytes()
        with self.assertRaisesRegex(RuntimeError, "Unexpected Node pin"):
            pin_security_node(self.lock)
        self.assertEqual(self.lock.read_bytes(), original)

    def test_rejects_unknown_schema_without_writing(self):
        self.data["schema"] = 2
        self.store()
        original = self.lock.read_bytes()
        with self.assertRaisesRegex(RuntimeError, "Unsupported Hermes PM lock schema"):
            pin_security_node(self.lock)
        self.assertEqual(self.lock.read_bytes(), original)


if __name__ == "__main__":
    unittest.main()
