"""Manifest and rootfs checks for the packed Rust strategies (#479, #480, E08-D005)."""

from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

import conformance_rootfs_rust as rust

NAME = "conform"
PROGRAM = b"strategy\n"
DIGEST = "sha256:" + "0" * 64
VERSION = "v1.0.0"


def _rootfs(base: Path) -> Path:
    (base / "bin").mkdir()
    (base / "bin" / "strategy").write_bytes(PROGRAM)
    return base


def _report(rootfs: Path) -> dict[str, object]:
    return {
        "digest": DIGEST,
        "kind": "strategy",
        "name": NAME,
        "os": "linux",
        "arch": "amd64",
        "contracts": ["worker.v1", "stsctl.v1", "md.v1", "td.v1"],
        "entrypoint": ["bin/strategy"],
        "env": {},
        "runtime": "go",
        "sdk_lang": "go",
        "sdk_version": VERSION,
        "rootfs": str(rootfs),
    }


class RustRootfsTests(unittest.TestCase):
    def test_image_accepts_the_packed_binary(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = _rootfs(Path(tmp))
            self.assertEqual(
                rust.require_image(_report(root), NAME, DIGEST, PROGRAM, VERSION), root
            )

    def test_image_rejects_another_manifest_field(self) -> None:
        cases: tuple[tuple[str, object], ...] = (
            ("digest", "sha256:" + "1" * 64),
            ("name", "md_watch"),
            ("runtime", "python"),
            ("sdk_version", "v1.0.1"),
            ("contracts", ["worker.v1", "stsctl.v1", "md.v1"]),
            ("entrypoint", ["bin/worker"]),
            ("env", {"A": "1"}),
        )
        with tempfile.TemporaryDirectory() as tmp:
            root = _rootfs(Path(tmp))
            for key, value in cases:
                with self.subTest(key=key):
                    report = _report(root)
                    report[key] = value
                    with self.assertRaises(SystemExit):
                        rust.require_image(report, NAME, DIGEST, PROGRAM, VERSION)

    def test_image_rejects_a_second_file(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = _rootfs(Path(tmp))
            (root / "bin" / "extra").write_bytes(b"")
            with self.assertRaises(SystemExit):
                rust.require_image(_report(root), NAME, DIGEST, PROGRAM, VERSION)

    def test_image_rejects_other_bytes(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = _rootfs(Path(tmp))
            with self.assertRaises(SystemExit):
                rust.require_image(_report(root), NAME, DIGEST, b"other\n", VERSION)


if __name__ == "__main__":
    unittest.main()
