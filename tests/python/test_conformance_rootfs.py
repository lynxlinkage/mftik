"""Path and report checks for the packed-rootfs conformance job (E05-D024)."""

from __future__ import annotations

import json
import os
import stat
import tempfile
import unittest
from pathlib import Path

import conformance_rootfs as rootfs


def _pass_report() -> dict[str, object]:
    return {"checks": [{"id": name, "status": "pass"} for name in rootfs.CHECK_IDS]}


class RootfsConformanceTests(unittest.TestCase):
    def test_report_accepts_eleven_passes(self) -> None:
        rootfs.assert_conformance_report(_pass_report())

    def test_report_rejects_a_skip(self) -> None:
        report = _pass_report()
        checks = report["checks"]
        assert isinstance(checks, list)
        checks[0] = {"id": "flow", "status": "skip", "detail": "worker.v1 not in contracts"}
        with self.assertRaises(SystemExit):
            rootfs.assert_conformance_report(report)

    def test_report_rejects_a_short_list(self) -> None:
        with self.assertRaises(SystemExit):
            rootfs.assert_conformance_report({"checks": [{"id": "flow", "status": "pass"}]})

    def test_relative_entrypoint_inside_rootfs(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            bindir = base / "python" / "bin"
            bindir.mkdir(parents=True)
            target = bindir / "python3.12"
            target.write_bytes(b"")
            target.chmod(target.stat().st_mode | stat.S_IEXEC)
            (bindir / "python3").symlink_to("python3.12")
            got = rootfs.require_entrypoint(base, list(rootfs.ENTRYPOINT))
            self.assertEqual(got, rootfs.ENTRYPOINT)

    def test_absolute_entrypoint_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            entry = ["/" + rootfs.ENTRYPOINT[0], *rootfs.ENTRYPOINT[1:]]
            with self.assertRaises(SystemExit):
                rootfs.require_entrypoint(Path(tmp), entry)

    def test_dotdot_entrypoint_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            entry = ["python/bin/../../bin/python3", *rootfs.ENTRYPOINT[1:]]
            with self.assertRaises(SystemExit):
                rootfs.require_entrypoint(Path(tmp), entry)

    def test_symlink_outside_rootfs_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            outside = base / "outside"
            outside.mkdir()
            host = outside / "python3"
            host.write_bytes(b"")
            host.chmod(host.stat().st_mode | stat.S_IEXEC)
            bindir = base / "root" / "python" / "bin"
            bindir.mkdir(parents=True)
            (bindir / "python3").symlink_to(host)
            with self.assertRaises(SystemExit):
                rootfs.require_entrypoint(base / "root", list(rootfs.ENTRYPOINT))

    def test_pth_follows_manifest_pythonpath(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            site = base / "python" / "lib" / "python3.12" / "site-packages"
            site.mkdir(parents=True)
            (base / "app").mkdir()
            (base / "site-packages").mkdir()
            rootfs.add_strategy_path(base, "3.12", "app:site-packages")
            text = (site / "mftik-conform.pth").read_text(encoding="utf-8")
            lines = text.splitlines()
            self.assertEqual(len(lines), 2)
            for line, name in zip(lines, ("app", "site-packages"), strict=True):
                got = Path(os.path.abspath(os.path.join(site, line))).resolve()
                self.assertEqual(got, (base / name).resolve())

    def test_tool_dir_rejects_host_python(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tool_dir = Path(tmp)
            binary = tool_dir / "mftiko-conformance"
            binary.write_bytes(b"")
            binary.chmod(binary.stat().st_mode | stat.S_IEXEC)
            rootfs.tool_dir_has_no_python(tool_dir)
            host = tool_dir / "python3"
            host.write_bytes(b"")
            host.chmod(host.stat().st_mode | stat.S_IEXEC)
            with self.assertRaises(SystemExit):
                rootfs.tool_dir_has_no_python(tool_dir)

    def test_packed_report_json_shape(self) -> None:
        raw = json.dumps(_pass_report())
        rootfs.assert_conformance_report(json.loads(raw))


if __name__ == "__main__":
    unittest.main()
