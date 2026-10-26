"""Volume read order and the path and bytes rules (E04-D044). Not a port of the Go tests."""

import os
import tempfile
import unittest
from pathlib import Path

import mftik.volume as volume


class Folder:
    """A path-like directory, so ``os.fspath`` is the only way in."""

    def __init__(self, path: str) -> None:
        self.path = path

    def __fspath__(self) -> str:
        return self.path


class BytesPath:
    def __fspath__(self) -> bytes:
        return b"dir"


class VolumeTest(unittest.TestCase):
    def test_nested_write_replaces_and_read_prefers_the_volume(self) -> None:
        with (
            tempfile.TemporaryDirectory(prefix="mftik-vol-") as vol,
            tempfile.TemporaryDirectory(prefix="mftik-in-") as inp,
        ):
            nest = Path(vol) / "nest"
            nest.mkdir()
            volume.write(vol, "nest/leaf", b"one")
            self.assertEqual((nest / "leaf").read_bytes(), b"one")
            self.assertEqual(volume.read(vol, inp, "nest/leaf"), b"one")
            self.assertIs(type(volume.read(vol, inp, "nest/leaf")), bytes)

            buf = bytearray(b"two")
            volume.write(Folder(vol), "nest/leaf", buf)
            buf[0] = ord("z")
            self.assertEqual(volume.read(Path(vol), Path(inp), "nest/leaf"), b"two")

            view = memoryview(b"mem")
            volume.write(vol, "nest/mem", view)
            self.assertEqual(volume.read(vol, "", "nest/mem"), b"mem")

            (Path(inp) / "only").write_bytes(b"in")
            self.assertIsNone(volume.read(vol, inp, "missing"))
            self.assertEqual(volume.read(vol, inp, "only"), b"in")
            self.assertEqual(volume.read("", inp, "only"), b"in")
            volume.write(vol, "only", b"")
            self.assertEqual(volume.read(vol, inp, "only"), b"")
            self.assertIsNone(volume.read("", "", "only"))

    def test_a_str_is_not_encoded_and_a_name_cannot_escape(self) -> None:
        with tempfile.TemporaryDirectory(prefix="mftik-bad-") as vol:
            with self.assertRaises(TypeError) as caught:
                volume.write(vol, "n", "text")  # type: ignore[arg-type]
            self.assertEqual(str(caught.exception), "volume data must be bytes-like, not str")

            with self.assertRaises(TypeError):
                volume.write(1, "n", b"x")  # type: ignore[arg-type]
            with self.assertRaises(TypeError):
                volume.write(BytesPath(), "n", b"x")  # type: ignore[arg-type]
            with self.assertRaises(TypeError):
                volume.write(vol, Path("n"), b"x")  # type: ignore[arg-type]

            before = sorted(os.listdir(vol))
            for name in ("", ".", "..", "/abs", "a/../b", "a/./b", "a//b"):
                with self.assertRaises(OSError) as caught:
                    volume.write(vol, name, b"x")
                self.assertIn("volume: name must be a relative path", str(caught.exception))
                with self.assertRaises(OSError):
                    volume.read(vol, "", name)
            self.assertEqual(sorted(os.listdir(vol)), before)

            with self.assertRaises(OSError) as caught:
                volume.write("", "n", b"x")
            self.assertEqual(str(caught.exception), "volume: empty directory")
