"""Module-level log lines (E06-D005, E06-D006, E06-D017)."""

from __future__ import annotations

import json
import os
import subprocess
import sys
import unittest

import mftik

_BELOW_THE_LEVEL = """
import mftik

class Count(int):
    pass

assert mftik.log.debug(1, k=[1]) is None
assert mftik.log.info("hello", k=Count(3)) is None
try:
    mftik.log.warn("hello", k=[1])
except TypeError as exc:
    assert "log field k must be str, int, float, bool, or None" in str(exc), exc
else:
    raise SystemExit("expected TypeError")
mftik.log.error("written", k=1)
"""


def _capture(call) -> str:
    read_fd, write_fd = os.pipe()
    saved = os.dup(2)
    os.dup2(write_fd, 2)
    os.close(write_fd)
    try:
        call()
    finally:
        os.dup2(saved, 2)
        os.close(saved)
    os.set_blocking(read_fd, False)
    chunks = []
    while True:
        try:
            block = os.read(read_fd, 65536)
        except BlockingIOError:
            break
        if not block:
            break
        chunks.append(block)
    os.close(read_fd)
    return b"".join(chunks).decode()


def _run_at(level: str, script: str) -> subprocess.CompletedProcess[str]:
    env = {**os.environ, "MFTIKO_LOG_LEVEL": level, "MFTIKO_LOG_FORMAT": "json"}
    return subprocess.run(
        [sys.executable, "-c", script],
        env=env,
        check=False,
        capture_output=True,
        text=True,
    )


class LogTests(unittest.TestCase):
    def test_one_json_object_without_session_id(self) -> None:
        text = _capture(lambda: mftik.log.info("hello", k="v1", n=1, on=True, missing=None))
        self.assertTrue(text.endswith("\n"), text)
        self.assertEqual(text.count("\n"), 1, text)
        row = json.loads(text)
        self.assertEqual(
            list(row),
            [
                "level",
                "time",
                "component",
                "version",
                "commit",
                "sdk_version",
                "k",
                "n",
                "on",
                "missing",
                "message",
            ],
        )
        self.assertEqual(row["level"], "info")
        self.assertEqual(row["component"], "strategy")
        self.assertEqual(row["message"], "hello")
        self.assertEqual(row["k"], "v1")
        self.assertEqual(row["n"], 1)
        self.assertIs(row["on"], True)
        self.assertIsNone(row["missing"])
        self.assertNotIn("session_id", row)
        version, _, sha = row["sdk_version"].partition("+")
        self.assertEqual(version, mftik.__version__)
        self.assertEqual(len(sha), 40)

    def test_rejected_values(self) -> None:
        class Name(str):
            pass

        class Count(int):
            pass

        with self.assertRaises(TypeError) as caught:
            mftik.log.info(Name("hello"))
        self.assertIn("log message must be str", str(caught.exception))

        with self.assertRaises(TypeError) as caught:
            mftik.log.error("hello", k=[1])
        self.assertIn(
            "log field k must be str, int, float, bool, or None",
            str(caught.exception),
        )

        with self.assertRaises(TypeError) as caught:
            mftik.log.warn("hello", k=Count(3))
        self.assertIn("log field k must be str, int, float, bool, or None", str(caught.exception))

    def test_a_call_below_the_level_checks_nothing(self) -> None:
        completed = _run_at("warn", _BELOW_THE_LEVEL)
        self.assertEqual(completed.returncode, 0, completed.stderr)
        rows = [json.loads(line) for line in completed.stderr.splitlines()]
        self.assertEqual([(row["level"], row["message"]) for row in rows], [("error", "written")])

    def test_disabled_checks_and_writes_nothing(self) -> None:
        completed = _run_at("disabled", "import mftik\nassert mftik.log.error(1, k=[1]) is None\n")
        self.assertEqual(completed.returncode, 0, completed.stderr)
        self.assertEqual(completed.stderr, "")

    def test_wide_int_is_a_json_number(self) -> None:
        wide = 1 << 80
        text = _capture(lambda: mftik.log.warn("hello", k=wide))
        row = json.loads(text)
        self.assertEqual(row["level"], "warn")
        self.assertEqual(row["k"], wide)


if __name__ == "__main__":
    unittest.main()
