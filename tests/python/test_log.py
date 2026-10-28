"""Module-level log lines (E06-D005, E06-D006)."""

from __future__ import annotations

import json
import os
import unittest

import mftik


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
            mftik.log.debug("hello", k=Count(3))
        self.assertIn("log field k must be str, int, float, bool, or None", str(caught.exception))

    def test_wide_int_is_a_json_number(self) -> None:
        wide = 1 << 80
        text = _capture(lambda: mftik.log.warn("hello", k=wide))
        row = json.loads(text)
        self.assertEqual(row["level"], "warn")
        self.assertEqual(row["k"], wide)


if __name__ == "__main__":
    unittest.main()
