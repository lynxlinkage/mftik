"""Public ``mftik.replay`` wrapper (E07-D020–D024)."""

from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

import mftik
from mftik.strategy import Strategy


class Bare(Strategy):
    def on_init(self, ctx):
        return None

    def on_start(self, ctx):
        return None

    def on_stop(self, ctx):
        return None


class ReplayApiTests(unittest.TestCase):
    def test_replay_error_is_an_error(self) -> None:
        self.assertTrue(issubclass(mftik.ReplayError, mftik.Error))
        self.assertFalse(issubclass(mftik.ReplayError, mftik.ContextError))
        err = mftik.ReplayError("sdk: replay: missing")
        self.assertIsInstance(err, mftik.Error)
        self.assertEqual(str(err), "sdk: replay: missing")

    def test_an_empty_directory_raises_replay_error(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            with self.assertRaises(mftik.ReplayError) as caught:
                mftik.replay(Bare, tmp)
            text = str(caught.exception)
            self.assertIn("sdk: replay:", text)
            self.assertIn("has no event log of incarnation 0", text)

    def test_a_bad_header_raises_replay_error(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            Path(tmp, "1-1.mel").write_bytes(b"not-a-log")
            with self.assertRaises(mftik.ReplayError) as caught:
                mftik.replay(Bare, Path(tmp))
            self.assertIn("sdk: replay:", str(caught.exception))

    def test_strategy_must_be_a_subclass(self) -> None:
        with self.assertRaises(TypeError) as caught:
            mftik.replay(Bare(), ".")
        self.assertEqual(str(caught.exception), "strategy must be a subclass of mftik.Strategy")
        with self.assertRaises(TypeError) as caught:
            mftik.replay(object, ".")
        self.assertEqual(str(caught.exception), "strategy must be a subclass of mftik.Strategy")

    def test_bool_incarnation_is_rejected(self) -> None:
        with self.assertRaises(TypeError) as caught:
            mftik.replay(Bare, ".", incarnation=True)
        self.assertEqual(str(caught.exception), "incarnation must be int")

    def test_bytes_dir_is_rejected(self) -> None:
        with self.assertRaises(TypeError) as caught:
            mftik.replay(Bare, b"eventlog")
        self.assertEqual(str(caught.exception), "dir must be a str, not bytes")

    def test_report_is_not_constructed_from_python(self) -> None:
        with self.assertRaises(TypeError):
            mftik.ReplayReport()

    def test_constructor_failure_is_not_a_replay_error(self) -> None:
        class Boom(Bare):
            def __init__(self) -> None:
                raise RuntimeError("nope")

        with self.assertRaises(RuntimeError) as caught:
            mftik.replay(Boom, ".")
        self.assertEqual(str(caught.exception), "nope")

    def test_none_options_use_the_rust_defaults(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            with self.assertRaises(mftik.ReplayError) as omitted:
                mftik.replay(Bare, tmp)
            with self.assertRaises(mftik.ReplayError) as explicit:
                mftik.replay(Bare, tmp, incarnation=None, volume_dir=None, input_dir=None)
            self.assertEqual(str(omitted.exception), str(explicit.exception))


if __name__ == "__main__":
    unittest.main()
