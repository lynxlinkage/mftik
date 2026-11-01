"""``mftik.replay`` arguments and errors (E07-D020–E07-D022). No NATS.

A recorded session is replayed in ``examples/python/sampler/test_e2e.py``.
"""

import os
import tempfile
import unittest
from pathlib import Path

import mftik


class Quiet(mftik.Strategy):
    def on_init(self, ctx: mftik.Context) -> None:
        return None

    def on_start(self, ctx: mftik.Context) -> None:
        return None

    def on_stop(self, ctx: mftik.Context) -> None:
        return None


class Built(Quiet):
    """Counts constructions, so a test can see that none happened."""

    made = 0

    def __init__(self) -> None:
        type(self).made += 1


class Broken(Quiet):
    def __init__(self) -> None:
        raise ValueError("cannot build")


class BytesPath:
    def __fspath__(self) -> bytes:
        return b"eventlog"


class ReplayArgumentsTest(unittest.TestCase):
    def setUp(self) -> None:
        self._dir = tempfile.TemporaryDirectory(prefix="mftik-replay-")
        self.dir = self._dir.name
        Built.made = 0

    def tearDown(self) -> None:
        self._dir.cleanup()

    def test_the_strategy_must_be_a_strategy_class(self) -> None:
        for value in (Quiet(), object, int, "mod:Class", None):
            with self.subTest(value=value):
                with self.assertRaisesRegex(TypeError, "subclass of mftik.Strategy"):
                    mftik.replay(value, self.dir)  # type: ignore[arg-type]

    def test_incarnation_is_none_or_a_u32_above_zero(self) -> None:
        for value in (0, -1, 2**32):
            with self.subTest(value=value):
                with self.assertRaisesRegex(ValueError, "incarnation must be between 1 and"):
                    mftik.replay(Built, self.dir, incarnation=value)
        for value in (True, 1.0, "1"):
            with self.subTest(value=value):
                with self.assertRaisesRegex(TypeError, "incarnation must be int or None"):
                    mftik.replay(Built, self.dir, incarnation=value)  # type: ignore[arg-type]
        self.assertEqual(Built.made, 0)

    def test_paths_are_str_or_path_like_str(self) -> None:
        for name in ("dir", "volume_dir", "input_dir"):
            for value in (b"eventlog", BytesPath(), 3):
                with self.subTest(name=name, value=value):
                    args: dict[str, object] = {"dir": self.dir, name: value}
                    with self.assertRaisesRegex(TypeError, f"{name} must be str"):
                        mftik.replay(Built, **args)  # type: ignore[arg-type]
        self.assertEqual(Built.made, 0)

    def test_a_constructor_error_is_raised_as_is(self) -> None:
        with self.assertRaisesRegex(ValueError, "cannot build"):
            mftik.replay(Broken, self.dir)


class ReplayErrorTest(unittest.TestCase):
    def test_replay_error_is_an_mftik_error(self) -> None:
        self.assertTrue(issubclass(mftik.ReplayError, mftik.Error))
        self.assertEqual(mftik.ReplayError.__module__, "mftik")

    def test_a_directory_without_a_log_is_a_replay_error(self) -> None:
        with tempfile.TemporaryDirectory(prefix="mftik-replay-") as tmp:
            with self.assertRaisesRegex(mftik.ReplayError, "has no event log of incarnation 0"):
                mftik.replay(Quiet, Path(tmp))
            with self.assertRaisesRegex(mftik.ReplayError, "has no event log of incarnation 4"):
                mftik.replay(Quiet, tmp, incarnation=4)

    def test_a_missing_directory_is_a_replay_error(self) -> None:
        with tempfile.TemporaryDirectory(prefix="mftik-replay-") as tmp:
            with self.assertRaisesRegex(mftik.ReplayError, "sdk: replay: "):
                mftik.replay(Quiet, os.path.join(tmp, "absent"))

    def test_a_file_that_is_not_an_event_log_is_a_replay_error(self) -> None:
        with tempfile.TemporaryDirectory(prefix="mftik-replay-") as tmp:
            Path(tmp, "1-1.mel").write_bytes(b"not a log, and long enough for a header" * 4)
            with self.assertRaisesRegex(mftik.ReplayError, "not an event log"):
                mftik.replay(Quiet, tmp)


class ReplayReportTest(unittest.TestCase):
    def test_the_report_is_not_built_from_python(self) -> None:
        with self.assertRaises(TypeError):
            mftik.ReplayReport()  # type: ignore[call-arg]


if __name__ == "__main__":
    unittest.main()
