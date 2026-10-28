"""Loader and release selection, without a session."""

import os
import subprocess
import sys
import unittest
from pathlib import Path

import mftik
from mftik.run import format_load_error, load_strategy, resolve_release

FIXTURES = Path(__file__).resolve().parent / "fixtures"


class ReleaseTests(unittest.TestCase):
    def tearDown(self):
        os.environ.pop("MFTIK_RELEASE", None)

    def test_argument_wins(self):
        os.environ["MFTIK_RELEASE"] = "rel-env"
        self.assertEqual(resolve_release("rel-arg"), "rel-arg")

    def test_env_then_version(self):
        os.environ["MFTIK_RELEASE"] = "rel-env"
        self.assertEqual(resolve_release(None), "rel-env")
        self.assertEqual(resolve_release(""), "rel-env")

    def test_empty_env_uses_the_package_version(self):
        os.environ["MFTIK_RELEASE"] = ""
        self.assertEqual(resolve_release(None), mftik.__version__)
        self.assertEqual(resolve_release(""), mftik.__version__)


class LoadTests(unittest.TestCase):
    def setUp(self):
        sys.path.insert(0, str(FIXTURES))

    def tearDown(self):
        sys.path.remove(str(FIXTURES))
        sys.modules.pop("sample_mod", None)

    def test_loads_a_strategy(self):
        strategy = load_strategy("sample_mod:Sample")
        self.assertIsInstance(strategy, mftik.Strategy)

    def test_bad_spec(self):
        with self.assertRaises(ValueError) as caught:
            load_strategy("not-a-spec")
        self.assertEqual(
            format_load_error("not-a-spec", caught.exception),
            "load not-a-spec: ValueError: want module:Class, got 'not-a-spec'",
        )

    def test_missing_module(self):
        with self.assertRaises(ModuleNotFoundError) as caught:
            load_strategy("no_such_mod:Missing")
        self.assertEqual(
            format_load_error("no_such_mod:Missing", caught.exception),
            "load no_such_mod:Missing: ModuleNotFoundError: No module named 'no_such_mod'",
        )

    def test_not_a_strategy(self):
        with self.assertRaises(TypeError) as caught:
            load_strategy("sample_mod:Plain")
        self.assertEqual(
            format_load_error("sample_mod:Plain", caught.exception),
            "load sample_mod:Plain: TypeError: sample_mod:Plain is not a mftik.Strategy",
        )

    def test_version_and_commit_are_strings(self):
        strategy = load_strategy("sample_mod:Sample")
        with self.assertRaises(TypeError) as caught:
            mftik.run(strategy, version=1)
        self.assertEqual(str(caught.exception), "version must be str")
        with self.assertRaises(TypeError) as caught:
            mftik.run(strategy, commit=1)
        self.assertEqual(str(caught.exception), "commit must be str")

    def test_constructor_failure(self):
        with self.assertRaises(RuntimeError) as caught:
            load_strategy("sample_mod:Boom")
        self.assertEqual(
            format_load_error("sample_mod:Boom", caught.exception),
            "load sample_mod:Boom: RuntimeError: construct broke",
        )


class RunAttributeTests(unittest.TestCase):
    def test_run_stays_the_function(self):
        script = """
import mftik
import sys
assert "mftik.run" not in sys.modules
first = mftik.run
second = mftik.run
assert first is second
assert callable(first)
try:
    first(object())
except TypeError as exc:
    assert "mftik.Strategy" in str(exc)
else:
    raise SystemExit("expected TypeError")
"""
        completed = subprocess.run(
            [sys.executable, "-c", script],
            check=False,
            capture_output=True,
            text=True,
        )
        self.assertEqual(completed.returncode, 0, completed.stderr)


if __name__ == "__main__":
    unittest.main()
