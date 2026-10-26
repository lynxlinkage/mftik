"""The user-doc checkers accept a matching tree and reject drift."""

from __future__ import annotations

import runpy
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
API = ROOT / "scripts" / "check-python-doc-api"
SNIPPETS = ROOT / "scripts" / "check-python-doc-snippets"

_PACKAGE = """\
__all__ = ["Widget", "run"]
"""

_PACKAGE_STUB = """\
__all__ = ["Widget", "run"]

__version__: str

def run(strategy: object) -> None: ...
"""

_NATIVE_STUB = """\
__all__ = ["Widget", "_run"]

class Widget:
    uid: int

    def ping(self) -> None: ...
    def __eq__(self, other: object, /) -> bool: ...

def _run() -> None: ...
"""

_DOCS = """\
# Guide

`Widget` `run` `__version__` `uid` `ping` `__eq__` `_run`
"""

_GENERATED = """\
__all__ = ["Ghost"]

class Ghost:
    spook: int
"""


def _run(script: Path, root: Path) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, str(script), "--root", str(root)],
        check=False,
        capture_output=True,
        text=True,
    )


def _write(root: Path, relative: str, text: str) -> None:
    path = root / relative
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def _api_tree(root: Path, docs: str = _DOCS) -> None:
    _write(root, "python/mftik/__init__.py", _PACKAGE)
    _write(root, "python/mftik/__init__.pyi", _PACKAGE_STUB)
    _write(root, "python/mftik/_mftik.pyi", _NATIVE_STUB)
    _write(root, "python/mftik/md/v1.pyi", _GENERATED)
    _write(root, "docs/python/guide.md", docs)


class DocApiTest(unittest.TestCase):
    def test_real_package_lists_handwritten_names_only(self) -> None:
        checker = runpy.run_path(str(API), run_name="docapi")
        names = checker["public_names"](ROOT)
        self.assertIn("Strategy", names)
        self.assertIn("_run", names)
        self.assertIn("session_contracts", names)
        self.assertNotIn("BestQuote", names)
        self.assertNotIn("Ghost", names)

    def test_documented_names_pass_and_generated_stubs_are_ignored(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            _api_tree(root)
            result = _run(API, root)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("public names", result.stdout)

    def test_prose_does_not_count_as_documentation(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            _api_tree(root, "# Guide\n\nWidget is a class.\n")
            result = _run(API, root)
        self.assertEqual(result.returncode, 1)
        self.assertIn("Widget", result.stderr)

    def test_a_missing_name_fails(self) -> None:
        docs = _DOCS.replace("`ping`", "ping")
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            _api_tree(root, docs)
            result = _run(API, root)
        self.assertEqual(result.returncode, 1)
        self.assertIn("ping", result.stderr)

    def test_py_and_pyi_all_must_match(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            _api_tree(root)
            _write(root, "python/mftik/__init__.pyi", _PACKAGE_STUB.replace('"run"', '"other"'))
            result = _run(API, root)
        self.assertEqual(result.returncode, 1)
        self.assertIn("__all__ differs", result.stderr)

    def test_a_name_inside_a_fence_counts(self) -> None:
        line = "`Widget` `run` `__version__` `uid` `ping` `__eq__` `_run`\n"
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            _api_tree(root, "```text examples/python/demo/app.py:1-1\n" + line + "```\n")
            _write(root, "examples/python/demo/app.py", line)
            result = _run(API, root)
        self.assertEqual(result.returncode, 0, result.stderr)


class DocSnippetTest(unittest.TestCase):
    def test_no_fences_passes(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            _write(root, "docs/python/guide.md", "No listings yet.\n")
            result = _run(SNIPPETS, root)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("0 snippets", result.stdout)

    def test_a_matching_excerpt_passes(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            _write(root, "examples/python/demo/app.py", "alpha\nbeta\n")
            _write(
                root,
                "docs/python/guide.md",
                "```python examples/python/demo/app.py:1-2\nalpha\nbeta\n```\n",
            )
            result = _run(SNIPPETS, root)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("1 snippets", result.stdout)

    def test_a_drifted_excerpt_fails(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            _write(root, "examples/python/demo/app.py", "alpha\nbeta\n")
            _write(
                root,
                "docs/python/guide.md",
                "```python examples/python/demo/app.py:1-2\nalpha\nBETA\n```\n",
            )
            result = _run(SNIPPETS, root)
        self.assertEqual(result.returncode, 1)
        self.assertIn("does not match", result.stderr)

    def test_an_uncited_block_fails(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            _write(root, "docs/python/guide.md", "```python\nx = 1\n```\n")
            result = _run(SNIPPETS, root)
        self.assertEqual(result.returncode, 1)
        self.assertIn("must cite examples/python", result.stderr)

    def test_a_path_outside_the_examples_fails(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            _write(root, "README.md", "hello\n")
            _write(
                root,
                "docs/python/guide.md",
                "```python README.md:1-1\nhello\n```\n",
            )
            result = _run(SNIPPETS, root)
        self.assertEqual(result.returncode, 1)
        self.assertIn("examples/python", result.stderr)

    def test_a_parent_segment_fails(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            _write(root, "examples/python/demo/app.py", "alpha\n")
            _write(
                root,
                "docs/python/guide.md",
                "```python examples/python/../python/demo/app.py:1-1\nalpha\n```\n",
            )
            result = _run(SNIPPETS, root)
        self.assertEqual(result.returncode, 1)
        self.assertIn("leaves examples/python", result.stderr)


if __name__ == "__main__":
    unittest.main()
