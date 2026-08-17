"""The ``mftik`` command.

``main`` is what ``[project.scripts]`` points at. It returns an exit code
rather than raising ``SystemExit``, so the whole CLI is callable from a test
without a subprocess.

The parser lives in :mod:`mftik.cli.app` rather than in a ``main`` module: a
module and a function reachable at the same dotted path is a trap for anything
that resolves names as strings — ``monkeypatch.setattr`` and
``importlib.import_module`` among them — and the one that wins is whichever
import ran last.
"""

from mftik.cli.app import main

__all__ = ["main"]
