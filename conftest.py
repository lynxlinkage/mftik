"""Workspace-wide fixtures: which database a test runs against, and which loop.

Every DB-touching fixture in the suite takes ``database_url``, so adding an
engine here fans the whole database suite out over it rather than editing nine
fixtures in five packages. The loop is here for the same reason — pytest-asyncio
builds every test's loop from one session fixture, so this is the only place
that has to know.
"""

from __future__ import annotations

import asyncio
import os
from collections.abc import Callable, Mapping

import pytest
from db_harness import POSTGRES_URL_ENV, dialect_urls

#: Which event loop the suite runs on: ``uvloop`` or ``asyncio``.
#:
#: Defaults to uvloop because that is what every process runs in production
#: (docs/EventLoop.md). A suite on a different loop from the node is a suite
#: that cannot see a loop-specific regression, which is the whole reason the
#: default is not simply left at CPython's.
#:
#: ``asyncio`` stays reachable, and CI runs ``packages`` that way too. Not for
#: symmetry: ``packages/common`` is published as ``mftik``, and nothing a
#: strategy author imports may require uvloop to work. That second pass is what
#: keeps the SDK honest, and it is also the setting for a contributor whose
#: environment has no uvloop.
TEST_LOOP_ENV = "MFTIK_TEST_LOOP"


def pytest_asyncio_loop_factories(
    config: pytest.Config, item: pytest.Item
) -> Mapping[str, Callable[[], asyncio.AbstractEventLoop]]:
    """Build every test's loop from :data:`TEST_LOOP_ENV`.

    The hook rather than the ``event_loop_policy`` fixture: overriding that
    fixture is deprecated in pytest-asyncio and warns, and loop policies are on
    their way out of asyncio itself. A factory is what both replace it with.

    It returns a *mapping*, and pytest-asyncio parametrises over it — so naming
    two factories here would run every async test on both loops in one pass.
    One is named on purpose: the second loop is worth a pass over the published
    SDK, not over five planes' worth of session machinery.
    """
    choice = os.getenv(TEST_LOOP_ENV, "uvloop")
    if choice == "asyncio":
        # What pytest-asyncio would have used anyway, named so the test id says
        # which loop a failure came from.
        return {"asyncio": asyncio.new_event_loop}
    if choice != "uvloop":
        raise pytest.UsageError(
            f"{TEST_LOOP_ENV}={choice!r}: expected 'uvloop' or 'asyncio'."
        )
    try:
        import uvloop
    except ImportError as exc:  # pragma: no cover - platform-dependent
        # Explicit rather than a quiet fall back to the stdlib loop. A suite
        # that silently stopped testing the loop production runs is the failure
        # this hook exists to prevent.
        raise pytest.UsageError(
            "uvloop is not installed, so the suite cannot run the loop "
            f"production uses. Install it, or set {TEST_LOOP_ENV}=asyncio to "
            "test the stdlib loop instead."
        ) from exc
    return {"uvloop": uvloop.new_event_loop}


def pytest_sessionstart(session: pytest.Session) -> None:
    """In CI, sqlite-only is a failure rather than a quiet degradation.

    Without this, a Postgres service that failed to come up would not turn the
    build red — the suite would run against sqlite alone and pass, which is the
    state this parametrisation exists to end.
    """
    if os.getenv("CI") and "postgres" not in dialect_urls():
        raise pytest.UsageError(
            f"{POSTGRES_URL_ENV} is unset: CI would test sqlite only."
        )


def pytest_generate_tests(metafunc: pytest.Metafunc) -> None:
    if "database_url" in metafunc.fixturenames:
        urls = dialect_urls()
        metafunc.parametrize(
            "database_url", list(urls.values()), ids=list(urls), scope="function"
        )
