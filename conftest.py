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
from broker_harness import server_address, server_is_up
from db_harness import POSTGRES_URL_ENV, dialect_urls
from sleep_guard import arm, disarm, install
from tier_budget import (
    annotation_lines,
    apply_timeouts,
    budget_summary_lines,
    budget_warnings,
    database_params,
    enforce_call_budget,
)

# Before any test, including ones collected from a path that does not import
# this module's helpers again. Idempotent if a plugin imports it twice.
install()

#: Over-budget call phases seen by the process that prints the summary.
#: xdist workers record the warning on the report; the controller collects
#: it from the serialized report and is the only process that prints.
_budget_offenders: list[tuple[str, str]] = []

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
        # What pytest-asyncio would have used anyway. The key names the
        # factory, it does not label the run: with a single factory
        # pytest-asyncio ids it `pytest.HIDDEN_PARAM`, so test ids read the
        # same on either loop and only this variable says which one ran.
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
    """Fail on a missing service rather than testing something weaker.

    Also drop call-phase warnings from a previous in-process session.

    Postgres is the integration job's dialect (§9.1 rule 6). That job sets
    ``MFTIK_REQUIRE_POSTGRES``; if the service did not come up, the postgres
    parameter would simply be absent and the job would go green without it.
    The unit job does not set the variable: sqlite is the dialect it is
    supposed to run. The broker has no fake at all any more, so a missing
    server is not a degradation but sixty modules of confusing connection
    errors; saying so once, here, is worth more than each of them saying it.
    """
    _budget_offenders.clear()
    # The unit+component job is sqlite on purpose (§9.1 rule 6). The
    # integration job sets this and fails here if its Postgres never came
    # up — otherwise that dialect would quietly not be parametrized.
    if os.getenv("MFTIK_REQUIRE_POSTGRES") and "postgres" not in dialect_urls():
        raise pytest.UsageError(
            f"{POSTGRES_URL_ENV} is unset: the integration job would "
            f"skip the Postgres dialect."
        )
    if not server_is_up():
        host, port = server_address()
        raise pytest.UsageError(
            f"no NATS server at {host}:{port}, and the broker "
            f"suite has no fake to fall back on. Start one with "
            f"`docker compose up -d nats`."
        )


def pytest_generate_tests(metafunc: pytest.Metafunc) -> None:
    if "database_url" in metafunc.fixturenames:
        metafunc.parametrize(
            "database_url", database_params(dialect_urls()), scope="function"
        )


def pytest_collection_modifyitems(
    config: pytest.Config, items: list[pytest.Item]
) -> None:
    del config
    apply_timeouts(items)


@pytest.hookimpl(wrapper=True)
def pytest_runtest_makereport(item: pytest.Item, call: object) -> object:
    """Apply the §9.1 call-phase cap after the call, before the report is logged."""
    del call
    report = yield
    enforce_call_budget(item, report)
    return report


def pytest_runtest_logreport(report: pytest.TestReport) -> None:
    """Collect CI call-phase warnings on the process that owns the summary.

    Workers see the report too. Their copy is not the one GitHub prints,
    and xdist already forwards ``user_properties`` to the controller.
    """
    if os.environ.get("PYTEST_XDIST_WORKER"):
        return
    if getattr(report, "when", None) != "call":
        return
    nodeid = getattr(report, "nodeid", "")
    for message in budget_warnings(report):
        _budget_offenders.append((str(nodeid), message))


def pytest_terminal_summary(
    terminalreporter, exitstatus: int, config: pytest.Config
) -> None:
    """Print every CI over-budget test, and cap the GitHub annotations."""
    del exitstatus, config
    if os.environ.get("PYTEST_XDIST_WORKER") or not _budget_offenders:
        return
    terminalreporter.write_sep("=", "call-phase budget warnings")
    for line in budget_summary_lines(_budget_offenders):
        terminalreporter.write_line(line)
    # Workflow commands must be a raw line. The terminal reporter wraps
    # and colors, which would keep GitHub from seeing ``::warning::``.
    for line in annotation_lines(_budget_offenders):
        print(line, flush=True)


@pytest.hookimpl(wrapper=True)
def pytest_runtest_protocol(
    item: pytest.Item, nextitem: pytest.Item | None
) -> object:
    """Forbid ``asyncio.sleep(x > 0)`` in unit and component tests (§9.2).

    integration and e2e are exempt. A unit or component test that still
    needs the wall clock opts out with ``@pytest.mark.real_sleep(reason=...)``.
    The markers themselves belong to B2-04; they are registered so this
    guard can see them.
    """
    token = arm(item)
    try:
        return (yield)
    finally:
        disarm(token)
