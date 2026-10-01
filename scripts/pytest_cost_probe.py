"""Where each test's wall time goes — B0-02 (#155), one-off.

``--durations`` says *which* tests are slow. This says *why*, which is what the
ticket actually asks for: the fifty slowest tests each tagged with their main
cause, and an answer to "is NATS what makes it slow" that F31 depends on.

It wraps the few calls that can block for real — the NATS client,
``asyncio.sleep``, subprocess spawn and wait, ``asyncpg.connect`` — and charges
the elapsed time to whichever test is running. Sleeps are also charged to their
call site, so a one-second wait inside ``LeasedSessionLink`` is not confused
with a one-second ``sleep`` written in the test.

Load it from outside the suite so no test file changes::

    PYTHONPATH=scripts pytest packages apps -p pytest_cost_probe \
        --cost-probe-out=probe.json

Two caveats on the numbers it reports:

* A background task that outlives the test that started it charges its sleeps
  to whichever test is running when they finish. At this granularity — tenths
  of a second on tests that take whole seconds — that is noise, not a bias.
* Wrapping costs something. The run that fills appendix C's module table is
  therefore an *uninstrumented* one, and this plugin's own total is reported
  next to it so the overhead is visible rather than assumed.
* Buckets nest, so they do not sum to the test. ``nats-py`` sleeps between
  connect attempts, for instance, and that second is in both ``nats_connect``
  and ``sleep``. The call site beside each number is what says which.
"""

from __future__ import annotations

import asyncio
import json
import os
import subprocess
import sys
import time
from collections import defaultdict
from pathlib import Path
from typing import Any

import pytest

_REPO_ROOT = Path(__file__).resolve().parent.parent

#: Where a frame chain stops belonging to the task that is running. Under
#: uvloop the chain ends on its own — the loop steps tasks from Cython, so
#: there is no Python frame below the coroutine — but the suite can also be
#: asked to run on the stdlib loop, where these appear.
_TASK_EDGE = ("asyncio/tasks.py", "asyncio/events.py", "asyncio/base_events.py")

_seconds: dict[str, float] = defaultdict(float)
_calls: dict[str, int] = defaultdict(int)
_sites: dict[tuple[str, str], float] = defaultdict(float)
_records: list[dict[str, Any]] = []
_phases: dict[str, dict[str, float]] = {}


def _reset() -> None:
    _seconds.clear()
    _calls.clear()
    _sites.clear()


def _charge(
    bucket: str, elapsed: float, site: str | None = None, suffix: str = ""
) -> None:
    _seconds[bucket + suffix] += elapsed
    _calls[bucket + suffix] += 1
    if site is not None:
        _sites[(bucket + suffix, site)] += elapsed


def _shown(filename: str, lineno: int) -> str:
    path = Path(filename)
    try:
        shown: Path = path.relative_to(_REPO_ROOT)
    except ValueError:
        shown = Path(*path.parts[-2:])
    return f"{shown}:{lineno}"


def _survey() -> tuple[str, bool]:
    """``(call site, is this the test's own task)`` for the caller.

    Two different things wait: the test, and the loops running behind it. A
    heartbeat task sleeping a second per beat and a test sleeping a second are
    not the same cost — only the second one is time the suite could not have
    spent otherwise — and summing them produced a "sleep" total twice the wall
    time of the run.

    So the walk stops at the edge of the running task and asks whether the
    test function is one of its frames. Production code the test awaited counts
    as the test; a helper the test started with ``create_task`` does not, even
    when it lives in the same file.
    """
    frame = sys._getframe(1)
    site = None
    while frame is not None:
        code = frame.f_code
        filename = code.co_filename
        if filename.endswith(_TASK_EDGE):
            break
        if filename != __file__:
            if site is None:
                site = _shown(filename, frame.f_lineno)
            # The test function itself, not just any frame in its module: a
            # heartbeat helper defined beside the test is still a background
            # loop. Both the file and the function have to be named `test_*`.
            if code.co_name.startswith("test_") and filename.rpartition("/")[
                2
            ].startswith("test_"):
                return site, True
        frame = frame.f_back
    return site or "<unknown>", False


def _wrap_async(owner: Any, name: str, bucket: str) -> None:
    """Charge ``owner.name``'s elapsed time to ``bucket``."""
    original = getattr(owner, name, None)
    if original is None:
        return

    async def measured(*args: Any, **kwargs: Any) -> Any:
        start = time.perf_counter()
        try:
            return await original(*args, **kwargs)
        finally:
            _charge(bucket, time.perf_counter() - start)

    setattr(owner, name, measured)


def _install() -> None:
    import nats.aio.client

    client = nats.aio.client.Client
    _wrap_async(client, "connect", "nats_connect")
    _wrap_async(client, "request", "nats_request")
    for name in ("publish", "subscribe", "flush", "drain", "close"):
        _wrap_async(client, name, "nats_other")

    real_sleep = asyncio.sleep

    async def measured_sleep(delay: float, *args: Any, **kwargs: Any) -> Any:
        # ``sleep(0)`` is a yield to the loop, not a wait. 29 call sites use it
        # that way; counting them would bury the waits that cost real time.
        if not delay or delay <= 0:
            return await real_sleep(delay, *args, **kwargs)
        site, on_test = _survey()
        start = time.perf_counter()
        try:
            return await real_sleep(delay, *args, **kwargs)
        finally:
            _charge(
                "sleep",
                time.perf_counter() - start,
                site=site,
                suffix="" if on_test else "_bg",
            )

    asyncio.sleep = measured_sleep

    real_wait_for = asyncio.wait_for

    async def measured_wait_for(fut: Any, timeout: Any = None) -> Any:
        # Only a wait_for that *timed out* is time spent waiting rather than
        # working, so only that case is charged. The lease heartbeat is one of
        # these rather than a sleep: `session.py` beats by waiting on its stop
        # event with the interval as the timeout.
        #
        # Reported on its own and never added to a total: the coroutine inside
        # may have been charged to another bucket already.
        site, on_test = _survey()
        start = time.perf_counter()
        try:
            return await real_wait_for(fut, timeout)
        except TimeoutError:
            _charge(
                "timeout",
                time.perf_counter() - start,
                site=site,
                suffix="" if on_test else "_bg",
            )
            raise

    asyncio.wait_for = measured_wait_for

    for name in ("create_subprocess_exec", "create_subprocess_shell"):
        _wrap_async(asyncio, name, "subprocess")
    _wrap_async(asyncio.subprocess.Process, "wait", "subprocess")
    _wrap_async(asyncio.subprocess.Process, "communicate", "subprocess")

    real_run = subprocess.run

    def measured_run(*args: Any, **kwargs: Any) -> Any:
        start = time.perf_counter()
        try:
            return real_run(*args, **kwargs)
        finally:
            _charge("subprocess", time.perf_counter() - start)

    subprocess.run = measured_run

    try:
        import asyncpg
    except ImportError:  # pragma: no cover - postgres is optional locally
        return
    _wrap_async(asyncpg, "connect", "pg_connect")


def pytest_addoption(parser: pytest.Parser) -> None:
    parser.addoption(
        "--cost-probe-out",
        default="cost-probe.json",
        help="where to write the per-test cost breakdown (B0-02)",
    )


def pytest_configure(config: pytest.Config) -> None:
    _install()


@pytest.hookimpl(hookwrapper=True)
def pytest_runtest_protocol(item: pytest.Item, nextitem: pytest.Item | None):
    _reset()
    _phases[item.nodeid] = {}
    yield
    phases = _phases.pop(item.nodeid, {})
    _records.append(
        {
            "nodeid": item.nodeid,
            "file": str(item.path.relative_to(_REPO_ROOT))
            if item.path.is_relative_to(_REPO_ROOT)
            else str(item.path),
            "total": sum(phases.values()),
            "phases": phases,
            "seconds": {k: round(v, 4) for k, v in _seconds.items() if v},
            "calls": dict(_calls),
            # Everything worth a tenth of the fastest tier's budget. A cap on
            # the count hid the cause of whole families of tests: TD's detach
            # re-asks a subject nobody serves for a second, and that was the
            # fifth-largest wait behind four background loops.
            "wait_sites": sorted(
                (
                    (bucket, site, round(sec, 4))
                    for (bucket, site), sec in _sites.items()
                    if sec >= 0.005
                ),
                key=lambda row: row[2],
                reverse=True,
            )[:12],
        }
    )


def pytest_runtest_logreport(report: pytest.TestReport) -> None:
    phases = _phases.get(report.nodeid)
    if phases is not None:
        phases[report.when] = report.duration


def pytest_sessionfinish(session: pytest.Session, exitstatus: int) -> None:
    out = Path(session.config.getoption("--cost-probe-out"))
    out.write_text(
        json.dumps(
            {
                "loop": os.getenv("MFTIK_TEST_LOOP", "uvloop"),
                "tests": _records,
            },
            indent=1,
        )
    )
