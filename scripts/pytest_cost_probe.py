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

_seconds: dict[str, float] = defaultdict(float)
_calls: dict[str, int] = defaultdict(int)
_sites: dict[str, float] = defaultdict(float)
_records: list[dict[str, Any]] = []
_phases: dict[str, dict[str, float]] = {}


def _reset() -> None:
    _seconds.clear()
    _calls.clear()
    _sites.clear()


def _charge(bucket: str, elapsed: float, site: str | None = None) -> None:
    _seconds[bucket] += elapsed
    _calls[bucket] += 1
    if site is not None:
        _sites[site] += elapsed


def _caller_site() -> str:
    """``path:lineno`` of the first frame outside this file.

    The sleep that makes a test slow is often not in the test — it is in the
    heartbeat loop or the reconnect backoff the test is waiting on, and the
    difference is the whole point of the ticket's cause tags.
    """
    frame = sys._getframe(1)
    while frame is not None and frame.f_code.co_filename == __file__:
        frame = frame.f_back
    if frame is None:
        return "<unknown>"
    path = Path(frame.f_code.co_filename)
    try:
        shown = path.relative_to(_REPO_ROOT)
    except ValueError:
        shown = Path(*path.parts[-2:])
    return f"{shown}:{frame.f_lineno}"


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
        site = _caller_site()
        start = time.perf_counter()
        try:
            return await real_sleep(delay, *args, **kwargs)
        finally:
            _charge("sleep", time.perf_counter() - start, site=site)

    asyncio.sleep = measured_sleep

    real_wait_for = asyncio.wait_for

    async def measured_wait_for(fut: Any, timeout: Any = None) -> Any:
        # Only a wait_for that *timed out* is time spent waiting rather than
        # working, so only that case is charged. It is reported on its own and
        # never added to a total: the coroutine inside may have been charged
        # to another bucket already.
        site = _caller_site()
        start = time.perf_counter()
        try:
            return await real_wait_for(fut, timeout)
        except TimeoutError:
            _charge("wait_timeout", time.perf_counter() - start, site=site)
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
            # Three is enough to name a cause and short enough to read.
            "wait_sites": sorted(
                ((site, round(sec, 4)) for site, sec in _sites.items()),
                key=lambda pair: pair[1],
                reverse=True,
            )[:3],
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
