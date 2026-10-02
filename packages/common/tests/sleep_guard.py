"""Fail unit and component tests that call ``asyncio.sleep(x > 0)``.

§9.2 rule 1: time is injected. A unit or component test drives a
:class:`mftik.clock.FakeClock` and does not wait on the wall clock. The
root ``conftest.py`` installs :func:`install` for the whole session and
arms :func:`arm` around each test.

``asyncio.sleep(0)`` is left alone. It yields the task; it does not wait.
A sleep whose direct caller is outside this repository — nats-py's ping
loop, the websockets keepalive — is left alone too. Opting out every
connection test to spare those would also hide a sleep the test itself
added. ``chase.py`` and the other in-repo callers are still caught.

Exempt, because those tiers are allowed real time (§9.1):

* ``@pytest.mark.integration``
* ``@pytest.mark.e2e``

A unit or component test that genuinely still needs the wall clock — a
real NATS round trip, a venue stub whose production code has not moved
onto :class:`~mftik.clock.Clock` yet — opts out with the narrowest marker::

    @pytest.mark.real_sleep(reason="why this test waits on the wall clock")

``reason`` is required and must be a non-empty string. ``component`` is
not an exemption: that tier forbids wall-clock sleep the same way unit
does. B2-04 owns the tier markers; they are registered here only so this
guard can read them.
"""

from __future__ import annotations

import asyncio
import traceback
from collections.abc import Callable
from contextvars import ContextVar, Token
from pathlib import Path
from typing import Any

import pytest

_ORIGINAL: Callable[..., Any] | None = None

# None means "not inside a test" — session startup may sleep. A test sets
# this, and a task the test creates copies the value with the context.
_POLICY: ContextVar[_Policy | None] = ContextVar("mftik_sleep_policy", default=None)


class RealSleepForbidden(RuntimeError):
    """A unit or component test called ``asyncio.sleep`` with a positive delay.

    ``filename`` and ``lineno`` are the caller, not this guard.
    """

    def __init__(
        self,
        delay: float,
        nodeid: str,
        filename: str,
        lineno: int,
        func: str,
    ) -> None:
        self.delay = delay
        self.nodeid = nodeid
        self.filename = filename
        self.lineno = lineno
        self.func = func
        super().__init__(
            f"{nodeid}: asyncio.sleep({delay!r}) is forbidden in a unit or "
            f"component test; called from {filename}:{lineno} in {func}. "
            f"Drive time with mftik.clock.FakeClock.advance(), or mark the "
            f"test @pytest.mark.real_sleep(reason='...') when it genuinely "
            f"needs wall-clock time. integration and e2e tests are exempt."
        )


class _Policy:
    __slots__ = ("allow", "nodeid")

    def __init__(self, *, allow: bool, nodeid: str) -> None:
        self.allow = allow
        self.nodeid = nodeid


def install() -> None:
    """Replace ``asyncio.sleep`` with the guard. Idempotent."""
    global _ORIGINAL
    if _ORIGINAL is not None:
        return
    _ORIGINAL = asyncio.sleep
    asyncio.sleep = _guarded_sleep  # type: ignore[assignment]


def arm(item: pytest.Item) -> Token[_Policy | None]:
    """Forbid real sleep for this test, unless it is exempt."""
    return _POLICY.set(
        _Policy(allow=allows(item), nodeid=item.nodeid)
    )


def disarm(token: Token[_Policy | None]) -> None:
    _POLICY.reset(token)


def allows(item: pytest.Item) -> bool:
    """Whether this test may call ``asyncio.sleep`` with a positive delay."""
    if item.get_closest_marker("integration") is not None:
        return True
    if item.get_closest_marker("e2e") is not None:
        return True
    mark = item.get_closest_marker("real_sleep")
    if mark is None:
        return False
    if _reason(mark) is None:
        raise pytest.UsageError(
            f"{item.nodeid}: @pytest.mark.real_sleep requires a non-empty "
            f"reason string"
        )
    return True


def _reason(mark: pytest.Mark) -> str | None:
    reason = mark.kwargs.get("reason")
    if reason is None and mark.args:
        reason = mark.args[0]
    if isinstance(reason, str) and reason.strip():
        return reason
    return None


def _positive(delay: Any) -> bool:
    try:
        return delay > 0
    except TypeError:
        return False


# packages/common/tests/sleep_guard.py → the repository root.
_ROOT = Path(__file__).resolve().parents[3]


def _in_repo(filename: str) -> bool:
    """Whether ``filename`` is this repo, and not a dependency installed in it.

    nats-py's ping loop and the websockets keepalive call ``asyncio.sleep``
    on their own. That is not the test, or the code under test, waiting.
    Treating it as a failure would opt every connection test out of the
    guard, which would then also hide a sleep the test itself added.
    """
    path = Path(filename)
    if (
        "site-packages" in path.parts
        or "dist-packages" in path.parts
        or ".venv" in path.parts
    ):
        return False
    try:
        path.resolve().relative_to(_ROOT)
    except ValueError:
        return False
    return True


def _caller() -> tuple[str, int, str] | None:
    """The direct caller of ``asyncio.sleep``, if it lives in this repo.

    ``None`` means the caller is a library or the standard library, and the
    guard stays out of it.
    """
    for frame in reversed(traceback.extract_stack()):
        if frame.filename == __file__:
            continue
        if not _in_repo(frame.filename):
            return None
        return frame.filename, frame.lineno, frame.name
    return None


async def _guarded_sleep(delay: float = 0, result: Any = None) -> Any:
    policy = _POLICY.get()
    if policy is not None and not policy.allow and _positive(delay):
        caller = _caller()
        # A library keepalive is not this test sleeping. See ``_in_repo``.
        if caller is not None:
            filename, lineno, func = caller
            raise RealSleepForbidden(
                delay, policy.nodeid, filename, lineno, func
            )
    assert _ORIGINAL is not None
    return await _ORIGINAL(delay, result)
