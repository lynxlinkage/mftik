"""Hook time budget (F15, §5.3). What a measured stretch means.

The strategy loop measures. This module classifies the number and is
the report the ingress puts on ``sts.status.{session_id}``. The loop
is the only thing that can see its own blocking (§3.3: hook progress
is the ingress's to publish, the measurement is the loop's to make).
:class:`mftik.strategy.budget.HookSlow` is the warning counter the
loop increments. The thresholds are not that counter's, and they are
not a ``strategy.yml`` key. A strategy does not get to raise its own
ceiling.

General hooks (``on_ticker``, ``on_order_update``, a timer callback,
anything that is not the three lifecycle hooks) are measured on
**blocked** time: the stretch the hook held the loop without an
``await``. ``await self.offload(...)`` is not blocked time. The loop
subtracts it before it asks here. Lifecycle hooks are measured on
**wall** time, because the lifecycle is waiting for them to finish.

The lines, and they are strict — the plan says 超過, so equal to the
line is still inside it:

* General hook, blocked time, past 1s — :attr:`Disposition.WARN`.
  Warning log, ``HookSlow`` + 1, the hook keeps running.
* General hook, blocked time, past 30s — :attr:`Disposition.CRASH_B`.
  Kill, platform cleanup, failed.
* ``on_start``, wall time, past ``start_timeout_s`` (default 60, cap
  3600) — :attr:`Disposition.INIT_FAILED`. Failed, do not restart.
* ``on_ready``, wall time, past 10s — :attr:`Disposition.INIT_FAILED`.
  Failed, do not restart.
* ``on_stop``, wall time, past 10s (``ON_STOP_TIMEOUT_S``) —
  :attr:`Disposition.STOP_EXPIRED`. Stop waiting, platform cleanup,
  then kill.

The 30s line does not apply to the lifecycle hooks. A 45s ``on_start``
inside a 60s budget is fine; that is the long warm-up F3 allows. A 2s
``on_ready`` is not a warning. The 1s warning is only the general row,
and it exists because a slow hook no longer stalls the ingress — it
only slows the strategy.

None of these outcomes relaunch the session. Relaunch is class A (the
strategy raised, ``on_stop`` ran) and it is the controller's decision
(IF-04, F11), not this table's. This layer also does not restart
itself (I2).

:func:`assess_hook` raises ``NotImplementedError("IF-05")``. The
numbers and :class:`HookBudgetReport` are the format, and they are
live. B5-04 classifies.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum

from mftik.protocol import (
    DEFAULT_START_TIMEOUT_S,
    ON_STOP_TIMEOUT_S,
    StsStatusProgress,
)

#: Blocked time past which a general hook is a warning and a ``HookSlow``.
HOOK_WARN_S = 1.0
#: Blocked time past which a general hook is a class-B crash.
HOOK_HARD_S = 30.0
#: Wall time past which ``on_ready`` is an init failure.
ON_READY_LIMIT_S = 10.0
#: Wall time past which ``on_stop`` is not waited for. The plan's 10s
#: is this constant: the same number ``on_stop`` is already limited by.
ON_STOP_LIMIT_S = ON_STOP_TIMEOUT_S

_LIFECYCLE = frozenset({"on_start", "on_ready", "on_stop"})


class Measure(StrEnum):
    """Which clock the number was taken from."""

    BLOCKED = "blocked"
    WALL = "wall"


class Disposition(StrEnum):
    """What the ingress should do with a measurement. Not a kill by itself.

    The ingress publishes the report and, for the rows that end the
    session, starts the fail path. The controller reads the exit. This
    object does not spawn anything.
    """

    OK = "ok"
    WARN = "warn"
    CRASH_B = "crash_b"
    INIT_FAILED = "init_failed"
    STOP_EXPIRED = "stop_expired"


_ENDS = frozenset(
    {Disposition.CRASH_B, Disposition.INIT_FAILED, Disposition.STOP_EXPIRED}
)


@dataclass(frozen=True)
class HookBudgetReport:
    """The progress line for one hook that is running, or has just been judged.

    ``hook`` and ``elapsed_s`` are what the UI shows ("stuck in
    ``on_ticker`` for 12s"). ``measure`` says which clock that was.
    ``limit_s`` is the line this ``elapsed_s`` was compared to — 1 for
    a warning, 30 for a class-B crash, the start budget, or 10.
    ``disposition`` is the row of the F15 table.

    :meth:`as_progress` is the ``sts.status`` payload IF-01 already
    defined (:class:`~mftik.protocol.messages.StsStatusProgress`).
    ``dropped`` on that payload is the delivery drop count, not a hook
    count. It is passed in because this report doesn't know it.
    """

    hook: str
    measure: Measure
    elapsed_s: float
    limit_s: float
    disposition: Disposition

    @property
    def ends_session(self) -> bool:
        """True when this row kills or fails the session.

        A warning does not. The hook keeps running, and so does the
        session.
        """
        return self.disposition in _ENDS

    @property
    def restarts(self) -> bool:
        """Always false. No row of this table relaunches the session."""
        return False

    def as_progress(self, *, dropped: int = 0) -> StsStatusProgress:
        """The status snapshot's ``progress`` object."""
        return StsStatusProgress(
            hook=self.hook, elapsed_s=self.elapsed_s, dropped=dropped
        )


def assess_hook(
    hook: str,
    elapsed_s: float,
    *,
    start_timeout_s: float = DEFAULT_START_TIMEOUT_S,
) -> HookBudgetReport:
    """Classify one measurement. The F15 table, and nothing else.

    ``hook`` is the lifecycle name or the general hook's name. Anything
    other than ``on_start``, ``on_ready`` and ``on_stop`` is a general
    hook, including a timer callback. There is no ``hook_timeout_s``
    argument: the lines are not configurable.

    ``start_timeout_s`` is the session's, from ``strategy.yml``. The
    cap at 3600 is the parser's (IF-07); this function trusts the
    number it is given.

    A general hook at exactly :data:`HOOK_WARN_S` or exactly
    :data:`HOOK_HARD_S` is not past that line. The same for the
    lifecycle budgets.

    Raises :class:`NotImplementedError` until B5-04. The signature is
    the interface; the table in the module docstring is the contract
    the tests pin.
    """
    del hook, elapsed_s, start_timeout_s
    raise NotImplementedError("IF-05")


def is_lifecycle(hook: str) -> bool:
    """True for the three hooks measured on wall time."""
    return hook in _LIFECYCLE


__all__ = [
    "DEFAULT_START_TIMEOUT_S",
    "HOOK_HARD_S",
    "HOOK_WARN_S",
    "ON_READY_LIMIT_S",
    "ON_STOP_LIMIT_S",
    "ON_STOP_TIMEOUT_S",
    "Disposition",
    "HookBudgetReport",
    "Measure",
    "assess_hook",
    "is_lifecycle",
]
