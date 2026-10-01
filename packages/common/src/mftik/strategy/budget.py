"""Hooks that held the strategy loop too long, counted (F15, §5.3).

A general hook is measured on **blocked** time: the stretch it occupies the
strategy's loop without awaiting anything. Past the warning line it is counted
here and a warning is logged; past the hard line it is a class-B crash and the
session is killed, cleaned up after and failed, because a loop that has been
stuck for that long cannot even cancel its own orders. The lifecycle hooks are
measured on wall time instead — the lifecycle is waiting for them to finish, so
whether they awaited something is beside the point.

Nothing about this is configurable from ``strategy.yml``, by decision: a
strategy cannot raise its own ceiling.

:class:`HookSlow` is the warning side — the count, not the kill. It exists so
that "this strategy is slow" is a number somebody can see rather than a pile of
log lines: the ingress reads it into the session's status progress, which is
what the UI and the board show.

**State authority (§3.3):** the measurement belongs to the strategy loop, which
is the only thing that can see its own blocking; the ingress reads the figure
and publishes it on ``sts.status.{session_id}``. This counter is that figure's
in-process home, not a second opinion about it.

**Invariants:**

* Counting is not killing. Passing the warning line changes nothing about
  whether the hook keeps running or the session keeps living.
* ``await self.offload(...)`` is not blocking. A hook that offloads its heavy
  work is not counted here however long it takes in wall time (§5.5).
* The thresholds themselves are the session worker's (IF-05), not the SDK's.
  This counter does not decide where the line is.

IF-06 defines the counter. The measurement that feeds it lands in B5-04.
"""

from __future__ import annotations

from collections.abc import Mapping


class HookSlow:
    """How many times each hook has blocked the loop past the warning line.

    Per hook rather than one total, because the useful question is which hook —
    a strategy with one slow ``on_ticker`` and a fast everything else is a
    different problem from one that is uniformly late.
    """

    @property
    def count(self) -> int:
        """Warnings across every hook. Zero on a strategy that is never late."""
        return 0

    def by_hook(self) -> Mapping[str, int]:
        """Hook name → how many times it was late. Empty when nothing was."""
        return {}

    def note(self, hook: str, blocked_s: float) -> None:
        """Record one hook that blocked for ``blocked_s`` seconds.

        Called by the strategy loop, not by strategies.

        Raises :class:`NotImplementedError` until B5-04.
        """
        raise NotImplementedError("IF-06")
