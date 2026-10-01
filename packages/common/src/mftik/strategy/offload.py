"""Moving a heavy computation out of the strategy's event loop (F9, §5.5).

The ingress thread protects the *platform* from a slow hook: NATS keeps being
read, acks keep being timed against real time, control signals still arrive
(§5.3). It does nothing for the strategy itself. A hook that computes for twenty
seconds has blocked its own fills, its own timers and its own ``on_stop`` for
twenty seconds, and no amount of I/O hygiene elsewhere changes that.

``offload`` is what replaces the old ``breathe`` / ``slice_deadline`` pacing:
instead of cutting a computation into slices that yield, hand the whole
computation somewhere else and ``await`` it. The loop is then genuinely empty —
which is also why hook time budgets measure *blocked* time rather than wall
time, so an ``await self.offload(...)`` inside a hook costs the hook nothing
(F15).

Two modes, and the choice is about the GIL rather than about speed:

thread (default)
    For work that releases the GIL — numpy, torch, anything that is really C.
    Objects already loaded are used directly, nothing is pickled, and the
    computation cannot be interrupted: at stop the ``await`` is cancelled but
    the thread runs to completion.
process (``isolate=True``, or :meth:`OffloadPool.call`)
    For pure-Python work, C extensions that hold the GIL for long stretches,
    and anything whose memory footprint is a risk. Arguments and results are
    pickled, so the function has to be importable at module level. The child
    can be killed, and it is the first thing sacrificed under memory pressure.

:class:`OffloadPool` is the process mode with somewhere to keep state: ``init``
runs once per worker — load a model, open a dataset — and every
:meth:`~OffloadPool.call` is handed its return value as the first argument. That
is the difference between loading a model once and pickling it on every call.

**State authority (§3.3):** a pool's workers belong to the STS session worker.
They are part of the session's process tree, counted against the session's
memory at admission, given ``PDEATHSIG`` so they cannot outlive it, and
terminated at the session's teardown. The SDK hands out a handle; it does not
own the children.

**Invariants:**

* An offloaded function must not call the SDK. The SDK checks the calling
  thread and refuses, because an order placed from a worker would be minting
  ``client_order_id`` sequence numbers outside the strategy thread that owns
  them.
* Inputs are arguments and outputs are return values. A thread-mode function
  that mutates the strategy is racing the strategy's own loop.
* Exceptions come back to the caller as themselves.
* A lost process worker is :class:`~mftik.strategy.errors.OffloadWorkerLost`,
  never a plausible-looking result, and the pool rebuilds on the next call —
  re-running ``init``, so nothing a pool was carrying survives.

IF-06 defines the surface. The pools land in B5-03.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any


class OffloadPool:
    """A process pool with per-worker state, from
    :meth:`~mftik.strategy.base.Strategy.offload_pool`.

    One pool is one ``init``. Call it for work that needs something expensive
    already in memory; use :meth:`~mftik.strategy.base.Strategy.offload` with
    ``isolate=True`` for one-off work that needs nothing kept.
    """

    async def call(self, func: Callable[..., Any], /, *args: Any) -> Any:
        """Run ``func(state, *args)`` in a worker and return its result.

        ``state`` is what this pool's ``init`` returned, so ``func`` takes it
        first and the strategy never pickles it. ``func`` and ``args`` must be
        picklable and ``func`` must be importable at module level — a closure
        or a bound method of the strategy cannot cross a process boundary.

        Raises :class:`~mftik.strategy.errors.OffloadWorkerLost` if the worker
        died while the call was outstanding. Any exception ``func`` itself
        raised is re-raised here as itself.

        Raises :class:`NotImplementedError` until B5-03.
        """
        raise NotImplementedError("IF-06")

    async def close(self) -> None:
        """Shut the pool down and terminate its workers.

        Called for the strategy at teardown, so a strategy does not have to.
        Worth calling by hand for a pool that was only needed for warm-up: the
        workers hold whatever ``init`` loaded until something closes them.
        """
        return None
