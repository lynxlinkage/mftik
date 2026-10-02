"""The strategy thread: hooks, decode, and the send connection (§5.3, F8).

Second thread, its own loop. It publishes orders on its own
connection, with ``reply`` pointed at the ingress inbox, and it
flushes before it goes back to a long hook — the publish would
otherwise sit in the buffer. The ack comes back on the ingress and
is handed over with ``call_soon_threadsafe``. That cross-thread hop
is the return path only. None of it is built here.

Decode happens on this thread, on the :class:`~mftik_sts.session_worker.events.Inbound`
it pulled. The ingress never runs the hook (I4).

**Order (I1).** :meth:`StrategyRunner.start` refuses unless the ingress
has already started, and :meth:`Ingress.close` refuses while this
thread is alive. ``on_stop`` therefore still has a receiver.

**Signals (I3).** This thread is not the main thread.
:func:`refuse_strategy_signal_handler` is what the SDK calls if a
strategy tries to install one. Handlers stay on the ingress.

**Code identity (F39).** The runner imports the strategy it was given.
It does not choose which tree or which extras generation that is, and
it does not import the platform registry. The controller checked
deployability without importing; the import happens here, in the
session process, from whatever path the spawn already selected.

The methods raise ``NotImplementedError("IF-05")``. :attr:`alive` is
false and :attr:`thread` is ``None`` until B4-03.
"""

from __future__ import annotations

import threading

from mftik.strategy import Strategy

from mftik_sts.session_worker.budget import HookBudgetReport
from mftik_sts.session_worker.ingress import Ingress


class StrategyRunner:
    """The strategy thread bound to one :class:`Ingress`.

    ``strategy`` is the instance this thread will call. Constructing
    the runner does not import anything and does not start a thread.
    """

    def __init__(self, ingress: Ingress, strategy: Strategy) -> None:
        self.ingress = ingress
        self.strategy = strategy

    @property
    def alive(self) -> bool:
        """True from :meth:`start` until :meth:`finish`. False on the stub."""
        return False

    @property
    def thread(self) -> threading.Thread | None:
        """The strategy thread. Not the main thread. ``None`` until :meth:`start`."""
        return None

    def start(self) -> None:
        """Phase 1. Open the send connection and import the strategy.

        The ingress moves to :attr:`~mftik_sts.session_worker.phase.Phase.LOAD`.
        B4-03 raises
        :class:`~mftik_sts.session_worker.errors.IngressNotStarted` if
        :meth:`Ingress.start` has not run, and
        :class:`~mftik_sts.session_worker.errors.IngressEnded` if the
        ingress has already aborted.

        Raises :class:`NotImplementedError` until B4-03.
        """
        raise NotImplementedError("IF-05")

    def begin_on_start(self) -> None:
        """Phase 2. ``on_start`` is running; the ingress delivers nothing."""
        raise NotImplementedError("IF-05")

    def end_on_start(self) -> None:
        """``on_start`` returned. The ingress subscribes TD (phase 3)."""
        raise NotImplementedError("IF-05")

    def begin_on_ready(self) -> None:
        """``on_ready`` is running. Delivery is still held."""
        raise NotImplementedError("IF-05")

    def end_on_ready(self) -> None:
        """``on_ready`` returned. Phase 4. Delivery starts."""
        raise NotImplementedError("IF-05")

    def begin_on_stop(self) -> None:
        """``on_stop`` is running inside phase 5. The ingress is still pulling."""
        raise NotImplementedError("IF-05")

    def finish(self) -> None:
        """``on_stop`` returned. This thread is done; the ingress is not (I1)."""
        raise NotImplementedError("IF-05")

    def note_hook(self, hook: str, elapsed_s: float) -> HookBudgetReport:
        """Hand the loop's measurement to the ingress for the status progress.

        ``elapsed_s`` is blocked time for a general hook and wall time
        for a lifecycle hook. The classification is
        :func:`mftik_sts.session_worker.budget.assess_hook`, using this
        ingress's ``start_timeout_s``. The last report is what
        :meth:`Ingress.progress` publishes.

        Raises :class:`NotImplementedError` until B5-04.
        """
        del hook, elapsed_s
        raise NotImplementedError("IF-05")


def refuse_strategy_signal_handler() -> None:
    """Refuse a strategy-installed signal handler (I3).

    B4-03 raises
    :class:`~mftik_sts.session_worker.errors.SignalHandlersReserved`
    from here, on any thread: the strategy does not get a handler even
    if it asks from the main thread. The stub raises
    :class:`NotImplementedError`.
    """
    raise NotImplementedError("IF-05")
