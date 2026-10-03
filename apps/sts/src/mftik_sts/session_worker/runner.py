"""The strategy thread: hooks, decode, and the send connection (§5.3, F8).

Second thread, its own loop. It publishes orders on its own connection,
with ``reply`` pointed at the ingress inbox, and it flushes before it
goes back to a long hook — the publish would otherwise sit in the
buffer. The ack comes back on the ingress and is handed over with
``call_soon_threadsafe``. That cross-thread hop is the return path
only. The process entry wires it; this object is the phase walk the
two threads share.

Decode happens on this thread, on the
:class:`~mftik_sts.session_worker.events.Inbound` it pulled. The ingress
never runs the hook (I4).

**Order (I1).** :meth:`StrategyRunner.start` refuses unless the ingress
has already started, and :meth:`Ingress.close` refuses while this
thread is alive. ``on_stop`` therefore still has a receiver.

**Signals (I3).** This thread is not the main thread.
:func:`refuse_strategy_signal_handler` is what the SDK calls if a
strategy tries to install one. Handlers stay on the ingress.

**Code identity (F39).** The runner is handed a strategy instance. It
does not choose which tree or which extras generation that is, and it
does not import the platform registry. The process entry resolves the
instance, in the session process, from whatever path the spawn already
selected.

:meth:`note_hook` still raises ``NotImplementedError("IF-05")``. Hook
classification is B5-04.
"""

from __future__ import annotations

import threading

from mftik.strategy import Strategy

from mftik_sts.session_worker.budget import HookBudgetReport
from mftik_sts.session_worker.errors import (
    IngressEnded,
    IngressNotStarted,
    SignalHandlersReserved,
)
from mftik_sts.session_worker.ingress import Ingress
from mftik_sts.session_worker.phase import Phase


class StrategyRunner:
    """The strategy thread bound to one :class:`Ingress`.

    ``strategy`` is the instance this thread will call. Constructing
    the runner does not import anything and does not start a thread.
    """

    def __init__(self, ingress: Ingress, strategy: Strategy) -> None:
        self.ingress = ingress
        self.strategy = strategy
        self._alive = False
        self._thread: threading.Thread | None = None
        self._done = threading.Event()
        ingress.bind_runner(self)

    @property
    def alive(self) -> bool:
        """True from :meth:`start` until :meth:`finish` or :meth:`kill`."""
        return self._alive

    @property
    def thread(self) -> threading.Thread | None:
        """The strategy thread. Not the main thread. ``None`` until :meth:`start`.

        Called from the main thread, :meth:`start` parks a daemon thread
        so the contract can observe one without a loop. Called from the
        strategy thread, that thread is this one: the process entry does
        not keep a second idle thread beside the loop.
        """
        return self._thread

    def start(self) -> None:
        """Phase 1. The ingress moves to :attr:`Phase.LOAD`.

        Raises :class:`IngressNotStarted` if :meth:`Ingress.start` has
        not run, and :class:`IngressEnded` if the ingress has already
        aborted or closed.
        """
        if self.ingress.ended:
            raise IngressEnded(
                f"ingress for {self.ingress.spec.session_id} has already ended"
            )
        if self.ingress.phase is None:
            raise IngressNotStarted(
                "the strategy thread starts after the ingress (I1)"
            )
        self._alive = True
        if threading.current_thread() is threading.main_thread():
            thread = threading.Thread(
                target=self._park,
                name="mftik-strategy",
                daemon=True,
            )
            thread.start()
            self._thread = thread
        else:
            self._thread = threading.current_thread()
        self.ingress.advance(Phase.LOAD)

    def begin_on_start(self) -> None:
        """Phase 2. ``on_start`` is running; the ingress delivers nothing."""
        self.ingress.advance(Phase.ON_START)

    def end_on_start(self) -> None:
        """``on_start`` returned. Phase 3. The ingress may subscribe TD.

        Delivery stays held. :meth:`Ingress.pull` is still ``None``.
        """
        self.ingress.advance(Phase.READY)

    def begin_on_ready(self) -> None:
        """``on_ready`` is running. Delivery is still held."""
        self.ingress.advance(Phase.READY)

    def end_on_ready(self) -> None:
        """``on_ready`` returned. Phase 4. Delivery starts."""
        self.ingress.advance(Phase.RUNNING)

    def begin_on_stop(self) -> None:
        """``on_stop`` is running inside phase 5. The ingress is still pulling."""
        if self.ingress.phase is not Phase.STOPPING:
            self.ingress.advance(Phase.STOPPING)

    def finish(self) -> None:
        """``on_stop`` returned. This thread is done; the ingress is not (I1)."""
        self._alive = False
        self._done.set()

    def kill(self) -> None:
        """The ingress aborted. This thread does not keep the process up (I2)."""
        self._alive = False
        self._done.set()

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

    def _park(self) -> None:
        """Hold the thread the contract observed until finish or kill."""
        self._done.wait()


def refuse_strategy_signal_handler() -> None:
    """Refuse a strategy-installed signal handler (I3).

    Raised on any thread: the strategy does not get a handler even if it
    asks from the main thread. Handlers run on the main thread, and the
    main thread is the ingress.
    """
    raise SignalHandlersReserved(
        "signal handlers are reserved for the ingress thread"
    )
