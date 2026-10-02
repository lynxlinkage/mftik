"""The ingress thread: receive, log, queue, heartbeat (§5.3, F8).

It is the main thread (I3). It owns the receive connection — MD atoms,
TD account events, ``sts.ctl.{session_id}``, the reply inbox — and it
does not own the send connection. It does not decode, it does not run
a hook, and it does not write the event log itself (I4). Writes are a
``put_nowait`` onto the writer thread. The file writer is B5-02;
:meth:`Ingress.log_records` is the handoff.

**Phases and delivery.** Events may arrive from :attr:`Phase.LOAD` on,
because that is when the MD subscription exists. They are queued the
whole time. :meth:`Ingress.pull` returns nothing until
:attr:`Phase.RUNNING`, and returns again during :attr:`Phase.STOPPING`
so ``on_stop`` still receives acks and fills (I1). :meth:`offer` during
:attr:`Phase.ON_START` and :attr:`Phase.READY` remembers the event and
:meth:`pull` keeps returning ``None``.

B4-03 keeps that queue inside :meth:`offer`. The bound is
:attr:`Delivery.capacity`. Past it, the oldest event is dropped. There
is no drop count and the session does not fail: a must-deliver overflow
fails the session only once :meth:`Delivery.accept` owns the queue
(B5-01). There is one queue; the temporary buffer does not survive that
delegation.

**What this object is the authority for (§3.3), once it runs:**

* the event log (the file under ``STS_EVENTLOG_DIR``; unset means the
  writer is off and the ingress still runs — B5-02)
* hook progress, offload progress, and the delivery drop count,
  published as ``sts.status.{session_id}`` progress

It is not the authority for the session row (the controller's
Supervisor writes that), for market data (the MD connection worker),
for the OMS (the TD account worker), or for which code the process
was started from (F39 — the spec it is handed already names the
session, and this object does not resolve a digest).

:meth:`progress` stays ``None`` until a hook report exists.
:meth:`StrategyRunner.note_hook` is what produces one, and that is
B5-04.
"""

from __future__ import annotations

import threading
from collections import deque
from collections.abc import Callable, Mapping

from mftik.protocol import (
    DEFAULT_START_TIMEOUT_S,
    StsCreateSessionRequest,
    StsStatusProgress,
)

from mftik_sts.session_worker.delivery import Delivery
from mftik_sts.session_worker.errors import (
    IngressEnded,
    IngressNotMainThread,
    StrategyStillRunning,
)
from mftik_sts.session_worker.events import Inbound, LogRecord
from mftik_sts.session_worker.phase import Phase


class Ingress:
    """One session's receive side. Born with the process, dies with it (I2).

    ``spec`` is the session the controller handed over. There is no
    ``SessionSpec`` type on this branch (IF-14). :class:`StsCreateSessionRequest`
    is the body IF-01 already calls the session spec: strategy, accounts,
    feeds, restart. It has no ``strategy_digest`` and no ``env_generation``
    (those are IF-16, F39). This constructor does not grow them.

    ``delivery_overrides`` is the parsed ``md_delivery`` map, not the
    yaml text. ``start_timeout_s`` is the parsed ``on_start`` budget.
    Parsing ``strategy.yml`` stays where IF-07 put it; the worker is
    given the result so it does not open the registry to find the file.

    ``capacity`` is forwarded to :class:`Delivery` and is also the bound
    of the temporary buffer :meth:`offer` keeps until B5-01. No default:
    the plan doesn't name one.
    """

    def __init__(
        self,
        spec: StsCreateSessionRequest,
        *,
        capacity: int,
        delivery_overrides: Mapping[str, str] | None = None,
        start_timeout_s: float = DEFAULT_START_TIMEOUT_S,
    ) -> None:
        self.spec = spec
        self.start_timeout_s = start_timeout_s
        self.delivery = Delivery(capacity=capacity, overrides=delivery_overrides)
        self._phase: Phase | None = None
        self._exit_code: int | None = None
        self._thread: threading.Thread | None = None
        self._ended = False
        self._runner: object | None = None
        self._held: deque[Inbound] = deque()
        self._ready: deque[Inbound] = deque()
        self._logs: list[LogRecord] = []
        self._log_seq = 0
        self._lock = threading.Lock()
        self._on_queued: Callable[[], None] | None = None

    def bind_runner(self, runner: object) -> None:
        """The strategy thread :meth:`close` and :meth:`abort` have to see."""
        self._runner = runner

    def set_queued_callback(self, callback: Callable[[], None] | None) -> None:
        """Called, outside the lock, when :meth:`pull` may have a new event.

        The process uses this to wake the strategy loop. The contract
        tests never set it.
        """
        self._on_queued = callback

    @property
    def phase(self) -> Phase | None:
        """The stage the ingress is in, or ``None`` before :meth:`start`."""
        return self._phase

    @property
    def exit_code(self) -> int | None:
        """The process exit, once the ingress has ended. ``None`` until then.

        :meth:`close` is ``0`` when nothing has already chosen a code.
        :meth:`abort` — the ingress died — is non-zero, and the process
        exits. It does not rebuild.
        """
        return self._exit_code

    @property
    def thread(self) -> threading.Thread | None:
        """The thread :meth:`start` ran on. ``None`` until it has.

        After :meth:`start` this is the main thread (I3).
        """
        return self._thread

    @property
    def ended(self) -> bool:
        """True after :meth:`abort` or :meth:`close`. The walk does not restart."""
        return self._ended

    def start(self) -> None:
        """Phase 0. Main thread only.

        Does not open a socket. The process entry subscribes
        ``sts.ctl.{session_id}`` and starts the shim heartbeat after
        this returns. Off the main thread this raises
        :class:`IngressNotMainThread`. A second call, or a call after
        :meth:`abort` or :meth:`close`, raises :class:`IngressEnded`.
        """
        if threading.current_thread() is not threading.main_thread():
            raise IngressNotMainThread(
                "Ingress.start runs on the main thread; signal handlers do"
            )
        with self._lock:
            if self._ended or self._phase is not None:
                raise IngressEnded(
                    f"ingress for {self.spec.session_id} has already started"
                )
            self._thread = threading.current_thread()
            self._phase = Phase.BOOT

    def offer(self, event: Inbound) -> None:
        """Receive one event: log it, then queue it under the phase rules.

        Does not decode ``event.body`` and does not call a strategy hook.
        Does not write a file; the line goes to :meth:`log_records` for
        the writer thread. Before :attr:`Phase.RUNNING` the event is
        held. :meth:`pull` returns it once :attr:`Phase.RUNNING` begins,
        including when that event arrived during ``on_start`` or
        ``on_ready``. During :attr:`Phase.STOPPING` it is queued at once.

        The temporary buffer drops the oldest event past
        :attr:`Delivery.capacity`. It does not call :meth:`Delivery.accept`.
        """
        notify = False
        with self._lock:
            self._remember(event)
            if self._phase in (Phase.RUNNING, Phase.STOPPING):
                self._enqueue(event)
                notify = True
            else:
                self._hold(event)
        if notify:
            self._kick()

    def log_only(self, event: Inbound) -> None:
        """Record ``event`` without queueing it for :meth:`pull`.

        An order ack is completed through the pending table. Offering it
        as well would hand the strategy the same reply twice.
        """
        with self._lock:
            self._remember(event)

    def pull(self) -> Inbound | None:
        """The next event the strategy thread may decode, or ``None``.

        ``None`` while the phase is holding delivery (``on_start``,
        ``on_ready``, and everything before them) and when the queue is
        empty.
        """
        with self._lock:
            if self._phase not in (Phase.RUNNING, Phase.STOPPING):
                return None
            if not self._ready:
                return None
            return self._ready.popleft()

    def advance(self, phase: Phase) -> None:
        """Move to ``phase``. Entering :attr:`Phase.RUNNING` releases held events."""
        notify = False
        with self._lock:
            if self._ended:
                raise IngressEnded(
                    f"ingress for {self.spec.session_id} has already ended"
                )
            self._phase = phase
            if phase is Phase.RUNNING:
                self._release_held()
                notify = True
        if notify:
            self._kick()

    def stop(self) -> None:
        """Phase 5. A control signal or SIGTERM.

        The strategy thread is still alive and runs ``on_stop``.
        :meth:`pull` keeps returning acks and fills until that returns.
        """
        with self._lock:
            if self._ended:
                raise IngressEnded(
                    f"ingress for {self.spec.session_id} has already ended"
                )
            self._phase = Phase.STOPPING

    def close(self) -> None:
        """Phase 6. The strategy thread has already finished (I1).

        If the strategy thread is still alive, raises
        :class:`StrategyStillRunning` instead of closing. The exit code
        is ``0`` unless :meth:`abort` or :meth:`set_exit_code` already
        chose one. Draining NATS is the process entry's job; this object
        does not hold a socket.
        """
        runner = self._runner
        alive = bool(getattr(runner, "alive", False)) if runner is not None else False
        if alive:
            raise StrategyStillRunning(
                "Ingress.close waits until the strategy thread has finished"
            )
        with self._lock:
            self._phase = Phase.TEARDOWN
            if self._exit_code is None:
                self._exit_code = 0
            self._ended = True

    def abort(self) -> None:
        """The ingress died. Fail-fast, non-zero, no in-process restart (I2).

        The strategy thread does not keep running, and :meth:`start` does
        not succeed again on this object. A new incarnation is a new
        process. The phase stays where it was: a dead ingress is not
        walked back to :attr:`Phase.BOOT`.
        """
        with self._lock:
            if self._exit_code in (None, 0):
                self._exit_code = 1
            self._ended = True
        runner = self._runner
        kill = getattr(runner, "kill", None) if runner is not None else None
        if kill is not None:
            kill()

    def set_exit_code(self, code: int) -> None:
        """Record ``code`` if nothing has recorded one yet.

        :meth:`close` keeps a code that is already set, so a failed
        session still tears down through phase 6 and does not turn into
        a zero exit.
        """
        with self._lock:
            if self._exit_code is None:
                self._exit_code = code

    def progress(self) -> StsStatusProgress | None:
        """The hook report on the status snapshot, or ``None``.

        The shape is :class:`~mftik_sts.session_worker.budget.HookBudgetReport`
        rendered with :meth:`~HookBudgetReport.as_progress`. ``dropped``
        on it is :attr:`Delivery.dropped`. Nothing measures a hook until
        B5-04, so this stays ``None``.
        """
        return None

    def log_records(self) -> tuple[LogRecord, ...]:
        """Lines queued for the writer. Never a file write."""
        with self._lock:
            return tuple(self._logs)

    def _remember(self, event: Inbound) -> None:
        self._log_seq += 1
        self._logs.append(
            LogRecord(
                event_id=event.event_id,
                log_seq=self._log_seq,
                recv_ts=event.recv_ts,
                body=event.body,
                kind=event.kind,
                feed=event.feed,
                event_seq=event.seq,
                bar_open=event.bar_open,
            )
        )

    def _hold(self, event: Inbound) -> None:
        self._held.append(event)
        self._trim(self._held)

    def _enqueue(self, event: Inbound) -> None:
        self._ready.append(event)
        self._trim(self._ready)

    def _release_held(self) -> None:
        while self._held:
            self._enqueue(self._held.popleft())

    def _trim(self, buf: deque[Inbound]) -> None:
        limit = self.delivery.capacity
        while len(buf) > limit:
            buf.popleft()

    def _kick(self) -> None:
        callback = self._on_queued
        if callback is not None:
            callback()
