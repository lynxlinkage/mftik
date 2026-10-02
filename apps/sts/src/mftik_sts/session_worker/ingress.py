"""The ingress thread: receive, log, queue, heartbeat (§5.3, F8).

It is the main thread (I3). It owns the receive connection — MD atoms,
TD account events, ``sts.ctl.{session_id}``, the reply inbox — and it
does not own the send connection. It does not decode, it does not run
a hook, and it does not write the event log itself (I4). Writes are a
``put_nowait`` onto the writer thread.

**Phases and delivery.** Events may arrive from :attr:`Phase.LOAD` on,
because that is when the MD subscription exists. They are queued
(and conflated, once :class:`~mftik_sts.session_worker.delivery.Delivery`
does that) the whole time. :meth:`Ingress.pull` returns nothing until
:attr:`Phase.RUNNING`, and returns again during :attr:`Phase.STOPPING`
so ``on_stop`` still receives acks and fills (I1). :meth:`offer` during
:attr:`Phase.ON_START` and :attr:`Phase.READY` remembers the event and
:meth:`pull` keeps returning ``None``.

B4-03 implements those phase rules. One held event is enough for its
tests, so it can buffer that event inside :meth:`offer` while
:meth:`Delivery.accept` still raises. B5-01 is what makes ``offer``
delegate to :attr:`delivery`. There is one queue; the temporary buffer
does not survive that delegation.

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

The lifecycle methods raise ``NotImplementedError("IF-05")``.
:meth:`pull`, :meth:`progress` and :meth:`log_records` return null.
"""

from __future__ import annotations

import threading
from collections.abc import Mapping

from mftik.protocol import (
    DEFAULT_START_TIMEOUT_S,
    StsCreateSessionRequest,
    StsStatusProgress,
)

from mftik_sts.session_worker.delivery import Delivery
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

    ``capacity`` is forwarded to :class:`Delivery`. No default: the plan
    doesn't name one.
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

    @property
    def phase(self) -> Phase | None:
        """The stage the ingress is in, or ``None`` before :meth:`start`.

        The stub has not started, so this is ``None``.
        """
        return None

    @property
    def exit_code(self) -> int | None:
        """The process exit, once the ingress has ended. ``None`` until then.

        :meth:`close` is ``0``. :meth:`abort` — the ingress died — is
        non-zero, and the process exits. It does not rebuild.
        """
        return None

    @property
    def thread(self) -> threading.Thread | None:
        """The thread :meth:`start` ran on. ``None`` until it has.

        After :meth:`start` this is the main thread (I3).
        """
        return None

    def start(self) -> None:
        """Phase 0. Main thread only.

        Opens the receive connection, subscribes ``sts.ctl.{session_id}``,
        starts the shim heartbeat. Does not start the strategy thread.

        Off the main thread, B4-03 raises
        :class:`~mftik_sts.session_worker.errors.IngressNotMainThread`.
        A second call after :meth:`abort` or :meth:`close` raises
        :class:`~mftik_sts.session_worker.errors.IngressEnded`.

        Raises :class:`NotImplementedError` until B4-03.
        """
        raise NotImplementedError("IF-05")

    def offer(self, event: Inbound) -> None:
        """Receive one event: log it, then queue it under the phase rules.

        Does not decode ``event.body`` and does not call a strategy
        hook. Does not write a file; the line goes to
        :meth:`log_records` for the writer thread.

        Raises :class:`NotImplementedError` until B4-03. The queue it
        eventually calls is :meth:`Delivery.accept` (B5-01).
        """
        del event
        raise NotImplementedError("IF-05")

    def pull(self) -> Inbound | None:
        """The next event the strategy thread may decode, or ``None``.

        ``None`` while the phase is holding delivery (``on_start``,
        ``on_ready``, and everything before them), when the queue is
        empty, and always on this stub.
        """
        return None

    def stop(self) -> None:
        """Phase 5. A control signal or SIGTERM.

        The strategy thread is still alive and runs ``on_stop``.
        :meth:`pull` keeps returning acks and fills until that returns.

        Raises :class:`NotImplementedError` until B4-03.
        """
        raise NotImplementedError("IF-05")

    def close(self) -> None:
        """Phase 6. The strategy thread has already finished (I1).

        Flushes the event log, drains NATS, exits 0. If the strategy
        thread is still alive, B4-03 raises
        :class:`~mftik_sts.session_worker.errors.StrategyStillRunning`
        instead of closing.

        Raises :class:`NotImplementedError` until B4-03.
        """
        raise NotImplementedError("IF-05")

    def abort(self) -> None:
        """The ingress died. Fail-fast, non-zero, no in-process restart (I2).

        The strategy thread does not keep running, and :meth:`start`
        does not succeed again on this object. A new incarnation is a
        new process.

        Raises :class:`NotImplementedError` until B4-03.
        """
        raise NotImplementedError("IF-05")

    def progress(self) -> StsStatusProgress | None:
        """The hook report on the status snapshot, or ``None``.

        The shape is :class:`~mftik_sts.session_worker.budget.HookBudgetReport`
        rendered with :meth:`~HookBudgetReport.as_progress`. ``dropped``
        on it is :attr:`Delivery.dropped`. The stub has no measurement,
        so this is ``None``.
        """
        return None

    def log_records(self) -> tuple[LogRecord, ...]:
        """Lines queued for the writer. Empty on the stub, and never a file write."""
        return ()
