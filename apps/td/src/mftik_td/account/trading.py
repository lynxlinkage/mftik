"""The trading layer: private connections, OMS, ledger, recon (F35).

On while some session holds a TdIntent for this account, off the moment
the last one is gone. No linger. The resident layer is not part of that
switch (T1, R2).

**State authority (§3.3).**

* The OMS and the ledger (pre-locks, available) live here, in memory.
  The exchange is the authority for resting orders, positions and
  balances. After a restart this layer rebuilds them with ``reconcile``
  and the worker publishes ``td.account.reset`` (F13).
* Observed open or closed is this object's. Desired open or closed is
  the TD controller's, level-triggered from intent (IF-12), delivered
  as ``td.account.trading`` on ``td.account.{api_id}``. When the
  controller is absent this layer keeps the last desired: silence is
  not a :meth:`deactivate` (P5). This class does not watch the
  controller. A fresh worker starts off and waits for the first push.

**Invariants.**

* **T1.** :meth:`activate` and :meth:`deactivate` do not touch the
  resident layer's pool or its keepalive hook.
* **T2.** The last intent disappearing deactivates immediately. There
  is no grace period.
* **T3.** The private websocket, the OMS, the ledger, recon, the
  leverage cache and the subscription on ``td.order.{api_id}`` exist
  only while :attr:`active`. Orders are accepted only after
  :meth:`Session.start` returns. ``TdReady`` on the order path is that
  gate: while this layer is off, :meth:`OrderHandler._offline` refuses
  ``TD_VENUE_NOT_CONNECTED``.
* **T4.** This object does not spawn a second incarnation. At-most-one
  is the supervisor's pid fence (F36, IF-12). A trading layer assumes
  it is the only one for its ``api_id``.

F11's ``restarting`` window does not drop the intent (R4 of that
section), so a strategy restart does not flap this layer. That rule
belongs to the STS controller; this class only sees the bit the TD
controller pushes.

:meth:`Session.destroy` is one-shot. The next :meth:`activate` builds
a new session from :meth:`set_factory`. Deactivate does not cancel
resting orders and does not flatten (C4). Crash cleanup is B5-06
calling ``cancel_session``.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable
from decimal import Decimal
from typing import TYPE_CHECKING

from mftik.broker.handler import Reply
from mftik.clock import Clock, SystemClock
from mftik.protocol import (
    TD_ACCOUNT_TRADING,
    TD_TRADING_DRAIN,
    Envelope,
    TdAccountTrading,
    TdCancelSessionResult,
    TdTradingDrain,
    TdTradingDrainResult,
    UntypedEnvelope,
)
from pydantic import ValidationError

from mftik_td.account._ticket import TICKET
from mftik_td.account.handlers import WAIT_TIMEOUT_S, _error
from mftik_td.account.resident import ResidentLayer
from mftik_td.controller.defaults import DRAIN_TIMEOUT_S, QUIESCE_LEASE_S
from mftik_td.oms import Ledger, Oms

if TYPE_CHECKING:
    from mftik_td.account.handlers import OrderHandler
    from mftik_td.account.session import Session, TradingConnector

logger = logging.getLogger(__name__)

#: Builds the next :class:`~mftik_td.account.session.Session`.
#:
#: :meth:`Session.destroy` cannot be started again, so every activate
#: after the first one that was destroyed calls this. The callable
#: closes over the resident pool's ``client_for``; it does not open a
#: second HTTP pool.
SessionBuilder = Callable[[], Awaitable["Session"]]


class TradingLayer:
    """The half of an account worker that follows intent (F35).

    ``oms`` and ``ledger`` start empty, or as the session's book when
    the caller passes one. That empty book is not a reconcile.
    ``private`` is the venue connector. The connector protocol is the
    one :class:`~mftik_td.account.session.Session` already consumes, not
    a second trading interface.

    The worker serves ``td.order.{api_id}`` for the life of the
    process. This layer only opens and closes the book. While it is
    off, submits and cancels are refused. A repeated desired ``true``
    while the layer is already up does not call :meth:`Session.start`
    again.
    """

    def __init__(
        self,
        resident: ResidentLayer,
        *,
        oms: Oms | None = None,
        ledger: Ledger | None = None,
        private: TradingConnector | None = None,
        session: Session | None = None,
        clock: Clock | None = None,
        drain_timeout_s: float = WAIT_TIMEOUT_S,
        replace_timeout_s: float = DRAIN_TIMEOUT_S,
        quiesce_lease_s: float = QUIESCE_LEASE_S,
    ) -> None:
        self.resident = resident
        self._clock: Clock = clock if clock is not None else SystemClock()
        #: How long :meth:`deactivate` waits for calls already inside
        #: the order handler. The plan does not name it.
        #: ``cancel_session`` waits :data:`WAIT_TIMEOUT_S`, so a shorter
        #: drain would destroy the book under that call. Provisional.
        self.drain_timeout_s = drain_timeout_s
        #: How long :meth:`drain_for_replace` waits. Not a deactivate:
        #: the book stays up either way. Provisional, the same number as
        #: :data:`DRAIN_TIMEOUT_S` (issue #286).
        self.replace_timeout_s = replace_timeout_s
        #: How long a quiesced layer waits to be stopped (issue #286).
        #:
        #: The controller should ``STOP`` inside this window. If it
        #: does not — the reply was lost, or the controller restarted —
        #: the layer resumes so cancels are possible again.
        self.quiesce_lease_s = quiesce_lease_s
        self._session = session
        self._factory: SessionBuilder | None = None
        if session is not None:
            self.oms = session.oms
            self.ledger = session.ledger
            self.private = session.private
        else:
            self.oms = oms if oms is not None else Oms()
            self.ledger = ledger if ledger is not None else Ledger()
            self.private = private
        #: Leverage figures, keyed by universal ticker. Empty until a
        #: lookup fills them. The cache is this layer's, so it goes
        #: away on :meth:`deactivate` and is not part of the resident
        #: pool.
        self.leverage: dict[str, Decimal] = {}
        self._active = False
        self._busy = 0
        self._idle = asyncio.Event()
        self._idle.set()
        self._gate = asyncio.Lock()
        self._armed = False
        #: True from the moment destroy is decided until it returns.
        #: A new ``cancel_session`` in that window is not started: the
        #: book is about to go away, and the call would race it.
        self._closing = False
        #: Drain-replace (F27). New submits are refused. Cancels still
        #: run until :attr:`_quiesced`, which is set only when nothing
        #: is in flight. A timeout clears both and service resumes.
        #: This is not :meth:`deactivate`: the book is not destroyed.
        self._draining = False
        self._quiesced = False
        #: Bumped whenever the quiesce ends or a new lease is armed.
        #: A lease task that wakes with a stale epoch does not resume.
        self._quiesce_epoch = 0
        self._lease: asyncio.Task[None] | None = None
        self._incarnation: int | None = None

    @property
    def session(self) -> Session | None:
        """The bound book, or ``None`` until :meth:`adopt` or activate."""
        return self._session

    @property
    def active(self) -> bool:
        """Whether the private book is up and accepting orders.

        False until :meth:`activate` finishes :meth:`Session.start`.
        False again after :meth:`deactivate` destroys it. A drain that
        times out leaves this true: the book stays up under the call
        that is still inside the handler.
        """
        return self._active

    @property
    def draining(self) -> bool:
        """Whether new submits are refused ``TD_DRAINING`` (F27).

        True from :meth:`drain_for_replace` until the wait finishes,
        times out, the controller aborts, or the quiesce lease expires.
        Cancels are still served while this is true and
        :attr:`refusing_cancels` is false.
        """
        return self._draining

    @property
    def refusing_cancels(self) -> bool:
        """Whether cancels are refused because nothing is left in flight.

        Set in the same lock acquisition that sees the busy count hit
        zero. Until then a cancel already inside, or one that arrives,
        is served.
        """
        return self._quiesced

    def note_incarnation(self, incarnation: int) -> None:
        """Remember which process this layer is, and log it off.

        The worker calls this once, at construction, before the trading
        layer is opened. ``TdReady`` at this step is false. The account
        state broadcast is B6-06; this log is the worker-local step.
        """
        self._incarnation = incarnation
        logger.info(
            "td ready api_id=%s incarnation=%s active=false",
            self.resident.api_id,
            incarnation,
        )

    def set_factory(self, factory: SessionBuilder) -> None:
        """Where the next session comes from after :meth:`Session.destroy`.

        The first session may already be bound with :meth:`adopt`.
        Destroy is one-shot, so a later :meth:`activate` calls
        ``factory`` instead of starting that same object again.
        """
        self._factory = factory

    def adopt(self, session: Session) -> None:
        """Bind a session that has not been started.

        The worker does this once the resident pool exists, and leaves
        the layer off until the controller pushes ``active``. Adopting
        while the layer is up would swap the book under a live order.
        """
        if self._active:
            raise RuntimeError(
                "cannot adopt a session while the trading layer is active"
            )
        self._bind(session)

    def arm(self, handler: OrderHandler) -> None:
        """Count submits, cancels and ``cancel_session`` already inside.

        The order subject and the account subject are different serve
        loops, so a trading push can arrive while an order call is in
        flight. :meth:`deactivate` has to wait for those calls. It
        cannot wait on the order task: that task is the serve loop and
        does not finish. Wrapping the three methods is the counter.
        A submit or cancel that arrives after the layer has stopped
        accepting is not counted. :meth:`OrderHandler._offline` refuses
        it with ``TD_VENUE_NOT_CONNECTED``. ``cancel_session`` does not
        use that check, and the book can be live while this layer is
        still off (the handler tests call it that way). Every
        ``cancel_session`` is counted, including one that arrives
        during the drain, so :meth:`deactivate` does not destroy under
        it. Once destroy has been decided, or the book is already
        destroyed, the call is not made. The result is ``ok=False``
        and names no order. The result type has no reject code; that
        shape is what this ticket can return without editing the
        dispatcher.
        """
        if self._armed:
            return
        self._armed = True
        submit = handler.submit
        cancel = handler.cancel
        cancel_session = handler.cancel_session

        async def tracked_submit(request):  # type: ignore[no-untyped-def]
            tracked = await self._enter(kind="submit")
            try:
                return await submit(request)
            finally:
                if tracked:
                    await self._leave()

        async def tracked_cancel(request):  # type: ignore[no-untyped-def]
            tracked = await self._enter(kind="cancel")
            try:
                return await cancel(request)
            finally:
                if tracked:
                    await self._leave()

        async def tracked_cancel_session(request, *args, **kwargs):  # type: ignore[no-untyped-def]
            # Always counted. ``_offline`` does not cover this call, so
            # an uncounted one can overlap destroy. No book still falls
            # through: the handler raises, which the surface test locks.
            tracked = await self._enter(force=True)
            if not tracked:
                session_id = getattr(request, "session_id", "")
                logger.warning(
                    "trading layer off api_id=%s refusing cancel_session session_id=%s",
                    self.resident.api_id,
                    session_id,
                )
                return TdCancelSessionResult(
                    session_id=str(session_id),
                    ok=False,
                    unconfirmed=[],
                )
            try:
                return await cancel_session(request, *args, **kwargs)
            finally:
                await self._leave()

        handler.submit = tracked_submit  # type: ignore[method-assign]
        handler.cancel = tracked_cancel  # type: ignore[method-assign]
        handler.cancel_session = tracked_cancel_session  # type: ignore[method-assign]

    async def handle(self, message: UntypedEnvelope) -> Reply | None:
        """Apply one ``td.account.trading`` push and ack the observed bit.

        Level-triggered. A repeated desired value is a no-op when the
        layer is already there. The ack reports what this layer is,
        which is ``false`` when activate failed and ``true`` when a
        drain timed out and the book was left up. A bad payload is
        ``invalid_payload`` and does not change the bit.
        """
        try:
            request = TdAccountTrading.model_validate(message.payload or {})
        except ValidationError as exc:
            return _error(message, "invalid_payload", str(exc))
        if request.api_id != self.resident.api_id:
            return _error(
                message,
                "invalid_payload",
                f"api_id {request.api_id} is not this worker",
            )
        try:
            if request.active:
                await self.activate()
            else:
                await self.deactivate()
        except NotImplementedError:
            return _error(message, "not_implemented", TICKET)
        except Exception as exc:
            logger.exception(
                "trading layer switch failed api_id=%s active=%s",
                self.resident.api_id,
                request.active,
            )
            return _error(message, "trading_switch_failed", str(exc))
        return Envelope[TdAccountTrading].wrap(
            TdAccountTrading(api_id=self.resident.api_id, active=self._active),
            type=TD_ACCOUNT_TRADING,
            source="td",
            session_id=message.session_id,
        )

    async def activate(self) -> None:
        """Open the private book. Does not touch the resident layer (T1).

        With a session, this is :meth:`Session.start`: connect, recon,
        the OMS and the ledger. ``active`` becomes true only after that
        returns, so an order cannot land before recon (T3). A session
        :meth:`Session.destroy` already ran is not started again; the
        factory builds the next one. A start that raises is destroyed
        and dropped, so the retry is a new session rather than a second
        :meth:`Session.start` that returns without reconciling.

        A layer that is already active returns without calling start.
        Without a session and without a factory this still raises
        ``NotImplementedError("IF-11")``.
        """
        if self._active:
            return
        session = self._session
        if session is not None and bool(getattr(session, "destroyed", False)):
            session = None
            self._session = None
        if session is None:
            if self._factory is None:
                raise NotImplementedError(TICKET)
            session = await self._factory()
            self._bind(session)
        try:
            await session.start()
        except BaseException:
            await self._abandon(session)
            raise
        self._bind(session)
        self._active = True
        logger.info(
            "td ready api_id=%s incarnation=%s active=true",
            self.resident.api_id,
            self._incarnation,
        )

    async def drain_for_replace(self) -> bool:
        """Refuse new submits and wait for in-flight calls (F27).

        Does not deactivate and does not destroy the book. When the
        busy count hits zero, cancels are refused too and this returns
        true: nothing is in flight, so the process may be stopped.
        On timeout both flags are cleared and this returns false. The
        worker keeps serving. A second call after this one already
        quiesced returns true without waiting again, and does not
        restart the quiesce lease.

        Quiesce arms :data:`QUIESCE_LEASE_S`. The process is expected
        to exit inside that window. If it is still up, the lease
        clears both flags and service resumes. :meth:`resume_after_drain`
        and a stop that has set :attr:`_closing` cancel the lease.

        The account subject is served one message at a time, so this
        is not entered twice concurrently.
        """
        async with self._gate:
            if self._draining and self._quiesced:
                return True
            self._draining = True
            self._quiesced = False
            # A lease from an older quiesce must not clear this wait.
            self._quiesce_epoch += 1
            self._cancel_quiesce_lease_locked()
        deadline = self._clock.monotonic() + self.replace_timeout_s
        while True:
            async with self._gate:
                if self._busy == 0:
                    self._quiesced = True
                    self._arm_quiesce_lease_locked()
                    logger.info(
                        "trading layer drained api_id=%s",
                        self.resident.api_id,
                    )
                    return True
                if self._clock.monotonic() >= deadline:
                    self._end_quiesce_locked()
                    logger.info(
                        "trading layer drain aborted api_id=%s; resuming",
                        self.resident.api_id,
                    )
                    return False
            remaining = deadline - self._clock.monotonic()
            if remaining <= 0:
                continue
            await self._pause(remaining)

    async def resume_after_drain(self) -> None:
        """Clear a drain that the controller could not follow with a stop.

        The success path leaves the layer refusing, because the process
        is about to exit. If the stop did not happen, the old worker
        has to accept again. A timeout already cleared the flags inside
        :meth:`drain_for_replace`. This also cancels the quiesce lease,
        so a late expiry does not race a newer drain.
        """
        async with self._gate:
            self._end_quiesce_locked()
        logger.info(
            "trading layer drain aborted api_id=%s; resuming",
            self.resident.api_id,
        )

    def _arm_quiesce_lease_locked(self) -> None:
        """Start the resume lease. Caller holds :attr:`_gate`."""
        self._cancel_quiesce_lease_locked()
        self._quiesce_epoch += 1
        epoch = self._quiesce_epoch
        self._lease = asyncio.create_task(
            self._quiesce_lease(epoch),
            name=f"td-quiesce-{self.resident.api_id}",
        )

    def _cancel_quiesce_lease_locked(self) -> None:
        """Drop the lease task. Caller holds :attr:`_gate`.

        Cancelling the running lease task is a no-op: that task is
        the one clearing the flags, and cancelling it would inject
        :class:`asyncio.CancelledError` at its next await.
        """
        lease = self._lease
        self._lease = None
        if (
            lease is None
            or lease.done()
            or lease is asyncio.current_task()
        ):
            return
        lease.cancel()

    def _end_quiesce_locked(self) -> None:
        """Clear the drain flags and cancel the lease. Caller holds the gate."""
        self._draining = False
        self._quiesced = False
        self._quiesce_epoch += 1
        self._cancel_quiesce_lease_locked()

    async def _quiesce_lease(self, epoch: int) -> None:
        """Resume if this quiesce is still in force when the lease ends.

        ``else`` runs only when the flags were cleared. An early return
        and a cancellation leave the layer as the other path set it.
        """
        try:
            await self._clock.sleep(self.quiesce_lease_s)
            async with self._gate:
                if (
                    epoch != self._quiesce_epoch
                    or self._closing
                    or not self._quiesced
                ):
                    return
                self._end_quiesce_locked()
        except asyncio.CancelledError:
            return
        else:
            logger.warning(
                "trading layer quiesce lease expired api_id=%s; resuming",
                self.resident.api_id,
            )

    async def handle_drain(self, message: UntypedEnvelope) -> Reply | None:
        """Apply one ``td.trading.drain`` and say whether the worker is idle.

        ``abort`` clears the draining flags and replies ``drained``
        false. A bad payload does not change the flags.
        """
        try:
            request = TdTradingDrain.model_validate(message.payload or {})
        except ValidationError as exc:
            return _error(message, "invalid_payload", str(exc))
        if request.api_id != self.resident.api_id:
            return _error(
                message,
                "invalid_payload",
                f"api_id {request.api_id} is not this worker",
            )
        if request.abort:
            await self.resume_after_drain()
            drained = False
        else:
            drained = await self.drain_for_replace()
        return Envelope[TdTradingDrainResult].wrap(
            TdTradingDrainResult(api_id=self.resident.api_id, drained=drained),
            type=TD_TRADING_DRAIN,
            source="td",
            session_id=message.session_id,
        )

    async def deactivate(self) -> None:
        """Close the private book. The resident layer stays (T1).

        Stop accepting first, so a submit or cancel that arrives now is
        refused ``TD_VENUE_NOT_CONNECTED``. Then wait, bounded, for
        submits, cancels and ``cancel_session`` already inside the
        handler. Then :meth:`Session.destroy`. Resting orders are left
        where they are and logged. The next activate reconciles them
        back (T3).

        If the wait expires, the book stays up and ``active`` is set
        true again. Destroying it under the in-flight call would drop
        an order the handler is still booking. The next push retries.

        A layer that is already off returns. The unstarted session
        stays bound: the first push is often ``active`` false, and
        that must not throw away the book the first true will start.
        Without a session and without a factory this still raises
        ``NotImplementedError("IF-11")``.
        """
        async with self._gate:
            if self._session is None and self._factory is None:
                raise NotImplementedError(TICKET)
            if not self._active:
                return
            self._active = False
        await self._drain()
        async with self._gate:
            # The last in-flight call can finish between the drain
            # noticing the deadline and this lock. Busy zero means it
            # is safe to destroy; busy still set means the book stays.
            if self._busy != 0:
                self._active = True
                logger.warning(
                    "trading layer staying up api_id=%s; %s order call(s) "
                    "still in flight after %.1fs",
                    self.resident.api_id,
                    self._busy,
                    self.drain_timeout_s,
                )
                return
            # Held across destroy. ``_enter`` sees it and does not start
            # a ``cancel_session`` between this check and the close.
            # The quiesce lease must not resume service under this stop.
            self._closing = True
            self._quiesce_epoch += 1
            self._cancel_quiesce_lease_locked()
        try:
            session = self._session
            if session is None:
                return
            self._warn_resting()
            await session.destroy()
            self.leverage = {}
        finally:
            async with self._gate:
                self._closing = False

    def _bind(self, session: Session) -> None:
        self._session = session
        self.oms = session.oms
        self.ledger = session.ledger
        self.private = session.private

    async def _abandon(self, session: Session) -> None:
        """Drop a session whose start did not finish.

        :meth:`Session.start` sets ``_started`` before recon. A later
        start on that same object returns immediately and would mark
        the layer active without a recon. Destroy, then forget it.
        """
        try:
            if not bool(getattr(session, "destroyed", False)):
                await session.destroy()
        except Exception:
            logger.exception(
                "trading layer could not drop a session that failed to start api_id=%s",
                self.resident.api_id,
            )
        if self._session is session:
            self._session = None
        self._active = False

    async def _enter(self, *, force: bool = False, kind: str = "order") -> bool:
        async with self._gate:
            if self._closing:
                return False
            session = self._session
            if session is not None and bool(getattr(session, "destroyed", False)):
                return False
            # Quiesced means the drain saw nothing in flight. A call
            # that starts now would be the thing the stop races.
            if self._quiesced:
                return False
            # Submits are refused for the whole drain. Cancels, and
            # ``cancel_session``, keep being counted until quiesce.
            if self._draining and kind == "submit":
                return False
            if not self._active and not force:
                return False
            self._busy += 1
            self._idle.clear()
            return True

    async def _leave(self) -> None:
        async with self._gate:
            self._busy -= 1
            if self._busy == 0:
                self._idle.set()

    async def _drain(self) -> bool:
        deadline = self._clock.monotonic() + self.drain_timeout_s
        while True:
            async with self._gate:
                if self._busy == 0:
                    return True
                if self._clock.monotonic() >= deadline:
                    return False
            remaining = deadline - self._clock.monotonic()
            if remaining <= 0:
                continue
            await self._pause(remaining)

    async def _pause(self, seconds: float) -> None:
        sleep_task = asyncio.create_task(self._clock.sleep(seconds))
        idle_task = asyncio.create_task(self._idle.wait())
        try:
            await asyncio.wait(
                {sleep_task, idle_task},
                return_when=asyncio.FIRST_COMPLETED,
            )
        finally:
            for task in (sleep_task, idle_task):
                if not task.done():
                    task.cancel()
            await asyncio.gather(sleep_task, idle_task, return_exceptions=True)

    def _warn_resting(self) -> None:
        orders = list(self.oms.view().orders.values())
        if not orders:
            return
        names = ",".join(order.client_order_id or order.order_id for order in orders)
        logger.warning(
            "trading layer off api_id=%s leaving %s resting order(s): %s",
            self.resident.api_id,
            len(orders),
            names,
        )
