"""Trading session — exchange connectivity + OMS pub/sub."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import AsyncIterator, Awaitable, Callable
from decimal import Decimal
from typing import Any, Protocol

from mftik.broker import Broker
from mftik.exchange.errors import ExchangeError
from mftik.exchange.models import (
    Balance,
    Fill,
    Order,
    OrderStatus,
    PlaceOrderRequest,
    can_transition,
    is_pending,
    is_terminal,
)
from mftik.exchange.oms import LedgerEntry, LedgerView, OmsView, Position
from mftik.exchange.reservations import is_linear_margin, reservation_for
from mftik.exchange.tickers import UniversalTicker
from mftik.protocol import (
    TD_BALANCE_UPDATE,
    TD_CANCEL_REJECT,
    TD_FILL,
    TD_ORDER_REJECT,
    TD_ORDER_UPDATE,
    TD_POSITION_UPDATE,
    CancelReject,
    OrderReject,
    RejectCode,
    SymbolInfo,
    Topics,
    UntypedEnvelope,
    publish_td_log,
)
from mftik.protocol.reject_codes import describe
from mftik.symbols import SymbolClient
from pydantic import BaseModel

from mftik_td.errors import normalize_reason
from mftik_td.history import HistoryWriter
from mftik_td.oms import (
    InsufficientAvailable,
    Ledger,
    Oms,
    order_key,
)

logger = logging.getLogger(__name__)

#: A venue that has not acknowledged an order in this long is not going to.
#: Generous enough to ride out a slow round-trip; short enough that a strategy
#: is not left believing an order is in flight for minutes.
PENDING_NEW_TIMEOUT_S = 5.0

#: How often the sweeper looks for aged-out PENDING_NEW orders.
PENDING_SWEEP_INTERVAL_S = 1.0

#: How long an UNKNOWN order may sit before TD forces a venue-wide recon.
#: Resolve is retried with backoff; this is the backstop when the lookup path
#: itself is dead (same transport failure that produced UNKNOWN).
UNKNOWN_FORCE_RECON_S = 10.0

#: Delay before the first resolve retry, doubled on every failed attempt.
UNKNOWN_RETRY_BASE_S = 1.0
#: Ceiling on that backoff, so a venue that never answers is asked this often
#: rather than once a tick. A dead link is the one least able to absorb a
#: lookup per order per second, and venues ban for that.
UNKNOWN_RETRY_MAX_S = 30.0
#: Minimum gap between the venue recons a stuck UNKNOWN forces.
#: :data:`UNKNOWN_FORCE_RECON_S` is an age — it says when the first one fires,
#: not how often after, which on its own is every tick forever.
UNKNOWN_FORCE_RECON_INTERVAL_S = 60.0
#: Cap on one venue lookup made from the chase loop.
UNKNOWN_RESOLVE_TIMEOUT_S = 5.0
#: Cap on the whole forced recon, which is several venue calls.
UNKNOWN_FORCE_RECON_TIMEOUT_S = 15.0

OrderCallback = Callable[[Order], None]
FillCallback = Callable[[Fill], None]
BalanceCallback = Callable[[Balance], None]
PositionCallback = Callable[[Position], None]


class TradingConnector(Protocol):
    """What TD needs of a venue, stated by TD rather than by the venue.

    ``mftik.exchange`` has no shared trading interface on purpose — venues differ
    too much for one to be honest (see :mod:`mftik.exchange.base`). So the shape
    lives here, with the consumer, and holds only what every venue really does
    provide.

    Three things are deliberately absent, because not every venue has them:

    * ``fetch_positions`` / ``stream_positions`` — only a venue with contract
      books has any. A spot venue does not report zero positions; it reports
      nothing, and the two are different answers.
    * ``fetch_leverage`` — only contract venues expose per-symbol leverage.
    * ``fetch_order_by_client_order_id`` — the way out of ``UNKNOWN``, which a
      venue that cannot look an order up by our id simply does not offer.
    * ``on_reconnect`` — only a socket-backed venue has a reconnect to hear
      about.

    Each is asked for with ``hasattr`` / ``getattr`` where it is used. That
    replaces two workarounds the old interface forced: comparing an
    implementation against the base class to see whether it was real, and
    catching ``NotImplementedError`` to discover the same thing a call too
    late.
    """

    name: str

    async def connect(self) -> None: ...

    async def close(self) -> None: ...

    async def place_order(self, request: PlaceOrderRequest) -> Order: ...

    async def cancel_by_client_order_id(self, client_order_id: str) -> Order: ...

    async def fetch_open_orders(
        self, symbol: str | None = None
    ) -> list[Order]: ...

    async def fetch_balances(self) -> list[Balance]: ...

    def stream_orders(self) -> AsyncIterator[Order]: ...

    def stream_fills(self) -> AsyncIterator[Fill]: ...

    def stream_balances(self) -> AsyncIterator[Balance]: ...


class Session:
    """Shared exchange session keyed by API id.

    Lifecycle:
    1. Created / started when first STS attaches (refcount 0→1).
    2. Further STS sessions attach via lease; OMS publishes on ``td.oms.{api_id}``.
    3. Private events fan out on ``td.{api_id}.global``.
    4. Last detach destroys the trading session.
    """

    def __init__(
        self,
        *,
        api_id: int,
        broker: Broker,
        private: TradingConnector,
        oms: Oms | None = None,
        symbols: SymbolClient | None = None,
        ledger: Ledger | None = None,
        history: HistoryWriter | None = None,
        pending_timeout: float = PENDING_NEW_TIMEOUT_S,
    ) -> None:
        self.api_id = api_id
        self.broker = broker
        self.private = private
        self.oms = oms or Oms()
        #: Records orders and fills for PnL. Optional: without one the session
        #: trades exactly as before and keeps no history, which is what every
        #: test that builds a bare session wants.
        self.history = history
        #: Balances with TD's own pre-locks layered on the venue's numbers.
        self.ledger = ledger or Ledger()
        #: Resolves base/quote so a reservation knows which asset it holds.
        #: Optional: without it TD still trades, it just cannot pre-lock.
        self.symbols = symbols
        #: universal_ticker → configured leverage, filled by
        #: :meth:`ensure_leverage` (and optionally recon). Used to size
        #: linear-margined pre-locks as ``notional / leverage``. Coin-m
        #: dated futures are not: they settle in the coin.
        self._leverage: dict[str, Decimal] = {}
        self._order_cbs: list[OrderCallback] = []
        self._fill_cbs: list[FillCallback] = []
        self._balance_cbs: list[BalanceCallback] = []
        self._position_cbs: list[PositionCallback] = []
        self._tasks: list[asyncio.Task[Any]] = []
        self._started = False
        self._destroyed = False
        #: How long a PENDING_NEW order waits for the venue before it is
        #: declared UNKNOWN and chased down.
        self.pending_timeout = pending_timeout
        #: client_order_id → loop time the submit went out.
        self._pending_since: dict[str, float] = {}
        #: client_order_id → (loop time the cancel went out, status to
        #: restore if the venue refuses it).
        self._cancel_since: dict[str, tuple[float, OrderStatus]] = {}
        #: client_order_id → when it became UNKNOWN (for re-chase / force recon).
        self._unknown_since: dict[str, float] = {}
        #: client_order_id → status to use when resolve finds nothing.
        self._unknown_if_missing: dict[str, OrderStatus] = {}
        #: client_order_id → earliest loop time the next resolve may go out.
        self._unknown_next_try: dict[str, float] = {}
        #: client_order_id → resolve attempts already spent (drives backoff).
        self._unknown_tries: dict[str, int] = {}
        #: Loop time of the last UNKNOWN-forced reconcile, for its cooldown.
        self._last_force_recon = float("-inf")
        #: Serializes book mutations that come from venue snapshots or from
        #: UNKNOWN resolution. Fetch I/O runs outside the lock; applying a
        #: resolve result or ``apply_reconcile`` runs inside so a late resolve
        #: cannot re-insert a phantom order after reconnect recon cleared it.
        self._recon_lock = asyncio.Lock()
        #: Single-flight background ``resolve_all_unknown`` (if any).
        self._resolve_all_task: asyncio.Task[None] | None = None
        #: Fired when the book has no UNKNOWN left (after resolve / recon).
        #: SessionManager uses this to flush STS ``ReconDone`` waiters.
        self._on_book_settled: Callable[[], Awaitable[None]] | None = None
        #: How long UNKNOWN may linger before a forced venue recon.
        self.unknown_force_recon = UNKNOWN_FORCE_RECON_S
        #: Ceiling on the per-order resolve backoff.
        self.unknown_retry_max = UNKNOWN_RETRY_MAX_S
        #: How often a still-stuck UNKNOWN may force another venue recon.
        self.unknown_force_recon_interval = UNKNOWN_FORCE_RECON_INTERVAL_S

        self.oms.bind(self)
        self.oms.on_update(self._on_oms_update)

    @property
    def started(self) -> bool:
        return self._started

    def has_unknown(self) -> bool:
        """True when any live order is still transport-ambiguous."""
        return any(
            order.status is OrderStatus.UNKNOWN
            for order in self.oms.view().orders.values()
        )

    def book_ready_for_snapshot(self) -> bool:
        """STS may take a ReconDone snapshot without another venue pass."""
        return self._started and not self.has_unknown()

    def view_for_sts(self) -> OmsView:
        """OMS snapshot for STS recon: live orders/positions, ledger balances.

        ``Oms._balances`` is only reliably refreshed on full venue reconcile;
        the ledger is what balance streams and pre-locks actually update. STS
        funding checks read the ledger, so ReconDone must match it.
        """
        view = self.oms.view()
        return OmsView(
            orders=view.orders,
            positions=view.positions,
            balances=self.ledger.snapshot(),
        )

    async def accept_venue_order(self, order: Order) -> None:
        """Book a venue place/cancel ack and announce it — do not wait on WS.

        The REST/WS trading call already confirmed the outcome; discarding that
        Order and only trusting the private stream leaves PENDING_* stuck when
        the confirming push is lost.

        The ack is authoritative about the *outcome* and about nothing else.
        Connectors build it from whatever the venue handed back, and some
        venues hand back almost nothing — Bybit's cancel ack carries two ids
        and no state, so its connector echoes the last order the stream
        reported. That makes an ack a stale source for fills, and a dangerous
        one for a cid the book has already finished with.
        """
        cid = order.client_order_id
        if cid:
            current = self.oms.get_order(cid)
            if current is None and not is_terminal(order.status):
                # Filled, or reconciled away, while the call was in flight.
                # Re-inserting it live from the ack would resurrect an order
                # no stream will ever terminate again.
                logger.info(
                    "TD venue ack ignored api_id=%s cid=%s (%s) — the book "
                    "has already finished with it",
                    self.api_id,
                    cid,
                    order.status.value,
                )
                self.cancel_settled(cid)
                self._pending_since.pop(cid, None)
                return
            if current is not None and order.filled_qty < current.filled_qty:
                # The stream is what knows about fills. Keep its numbers.
                order = order.model_copy(
                    update={
                        "filled_qty": current.filled_qty,
                        "avg_price": current.avg_price,
                    }
                )
        for cb in list(self._order_cbs):
            cb(order)
        await self._store_then_announce_order(order)
        if cid and is_terminal(order.status):
            self.cancel_settled(cid)
            self._forget_unknown(cid)
            self._pending_since.pop(cid, None)

    @property
    def destroyed(self) -> bool:
        return self._destroyed

    @property
    def venue(self) -> str:
        """Canonical venue name, which is what the client calls itself.

        Adapters name themselves after their entry in ``mftik.exchange.venues``,
        so this is also the key error normalization looks the venue up by.
        """
        return self.private.name

    def on_order(self, cb: OrderCallback) -> None:
        self._order_cbs.append(cb)

    def on_fill(self, cb: FillCallback) -> None:
        self._fill_cbs.append(cb)

    def on_balance(self, cb: BalanceCallback) -> None:
        self._balance_cbs.append(cb)

    def on_position(self, cb: PositionCallback) -> None:
        self._position_cbs.append(cb)

    async def start(self) -> None:
        """Connect private venue client, seed OMS, and begin stream pumps."""
        if self._started:
            return
        await self.private.connect()
        self._started = True

        await self.reconcile()

        order_stream = self.private.stream_orders()
        fill_stream = self.private.stream_fills()
        balance_stream = self.private.stream_balances()
        self._tasks = [
            asyncio.create_task(
                self._pump(order_stream, self._dispatch_order),
                name=f"sess-{self.api_id}-orders",
            ),
            asyncio.create_task(
                self._pump(fill_stream, self._dispatch_fill),
                name=f"sess-{self.api_id}-fills",
            ),
            asyncio.create_task(
                self._pump(balance_stream, self._dispatch_balance),
                name=f"sess-{self.api_id}-balances",
            ),
            asyncio.create_task(
                self._sweep_loop(), name=f"sess-{self.api_id}-sweep"
            ),
            asyncio.create_task(
                self._chase_loop(), name=f"sess-{self.api_id}-chase"
            ),
        ]
        # Only if the venue has positions at all. A spot venue offers no such
        # stream, and pumping one that does not exist would fail the start of
        # a session that is otherwise perfectly healthy.
        stream_positions = getattr(self.private, "stream_positions", None)
        if stream_positions is not None:
            self._tasks.append(
                asyncio.create_task(
                    self._pump(stream_positions(), self._dispatch_position),
                    name=f"sess-{self.api_id}-positions",
                )
            )
        # A dead socket looks exactly like a quiet venue, so rebuild from the
        # venue rather than trusting a book that stopped updating. Only a
        # socket-backed venue has a reconnect to report; the rest simply have
        # no such method.
        on_reconnect = getattr(self.private, "on_reconnect", None)
        if on_reconnect is not None:
            on_reconnect(self._on_venue_reconnect)
        logger.info("Session started api_id=%s", self.api_id)

    async def _sweep_loop(self) -> None:
        while not self._destroyed:
            try:
                await asyncio.sleep(PENDING_SWEEP_INTERVAL_S)
                await self.sweep_pending()
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("TD pending sweep failed api_id=%s", self.api_id)

    async def _chase_loop(self) -> None:
        """Chase UNKNOWN orders on their own timer, not the sweeper's.

        Sharing a task would let a slow venue lookup stop PENDING_NEW from
        ageing out — the watchdog disabled by exactly the condition it is
        there to catch.
        """
        while not self._destroyed:
            try:
                await asyncio.sleep(PENDING_SWEEP_INTERVAL_S)
                await self.chase_unknown()
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception(
                    "TD unknown chase failed api_id=%s", self.api_id
                )

    async def _on_venue_reconnect(self) -> None:
        """Re-run recon after the venue connection comes back."""
        if self._destroyed or not self._started:
            return
        logger.warning(
            "TD venue reconnected api_id=%s — reconciling", self.api_id
        )
        try:
            await self.reconcile()
        except Exception:
            logger.exception("TD post-reconnect recon failed api_id=%s", self.api_id)

    async def reconcile(self) -> OmsView:
        """Query venue open orders / positions / balances into OMS and publish.

        Single-flight per session: overlapping reconnect / internal recon
        share one lock so two snapshots cannot interleave apply/publish.
        """
        if self._destroyed:
            raise RuntimeError(f"session api_id={self.api_id} is destroyed")
        async with self._recon_lock:
            if self._destroyed:
                raise RuntimeError(f"session api_id={self.api_id} is destroyed")
            orders = await self.private.fetch_open_orders()
            balances = await self.private.fetch_balances()
            # Spot venues have no positions to report and no method for them,
            # which is a different thing from reporting none.
            positions: list[Position] | None = None
            fetch_positions = getattr(self.private, "fetch_positions", None)
            if fetch_positions is not None:
                positions = list(await fetch_positions())

            # What this snapshot is about to settle. ``apply_reconcile``
            # replaces the book wholesale, so an UNKNOWN the venue no longer
            # lists just disappears — no terminal event for it. STS drops a
            # leg on a terminal update and on nothing else, so a silent drop
            # leaves the strategy holding an order that no longer exists.
            was_unknown = {
                cid: order
                for cid, order in self.oms.view().orders.items()
                if order.status is OrderStatus.UNKNOWN
            }
            outcomes = dict(self._unknown_if_missing)

            view = self.oms.apply_reconcile(
                orders=orders,
                balances=balances,
                positions=positions,
            )
            # Recon is the moment TD's books become authoritative: the venue was
            # just asked, so both hashes are replaced wholesale rather than
            # merged. Pre-locks survive it — they cover orders the venue has not
            # acknowledged yet, which is exactly what a snapshot cannot see.
            self._pending_since.clear()
            self._cancel_since.clear()
            self._unknown_since.clear()
            self._unknown_if_missing.clear()
            self._unknown_next_try.clear()
            self._unknown_tries.clear()
            self.ledger.apply_venue_many(list(balances))
            await self.write_ledger()
            await self.publish_oms(view)
            for cid, order in was_unknown.items():
                if cid in view.orders:
                    continue
                await self._announce_recon_settled(
                    cid,
                    order,
                    # CANCELED, not REJECTED, when nothing was recorded: the
                    # order was live enough to be in the book, so claiming it
                    # never landed would be the stronger and wronger guess.
                    outcomes.get(cid, OrderStatus.CANCELED),
                )
        await self._emit_book_settled_if_clean()
        return view

    async def _announce_recon_settled(
        self, cid: str, order: Order, status: OrderStatus
    ) -> None:
        """Publish the terminal state recon implied for a dropped UNKNOWN."""
        settled = order.model_copy(update={"status": status})
        await self.write_order(settled)
        await self._publish_order_update(settled)
        await self.release(cid)
        logger.info(
            "TD recon settled UNKNOWN api_id=%s cid=%s → %s "
            "(venue no longer lists it)",
            self.api_id,
            cid,
            status.value,
        )

    async def clear_state(self) -> None:
        """Delete this account's Redis state. Call when the session dies.

        State that outlives the session it describes is a trap: a strategy
        attaching later reads balances and orders that nobody is updating and
        has no way to tell they are dead.
        """
        self.ledger.release_all()
        self._leverage.clear()
        await self.broker.state_clear(self._ledger_key, self._oms_key)

    async def publish_oms(self, view: OmsView | None = None) -> None:
        """Write the OMS to ``td.oms.{api_id}``: client_order_id → Order.

        Whole-hash replace, so what STS reads is exactly TD's live book — a
        cancelled order disappears rather than lingering as a stale field.
        """
        snap = view if view is not None else self.oms.view()
        await self.broker.state_replace(
            self._oms_key, {order_key(o): o for o in snap.orders.values()}
        )

    async def record_pending_new(
        self, request: PlaceOrderRequest, *, session_id: str | None = None
    ) -> Order:
        """Book the order locally before it goes to the venue.

        Written and announced before the submit is acked, so a strategy that
        is told True can immediately read the order it just placed. It carries
        no venue ``order_id`` yet — until the venue answers there is nothing to
        cancel, which is exactly what ``PENDING_NEW`` means.

        ``session_id`` is recorded to history here and nowhere else. This is
        the only moment TD holds it and the ``client_order_id`` together: every
        later event about this order arrives from the venue, which has never
        heard of an STS session. Attribution derived afterwards would have to
        decode the slot packed into the cid, and that slot is unique only among
        sessions alive at the same time — sound for a live lookup, a guess
        against history.
        """
        order = Order(
            client_order_id=request.client_order_id,
            # The connector's own market: an order arrives as a bare symbol
            # because ``api_id`` already pins which account, and so which
            # market, it is for. A unified-account venue would have to be told,
            # and that is where ``OrderSubmit`` grows a ticker.
            universal_ticker=request.universal_ticker,
            side=request.side,
            type=request.type,
            status=OrderStatus.PENDING_NEW,
            qty=request.qty if request.qty is not None else Decimal("0"),
            quote_qty=request.quote_qty,
            price=request.price,
        )
        self.oms.handle_order(order)
        self._pending_since[order.client_order_id or order.order_id] = (
            asyncio.get_running_loop().time()
        )
        await self.write_order(order)
        self._record_order(order, session_id=session_id, submitted_at=order.ts)
        await self._publish_order_update(order)
        return order

    async def record_rejected(self, client_order_id: str) -> Order | None:
        """Settle a booked order the venue refused, and give the funds back.

        The submit path publishes its own reject envelope, but the order it
        booked ``PENDING_NEW`` is still sitting in the book — without this it
        would linger until the sweeper called it ``UNKNOWN`` and chased a
        venue that already said no.

        The pre-lock comes back here too, and only here. Every other terminal
        state releases through the manager's ``_on_order_settled``, which is
        registered on the *venue stream* — and a submit the venue refused by
        raising never reaches that stream. Left undone it is a standing leak:
        the order is gone from the book while its reservation still counts
        against ``available``, and nothing frees it until the session dies and
        ``clear_state`` drops the ledger wholesale. A crossed post-only alone
        earns one every time the book moves under a passive quote.

        Released before the caller announces the refusal, for the reason
        :meth:`reserve` writes the ledger before the ack goes out: a strategy
        that resubmits off the reject must not read a balance still holding
        funds for the order it was just told about.
        """
        self._pending_since.pop(client_order_id, None)
        # Ahead of the early return, and idempotent by design: an order the
        # stream already settled has had this cid released, and one whose
        # commitment could not be priced never reserved it at all.
        await self.release(client_order_id)
        order = self.oms.get_order(client_order_id)
        if order is None or is_terminal(order.status):
            return None
        rejected = order.model_copy(update={"status": OrderStatus.REJECTED})
        self.oms.handle_order(rejected)
        await self.write_order(rejected)
        # Recorded here rather than through ``_publish_order_update``, which
        # this path deliberately skips: the submit already publishes its own
        # reject envelope and a second announcement would be noise. History
        # still has to hear it — an order left at its previous state would sit
        # in the record as pending forever, indistinguishable from one that
        # really is.
        self._record_order(rejected)
        return rejected

    async def record_unfilled(
        self, client_order_id: str, *, reason: str
    ) -> Order | None:
        """Settle an immediate-or-nothing order that filled nothing.

        A fill-or-kill the book could not serve did what it was told, so it
        ends as a cancelled order with nothing filled — never as a reject.
        Most venues report exactly that on their private order stream and this
        is never reached for them; Gate and Binance's margined books answer
        the *call* instead, and this is what makes the two look the same to a
        strategy. See :func:`mftik_td.errors.is_unfilled_immediate`.

        Unlike :meth:`record_rejected` this *does* announce the order, because
        no reject envelope follows it. The order update is the only thing that
        will ever say what happened, so a strategy waiting on the terminal
        state hears it here.

        ``reason`` is the venue's own words. They go to the log and no further:
        nothing was refused, and ``reject_reason`` is for orders that were.
        """
        self._pending_since.pop(client_order_id, None)
        await self.release(client_order_id)
        order = self.oms.get_order(client_order_id)
        if order is None or is_terminal(order.status):
            return None
        canceled = order.model_copy(update={"status": OrderStatus.CANCELED})
        self.oms.handle_order(canceled)
        await self.write_order(canceled)
        self._record_order(canceled)
        await self._td_log(
            f"order unfilled cid={client_order_id} "
            f"(immediate-or-nothing, nothing filled): {reason}"
        )
        await self._publish_order_update(canceled)
        return canceled

    async def record_pending_cancel(self, client_order_id: str) -> str | None:
        """Mark an order PENDING_CANCEL before the cancel goes to the venue.

        Returns None when the order is now marked (or is not ours to mark),
        otherwise the reason the cancel cannot be attempted.

        The state matters for risk more than for bookkeeping: a cancel in
        flight has not removed the order, so the exposure is still real, but
        the size is no longer capacity anyone should plan to cancel again.
        """
        order = self.oms.get_order(client_order_id)
        if order is None:
            # Not in our book — recon found it, or another process placed it.
            # The venue may still know it, so let the cancel through unmarked.
            return None
        if not can_transition(order.status, OrderStatus.PENDING_CANCEL):
            return (
                f"order is {order.status.value}; "
                "it cannot be cancelled from that state"
            )
        pending = order.model_copy(
            update={"status": OrderStatus.PENDING_CANCEL}
        )
        self.oms.handle_order(pending)
        # Remember what to fall back to: a refused cancel puts the order back
        # exactly where it was, not merely "open".
        self._cancel_since[client_order_id] = (
            asyncio.get_running_loop().time(),
            order.status,
        )
        await self.write_order(pending)
        await self._publish_order_update(pending)
        return None

    async def revert_pending_cancel(self, client_order_id: str) -> Order | None:
        """Put an order back after the venue refused the cancel."""
        held = self._cancel_since.pop(client_order_id, None)
        if held is None:
            return None
        _sent_at, prior = held
        order = self.oms.get_order(client_order_id)
        if order is None or order.status is not OrderStatus.PENDING_CANCEL:
            return None
        restored = order.model_copy(update={"status": prior})
        self.oms.handle_order(restored)
        await self.write_order(restored)
        await self._publish_order_update(restored)
        logger.info(
            "TD cancel refused api_id=%s cid=%s → back to %s",
            self.api_id,
            client_order_id,
            prior.value,
        )
        return restored

    def cancel_settled(self, client_order_id: str) -> None:
        """Forget cancel bookkeeping once the venue has answered."""
        self._cancel_since.pop(client_order_id, None)

    async def sweep_pending(self) -> list[Order]:
        """Flip PENDING_NEW orders that aged out to UNKNOWN, then resolve them.

        Silence is ambiguous: the order may be resting, may have filled, may
        never have arrived. ``UNKNOWN`` says so honestly rather than guessing,
        and the venue query that follows is the only thing that can settle it.
        """
        if self._destroyed:
            return []
        now = asyncio.get_running_loop().time()
        stale = [
            cid
            for cid, sent_at in self._pending_since.items()
            if now - sent_at >= self.pending_timeout
        ]
        stale += [
            cid
            for cid, (sent_at, _prior) in self._cancel_since.items()
            if now - sent_at >= self.pending_timeout
        ]
        moved: list[Order] = []
        for cid in stale:
            self._pending_since.pop(cid, None)
            self._cancel_since.pop(cid, None)
            async with self._recon_lock:
                order = self.oms.get_order(cid)
                if order is None or not is_pending(order.status):
                    continue
                prior_status = order.status
                unknown = order.model_copy(
                    update={"status": OrderStatus.UNKNOWN}
                )
                self.oms.handle_order(unknown)
                await self.write_order(unknown)
                await self._publish_order_update(unknown)
                self._remember_unknown(
                    cid,
                    if_missing=(
                        OrderStatus.CANCELED
                        if prior_status is OrderStatus.PENDING_CANCEL
                        else OrderStatus.REJECTED
                    ),
                )
                logger.warning(
                    "TD %s unanswered after %ss api_id=%s cid=%s → UNKNOWN",
                    prior_status.value,
                    self.pending_timeout,
                    self.api_id,
                    cid,
                )
                moved.append(unknown)
            await self.resolve_unknown(unknown)
        await self._emit_book_settled_if_clean()
        return moved

    def _remember_unknown(
        self,
        client_order_id: str,
        *,
        if_missing: OrderStatus,
    ) -> None:
        now = asyncio.get_running_loop().time()
        self._unknown_since.setdefault(client_order_id, now)
        self._unknown_if_missing[client_order_id] = if_missing
        self._unknown_next_try.setdefault(client_order_id, now)
        self._unknown_tries.setdefault(client_order_id, 0)

    def _forget_unknown(self, client_order_id: str) -> None:
        self._unknown_since.pop(client_order_id, None)
        self._unknown_if_missing.pop(client_order_id, None)
        self._unknown_next_try.pop(client_order_id, None)
        self._unknown_tries.pop(client_order_id, None)

    def _defer_unknown_retry(self, client_order_id: str, now: float) -> None:
        """Push the next resolve out, doubling the wait per spent attempt."""
        tries = self._unknown_tries.get(client_order_id, 0) + 1
        self._unknown_tries[client_order_id] = tries
        self._unknown_next_try[client_order_id] = now + min(
            UNKNOWN_RETRY_BASE_S * 2 ** (tries - 1), self.unknown_retry_max
        )

    async def mark_unknown_and_resolve(
        self,
        client_order_id: str,
        *,
        if_missing: OrderStatus = OrderStatus.REJECTED,
    ) -> Order | None:
        """Transport failed mid-flight: mark UNKNOWN and ask the venue.

        ``if_missing`` is used when the venue has no row for the cid — submit
        silence means the order never landed (``REJECTED``); cancel silence
        usually means it is already gone (``CANCELED``).
        """
        self._pending_since.pop(client_order_id, None)
        self._cancel_since.pop(client_order_id, None)
        async with self._recon_lock:
            order = self.oms.get_order(client_order_id)
            if order is None:
                return None
            if is_terminal(order.status):
                self._forget_unknown(client_order_id)
                return order
            if order.status is not OrderStatus.UNKNOWN:
                if not can_transition(order.status, OrderStatus.UNKNOWN):
                    logger.warning(
                        "TD cannot mark UNKNOWN api_id=%s cid=%s from %s",
                        self.api_id,
                        client_order_id,
                        order.status.value,
                    )
                    return None
                unknown = order.model_copy(
                    update={"status": OrderStatus.UNKNOWN}
                )
                self.oms.handle_order(unknown)
                await self.write_order(unknown)
                await self._publish_order_update(unknown)
                logger.warning(
                    "TD transport ambiguous api_id=%s cid=%s (%s) → UNKNOWN",
                    self.api_id,
                    client_order_id,
                    order.status.value,
                )
                order = unknown
            self._remember_unknown(client_order_id, if_missing=if_missing)
        settled = await self.resolve_unknown(order, if_missing=if_missing)
        await self._emit_book_settled_if_clean()
        return settled

    async def resolve_unknown(
        self,
        order: Order,
        *,
        if_missing: OrderStatus | None = None,
    ) -> Order | None:
        """Ask the venue what actually happened to an UNKNOWN order.

        A ``None`` from the venue is a real answer — interpreted as
        ``if_missing`` (default ``REJECTED``: the submit never landed). A
        venue that cannot answer leaves the order UNKNOWN for recon to settle.

        The venue lookup runs without the recon lock; applying the result
        takes the lock and refuses to write unless the cid is still UNKNOWN,
        so a concurrent ``reconcile()`` cannot be undone by a late resolve.
        """
        cid = order.client_order_id
        if not cid:
            return None
        missing = if_missing or self._unknown_if_missing.get(
            cid, OrderStatus.REJECTED
        )
        resolve = getattr(self.private, "fetch_order_by_client_order_id", None)
        if resolve is None:
            logger.debug(
                "TD venue %s cannot resolve by client_order_id; leaving "
                "cid=%s UNKNOWN for recon",
                self.private.name,
                cid,
            )
            return None
        try:
            # Bounded: the transport failure that produced the UNKNOWN is
            # just as capable of hanging the lookup, and this runs on a loop
            # that has other orders to watch.
            found = await asyncio.wait_for(
                resolve(cid, ticker=order.ticker), UNKNOWN_RESOLVE_TIMEOUT_S
            )
        except Exception:
            logger.exception(
                "TD resolve failed api_id=%s cid=%s", self.api_id, cid
            )
            return None

        settled = found or order.model_copy(update={"status": missing})
        async with self._recon_lock:
            current = self.oms.get_order(cid)
            if current is None or current.status is not OrderStatus.UNKNOWN:
                logger.info(
                    "TD resolve skipped api_id=%s cid=%s — no longer UNKNOWN "
                    "(current=%s)",
                    self.api_id,
                    cid,
                    None if current is None else current.status.value,
                )
                self._forget_unknown(cid)
                return current
            self.oms.handle_order(settled)
            await self.write_order(settled)
            await self._publish_order_update(settled)
            self._cancel_since.pop(cid, None)
            self._forget_unknown(cid)
            await self.release(cid)
        logger.info(
            "TD resolved UNKNOWN api_id=%s cid=%s → %s",
            self.api_id,
            cid,
            settled.status,
        )
        return settled

    async def _emit_book_settled_if_clean(self) -> None:
        if self.has_unknown() or self._on_book_settled is None:
            return
        try:
            await self._on_book_settled()
        except Exception:
            logger.exception(
                "TD book-settled callback failed api_id=%s", self.api_id
            )

    async def resolve_all_unknown(self) -> None:
        """Chase every UNKNOWN order currently in the book."""
        unknowns = [
            order
            for order in list(self.oms.view().orders.values())
            if order.status is OrderStatus.UNKNOWN
        ]
        for order in unknowns:
            await self.resolve_unknown(order)
        await self._emit_book_settled_if_clean()

    def kick_resolve_all_unknown(self) -> asyncio.Task[None] | None:
        """Start at most one background ``resolve_all_unknown`` for this session.

        Stores the task so it is not GC'd mid-flight, and coalesces concurrent
        kickers (e.g. two ``sts.recon`` waiters) onto the same pass.
        """
        if self._destroyed:
            return None
        task = self._resolve_all_task
        if task is not None and not task.done():
            return task

        async def _run() -> None:
            try:
                await self.resolve_all_unknown()
            except Exception:
                logger.exception(
                    "TD resolve_all_unknown failed api_id=%s", self.api_id
                )

        self._resolve_all_task = asyncio.create_task(
            _run(), name=f"td-resolve-all-{self.api_id}"
        )
        return self._resolve_all_task

    async def chase_unknown(self) -> None:
        """Re-resolve lingering UNKNOWN orders; force venue recon if stuck.

        ``mark_unknown_and_resolve`` drops pending/cancel timers, so without
        this path a failed resolve would sit forever and park STS recon
        waiters with no production trigger to flush them.

        Both the per-order retry and the forced recon are rate-limited. An
        order that cannot be resolved is usually one whose venue link is
        already unhealthy, and retrying it every tick until it works turns a
        transport failure into a rate-limit ban.
        """
        if self._destroyed or not self._unknown_since:
            return
        now = asyncio.get_running_loop().time()
        # Drop bookkeeping for orders that left UNKNOWN some other way.
        for cid in list(self._unknown_since):
            order = self.oms.get_order(cid)
            if order is None or order.status is not OrderStatus.UNKNOWN:
                self._forget_unknown(cid)
        if not self._unknown_since:
            await self._emit_book_settled_if_clean()
            return

        due = [cid for cid, at in self._unknown_next_try.items() if at <= now]
        for cid in due:
            order = self.oms.get_order(cid)
            if order is None or order.status is not OrderStatus.UNKNOWN:
                self._forget_unknown(cid)
                continue
            # Spend the attempt before making it: a resolve that raises must
            # still push the next one out, or the backoff never applies.
            self._defer_unknown_retry(cid, now)
            await self.resolve_unknown(order)
        if due:
            await self._emit_book_settled_if_clean()
        if not self._unknown_since:
            return

        oldest = min(self._unknown_since.values())
        if now - oldest < self.unknown_force_recon:
            return
        if now - self._last_force_recon < self.unknown_force_recon_interval:
            return
        self._last_force_recon = now
        logger.warning(
            "TD UNKNOWN stuck >%ss api_id=%s cids=%s — forcing venue recon",
            self.unknown_force_recon,
            self.api_id,
            sorted(self._unknown_since),
        )
        try:
            await asyncio.wait_for(
                self.reconcile(), UNKNOWN_FORCE_RECON_TIMEOUT_S
            )
        except Exception:
            logger.exception(
                "TD forced recon after UNKNOWN failed api_id=%s", self.api_id
            )

    async def write_order(self, order: Order) -> None:
        """Persist one order, or drop it once it is finished.

        Terminal orders leave the hash entirely: STS reads this to know what
        is live, and a filled order sitting there reads as an open position
        that does not exist.
        """
        field = order_key(order)
        if is_terminal(order.status):
            await self.broker.state_drop(self._oms_key, field)
        else:
            await self.broker.state_put(self._oms_key, field, order)

    def cached_leverage(self, ticker: UniversalTicker | str) -> Decimal | None:
        """Leverage last stored for ``ticker``, or None if never ensured."""
        return self._leverage.get(str(ticker))

    async def ensure_leverage(self, ticker: UniversalTicker) -> Decimal:
        """Return this account's leverage for ``ticker``, fetching if needed.

        Spot tickers and venues without ``fetch_leverage`` raise
        :class:`~mftik.exchange.errors.ExchangeError`. A successful read is
        cached for :meth:`reserve` and later ensure calls.
        """
        key = str(ticker)
        cached = self._leverage.get(key)
        if cached is not None:
            return cached
        if not is_linear_margin(ticker.category, venue=ticker.venue):
            raise ExchangeError(
                f"leverage is only defined on linear margined books, got {ticker}"
            )
        fetch = getattr(self.private, "fetch_leverage", None)
        if fetch is None:
            raise ExchangeError(
                f"{self.venue} does not support leverage lookup"
            )
        value = await fetch(ticker)
        if not isinstance(value, Decimal):
            value = Decimal(str(value))
        if value <= 0:
            raise ExchangeError(
                f"leverage for {ticker} is not positive: {value}"
            )
        self._leverage[key] = value
        logger.info(
            "TD cached leverage api_id=%s ticker=%s leverage=%s",
            self.api_id,
            key,
            value,
        )
        return value

    async def reserve(self, request: PlaceOrderRequest) -> str | None:
        """Pre-lock the funds ``request`` commits. None on success, else why not.

        Returning a reason means nothing was reserved and the caller must not
        send the order. An order whose commitment cannot be priced (a market
        buy) or whose instrument cannot be resolved goes through unreserved
        rather than being blocked — the ledger is a guard against overspending
        what we know about, not a gate that fails closed on missing data.
        """
        info = await self._symbol_info(request.ticker)
        if info is None:
            logger.debug(
                "TD no instrument for %s api_id=%s; submitting unreserved",
                request.universal_ticker,
                self.api_id,
            )
            return None
        leverage = (
            self._leverage.get(request.universal_ticker)
            if is_linear_margin(request.category, venue=request.ticker.venue)
            else None
        )
        held = reservation_for(
            request, base=info.base, quote=info.quote, leverage=leverage
        )
        if held is None:
            logger.debug(
                "TD cannot price %s %s; submitting unreserved",
                request.type,
                request.universal_ticker,
            )
            return None
        asset, amount = held
        assert request.client_order_id is not None
        try:
            self.ledger.reserve(request.client_order_id, asset, amount)
        except InsufficientAvailable as exc:
            return str(exc)
        # Both awaited, not scheduled. The caller acks the submit off the back
        # of this, and STS reads the ledger out of Redis — so the write has to
        # have landed before True goes out, or a strategy could act on a
        # balance that does not yet know about the order it just placed.
        await self.write_ledger(asset)
        await self._publish_balance(asset)
        return None

    async def release(self, client_order_id: str) -> bool:
        """Drop a pre-lock — the venue is accounting for it now, or never will."""
        asset = self.ledger.asset_for(client_order_id)
        released = self.ledger.release(client_order_id)
        if released and asset is not None:
            await self.write_ledger(asset)
            await self._publish_balance(asset)
        return released

    async def _publish_balance(self, asset: str) -> None:
        """Tell STS the asset moved, so it knows when to go and look.

        A pre-lock changes what is spendable exactly as a venue debit does, so
        it raises the same event: the hook is a decision point, and STS should
        get one whether the money moved at the venue or only here.
        """
        try:
            await self._publish_global(
                TD_BALANCE_UPDATE, self.ledger.balance(asset)
            )
        except Exception:
            logger.exception(
                "TD balance publish failed api_id=%s asset=%s",
                self.api_id,
                asset,
            )

    async def _symbol_info(self, ticker: UniversalTicker) -> SymbolInfo | None:
        """The plane's row for an order, or None if it cannot say.

        The order names it. TD used to build the ticker here from the
        connector's own venue and category, which worked only because every
        venue traded one market — a unified account has two, and ``BTCUSDT``
        does not say which.
        """
        if self.symbols is None:
            return None
        try:
            return await self.symbols.get(ticker)
        except Exception:
            logger.debug("TD symbol lookup failed ticker=%s", ticker, exc_info=True)
            return None

    def ledger_view(self) -> LedgerView:
        """Balances as TD knows them, pre-locks included."""
        return LedgerView(api_id=self.api_id, balances=self.ledger.snapshot())

    async def write_ledger(self, *assets: str) -> None:
        """Write the ledger to ``td.ledger.{api_id}``: asset → free/prelock/lock.

        Named assets only, or the whole hash when none are given. This is the
        state STS reads; TD is its sole writer.
        """
        snapshot = self.ledger.snapshot()
        if assets:
            rows = {
                asset: LedgerEntry.of(self.ledger.balance(asset))
                for asset in assets
            }
            await self.broker.state_put_many(self._ledger_key, rows)
            return
        await self.broker.state_replace(
            self._ledger_key,
            {a: LedgerEntry.of(b) for a, b in snapshot.items()},
        )

    @property
    def _ledger_key(self) -> str:
        return Topics.td_ledger(self.api_id)

    @property
    def _oms_key(self) -> str:
        return Topics.td_oms(self.api_id)

    async def publish_order_reject(
        self,
        *,
        reason: str,
        client_order_id: str | None = None,
        order_id: str | None = None,
        universal_ticker: str | None = None,
        error_code: int | str = RejectCode.NONE,
    ) -> None:
        """Publish a submit reject on ``td.{api_id}.global``.

        ``reason`` is what a human reads; ``error_code`` is what a strategy
        branches on. Callers that have an exception should run it through
        :func:`mftik_td.errors.normalize` rather than picking a code by hand;
        one holding a venue's ``rejectReason`` wants
        :func:`mftik_td.errors.normalize_reason`.

        A crossed post-only logs at ``info``. It is not a fault: post-only
        exists to be refused when the price crossed, so a strategy quoting
        passively earns one every time the book moves under it, and a node
        with an alert matcher on TD warnings would page someone for each.
        Every other refusal stays a warning.
        """
        crossed = error_code == RejectCode.VENUE_POST_ONLY_WOULD_CROSS
        await self._td_log(
            (
                f"order rejected cid={client_order_id or '?'} "
                f"[{describe(error_code)}]: {reason}"
                + (f" {universal_ticker}" if universal_ticker else "")
            ),
            level="info" if crossed else "warn",
        )
        await self._publish_global(
            TD_ORDER_REJECT,
            OrderReject(
                api_id=self.api_id,
                client_order_id=client_order_id,
                order_id=order_id,
                universal_ticker=universal_ticker,
                reason=reason,
                error_code=error_code,
            ),
        )

    async def publish_cancel_reject(
        self,
        *,
        reason: str,
        client_order_id: str | None = None,
        order_id: str | None = None,
        error_code: int | str = RejectCode.NONE,
    ) -> None:
        """Publish a cancel reject on ``td.{api_id}.global``."""
        await self._td_log(
            (
                f"cancel rejected cid={client_order_id or '?'} "
                f"[{describe(error_code)}]: {reason}"
            ),
            level="warn",
        )
        await self._publish_global(
            TD_CANCEL_REJECT,
            CancelReject(
                api_id=self.api_id,
                client_order_id=client_order_id,
                order_id=order_id,
                reason=reason,
                error_code=error_code,
            ),
        )

    async def destroy(self) -> None:
        """Tear down exchange pumps and private client."""
        if self._destroyed:
            return
        self._destroyed = True

        for task in self._tasks:
            task.cancel()
        if self._resolve_all_task is not None:
            self._resolve_all_task.cancel()
            self._tasks.append(self._resolve_all_task)
            self._resolve_all_task = None
        if self._tasks:
            await asyncio.gather(*self._tasks, return_exceptions=True)
        self._tasks.clear()

        if self.private.connected:
            await self.private.close()
        self._started = False
        try:
            await self.clear_state()
        except Exception:
            logger.exception("TD state clear failed api_id=%s", self.api_id)
        logger.info("Session destroyed api_id=%s", self.api_id)

    async def _pump(self, stream: Any, dispatch: Callable[[Any], None]) -> None:
        try:
            async for item in stream:
                dispatch(item)
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("venue pump failed api_id=%s", self.api_id)

    def _dispatch_order(self, order: Order) -> None:
        for cb in list(self._order_cbs):
            cb(order)
        self._schedule_publish(self._store_then_announce_order(order))

    async def _store_then_announce_order(self, order: Order) -> None:
        """Persist the order, *then* raise the event.

        The order matters. STS treats the event as "go and look", and what it
        looks at is Redis — announcing first would hand it a window in which
        the hash still describes the previous state.
        """
        try:
            await self.write_order(order)
        except Exception:
            logger.exception(
                "TD order state write failed api_id=%s cid=%s",
                self.api_id,
                order.client_order_id,
            )
        if order.status is OrderStatus.REJECTED:
            # A reject that arrived on the order stream, not as a failed call:
            # the venue said no after accepting the request. Some venues put
            # their reason on the row — Bybit refuses a crossed post-only only
            # here, never as an exception — so it goes through the same tables
            # a raised refusal would. A row that carries no reason still comes
            # back VENUE_REJECTED, which is what that code means.
            #
            # Recorded before the refusal goes out, because this branch skips
            # ``_publish_order_update`` and would otherwise leave the order in
            # history at whatever state it last reached.
            self._record_order(order)
            await self.publish_order_reject(
                reason=order.reject_reason or "rejected",
                client_order_id=order.client_order_id,
                order_id=order.order_id,
                universal_ticker=order.universal_ticker,
                error_code=normalize_reason(order.reject_reason, venue=self.venue),
            )
        else:
            await self._publish_order_update(order)

    def _dispatch_fill(self, fill: Fill) -> None:
        for cb in list(self._fill_cbs):
            cb(fill)
        # Queued, not written: this runs on the socket pump, and a database
        # round trip taken here would stall the stream feeding the OMS, the
        # ledger and every strategy on the account.
        if self.history is not None:
            self.history.record_fill(fill, api_id=self.api_id)
        self._schedule_publish(self._announce_fill(fill))

    async def _announce_fill(self, fill: Fill) -> None:
        fee = f" fee={fill.fee}"
        if fill.fee_asset:
            fee = f"{fee} {fill.fee_asset}"
        await self._td_log(
            f"fill cid={fill.client_order_id or '?'} "
            f"{fill.side} {fill.price}@{fill.qty}{fee} "
            f"{fill.universal_ticker}"
        )
        await self._publish_global(TD_FILL, fill)

    def _record_order(
        self,
        order: Order,
        *,
        session_id: str | None = None,
        submitted_at: float | None = None,
    ) -> None:
        """Hand one order state to the history writer, if there is one.

        Never awaited and never able to raise: history is a bystander to
        trading, and a database that is unwell must not be able to fail an
        order path that is otherwise fine.
        """
        if self.history is None:
            return
        self.history.record_order(
            order,
            api_id=self.api_id,
            session_id=session_id,
            submitted_at=submitted_at,
        )

    async def _publish_order_update(self, order: Order) -> None:
        """Announce an order on ``td.{api_id}.global`` and log it.

        Every order state change funnels through here — venue pushes, sweeps,
        resolves, recon — so this is where history hears about all of them.
        The row it writes names no session, because at this point nothing on
        the wire says which one: the upsert keeps whatever the submit recorded,
        and an order that was never ours stays honestly unattributed.
        """
        self._record_order(order)
        level = (
            "warn"
            if order.status in (OrderStatus.UNKNOWN, OrderStatus.REJECTED)
            else "info"
        )
        await self._td_log(
            (
                f"order update cid={order.client_order_id or order.order_id} "
                f"{order.status.value} {order.side} "
                f"filled={order.filled_qty}/{order.qty} "
                f"{order.universal_ticker}"
            ),
            level=level,
        )
        await self._publish_global(TD_ORDER_UPDATE, order)

    async def _td_log(self, message: str, *, level: str = "info") -> None:
        try:
            await publish_td_log(
                self.broker,
                self.api_id,
                message,
                source="td",
                level=level,
            )
        except Exception:
            logger.exception(
                "TD session log failed api_id=%s", self.api_id
            )

    def _dispatch_position(self, position: Position) -> None:
        """Fan out one position change and announce it.

        Announced rather than merely booked: a position moves for reasons no
        fill of ours reports — funding, ADL, liquidation — so a strategy that
        inferred its exposure from its own fills would drift, and only the
        venue can say when.
        """
        for cb in list(self._position_cbs):
            cb(position)
        self._schedule_publish(
            self._publish_global(TD_POSITION_UPDATE, position)
        )

    def _dispatch_balance(self, balance: Balance) -> None:
        # The ledger sees the venue's number first so anything reading it
        # downstream of this callback already has the fresh figure.
        self.ledger.apply_venue(balance)
        for cb in list(self._balance_cbs):
            cb(balance)
        self._schedule_publish(self._store_then_announce_balance(balance.asset))

    async def _store_then_announce_balance(self, asset: str) -> None:
        try:
            await self.write_ledger(asset)
        except Exception:
            logger.exception(
                "TD ledger write failed api_id=%s asset=%s", self.api_id, asset
            )
        await self._publish_balance(asset)

    def _on_oms_update(self, view: OmsView) -> None:
        if self._destroyed or not self._started:
            return
        self._schedule_publish(self._safe_publish_oms(view))

    def _schedule_publish(self, coro: Any) -> None:
        if self._destroyed:
            return
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            return
        loop.create_task(coro)

    async def _safe_publish_oms(self, view: OmsView) -> None:
        try:
            await self.publish_oms(view)
        except Exception:
            logger.exception("failed to publish OMS view api_id=%s", self.api_id)

    async def _publish_global(self, type_: str, payload: BaseModel) -> None:
        if self._destroyed:
            return
        try:
            await self.broker.publish(
                Topics.td_global(self.api_id),
                UntypedEnvelope.wrap(
                    payload.model_dump(mode="json"),
                    type=type_,
                    source="td",
                    session_id=str(self.api_id),
                ),
            )
        except Exception:
            logger.exception(
                "failed to publish %s on td.%s.global", type_, self.api_id
            )
