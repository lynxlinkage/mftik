"""Trading session — exchange connectivity + OMS pub/sub."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Callable
from typing import Any

from mft.broker import Broker
from mft.exchange.base import PrivateClient
from mft.exchange.models import (
    Balance,
    Fill,
    Instrument,
    Order,
    OrderStatus,
    PlaceOrderRequest,
    can_transition,
    is_pending,
    is_terminal,
)
from mft.exchange.oms import LedgerEntry, LedgerView, OmsView, Position
from mft.protocol import (
    TD_BALANCE_UPDATE,
    TD_CANCEL_REJECT,
    TD_FILL,
    TD_ORDER_REJECT,
    TD_ORDER_UPDATE,
    CancelReject,
    OrderReject,
    RejectCode,
    Topics,
    UntypedEnvelope,
)
from mft.symbols import SymbolClient
from pydantic import BaseModel

from mft_td.oms import (
    InsufficientAvailable,
    Ledger,
    Oms,
    order_key,
    reservation_for,
)

logger = logging.getLogger(__name__)

#: A venue that has not acknowledged an order in this long is not going to.
#: Generous enough to ride out a slow round-trip; short enough that a strategy
#: is not left believing an order is in flight for minutes.
PENDING_NEW_TIMEOUT_S = 5.0

#: How often the sweeper looks for aged-out PENDING_NEW orders.
PENDING_SWEEP_INTERVAL_S = 1.0

OrderCallback = Callable[[Order], None]
FillCallback = Callable[[Fill], None]
BalanceCallback = Callable[[Balance], None]


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
        private: PrivateClient,
        oms: Oms | None = None,
        symbols: SymbolClient | None = None,
        ledger: Ledger | None = None,
        pending_timeout: float = PENDING_NEW_TIMEOUT_S,
    ) -> None:
        self.api_id = api_id
        self.broker = broker
        self.private = private
        self.oms = oms or Oms()
        #: Balances with TD's own pre-locks layered on the venue's numbers.
        self.ledger = ledger or Ledger()
        #: Resolves base/quote so a reservation knows which asset it holds.
        #: Optional: without it TD still trades, it just cannot pre-lock.
        self.symbols = symbols
        self._order_cbs: list[OrderCallback] = []
        self._fill_cbs: list[FillCallback] = []
        self._balance_cbs: list[BalanceCallback] = []
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

        self.oms.bind(self)
        self.oms.on_update(self._on_oms_update)

    @property
    def destroyed(self) -> bool:
        return self._destroyed

    @property
    def venue(self) -> str:
        """Canonical venue name, which is what the client calls itself.

        Adapters name themselves after their entry in ``mft.exchange.venues``,
        so this is also the key error normalization looks the venue up by.
        """
        return self.private.name

    def on_order(self, cb: OrderCallback) -> None:
        self._order_cbs.append(cb)

    def on_fill(self, cb: FillCallback) -> None:
        self._fill_cbs.append(cb)

    def on_balance(self, cb: BalanceCallback) -> None:
        self._balance_cbs.append(cb)

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
        ]
        # A dead socket looks exactly like a quiet venue, so rebuild from the
        # venue rather than trusting a book that stopped updating.
        self.private.on_reconnect(self._on_venue_reconnect)
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
        """Query venue open orders / positions / balances into OMS and publish."""
        if self._destroyed:
            raise RuntimeError(f"session api_id={self.api_id} is destroyed")
        orders = await self.private.fetch_open_orders()
        balances = await self.private.fetch_balances()
        positions: list[Position] | None = None
        if type(self.private).fetch_positions is not PrivateClient.fetch_positions:
            positions = list(await self.private.fetch_positions())

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
        self.ledger.apply_venue_many(list(balances))
        await self.write_ledger()
        await self.publish_oms(view)
        return view

    async def clear_state(self) -> None:
        """Delete this account's Redis state. Call when the session dies.

        State that outlives the session it describes is a trap: a strategy
        attaching later reads balances and orders that nobody is updating and
        has no way to tell they are dead.
        """
        self.ledger.release_all()
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

    async def record_pending_new(self, request: PlaceOrderRequest) -> Order:
        """Book the order locally before it goes to the venue.

        Written and announced before the submit is acked, so a strategy that
        is told True can immediately read the order it just placed. It carries
        no venue ``order_id`` yet — until the venue answers there is nothing to
        cancel, which is exactly what ``PENDING_NEW`` means.
        """
        order = Order(
            client_order_id=request.client_order_id,
            symbol=request.symbol,
            side=request.side,
            type=request.type,
            status=OrderStatus.PENDING_NEW,
            qty=request.qty,
            price=request.price,
        )
        self.oms.handle_order(order)
        self._pending_since[order.client_order_id or order.order_id] = (
            asyncio.get_running_loop().time()
        )
        await self.write_order(order)
        await self._publish_global(TD_ORDER_UPDATE, order)
        return order

    async def record_rejected(self, client_order_id: str) -> Order | None:
        """Settle a booked order the venue refused.

        The submit path publishes its own reject envelope, but the order it
        booked ``PENDING_NEW`` is still sitting in the book — without this it
        would linger until the sweeper called it ``UNKNOWN`` and chased a
        venue that already said no.
        """
        self._pending_since.pop(client_order_id, None)
        order = self.oms.get_order(client_order_id)
        if order is None or is_terminal(order.status):
            return None
        rejected = order.model_copy(update={"status": OrderStatus.REJECTED})
        self.oms.handle_order(rejected)
        await self.write_order(rejected)
        return rejected

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
        await self._publish_global(TD_ORDER_UPDATE, pending)
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
        await self._publish_global(TD_ORDER_UPDATE, restored)
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
            order = self.oms.get_order(cid)
            if order is None or not is_pending(order.status):
                continue
            unknown = order.model_copy(update={"status": OrderStatus.UNKNOWN})
            self.oms.handle_order(unknown)
            await self.write_order(unknown)
            await self._publish_global(TD_ORDER_UPDATE, unknown)
            logger.warning(
                "TD %s unanswered after %ss api_id=%s cid=%s → UNKNOWN",
                order.status.value,
                self.pending_timeout,
                self.api_id,
                cid,
            )
            moved.append(unknown)
            await self.resolve_unknown(unknown)
        return moved

    async def resolve_unknown(self, order: Order) -> Order | None:
        """Ask the venue what actually happened to an UNKNOWN order.

        A ``None`` from the venue is a real answer — the order never landed —
        so it becomes REJECTED and its reservation goes back. A venue that
        cannot answer leaves the order UNKNOWN for recon to settle.
        """
        cid = order.client_order_id
        if not cid:
            return None
        try:
            found = await self.private.fetch_order_by_client_order_id(
                cid, symbol=order.symbol
            )
        except NotImplementedError:
            logger.debug(
                "TD venue %s cannot resolve by client_order_id; leaving "
                "cid=%s UNKNOWN for recon",
                self.private.name,
                cid,
            )
            return None
        except Exception:
            logger.exception(
                "TD resolve failed api_id=%s cid=%s", self.api_id, cid
            )
            return None

        settled = found or order.model_copy(
            update={"status": OrderStatus.REJECTED}
        )
        self.oms.handle_order(settled)
        await self.write_order(settled)
        await self._publish_global(TD_ORDER_UPDATE, settled)
        self._cancel_since.pop(cid, None)
        await self.release(cid)
        logger.info(
            "TD resolved UNKNOWN api_id=%s cid=%s → %s",
            self.api_id,
            cid,
            settled.status,
        )
        return settled

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

    async def reserve(self, request: PlaceOrderRequest) -> str | None:
        """Pre-lock the funds ``request`` commits. None on success, else why not.

        Returning a reason means nothing was reserved and the caller must not
        send the order. An order whose commitment cannot be priced (a market
        buy) or whose instrument cannot be resolved goes through unreserved
        rather than being blocked — the ledger is a guard against overspending
        what we know about, not a gate that fails closed on missing data.
        """
        instrument = await self._instrument(request.symbol)
        if instrument is None:
            logger.debug(
                "TD no instrument for %s api_id=%s; submitting unreserved",
                request.symbol,
                self.api_id,
            )
            return None
        held = reservation_for(request, instrument)
        if held is None:
            logger.debug(
                "TD cannot price %s %s; submitting unreserved",
                request.type,
                request.symbol,
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

    async def _instrument(self, symbol: str) -> Instrument | None:
        if self.symbols is None:
            return None
        try:
            info = await self.symbols.get(self.private.name, symbol)
        except Exception:
            logger.debug(
                "TD symbol lookup failed venue=%s symbol=%s",
                self.private.name,
                symbol,
                exc_info=True,
            )
            return None
        return Instrument(
            symbol=info.symbol, base=info.base, quote=info.quote
        )

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
        symbol: str | None = None,
        error_code: int | str = RejectCode.NONE,
    ) -> None:
        """Publish a submit reject on ``td.{api_id}.global``.

        ``reason`` is what a human reads; ``error_code`` is what a strategy
        branches on. Callers that have an exception should run it through
        :func:`mft_td.errors.normalize` rather than picking a code by hand.
        """
        await self._publish_global(
            TD_ORDER_REJECT,
            OrderReject(
                api_id=self.api_id,
                client_order_id=client_order_id,
                order_id=order_id,
                symbol=symbol,
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
            # the venue said no after accepting the request, and the stream
            # carries no reason with it. VENUE_REJECTED is the honest code —
            # the venue refused it, and that is all anyone knows.
            await self.publish_order_reject(
                reason="rejected",
                client_order_id=order.client_order_id,
                order_id=order.order_id,
                symbol=order.symbol,
                error_code=RejectCode.VENUE_REJECTED,
            )
        else:
            await self._publish_global(TD_ORDER_UPDATE, order)

    def _dispatch_fill(self, fill: Fill) -> None:
        for cb in list(self._fill_cbs):
            cb(fill)
        self._schedule_publish(self._publish_global(TD_FILL, fill))

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
