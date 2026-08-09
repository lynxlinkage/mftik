"""In-process paper venue — shared state for public + private clients."""

from __future__ import annotations

import asyncio
import logging
import random
import uuid
from collections.abc import Callable
from decimal import ROUND_HALF_UP, Decimal
from typing import TYPE_CHECKING, Any

from mft.exchange.errors import (
    InstrumentNotFoundError,
    InsufficientBalanceError,
    OrderError,
)
from mft.exchange.models import (
    Balance,
    BookLevel,
    Fill,
    Instrument,
    Order,
    OrderBook,
    OrderStatus,
    OrderType,
    PlaceOrderRequest,
    Side,
    Ticker,
    TimeInForce,
    Trade,
)
from mft.exchange.stream import EventStream
from mft.exchange.symbols import check_venue
from mft.exchange.tickers import Category, UniversalTicker

#: The venue and market every paper instrument is on. Hardcoded because paper
#: really is one of each — there is no plane to ask, and no second book to be
#: wrong about.
PAPER_VENUE = "Paper"
PAPER_CATEGORY = Category.SPOT

if TYPE_CHECKING:
    from mft.exchange.paper.private import PaperPrivateClient
    from mft.exchange.paper.public import PaperPublicClient

logger = logging.getLogger(__name__)

# symbol: (base, quote, mid, bid_px, bid_qty, ask_px, ask_qty)
#: symbol → base, quote, mid, bid_px, bid_qty, ask_px, ask_qty
_DEFAULT_SYMBOLS: dict[
    str, tuple[str, str, Decimal, Decimal, Decimal, Decimal, Decimal]
] = {
    "BTCUSDT": (
        "BTC",
        "USDT",
        Decimal("50000"),
        Decimal("49999"),
        Decimal("1"),
        Decimal("50001"),
        Decimal("1"),
    ),
    "ETHUSDT": (
        "ETH",
        "USDT",
        Decimal("3000"),
        Decimal("2999"),
        Decimal("1"),
        Decimal("3001"),
        Decimal("1"),
    ),
}

#: symbol → tick_size, lot_size, min_qty, min_notional. Real venues publish
#: these per instrument; paper does too so the symbol plane has something
#: truthful to serve and strategies exercise the same rounding path.
_DEFAULT_FILTERS: dict[
    str, tuple[Decimal, Decimal, Decimal, Decimal]
] = {
    "BTCUSDT": (
        Decimal("0.01"),
        Decimal("0.00001"),
        Decimal("0.00001"),
        Decimal("5"),
    ),
    "ETHUSDT": (
        Decimal("0.01"),
        Decimal("0.0001"),
        Decimal("0.0001"),
        Decimal("5"),
    ),
}

_FALLBACK_FILTERS = (
    Decimal("0.01"),
    Decimal("0.0001"),
    Decimal("0.0001"),
    Decimal("5"),
)


def paper_ticker(symbol: str) -> UniversalTicker:
    """The universal identity of a symbol on the paper venue.

    Built rather than looked up: paper is one venue with one market, and it
    spells pairs the canonical way — so unlike a real adapter there is no
    symbol plane in the loop.
    """
    return UniversalTicker.of(PAPER_VENUE, PAPER_CATEGORY, symbol)


def _instrument(symbol: str, base: str, quote: str) -> Instrument:
    tick, lot, min_qty, min_notional = _DEFAULT_FILTERS.get(
        symbol, _FALLBACK_FILTERS
    )
    return Instrument(
        symbol=symbol,
        base=base,
        quote=quote,
        tick_size=tick,
        lot_size=lot,
        min_qty=min_qty,
        min_notional=min_notional,
    )


# Optional hooks for the paper-engine Redis bridge (sync; may schedule work).
OrderSink = Callable[[str, Order], Any]
FillSink = Callable[[str, Fill], Any]
BalanceSink = Callable[[str, Balance], Any]


class PaperExchange:
    """Fake exchange engine used by paper public/private clients.

    Default BTCUSDT top of book: bid ``[[49999, 1]]``, ask ``[[50001, 1]]``.

    Lifecycle::

        async with PaperExchange() as ex:
            public = ex.public()
            private = ex.private()
            await public.connect()
            await private.connect()
    """

    name = "paper"

    def __init__(
        self,
        *,
        symbols: dict[str, Decimal] | None = None,
        tick_interval: float = 0.2,
        fee_bps: Decimal = Decimal("5"),
        volatility_bps: Decimal = Decimal("0"),
        initial_balances: dict[str, Decimal] | None = None,
        seed: int | None = 1,
        on_order: OrderSink | None = None,
        on_fill: FillSink | None = None,
        on_balance: BalanceSink | None = None,
    ) -> None:
        self.tick_interval = tick_interval
        self.fee_bps = fee_bps
        self.volatility_bps = volatility_bps
        self._rng = random.Random(seed)
        self._on_order = on_order
        self._on_fill = on_fill
        self._on_balance = on_balance

        self._instruments: dict[str, Instrument] = {}
        self._mid: dict[str, Decimal] = {}
        # symbol → (bid_px, bid_qty, ask_px, ask_qty)
        self._books: dict[str, tuple[Decimal, Decimal, Decimal, Decimal]] = {}
        if symbols is None:
            for symbol, (
                base,
                quote,
                mid,
                bid_px,
                bid_qty,
                ask_px,
                ask_qty,
            ) in _DEFAULT_SYMBOLS.items():
                self._instruments[symbol] = _instrument(symbol, base, quote)
                self._mid[symbol] = mid
                self._books[symbol] = (bid_px, bid_qty, ask_px, ask_qty)
        else:
            for symbol, mid in symbols.items():
                base, quote = _split_symbol(symbol)
                self._instruments[symbol] = _instrument(symbol, base, quote)
                self._mid[symbol] = mid
                # 1-tick book around mid when custom symbols are supplied
                tick = Decimal("1")
                self._books[symbol] = (
                    mid - tick,
                    Decimal("1"),
                    mid + tick,
                    Decimal("1"),
                )

        self._default_balances = dict(
            initial_balances
            or {
                "USDT": Decimal("100000"),
                "BTC": Decimal("1"),
                "ETH": Decimal("10"),
            }
        )
        # api_key → secret (paper accounts are keyed by api_key)
        self._api_secrets: dict[str, str] = {}
        self._api_passphrases: dict[str, str | None] = {}
        # account → asset → free / locked
        self._free: dict[str, dict[str, Decimal]] = {}
        self._locked: dict[str, dict[str, Decimal]] = {}
        self._orders: dict[str, Order] = {}
        self._order_account: dict[str, str] = {}
        self._open_by_account: dict[str, set[str]] = {}
        # account → client_order_id → order_id
        self._client_order_ids: dict[str, dict[str, str]] = {}

        self._ticker_subs: dict[str, set[EventStream[Ticker]]] = {}
        self._trade_subs: dict[str, set[EventStream[Trade]]] = {}
        self._book_subs: dict[str, set[EventStream[OrderBook]]] = {}
        self._order_subs: dict[str, set[EventStream[Order]]] = {}
        self._fill_subs: dict[str, set[EventStream[Fill]]] = {}
        self._balance_subs: dict[str, set[EventStream[Balance]]] = {}

        self._task: asyncio.Task[None] | None = None
        self._started = False
        self._lock = asyncio.Lock()

    # --- lifecycle ---------------------------------------------------------

    async def start(self) -> None:
        if self._started:
            return
        self._started = True
        self._task = asyncio.create_task(self._run(), name="paper-exchange")
        logger.info("Paper exchange started symbols=%s", list(self._mid))

    async def stop(self) -> None:
        self._started = False
        if self._task is not None:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None
        for bucket in (
            self._ticker_subs,
            self._trade_subs,
            self._book_subs,
            self._order_subs,
            self._fill_subs,
            self._balance_subs,
        ):
            for streams in bucket.values():
                for stream in list(streams):
                    stream.close()
        logger.info("Paper exchange stopped")

    async def __aenter__(self) -> PaperExchange:
        await self.start()
        return self

    async def __aexit__(self, *args: object) -> None:
        await self.stop()

    def public(self) -> PaperPublicClient:
        from mft.exchange.paper.public import PaperPublicClient

        return PaperPublicClient(self)

    def register_api(
        self,
        api_key: str,
        api_secret: str,
        *,
        passphrase: str | None = None,
        balances: dict[str, Decimal] | None = None,
    ) -> None:
        """Register / upsert a paper API credential → isolated account."""
        if not api_key or not api_secret:
            raise OrderError("api_key and api_secret are required")
        self._api_secrets[api_key] = api_secret
        self._api_passphrases[api_key] = passphrase
        self._ensure_account(api_key, balances)

    def authenticate(
        self,
        api_key: str,
        api_secret: str,
        *,
        passphrase: str | None = None,
    ) -> None:
        """Validate paper credentials (called from private client connect)."""
        from mft.exchange.paper.private import PaperAuthError

        expected = self._api_secrets.get(api_key)
        if expected is None:
            raise PaperAuthError(f"unknown paper api_key={api_key!r}")
        if expected != api_secret:
            raise PaperAuthError("invalid paper api_secret")
        if self._api_passphrases.get(api_key) != passphrase:
            raise PaperAuthError("invalid paper passphrase")

    def private(
        self,
        *,
        api_key: str,
        api_secret: str,
        passphrase: str | None = None,
        auto_register: bool = True,
    ) -> PaperPrivateClient:
        """Create a private client for ``api_key`` (isolated paper account)."""
        from mft.exchange.paper.private import PaperPrivateClient

        if api_key not in self._api_secrets:
            if not auto_register:
                from mft.exchange.paper.private import PaperAuthError

                raise PaperAuthError(f"unknown paper api_key={api_key!r}")
            self.register_api(api_key, api_secret, passphrase=passphrase)
        return PaperPrivateClient(
            self,
            api_key=api_key,
            api_secret=api_secret,
            passphrase=passphrase,
        )

    def _ensure_account(
        self,
        api_key: str,
        balances: dict[str, Decimal] | None = None,
    ) -> None:
        if api_key not in self._free:
            self._free[api_key] = dict(balances or self._default_balances)
            self._locked[api_key] = {}
            self._open_by_account[api_key] = set()
            self._client_order_ids[api_key] = {}

    # --- market data (req-reply) -------------------------------------------

    def list_instruments(self) -> list[Instrument]:
        return list(self._instruments.values())

    def get_ticker(self, symbol: str) -> Ticker:
        self._require_instrument(symbol)
        bid_px, ask_px = self._bbo_prices(symbol)
        mid = self._mid[symbol]
        return Ticker(
            universal_ticker=str(paper_ticker(symbol)),
            bid=bid_px,
            ask=ask_px,
            last=mid,
        )

    def get_order_book(self, symbol: str, *, depth: int = 10) -> OrderBook:
        self._require_instrument(symbol)
        bids_map: dict[Decimal, Decimal] = {}
        asks_map: dict[Decimal, Decimal] = {}
        for _account, order, remaining in self._resting(symbol):
            assert order.price is not None
            book = bids_map if order.side is Side.BUY else asks_map
            book[order.price] = book.get(order.price, Decimal("0")) + remaining
        if not bids_map and not asks_map:
            # Empty book fallback around mid (display only; no synthetic liquidity).
            bid_px, _bq, ask_px, _aq = self._books[symbol]
            bids_map[bid_px] = Decimal("1")
            asks_map[ask_px] = Decimal("1")
        bids = [
            BookLevel(price=p, qty=q)
            for p, q in sorted(bids_map.items(), reverse=True)[:depth]
        ]
        asks = [
            BookLevel(price=p, qty=q)
            for p, q in sorted(asks_map.items())[:depth]
        ]
        return OrderBook(
            universal_ticker=str(paper_ticker(symbol)), bids=bids, asks=asks
        )

    # --- trading (req-reply) -----------------------------------------------

    async def place_order(self, account: str, request: PlaceOrderRequest) -> Order:
        async with self._lock:
            return self._place_order_locked(account, request)

    def _place_order_locked(self, account: str, request: PlaceOrderRequest) -> Order:
        check_venue(request.ticker, PAPER_VENUE, {PAPER_CATEGORY})
        inst = self._require_instrument(request.symbol)
        if request.qty <= 0:
            raise OrderError("qty must be positive")
        if request.type is OrderType.LIMIT and request.price is None:
            raise OrderError("limit orders require price")

        client_order_id = self._allocate_client_order_id(
            account, request.client_order_id
        )
        limit_price: Decimal | None = None
        if request.type is OrderType.LIMIT:
            assert request.price is not None
            limit_price = _round_price(request.price, inst.tick_size)

        if request.tif is TimeInForce.FOK:
            # All-or-nothing has to be decided before anything trades, so it
            # is depth that is checked, not the result of a partial match.
            available = sum(
                (
                    row[2]
                    for row in self._sorted_makers(
                        request.symbol,
                        request.side,
                        exclude_account=account,
                        limit_price=limit_price,
                    )
                ),
                Decimal("0"),
            )
            if available < request.qty:
                raise OrderError(
                    f"fill-or-kill needs {request.qty}, book has {available}"
                )

        if request.tif is TimeInForce.POST_ONLY:
            # Refuse before the order exists rather than matching and undoing
            # it: post-only is the venue promising it will never take, and a
            # strategy chasing the book relies on that refusal to know its
            # price crossed. Checked against the same makers the taker pass
            # would have hit, so the two can never disagree.
            crossable = self._sorted_makers(
                request.symbol,
                request.side,
                exclude_account=account,
                limit_price=limit_price,
            )
            if crossable:
                raise OrderError(
                    f"post-only order would cross at {limit_price} "
                    f"(best opposite {crossable[0][1].price})"
                )

        order = Order(
            client_order_id=client_order_id,
            universal_ticker=request.universal_ticker,
            side=request.side,
            type=request.type,
            # Not yet matched — the engine overwrites this before it emits.
            status=OrderStatus.PENDING_NEW,
            qty=request.qty,
            price=limit_price,
            filled_qty=Decimal("0"),
            avg_price=None,
        )
        self._register_order(account, order)

        filled_qty, notional = self._match_taker_locked(
            account,
            order,
            limit_price=limit_price,
        )
        remaining = request.qty - filled_qty

        if remaining > 0 and request.type is OrderType.MARKET:
            raise OrderError("insufficient liquidity for market order")

        if remaining > 0 and request.tif is TimeInForce.IOC:
            # IOC keeps what crossed and drops the rest. The remainder never
            # rests, so it is never reserved — locking funds for it would fail
            # orders the account can perfectly well afford to have filled.
            # Acknowledged before being cancelled because PENDING_NEW →
            # CANCELED is not a legal transition.
            avg = (notional / filled_qty) if filled_qty > 0 else None
            acked = order.model_copy(
                update={
                    "status": (
                        OrderStatus.PARTIALLY_FILLED
                        if filled_qty > 0
                        else OrderStatus.NEW
                    ),
                    "filled_qty": filled_qty,
                    "avg_price": avg,
                }
            )
            self._orders[acked.order_id] = acked
            self._emit_order(account, acked)
            done = acked.model_copy(update={"status": OrderStatus.CANCELED})
            self._orders[done.order_id] = done
            self._emit_order(account, done)
            self._emit_balances(account)
            return done

        if remaining > 0:
            return self._rest_order_locked(
                account, inst, request, order, filled_qty, notional, remaining
            )

        avg = notional / filled_qty if filled_qty > 0 else None
        order = order.model_copy(
            update={
                "status": OrderStatus.FILLED,
                "filled_qty": filled_qty,
                "avg_price": avg,
            }
        )
        self._orders[order.order_id] = order
        self._emit_order(account, order)
        self._emit_balances(account)
        return order

    def _rest_order_locked(
        self,
        account: str,
        inst,
        request: PlaceOrderRequest,
        order: Order,
        filled_qty: Decimal,
        notional: Decimal,
        remaining: Decimal,
    ) -> Order:
        """Put the unfilled size on the book — locked at the limit price."""
        limit_price = order.price
        assert limit_price is not None
        self._reserve_and_settle(
            account,
            inst,
            request.side,
            remaining,
            limit_price,
            lock_only=True,
        )
        status = (
            OrderStatus.PARTIALLY_FILLED if filled_qty > 0 else OrderStatus.NEW
        )
        avg = (notional / filled_qty) if filled_qty > 0 else None
        order = order.model_copy(
            update={
                "status": status,
                "filled_qty": filled_qty,
                "avg_price": avg,
            }
        )
        self._orders[order.order_id] = order
        self._open_by_account.setdefault(account, set()).add(order.order_id)
        self._emit_order(account, order)
        self._emit_balances(account)
        return order

    async def cancel_order(self, account: str, order_id: str) -> Order:
        async with self._lock:
            return self._cancel_order_locked(account, order_id)

    async def cancel_by_client_order_id(
        self, account: str, client_order_id: str
    ) -> Order:
        async with self._lock:
            order_id = self._client_order_ids.get(account, {}).get(client_order_id)
            if order_id is None:
                raise OrderError(f"unknown client_order_id={client_order_id}")
            return self._cancel_order_locked(account, order_id)

    def _cancel_order_locked(self, account: str, order_id: str) -> Order:
        order = self._orders.get(order_id)
        if order is None:
            raise OrderError(f"unknown order_id={order_id}")
        open_ids = self._open_by_account.get(account, set())
        if order_id not in open_ids:
            raise OrderError(f"order {order_id} is not open for account={account}")
        inst = self._require_instrument(order.symbol)
        assert order.price is not None
        remaining = order.qty - order.filled_qty
        if remaining > 0:
            self._unlock(account, inst, order.side, remaining, order.price)
        canceled = order.model_copy(update={"status": OrderStatus.CANCELED})
        self._orders[order_id] = canceled
        open_ids.remove(order_id)
        self._emit_order(account, canceled)
        self._emit_balances(account)
        return canceled

    def get_order(self, order_id: str) -> Order:
        order = self._orders.get(order_id)
        if order is None:
            raise OrderError(f"unknown order_id={order_id}")
        return order

    def get_order_by_client_id(self, account: str, client_order_id: str) -> Order:
        order_id = self._client_order_ids.get(account, {}).get(client_order_id)
        if order_id is None:
            raise OrderError(f"unknown client_order_id={client_order_id}")
        return self.get_order(order_id)

    def list_open_orders(
        self, account: str, symbol: str | None = None
    ) -> list[Order]:
        ids = self._open_by_account.get(account, set())
        orders = [self._orders[i] for i in ids]
        if symbol is not None:
            orders = [o for o in orders if o.symbol == symbol]
        return orders

    def list_balances(self, account: str) -> list[Balance]:
        free = self._free.get(account, {})
        locked = self._locked.get(account, {})
        assets = sorted(set(free) | set(locked))
        return [
            Balance(
                asset=asset,
                free=free.get(asset, Decimal("0")),
                locked=locked.get(asset, Decimal("0")),
            )
            for asset in assets
        ]

    # --- subscriptions -----------------------------------------------------

    def subscribe_ticker(self, symbol: str) -> EventStream[Ticker]:
        self._require_instrument(symbol)
        stream: EventStream[Ticker] = EventStream(
            on_close=lambda s: self._ticker_subs.get(symbol, set()).discard(s)
        )
        self._ticker_subs.setdefault(symbol, set()).add(stream)
        stream.push(self.get_ticker(symbol))
        return stream

    def subscribe_trades(self, symbol: str) -> EventStream[Trade]:
        self._require_instrument(symbol)
        stream: EventStream[Trade] = EventStream(
            on_close=lambda s: self._trade_subs.get(symbol, set()).discard(s)
        )
        self._trade_subs.setdefault(symbol, set()).add(stream)
        return stream

    def subscribe_order_book(self, symbol: str) -> EventStream[OrderBook]:
        self._require_instrument(symbol)
        stream: EventStream[OrderBook] = EventStream(
            on_close=lambda s: self._book_subs.get(symbol, set()).discard(s)
        )
        self._book_subs.setdefault(symbol, set()).add(stream)
        stream.push(self.get_order_book(symbol))
        return stream

    def subscribe_orders(self, account: str) -> EventStream[Order]:
        stream: EventStream[Order] = EventStream(
            on_close=lambda s: self._order_subs.get(account, set()).discard(s)
        )
        self._order_subs.setdefault(account, set()).add(stream)
        return stream

    def subscribe_fills(self, account: str) -> EventStream[Fill]:
        stream: EventStream[Fill] = EventStream(
            on_close=lambda s: self._fill_subs.get(account, set()).discard(s)
        )
        self._fill_subs.setdefault(account, set()).add(stream)
        return stream

    def subscribe_balances(self, account: str) -> EventStream[Balance]:
        stream: EventStream[Balance] = EventStream(
            on_close=lambda s: self._balance_subs.get(account, set()).discard(s)
        )
        self._balance_subs.setdefault(account, set()).add(stream)
        for bal in self.list_balances(account):
            stream.push(bal)
        return stream

    # --- internals ---------------------------------------------------------

    def _require_instrument(self, symbol: str) -> Instrument:
        inst = self._instruments.get(symbol)
        if inst is None:
            raise InstrumentNotFoundError(symbol)
        return inst

    def _allocate_client_order_id(
        self, account: str, client_order_id: str | None
    ) -> str:
        cid = client_order_id or uuid.uuid4().hex
        index = self._client_order_ids.setdefault(account, {})
        if cid in index:
            raise OrderError(f"duplicate client_order_id={cid}")
        return cid

    def _register_order(self, account: str, order: Order) -> None:
        assert order.client_order_id is not None
        self._orders[order.order_id] = order
        self._order_account[order.order_id] = account
        self._client_order_ids.setdefault(account, {})[
            order.client_order_id
        ] = order.order_id

    def _resting(
        self, symbol: str
    ) -> list[tuple[str, Order, Decimal]]:
        out: list[tuple[str, Order, Decimal]] = []
        for account, ids in self._open_by_account.items():
            for order_id in ids:
                order = self._orders[order_id]
                if order.symbol != symbol or order.price is None:
                    continue
                remaining = order.qty - order.filled_qty
                if remaining > 0:
                    out.append((account, order, remaining))
        return out

    def _bbo_prices(self, symbol: str) -> tuple[Decimal, Decimal]:
        bids = [o.price for _a, o, _r in self._resting(symbol) if o.side is Side.BUY]
        asks = [o.price for _a, o, _r in self._resting(symbol) if o.side is Side.SELL]
        bid_px, _bq, ask_px, _aq = self._books[symbol]
        if bids:
            bid_px = max(bids)  # type: ignore[arg-type]
        if asks:
            ask_px = min(asks)  # type: ignore[arg-type]
        return bid_px, ask_px

    def _match_taker_locked(
        self,
        taker_account: str,
        taker: Order,
        *,
        limit_price: Decimal | None,
    ) -> tuple[Decimal, Decimal]:
        """Match ``taker`` against opposite resting orders.

        Returns ``(filled_qty, notional)``.
        """
        inst = self._instruments[taker.symbol]
        remaining = taker.qty - taker.filled_qty
        filled = Decimal("0")
        notional = Decimal("0")
        makers = self._sorted_makers(
            taker.symbol,
            taker.side,
            exclude_account=taker_account,
            limit_price=limit_price,
        )
        for maker_account, maker, maker_rem in makers:
            if remaining <= 0:
                break
            qty = min(remaining, maker_rem)
            assert maker.price is not None
            fill_price = maker.price
            # Maker: unlock locked size then settle at trade price.
            self._unlock(
                maker_account, inst, maker.side, qty, maker.price
            )
            self._reserve_and_settle(
                maker_account,
                inst,
                maker.side,
                qty,
                fill_price,
                lock_only=False,
            )
            # Taker: settle immediately (no prior lock on matched size).
            self._reserve_and_settle(
                taker_account,
                inst,
                taker.side,
                qty,
                fill_price,
                lock_only=False,
            )
            maker_filled = maker.filled_qty + qty
            if maker_filled >= maker.qty:
                maker_status = OrderStatus.FILLED
                self._open_by_account.get(maker_account, set()).discard(
                    maker.order_id
                )
            else:
                maker_status = OrderStatus.PARTIALLY_FILLED
            maker_avg = (
                ((maker.avg_price or Decimal("0")) * maker.filled_qty)
                + fill_price * qty
            ) / maker_filled
            maker_updated = maker.model_copy(
                update={
                    "status": maker_status,
                    "filled_qty": maker_filled,
                    "avg_price": maker_avg,
                }
            )
            self._orders[maker.order_id] = maker_updated
            maker_fill = Fill(
                order_id=maker.order_id,
                client_order_id=maker.client_order_id,
                universal_ticker=maker.universal_ticker,
                side=maker.side,
                price=fill_price,
                qty=qty,
                fee=_fee(fill_price, qty, self.fee_bps),
                fee_asset=inst.quote,
            )
            taker_fill = Fill(
                order_id=taker.order_id,
                client_order_id=taker.client_order_id,
                universal_ticker=taker.universal_ticker,
                side=taker.side,
                price=fill_price,
                qty=qty,
                fee=_fee(fill_price, qty, self.fee_bps),
                fee_asset=inst.quote,
            )
            self._emit_order(maker_account, maker_updated)
            self._emit_fill(maker_account, maker_fill)
            self._emit_balances(maker_account)
            self._emit_fill(taker_account, taker_fill)
            self._emit_public_trade(
                Trade(
                    universal_ticker=taker.universal_ticker,
                    price=fill_price,
                    qty=qty,
                    side=taker.side,
                )
            )
            filled += qty
            notional += fill_price * qty
            remaining -= qty
        return filled, notional

    def _sorted_makers(
        self,
        symbol: str,
        taker_side: Side,
        *,
        exclude_account: str,
        limit_price: Decimal | None,
    ) -> list[tuple[str, Order, Decimal]]:
        maker_side = Side.SELL if taker_side is Side.BUY else Side.BUY
        out: list[tuple[str, Order, Decimal]] = []
        for account, order, remaining in self._resting(symbol):
            if account == exclude_account or order.side is not maker_side:
                continue
            assert order.price is not None
            if limit_price is not None:
                if taker_side is Side.BUY and order.price > limit_price:
                    continue
                if taker_side is Side.SELL and order.price < limit_price:
                    continue
            out.append((account, order, remaining))
        # BUY taker: lowest ask first; SELL taker: highest bid first.
        if taker_side is Side.BUY:
            out.sort(key=lambda row: (row[1].price or Decimal("0"), row[1].ts))
        else:
            out.sort(
                key=lambda row: (row[1].price or Decimal("0"), row[1].ts),
                reverse=True,
            )
        return out

    async def _run(self) -> None:
        try:
            while self._started:
                async with self._lock:
                    self._tick_locked()
                await asyncio.sleep(self.tick_interval)
        except asyncio.CancelledError:
            raise

    def _tick_locked(self) -> None:
        for symbol, mid in list(self._mid.items()):
            if self.volatility_bps > 0:
                move = mid * self.volatility_bps / Decimal("10000")
                delta = Decimal(str(self._rng.uniform(-1.0, 1.0))) * move
                inst = self._instruments[symbol]
                new_mid = max(mid + delta, inst.tick_size)
                self._mid[symbol] = _round_price(new_mid, inst.tick_size)
                tick = inst.tick_size
                self._books[symbol] = (
                    self._mid[symbol] - tick,
                    Decimal("1"),
                    self._mid[symbol] + tick,
                    Decimal("1"),
                )
            ticker = self.get_ticker(symbol)
            for stream in list(self._ticker_subs.get(symbol, ())):
                stream.push(ticker)
            book = self.get_order_book(symbol)
            for stream in list(self._book_subs.get(symbol, ())):
                stream.push(book)

            if self.volatility_bps > 0 and self._rng.random() < 0.35:
                side = Side.BUY if self._rng.random() < 0.5 else Side.SELL
                px = ticker.ask if side is Side.BUY else ticker.bid
                qty = Decimal("0.001") * Decimal(self._rng.randint(1, 20))
                self._emit_public_trade(
                    Trade(
                        universal_ticker=str(paper_ticker(symbol)),
                        price=px,
                        qty=qty,
                        side=side,
                    )
                )

    def _reserve_and_settle(
        self,
        account: str,
        inst: Instrument,
        side: Side,
        qty: Decimal,
        price: Decimal,
        *,
        lock_only: bool,
    ) -> None:
        free = self._free.setdefault(account, {})
        locked = self._locked.setdefault(account, {})
        if side is Side.BUY:
            cost = price * qty
            if lock_only:
                # Resting buy: move notional free → locked (fee on fill).
                have = free.get(inst.quote, Decimal("0"))
                if have < cost:
                    raise InsufficientBalanceError(
                        f"need {cost} {inst.quote}, free={have}"
                    )
                free[inst.quote] = have - cost
                locked[inst.quote] = locked.get(inst.quote, Decimal("0")) + cost
            else:
                fee = _fee(price, qty, self.fee_bps)
                need = cost + fee
                have = free.get(inst.quote, Decimal("0"))
                if have < need:
                    raise InsufficientBalanceError(
                        f"need {need} {inst.quote}, free={have}"
                    )
                free[inst.quote] = have - need
                free[inst.base] = free.get(inst.base, Decimal("0")) + qty
        else:
            if lock_only:
                have = free.get(inst.base, Decimal("0"))
                if have < qty:
                    raise InsufficientBalanceError(
                        f"need {qty} {inst.base}, free={have}"
                    )
                free[inst.base] = have - qty
                locked[inst.base] = locked.get(inst.base, Decimal("0")) + qty
            else:
                have = free.get(inst.base, Decimal("0"))
                if have < qty:
                    raise InsufficientBalanceError(
                        f"need {qty} {inst.base}, free={have}"
                    )
                free[inst.base] = have - qty
                proceeds = price * qty
                fee = _fee(price, qty, self.fee_bps)
                free[inst.quote] = (
                    free.get(inst.quote, Decimal("0")) + proceeds - fee
                )

    def _unlock(
        self,
        account: str,
        inst: Instrument,
        side: Side,
        qty: Decimal,
        price: Decimal,
    ) -> None:
        """Release a resting lock: locked → free (cancel or before maker settle)."""
        free = self._free.setdefault(account, {})
        locked = self._locked.setdefault(account, {})
        if side is Side.BUY:
            cost = price * qty
            have = locked.get(inst.quote, Decimal("0"))
            if have < cost:
                raise OrderError(
                    f"unlock {cost} {inst.quote} but locked={have}"
                )
            locked[inst.quote] = have - cost
            free[inst.quote] = free.get(inst.quote, Decimal("0")) + cost
        else:
            have = locked.get(inst.base, Decimal("0"))
            if have < qty:
                raise OrderError(f"unlock {qty} {inst.base} but locked={have}")
            locked[inst.base] = have - qty
            free[inst.base] = free.get(inst.base, Decimal("0")) + qty

    def _emit_order(self, account: str, order: Order) -> None:
        for stream in list(self._order_subs.get(account, ())):
            stream.push(order)
        if self._on_order is not None:
            self._on_order(account, order)

    def _emit_fill(self, account: str, fill: Fill) -> None:
        for stream in list(self._fill_subs.get(account, ())):
            stream.push(fill)
        if self._on_fill is not None:
            self._on_fill(account, fill)

    def _emit_balances(self, account: str) -> None:
        for bal in self.list_balances(account):
            for stream in list(self._balance_subs.get(account, ())):
                stream.push(bal)
            if self._on_balance is not None:
                self._on_balance(account, bal)

    def _emit_public_trade(self, trade: Trade) -> None:
        for stream in list(self._trade_subs.get(trade.symbol, ())):
            stream.push(trade)


def _split_symbol(symbol: str) -> tuple[str, str]:
    for quote in ("USDT", "USD", "USDC", "BTC", "ETH"):
        if symbol.endswith(quote) and len(symbol) > len(quote):
            return symbol[: -len(quote)], quote
    raise OrderError(f"cannot infer base/quote from symbol={symbol}")


def _round_price(price: Decimal, tick: Decimal) -> Decimal:
    if tick <= 0:
        return price
    return (price / tick).to_integral_value(rounding=ROUND_HALF_UP) * tick


def _fee(price: Decimal, qty: Decimal, fee_bps: Decimal) -> Decimal:
    return (price * qty * fee_bps / Decimal("10000")).quantize(Decimal("0.00000001"))
