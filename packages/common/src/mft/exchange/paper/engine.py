"""In-process paper venue — shared state for public + private clients."""

from __future__ import annotations

import asyncio
import logging
import random
from decimal import ROUND_HALF_UP, Decimal
from typing import TYPE_CHECKING

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
    Trade,
)
from mft.exchange.stream import EventStream

if TYPE_CHECKING:
    from mft.exchange.paper.private import PaperPrivateClient
    from mft.exchange.paper.public import PaperPublicClient

logger = logging.getLogger(__name__)

_DEFAULT_SYMBOLS: dict[str, tuple[str, str, Decimal]] = {
    # symbol: (base, quote, mid)
    "BTCUSDT": ("BTC", "USDT", Decimal("50000")),
    "ETHUSDT": ("ETH", "USDT", Decimal("3000")),
}


class PaperExchange:
    """Fake exchange engine used by paper public/private clients.

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
        spread_bps: Decimal = Decimal("5"),
        fee_bps: Decimal = Decimal("5"),
        volatility_bps: Decimal = Decimal("8"),
        initial_balances: dict[str, Decimal] | None = None,
        seed: int | None = 1,
    ) -> None:
        self.tick_interval = tick_interval
        self.spread_bps = spread_bps
        self.fee_bps = fee_bps
        self.volatility_bps = volatility_bps
        self._rng = random.Random(seed)

        self._instruments: dict[str, Instrument] = {}
        self._mid: dict[str, Decimal] = {}
        if symbols is None:
            for symbol, (base, quote, mid) in _DEFAULT_SYMBOLS.items():
                self._instruments[symbol] = Instrument(
                    symbol=symbol, base=base, quote=quote
                )
                self._mid[symbol] = mid
        else:
            for symbol, mid in symbols.items():
                base, quote = _split_symbol(symbol)
                self._instruments[symbol] = Instrument(
                    symbol=symbol, base=base, quote=quote
                )
                self._mid[symbol] = mid

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
        self._balances: dict[str, dict[str, Decimal]] = {}
        self._orders: dict[str, Order] = {}
        self._open_by_account: dict[str, set[str]] = {}

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
        if api_key not in self._balances:
            self._balances[api_key] = dict(balances or self._default_balances)
            self._open_by_account[api_key] = set()

    # --- market data (req-reply) -------------------------------------------

    def list_instruments(self) -> list[Instrument]:
        return list(self._instruments.values())

    def get_ticker(self, symbol: str) -> Ticker:
        inst = self._require_instrument(symbol)
        mid = self._mid[symbol]
        half = mid * self.spread_bps / Decimal("10000") / Decimal("2")
        bid = _round_price(mid - half, inst.tick_size)
        ask = _round_price(mid + half, inst.tick_size)
        return Ticker(symbol=symbol, bid=bid, ask=ask, last=mid)

    def get_order_book(self, symbol: str, *, depth: int = 10) -> OrderBook:
        inst = self._require_instrument(symbol)
        ticker = self.get_ticker(symbol)
        bids: list[BookLevel] = []
        asks: list[BookLevel] = []
        for i in range(depth):
            step = inst.tick_size * Decimal(i + 1) * Decimal("5")
            qty = Decimal("0.1") * Decimal(depth - i)
            bids.append(BookLevel(price=ticker.bid - step, qty=qty))
            asks.append(BookLevel(price=ticker.ask + step, qty=qty))
        return OrderBook(symbol=symbol, bids=bids, asks=asks)

    # --- trading (req-reply) -----------------------------------------------

    async def place_order(self, account: str, request: PlaceOrderRequest) -> Order:
        async with self._lock:
            return self._place_order_locked(account, request)

    def _place_order_locked(self, account: str, request: PlaceOrderRequest) -> Order:
        inst = self._require_instrument(request.symbol)
        if request.qty <= 0:
            raise OrderError("qty must be positive")
        if request.type is OrderType.LIMIT and request.price is None:
            raise OrderError("limit orders require price")

        ticker = self.get_ticker(request.symbol)

        if request.type is OrderType.MARKET:
            fill_price = ticker.ask if request.side is Side.BUY else ticker.bid
            self._reserve_and_settle(
                account,
                inst,
                request.side,
                request.qty,
                fill_price,
                lock_only=False,
            )
            order = Order(
                client_order_id=request.client_order_id,
                symbol=request.symbol,
                side=request.side,
                type=request.type,
                status=OrderStatus.FILLED,
                qty=request.qty,
                price=None,
                filled_qty=request.qty,
                avg_price=fill_price,
            )
            fill = Fill(
                order_id=order.order_id,
                symbol=order.symbol,
                side=order.side,
                price=fill_price,
                qty=request.qty,
                fee=_fee(fill_price, request.qty, self.fee_bps),
                fee_asset=inst.quote,
            )
            self._orders[order.order_id] = order
            self._emit_order(account, order)
            self._emit_fill(account, fill)
            self._emit_balances(account)
            self._emit_public_trade(
                Trade(
                    symbol=order.symbol,
                    price=fill_price,
                    qty=request.qty,
                    side=request.side,
                )
            )
            return order

        # LIMIT
        assert request.price is not None
        price = _round_price(request.price, inst.tick_size)
        crosses = (request.side is Side.BUY and price >= ticker.ask) or (
            request.side is Side.SELL and price <= ticker.bid
        )
        if crosses:
            fill_price = ticker.ask if request.side is Side.BUY else ticker.bid
            self._reserve_and_settle(
                account,
                inst,
                request.side,
                request.qty,
                fill_price,
                lock_only=False,
            )
            order = Order(
                client_order_id=request.client_order_id,
                symbol=request.symbol,
                side=request.side,
                type=request.type,
                status=OrderStatus.FILLED,
                qty=request.qty,
                price=price,
                filled_qty=request.qty,
                avg_price=fill_price,
            )
            fill = Fill(
                order_id=order.order_id,
                symbol=order.symbol,
                side=order.side,
                price=fill_price,
                qty=request.qty,
                fee=_fee(fill_price, request.qty, self.fee_bps),
                fee_asset=inst.quote,
            )
            self._orders[order.order_id] = order
            self._emit_order(account, order)
            self._emit_fill(account, fill)
            self._emit_balances(account)
            self._emit_public_trade(
                Trade(
                    symbol=order.symbol,
                    price=fill_price,
                    qty=request.qty,
                    side=request.side,
                )
            )
            return order

        # Resting order — lock funds
        self._reserve_and_settle(
            account, inst, request.side, request.qty, price, lock_only=True
        )
        order = Order(
            client_order_id=request.client_order_id,
            symbol=request.symbol,
            side=request.side,
            type=request.type,
            status=OrderStatus.OPEN,
            qty=request.qty,
            price=price,
            filled_qty=Decimal("0"),
            avg_price=None,
        )
        self._orders[order.order_id] = order
        self._open_by_account.setdefault(account, set()).add(order.order_id)
        self._emit_order(account, order)
        self._emit_balances(account)
        return order

    async def cancel_order(self, account: str, order_id: str) -> Order:
        async with self._lock:
            order = self._orders.get(order_id)
            if order is None:
                raise OrderError(f"unknown order_id={order_id}")
            open_ids = self._open_by_account.get(account, set())
            if order_id not in open_ids:
                raise OrderError(f"order {order_id} is not open for account={account}")
            inst = self._require_instrument(order.symbol)
            assert order.price is not None
            self._unlock(account, inst, order.side, order.qty, order.price)
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

    def list_open_orders(
        self, account: str, symbol: str | None = None
    ) -> list[Order]:
        ids = self._open_by_account.get(account, set())
        orders = [self._orders[i] for i in ids]
        if symbol is not None:
            orders = [o for o in orders if o.symbol == symbol]
        return orders

    def list_balances(self, account: str) -> list[Balance]:
        bal = self._balances.get(account, {})
        # locked tracked implicitly via free-only store; expose free for paper
        return [
            Balance(asset=asset, free=free, locked=Decimal("0"))
            for asset, free in sorted(bal.items())
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
            # Random-walk mid price.
            move = mid * self.volatility_bps / Decimal("10000")
            delta = Decimal(str(self._rng.uniform(-1.0, 1.0))) * move
            inst = self._instruments[symbol]
            new_mid = max(mid + delta, inst.tick_size)
            self._mid[symbol] = _round_price(new_mid, inst.tick_size)
            ticker = self.get_ticker(symbol)
            for stream in list(self._ticker_subs.get(symbol, ())):
                stream.push(ticker)
            book = self.get_order_book(symbol)
            for stream in list(self._book_subs.get(symbol, ())):
                stream.push(book)

            # Occasionally emit a synthetic public trade.
            if self._rng.random() < 0.35:
                side = Side.BUY if self._rng.random() < 0.5 else Side.SELL
                px = ticker.ask if side is Side.BUY else ticker.bid
                qty = Decimal("0.001") * Decimal(self._rng.randint(1, 20))
                self._emit_public_trade(
                    Trade(symbol=symbol, price=px, qty=qty, side=side)
                )

            self._match_resting_locked(symbol, ticker)

    def _match_resting_locked(self, symbol: str, ticker: Ticker) -> None:
        for account, open_ids in list(self._open_by_account.items()):
            for order_id in list(open_ids):
                order = self._orders[order_id]
                if order.symbol != symbol or order.price is None:
                    continue
                should_fill = (
                    order.side is Side.BUY and order.price >= ticker.ask
                ) or (order.side is Side.SELL and order.price <= ticker.bid)
                if not should_fill:
                    continue
                fill_price = ticker.ask if order.side is Side.BUY else ticker.bid
                inst = self._instruments[symbol]
                # Convert lock → settle
                self._unlock(account, inst, order.side, order.qty, order.price)
                self._reserve_and_settle(
                    account,
                    inst,
                    order.side,
                    order.qty,
                    fill_price,
                    lock_only=False,
                )
                filled = order.model_copy(
                    update={
                        "status": OrderStatus.FILLED,
                        "filled_qty": order.qty,
                        "avg_price": fill_price,
                    }
                )
                fill = Fill(
                    order_id=filled.order_id,
                    symbol=filled.symbol,
                    side=filled.side,
                    price=fill_price,
                    qty=filled.qty,
                    fee=_fee(fill_price, filled.qty, self.fee_bps),
                    fee_asset=inst.quote,
                )
                self._orders[order_id] = filled
                open_ids.remove(order_id)
                self._emit_order(account, filled)
                self._emit_fill(account, fill)
                self._emit_balances(account)
                self._emit_public_trade(
                    Trade(
                        symbol=filled.symbol,
                        price=fill_price,
                        qty=filled.qty,
                        side=filled.side,
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
        bal = self._balances.setdefault(account, {})
        if side is Side.BUY:
            cost = price * qty
            fee = _fee(price, qty, self.fee_bps)
            need = cost + fee
            free = bal.get(inst.quote, Decimal("0"))
            if free < need:
                raise InsufficientBalanceError(
                    f"need {need} {inst.quote}, free={free}"
                )
            bal[inst.quote] = free - need
            if not lock_only:
                bal[inst.base] = bal.get(inst.base, Decimal("0")) + qty
            else:
                # For paper simplicity locks are deducted from free; unlock restores.
                pass
        else:
            free = bal.get(inst.base, Decimal("0"))
            if free < qty:
                raise InsufficientBalanceError(
                    f"need {qty} {inst.base}, free={free}"
                )
            bal[inst.base] = free - qty
            if not lock_only:
                proceeds = price * qty
                fee = _fee(price, qty, self.fee_bps)
                bal[inst.quote] = bal.get(inst.quote, Decimal("0")) + proceeds - fee

    def _unlock(
        self,
        account: str,
        inst: Instrument,
        side: Side,
        qty: Decimal,
        price: Decimal,
    ) -> None:
        bal = self._balances.setdefault(account, {})
        if side is Side.BUY:
            cost = price * qty
            fee = _fee(price, qty, self.fee_bps)
            bal[inst.quote] = bal.get(inst.quote, Decimal("0")) + cost + fee
        else:
            bal[inst.base] = bal.get(inst.base, Decimal("0")) + qty

    def _emit_order(self, account: str, order: Order) -> None:
        for stream in list(self._order_subs.get(account, ())):
            stream.push(order)

    def _emit_fill(self, account: str, fill: Fill) -> None:
        for stream in list(self._fill_subs.get(account, ())):
            stream.push(fill)

    def _emit_balances(self, account: str) -> None:
        for bal in self.list_balances(account):
            for stream in list(self._balance_subs.get(account, ())):
                stream.push(bal)

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
