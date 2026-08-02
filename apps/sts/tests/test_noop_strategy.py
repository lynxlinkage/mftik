"""NoopStrategy — book-driven mid, plane-driven rounding."""

from __future__ import annotations

from decimal import Decimal

import pytest
from mft.exchange.models import BookLevel, OrderBook, Side
from mft.protocol import SymbolInfo, UntypedEnvelope
from mft_sts.impl.noop import NoopStrategy

PAPER_BTC = SymbolInfo(
    venue="paper",
    symbol="BTCUSDT",
    base="BTC",
    quote="USDT",
    exch_ticker="BTCUSDT",
    filters=[
        {"name": "price_tick", "value": Decimal("0.01")},
        {"name": "qty_step", "value": Decimal("0.00001")},
        {"name": "min_qty", "value": Decimal("0.00001")},
        {"name": "min_notional", "value": Decimal("5")},
    ],
)


class FakeOms:
    def __init__(self) -> None:
        self.submitted: list[dict] = []
        self.cancelled: list[str] = []
        self._n = 0

    async def submit_order(self, api_id, *, symbol, side, qty, type, price):
        self._n += 1
        cid = f"cid-{self._n}"
        self.submitted.append(
            {
                "api_id": api_id,
                "symbol": symbol,
                "side": side,
                "qty": qty,
                "price": price,
                "cid": cid,
            }
        )
        return cid

    async def cancel_order(self, api_id, cid):
        self.cancelled.append(cid)

    def get(self, api_id):
        return None


class FakePlane:
    def __init__(self, info: SymbolInfo | None = PAPER_BTC) -> None:
        self.info = info
        self.calls = 0

    async def get(self, venue, symbol, *, category="spot"):
        self.calls += 1
        if self.info is None:
            raise LookupError(f"no {symbol} on {venue}")
        return self.info


class FakeSession:
    def __init__(self, md_ids=("paper.orderbook.BTCUSDT",), td=(1,)) -> None:
        self.md_ids = list(md_ids)
        self.td_api_ids = list(td)
        self.session_id = "noop-test"
        self.cid_slot = 1
        self.symbols = FakePlane()


def _strategy(**paras) -> NoopStrategy:
    """A strategy wired to fakes, bypassing the real session plumbing."""
    strat = NoopStrategy()
    strat.session = FakeSession()  # type: ignore[assignment]
    strat.paras = NoopStrategy.on_initialized(paras)
    strat.oms = FakeOms()  # type: ignore[assignment]
    strat._resolve_feed()

    async def _log(message, *, level="info", **extra):
        return None

    strat.log = _log  # type: ignore[method-assign]
    return strat


async def _book(strat: NoopStrategy, bid: str, ask: str) -> None:
    book = OrderBook(
        symbol="BTCUSDT",
        bids=[BookLevel(price=Decimal(bid), qty=Decimal("1"))],
        asks=[BookLevel(price=Decimal(ask), qty=Decimal("1"))],
    )
    await strat.on_order_book(
        UntypedEnvelope.wrap(book.model_dump(mode="json"), type="md.orderbook",
                             source="md")
    )


# --- params ----------------------------------------------------------------


def test_mid_is_no_longer_a_parameter() -> None:
    """The book tells the strategy the price; a configured mid would lie."""
    with pytest.raises(ValueError, match="no longer takes a mid"):
        NoopStrategy.on_initialized({"mid": 50000})


def test_params_have_defaults() -> None:
    out = NoopStrategy.on_initialized({})
    assert out["exec_interval_ms"] == 1000
    assert out["gap_bps"] == Decimal("10")
    assert out["qty_quote"] == Decimal("100")


@pytest.mark.parametrize(
    ("params", "match"),
    [
        ({"exec_interval_ms": 0}, "exec_interval_ms must be positive"),
        ({"gap_bps": -1}, "gap_bps must not be negative"),
        ({"qty_quote": 0}, "qty_quote must be positive"),
    ],
)
def test_invalid_params_are_rejected(params, match) -> None:
    with pytest.raises(ValueError, match=match):
        NoopStrategy.on_initialized(params)


# --- venue / symbol resolution ---------------------------------------------


def test_venue_and_symbol_come_from_the_md_feed() -> None:
    strat = _strategy()
    assert strat._venue == "paper"
    assert strat._symbol == "BTCUSDT"


# --- execution -------------------------------------------------------------


async def test_no_orders_until_the_book_arrives() -> None:
    strat = _strategy()
    await strat._on_tick()
    assert strat.oms.submitted == []


async def test_quotes_three_levels_around_the_live_mid() -> None:
    strat = _strategy(gap_bps=10, qty_quote=100)
    await _book(strat, "49999", "50001")  # mid 50000
    await strat._on_tick()

    orders = strat.oms.submitted
    assert len(orders) == 3
    assert [o["side"] for o in orders] == [Side.BUY, Side.BUY, Side.SELL]
    # 10bps either side of 50000.
    assert [o["price"] for o in orders] == [
        Decimal("49950.00"),
        Decimal("50000"),
        Decimal("50050.00"),
    ]


async def test_prices_are_rounded_to_the_venue_tick() -> None:
    """A mid off-tick must not go out as-is; TD would not catch it."""
    strat = _strategy(gap_bps=7, qty_quote=100)
    await _book(strat, "49999.995", "50000.005")  # mid 50000.000
    await strat._on_tick()

    tick = PAPER_BTC.price_tick
    for order in strat.oms.submitted:
        assert order["price"] % tick == 0, order["price"]


async def test_qty_comes_from_qty_quote_at_the_rounded_price() -> None:
    strat = _strategy(gap_bps=10, qty_quote=100)
    await _book(strat, "49999", "50001")
    await strat._on_tick()

    step = PAPER_BTC.qty_step
    for order in strat.oms.submitted:
        assert order["qty"] % step == 0
        notional = order["qty"] * order["price"]
        # Floored to the step, so never over the target and never more than
        # one step of notional under it.
        assert notional <= Decimal("100")
        assert notional > Decimal("100") - step * order["price"]


async def test_orders_below_the_venue_minimum_are_skipped() -> None:
    """min_notional is 5 USD; a 1 USD target cannot clear it."""
    strat = _strategy(qty_quote=1)
    await _book(strat, "49999", "50001")
    await strat._on_tick()

    assert strat.oms.submitted == []


async def test_each_tick_cancels_the_previous_quotes() -> None:
    strat = _strategy()
    await _book(strat, "49999", "50001")

    await strat._on_tick()
    first = [o["cid"] for o in strat.oms.submitted]
    assert strat.oms.cancelled == []

    await strat._on_tick()
    assert strat.oms.cancelled == first
    assert len(strat.oms.submitted) == 6


async def test_instrument_is_resolved_once_per_session() -> None:
    """Filters are near-static; re-asking the plane per tick would be waste."""
    strat = _strategy()
    await _book(strat, "49999", "50001")

    await strat._on_tick()
    await strat._on_tick()
    await strat._on_tick()

    assert strat.session.symbols.calls == 1


async def test_unresolvable_instrument_stops_quoting() -> None:
    strat = _strategy()
    strat.session.symbols = FakePlane(info=None)
    await _book(strat, "49999", "50001")

    await strat._on_tick()

    assert strat.oms.submitted == []


async def test_mid_tracks_the_book() -> None:
    strat = _strategy(gap_bps=0)
    await _book(strat, "49999", "50001")
    await strat._on_tick()
    assert strat.oms.submitted[0]["price"] == Decimal("50000")

    strat.oms.submitted.clear()
    await _book(strat, "59999", "60001")
    await strat._on_tick()
    assert strat.oms.submitted[0]["price"] == Decimal("60000")


def test_the_old_qty_usd_name_is_rejected() -> None:
    """Silently ignoring it would quietly resize every order to the default."""
    with pytest.raises(ValueError, match="qty_usd is now qty_quote"):
        NoopStrategy.on_initialized({"qty_usd": 500})


async def test_size_is_in_the_quote_currency_not_dollars() -> None:
    """On ETHBTC the size is BTC — the name no longer implies otherwise."""
    eth_btc = SymbolInfo(
        venue="paper",
        symbol="ETHBTC",
        base="ETH",
        quote="BTC",
        exch_ticker="ETHBTC",
        filters=[
            {"name": "price_tick", "value": Decimal("0.000001")},
            {"name": "qty_step", "value": Decimal("0.001")},
        ],
    )
    strat = _strategy(gap_bps=0, qty_quote=1)
    strat.session.symbols = FakePlane(eth_btc)
    strat._symbol = "ETHBTC"
    await _book(strat, "0.05", "0.06")  # mid 0.055 BTC

    await strat._on_tick()

    order = strat.oms.submitted[0]
    # 1 BTC of notional at 0.055 → ~18.181 ETH, floored to the 0.001 step.
    assert order["qty"] == Decimal("18.181")
    assert order["qty"] * order["price"] <= Decimal("1")
