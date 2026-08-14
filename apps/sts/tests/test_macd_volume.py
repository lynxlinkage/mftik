"""MacdVolumeBars — bars, warm-up, and the crosses that become orders.

Driven directly: prints go in through the feed hooks, orders come out through a
fake OMS. What is under test is the bar boundary, the refusal to trade before
the indicator is warm, the long-only shape of the position, and the two
configuration mistakes that would be silent — subscribing to both trade feeds,
and replaying a print that was already on the tape.
"""

from __future__ import annotations

from decimal import Decimal

import pytest
from mft.exchange.models import (
    AggTrade,
    Fill,
    Order,
    OrderStatus,
    OrderType,
    Side,
)
from mft.protocol import RejectCode, SymbolInfo
from mft_sts.impl.macd_volume import MacdVolumeBars, _BarBuilder, _Ema
from mft_sts.tape import TapeSlice

TICKER = "BinanceFuture_Perp_BTCUSDT"

INFO = SymbolInfo(
    universal_ticker=TICKER,
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
    def __init__(self, *, accept: bool = True) -> None:
        self.accept = accept
        self.submitted: list[dict] = []
        self.last_reject_reason = ""
        self.last_reject_code: int | str = RejectCode.NONE
        self._n = 0
        self._last_cid: str | None = None

    @property
    def last_client_order_id(self) -> str | None:
        return self._last_cid

    async def submit_order(
        self, api_id, *, ticker, side, qty, type, price=None, tif=None
    ):
        self._n += 1
        self._last_cid = f"cid-{self._n}"
        self.submitted.append(
            {"cid": self._last_cid, "side": side, "qty": qty, "type": type}
        )
        return self.accept


class FakeSession:
    def __init__(self, md_ids: list[str] | None = None) -> None:
        self.td_api_ids = [7]
        self.md_ids = md_ids if md_ids is not None else [f"aggtrade.{TICKER}"]
        self.failures: list[str] = []

    def request_exit(self, reason: str, *, failed: bool = False) -> None:
        if failed:
            self.failures.append(reason)


class FakeTape:
    def __init__(self, slice_: TapeSlice | None = None) -> None:
        self.slice = slice_ or TapeSlice()
        self.calls: list[tuple] = []

    async def read(self, ticker, *, topic="aggtrade", limit=0):
        self.calls.append((str(ticker), topic, limit))
        return self.slice


def _strategy(*, md_ids: list[str] | None = None, **paras) -> MacdVolumeBars:
    payload = {
        "bar_quote_volume": "1000",
        "qty_quote": "100",
        # Short periods keep the warm-up requirement small enough to drive by
        # hand: slow + signal bars, so 3 + 2 = 5 here.
        "fast": 2,
        "slow": 3,
        "signal": 2,
    }
    payload.update(paras)

    strat = MacdVolumeBars()
    strat.paras = MacdVolumeBars.on_initialized(payload)
    strat.session = FakeSession(md_ids)  # type: ignore[assignment]
    strat.oms = FakeOms()  # type: ignore[assignment]
    strat.tape = FakeTape()  # type: ignore[assignment]
    strat._info = INFO

    async def _noop_log(message: str, *, level: str = "info", **extra) -> None:
        return None

    strat.log = _noop_log  # type: ignore[method-assign]
    strat.owns = lambda cid: True  # type: ignore[method-assign]
    return strat


def _print(price: str, qty: str = "1", *, trade_id: str = "", ts: float = 0.0):
    return AggTrade(
        universal_ticker=TICKER,
        trade_id=trade_id,
        price=Decimal(price),
        qty=Decimal(qty),
        side=Side.BUY,
        ts=ts,
    )


# --- bars ------------------------------------------------------------------


def test_a_bar_closes_on_the_print_that_crosses_the_threshold() -> None:
    builder = _BarBuilder(Decimal("1000"))

    assert builder.push(_print("100", "5")) is None  # 500
    bar = builder.push(_print("110", "5"))  # 500 more → 1050, closes

    assert bar is not None
    assert bar.quote_volume == Decimal("1050")
    assert bar.open == Decimal("100")
    assert bar.close == Decimal("110")
    assert bar.prints == 2


def test_the_next_bar_starts_empty() -> None:
    """The overshoot is bounded by one print; it is not carried forward."""
    builder = _BarBuilder(Decimal("1000"))
    builder.push(_print("100", "20"))  # 2000 — closes well over

    assert builder.quote_volume == Decimal("0")


def test_zero_quantity_prints_do_not_open_a_bar() -> None:
    """Venues do print these, and one must not set a bar's open price."""
    builder = _BarBuilder(Decimal("1000"))

    assert builder.push(_print("99999", "0")) is None
    assert builder.push(_print("100", "1")) is None

    # The absurd price carried no volume, so it neither opened the bar nor
    # moved it: only the real print is in there.
    assert builder.quote_volume == Decimal("100")
    bar = builder.push(_print("100", "9"))
    assert bar is not None
    assert bar.open == Decimal("100")
    assert bar.high == Decimal("100")


def test_bar_threshold_must_be_positive() -> None:
    with pytest.raises(ValueError, match="must be positive"):
        _BarBuilder(Decimal("0"))


def test_ema_is_seeded_with_its_first_sample() -> None:
    ema = _Ema(3)
    assert ema.update(Decimal("10")) == Decimal("10")
    # Moves toward the next sample rather than jumping to it.
    second = ema.update(Decimal("20"))
    assert Decimal("10") < second < Decimal("20")


# --- configuration ---------------------------------------------------------


def test_fast_must_be_shorter_than_slow() -> None:
    with pytest.raises(ValueError, match="must be shorter than slow"):
        MacdVolumeBars.on_initialized(
            {
                "bar_quote_volume": "1000",
                "qty_quote": "100",
                "fast": 26,
                "slow": 12,
            }
        )


def test_feed_must_be_a_recorded_topic() -> None:
    with pytest.raises(ValueError, match="aggtrade"):
        MacdVolumeBars.on_initialized(
            {
                "bar_quote_volume": "1000",
                "qty_quote": "100",
                "feed": "orderbook",
            }
        )


def test_bar_quote_volume_is_required() -> None:
    with pytest.raises(ValueError, match="bar_quote_volume is required"):
        MacdVolumeBars.on_initialized({"qty_quote": "100"})


@pytest.mark.asyncio
async def test_subscribing_to_both_trade_feeds_is_refused() -> None:
    """They report the same matches — every bar would double-count its volume."""
    strat = _strategy(md_ids=[f"aggtrade.{TICKER}", f"trade.{TICKER}"])

    await strat.on_start()

    assert strat.session.failures
    assert "twice" in strat.session.failures[0]


@pytest.mark.asyncio
async def test_a_missing_feed_is_refused() -> None:
    strat = _strategy(md_ids=[f"bestquote.{TICKER}"])

    await strat.on_start()

    assert strat.session.failures
    assert "no aggtrade feed" in strat.session.failures[0]


@pytest.mark.asyncio
async def test_more_than_one_account_is_refused() -> None:
    strat = _strategy()
    strat.session.td_api_ids = [7, 8]

    await strat.on_start()

    assert strat.session.failures
    assert "exactly one td" in strat.session.failures[0]


# --- warm-up ---------------------------------------------------------------


@pytest.mark.asyncio
async def test_warm_up_builds_bars_from_the_tape() -> None:
    strat = _strategy()
    # 10 prints of 500 each = 5 bars at a 1000 threshold.
    strat.tape = FakeTape(
        TapeSlice(
            records=[_print(str(100 + n), "5") for n in range(10)],
            continuous_since_ms=1,
            recording=True,
        )
    )

    await strat.on_start()

    assert strat._bars_seen == 5
    assert strat._required_bars == 5


@pytest.mark.asyncio
async def test_no_orders_before_the_indicator_is_warm() -> None:
    """A short history is a reason to wait, not a reason to trade on noise."""
    strat = _strategy()
    strat.tape = FakeTape()  # nothing recorded
    await strat.on_start()

    # Four bars' worth — one short of slow + signal.
    for n in range(8):
        await strat.on_agg_trade(_print(str(100 + n * 10), "5"))

    assert strat._bars_seen == 4
    assert strat.oms.submitted == []


@pytest.mark.asyncio
async def test_prints_arriving_during_the_tape_read_are_not_lost() -> None:
    """The md pump starts before on_start runs."""
    strat = _strategy()
    strat.tape = FakeTape()

    # Stand in for the pump delivering while _warm_up is in flight.
    strat._warming = True
    await strat.on_agg_trade(_print("100", "5"))
    await strat.on_agg_trade(_print("101", "5"))
    assert strat._bars_seen == 0  # buffered, not yet folded in

    await strat.on_start()

    assert strat._bars_seen == 1


@pytest.mark.asyncio
async def test_a_print_already_on_the_tape_is_not_counted_twice() -> None:
    """A duplicate is a permanent error in a sum; a miss is a rounding one."""
    strat = _strategy()
    strat.tape = FakeTape(
        TapeSlice(
            records=[_print("100", "5", trade_id="a")],
            continuous_since_ms=1,
            recording=True,
        )
    )
    strat._warming = True
    # The same print, delivered live while the tape was being read.
    await strat.on_agg_trade(_print("100", "5", trade_id="a"))

    await strat.on_start()

    # 500 from the tape, and the live copy of it ignored — not 1000, which
    # would have closed a bar that never happened.
    assert strat._bars_seen == 0


# --- trading ---------------------------------------------------------------


#: Quantity that makes one print exactly one bar at the 1000 threshold used
#: here, so a list of prices reads as a list of bar closes.
ONE_BAR = "20"


async def _warm(strat: MacdVolumeBars, prices: list[str]) -> None:
    """Push enough bars through the tape to leave the strategy ready."""
    strat.tape = FakeTape(
        TapeSlice(
            records=[_print(p, ONE_BAR) for p in prices],
            continuous_since_ms=1,
            recording=True,
        )
    )
    await strat.on_start()


@pytest.mark.asyncio
async def test_a_bullish_cross_buys_when_flat() -> None:
    strat = _strategy()
    # Falling then rising: the fast EMA crosses back above the slow one.
    await _warm(strat, ["120", "110", "100", "90", "80", "70"])
    assert strat._bars_seen >= strat._required_bars

    for price in ("90", "120", "160", "220"):
        await strat.on_agg_trade(_print(price, ONE_BAR))

    assert strat.oms.submitted, "a rally after a fall should cross bullish"
    first = strat.oms.submitted[0]
    assert first["side"] is Side.BUY
    assert first["type"] is OrderType.MARKET


@pytest.mark.asyncio
async def test_a_bearish_cross_while_flat_does_nothing() -> None:
    """Long only: not being long is not a reason to go short."""
    strat = _strategy()
    await _warm(strat, ["70", "80", "90", "100", "110", "120"])

    for price in ("110", "80", "50", "20"):
        await strat.on_agg_trade(_print(price, ONE_BAR))

    assert strat.oms.submitted == []


@pytest.mark.asyncio
async def test_a_bearish_cross_sells_the_position() -> None:
    strat = _strategy()
    await _warm(strat, ["120", "110", "100", "90", "80", "70"])
    for price in ("90", "120", "160", "220"):
        await strat.on_agg_trade(_print(price, ONE_BAR))
    assert strat.oms.submitted

    # The buy fills, so the strategy is long.
    bought = strat.oms.submitted[0]
    await strat.on_order_update(
        7, _order(bought["cid"], OrderStatus.FILLED, bought["qty"])
    )
    await strat.on_fill(7, _fill(bought["cid"], Side.BUY, bought["qty"]))
    assert strat._position > 0

    for price in ("180", "120", "60", "30"):
        await strat.on_agg_trade(_print(price, ONE_BAR))

    assert len(strat.oms.submitted) == 2
    assert strat.oms.submitted[1]["side"] is Side.SELL
    assert strat.oms.submitted[1]["qty"] == strat._position


@pytest.mark.asyncio
async def test_only_one_order_is_in_flight_at_a_time() -> None:
    strat = _strategy()
    await _warm(strat, ["120", "110", "100", "90", "80", "70"])
    for price in ("90", "120", "160", "220"):
        await strat.on_agg_trade(_print(price, ONE_BAR))
    assert len(strat.oms.submitted) == 1

    # No terminal update, so the first order is still outstanding.
    for price in ("180", "120", "60", "30"):
        await strat.on_agg_trade(_print(price, ONE_BAR))

    assert len(strat.oms.submitted) == 1


@pytest.mark.asyncio
async def test_a_refused_submit_does_not_leave_an_order_in_flight() -> None:
    """TD refusals are standing conditions — but they are not open orders."""
    strat = _strategy()
    strat.oms.accept = False
    await _warm(strat, ["120", "110", "100", "90", "80", "70"])
    for price in ("90", "120", "160", "220"):
        await strat.on_agg_trade(_print(price, ONE_BAR))

    assert strat.oms.submitted
    assert strat._pending_cid is None


@pytest.mark.asyncio
async def test_a_fill_that_oversells_is_clamped_to_flat() -> None:
    strat = _strategy()
    await strat.on_fill(7, _fill("cid-1", Side.SELL, Decimal("1")))
    assert strat._position == Decimal("0")


def _order(cid: str, status: OrderStatus, qty: Decimal) -> Order:
    return Order(
        universal_ticker=TICKER,
        client_order_id=cid,
        order_id=f"venue-{cid}",
        side=Side.BUY,
        type=OrderType.MARKET,
        qty=qty,
        filled_qty=qty,
        status=status,
    )


def _fill(cid: str, side: Side, qty: Decimal) -> Fill:
    return Fill(
        universal_ticker=TICKER,
        client_order_id=cid,
        order_id=f"venue-{cid}",
        trade_id=f"t-{cid}",
        side=side,
        price=Decimal("100"),
        qty=qty,
    )
