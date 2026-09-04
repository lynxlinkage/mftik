"""TwapStrategy — schedule, IOC takes, and when it stops.

Driven directly: recon + quotes arm it, ticks place, venue answers arrive
through ``on_order_update``. What is under test is the arming gate, the
touch price each side crosses, success counting, and the two exits
(``num_round`` successes / end of window).
"""

from __future__ import annotations

import asyncio
from decimal import Decimal

import pytest
from mftik.exchange.models import (
    BestQuote,
    Order,
    OrderStatus,
    OrderType,
    Side,
    TimeInForce,
)
from mftik.exchange.oms import OmsView
from mftik.exchange.tickers import UniversalTicker
from mftik.protocol import ReconDone, RejectCode, SymbolInfo
from mftik_sts.impl.twap import TwapStrategy

BTCUSDT = SymbolInfo(
    universal_ticker="Paper_Spot_BTCUSDT",
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

BTCUSDT_PERP = SymbolInfo(
    universal_ticker="BinanceUM_Perp_BTCUSDT",
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
        self.submitted: list[dict] = []
        self.reject_reason = ""
        self.reject_code: int | str = RejectCode.NONE
        self.accept = accept
        self.strategy: TwapStrategy | None = None
        self.ioc_fill: Decimal | None = None
        self._n = 0
        self._last_cid: str | None = None

    @property
    def last_client_order_id(self) -> str | None:
        return self._last_cid

    @property
    def last_reject_reason(self) -> str:
        return "" if self.accept else self.reject_reason

    @property
    def last_reject_code(self) -> int | str:
        return RejectCode.NONE if self.accept else self.reject_code

    async def submit_order(
        self, api_id, *, ticker, side, qty, type, price=None, tif=None
    ):
        self._n += 1
        self._last_cid = f"cid-{self._n}"
        self.submitted.append(
            {
                "cid": self._last_cid,
                "ticker": ticker,
                "side": side,
                "qty": qty,
                "type": type,
                "price": price,
                "tif": tif,
            }
        )
        if tif is TimeInForce.IOC and self.strategy is not None and self.accept:
            asyncio.create_task(self._answer_ioc(self._last_cid, qty))
        return self.accept

    async def _answer_ioc(self, cid: str, qty: Decimal) -> None:
        await asyncio.sleep(0)
        filled = qty if self.ioc_fill is None else min(qty, self.ioc_fill)
        status = OrderStatus.FILLED if filled >= qty else OrderStatus.CANCELED
        assert self.strategy is not None
        await self.strategy.on_order_update(7, _update(cid, str(filled), status))


class FakeLedger:
    def __init__(self) -> None:
        self.balances: dict[str, Decimal] = {}
        self.default = Decimal("1000000")
        self._leverage: dict[str, Decimal] = {}
        self.ensure_calls: list[tuple[str, int | None]] = []
        self.ensure_result: Decimal | None = Decimal("10")
        self.last_reject_reason = ""
        self.last_reject_code: int | str = RejectCode.NONE

    async def available(self, asset: str, api_id=None) -> Decimal:
        return self.balances.get(asset, self.default)

    def leverage(self, ticker, api_id=None) -> Decimal | None:
        return self._leverage.get(str(ticker))

    async def ensure_leverage(self, ticker, api_id=None) -> Decimal | None:
        self.ensure_calls.append((str(ticker), api_id))
        if self.ensure_result is None:
            self.last_reject_reason = "leverage unavailable"
            self.last_reject_code = RejectCode.TD_LEVERAGE_UNAVAILABLE
            return None
        self._leverage[str(ticker)] = self.ensure_result
        return self.ensure_result


class FakeSession:
    def __init__(self) -> None:
        self.td_api_ids = [7]
        self.md_ids = ["bestquote.Paper_Spot_BTCUSDT"]
        self.exits: list[str] = []
        self.failures: list[str] = []

    def request_exit(self, reason: str, *, failed: bool = False) -> None:
        self.exits.append(reason)
        if failed:
            self.failures.append(reason)

    def td_sole(self) -> int:
        ids = list(self.td_api_ids)
        if len(ids) != 1:
            raise RuntimeError(f"needs exactly one td account, got {ids}")
        return ids[0]


class FakeTimer:
    def __init__(self) -> None:
        self._now = 1_000_000
        self.token_handle = _FakeToken()

    def now_ms(self) -> int:
        return self._now

    def advance_s(self, seconds: float) -> None:
        self._now += int(seconds * 1000)

    def token(self):
        return self.token_handle


class _FakeToken:
    def __init__(self) -> None:
        self.registered: list[tuple] = []

    def register(self, first_ms, interval_ms, func):
        self.registered.append((first_ms, interval_ms, func))
        return self

    def cancel(self) -> None:
        self.registered.clear()


def _strategy(*, perp: bool = False, **paras) -> TwapStrategy:
    payload = {
        "side": "buy",
        "qty_per_round": Decimal("0.1"),
        "exec_interval_s": 10,
        "num_round": 3,
    }
    payload.update(paras)

    info = BTCUSDT_PERP if perp else BTCUSDT
    ticker = info.universal_ticker

    strat = TwapStrategy()
    strat.paras = TwapStrategy.on_initialized(payload)
    strat.session = FakeSession()  # type: ignore[assignment]
    if perp:
        strat.session.md_ids = [f"bestquote.{ticker}"]
    strat.oms = FakeOms()  # type: ignore[assignment]
    strat.oms.strategy = strat
    strat.timer = FakeTimer()  # type: ignore[assignment]
    strat.ledger = FakeLedger()  # type: ignore[assignment]
    strat._info = info
    strat._ticker = UniversalTicker.parse(ticker)

    async def _noop_log(message: str, *, level: str = "info", **extra) -> None:
        return None

    strat.log = _noop_log  # type: ignore[method-assign]
    strat.owns = lambda cid: True  # type: ignore[method-assign]
    return strat


def _quote(bid: str, ask: str, *, ticker: str = "Paper_Spot_BTCUSDT") -> BestQuote:
    return BestQuote(
        universal_ticker=ticker,
        bid=Decimal(bid),
        bid_qty=Decimal("1"),
        ask=Decimal(ask),
        ask_qty=Decimal("1"),
    )


def _update(cid: str, filled: str, status: OrderStatus) -> Order:
    return Order(
        client_order_id=cid,
        universal_ticker="Paper_Spot_BTCUSDT",
        side=Side.BUY,
        type=OrderType.LIMIT,
        status=status,
        qty=Decimal("0.1"),
        filled_qty=Decimal(filled),
        price=Decimal("50000"),
    )


async def _arm(strat: TwapStrategy, bid: str = "49999", ask: str = "50000") -> None:
    ticker = str(strat._ticker) if strat._ticker is not None else "Paper_Spot_BTCUSDT"
    await strat.on_recon_done(ReconDone(session_id="s1", api_id=7, oms=OmsView()))
    await strat.on_best_quote(_quote(bid, ask, ticker=ticker))


async def _settle() -> None:
    """Let the fake venue's IOC answer task run past its yield."""
    await asyncio.sleep(0)
    await asyncio.sleep(0)


# --- parameters ------------------------------------------------------------


def test_side_must_be_buy_or_sell() -> None:
    with pytest.raises(ValueError, match="side must be"):
        TwapStrategy.on_initialized(
            {
                "side": "long",
                "qty_per_round": 1,
                "exec_interval_s": 5,
                "num_round": 2,
            }
        )


def test_one_of_qty_knobs_is_required() -> None:
    with pytest.raises(ValueError, match="one of qty_per_round"):
        TwapStrategy.on_initialized(
            {"side": "buy", "exec_interval_s": 5, "num_round": 2}
        )


def test_qty_knobs_together_are_refused() -> None:
    with pytest.raises(ValueError, match="not both"):
        TwapStrategy.on_initialized(
            {
                "side": "buy",
                "qty_per_round": 1,
                "qty_quote_per_round": 100,
                "exec_interval_s": 5,
                "num_round": 2,
            }
        )


@pytest.mark.parametrize("field", ["exec_interval_s", "num_round"])
def test_schedule_knobs_are_required_and_positive(field: str) -> None:
    payload = {
        "side": "buy",
        "qty_per_round": 1,
        "exec_interval_s": 5,
        "num_round": 2,
    }
    del payload[field]
    with pytest.raises(ValueError):
        TwapStrategy.on_initialized(payload)


def test_exec_total_s_is_interval_times_rounds() -> None:
    out = TwapStrategy.on_initialized(
        {
            "side": "buy",
            "qty_per_round": 1,
            "exec_interval_s": 5,
            "num_round": 6,
        }
    )
    assert out["exec_total_s"] == Decimal("30")
    assert out["num_round"] == 6


# --- arming ----------------------------------------------------------------


@pytest.mark.asyncio
async def test_arms_only_after_recon_and_first_quote() -> None:
    strat = _strategy()
    await strat.on_start()
    assert not strat._armed

    await strat.on_best_quote(_quote("49999", "50000"))
    assert not strat._armed

    await strat.on_recon_done(ReconDone(session_id="s1", api_id=7, oms=OmsView()))
    assert strat._armed
    assert strat._successes == 0
    assert strat._filled_qty() == 0

    first_ms, interval_ms, _ = strat.timer.token_handle.registered[-1]
    # first = now + interval/2; interval = exec_interval_s
    assert interval_ms == 10_000
    assert first_ms == strat.timer.now_ms() - 0 + 5_000
    # end = now + exec_total_s
    assert strat._end_ms == strat.timer.now_ms() + 30_000


@pytest.mark.asyncio
async def test_recon_after_quote_also_arms() -> None:
    strat = _strategy()
    await strat.on_start()
    await strat.on_recon_done(ReconDone(session_id="s1", api_id=7, oms=OmsView()))
    assert not strat._armed
    await strat.on_best_quote(_quote("49999", "50000"))
    assert strat._armed


# --- IOC takes -------------------------------------------------------------


@pytest.mark.asyncio
async def test_buy_ioc_crosses_the_ask() -> None:
    strat = _strategy(side="buy")
    await _arm(strat, bid="49999", ask="50000")
    await strat._on_tick()

    order = strat.oms.submitted[0]
    assert order["side"] is Side.BUY
    assert order["price"] == Decimal("50000")
    assert order["qty"] == Decimal("0.1")
    assert order["tif"] is TimeInForce.IOC
    assert order["type"] is OrderType.LIMIT


@pytest.mark.asyncio
async def test_sell_ioc_crosses_the_bid() -> None:
    strat = _strategy(side="sell")
    await _arm(strat, bid="50000", ask="50001")
    await strat._on_tick()

    order = strat.oms.submitted[0]
    assert order["side"] is Side.SELL
    assert order["price"] == Decimal("50000")
    assert order["tif"] is TimeInForce.IOC


@pytest.mark.asyncio
async def test_qty_quote_per_round_sizes_off_the_touch() -> None:
    strat = _strategy()
    strat.paras = TwapStrategy.on_initialized(
        {
            "side": "buy",
            "qty_quote_per_round": Decimal("500"),
            "exec_interval_s": 10,
            "num_round": 3,
        }
    )
    await _arm(strat, bid="49999", ask="50000")
    await strat._on_tick()

    # 500 / 50000 = 0.01
    assert strat.oms.submitted[0]["qty"] == Decimal("0.01")


@pytest.mark.asyncio
async def test_partial_fill_counts_as_a_success() -> None:
    strat = _strategy(num_round=2)
    strat.oms.ioc_fill = Decimal("0.04")
    await _arm(strat)
    await strat._on_tick()
    await _settle()

    assert strat._successes == 1
    assert strat._filled_qty() == Decimal("0.04")
    assert not strat._done


@pytest.mark.asyncio
async def test_zero_fill_does_not_count() -> None:
    strat = _strategy(num_round=2)
    strat.oms.ioc_fill = Decimal("0")
    await _arm(strat)
    await strat._on_tick()
    await _settle()

    assert strat._successes == 0
    assert strat._open_cid is None
    assert not strat._done


@pytest.mark.asyncio
async def test_exits_after_num_round_successes() -> None:
    strat = _strategy(num_round=2)
    await _arm(strat)

    await strat._on_tick()
    await _settle()
    assert strat._successes == 1
    assert not strat._done

    await strat._on_tick()
    await _settle()
    assert strat._successes == 2
    assert strat._done
    assert strat.session.exits[-1] == "twap_done"
    assert strat._filled_qty() == Decimal("0.2")


@pytest.mark.asyncio
async def test_exits_when_the_window_ends() -> None:
    strat = _strategy(num_round=5, exec_interval_s=10)
    await _arm(strat)
    # Window is 50s; past the end the tick exits without placing.
    strat.timer.advance_s(50)
    await strat._on_tick()

    assert strat._done
    assert strat.session.exits[-1] == "twap_time_up"
    assert strat.oms.submitted == []


@pytest.mark.asyncio
async def test_skips_while_an_ioc_is_still_open() -> None:
    strat = _strategy(num_round=3)
    # Do not auto-answer — leave the IOC in flight.
    strat.oms.strategy = None
    await _arm(strat)
    await strat._on_tick()
    assert len(strat.oms.submitted) == 1
    assert strat._open_cid is not None

    await strat._on_tick()
    assert len(strat.oms.submitted) == 1


@pytest.mark.asyncio
async def test_insufficient_balance_fails_the_session() -> None:
    strat = _strategy(side="buy")
    strat.ledger.balances["USDT"] = Decimal("1")
    await _arm(strat)
    await strat._on_tick()

    assert strat._done
    assert strat.session.failures[-1] == "twap_insufficient_balance"


@pytest.mark.asyncio
async def test_td_refusal_fails_the_session() -> None:
    strat = _strategy()
    strat.oms.accept = False
    strat.oms.reject_reason = "no td"
    strat.oms.reject_code = RejectCode.TD_SESSION_NOT_ATTACHED
    await _arm(strat)
    await strat._on_tick()

    assert strat._done
    assert strat.session.failures[-1] == "twap_refused"


# --- Perp ------------------------------------------------------------------


@pytest.mark.asyncio
async def test_spot_arm_skips_ensure_leverage() -> None:
    strat = _strategy()
    await _arm(strat)
    assert strat._armed
    assert strat.ledger.ensure_calls == []


@pytest.mark.asyncio
async def test_perp_arm_ensures_leverage() -> None:
    strat = _strategy(perp=True)
    await _arm(strat)
    assert strat._armed
    assert strat.ledger.ensure_calls == [
        ("BinanceUM_Perp_BTCUSDT", 7),
    ]
    assert strat.ledger.leverage("BinanceUM_Perp_BTCUSDT") == Decimal("10")


@pytest.mark.asyncio
async def test_perp_arm_fails_when_leverage_unavailable() -> None:
    strat = _strategy(perp=True)
    strat.ledger.ensure_result = None
    await _arm(strat)
    assert not strat._armed
    assert strat._done
    assert strat.session.failures[-1] == "twap_leverage_unavailable"


@pytest.mark.asyncio
async def test_perp_buy_shortfall_uses_margin_not_full_notional() -> None:
    # 0.1 * 50000 / 10 = 500 USDT margin; 1000 free is enough.
    strat = _strategy(perp=True, side="buy")
    strat.ledger.balances["USDT"] = Decimal("1000")
    strat.ledger.balances["BTC"] = Decimal("0")
    await _arm(strat)
    await strat._on_tick()

    assert len(strat.oms.submitted) == 1
    assert not strat._done


@pytest.mark.asyncio
async def test_perp_sell_also_needs_quote_margin() -> None:
    # Spot sell would only need BTC; Perp sell still locks USDT margin.
    strat = _strategy(perp=True, side="sell")
    strat.ledger.balances["BTC"] = Decimal("100")
    strat.ledger.balances["USDT"] = Decimal("1")
    await _arm(strat, bid="50000", ask="50001")
    await strat._on_tick()

    assert strat.oms.submitted == []
    assert strat._done
    assert strat.session.failures[-1] == "twap_insufficient_balance"
