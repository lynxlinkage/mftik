"""CrossArb — pricing math, range cancel, and one-shot full-qty hedge."""

from __future__ import annotations

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
from mftik.exchange.tickers import InvalidTickerError, UniversalTicker
from mftik.protocol import (
    CancelReject,
    OrderReject,
    ReconDone,
    RejectCode,
    SymbolInfo,
)
from mftik_sts.impl.cross_arb import (
    CrossArb,
    edge_bps,
    edge_in_band,
    hedge_raw_price,
    quote_raw_price,
    x_mid_bps,
)

QUOTE_TICKER = UniversalTicker.parse("Binance_Spot_BTCUSDT")
HEDGE_TICKER = UniversalTicker.parse("Gate_Spot_BTCUSDT")

QUOTE_INFO = SymbolInfo(
    universal_ticker="Binance_Spot_BTCUSDT",
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

HEDGE_INFO = SymbolInfo(
    universal_ticker="Gate_Spot_BTCUSDT",
    base="BTC",
    quote="USDT",
    exch_ticker="BTC_USDT",
    filters=[
        {"name": "price_tick", "value": Decimal("0.01")},
        {"name": "qty_step", "value": Decimal("0.00001")},
        {"name": "min_qty", "value": Decimal("0.00001")},
        {"name": "min_notional", "value": Decimal("5")},
    ],
)

QUOTE_API = 11
HEDGE_API = 22


class FakeOms:
    def __init__(self, *, accepts: list[bool] | None = None) -> None:
        self.submitted: list[dict] = []
        self.cancelled: list[str] = []
        self.accepts = accepts
        self.accept_cancel = True
        self.reject_reason = "no funds"
        self.reject_code: int | str = RejectCode.TD_INSUFFICIENT_BALANCE
        self._n = 0
        self._last_cid: str | None = None
        self._accepted_last = True

    @property
    def last_client_order_id(self) -> str | None:
        return self._last_cid

    @property
    def last_reject_reason(self) -> str:
        return "" if self._accepted_last else self.reject_reason

    @property
    def last_reject_code(self) -> int | str:
        return RejectCode.NONE if self._accepted_last else self.reject_code

    async def submit_order(
        self, api_id, *, ticker, side, qty, type, price=None, tif=None
    ):
        self._n += 1
        self._last_cid = f"cid-{self._n}"
        accepted = True
        if self.accepts is not None and self._n <= len(self.accepts):
            accepted = self.accepts[self._n - 1]
        self._accepted_last = accepted
        self.submitted.append(
            {
                "cid": self._last_cid,
                "api_id": api_id,
                "ticker": ticker,
                "side": side,
                "qty": qty,
                "type": type,
                "price": price,
                "tif": tif,
                "accepted": accepted,
            }
        )
        return accepted

    async def cancel_order(self, api_id, client_order_id):
        self.cancelled.append(str(client_order_id))
        self._accepted_last = self.accept_cancel
        return self.accept_cancel


class FakeLedger:
    def __init__(self) -> None:
        self.balances: dict[tuple[int, str], Decimal] = {}
        self.default = Decimal("1000000")

    async def available(self, asset: str, api_id=None) -> Decimal:
        return self.balances.get((api_id, asset), self.default)


class FakeSymbols:
    def __init__(self) -> None:
        self._by = {
            str(QUOTE_TICKER): QUOTE_INFO,
            str(HEDGE_TICKER): HEDGE_INFO,
        }

    async def get(self, ticker: UniversalTicker) -> SymbolInfo:
        return self._by[str(ticker)]


class FakeSession:
    def __init__(self) -> None:
        self.td_api_ids = [QUOTE_API, HEDGE_API]
        self.md_ids = [
            f"bestquote.{QUOTE_TICKER}",
            f"bestquote.{HEDGE_TICKER}",
        ]
        self.exits: list[str] = []
        self.failures: list[str] = []
        self.symbols = FakeSymbols()

    def request_exit(self, reason: str, *, failed: bool = False) -> None:
        self.exits.append(reason)
        if failed:
            self.failures.append(reason)


def _paras(**overrides) -> dict:
    payload = {
        "quote_ticker": str(QUOTE_TICKER),
        "hedge_ticker": str(HEDGE_TICKER),
        "side": ["buy", "sell"],
        "qty": Decimal("0.001"),
        "x_lo_bps": Decimal("5"),
        "x_hi_bps": Decimal("15"),
    }
    payload.update(overrides)
    return payload


def _strategy(**overrides) -> CrossArb:
    strat = CrossArb()
    strat.paras = CrossArb.on_initialized(_paras(**overrides))
    session = FakeSession()
    strat.session = session  # type: ignore[assignment]
    strat.oms = FakeOms()  # type: ignore[assignment]
    strat.ledger = FakeLedger()  # type: ignore[assignment]
    # Strategy.symbols is a property off session — seed caches instead.
    strat._quote_info = QUOTE_INFO
    strat._hedge_info = HEDGE_INFO
    strat._quote_ticker = QUOTE_TICKER
    strat._hedge_ticker = HEDGE_TICKER

    async def _noop_log(message: str, *, level: str = "info", **extra) -> None:
        return None

    strat.log = _noop_log  # type: ignore[method-assign]
    strat.owns = lambda cid: True  # type: ignore[method-assign]
    return strat


async def _armed(**overrides) -> CrossArb:
    strat = _strategy(**overrides)
    await strat.on_start()
    await strat.on_recon_done(ReconDone(session_id="s", api_id=QUOTE_API))
    await strat.on_recon_done(ReconDone(session_id="s", api_id=HEDGE_API))
    return strat


def _hedge_quote(bid: str = "50000", ask: str = "50010") -> BestQuote:
    return BestQuote(
        universal_ticker=str(HEDGE_TICKER),
        bid=Decimal(bid),
        bid_qty=Decimal("1"),
        ask=Decimal(ask),
        ask_qty=Decimal("1"),
    )


def _quote_quote(bid: str = "50000", ask: str = "50010") -> BestQuote:
    return BestQuote(
        universal_ticker=str(QUOTE_TICKER),
        bid=Decimal(bid),
        bid_qty=Decimal("1"),
        ask=Decimal(ask),
        ask_qty=Decimal("1"),
    )


def _update(
    cid: str,
    status: OrderStatus,
    *,
    side: Side,
    filled: str = "0",
    qty: str = "0.001",
    price: str = "50000",
) -> Order:
    return Order(
        client_order_id=cid,
        universal_ticker=str(QUOTE_TICKER),
        side=side,
        type=OrderType.LIMIT,
        status=status,
        qty=Decimal(qty),
        filled_qty=Decimal(filled),
        price=Decimal(price),
    )


# --- parameters ------------------------------------------------------------


def test_x_mid_is_average() -> None:
    assert x_mid_bps(Decimal("5"), Decimal("15")) == Decimal("10")


def test_quote_and_hedge_prices() -> None:
    q = _hedge_quote("100", "100")
    mid = Decimal("10")
    hi = Decimal("15")
    assert quote_raw_price(Side.SELL, q, mid) == Decimal("100.1")
    assert quote_raw_price(Side.BUY, q, mid) == Decimal("99.9")
    assert hedge_raw_price(Side.BUY, q, hi) == Decimal("100.3")
    assert hedge_raw_price(Side.SELL, q, hi) == Decimal("99.7")


def test_edge_band() -> None:
    q = _hedge_quote("100", "100")
    assert edge_bps(Side.SELL, Decimal("100.1"), q) == Decimal("10")
    assert edge_in_band(
        Side.SELL, Decimal("100.1"), q, Decimal("5"), Decimal("15")
    )
    assert not edge_in_band(
        Side.SELL, Decimal("100.2"), q, Decimal("5"), Decimal("15")
    )


def test_on_initialized_ok() -> None:
    paras = CrossArb.on_initialized(_paras())
    assert paras["x_mid_bps"] == Decimal("10")
    assert paras["side"] == [Side.BUY, Side.SELL]


def test_side_buy_only() -> None:
    paras = CrossArb.on_initialized(_paras(side=["buy"]))
    assert paras["side"] == [Side.BUY]


@pytest.mark.parametrize(
    "bad",
    [
        {"side": []},
        {"side": ["long"]},
        {"qty": 0},
        {"x_lo_bps": 20, "x_hi_bps": 10},
        {"quote_ticker": "Gate_Spot_BTCUSDT", "hedge_ticker": "Gate_Spot_BTCUSDT"},
    ],
)
def test_on_initialized_rejects_bad_config(bad: dict) -> None:
    payload = _paras()
    payload.update(bad)
    with pytest.raises(ValueError):
        CrossArb.on_initialized(payload)


def test_on_initialized_rejects_missing_tickers() -> None:
    with pytest.raises(InvalidTickerError):
        CrossArb.on_initialized({})


# --- arming / quoting ------------------------------------------------------


async def test_needs_both_recons_before_quoting() -> None:
    strat = _strategy()
    await strat.on_start()
    await strat.on_recon_done(ReconDone(session_id="s", api_id=QUOTE_API))
    await strat.on_best_quote(_hedge_quote())
    assert strat.oms.submitted == []

    await strat.on_recon_done(ReconDone(session_id="s", api_id=HEDGE_API))
    await strat.on_best_quote(_hedge_quote())
    assert len(strat.oms.submitted) == 2
    assert {row["side"] for row in strat.oms.submitted} == {Side.BUY, Side.SELL}
    assert all(row["tif"] is TimeInForce.POST_ONLY for row in strat.oms.submitted)
    assert all(row["api_id"] == QUOTE_API for row in strat.oms.submitted)


async def test_single_side_places_one_leg() -> None:
    strat = await _armed(side=["sell"])
    await strat.on_best_quote(_hedge_quote("50000", "50000"))
    assert len(strat.oms.submitted) == 1
    assert strat.oms.submitted[0]["side"] is Side.SELL
    # mid = 10 bps → 50000 * 1.001 = 50050
    assert strat.oms.submitted[0]["price"] == Decimal("50050")


async def test_skips_leg_when_quote_balance_short() -> None:
    strat = await _armed(side=["buy"])
    strat.ledger.balances[(QUOTE_API, "USDT")] = Decimal("1")
    await strat.on_best_quote(_hedge_quote("50000", "50010"))
    assert strat.oms.submitted == []
    assert strat.session.failures == []


async def test_skips_leg_when_hedge_balance_short() -> None:
    """A SELL quote needs the hedge account able to BUY the full qty."""
    strat = await _armed(side=["sell"])
    # Quote account can sell BTC; hedge cannot buy (no USDT).
    strat.ledger.balances[(QUOTE_API, "BTC")] = Decimal("1")
    strat.ledger.balances[(HEDGE_API, "USDT")] = Decimal("1")
    await strat.on_best_quote(_hedge_quote("50000", "50000"))
    assert strat.oms.submitted == []
    assert strat.session.failures == []


async def test_out_of_band_cancels_and_reprices() -> None:
    strat = await _armed(side=["sell"])
    await strat.on_best_quote(_hedge_quote("50000", "50000"))
    assert len(strat.oms.submitted) == 1
    cid = strat.oms.submitted[0]["cid"]

    # Hedge ask jumps so resting 50050 edge collapses below x_lo.
    await strat.on_best_quote(_hedge_quote("50000", "50100"))
    assert cid in strat.oms.cancelled

    # Cancel confirmed → leg cleared → next quote reposts.
    await strat.on_order_update(
        QUOTE_API,
        _update(cid, OrderStatus.CANCELED, side=Side.SELL, price="50050"),
    )
    await strat.on_best_quote(_hedge_quote("50000", "50100"))
    assert len(strat.oms.submitted) == 2
    assert strat.oms.submitted[1]["price"] == QUOTE_INFO.round_price(
        Decimal("50100") * Decimal("1.001")
    )


# --- hedge -----------------------------------------------------------------


async def test_partial_fill_hedges_full_qty_once() -> None:
    strat = await _armed(side=["sell"])
    await strat.on_best_quote(_hedge_quote("50000", "50000"))
    cid = strat.oms.submitted[0]["cid"]

    await strat.on_order_update(
        QUOTE_API,
        _update(
            cid,
            OrderStatus.PARTIALLY_FILLED,
            side=Side.SELL,
            filled="0.0003",
            price="50050",
        ),
    )
    hedges = [r for r in strat.oms.submitted if r["api_id"] == HEDGE_API]
    assert len(hedges) == 1
    assert hedges[0]["side"] is Side.BUY
    assert hedges[0]["qty"] == Decimal("0.001")
    assert hedges[0]["tif"] is TimeInForce.IOC
    # ask 50000 + 2*15 bps = 50150
    assert hedges[0]["price"] == Decimal("50150")

    # Same cid filled later — no second hedge.
    await strat.on_order_update(
        QUOTE_API,
        _update(
            cid,
            OrderStatus.FILLED,
            side=Side.SELL,
            filled="0.001",
            price="50050",
        ),
    )
    hedges = [r for r in strat.oms.submitted if r["api_id"] == HEDGE_API]
    assert len(hedges) == 1


async def test_buy_fill_hedges_with_sell_ioc() -> None:
    strat = await _armed(side=["buy"])
    await strat.on_best_quote(_hedge_quote("50000", "50000"))
    cid = strat.oms.submitted[0]["cid"]
    await strat.on_order_update(
        QUOTE_API,
        _update(
            cid, OrderStatus.FILLED, side=Side.BUY, filled="0.001", price="49950"
        ),
    )
    hedge = [r for r in strat.oms.submitted if r["api_id"] == HEDGE_API][0]
    assert hedge["side"] is Side.SELL
    assert hedge["price"] == Decimal("49850")


async def test_fill_cancels_sibling_leg() -> None:
    strat = await _armed(side=["buy", "sell"])
    await strat.on_best_quote(_hedge_quote("50000", "50000"))
    assert len(strat.oms.submitted) == 2
    by_side = {r["side"]: r["cid"] for r in strat.oms.submitted}
    await strat.on_order_update(
        QUOTE_API,
        _update(
            by_side[Side.SELL],
            OrderStatus.FILLED,
            side=Side.SELL,
            filled="0.001",
            price="50050",
        ),
    )
    assert by_side[Side.BUY] in strat.oms.cancelled


async def test_rearms_on_next_quote_after_hedge() -> None:
    strat = await _armed(side=["sell"])
    await strat.on_best_quote(_hedge_quote("50000", "50000"))
    cid = strat.oms.submitted[0]["cid"]
    await strat.on_order_update(
        QUOTE_API,
        _update(cid, OrderStatus.FILLED, side=Side.SELL, filled="0.001"),
    )
    assert Side.SELL not in strat._open

    await strat.on_best_quote(_quote_quote())
    # Quote-venue push re-arms using the last hedge touch.
    assert len([r for r in strat.oms.submitted if r["api_id"] == QUOTE_API]) == 2


async def test_post_only_reject_clears_open() -> None:
    strat = await _armed(side=["sell"])
    await strat.on_best_quote(_hedge_quote("50000", "50000"))
    cid = strat.oms.submitted[0]["cid"]
    await strat.on_order_reject(
        QUOTE_API,
        OrderReject(
            api_id=QUOTE_API,
            client_order_id=cid,
            reason="would take",
            error_code=RejectCode.VENUE_POST_ONLY_WOULD_CROSS,
        ),
    )
    assert Side.SELL not in strat._open


async def test_order_reject_send_failed_keeps_leg() -> None:
    """on_order_reject must match cancel-reject: TD_SEND_FAILED is ambiguous."""
    strat = await _armed(side=["sell"])
    await strat.on_best_quote(_hedge_quote("50000", "50000"))
    cid = strat.oms.submitted[0]["cid"]

    await strat.on_order_reject(
        QUOTE_API,
        OrderReject(
            api_id=QUOTE_API,
            client_order_id=cid,
            reason="Connection lost",
            error_code=RejectCode.TD_SEND_FAILED,
        ),
    )
    assert Side.SELL in strat._open
    assert strat._open[Side.SELL].cid == cid


async def test_cancel_send_failed_keeps_leg_until_terminal() -> None:
    """TD_SEND_FAILED must not optimistic-pop — that re-quotes a live order."""
    strat = await _armed(side=["sell"])
    await strat.on_best_quote(_hedge_quote("50000", "50000"))
    cid = strat.oms.submitted[0]["cid"]
    leg = strat._open[Side.SELL]
    leg.canceling = True

    await strat.on_cancel_reject(
        QUOTE_API,
        CancelReject(
            api_id=QUOTE_API,
            client_order_id=cid,
            reason="Connection lost",
            error_code=RejectCode.TD_SEND_FAILED,
        ),
    )
    assert Side.SELL in strat._open
    assert strat._open[Side.SELL].cid == cid
    assert strat._open[Side.SELL].canceling is False

    before = len(strat.oms.submitted)
    # Edge still outside band on a moved touch — may retry cancel, not place.
    await strat.on_best_quote(_hedge_quote("49000", "49000"))
    assert len([r for r in strat.oms.submitted if r["api_id"] == QUOTE_API]) == before

    await strat.on_order_update(
        QUOTE_API,
        _update(cid, OrderStatus.CANCELED, side=Side.SELL, price="50050"),
    )
    assert Side.SELL not in strat._open

    await strat.on_best_quote(_hedge_quote("50000", "50000"))
    quotes = [r for r in strat.oms.submitted if r["api_id"] == QUOTE_API]
    assert len(quotes) == before + 1
    assert quotes[-1]["cid"] != cid


async def test_venue_cancel_reject_still_clears_leg() -> None:
    strat = await _armed(side=["sell"])
    await strat.on_best_quote(_hedge_quote("50000", "50000"))
    cid = strat.oms.submitted[0]["cid"]

    await strat.on_cancel_reject(
        QUOTE_API,
        CancelReject(
            api_id=QUOTE_API,
            client_order_id=cid,
            reason="Unknown order",
            error_code=RejectCode.VENUE_ORDER_NOT_FOUND,
        ),
    )
    assert Side.SELL not in strat._open


async def test_cancel_ack_not_cancelable_keeps_leg() -> None:
    """TD_NOT_CANCELABLE must not drop the leg — that re-quotes a live order."""
    strat = await _armed(side=["sell"])
    await strat.on_best_quote(_hedge_quote("50000", "50000"))
    cid = strat.oms.submitted[0]["cid"]
    strat.oms.accept_cancel = False
    strat.oms.reject_code = RejectCode.TD_NOT_CANCELABLE
    strat.oms.reject_reason = "order is unknown; it cannot be cancelled"

    await strat._cancel_leg(QUOTE_API, Side.SELL)

    assert Side.SELL in strat._open
    assert strat._open[Side.SELL].cid == cid
    assert strat._open[Side.SELL].canceling is False


async def test_refused_cancel_is_paced_not_retried_every_tick() -> None:
    """Keeping the leg must not mean cancelling it on every book update.

    ``_maintain_quotes`` runs per quote, so an un-paced retry against a down
    link is a cancel request per market tick.
    """
    strat = await _armed(side=["sell"])
    await strat.on_best_quote(_hedge_quote("50000", "50000"))
    cid = strat.oms.submitted[0]["cid"]
    strat.oms.accept_cancel = False
    strat.oms.reject_code = RejectCode.TD_SEND_FAILED
    strat.oms.reject_reason = "connection lost"

    # Every one of these puts the resting quote outside the band.
    for _ in range(5):
        await strat.on_best_quote(_hedge_quote("49000", "49000"))

    assert strat.oms.cancelled == [cid]
    assert Side.SELL in strat._open

    # Deferred, not abandoned: past the cooldown the retry goes out.
    strat._open[Side.SELL].retry_cancel_at = 0.0
    await strat.on_best_quote(_hedge_quote("49000", "49000"))
    assert strat.oms.cancelled == [cid, cid]


# --- rebuild ---------------------------------------------------------------


async def test_rebuild_is_a_clean_restart() -> None:
    assert CrossArb.rebuildable is True
    strat = await _armed(side=["sell"])
    await strat.on_best_quote(_hedge_quote("50000", "50000"))
    assert strat._open

    leftover = _update(
        "old-cid", OrderStatus.NEW, side=Side.SELL, price="50050"
    )
    await strat.on_rebuild({})
    assert strat._restoring
    assert not strat._armed
    assert strat._open == {}
    assert strat._hedge_quote is None

    await strat.on_recon_done(
        ReconDone(
            session_id="s",
            api_id=QUOTE_API,
            oms=OmsView(orders={"old-cid": leftover}),
        )
    )
    assert "old-cid" in strat.oms.cancelled

    await strat.on_recon_done(ReconDone(session_id="s", api_id=HEDGE_API))
    assert strat._armed
    assert not strat._restoring

    await strat.on_best_quote(_hedge_quote("50000", "50000"))
    fresh = [r for r in strat.oms.submitted if r["api_id"] == QUOTE_API]
    # One from before rebuild, one after the clean restart.
    assert len(fresh) == 2
    assert fresh[-1]["cid"] != "old-cid"
    assert fresh[-1]["tif"] is TimeInForce.POST_ONLY
