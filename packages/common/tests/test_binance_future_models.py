"""Futures wire models — the readings that differ from spot's.

Every test here is about a field that means something different on this market
than on the other one, because those are the ones a shared model can silently
get backwards: a liquidation's side, a ticker with no quote in it, a partial
depth push shaped exactly like a diff, a wallet with no free/locked split.
"""

from __future__ import annotations

from decimal import Decimal

import pytest
from mftik.exchange.binance.future.listing import to_listed
from mftik.exchange.binance.future.models import (
    BinanceFutureAccountUpdate,
    BinanceFutureAggTrade,
    BinanceFutureBalance,
    BinanceFutureBookTicker,
    BinanceFutureDepth,
    BinanceFutureDepthUpdate,
    BinanceFutureKlineEvent,
    BinanceFutureLiquidation,
    BinanceFutureMarkPrice,
    BinanceFutureOrderAck,
    BinanceFutureOrderTradeUpdate,
    BinanceFuturePosition,
    BinanceFutureTicker,
    status_of,
    type_of,
)
from mftik.exchange.models import OrderStatus, OrderType, Side
from mftik.exchange.tickers import UniversalTicker

TICKER = UniversalTicker.parse("BinanceFuture_Perp_BTCUSDT")


# --- market streams --------------------------------------------------------


def test_the_tape_reports_the_aggressor_not_the_maker() -> None:
    """``m`` is the maker flag: true means the buyer rested, so the taker sold."""
    row = BinanceFutureAggTrade.model_validate(
        {
            "e": "aggTrade",
            "E": 1672515782136,
            "s": "BTCUSDT",
            "a": 12345,
            "p": "40000",
            "q": "0.5",
            "f": 100,
            "l": 139,
            "T": 1672515782136,
            "m": True,
        }
    )
    trade = row.to_trade(TICKER)
    assert trade.side is Side.SELL
    assert trade.price == Decimal("40000")
    assert trade.trade_id == "12345", "the aggregate's id — futures has no other"
    assert row.to_agg_trade(TICKER).match_count == 40


def test_the_ticker_has_no_quote_of_its_own() -> None:
    """The futures 24h ticker carries no ``b``/``a`` — the model must not invent."""
    row = BinanceFutureTicker.model_validate(
        {
            "e": "24hrTicker",
            "E": 1672515782136,
            "s": "BTCUSDT",
            "c": "40000",
            "o": "39000",
            "h": "41000",
            "l": "38000",
            "v": "100",
            "q": "4000000",
        }
    )
    with pytest.raises(TypeError):
        row.to_ticker(TICKER)  # type: ignore[call-arg]

    ticker = row.to_ticker(TICKER, bid=Decimal("39999"), ask=Decimal("40001"))
    assert (ticker.bid, ticker.last, ticker.ask) == (
        Decimal("39999"),
        Decimal("40000"),
        Decimal("40001"),
    )


def test_the_book_ticker_dates_itself() -> None:
    """Unlike spot's, which carries only an update id."""
    quote = BinanceFutureBookTicker.model_validate(
        {
            "e": "bookTicker",
            "u": 400900217,
            "s": "BTCUSDT",
            "b": "25.35190000",
            "B": "31.21000000",
            "a": "25.36520000",
            "A": "40.66000000",
            "T": 1568014460893,
            "E": 1568014460893,
        }
    ).to_best_quote(TICKER)
    assert quote.bid == Decimal("25.35190000")
    assert quote.ask_qty == Decimal("40.66000000")
    assert quote.ts == 1568014460.893


def test_partial_depth_is_a_book_and_the_diff_stream_is_not() -> None:
    """One payload shape, two meanings — the stream it came from decides."""
    payload = {
        "e": "depthUpdate",
        "E": 1571889248277,
        "T": 1571889248276,
        "s": "BTCUSDT",
        "U": 390497796,
        "u": 390497878,
        "pu": 390497794,
        "b": [["7403.89", "0.002"], ["7403.90", "3.906"]],
        "a": [["7405.96", "3.340"]],
    }
    row = BinanceFutureDepthUpdate.model_validate(payload)

    book = row.to_order_book(TICKER)
    assert [level.price for level in book.bids] == [
        Decimal("7403.89"),
        Decimal("7403.90"),
    ]
    assert book.ts == 1571889248.276, "futures dates its book pushes"

    # As a diff, the same message is checked against the previous ``u`` — a
    # rule spot has no field for.
    assert row.follows(390497794)
    assert not row.follows(390497790)


def test_the_depth_reply_carries_no_symbol_so_it_is_told_one() -> None:
    book = BinanceFutureDepth.model_validate(
        {
            "lastUpdateId": 1027024,
            "E": 1589436922972,
            "T": 1589436922959,
            "bids": [["4.00000000", "431.00000000"]],
            "asks": [["4.00000200", "12.00000000"]],
        }
    ).to_order_book(TICKER)
    assert book.universal_ticker == "BinanceFuture_Perp_BTCUSDT"
    assert book.bids[0].qty == Decimal("431")


def test_a_liquidation_reports_the_position_side_not_the_order_side() -> None:
    """Binance sends the *closing* order: a SELL means a long was wiped out.

    :class:`~mftik.exchange.models.Liquidation` states the liquidated position's
    side, so reading ``S`` straight through would mirror every event on the
    feed.
    """
    event = BinanceFutureLiquidation.model_validate(
        {
            "e": "forceOrder",
            "E": 1568014460893,
            "o": {
                "s": "BTCUSDT",
                "S": "SELL",
                "o": "LIMIT",
                "f": "IOC",
                "q": "0.014",
                "p": "9910",
                "ap": "9910.3",
                "X": "FILLED",
                "l": "0.014",
                "z": "0.014",
                "T": 1568014460893,
            },
        }
    )
    liquidation = event.to_liquidation(TICKER)
    assert liquidation.side is Side.BUY, "a long was closed out"
    assert liquidation.qty == Decimal("0.014")
    assert liquidation.price == Decimal("9910.3"), "the average fill, not the limit"
    assert liquidation.ts == 1568014460.893


def test_mark_price_carries_the_funding_schedule() -> None:
    row = BinanceFutureMarkPrice.model_validate(
        {
            "e": "markPriceUpdate",
            "E": 1562305380000,
            "s": "BTCUSDT",
            "p": "11794.15000000",
            "i": "11784.62659091",
            "P": "11784.25641265",
            "r": "0.00038167",
            "T": 1562306400000,
        }
    )
    assert row.mark_price == Decimal("11794.15000000")
    assert row.funding_rate == Decimal("0.00038167")
    assert row.next_funding_time == 1562306400000
    assert not hasattr(row, "to_ticker"), "a mark price is not a quote"
    funding = row.to_funding_rate(TICKER)
    assert funding is not None
    assert funding.rate == Decimal("0.00038167")
    assert funding.ts == 1562305380.0
    assert not hasattr(funding, "next_funding_time")


def test_a_mark_price_without_a_rate_is_not_a_funding_print() -> None:
    row = BinanceFutureMarkPrice.model_validate(
        {
            "e": "markPriceUpdate",
            "E": 1562305380000,
            "s": "BTCUSDT",
            "p": "11794.15000000",
        }
    )
    assert row.to_funding_rate(TICKER) is None


def test_a_kline_says_whether_its_window_closed() -> None:
    event = BinanceFutureKlineEvent.model_validate(
        {
            "e": "kline",
            "E": 1638747660000,
            "s": "BTCUSDT",
            "k": {
                "t": 1638747660000,
                "T": 1638747719999,
                "s": "BTCUSDT",
                "i": "1m",
                "o": "41000",
                "c": "41100",
                "h": "41200",
                "l": "40900",
                "v": "12",
                "q": "492000",
                "n": 30,
                "x": False,
            },
        }
    )
    kline = event.to_kline(TICKER)
    assert kline.interval == "1m", "still Binance's spelling at this layer"
    assert kline.closed is False
    assert kline.open_time == 1638747660.0
    assert kline.quote_volume == Decimal("492000")


# --- user data -------------------------------------------------------------


ORDER_UPDATE = {
    "e": "ORDER_TRADE_UPDATE",
    "E": 1568879465651,
    "T": 1568879465650,
    "o": {
        "s": "BTCUSDT",
        "c": "c-42",
        "S": "BUY",
        "o": "LIMIT",
        "f": "GTC",
        "q": "1.5",
        "p": "40000",
        "ap": "40000.5",
        "x": "TRADE",
        "X": "PARTIALLY_FILLED",
        "i": 8886774,
        "l": "0.5",
        "z": "0.9",
        "L": "40001",
        "N": "USDT",
        "n": "0.008",
        "T": 1568879465650,
        "t": 91921,
        "m": False,
        "R": False,
        "ps": "BOTH",
        "rp": "0",
    },
}


def test_an_order_update_reports_the_venue_average_price() -> None:
    """Futures publishes ``ap``; nothing here divides one total by another."""
    order = BinanceFutureOrderTradeUpdate.model_validate(ORDER_UPDATE).to_order(
        TICKER
    )
    assert order.order_id == "8886774"
    assert order.client_order_id == "c-42"
    assert order.status is OrderStatus.PARTIALLY_FILLED
    assert order.type is OrderType.LIMIT
    assert order.filled_qty == Decimal("0.9")
    assert order.avg_price == Decimal("40000.5")
    assert order.price == Decimal("40000")


def test_a_fill_is_this_execution_not_the_running_total() -> None:
    """``l``/``L``, never ``z`` — using the total double-counts every partial."""
    update = BinanceFutureOrderTradeUpdate.model_validate(ORDER_UPDATE)
    assert update.is_fill
    fill = update.to_fill(TICKER)
    assert fill.qty == Decimal("0.5"), "this execution, not the order's 0.9"
    assert fill.price == Decimal("40001")
    assert fill.fill_id == "91921"
    assert (fill.fee, fill.fee_asset) == (Decimal("0.008"), "USDT")


def test_a_state_change_without_an_execution_is_not_a_fill() -> None:
    payload = {**ORDER_UPDATE, "o": {**ORDER_UPDATE["o"], "x": "NEW", "l": "0"}}
    assert not BinanceFutureOrderTradeUpdate.model_validate(payload).is_fill


def test_the_client_order_id_survives_a_cancel() -> None:
    """Futures keeps it in ``c`` throughout, where spot moves it to ``C``."""
    payload = {
        **ORDER_UPDATE,
        "o": {**ORDER_UPDATE["o"], "x": "CANCELED", "X": "CANCELED"},
    }
    update = BinanceFutureOrderTradeUpdate.model_validate(payload)
    assert update.client_order_id == "c-42"
    assert update.to_order(TICKER).status is OrderStatus.CANCELED


ACCOUNT_UPDATE = {
    "e": "ACCOUNT_UPDATE",
    "E": 1564745798939,
    "T": 1564745798938,
    "a": {
        "m": "ORDER",
        "B": [
            {"a": "USDT", "wb": "122624", "cw": "100000", "bc": "50"},
            {"a": "BUSD", "wb": "1", "cw": "1", "bc": "0"},
        ],
        "P": [
            {
                "s": "BTCUSDT",
                "pa": "-1.5",
                "ep": "40000",
                "cr": "200",
                "up": "-12.5",
                "mt": "cross",
                "iw": "0",
                "ps": "BOTH",
            }
        ],
    },
}


def test_an_account_update_splits_the_wallet_into_free_and_held() -> None:
    """A futures wallet publishes no free/locked; the split is what is usable.

    ``wb`` is everything and ``cw`` what is not tied up in an isolated
    position, so reporting ``wb`` as free would let a session size an order
    against margin it cannot spend.
    """
    balances = BinanceFutureAccountUpdate.model_validate(ACCOUNT_UPDATE).to_balances()
    usdt = next(b for b in balances if b.asset == "USDT")
    assert usdt.free == Decimal("100000")
    assert usdt.locked == Decimal("22624")
    assert usdt.total == Decimal("122624")


def test_a_position_row_is_signed() -> None:
    """Negative is short — one number that cannot disagree with itself."""
    update = BinanceFutureAccountUpdate.model_validate(ACCOUNT_UPDATE)
    assert update.reason == "ORDER"
    position = update.position_rows()[0].to_position(TICKER)
    assert position.qty == Decimal("-1.5")
    assert position.entry_price == Decimal("40000")
    assert position.unrealised_pnl == Decimal("-12.5")
    assert not position.flat


# --- call replies ----------------------------------------------------------


def test_an_order_reply_reports_the_order_that_was_placed() -> None:
    """``origType`` where Binance rewrote ``type`` for a triggered order."""
    ack = BinanceFutureOrderAck.model_validate(
        {
            "orderId": 22542179,
            "symbol": "BTCUSDT",
            "status": "NEW",
            "clientOrderId": "c-42",
            "price": "40000",
            "avgPrice": "0.00000",
            "origQty": "1.5",
            "executedQty": "0",
            "cumQuote": "0",
            "timeInForce": "GTX",
            "type": "LIMIT",
            "origType": "LIMIT",
            "side": "BUY",
            "positionSide": "BOTH",
            "reduceOnly": False,
            "updateTime": 1566818724722,
        }
    )
    order = ack.to_order(TICKER)
    assert order.status is OrderStatus.NEW
    assert order.side is Side.BUY, "uppercase on the wire, lowercase here"
    assert order.avg_price is None, "nothing has filled — not a zero price"
    assert order.qty == Decimal("1.5")
    assert order.ts == 1566818724.722


def test_a_balance_row_derives_what_is_still_committable() -> None:
    balance = BinanceFutureBalance.model_validate(
        {
            "accountAlias": "SgsR",
            "asset": "USDT",
            "balance": "122624",
            "crossWalletBalance": "122624",
            "crossUnPnl": "0",
            "availableBalance": "100000",
            "maxWithdrawAmount": "100000",
            "marginAvailable": True,
        }
    ).to_balance()
    assert balance.free == Decimal("100000")
    assert balance.locked == Decimal("22624"), "margin already posted"


def test_a_position_reply_carries_the_signed_size() -> None:
    position = BinanceFuturePosition.model_validate(
        {
            "symbol": "BTCUSDT",
            "positionSide": "BOTH",
            "positionAmt": "0",
            "entryPrice": "0.0",
            "unRealizedProfit": "0",
            "markPrice": "40000",
            "updateTime": 1655217461579,
        }
    ).to_position(TICKER)
    assert position.flat, "a closed position is reported, not omitted"
    assert position.entry_price is None, "0 means unset, not free"


def test_instruments_read_the_futures_notional_key() -> None:
    """``notional``, not spot's ``minNotional`` — the floor vanishes otherwise."""
    instrument = to_listed(
        {
            "symbol": "BTCUSDT",
            "contractType": "PERPETUAL",
            "baseAsset": "BTC",
            "quoteAsset": "USDT",
            "filters": [
                {"filterType": "PRICE_FILTER", "tickSize": "0.10"},
                {"filterType": "LOT_SIZE", "stepSize": "0.001", "minQty": "0.001"},
                {"filterType": "MIN_NOTIONAL", "notional": "100"},
            ],
        }
    )
    assert instrument is not None
    assert instrument.exch_ticker == "BTCUSDT"
    assert instrument.filters["price_tick"] == Decimal("0.10")
    assert instrument.filters["qty_step"] == Decimal("0.001")
    assert instrument.filters["min_notional"] == Decimal("100")


def test_a_step_the_venue_does_not_enforce_reads_as_absent() -> None:
    """A zero step would divide."""
    instrument = to_listed(
        {
            "symbol": "BTCUSDT",
            "contractType": "PERPETUAL",
            "baseAsset": "BTC",
            "quoteAsset": "USDT",
            "filters": [{"filterType": "MIN_NOTIONAL", "notional": "0"}],
        }
    )
    assert instrument is not None
    assert instrument.filters["min_notional"] is None


# --- vocabulary ------------------------------------------------------------


def test_liquidation_side_statuses_are_resting_orders() -> None:
    """The insurance fund and ADL take the other side; both name a live order."""
    assert status_of("NEW_INSURANCE") is OrderStatus.NEW
    assert status_of("NEW_ADL") is OrderStatus.NEW


def test_an_unknown_status_is_unknown_rather_than_a_guess() -> None:
    assert status_of("SOMETHING_NEW") is OrderStatus.UNKNOWN
    assert status_of(None) is OrderStatus.UNKNOWN


def test_conditional_types_read_as_the_leg_they_become() -> None:
    assert type_of("STOP_MARKET") is OrderType.MARKET
    assert type_of("TAKE_PROFIT") is OrderType.LIMIT
    assert type_of("LIQUIDATION") is OrderType.MARKET
