"""Binance spot wire models, against real payload shapes from the docs.

The readings worth pinning down are the ones a plausible-looking mistake would
survive review: which way ``m`` points, which of ``l``/``z`` is this fill and
which is the order's total, and where a client order id lives on a cancel.
"""

from __future__ import annotations

from decimal import Decimal

import pytest
from mft.exchange.binance.spot.models import (
    BinanceAccountPosition,
    BinanceAggTrade,
    BinanceBalanceUpdate,
    BinanceBookTicker,
    BinanceDepth,
    BinanceDepthUpdate,
    BinanceExecutionReport,
    BinanceKlineEvent,
    BinanceOrderAck,
    BinanceSpotHistoricalOrder,
    BinanceSpotMyTrade,
    BinanceTicker,
    BinanceTrade,
    kline_from_row,
    status_of,
    type_of,
)
from mft.exchange.models import AggTrade, OrderStatus, OrderType, Side, Trade
from mft.exchange.tickers import UniversalTicker

#: The instrument every payload in this module is stamped with.
TICKER = UniversalTicker.parse("Binance_Spot_BTCUSDT")

AGG_TRADE = {
    "e": "aggTrade",
    "E": 1672515782136,
    "s": "BNBBTC",
    "a": 12345,
    "p": "0.001",
    "q": "100",
    "f": 100,
    "l": 105,
    "T": 1672515782136,
    "m": True,
    "M": True,
}

EXECUTION_REPORT = {
    "e": "executionReport",
    "E": 1499405658658,
    "s": "ETHBTC",
    "c": "mUvoqJxFIILMdfAW5iGSOW",
    "S": "BUY",
    "o": "LIMIT",
    "f": "GTC",
    "q": "1.00000000",
    "p": "0.10264410",
    "P": "0.00000000",
    "F": "0.00000000",
    "g": -1,
    "C": "",
    "x": "NEW",
    "X": "NEW",
    "r": "NONE",
    "i": 4293153,
    "l": "0.00000000",
    "z": "0.00000000",
    "L": "0.00000000",
    "n": "0",
    "N": None,
    "T": 1499405658657,
    "t": -1,
    "I": 8641984,
    "w": True,
    "m": False,
    "M": False,
    "O": 1499405658657,
    "Z": "0.00000000",
    "Y": "0.00000000",
    "Q": "0.00000000",
    "W": 1499405658657,
    "V": "NONE",
}

ORDER_ACK = {
    "symbol": "BTCUSDT",
    "orderId": 12569099453,
    "orderListId": -1,
    "clientOrderId": "my-order-1",
    "transactTime": 1660801715639,
    "price": "23416.10000000",
    "origQty": "0.00847000",
    "executedQty": "0.00000000",
    "cummulativeQuoteQty": "0.00000000",
    "status": "NEW",
    "timeInForce": "GTC",
    "type": "LIMIT",
    "side": "SELL",
    "workingTime": 1660801715639,
    "selfTradePreventionMode": "NONE",
}


# --- market data -----------------------------------------------------------


def test_the_maker_flag_names_the_taker_not_the_buyer() -> None:
    """``m: true`` means the buyer rested, so the aggressor sold."""
    sell = BinanceAggTrade.model_validate(AGG_TRADE).to_trade(TICKER)
    assert sell.side is Side.SELL

    buy = BinanceAggTrade.model_validate({**AGG_TRADE, "m": False}).to_trade(TICKER)
    assert buy.side is Side.BUY


def test_agg_trade_converts_with_the_aggregate_id_and_trade_time() -> None:
    trade = BinanceAggTrade.model_validate(AGG_TRADE).to_trade(TICKER)
    assert trade.trade_id == "12345"
    assert trade.universal_ticker == str(TICKER)
    assert trade.price == Decimal("0.001")
    assert trade.qty == Decimal("100")
    assert trade.ts == pytest.approx(1672515782.136)


def test_agg_trade_keeps_the_match_range_the_tape_cannot_report() -> None:
    agg = BinanceAggTrade.model_validate(AGG_TRADE).to_agg_trade(TICKER)

    assert agg.trade_id == "12345", "the aggregate's id, not a match's"
    assert agg.first_trade_id == "100"
    assert agg.last_trade_id == "105"
    assert agg.match_count == 6
    # Everything a plain Trade promises still means the same thing.
    assert agg.price == Decimal("0.001")
    assert agg.side is Side.SELL


def test_an_agg_trade_is_usable_wherever_a_trade_is() -> None:
    """Additive, so a strategy that ignores the aggregation need not care."""
    agg = BinanceAggTrade.model_validate(AGG_TRADE).to_agg_trade(TICKER)
    assert isinstance(agg, Trade)


def test_a_single_match_print_counts_as_one() -> None:
    agg = BinanceAggTrade.model_validate(
        {**AGG_TRADE, "f": 42, "l": 42}
    ).to_agg_trade(TICKER)
    assert agg.match_count == 1


def test_a_print_with_no_range_counts_nothing_rather_than_guessing() -> None:
    """Zero means the venue sent no range, not that nothing traded."""
    bare = AggTrade(
        universal_ticker=str(TICKER),
        price=Decimal("1"),
        qty=Decimal("2"),
        side=Side.BUY,
    )
    assert bare.match_count == 0
    assert bare.qty == Decimal("2")


def test_non_numeric_trade_ids_do_not_break_the_count() -> None:
    """A venue whose ids are not integers still identifies the range."""
    odd = AggTrade(
        universal_ticker=str(TICKER),
        price=Decimal("1"),
        qty=Decimal("2"),
        side=Side.BUY,
        first_trade_id="a1",
        last_trade_id="a9",
    )
    assert odd.match_count == 0
    assert odd.first_trade_id == "a1"


def test_to_trade_drops_the_aggregation() -> None:
    """Still offered, and still honest about what the id is."""
    plain = BinanceAggTrade.model_validate(AGG_TRADE).to_trade(TICKER)
    assert type(plain) is Trade
    assert plain.trade_id == "12345"


def test_raw_trade_uses_its_own_trade_id() -> None:
    trade = BinanceTrade.model_validate(
        {
            "e": "trade",
            "E": 1672515782136,
            "s": "BNBBTC",
            "t": 12345,
            "p": "0.001",
            "q": "100",
            "T": 1672515782136,
            "m": False,
        }
    ).to_trade(TICKER)
    assert trade.trade_id == "12345"
    assert trade.side is Side.BUY


def test_kline_reads_open_time_in_seconds_and_the_closed_flag() -> None:
    event = BinanceKlineEvent.model_validate(
        {
            "e": "kline",
            "E": 1672515782136,
            "s": "BNBBTC",
            "k": {
                "t": 1672515780000,
                "T": 1672515839999,
                "s": "BNBBTC",
                "i": "1m",
                "o": "0.001",
                "c": "0.002",
                "h": "0.0025",
                "l": "0.0015",
                "v": "1000",
                "n": 100,
                "x": False,
                "q": "1",
            },
        }
    )
    assert event.interval == "1m"
    kline = event.to_kline(TICKER)
    assert kline.open_time == pytest.approx(1672515780.0)
    assert (kline.open, kline.high, kline.low, kline.close) == (
        Decimal("0.001"),
        Decimal("0.0025"),
        Decimal("0.0015"),
        Decimal("0.002"),
    )
    assert kline.volume == Decimal("1000")
    assert kline.quote_volume == Decimal("1")
    assert kline.closed is False


def test_ticker_falls_back_to_last_when_a_side_is_unquoted() -> None:
    """Binance publishes zero rather than omitting the side on an empty book."""
    ticker = BinanceTicker.model_validate(
        {"e": "24hrTicker", "E": 1, "s": "BNBBTC", "c": "0.0025", "b": "0", "a": "0"}
    ).to_ticker(TICKER)
    assert ticker.bid == Decimal("0.0025")
    assert ticker.ask == Decimal("0.0025")


def test_book_ticker_keeps_both_sizes() -> None:
    quote = BinanceBookTicker.model_validate(
        {
            "u": 400900217,
            "s": "BNBUSDT",
            "b": "25.3519",
            "B": "31.21",
            "a": "25.3652",
            "A": "40.66",
        }
    )
    assert quote.bid == Decimal("25.3519")
    assert quote.bid_size == Decimal("31.21")
    assert quote.ask_size == Decimal("40.66")


def test_partial_depth_is_told_which_instrument_it_is() -> None:
    """The payload names no instrument; the caller supplies it."""
    depth = BinanceDepth.model_validate(
        {
            "lastUpdateId": 160,
            "bids": [["0.0024", "10"]],
            "asks": [["0.0026", "100"]],
        }
    )
    book = depth.to_order_book(TICKER, ts=1.5)
    assert book.universal_ticker == str(TICKER)
    assert book.ts == 1.5
    assert book.bids[0].price == Decimal("0.0024")
    assert book.asks[0].qty == Decimal("100")


def test_a_book_with_no_timestamp_is_stamped_on_arrival() -> None:
    book = BinanceDepth.model_validate({"bids": [], "asks": []}).to_order_book("X")
    assert book.ts > 0


def test_depth_diff_knows_whether_it_slots_onto_a_book() -> None:
    diff = BinanceDepthUpdate.model_validate(
        {
            "e": "depthUpdate",
            "E": 1672515782136,
            "s": "BNBBTC",
            "U": 157,
            "u": 160,
            "b": [["0.0024", "10"]],
            "a": [["0.0026", "0"]],
        }
    )
    # A book at 156 needs 157, which this diff starts at; one at 159 needs 160,
    # which it still covers. A book already at 160 has applied it.
    assert diff.follows(156) is True
    assert diff.follows(159) is True
    assert diff.follows(160) is False
    assert diff.follows(150) is False, "a gap must not be papered over"
    # A zero quantity is a deletion, and survives as one.
    assert diff.ask_levels()[0].qty == Decimal("0")
    assert not hasattr(diff, "to_order_book")


# --- user data -------------------------------------------------------------


def test_execution_report_converts_a_resting_order() -> None:
    order = BinanceExecutionReport.model_validate(EXECUTION_REPORT).to_order(TICKER)
    assert order.order_id == "4293153"
    assert order.client_order_id == "mUvoqJxFIILMdfAW5iGSOW"
    assert order.universal_ticker == str(TICKER)
    assert order.side is Side.BUY
    assert order.type is OrderType.LIMIT
    assert order.status is OrderStatus.NEW
    assert order.qty == Decimal("1")
    assert order.price == Decimal("0.10264410")
    assert order.filled_qty == Decimal("0")
    # Nothing filled, so there is no average to report.
    assert order.avg_price is None


def test_a_partial_fill_reports_this_execution_not_the_running_total() -> None:
    """``l``/``L`` are this fill; ``z``/``Z`` are the order so far."""
    report = BinanceExecutionReport.model_validate(
        {
            **EXECUTION_REPORT,
            "x": "TRADE",
            "X": "PARTIALLY_FILLED",
            "l": "0.30000000",
            "L": "0.10000000",
            "z": "0.70000000",
            "Z": "0.07100000",
            "n": "0.00010000",
            "N": "BNB",
            "t": 987654,
        }
    )
    assert report.is_fill

    fill = report.to_fill(TICKER)
    assert fill.qty == Decimal("0.3"), "must be this fill, not the running total"
    assert fill.price == Decimal("0.1")
    assert fill.fill_id == "987654"
    assert fill.fee == Decimal("0.0001")
    assert fill.fee_asset == "BNB"

    order = report.to_order(TICKER)
    assert order.filled_qty == Decimal("0.7")
    # Average price is Z/z — Binance publishes no field for it.
    assert order.avg_price == Decimal("0.071") / Decimal("0.7")


def test_a_state_change_that_carries_no_execution_is_not_a_fill() -> None:
    canceled = BinanceExecutionReport.model_validate(
        {**EXECUTION_REPORT, "x": "CANCELED", "X": "CANCELED"}
    )
    assert canceled.is_fill is False
    assert canceled.to_order(TICKER).status is OrderStatus.CANCELED


def test_a_cancel_report_keeps_the_orders_id_not_the_cancels() -> None:
    """``c`` is the cancel request's id; ``C`` holds the order's."""
    report = BinanceExecutionReport.model_validate(
        {
            **EXECUTION_REPORT,
            "x": "CANCELED",
            "X": "CANCELED",
            "c": "cancel-request-9",
            "C": "my-order-1",
        }
    )
    assert report.client_order_id == "my-order-1"


def test_a_market_order_reports_no_limit_price() -> None:
    order = BinanceExecutionReport.model_validate(
        {**EXECUTION_REPORT, "o": "MARKET", "p": "0.00000000"}
    ).to_order(TICKER)
    assert order.type is OrderType.MARKET
    assert order.price is None


def test_account_position_flattens_to_one_balance_per_asset() -> None:
    balances = BinanceAccountPosition.model_validate(
        {
            "e": "outboundAccountPosition",
            "E": 1564034571105,
            "u": 1564034571073,
            "B": [
                {"a": "ETH", "f": "10000.000000", "l": "0.000000"},
                {"a": "BTC", "f": "1.5", "l": "0.5"},
            ],
        }
    ).to_balances()
    assert [b.asset for b in balances] == ["ETH", "BTC"]
    assert balances[1].free == Decimal("1.5")
    assert balances[1].locked == Decimal("0.5")
    assert balances[1].total == Decimal("2.0")


def test_a_balance_delta_is_not_offered_as_a_balance() -> None:
    """``balanceUpdate`` is a movement; a Balance states a position."""
    update = BinanceBalanceUpdate.model_validate(
        {"e": "balanceUpdate", "E": 1573200697110, "a": "BTC", "d": "100.00000000"}
    )
    assert update.asset == "BTC"
    assert update.delta == Decimal("100")
    assert not hasattr(update, "to_balance")


# --- call replies ----------------------------------------------------------


def test_order_ack_converts_a_new_order() -> None:
    order = BinanceOrderAck.model_validate(ORDER_ACK).to_order(TICKER)
    assert order.order_id == "12569099453"
    assert order.client_order_id == "my-order-1"
    assert order.side is Side.SELL
    assert order.status is OrderStatus.NEW
    assert order.qty == Decimal("0.00847")
    assert order.filled_qty == Decimal("0")
    assert order.ts == pytest.approx(1660801715.639)


def test_a_cancel_reply_reports_the_orders_client_id_not_the_cancels() -> None:
    order = BinanceOrderAck.model_validate(
        {
            **ORDER_ACK,
            "status": "CANCELED",
            "clientOrderId": "cancel-request-9",
            "origClientOrderId": "my-order-1",
        }
    ).to_order(TICKER)
    assert order.client_order_id == "my-order-1"
    assert order.status is OrderStatus.CANCELED


def test_order_ack_fills_become_fills_with_the_orders_ids() -> None:
    ack = BinanceOrderAck.model_validate(
        {
            **ORDER_ACK,
            "status": "FILLED",
            "executedQty": "0.00847000",
            "cummulativeQuoteQty": "198.33477000",
            "fills": [
                {
                    "price": "23416.10000000",
                    "qty": "0.00847000",
                    "commission": "0.00001",
                    "commissionAsset": "BNB",
                    "tradeId": 1650422481,
                }
            ],
        }
    )
    order = ack.to_order(TICKER)
    assert order.status is OrderStatus.FILLED
    assert order.avg_price == Decimal("198.33477") / Decimal("0.00847")

    fills = ack.to_fills(TICKER)
    assert len(fills) == 1
    assert fills[0].order_id == "12569099453"
    assert fills[0].client_order_id == "my-order-1"
    assert fills[0].qty == Decimal("0.00847")
    assert fills[0].fee_asset == "BNB"


def test_klines_rows_are_read_by_column_not_by_ohlc_order() -> None:
    """The two volumes sit either side of a second timestamp."""
    kline = kline_from_row(
        [
            1499040000000,
            "0.01634790",
            "0.80000000",
            "0.01575800",
            "0.01577100",
            "148976.11427815",
            1499644799999,
            "2434.19055334",
            308,
            "1756.87402397",
            "28.46694368",
            "0",
        ],
        "BTCUSDT",
        "1m",
    )
    assert kline.open_time == pytest.approx(1499040000.0)
    assert kline.open == Decimal("0.01634790")
    assert kline.high == Decimal("0.80000000")
    assert kline.low == Decimal("0.01575800")
    assert kline.close == Decimal("0.01577100")
    assert kline.volume == Decimal("148976.11427815")
    assert kline.quote_volume == Decimal("2434.19055334")


def test_a_short_kline_row_is_refused_rather_than_misread() -> None:
    with pytest.raises(ValueError, match="expected at least 8"):
        kline_from_row([1, "2", "3"], "BTCUSDT", "1m")


# --- enum mapping ----------------------------------------------------------


@pytest.mark.parametrize(
    ("venue", "expected"),
    [
        ("NEW", OrderStatus.NEW),
        ("PARTIALLY_FILLED", OrderStatus.PARTIALLY_FILLED),
        ("FILLED", OrderStatus.FILLED),
        ("CANCELED", OrderStatus.CANCELED),
        ("PENDING_CANCEL", OrderStatus.PENDING_CANCEL),
        ("REJECTED", OrderStatus.REJECTED),
        # Both endings that are not a fill read as a cancel.
        ("EXPIRED", OrderStatus.CANCELED),
        ("EXPIRED_IN_MATCH", OrderStatus.CANCELED),
    ],
)
def test_status_mapping(venue: str, expected: OrderStatus) -> None:
    assert status_of(venue) is expected


def test_an_unknown_status_asks_again_rather_than_guessing() -> None:
    assert status_of("SOMETHING_NEW") is OrderStatus.UNKNOWN


def test_limit_maker_is_a_limit_order_here() -> None:
    """Post-only is a time-in-force in our vocabulary, not a type."""
    assert type_of("LIMIT_MAKER") is OrderType.LIMIT
    assert type_of("MARKET") is OrderType.MARKET


# --- account history -------------------------------------------------------

#: The same execution as the partial fill above, as ``myTrades`` reports it
#: after the fact: trade 987654 of order 4293153, 0.3 @ 0.1, 0.0001 BNB.
MY_TRADE = {
    "symbol": "ETHBTC",
    "id": 987654,
    "orderId": 4293153,
    "orderListId": -1,
    "price": "0.10000000",
    "qty": "0.30000000",
    "quoteQty": "0.03000000",
    "commission": "0.00010000",
    "commissionAsset": "BNB",
    "time": 1499405658657,
    "isBuyer": True,
    "isMaker": False,
    "isBestMatch": True,
}

HISTORICAL_ORDER = {
    "symbol": "ETHBTC",
    "orderId": 4293153,
    "orderListId": -1,
    "clientOrderId": "mUvoqJxFIILMdfAW5iGSOW",
    "price": "0.10264410",
    "origQty": "1.00000000",
    "executedQty": "0.70000000",
    "cummulativeQuoteQty": "0.07100000",
    "status": "CANCELED",
    "timeInForce": "GTC",
    "type": "LIMIT",
    "side": "BUY",
    "stopPrice": "0.00000000",
    "icebergQty": "0.00000000",
    "time": 1499827319559,
    "updateTime": 1499827420000,
    "isWorking": True,
    "workingTime": 1499827319559,
    "origQuoteOrderQty": "0.00000000",
    "selfTradePreventionMode": "NONE",
}


def test_a_streamed_fill_and_a_backfilled_one_are_the_same_record() -> None:
    """The invariant the whole two-tier history design rests on.

    A fill can arrive twice — live on the user data stream, and again when a
    backfill re-reads the window from ``myTrades``. Storage dedupes on
    ``fill_id``, so if these two paths spelled the same execution differently
    every backfilled trade would be booked a second time and every PnL number
    derived from them would double. Binance's ``t`` and ``id`` are the same
    trade id; this pins that they stay the same *fill_id*.
    """
    streamed = BinanceExecutionReport.model_validate(
        {
            **EXECUTION_REPORT,
            "x": "TRADE",
            "X": "PARTIALLY_FILLED",
            "l": "0.30000000",
            "L": "0.10000000",
            "z": "0.70000000",
            "Z": "0.07100000",
            "n": "0.00010000",
            "N": "BNB",
            "t": 987654,
        }
    ).to_fill(TICKER)
    backfilled = BinanceSpotMyTrade.model_validate(MY_TRADE).to_fill(TICKER)

    assert streamed.fill_id == backfilled.fill_id == "987654"
    assert streamed.order_id == backfilled.order_id
    assert streamed.side is backfilled.side
    assert streamed.price == backfilled.price
    assert streamed.qty == backfilled.qty
    assert streamed.fee == backfilled.fee
    assert streamed.fee_asset == backfilled.fee_asset
    assert streamed.ts == backfilled.ts
    assert streamed.universal_ticker == backfilled.universal_ticker


def test_a_trade_row_is_the_one_thing_that_cannot_name_its_own_order_id() -> None:
    """Which is why ``allOrders`` is read alongside ``myTrades``, not instead.

    Left unset rather than filled with the venue's order id: the field means
    *our* id for the order, and putting anything else there would make an
    execution look attributable when it is not.
    """
    fill = BinanceSpotMyTrade.model_validate(MY_TRADE).to_fill(TICKER)
    assert fill.client_order_id is None
    assert fill.order_id == "4293153", "the venue's id is all this row carries"


def test_is_buyer_is_the_accounts_side_not_the_tapes_maker_flag() -> None:
    """``m`` on the tape inverts; ``isBuyer`` here does not. Different fields."""
    bought = BinanceSpotMyTrade.model_validate(MY_TRADE)
    sold = BinanceSpotMyTrade.model_validate(
        {**MY_TRADE, "isBuyer": False, "isMaker": True}
    )
    assert bought.side is Side.BUY
    assert bought.is_maker is False
    assert sold.side is Side.SELL
    assert sold.is_maker is True


def test_historical_order_converts_a_terminal_order() -> None:
    order = BinanceSpotHistoricalOrder.model_validate(HISTORICAL_ORDER).to_order(
        TICKER
    )
    assert order.order_id == "4293153"
    assert order.client_order_id == "mUvoqJxFIILMdfAW5iGSOW"
    assert order.status is OrderStatus.CANCELED
    assert order.side is Side.BUY
    assert order.type is OrderType.LIMIT
    assert order.qty == Decimal("1")
    assert order.filled_qty == Decimal("0.7")
    # Same Z/z division the stream needs — allOrders has no average either.
    assert order.avg_price == Decimal("0.071") / Decimal("0.7")


def test_a_historical_order_is_stamped_when_it_last_moved() -> None:
    """``updateTime``, not ``time``: a history read is about where it ended."""
    order = BinanceSpotHistoricalOrder.model_validate(HISTORICAL_ORDER).to_order(
        TICKER
    )
    assert order.ts == 1499827420000 / 1000.0

    never_moved = BinanceSpotHistoricalOrder.model_validate(
        {**HISTORICAL_ORDER, "updateTime": 0}
    ).to_order(TICKER)
    assert never_moved.ts == 1499827319559 / 1000.0


def test_an_order_this_platform_never_placed_keeps_the_venues_id() -> None:
    """Manual and third-party orders come back on the same endpoint.

    Passed through rather than dropped or normalized: recognising which ids
    are ours is the caller's job, and it cannot do it on an id we rewrote.
    """
    order = BinanceSpotHistoricalOrder.model_validate(
        {**HISTORICAL_ORDER, "clientOrderId": "web_a1b2c3d4"}
    ).to_order(TICKER)
    assert order.client_order_id == "web_a1b2c3d4"


def test_a_historical_order_with_no_client_order_id_reports_none() -> None:
    order = BinanceSpotHistoricalOrder.model_validate(
        {**HISTORICAL_ORDER, "clientOrderId": ""}
    ).to_order(TICKER)
    assert order.client_order_id is None


def test_a_historical_market_order_reports_no_limit_price() -> None:
    order = BinanceSpotHistoricalOrder.model_validate(
        {**HISTORICAL_ORDER, "type": "MARKET", "price": "0.00000000"}
    ).to_order(TICKER)
    assert order.type is OrderType.MARKET
    assert order.price is None


def test_an_unfilled_historical_order_has_no_average_price() -> None:
    order = BinanceSpotHistoricalOrder.model_validate(
        {
            **HISTORICAL_ORDER,
            "status": "NEW",
            "executedQty": "0.00000000",
            "cummulativeQuoteQty": "0.00000000",
        }
    ).to_order(TICKER)
    assert order.avg_price is None
