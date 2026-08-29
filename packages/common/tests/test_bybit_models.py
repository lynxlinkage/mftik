"""Bybit wire models — the readings the converters depend on."""

from __future__ import annotations

from decimal import Decimal

import pytest
from mftik.exchange.bybit.listing import to_listed
from mftik.exchange.bybit.models import (
    BybitExecution,
    BybitKline,
    BybitLiquidation,
    BybitOrderBook,
    BybitOrderUpdate,
    BybitPosition,
    BybitPublicTrade,
    BybitTicker,
    BybitWallet,
    BybitWalletCoin,
    category_of,
    kline_from_row,
    order_book_from_result,
    status_of,
    type_of,
)
from mftik.exchange.models import OrderStatus, OrderType, Side
from mftik.exchange.tickers import Category, UniversalTicker

#: The instrument every payload in this module is stamped with.
TICKER = UniversalTicker.parse("Bybit_Spot_BTCUSDT")
#: Its perp twin — same symbol, different book.
PERP = UniversalTicker.parse("Bybit_Perp_BTCUSDT")

# --- order updates ---------------------------------------------------------


def test_the_empty_string_is_how_bybit_says_not_applicable() -> None:
    """Every number is a string here, and an absent one is ``""``.

    Parsing that as zero would turn "has not traded yet" into "traded at zero",
    which is a price a caller could act on.
    """
    row = BybitOrderUpdate.model_validate(
        {
            "symbol": "BTCUSDT",
            "orderId": "ord-1",
            "orderLinkId": "c-1",
            "side": "Buy",
            "orderType": "Limit",
            "orderStatus": "New",
            "price": "60000",
            "qty": "0.001",
            "avgPrice": "",
            "cumExecQty": "",
        }
    )
    assert row.cum_exec_qty == Decimal("0")
    assert row.avg_price is None


def test_average_price_falls_back_to_the_division_bybit_skipped() -> None:
    """``avgPrice`` is empty on some spot payloads; value/qty is the same
    number, and the division has to be guarded against a zero."""
    row = BybitOrderUpdate.model_validate(
        {
            "symbol": "BTCUSDT",
            "orderId": "ord-1",
            "side": "Sell",
            "orderStatus": "PartiallyFilled",
            "qty": "2",
            "avgPrice": "",
            "cumExecQty": "1",
            "cumExecValue": "60100",
        }
    )
    assert row.avg_price == Decimal("60100")


def test_a_market_order_reports_no_limit_price() -> None:
    order = BybitOrderUpdate.model_validate(
        {
            "symbol": "BTCUSDT",
            "orderId": "ord-1",
            "side": "Buy",
            "orderType": "Market",
            "orderStatus": "Filled",
            "price": "0",
            "qty": "1",
            "cumExecQty": "1",
            "cumExecValue": "60000",
        }
    ).to_order(TICKER)
    assert order.price is None
    assert order.type is OrderType.MARKET
    assert order.avg_price == Decimal("60000")
    assert order.quote_qty is None


def test_a_quote_sized_market_order_does_not_copy_qty_as_base() -> None:
    order = BybitOrderUpdate.model_validate(
        {
            "symbol": "BTCUSDT",
            "orderId": "ord-1",
            "side": "Buy",
            "orderType": "Market",
            "orderStatus": "Filled",
            "price": "0",
            "qty": "100",
            "marketUnit": "quoteCoin",
            "cumExecQty": "0.002",
            "cumExecValue": "100",
        }
    ).to_order(TICKER)
    assert order.qty == Decimal("0.002")
    assert order.filled_qty == Decimal("0.002")
    assert order.quote_qty == Decimal("100")


@pytest.mark.parametrize(
    ("venue_status", "expected"),
    [
        ("New", OrderStatus.NEW),
        ("PartiallyFilled", OrderStatus.PARTIALLY_FILLED),
        ("Filled", OrderStatus.FILLED),
        ("Cancelled", OrderStatus.CANCELED),
        # A partially filled order whose remainder was cancelled is finished,
        # and a conditional order that will never trigger is too.
        ("PartiallyFilledCanceled", OrderStatus.CANCELED),
        ("Deactivated", OrderStatus.CANCELED),
        ("Rejected", OrderStatus.REJECTED),
        # Live orders that are not in the book yet, but can still be cancelled.
        ("Untriggered", OrderStatus.NEW),
        ("Triggered", OrderStatus.NEW),
    ],
)
def test_order_statuses_map_onto_the_shared_lifecycle(
    venue_status: str, expected: OrderStatus
) -> None:
    assert status_of(venue_status) is expected


def test_a_status_we_have_no_name_for_is_unknown_not_a_guess() -> None:
    """UNKNOWN is the state that says "ask again" rather than inventing an
    event the venue did not report."""
    assert status_of("SomethingNew") is OrderStatus.UNKNOWN
    assert status_of("") is OrderStatus.UNKNOWN
    assert type_of("") is OrderType.LIMIT


# --- executions ------------------------------------------------------------


def test_only_a_trade_execution_is_a_fill() -> None:
    """Funding and ADL arrive on the same topic and are not something an order
    did."""
    base = {
        "symbol": "BTCUSDT",
        "orderId": "ord-1",
        "execId": "e-1",
        "side": "Buy",
        "execPrice": "60000",
        "execQty": "0.5",
        "execFee": "0.03",
        "feeCurrency": "USDT",
        "execTime": "1700000000000",
    }
    assert BybitExecution.model_validate({**base, "execType": "Trade"}).is_fill
    assert not BybitExecution.model_validate({**base, "execType": "Funding"}).is_fill
    # A "Trade" with nothing executed is not one either.
    assert not BybitExecution.model_validate(
        {**base, "execType": "Trade", "execQty": "0"}
    ).is_fill


def test_a_fill_carries_this_execution_alone() -> None:
    fill = BybitExecution.model_validate(
        {
            "symbol": "BTCUSDT",
            "orderId": "ord-1",
            "orderLinkId": "c-1",
            "execId": "e-1",
            "side": "Sell",
            "execPrice": "60000",
            "execQty": "0.5",
            "execFee": "0.03",
            "feeCurrency": "USDT",
            "execType": "Trade",
            "execTime": "1700000000000",
            "leavesQty": "0.5",
        }
    ).to_fill(TICKER)
    assert fill.fill_id == "e-1"
    assert fill.client_order_id == "c-1"
    assert fill.side is Side.SELL
    assert fill.qty == Decimal("0.5")
    assert fill.fee_asset == "USDT"
    assert fill.ts == 1700000000.0


# --- wallet ----------------------------------------------------------------


def test_the_spendable_balance_comes_from_whichever_field_the_account_has() -> None:
    """A classic spot account has ``free``; a unified one has
    ``availableToWithdraw`` — and leaves it empty for collateral."""
    classic = BybitWalletCoin.model_validate(
        {"coin": "USDT", "walletBalance": "100", "free": "80", "locked": "20"}
    ).to_balance()
    assert classic.free == Decimal("80")

    unified = BybitWalletCoin.model_validate(
        {
            "coin": "USDT",
            "walletBalance": "100",
            "availableToWithdraw": "70",
            "locked": "30",
        }
    ).to_balance()
    assert unified.free == Decimal("70")
    assert unified.locked == Decimal("30")


def test_a_collateralised_coin_falls_back_to_wallet_minus_held() -> None:
    coin = BybitWalletCoin.model_validate(
        {
            "coin": "USDT",
            "walletBalance": "100",
            "availableToWithdraw": "",
            "locked": "0",
            "totalOrderIM": "40",
        }
    ).to_balance()
    assert coin.free == Decimal("60")
    assert coin.locked == Decimal("40")


def test_a_negative_spendable_balance_is_clamped_to_zero() -> None:
    """Borrowing can put the subtraction below zero, which is not money."""
    coin = BybitWalletCoin.model_validate(
        {"coin": "USDT", "walletBalance": "10", "totalOrderIM": "40"}
    ).to_balance()
    assert coin.free == Decimal("0")


def test_a_wallet_push_is_one_balance_per_coin() -> None:
    balances = BybitWallet.model_validate(
        {
            "accountType": "UNIFIED",
            "coin": [
                {"coin": "USDT", "walletBalance": "100", "availableToWithdraw": "100"},
                {"coin": "BTC", "walletBalance": "1", "availableToWithdraw": "1"},
                # A row with no coin names nothing and is dropped.
                {"walletBalance": "5"},
            ],
        }
    ).to_balances()
    assert [b.asset for b in balances] == ["USDT", "BTC"]


# --- positions -------------------------------------------------------------


def test_a_short_position_is_a_negative_quantity() -> None:
    """Bybit reports size unsigned and puts the direction in ``side``; one
    signed number cannot disagree with itself."""
    short = BybitPosition.model_validate(
        {
            "symbol": "BTCUSDT",
            "side": "Sell",
            "size": "2",
            "category": "linear",
            "entryPrice": "60000",
            "unrealisedPnl": "-15.5",
        }
    )
    position = short.to_position(PERP)
    assert position.qty == Decimal("-2")
    assert position.universal_ticker == str(PERP)
    # The venue's own figures, kept: a size on its own cannot answer the first
    # question anyone asks of a position.
    assert position.entry_price == Decimal("60000")
    assert position.unrealised_pnl == Decimal("-15.5")

    long = BybitPosition.model_validate(
        {"symbol": "BTCUSDT", "side": "Buy", "size": "2"}
    )
    assert long.to_position(PERP).qty == Decimal("2")
    # Bybit empties ``side`` once the position closes.
    flat = BybitPosition.model_validate({"symbol": "BTCUSDT", "side": "", "size": "0"})
    assert flat.to_position(PERP).flat


@pytest.mark.parametrize(
    ("venue_category", "expected"),
    [
        ("spot", Category.SPOT),
        ("linear", Category.PERP),
        ("inverse", Category.PERP),
        ("option", Category.OPTION),
    ],
)
def test_a_row_names_the_book_it_came_from(
    venue_category: str, expected: Category
) -> None:
    """What lets one unified session resolve rows from every book: the account
    payloads say which one they are, so nothing has to be guessed."""
    assert category_of(venue_category, Category.SPOT) is expected


def test_a_row_that_names_no_book_falls_back_to_the_connectors() -> None:
    assert category_of("", Category.PERP) is Category.PERP
    assert category_of(None, Category.SPOT) is Category.SPOT


# --- public data -----------------------------------------------------------


def test_the_tape_reports_the_aggressor_with_nothing_to_invert() -> None:
    """Unlike Binance's maker flag, ``S`` is already the taker's side."""
    trade = BybitPublicTrade.model_validate(
        {
            "T": 1700000000000,
            "s": "BTCUSDT",
            "S": "Sell",
            "v": "0.001",
            "p": "60000",
            "i": "trade-1",
            "L": "MinusTick",
        }
    ).to_trade(TICKER)
    assert trade.side is Side.SELL
    assert trade.trade_id == "trade-1"
    assert trade.ts == 1700000000.0


def test_a_liquidation_reports_the_position_that_was_closed() -> None:
    """``Buy`` means a long was liquidated — the opposite of the tape's ``S``."""
    row = BybitLiquidation.model_validate(
        {
            "T": 1700000000000,
            "s": "BTCUSDT",
            "S": "Buy",
            "v": "1.5",
            "p": "59100",
        }
    ).to_liquidation(TICKER)
    assert row.side is Side.BUY
    assert row.qty == Decimal("1.5")
    assert row.price == Decimal("59100")
    assert row.ts == 1700000000.0


def test_a_spot_ticker_has_no_quote_and_falls_back_to_last() -> None:
    """Spot's push carries no bid or ask at all; a zero would read as a price
    a caller could cross."""
    ticker = BybitTicker.model_validate(
        {"symbol": "BTCUSDT", "lastPrice": "60000", "volume24h": "10"}
    )
    assert ticker.quoted
    assert ticker.to_ticker(TICKER).bid == Decimal("60000")
    assert ticker.to_ticker(TICKER).ask == Decimal("60000")


def test_a_ticker_delta_with_no_price_is_not_a_ticker() -> None:
    """The contract books push only what changed, so a funding-rate update
    carries no quote — and publishing one would mean inventing prices."""
    delta = BybitTicker.model_validate(
        {"symbol": "BTCUSDT", "fundingRate": "0.0001"}
    )
    assert not delta.quoted


def test_a_quoted_ticker_keeps_both_sides() -> None:
    ticker = BybitTicker.model_validate(
        {
            "symbol": "BTCUSDT",
            "lastPrice": "60000",
            "bid1Price": "59999",
            "ask1Price": "60001",
        }
    ).to_ticker(TICKER, ts=1700000000.0)
    assert (ticker.bid, ticker.ask, ticker.ts) == (
        Decimal("59999"),
        Decimal("60001"),
        1700000000.0,
    )


def test_a_book_payload_reads_levels_and_the_update_id() -> None:
    book = BybitOrderBook.model_validate(
        {
            "s": "BTCUSDT",
            "b": [["59999", "1"], ["59998", "2"]],
            "a": [["60001", "3"]],
            "u": 42,
            "seq": 7,
        }
    )
    assert book.u == 42
    assert [level.price for level in book.bid_levels()] == [
        Decimal("59999"),
        Decimal("59998"),
    ]
    assert book.to_order_book(TICKER, ts=1.0).asks[0].qty == Decimal("3")


def test_a_one_sided_top_of_book_is_not_a_quote() -> None:
    """An empty side means there is nothing to cross, not a zero price."""
    empty = BybitOrderBook.model_validate({"s": "BTCUSDT", "b": [], "a": [["1", "1"]]})
    assert empty.to_best_quote(TICKER) is None
    both = BybitOrderBook.model_validate(
        {"s": "BTCUSDT", "b": [["1", "2"]], "a": [["3", "4"]]}
    )
    quote = both.to_best_quote(TICKER, ts=5.0)
    assert quote is not None
    assert (quote.bid, quote.bid_qty, quote.ask, quote.ask_qty) == (
        Decimal("1"),
        Decimal("2"),
        Decimal("3"),
        Decimal("4"),
    )


def test_a_candle_is_final_only_once_bybit_confirms_it() -> None:
    """Only a closed candle is safe to append to a series."""
    row = {
        "start": 1700000000000,
        "end": 1700000059999,
        "interval": "1",
        "open": "1",
        "close": "2",
        "high": "3",
        "low": "0.5",
        "volume": "10",
        "turnover": "20",
        "confirm": False,
        "timestamp": 1700000030000,
    }
    live = BybitKline.model_validate(row).to_kline("BTCUSDT")
    assert not live.closed
    assert live.open_time == 1700000000.0
    assert live.quote_volume == Decimal("20")
    assert BybitKline.model_validate({**row, "confirm": True}).to_kline("X").closed


# --- REST rows -------------------------------------------------------------


def test_a_kline_row_is_positional_and_ohlc_then_volumes() -> None:
    kline = kline_from_row(
        ["1700000000000", "1", "3", "0.5", "2", "10", "20"], "BTCUSDT", "1"
    )
    assert (kline.open, kline.high, kline.low, kline.close) == (
        Decimal("1"),
        Decimal("3"),
        Decimal("0.5"),
        Decimal("2"),
    )
    assert kline.volume == Decimal("10")
    assert kline.quote_volume == Decimal("20")


def test_a_short_kline_row_is_an_error_not_a_partial_candle() -> None:
    with pytest.raises(ValueError, match="expected at least 7"):
        kline_from_row(["1700000000000", "1", "3"], "BTCUSDT", "1")


def test_spot_and_contract_instruments_read_the_same_way() -> None:
    """The two books spell the quantity step and the notional floor
    differently, and callers should not have to know which."""
    spot = to_listed(
        {
            "symbol": "BTCUSDT",
            "baseCoin": "BTC",
            "quoteCoin": "USDT",
            "status": "Trading",
            "lotSizeFilter": {
                "basePrecision": "0.000001",
                "minOrderQty": "0.000048",
                "minOrderAmt": "1",
            },
            "priceFilter": {"tickSize": "0.01"},
        }
    )
    assert spot is not None
    assert spot.filters["qty_step"] == Decimal("0.000001")
    assert spot.filters["min_notional"] == Decimal("1")

    perp = to_listed(
        {
            "symbol": "BTCUSDT",
            "baseCoin": "BTC",
            "quoteCoin": "USDT",
            "contractType": "LinearPerpetual",
            "lotSizeFilter": {
                "qtyStep": "0.001",
                "minOrderQty": "0.001",
                "minNotionalValue": "5",
            },
            "priceFilter": {"tickSize": "0.1"},
        },
        category=Category.PERP,
    )
    assert perp is not None
    assert perp.filters["qty_step"] == Decimal("0.001")
    assert perp.filters["price_tick"] == Decimal("0.1")
    assert perp.filters["min_notional"] == Decimal("5")


def test_a_zero_step_is_dropped_rather_than_stored() -> None:
    """A zero here would divide."""
    row = to_listed(
        {
            "symbol": "X",
            "baseCoin": "X",
            "quoteCoin": "USDT",
            "lotSizeFilter": {"qtyStep": "0", "minOrderQty": ""},
            "priceFilter": {"tickSize": ""},
        }
    )
    assert row is not None
    assert row.filters["min_qty"] is None
    assert row.filters["price_tick"] is None
    assert row.filters["qty_step"] is None


def test_a_null_settle_coin_does_not_abort_the_listing() -> None:
    inst = to_listed(
        {
            "symbol": "BTCUSDT",
            "baseCoin": "BTC",
            "quoteCoin": "USDT",
            "settleCoin": None,
            "status": "Trading",
            "lotSizeFilter": None,
            "priceFilter": None,
        }
    )
    assert inst is not None
    assert inst.settlement_asset is None


def test_a_rest_book_is_dated_by_the_venue() -> None:
    book = order_book_from_result(
        {"s": "BTCUSDT", "b": [["1", "2"]], "a": [["3", "4"]], "ts": 1700000000000},
        TICKER,
    )
    assert book.universal_ticker == str(TICKER)
    assert book.ts == 1700000000.0
