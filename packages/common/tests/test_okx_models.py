"""OKX wire models — the readings the converters depend on."""

from __future__ import annotations

from decimal import Decimal

import pytest
from mftik.exchange.models import OrderStatus, OrderType, Side
from mftik.exchange.okx.feed import OkxBook
from mftik.exchange.okx.listing import to_listed
from mftik.exchange.okx.models import (
    OkxAccount,
    OkxFill,
    OkxLiquidation,
    OkxOrderBook,
    OkxOrderUpdate,
    OkxPosition,
    OkxPublicTrade,
    OkxTicker,
    category_of,
    kline_from_row,
    status_of,
)
from mftik.exchange.tickers import Category, UniversalTicker

TICKER = UniversalTicker.parse("Okx_Spot_BTCUSDT")
PERP = UniversalTicker.parse("Okx_Perp_BTCUSDT")


def test_the_empty_string_is_how_okx_says_not_applicable() -> None:
    row = OkxOrderUpdate.model_validate(
        {
            "instId": "BTC-USDT",
            "ordId": "ord-1",
            "clOrdId": "c-1",
            "side": "buy",
            "ordType": "limit",
            "state": "live",
            "px": "60000",
            "sz": "0.001",
            "avgPx": "",
            "accFillSz": "",
        }
    )
    assert row.acc_fill_sz == Decimal("0")
    assert row.avg_price is None


def test_a_market_order_reports_no_limit_price() -> None:
    order = OkxOrderUpdate.model_validate(
        {
            "instId": "BTC-USDT",
            "ordId": "ord-1",
            "side": "buy",
            "ordType": "market",
            "state": "filled",
            "px": "0",
            "sz": "1",
            "accFillSz": "1",
            "avgPx": "60000",
        }
    ).to_order(TICKER)
    assert order.price is None
    assert order.type is OrderType.MARKET
    assert order.avg_price == Decimal("60000")


def test_a_quote_sized_market_order_does_not_copy_sz_as_base() -> None:
    order = OkxOrderUpdate.model_validate(
        {
            "instId": "BTC-USDT",
            "ordId": "ord-1",
            "side": "buy",
            "ordType": "market",
            "state": "filled",
            "px": "0",
            "sz": "100",
            "tgtCcy": "quote_ccy",
            "accFillSz": "0.002",
        }
    ).to_order(TICKER)
    assert order.qty == Decimal("0.002")
    assert order.filled_qty == Decimal("0.002")
    assert order.quote_qty == Decimal("100")


@pytest.mark.parametrize(
    ("venue_status", "expected"),
    [
        ("live", OrderStatus.NEW),
        ("partially_filled", OrderStatus.PARTIALLY_FILLED),
        ("filled", OrderStatus.FILLED),
        ("canceled", OrderStatus.CANCELED),
        ("mmp_canceled", OrderStatus.CANCELED),
        ("order_failed", OrderStatus.REJECTED),
    ],
)
def test_order_statuses_map_onto_the_shared_lifecycle(
    venue_status: str, expected: OrderStatus
) -> None:
    assert status_of(venue_status) is expected


def test_a_status_we_have_no_name_for_is_unknown_not_a_guess() -> None:
    assert status_of("something_new") is OrderStatus.UNKNOWN
    assert status_of("") is OrderStatus.UNKNOWN


def _canceled(**extra: str) -> OkxOrderUpdate:
    """A post-only BUY that OKX killed rather than rested."""
    return OkxOrderUpdate.model_validate(
        {
            "instId": "BTC-USDT",
            "ordId": "ord-1",
            "clOrdId": "c-1",
            "side": "buy",
            "ordType": "post_only",
            "state": "canceled",
            "px": "77518.1",
            "sz": "0.00001",
            "accFillSz": "0",
            **extra,
        }
    )


def test_a_post_only_okx_killed_for_crossing_is_a_reject_not_a_cancel() -> None:
    """The whole complaint in #37: OKX refuses a crossed post-only by
    cancelling it, so a strategy saw the same hook and the same terminal state
    it sees when it cancels an order itself, with nothing to tell them apart."""
    order = _canceled(cancelSource="31").to_order(TICKER)

    assert order.status is OrderStatus.REJECTED
    assert order.reject_reason.startswith("cancelSource=31 ")
    assert "post-only order will take liquidity" in order.reject_reason


def test_a_cancel_the_strategy_asked_for_stays_a_cancel() -> None:
    """``1`` is the user's own cancel — the case a refusal must not be
    confused with, and the reason this reads the source rather than treating
    every cancellation as a rejection."""
    order = _canceled(cancelSource="1").to_order(TICKER)

    assert order.status is OrderStatus.CANCELED
    assert order.reject_reason == ""


@pytest.mark.parametrize("source", ["", "13", "14", "20", "32", "99"])
def test_every_other_cancellation_source_is_left_alone(source: str) -> None:
    """FOK and IOC expiring, cancel-all-after, self-trade prevention, one OKX
    has not documented yet. All end an order the venue accepted, and calling
    any of them a refusal would invent a rejection that never happened."""
    order = _canceled(cancelSource=source).to_order(TICKER)

    assert order.status is OrderStatus.CANCELED
    assert order.reject_reason == ""


def test_a_cancellation_that_traded_is_never_read_as_a_refusal() -> None:
    """A reject supersedes the order update rather than riding with it, so
    reading a partly filled order as refused would lose the fill."""
    order = _canceled(cancelSource="31", accFillSz="0.000004").to_order(TICKER)

    assert order.status is OrderStatus.CANCELED
    assert order.reject_reason == ""
    assert order.filled_qty == Decimal("0.000004")


def test_an_order_that_was_not_refused_carries_no_reason() -> None:
    order = _canceled(state="filled", accFillSz="0.00001").to_order(TICKER)

    assert order.status is OrderStatus.FILLED
    assert order.reject_reason == ""


def test_a_fill_carries_this_execution_alone_and_a_positive_fee() -> None:
    """OKX reports a paid fee as a negative number; the shared model does not."""
    fill = OkxFill.model_validate(
        {
            "instId": "BTC-USDT",
            "ordId": "ord-1",
            "clOrdId": "c-1",
            "tradeId": "t-1",
            "side": "sell",
            "fillPx": "60000",
            "fillSz": "0.5",
            "fillFee": "-0.03",
            "fillFeeCcy": "USDT",
            "ts": "1700000000000",
        }
    ).to_fill(TICKER)
    assert fill.fill_id == "t-1"
    assert fill.client_order_id == "c-1"
    assert fill.side is Side.SELL
    assert fill.qty == Decimal("0.5")
    assert fill.fee == Decimal("0.03")
    assert fill.fee_asset == "USDT"
    assert fill.ts == 1700000000.0


def test_a_fill_keeps_the_bill_id_pagination_needs() -> None:
    row = OkxFill.model_validate(
        {"tradeId": "t-1", "billId": "b-9", "fillSz": "1"}
    )
    assert row.bill_id == "b-9"


def test_a_zero_fill_is_not_a_fill() -> None:
    assert not OkxFill.model_validate(
        {"fillSz": "0", "fillPx": "1", "tradeId": "t"}
    ).is_fill


def test_the_spendable_balance_prefers_avail_eq_on_a_unified_account() -> None:
    wallet = OkxAccount.model_validate(
        {
            "details": [
                {
                    "ccy": "USDT",
                    "eq": "100",
                    "cashBal": "100",
                    "availEq": "70",
                    "availBal": "80",
                    "frozenBal": "20",
                }
            ]
        }
    )
    [balance] = wallet.to_balances()
    assert balance.asset == "USDT"
    assert balance.free == Decimal("70")
    assert balance.locked == Decimal("20")


def test_a_net_mode_position_is_already_signed() -> None:
    short = OkxPosition.model_validate(
        {
            "instType": "SWAP",
            "instId": "BTC-USDT-SWAP",
            "pos": "-1.5",
            "posSide": "net",
            "avgPx": "60000",
            "upl": "-10",
        }
    )
    assert short.signed_size == Decimal("-1.5")
    pos = short.to_position(PERP)
    assert pos.qty == Decimal("-1.5")
    assert pos.entry_price == Decimal("60000")


def test_a_hedge_mode_short_flips_the_unsigned_size() -> None:
    row = OkxPosition.model_validate(
        {"pos": "2", "posSide": "short", "instId": "BTC-USDT-SWAP"}
    )
    assert row.signed_size == Decimal("-2")


def test_category_of_reads_the_row_not_the_connector() -> None:
    assert category_of("SWAP", Category.SPOT) is Category.PERP
    assert category_of("SPOT", Category.PERP) is Category.SPOT
    assert category_of("MARGIN", Category.PERP) is Category.SPOT
    assert category_of("", Category.SPOT) is Category.SPOT


def test_a_swap_instrument_takes_base_and_quote_from_the_contract_fields() -> None:
    inst = to_listed(
        {
            "instId": "BTC-USDT-SWAP",
            "baseCcy": "",
            "quoteCcy": "",
            "ctValCcy": "BTC",
            "settleCcy": "USDT",
            "ctType": "linear",
            "ctVal": "0.01",
            "tickSz": "0.1",
            "lotSz": "1",
            "minSz": "1",
        },
        category=Category.PERP,
    )
    assert inst is not None
    assert inst.exch_ticker == "BTC-USDT-SWAP"
    assert inst.symbol == "BTCUSDT"
    assert inst.base == "BTC"
    assert inst.quote == "USDT"
    assert inst.filters["price_tick"] == Decimal("0.1")
    assert inst.filters["min_qty"] == Decimal("0.01")
    assert inst.contract_size == Decimal("0.01")


def test_a_null_exp_time_does_not_abort_the_listing() -> None:
    """OKX publishes JSON null for fields that do not apply."""
    inst = to_listed(
        {
            "instId": "BTC-USDT-SWAP",
            "baseCcy": None,
            "quoteCcy": None,
            "ctValCcy": "BTC",
            "settleCcy": "USDT",
            "ctType": "linear",
            "ctVal": "0.01",
            "expTime": None,
            "state": "live",
        },
        category=Category.PERP,
    )
    assert inst is not None
    assert inst.exch_ticker == "BTC-USDT-SWAP"


def test_a_kline_row_is_ohlc_then_volumes() -> None:
    candle = kline_from_row(
        ["1700000000000", "1", "3", "0.5", "2", "10", "20", "20", "1"],
        TICKER,
        "1m",
    )
    assert candle.open == Decimal("1")
    assert candle.high == Decimal("3")
    assert candle.low == Decimal("0.5")
    assert candle.close == Decimal("2")
    assert candle.volume == Decimal("10")
    assert candle.closed is True
    assert candle.open_time == 1700000000.0


def test_an_in_progress_candle_is_not_closed() -> None:
    candle = kline_from_row(
        ["1700000000000", "1", "1", "1", "1", "1", "1", "1", "0"],
        TICKER,
        "1m",
    )
    assert candle.closed is False


def test_a_public_trade_side_is_the_aggressor() -> None:
    trade = OkxPublicTrade.model_validate(
        {
            "instId": "BTC-USDT",
            "tradeId": "t-1",
            "px": "60000",
            "sz": "0.001",
            "side": "buy",
            "ts": "1700000000000",
        }
    ).to_trade(TICKER)
    assert trade.side is Side.BUY
    assert trade.price == Decimal("60000")


def test_a_ticker_without_a_price_is_not_quoted() -> None:
    assert not OkxTicker.model_validate({"instId": "BTC-USDT"}).quoted
    assert OkxTicker.model_validate({"last": "1"}).quoted


def test_liquidation_details_become_one_event_each() -> None:
    events = OkxLiquidation.model_validate(
        {
            "instId": "BTC-USDT-SWAP",
            "instType": "SWAP",
            "details": [
                {"bkPx": "59900", "sz": "1", "side": "sell", "ts": "1700000000000"},
                {"bkPx": "1", "sz": "0", "side": "buy"},
            ],
        }
    ).to_liquidations(PERP)
    assert len(events) == 1
    assert events[0].qty == Decimal("1")
    assert events[0].price == Decimal("59900")


def test_swap_sizes_convert_through_contract_size() -> None:
    """SWAP ``sz`` is contracts. The shared model is base."""
    ct = Decimal("0.01")
    order = OkxOrderUpdate.model_validate(
        {
            "instId": "BTC-USDT-SWAP",
            "ordId": "ord-1",
            "side": "buy",
            "ordType": "limit",
            "state": "live",
            "px": "60000",
            "sz": "2",
            "accFillSz": "1",
        }
    ).to_order(PERP, contract_size=ct)
    assert order.qty == Decimal("0.02")
    assert order.filled_qty == Decimal("0.01")

    fill = OkxFill.model_validate(
        {"fillSz": "3", "fillPx": "1", "fillFee": "0", "tradeId": "t"}
    ).to_fill(PERP, contract_size=ct)
    assert fill.qty == Decimal("0.03")

    pos = OkxPosition.model_validate(
        {"pos": "-4", "posSide": "net"}
    ).to_position(PERP, contract_size=ct)
    assert pos.qty == Decimal("-0.04")

    candle = kline_from_row(
        ["1700000000000", "1", "1", "1", "1", "10", "0.1", "600", "1"],
        PERP,
        "1m",
        contract_size=ct,
    )
    assert candle.volume == Decimal("0.1")
    assert candle.quote_volume == Decimal("600")


def test_a_book_update_sets_levels_and_a_zero_deletes_one() -> None:
    book = OkxBook("BTC-USDT")
    book.apply(
        OkxOrderBook.model_validate(
            {
                "instId": "BTC-USDT",
                "bids": [["59999", "1"], ["59998", "2"]],
                "asks": [["60001", "3"]],
                "seqId": 1,
                "prevSeqId": -1,
            }
        ),
        "snapshot",
    )
    book.apply(
        OkxOrderBook.model_validate(
            {
                "bids": [["59998", "0"], ["59997", "5"]],
                "asks": [],
                "seqId": 2,
                "prevSeqId": 1,
            }
        ),
        "update",
    )
    folded = book.snapshot()
    assert [(level.price, level.qty) for level in folded.bids] == [
        (Decimal("59999"), Decimal("1")),
        (Decimal("59997"), Decimal("5")),
    ]
    assert folded.asks[0].price == Decimal("60001")


def test_a_gap_empties_the_book_rather_than_drifting() -> None:
    book = OkxBook("BTC-USDT")
    book.apply(
        OkxOrderBook.model_validate(
            {"bids": [["1", "1"]], "asks": [], "seqId": 1, "prevSeqId": -1}
        ),
        "snapshot",
    )
    assert (
        book.apply(
            OkxOrderBook.model_validate(
                {"bids": [["2", "1"]], "asks": [], "seqId": 9, "prevSeqId": 8}
            ),
            "update",
        )
        is False
    )
    assert book.stale
    assert book.snapshot().bids == []
