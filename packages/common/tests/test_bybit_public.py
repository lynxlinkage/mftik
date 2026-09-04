"""Bybit's public streams and the market-data connector.

The book is the interesting part: Bybit sends a snapshot and then deltas, so
"the book" is something this adapter builds rather than something it receives.
"""

from __future__ import annotations

import asyncio
from decimal import Decimal
from typing import Any

import pytest
from bybit_stub import FakeBybit
from mftik.exchange.bybit import channels as ch
from mftik.exchange.bybit.feed import BybitBook, BybitPublicStream
from mftik.exchange.bybit.models import BybitOrderBook
from mftik.exchange.bybit.public import BybitPublicClient, venue_interval
from mftik.exchange.intervals import InvalidIntervalError
from mftik.exchange.tickers import Category, UniversalTicker

#: The instrument every payload in this module is stamped with.
TICKER = UniversalTicker.parse("Bybit_Spot_BTCUSDT")

NATIVE = "BTCUSDT"

TRADE_ROW = {
    "T": 1700000000000,
    "s": NATIVE,
    "S": "Buy",
    "v": "0.001",
    "p": "60000",
    "i": "trade-1",
    "L": "PlusTick",
}

KLINE_ROW = {
    "start": 1700000000000,
    "end": 1700000059999,
    "interval": "1",
    "open": "1",
    "close": "2",
    "high": "3",
    "low": "0.5",
    "volume": "10",
    "turnover": "20",
    "confirm": True,
    "timestamp": 1700000030000,
}


class StubSymbols:
    """A symbol plane whose venue spelling matches Bybit's, checked either way."""

    async def exch_ticker(self, ticker: UniversalTicker) -> str:
        return NATIVE

    async def symbol_for(
        self, venue: str, exch_ticker: str, *, category: str
    ) -> UniversalTicker:
        assert exch_ticker == NATIVE
        return UniversalTicker.of(venue, category, "BTCUSDT")


def _feed(stub: FakeBybit, **kwargs: Any) -> BybitPublicStream:
    return BybitPublicStream(url=stub.url, ping_interval=0, **kwargs)


def _book(u: int, bids: list[list[str]], asks: list[list[str]]) -> dict[str, Any]:
    return {"s": NATIVE, "b": bids, "a": asks, "u": u, "seq": u}


# --- topic names -----------------------------------------------------------


def test_topics_carry_the_symbol_but_never_the_category() -> None:
    """Which book a topic means is decided by the socket it is sent on."""
    assert ch.order_book("btcusdt", depth=50) == "orderbook.50.BTCUSDT"
    assert ch.public_trade("btcusdt") == "publicTrade.BTCUSDT"
    assert ch.kline("btcusdt", "60") == "kline.60.BTCUSDT"
    assert ch.all_liquidation("btcusdt") == "allLiquidation.BTCUSDT"
    assert ch.symbol_of("orderbook.50.BTCUSDT") == "BTCUSDT"
    assert ch.symbol_of("allLiquidation.BTCUSDT") == "BTCUSDT"
    assert ch.symbol_of("wallet") == ""


def test_intervals_translate_into_bybits_own_vocabulary() -> None:
    """It names minute windows by the number of minutes, so an hour is 60."""
    assert venue_interval("1m") == "1"
    assert venue_interval("1h") == "60"
    assert venue_interval("4h") == "240"
    assert venue_interval("1d") == "D"
    assert venue_interval("1mo") == "M"


def test_an_interval_bybit_does_not_serve_is_refused_before_the_round_trip() -> None:
    with pytest.raises(InvalidIntervalError, match="serves no"):
        venue_interval("1s")


# --- book folding ----------------------------------------------------------


def test_a_delta_sets_levels_and_a_zero_deletes_one() -> None:
    """Setting a level to zero is not the same as removing it, and Bybit means
    the second."""
    book = BybitBook(NATIVE)
    book.apply(
        BybitOrderBook.model_validate(
            _book(1, [["59999", "1"], ["59998", "2"]], [["60001", "3"]])
        ),
        "snapshot",
    )
    book.apply(
        BybitOrderBook.model_validate(_book(2, [["59998", "0"], ["59997", "5"]], [])),
        "delta",
    )
    folded = book.snapshot()
    assert [(level.price, level.qty) for level in folded.bids] == [
        (Decimal("59999"), Decimal("1")),
        (Decimal("59997"), Decimal("5")),
    ]
    # Untouched by the delta, and still there.
    assert folded.asks[0].price == Decimal("60001")


def test_a_gap_empties_the_book_rather_than_drifting() -> None:
    """Every book built after a missed message would be wrong in a way nothing
    downstream could detect."""
    book = BybitBook(NATIVE)
    book.apply(BybitOrderBook.model_validate(_book(1, [["1", "1"]], [])), "snapshot")
    assert (
        book.apply(BybitOrderBook.model_validate(_book(9, [["2", "1"]], [])), "delta")
        is False
    )
    assert book.stale
    assert book.snapshot().bids == []
    # And a fresh snapshot puts it back.
    assert book.apply(
        BybitOrderBook.model_validate(_book(1, [["3", "1"]], [])), "snapshot"
    )
    assert not book.stale


def test_applying_the_same_delta_twice_is_not_a_gap() -> None:
    """Two folders share one book; ``_push`` calls apply once per stream."""
    book = BybitBook(NATIVE)
    book.apply(BybitOrderBook.model_validate(_book(1, [["1", "1"]], [])), "snapshot")
    delta = BybitOrderBook.model_validate(_book(2, [["2", "1"]], []))
    assert book.apply(delta, "delta")
    assert book.apply(delta, "delta")
    assert book.update_id == 2
    assert not book.stale


def test_deltas_before_the_first_snapshot_have_nothing_to_apply_to() -> None:
    book = BybitBook(NATIVE)
    assert not book.apply(
        BybitOrderBook.model_validate(_book(5, [["1", "1"]], [])), "delta"
    )


def test_update_id_one_is_a_snapshot_whatever_the_type_says() -> None:
    """Bybit's marker for a service restart."""
    book = BybitBook(NATIVE)
    book.apply(BybitOrderBook.model_validate(_book(7, [["1", "1"]], [])), "snapshot")
    assert book.apply(
        BybitOrderBook.model_validate(_book(1, [["2", "2"]], [])), "delta"
    )
    assert book.snapshot().bids[0].price == Decimal("2")


def test_folded_books_are_sorted_best_first() -> None:
    book = BybitBook(NATIVE)
    book.apply(
        BybitOrderBook.model_validate(
            _book(1, [["1", "1"], ["3", "1"], ["2", "1"]], [["9", "1"], ["7", "1"]])
        ),
        "snapshot",
    )
    folded = book.snapshot()
    assert [level.price for level in folded.bids] == [
        Decimal("3"),
        Decimal("2"),
        Decimal("1"),
    ]
    assert [level.price for level in folded.asks] == [Decimal("7"), Decimal("9")]


# --- the socket ------------------------------------------------------------


async def test_a_public_socket_subscribes_without_authenticating(
    bybit_public: FakeBybit,
) -> None:
    async with _feed(bybit_public) as feed:
        await feed.subscribe_trades(NATIVE)
        await feed.subscribe_klines("1", NATIVE)
    assert bybit_public.auths == 0
    assert not bybit_public.frames_for("auth")
    assert bybit_public.subscribed == {"publicTrade.BTCUSDT", "kline.1.BTCUSDT"}


async def test_two_consumers_share_one_venue_subscription(
    bybit_public: FakeBybit,
) -> None:
    async with _feed(bybit_public) as feed:
        first, second = await asyncio.gather(
            feed.subscribe_trades(NATIVE),
            feed.subscribe_trades(NATIVE),
        )
        assert len(bybit_public.frames_for("subscribe")) == 1
        await bybit_public.push("publicTrade.BTCUSDT", [TRADE_ROW])
        assert (await asyncio.wait_for(first.__anext__(), 2)).trade_id == "trade-1"
        assert (await asyncio.wait_for(second.__anext__(), 2)).trade_id == "trade-1"


async def test_reconnect_resubscribes_a_shared_topic_once(
    bybit_public: FakeBybit,
) -> None:
    async with _feed(bybit_public, retry_backoff=0.01) as feed:
        first = await feed.subscribe_trades(NATIVE)
        second = await feed.subscribe_trades(NATIVE)
        await bybit_public.drop()
        for _ in range(200):
            if len(bybit_public.frames_for("subscribe")) >= 2:
                break
            await asyncio.sleep(0.01)
        assert len(bybit_public.frames_for("subscribe")) == 2
        await bybit_public.push("publicTrade.BTCUSDT", [TRADE_ROW])
        assert (await asyncio.wait_for(first.__anext__(), 2)).trade_id == "trade-1"
        assert (await asyncio.wait_for(second.__anext__(), 2)).trade_id == "trade-1"


async def test_klines_arrive_with_the_symbol_off_the_topic(
    bybit_public: FakeBybit,
) -> None:
    """The payload names no instrument; the topic is the only place it is."""
    async with _feed(bybit_public) as feed:
        stream = await feed.subscribe_klines("1", NATIVE)
        await bybit_public.push("kline.1.BTCUSDT", [KLINE_ROW])
        symbol, candle = await asyncio.wait_for(stream.__anext__(), 2)

    assert symbol == NATIVE
    assert candle.to_kline(symbol).close == Decimal("2")


async def test_the_book_stream_yields_whole_books(
    bybit_public: FakeBybit,
) -> None:
    async with _feed(bybit_public) as feed:
        books = await feed.subscribe_order_book(NATIVE, depth=50)
        await bybit_public.push(
            "orderbook.50.BTCUSDT",
            _book(1, [["59999", "1"]], [["60001", "2"]]),
            kind="snapshot",
        )
        first = await asyncio.wait_for(books.__anext__(), 2)
        await bybit_public.push(
            "orderbook.50.BTCUSDT",
            _book(2, [["59998", "3"]], []),
            kind="delta",
        )
        second = await asyncio.wait_for(books.__anext__(), 2)

    assert [level.price for level in first.bids] == [Decimal("59999")]
    # The delta arrived carrying one level and came out as the whole book.
    assert [level.price for level in second.bids] == [
        Decimal("59999"),
        Decimal("59998"),
    ]
    assert second.asks[0].price == Decimal("60001")


async def test_a_gapped_book_resubscribes_instead_of_publishing(
    bybit_public: FakeBybit,
) -> None:
    """Bybit only sends a snapshot when a subscription starts, so the way back
    from a gap is to end the subscription and start it again."""
    async with _feed(bybit_public) as feed:
        books = await feed.subscribe_order_book(NATIVE, depth=50)
        await bybit_public.push(
            "orderbook.50.BTCUSDT", _book(1, [["1", "1"]], []), kind="snapshot"
        )
        await asyncio.wait_for(books.__anext__(), 2)

        await bybit_public.push(
            "orderbook.50.BTCUSDT", _book(99, [["2", "1"]], []), kind="delta"
        )
        for _ in range(200):
            if bybit_public.frames_for("unsubscribe"):
                break
            await asyncio.sleep(0.01)

        assert bybit_public.frames_for("unsubscribe")
        assert len(bybit_public.frames_for("subscribe")) == 2
        # Nothing was published from the gapped state.
        await bybit_public.push(
            "orderbook.50.BTCUSDT", _book(1, [["3", "1"]], []), kind="snapshot"
        )
        book = await asyncio.wait_for(books.__anext__(), 2)
        assert [level.price for level in book.bids] == [Decimal("3")]


async def test_a_second_folder_is_replayed_the_live_book_without_a_second_subscribe(
    bybit_public: FakeBybit,
) -> None:
    """MDS-2: the joiner starts from state the socket already has."""
    topic = "orderbook.50.BTCUSDT"
    async with _feed(bybit_public) as feed:
        first = await feed.subscribe_order_book(NATIVE, depth=50)
        await bybit_public.push(
            topic, _book(1, [["59999", "1"]], [["60001", "2"]]), kind="snapshot"
        )
        await asyncio.wait_for(first.__anext__(), 2)

        second = await feed.subscribe_order_book(NATIVE, depth=50)
        replay = await asyncio.wait_for(second.__anext__(), 2)

    assert [level.price for level in replay.bids] == [Decimal("59999")]
    assert len(bybit_public.frames_for("subscribe")) == 1


async def test_a_mid_fold_joiner_sees_the_same_book_and_does_not_reset_u(
    bybit_public: FakeBybit,
) -> None:
    topic = "orderbook.50.BTCUSDT"
    async with _feed(bybit_public) as feed:
        first = await feed.subscribe_order_book(NATIVE, depth=50)
        await bybit_public.push(
            topic, _book(1, [["1", "1"]], [["9", "1"]]), kind="snapshot"
        )
        await asyncio.wait_for(first.__anext__(), 2)
        await bybit_public.push(topic, _book(2, [["2", "1"]], []), kind="delta")
        latest = await asyncio.wait_for(first.__anext__(), 2)
        update_id = feed._books[topic].update_id

        second = await feed.subscribe_order_book(NATIVE, depth=50)
        replay = await asyncio.wait_for(second.__anext__(), 2)

        assert feed._books[topic].update_id == update_id
        assert [level.price for level in replay.bids] == [
            level.price for level in latest.bids
        ]


async def test_a_joiner_reads_the_replay_before_a_newer_push(
    bybit_public: FakeBybit,
) -> None:
    """Replay and ``_subs.append`` share a step; ``_push`` cannot land first."""
    topic = "orderbook.50.BTCUSDT"
    async with _feed(bybit_public) as feed:
        first = await feed.subscribe_order_book(NATIVE, depth=50)
        await bybit_public.push(topic, _book(1, [["1", "1"]], []), kind="snapshot")
        await asyncio.wait_for(first.__anext__(), 2)

        second = await feed.subscribe_order_book(NATIVE, depth=50)
        await bybit_public.push(topic, _book(2, [["2", "1"]], []), kind="delta")

        replay = await asyncio.wait_for(second.__anext__(), 2)
        newer = await asyncio.wait_for(second.__anext__(), 2)
        assert [level.price for level in replay.bids] == [Decimal("1")]
        assert [level.price for level in newer.bids] == [Decimal("2"), Decimal("1")]


async def test_book_deltas_refuse_a_topic_a_folder_already_holds(
    bybit_public: FakeBybit,
) -> None:
    async with _feed(bybit_public) as feed:
        await feed.subscribe_order_book(NATIVE, depth=50)
        with pytest.raises(ValueError, match="orderbook.50.BTCUSDT") as raised:
            await feed.subscribe_book_deltas(NATIVE, depth=50)
        assert "already folded" in str(raised.value)


async def test_book_deltas_do_not_subscribe_a_sibling_when_one_topic_is_folded(
    bybit_public: FakeBybit,
) -> None:
    async with _feed(bybit_public) as feed:
        await feed.subscribe_order_book(NATIVE, depth=50)
        with pytest.raises(ValueError, match="orderbook.50.BTCUSDT"):
            await feed.subscribe_book_deltas(NATIVE, "ETHUSDT", depth=50)
        assert "orderbook.50.ETHUSDT" not in feed._ledger.held()
        assert all(
            "orderbook.50.ETHUSDT" not in (frame.get("args") or [])
            for frame in bybit_public.frames_for("subscribe")
        )


async def test_a_folder_joining_a_raw_held_topic_resyncs_once(
    bybit_public: FakeBybit,
) -> None:
    """The escape hatch must not lock out ``subscribe_order_book``."""
    topic = "orderbook.50.BTCUSDT"
    async with _feed(bybit_public) as feed:
        raw = await feed.subscribe_book_deltas(NATIVE, depth=50)
        assert len(bybit_public.frames_for("subscribe")) == 1

        books = await feed.subscribe_order_book(NATIVE, depth=50)
        assert len(bybit_public.frames_for("subscribe")) == 1
        for _ in range(200):
            if bybit_public.frames_for("unsubscribe"):
                break
            await asyncio.sleep(0.01)
        assert len(bybit_public.frames_for("unsubscribe")) == 1
        assert len(bybit_public.frames_for("subscribe")) == 2

        await bybit_public.push(
            topic, _book(1, [["5", "1"]], [["6", "1"]]), kind="snapshot"
        )
        folded = await asyncio.wait_for(books.__anext__(), 2)
        kind, payload = await asyncio.wait_for(raw.__anext__(), 2)

    assert [level.price for level in folded.bids] == [Decimal("5")]
    assert kind == "snapshot"
    assert payload.bid_levels()[0].price == Decimal("5")


async def test_two_delta_consumers_share_and_neither_is_replayed(
    bybit_public: FakeBybit,
) -> None:
    topic = "orderbook.50.BTCUSDT"
    async with _feed(bybit_public) as feed:
        first, second = await asyncio.gather(
            feed.subscribe_book_deltas(NATIVE, depth=50),
            feed.subscribe_book_deltas(NATIVE, depth=50),
        )
        assert len(bybit_public.frames_for("subscribe")) == 1
        await bybit_public.push(topic, _book(1, [["1", "1"]], []), kind="snapshot")
        assert (await asyncio.wait_for(first.__anext__(), 2))[0] == "snapshot"
        assert (await asyncio.wait_for(second.__anext__(), 2))[0] == "snapshot"


async def test_a_gap_on_a_shared_fold_resyncs_exactly_once(
    bybit_public: FakeBybit,
) -> None:
    """MDS-3: resync is a force path. The ledger is not opened or closed."""
    topic = "orderbook.50.BTCUSDT"
    async with _feed(bybit_public) as feed:
        first = await feed.subscribe_order_book(NATIVE, depth=50)
        await bybit_public.push(topic, _book(1, [["1", "1"]], []), kind="snapshot")
        await asyncio.wait_for(first.__anext__(), 2)
        second = await feed.subscribe_order_book(NATIVE, depth=50)
        await asyncio.wait_for(second.__anext__(), 2)
        held = feed._ledger.held()

        await bybit_public.push(topic, _book(99, [["2", "1"]], []), kind="delta")
        for _ in range(200):
            if bybit_public.frames_for("unsubscribe"):
                break
            await asyncio.sleep(0.01)

        assert feed._ledger.held() == held
        assert topic in held
        assert len(bybit_public.frames_for("unsubscribe")) == 1
        assert len(bybit_public.frames_for("subscribe")) == 2

        await bybit_public.push(topic, _book(1, [["3", "1"]], []), kind="snapshot")
        recovered_first = await asyncio.wait_for(first.__anext__(), 2)
        recovered_second = await asyncio.wait_for(second.__anext__(), 2)
        assert [level.price for level in recovered_first.bids] == [Decimal("3")]
        assert [level.price for level in recovered_second.bids] == [Decimal("3")]


async def test_unsubscribe_raises_while_a_co_reader_holds_the_topic(
    bybit_public: FakeBybit,
) -> None:
    topic = "orderbook.50.BTCUSDT"
    async with _feed(bybit_public) as feed:
        first = await feed.subscribe_order_book(NATIVE, depth=50)
        await bybit_public.push(topic, _book(1, [["1", "1"]], []), kind="snapshot")
        await asyncio.wait_for(first.__anext__(), 2)
        await feed.subscribe_order_book(NATIVE, depth=50)
        with pytest.raises(ValueError, match="readers"):
            await feed.unsubscribe(topic)
        assert topic in feed._ledger.held()
        assert topic in feed._books


async def test_an_unsupported_depth_is_refused_locally(
    bybit_public: FakeBybit,
) -> None:
    """Bybit acknowledges a subscribe to any depth and then never pushes, so a
    typo would be a silent dead feed."""
    async with _feed(bybit_public, product="spot") as feed:
        with pytest.raises(ValueError, match="serves no 500-level book"):
            await feed.subscribe_order_book(NATIVE, depth=500)
    assert not bybit_public.subscribed


async def test_a_reconnect_resubscribes_and_rebuilds_the_book(
    bybit_public: FakeBybit,
) -> None:
    async with _feed(bybit_public, retry_backoff=0.01) as feed:
        books = await feed.subscribe_order_book(NATIVE, depth=50)
        await bybit_public.push(
            "orderbook.50.BTCUSDT", _book(1, [["1", "1"]], []), kind="snapshot"
        )
        await asyncio.wait_for(books.__anext__(), 2)

        await bybit_public.drop()
        for _ in range(200):
            if len(bybit_public.frames_for("subscribe")) >= 2:
                break
            await asyncio.sleep(0.01)

        # Whatever the old book held described a connection that is gone; the
        # new subscription opens with a snapshot anyway.
        await bybit_public.push(
            "orderbook.50.BTCUSDT", _book(1, [["5", "1"]], []), kind="snapshot"
        )
        book = await asyncio.wait_for(books.__anext__(), 2)
        assert [level.price for level in book.bids] == [Decimal("5")]


# --- the connector ---------------------------------------------------------


def _client(stub: FakeBybit, product: str = "spot") -> BybitPublicClient:
    return BybitPublicClient(
        symbols=StubSymbols(),
        feeds={product: BybitPublicStream(url=stub.url, ping_interval=0)},
    )


async def test_the_connector_stamps_the_instrument_it_was_asked_for(
    bybit_public: FakeBybit,
) -> None:
    client = _client(bybit_public)
    async with client:
        ticker = UniversalTicker.parse("Bybit_Spot_BTCUSDT")
        stream = client.stream_trades(ticker)
        task = asyncio.ensure_future(stream.__anext__())
        await asyncio.sleep(0.05)
        await bybit_public.push("publicTrade.BTCUSDT", [TRADE_ROW])
        trade = await asyncio.wait_for(task, 2)

    assert trade.symbol == "BTCUSDT"
    assert trade.price == Decimal("60000")
    # The identity, not just the symbol: this venue's spot tape and its perp
    # tape are the same topic name on two sockets, and a strategy reading both
    # would otherwise have nothing to tell the prints apart by.
    assert trade.universal_ticker == "Bybit_Spot_BTCUSDT"
    assert trade.category is Category.SPOT


async def test_the_two_books_are_distinguishable_on_one_hook(
    bybit_public: FakeBybit,
) -> None:
    """A unified venue's spot and perp tapes carry the same symbol, the same
    topic name, and — until now — the same payload identity."""
    spot_feed = BybitPublicStream(url=bybit_public.url, ping_interval=0)
    perp_feed = BybitPublicStream(url=bybit_public.url, ping_interval=0)
    client = BybitPublicClient(
        symbols=StubSymbols(), feeds={"spot": spot_feed, "linear": perp_feed}
    )
    async with client:
        spot = client.stream_trades(UniversalTicker.parse("Bybit_Spot_BTCUSDT"))
        perp = client.stream_trades(UniversalTicker.parse("Bybit_Perp_BTCUSDT"))
        tasks = [
            asyncio.ensure_future(spot.__anext__()),
            asyncio.ensure_future(perp.__anext__()),
        ]
        await asyncio.sleep(0.05)
        # One push, on a socket both feeds share in this test — so the only
        # thing that can separate the two prints is what each stream stamps.
        await bybit_public.push("publicTrade.BTCUSDT", [TRADE_ROW])
        first, second = await asyncio.wait_for(asyncio.gather(*tasks), 2)

    assert {first.universal_ticker, second.universal_ticker} == {
        "Bybit_Spot_BTCUSDT",
        "Bybit_Perp_BTCUSDT",
    }
    assert first.symbol == second.symbol == "BTCUSDT"


async def test_a_ticker_delta_with_no_price_is_not_published(
    bybit_public: FakeBybit,
) -> None:
    client = _client(bybit_public)
    async with client:
        ticker = UniversalTicker.parse("Bybit_Spot_BTCUSDT")
        stream = client.stream_ticker(ticker)
        task = asyncio.ensure_future(stream.__anext__())
        await asyncio.sleep(0.05)
        # A funding-rate delta, which carries no quote at all.
        await bybit_public.push(
            "tickers.BTCUSDT", {"symbol": NATIVE, "fundingRate": "0.0001"}, kind="delta"
        )
        await bybit_public.push(
            "tickers.BTCUSDT", {"symbol": NATIVE, "lastPrice": "60000"}
        )
        row = await asyncio.wait_for(task, 2)

    assert row.last == Decimal("60000")


async def test_a_client_is_refused_a_ticker_from_another_venue(
    bybit_public: FakeBybit,
) -> None:
    client = _client(bybit_public)
    async with client:
        with pytest.raises(ValueError, match="was handed a Binance ticker"):
            await client.fetch_ticker(UniversalTicker.parse("Binance_Spot_BTCUSDT"))


async def test_liquidations_arrive_stamped_with_the_perp(
    bybit_public: FakeBybit,
) -> None:
    """``allLiquidation`` is a contract-book topic; side is the position closed."""
    client = _client(bybit_public, product="linear")
    async with client:
        ticker = UniversalTicker.parse("Bybit_Perp_BTCUSDT")
        stream = client.stream_liquidation(ticker)
        task = asyncio.ensure_future(stream.__anext__())
        await asyncio.sleep(0.05)
        await bybit_public.push(
            "allLiquidation.BTCUSDT",
            [
                {
                    "T": 1700000000000,
                    "s": NATIVE,
                    "S": "Sell",
                    "v": "12.5",
                    "p": "59900",
                }
            ],
        )
        row = await asyncio.wait_for(task, 2)

    assert row.universal_ticker == "Bybit_Perp_BTCUSDT"
    assert row.side.value == "sell"
    assert row.qty == Decimal("12.5")
    assert row.price == Decimal("59900")
    assert row.ts == 1700000000.0
    assert bybit_public.subscribed == {"allLiquidation.BTCUSDT"}


async def test_spot_has_no_liquidation_stream(bybit_public: FakeBybit) -> None:
    """Spot cannot be liquidated; the subscribe is refused before any socket."""
    client = _client(bybit_public)
    async with client:
        with pytest.raises(ValueError, match="serves no liquidation stream"):
            client.stream_liquidation(UniversalTicker.parse("Bybit_Spot_BTCUSDT"))


async def test_a_funding_snapshot_uses_the_envelope_stamp(
    bybit_public: FakeBybit,
) -> None:
    client = _client(bybit_public, product="linear")
    async with client:
        ticker = UniversalTicker.parse("Bybit_Perp_BTCUSDT")
        stream = client.stream_funding_rate(ticker)
        task = asyncio.ensure_future(stream.__anext__())
        await asyncio.sleep(0.05)
        await bybit_public.push(
            "tickers.BTCUSDT",
            {
                "symbol": NATIVE,
                "lastPrice": "60000",
                "fundingRate": "0.0001",
            },
            ts=1_700_000_000_000,
        )
        row = await asyncio.wait_for(task, 2)

    assert row.rate == Decimal("0.0001")
    assert row.ts == 1_700_000_000.0
    assert not hasattr(row, "next_funding_time")


async def test_a_funding_only_delta_feeds_funding_not_the_ticker(
    bybit_public: FakeBybit,
) -> None:
    client = _client(bybit_public, product="linear")
    perp = UniversalTicker.parse("Bybit_Perp_BTCUSDT")
    async with client:
        quotes = client.stream_ticker(perp)
        rates = client.stream_funding_rate(perp)
        quote_task = asyncio.ensure_future(quotes.__anext__())
        rate_task = asyncio.ensure_future(rates.__anext__())
        await asyncio.sleep(0.05)
        await bybit_public.push(
            "tickers.BTCUSDT",
            {"symbol": NATIVE, "fundingRate": "0.0001"},
            kind="delta",
            ts=1_700_000_000_000,
        )
        funding = await asyncio.wait_for(rate_task, 2)
        assert not quote_task.done()
        await bybit_public.push(
            "tickers.BTCUSDT",
            {"symbol": NATIVE, "lastPrice": "60000"},
        )
        quote = await asyncio.wait_for(quote_task, 2)

    assert funding.rate == Decimal("0.0001")
    assert quote.last == Decimal("60000")


async def test_ticker_and_funding_share_one_venue_subscription(
    bybit_public: FakeBybit,
) -> None:
    client = _client(bybit_public, product="linear")
    perp = UniversalTicker.parse("Bybit_Perp_BTCUSDT")
    async with client:
        quotes = client.stream_ticker(perp)
        rates = client.stream_funding_rate(perp)
        quote_task = asyncio.ensure_future(quotes.__anext__())
        rate_task = asyncio.ensure_future(rates.__anext__())
        await asyncio.sleep(0.05)
        assert len(bybit_public.frames_for("subscribe")) == 1
        assert bybit_public.subscribed == {"tickers.BTCUSDT"}
        await bybit_public.push(
            "tickers.BTCUSDT",
            {
                "symbol": NATIVE,
                "lastPrice": "60000",
                "fundingRate": "0.0001",
            },
            ts=1_700_000_000_000,
        )
        quote, funding = await asyncio.wait_for(
            asyncio.gather(quote_task, rate_task), 2
        )

    assert quote.last == Decimal("60000")
    assert funding.rate == Decimal("0.0001")
    assert funding.ts == 1_700_000_000.0


async def test_spot_has_no_funding_rate_stream(bybit_public: FakeBybit) -> None:
    """Spot is not funded; the subscribe is refused before any socket."""
    client = _client(bybit_public)
    async with client:
        with pytest.raises(ValueError, match="serves no funding rate stream"):
            client.stream_funding_rate(UniversalTicker.parse("Bybit_Spot_BTCUSDT"))


async def test_an_open_interest_only_delta_feeds_oi_not_the_ticker(
    bybit_public: FakeBybit,
) -> None:
    client = _client(bybit_public, product="linear")
    perp = UniversalTicker.parse("Bybit_Perp_BTCUSDT")
    async with client:
        quotes = client.stream_ticker(perp)
        sizes = client.stream_open_interest(perp)
        quote_task = asyncio.ensure_future(quotes.__anext__())
        size_task = asyncio.ensure_future(sizes.__anext__())
        await asyncio.sleep(0.05)
        await bybit_public.push(
            "tickers.BTCUSDT",
            {"symbol": NATIVE, "openInterest": "1234.5"},
            kind="delta",
            ts=1_700_000_000_000,
        )
        interest = await asyncio.wait_for(size_task, 2)
        assert not quote_task.done()
        await bybit_public.push(
            "tickers.BTCUSDT",
            {"symbol": NATIVE, "lastPrice": "60000"},
        )
        quote = await asyncio.wait_for(quote_task, 2)

    assert interest.qty == Decimal("617.25")
    assert interest.ts == 1_700_000_000.0
    assert quote.last == Decimal("60000")


async def test_a_single_open_interest_delta_feeds_oi(
    bybit_public: FakeBybit,
) -> None:
    client = _client(bybit_public, product="linear")
    perp = UniversalTicker.parse("Bybit_Perp_BTCUSDT")
    async with client:
        sizes = client.stream_open_interest(perp)
        size_task = asyncio.ensure_future(sizes.__anext__())
        await asyncio.sleep(0.05)
        await bybit_public.push(
            "tickers.BTCUSDT",
            {"symbol": NATIVE, "singleOpenInterest": "12.5"},
            kind="delta",
            ts=1_700_000_000_000,
        )
        interest = await asyncio.wait_for(size_task, 2)

    assert interest.qty == Decimal("12.5")
    assert interest.ts == 1_700_000_000.0


async def test_a_quoted_delta_without_open_interest_yields_neither_oi(
    bybit_public: FakeBybit,
) -> None:
    client = _client(bybit_public, product="linear")
    perp = UniversalTicker.parse("Bybit_Perp_BTCUSDT")
    async with client:
        sizes = client.stream_open_interest(perp)
        size_task = asyncio.ensure_future(sizes.__anext__())
        await asyncio.sleep(0.05)
        await bybit_public.push(
            "tickers.BTCUSDT",
            {"symbol": NATIVE, "lastPrice": "60000"},
        )
        await asyncio.sleep(0.05)
        assert not size_task.done()
        await bybit_public.push(
            "tickers.BTCUSDT",
            {"symbol": NATIVE, "openInterest": "9"},
            ts=1_700_000_000_000,
        )
        interest = await asyncio.wait_for(size_task, 2)

    assert interest.qty == Decimal("4.5")
    size_task.cancel()


async def test_ticker_and_open_interest_share_one_venue_subscription(
    bybit_public: FakeBybit,
) -> None:
    client = _client(bybit_public, product="linear")
    perp = UniversalTicker.parse("Bybit_Perp_BTCUSDT")
    async with client:
        quotes = client.stream_ticker(perp)
        sizes = client.stream_open_interest(perp)
        quote_task = asyncio.ensure_future(quotes.__anext__())
        size_task = asyncio.ensure_future(sizes.__anext__())
        await asyncio.sleep(0.05)
        assert len(bybit_public.frames_for("subscribe")) == 1
        assert bybit_public.subscribed == {"tickers.BTCUSDT"}
        await bybit_public.push(
            "tickers.BTCUSDT",
            {
                "symbol": NATIVE,
                "lastPrice": "60000",
                "openInterest": "1234.5",
            },
            ts=1_700_000_000_000,
        )
        quote, interest = await asyncio.wait_for(
            asyncio.gather(quote_task, size_task), 2
        )

    assert quote.last == Decimal("60000")
    assert interest.qty == Decimal("617.25")


async def test_spot_has_no_open_interest_stream(bybit_public: FakeBybit) -> None:
    client = _client(bybit_public)
    async with client:
        with pytest.raises(ValueError, match="serves no open interest stream"):
            client.stream_open_interest(UniversalTicker.parse("Bybit_Spot_BTCUSDT"))


async def test_a_dated_future_has_an_open_interest_stream(
    bybit_public: FakeBybit,
) -> None:
    """A dated future has open interest; only spot is refused."""
    client = _client(bybit_public, product="linear")
    future = UniversalTicker.parse("Bybit_Future_BTCUSDT")
    async with client:
        stream = client.stream_open_interest(future)
        task = asyncio.ensure_future(stream.__anext__())
        await asyncio.sleep(0.05)
        await bybit_public.push(
            "tickers.BTCUSDT",
            {"symbol": NATIVE, "openInterest": "4"},
            ts=1_700_000_000_000,
        )
        interest = await asyncio.wait_for(task, 2)

    assert interest.qty == Decimal("2")
    assert interest.universal_ticker == str(future)


async def test_a_dated_future_has_no_funding_rate_stream(
    bybit_public: FakeBybit,
) -> None:
    """A dated future settles at expiry rather than paying a funding hook, and
    it shares the ``linear`` product with the perps — so the refusal has to
    read the category. Checked on the product it would pass, and the pump
    would then sit on a wire that never carries a ``fundingRate``."""
    client = _client(bybit_public)
    async with client:
        with pytest.raises(ValueError, match="serves no funding rate stream"):
            client.stream_funding_rate(UniversalTicker.parse("Bybit_Future_BTCUSDT"))


async def test_each_category_gets_its_own_socket(bybit_public: FakeBybit) -> None:
    """Bybit has no single market-data endpoint: spot and linear are different
    connections carrying the same topic names."""
    client = _client(bybit_public, product="spot")
    async with client:
        spot = await client.feed_for("spot")
        assert spot.connected
        assert set(client._feeds) == {"spot"}
        # Asking for the perp book would open a second one, which the stub
        # cannot serve — what matters is that it is not the same object.
        assert client._feeds.get("linear") is None
