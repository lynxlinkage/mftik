"""Last-reader unsubscribe on public sockets.

A close only enqueues. One flusher per socket waits out the linger and
sends a single batched UNSUBSCRIBE. Explicit venue rejections stay held;
a timeout does not. Book resync retries SUBSCRIBE, then discards and
reconnects.
"""

from __future__ import annotations

import asyncio
from decimal import Decimal
from typing import Any

import pytest
from binance_stub import FakeBinanceStream
from bitget_stub import FakeBitget
from bybit_stub import FakeBybit
from deribit_stub import FakeDeribit
from gate_stub import API_KEY, API_SECRET, FakeGate
from mftik.exchange.binance.future import streams as fst
from mftik.exchange.binance.future.feed import BinanceFutureStream
from mftik.exchange.binance.spot import streams as st
from mftik.exchange.binance.spot.feed import BinanceSpotStream
from mftik.exchange.bitget import channels as bgch
from mftik.exchange.bitget.feed import BitgetPublicStream
from mftik.exchange.bybit.feed import BybitPublicStream
from mftik.exchange.deribit import channels as dch
from mftik.exchange.deribit.feed import DeribitPublicStream
from mftik.exchange.gate.spot import channels as gch
from mftik.exchange.gate.spot.client import GateSpotWebSocket
from mftik.exchange.okx import channels as och
from mftik.exchange.okx.feed import OkxPublicStream
from mftik.exchange.stream import EventStream
from okx_stub import FakeOkx
from test_binance_spot_client import AGG_TRADE
from test_bybit_public import NATIVE, TRADE_ROW, _book

BTC = "btcusdt@aggTrade"
ETH = "ethusdt@aggTrade"
BNB = "bnbusdt@aggTrade"


def _spot(stub: FakeBinanceStream, **kwargs: Any) -> BinanceSpotStream:
    return BinanceSpotStream(url=stub.url, keepalive=0, **kwargs)  # type: ignore[attr-defined]


def _bybit(stub: FakeBybit, **kwargs: Any) -> BybitPublicStream:
    return BybitPublicStream(url=stub.url, ping_interval=0, **kwargs)


def _names(frames: list[dict[str, Any]]) -> list[str]:
    return [name for frame in frames for name in (frame.get("params") or [])]


# --- linger and batching ---------------------------------------------------


@pytest.mark.real_sleep(
    reason="WireLedger still sleeps on the wall clock"
)
async def test_a_burst_of_closes_is_one_unsubscribe(
    binance_stream: FakeBinanceStream,
) -> None:
    async with _spot(binance_stream, release_linger=0) as feed:
        streams = [
            await feed.subscribe_agg_trades(symbol)
            for symbol in ("BTCUSDT", "ETHUSDT", "BNBUSDT")
        ]
        for stream in streams:
            stream.close()
        await feed._releaser.drained()
        assert feed._ledger.held() == set()

    frames = binance_stream.frames_for(st.UNSUBSCRIBE)
    assert len(frames) == 1
    assert set(frames[0]["params"]) == {BTC, ETH, BNB}


@pytest.mark.real_sleep(
    reason="WireLedger still sleeps on the wall clock"
)
async def test_a_reopen_inside_the_linger_sends_nothing(
    binance_stream: FakeBinanceStream,
) -> None:
    async with _spot(binance_stream, release_linger=30) as feed:
        first = await feed.subscribe_agg_trades("BTCUSDT")
        first.close()
        again = await feed.subscribe_agg_trades("BTCUSDT")
        await asyncio.sleep(0.05)
        assert binance_stream.frames_for(st.UNSUBSCRIBE) == []
        assert len(binance_stream.frames_for(st.SUBSCRIBE)) == 1
        await binance_stream.push(BTC, AGG_TRADE)
        assert (await asyncio.wait_for(anext(again), timeout=2)).s == "BTCUSDT"


async def test_one_of_two_readers_closing_sends_nothing(
    binance_stream: FakeBinanceStream,
) -> None:
    async with _spot(binance_stream, release_linger=0) as feed:
        first, second = await asyncio.gather(
            feed.subscribe_agg_trades("BTCUSDT"),
            feed.subscribe_agg_trades("BTCUSDT"),
        )
        first.close()
        await feed._releaser.drained()
        assert binance_stream.frames_for(st.UNSUBSCRIBE) == []
        assert BTC in feed._ledger.held()
        await binance_stream.push(BTC, AGG_TRADE)
        assert (await asyncio.wait_for(anext(second), timeout=2)).s == "BTCUSDT"


@pytest.mark.real_sleep(
    reason="WireLedger still sleeps on the wall clock"
)
async def test_each_futures_socket_flushes_its_own_names(
    future_public_stream: FakeBinanceStream,
    future_market_stream: FakeBinanceStream,
) -> None:
    feed = BinanceFutureStream(
        public_url=future_public_stream.url,  # type: ignore[attr-defined]
        market_url=future_market_stream.url,  # type: ignore[attr-defined]
        keepalive=0,
        release_linger=0,
    )
    async with feed:
        book = await feed.subscribe_order_book("BTCUSDT")
        tape = await feed.subscribe_agg_trades("BTCUSDT")
        book.close()
        tape.close()
        for socket in feed._sockets.values():
            await socket._releaser.drained()
            assert socket._ledger.held() == set()

    assert len(future_public_stream.frames_for(fst.UNSUBSCRIBE)) == 1
    assert len(future_market_stream.frames_for(fst.UNSUBSCRIBE)) == 1


@pytest.mark.real_sleep(
    reason="WireLedger still sleeps on the wall clock"
)
async def test_a_timed_out_unsubscribe_lets_the_next_reader_subscribe(
    binance_stream: FakeBinanceStream,
) -> None:
    binance_stream.silent_methods.add(st.UNSUBSCRIBE)
    async with _spot(
        binance_stream, release_linger=0, ack_timeout=0.05
    ) as feed:
        trades = await feed.subscribe_agg_trades("BTCUSDT")
        trades.close()
        await feed._releaser.drained()
        assert BTC not in feed._ledger.held()
        assert binance_stream.frames_for(st.UNSUBSCRIBE)
        again = await feed.subscribe_agg_trades("BTCUSDT")
        assert _names(binance_stream.frames_for(st.SUBSCRIBE)).count(BTC) == 2
        await binance_stream.push(BTC, AGG_TRADE)
        assert (await asyncio.wait_for(anext(again), timeout=2)).s == "BTCUSDT"


@pytest.mark.real_sleep(
    reason="WireLedger still sleeps on the wall clock"
)
async def test_reconnect_does_not_restore_a_released_channel(
    binance_stream: FakeBinanceStream,
) -> None:
    async with _spot(
        binance_stream, release_linger=0, retry_backoff=0.01
    ) as feed:
        keep = await feed.subscribe_agg_trades("BTCUSDT")
        drop = await feed.subscribe_agg_trades("ETHUSDT")
        drop.close()
        await feed._releaser.drained()
        assert ETH not in feed._ledger.held()
        sent = len(binance_stream.frames_for(st.SUBSCRIBE))
        await binance_stream.drop()
        for _ in range(200):
            if len(binance_stream.frames_for(st.SUBSCRIBE)) > sent:
                break
            await asyncio.sleep(0.01)
        replayed = _names(binance_stream.frames_for(st.SUBSCRIBE)[sent:])
        assert BTC in replayed
        assert ETH not in replayed
        await binance_stream.push(BTC, AGG_TRADE)
        assert (await asyncio.wait_for(anext(keep), timeout=2)).s == "BTCUSDT"


async def test_teardown_clears_the_ledger_before_closing_streams(
    binance_stream: FakeBinanceStream,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    order: list[str] = []
    feed = _spot(binance_stream, release_linger=0)
    real_clear = feed._ledger.clear

    def spy_clear() -> None:
        order.append("clear")
        real_clear()

    feed._ledger.clear = spy_clear  # type: ignore[method-assign]
    real_close = EventStream.close

    def spy_close(self: EventStream[Any], reason: str | None = None) -> None:
        order.append("close")
        real_close(self, reason)

    monkeypatch.setattr(EventStream, "close", spy_close)
    async with feed:
        await feed.subscribe_agg_trades("BTCUSDT")
    assert order.index("clear") < order.index("close")
    assert binance_stream.frames_for(st.UNSUBSCRIBE) == []


# --- Bybit -----------------------------------------------------------------


@pytest.mark.real_sleep(
    reason="WireLedger still sleeps on the wall clock"
)
async def test_already_subscribed_after_a_timed_out_unsubscribe_delivers(
    bybit_public: FakeBybit,
) -> None:
    topic = "publicTrade.BTCUSDT"
    bybit_public.silent_ops.add("unsubscribe")
    async with _bybit(bybit_public, release_linger=0, ack_timeout=0.05) as feed:
        trades = await feed.subscribe_trades(NATIVE)
        trades.close()
        await feed._releaser.drained()
        assert topic not in feed._ledger.held()
        bybit_public.errors["subscribe"] = (1, "already subscribed")
        again = await feed.subscribe_trades(NATIVE)
        assert topic in feed._ledger.held()
        await bybit_public.push(topic, [TRADE_ROW])
        assert (await asyncio.wait_for(anext(again), timeout=2)).trade_id == "trade-1"


@pytest.mark.real_sleep(
    reason="the venue socket still sleeps on the wall clock"
)
async def test_a_failed_resync_subscribe_reconnects_the_reader(
    bybit_public: FakeBybit,
) -> None:
    topic = "orderbook.50.BTCUSDT"
    async with _bybit(bybit_public, release_linger=0, retry_backoff=0.01) as feed:
        books = await feed.subscribe_order_book(NATIVE, depth=50)
        await bybit_public.push(topic, _book(1, [["1", "1"]], []), kind="snapshot")
        await asyncio.wait_for(anext(books), timeout=2)
        bybit_public.fail_times["subscribe"] = 3
        await bybit_public.push(topic, _book(99, [["2", "1"]], []), kind="delta")
        for _ in range(400):
            if (
                bybit_public.connections > 1
                and len(bybit_public.frames_for("subscribe")) >= 5
            ):
                break
            await asyncio.sleep(0.01)
        assert bybit_public.connections > 1
        assert bybit_public.fail_times["subscribe"] == 0
        assert topic in feed._ledger.held()
        await bybit_public.push(topic, _book(1, [["3", "1"]], []), kind="snapshot")
        book = await asyncio.wait_for(anext(books), timeout=2)
        assert [level.price for level in book.bids] == [Decimal("3")]


@pytest.mark.real_sleep(
    reason="WireLedger still sleeps on the wall clock"
)
async def test_a_batch_resubscribe_during_the_linger_keeps_the_shared_key(
    bybit_public: FakeBybit,
) -> None:
    """Closing A and then subscribing A with B must not drop A.

    B's ack is what ``subscribe_raw`` waits on, and the ``_Sub`` for A
    is appended only after that. The linger can fire in between.
    """
    topic_a = "publicTrade.BTCUSDT"
    topic_b = "publicTrade.ETHUSDT"
    async with _bybit(bybit_public, release_linger=0.05) as feed:
        first = await feed.subscribe_raw(topic_a)
        first.close()
        gate = asyncio.Event()
        entered = asyncio.Event()
        original = feed.request

        async def request(frame, req_id, *, op="", timeout=None):
            if op == "subscribe":
                entered.set()
                await gate.wait()
            return await original(frame, req_id, op=op, timeout=timeout)

        feed.request = request  # type: ignore[method-assign]
        pending = asyncio.create_task(feed.subscribe_raw(topic_a, topic_b))
        await entered.wait()
        await asyncio.sleep(0.15)
        unsubscribed = [
            topic
            for frame in bybit_public.frames_for("unsubscribe")
            for topic in (frame.get("args") or [])
        ]
        assert topic_a not in unsubscribed
        assert topic_a in feed._ledger.held()
        gate.set()
        stream = await pending
        await asyncio.sleep(0.15)
        assert topic_a not in [
            topic
            for frame in bybit_public.frames_for("unsubscribe")
            for topic in (frame.get("args") or [])
        ]
        await bybit_public.push(topic_a, [TRADE_ROW])
        assert (await asyncio.wait_for(anext(stream), timeout=2))["i"] == "trade-1"


@pytest.mark.real_sleep(
    reason="this test calls asyncio.sleep while waiting for a real side effect"
)
async def test_an_already_subscribed_resync_keeps_the_socket(
    bybit_public: FakeBybit,
) -> None:
    """A subscribe that landed, whose ack missed the timeout, is not a failure."""
    topic = "orderbook.50.BTCUSDT"
    async with _bybit(
        bybit_public, release_linger=0, ack_timeout=0.05, retry_backoff=0.01
    ) as feed:
        books = await feed.subscribe_order_book(NATIVE, depth=50)
        await bybit_public.push(topic, _book(1, [["1", "1"]], []), kind="snapshot")
        await asyncio.wait_for(anext(books), timeout=2)
        bybit_public.silent_times["subscribe"] = 1
        bybit_public.errors["subscribe"] = (1, "already subscribed")
        await bybit_public.push(topic, _book(99, [["2", "1"]], []), kind="delta")
        for _ in range(100):
            subs = len(bybit_public.frames_for("subscribe"))
            unsubs = len(bybit_public.frames_for("unsubscribe"))
            if subs >= 3 and unsubs >= 1:
                break
            await asyncio.sleep(0.01)
        await asyncio.sleep(0.2)
        assert bybit_public.connections == 1
        assert len(bybit_public.frames_for("subscribe")) == 3
        assert topic in feed._ledger.held()
        bybit_public.errors.pop("subscribe", None)
        await bybit_public.push(topic, _book(1, [["3", "1"]], []), kind="snapshot")
        book = await asyncio.wait_for(anext(books), timeout=2)
        assert [level.price for level in book.bids] == [Decimal("3")]


# --- Deribit ---------------------------------------------------------------


@pytest.mark.real_sleep(
    reason="WireLedger still sleeps on the wall clock"
)
async def test_closing_some_tickers_releases_only_those(
    deribit_public: FakeDeribit,
) -> None:
    feed = DeribitPublicStream(
        deribit_public.url, ping_interval=0, heartbeat=0, release_linger=0
    )
    btc = dch.ticker("BTC_USDC")
    eth = dch.ticker("ETH_USDC")
    async with feed:
        btc_stream = await feed.subscribe_tickers("BTC_USDC")
        await feed.subscribe_tickers("ETH_USDC")
        btc_stream.close()
        await feed._releaser.drained()
        assert feed._ledger.held() == {eth}
        unsubscribed = [
            channel
            for frame in deribit_public.frames_for(dch.PUBLIC_UNSUBSCRIBE)
            for channel in frame["params"]["channels"]
        ]
        assert unsubscribed == [btc]


# --- Gate ------------------------------------------------------------------


def _gate(stub: FakeGate, **kwargs: Any) -> GateSpotWebSocket:
    return GateSpotWebSocket(
        url=stub.url,  # type: ignore[attr-defined]
        ping_interval=0,
        api_key=API_KEY,
        api_secret=API_SECRET,
        **kwargs,
    )


async def test_a_private_sub_blocks_release_of_the_same_key(gate: FakeGate) -> None:
    async with _gate(gate, release_linger=0) as ws:
        public = await ws.subscribe_tickers("BTC_USDT")
        await ws._subscribe(
            gch.TICKERS, ["BTC_USDT"], lambda row: row, private=True
        )
        public.close()
        await ws._releaser.drained()
        assert gate.frames_for(gch.TICKERS, gch.UNSUBSCRIBE) == []
        assert (gch.TICKERS, ("BTC_USDT",)) in ws._ledger.held()


@pytest.mark.real_sleep(
    reason="WireLedger still sleeps on the wall clock"
)
async def test_a_structured_batch_keeps_a_rejected_frame_held(gate: FakeGate) -> None:
    eth = (gch.ORDER_BOOK, ("ETH_USDT", "20", "1000ms"))
    btc = (gch.ORDER_BOOK, ("BTC_USDT", "20", "1000ms"))
    async with _gate(gate, release_linger=0) as ws:
        btc_stream = await ws.subscribe_order_book("BTC_USDT")
        eth_stream = await ws.subscribe_order_book("ETH_USDT")
        gate.reject_payloads.add(eth[1])
        btc_stream.close()
        eth_stream.close()
        await ws._releaser.drained()
        assert btc not in ws._ledger.held()
        assert eth in ws._ledger.held()
        assert len(gate.frames_for(gch.ORDER_BOOK, gch.UNSUBSCRIBE)) == 2


async def test_fail_streams_clears_before_close_and_sends_nothing(
    gate: FakeGate,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    order: list[str] = []
    ws = _gate(gate, release_linger=0)
    real_clear = ws._ledger.clear

    def spy_clear() -> None:
        order.append("clear")
        real_clear()

    ws._ledger.clear = spy_clear  # type: ignore[method-assign]
    real_close = EventStream.close

    def spy_close(self: EventStream[Any], reason: str | None = None) -> None:
        order.append("close")
        real_close(self, reason)

    monkeypatch.setattr(EventStream, "close", spy_close)
    async with ws:
        await ws.subscribe_trades("BTC_USDT")
        ws._fail_streams()
        assert order.index("clear") < order.index("close")
    assert gate.frames_for(gch.TRADES, gch.UNSUBSCRIBE) == []


@pytest.mark.real_sleep(
    reason="WireLedger still sleeps on the wall clock"
)
async def test_reconcile_leaves_a_closed_private_sub(gate: FakeGate) -> None:
    """A public close arms reconcile. A private key left up stays up."""
    async with _gate(gate, release_linger=0) as ws:
        ws._releaser.reconcile_interval = 0.05
        private = await ws._subscribe(
            gch.ORDERS, ["BTC_USDT"], lambda row: row, private=True
        )
        private.close()
        public = await ws.subscribe_trades("ETH_USDT")
        public.close()
        await ws._releaser.drained()
        await asyncio.sleep(0.15)
        assert (gch.ORDERS, ("BTC_USDT",)) in ws._ledger.held()
        assert gate.frames_for(gch.ORDERS, gch.UNSUBSCRIBE) == []
        assert (gch.TRADES, ("ETH_USDT",)) not in ws._ledger.held()


# --- OKX and Bitget books --------------------------------------------------


def _okx(stub: FakeOkx, **kwargs: Any) -> OkxPublicStream:
    return OkxPublicStream(stub.url, ping_interval=0, **kwargs)  # type: ignore[attr-defined]


def _bitget(stub: FakeBitget, **kwargs: Any) -> BitgetPublicStream:
    return BitgetPublicStream(
        stub.url,  # type: ignore[attr-defined]
        inst_type="spot",
        ping_interval=0,
        **kwargs,
    )


@pytest.mark.real_sleep(
    reason="WireLedger still sleeps on the wall clock"
)
async def test_okx_book_close_sends_unsubscribe(okx_public: FakeOkx) -> None:
    arg = och.books("BTC-USDT")
    async with _okx(okx_public, release_linger=0) as feed:
        books = await feed.subscribe_order_book("BTC-USDT")
        books.close()
        await feed._releaser.drained()
        frames = okx_public.frames_for("unsubscribe")
        assert frames[-1]["args"] == [arg]
        assert och.arg_key(arg) not in feed._ledger.held()


@pytest.mark.real_sleep(
    reason="WireLedger still sleeps on the wall clock"
)
async def test_okx_book_without_args_stays_held(okx_public: FakeOkx) -> None:
    arg = och.books("BTC-USDT")
    async with _okx(okx_public, release_linger=0) as feed:
        books = await feed.subscribe_order_book("BTC-USDT")
        feed._args.clear()
        books.close()
        await feed._releaser.drained()
        assert okx_public.frames_for("unsubscribe") == []
        assert och.arg_key(arg) in feed._ledger.held()


@pytest.mark.real_sleep(
    reason="WireLedger still sleeps on the wall clock"
)
async def test_bitget_book_close_sends_unsubscribe(bitget_public: FakeBitget) -> None:
    arg = bgch.books("spot", "BTCUSDT")
    async with _bitget(bitget_public, release_linger=0) as feed:
        books = await feed.subscribe_order_book("spot", "BTCUSDT")
        books.close()
        await feed._releaser.drained()
        frames = bitget_public.frames_for("unsubscribe")
        assert frames[-1]["args"] == [arg]
        assert bgch.arg_key(arg) not in feed._ledger.held()


@pytest.mark.real_sleep(
    reason="WireLedger still sleeps on the wall clock"
)
async def test_bitget_book_without_args_stays_held(bitget_public: FakeBitget) -> None:
    arg = bgch.books("spot", "BTCUSDT")
    async with _bitget(bitget_public, release_linger=0) as feed:
        books = await feed.subscribe_order_book("spot", "BTCUSDT")
        feed._args.clear()
        books.close()
        await feed._releaser.drained()
        assert bitget_public.frames_for("unsubscribe") == []
        assert bgch.arg_key(arg) in feed._ledger.held()
