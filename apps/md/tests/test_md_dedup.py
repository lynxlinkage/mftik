"""Overlapping MD publishers must not double-deliver to STS or the tape."""

from __future__ import annotations

from decimal import Decimal

import fakeredis.aioredis
import pytest
from mftik.broker import Broker, BrokerConfig
from mftik.exchange.tickers import UniversalTicker
from mftik.protocol import MD_TRADE, Topics, UntypedEnvelope
from mftik_md.dedup import event_id
from mftik_md.publish_track import ROLE_MIRROR, ROLE_PRIMARY, PublishTracker
from mftik_md.session.dispatcher import Dispatcher
from mftik_md.session.manager import StsLink
from mftik_md.tape import TapeRecorder

BTC = UniversalTicker.parse("Bybit_Perp_BTCUSDT")
ETH = UniversalTicker.parse("Gate_Spot_ETHUSDT")


@pytest.fixture
async def broker() -> Broker:
    redis = fakeredis.aioredis.FakeRedis(decode_responses=True)
    client = Broker(
        BrokerConfig(redis_url="redis://fake", key_prefix="test-dedup"),
        redis_client=redis,
    )
    await client.connect()
    yield client
    await client.close()
    await redis.aclose()


def _trade(ticker: UniversalTicker, trade_id: str, *, price: str = "100") -> UntypedEnvelope:
    return UntypedEnvelope.wrap(
        {
            "universal_ticker": str(ticker),
            "trade_id": trade_id,
            "price": price,
            "qty": "1",
            "side": "buy",
            "ts": 1.0,
        },
        type=MD_TRADE,
        source="md",
    )


@pytest.mark.asyncio
async def test_second_publish_of_the_same_trade_is_dropped(broker: Broker) -> None:
    recorder = TapeRecorder(broker, topics=("trade",), maxlen=100, retention_s=60)
    tracker = PublishTracker(broker, role=ROLE_PRIMARY)
    await tracker.reset()
    dispatcher = Dispatcher(broker, recorder=recorder, tracker=tracker)
    link = StsLink(session_id="s1", created_by=1)
    dispatcher.register_link(link)
    dispatcher.subscribe("s1", "trade", BTC)

    seen: list[str] = []
    stop = __import__("asyncio").Event()

    async def _collect() -> None:
        async for env in broker.subscribe(Topics.md_session("s1"), stop=stop):
            seen.append(env.payload["trade_id"])

    task = __import__("asyncio").create_task(_collect())
    await __import__("asyncio").sleep(0.02)

    env = _trade(BTC, "t-1")
    await dispatcher.publish("trade", BTC, env)
    await dispatcher.publish("trade", BTC, env)
    await __import__("asyncio").sleep(0.05)
    stop.set()
    await task

    assert seen == ["t-1"]
    tape = await broker.tape_tail(Topics.md_feed("trade", BTC), count=10)
    assert len(tape) == 1
    assert tape[0][1]["trade_id"] == "t-1"
    assert Topics.md_feed("trade", BTC) in await tracker.published()


@pytest.mark.asyncio
async def test_same_trade_id_on_another_ticker_is_kept(broker: Broker) -> None:
    dispatcher = Dispatcher(broker)
    env = _trade(BTC, "shared")
    other = _trade(ETH, "shared")
    assert await dispatcher._claim("trade", BTC, env.payload) is True  # noqa: SLF001
    assert await dispatcher._claim("trade", ETH, other.payload) is True  # noqa: SLF001
    assert await dispatcher._claim("trade", BTC, env.payload) is False  # noqa: SLF001


def test_event_id_uses_trade_id_and_kline_window() -> None:
    assert event_id("trade", {"trade_id": "9"}) == "9"
    assert event_id("aggtrade", {"trade_id": "a"}) == "a"
    assert (
        event_id("kline_1m", {"interval": "1m", "open_time": 10.0, "closed": False})
        == "1m|10.0|0"
    )
    assert (
        event_id("kline_1m", {"interval": "1m", "open_time": 10.0, "closed": True})
        == "1m|10.0|1"
    )
    assert event_id("bestquote", {"bid": "1", "ask": "2", "bid_qty": "3", "ask_qty": "4"}) == (
        "1|2|3|4"
    )
    assert event_id("orderbook", {"last_update_id": 77}) == "77"
    assert event_id("orderbook", {"u": 8}) == "8"


@pytest.mark.asyncio
async def test_mirror_ready_after_every_pinned_feed_publishes(broker: Broker) -> None:
    feed = Topics.md_feed("trade", BTC)
    tracker = PublishTracker(broker, role=ROLE_MIRROR, pinned=[feed])
    await tracker.reset()
    assert await tracker.is_ready() is False
    await tracker.mark_published(feed)
    assert await tracker.is_ready() is True


@pytest.mark.asyncio
async def test_empty_pin_list_is_ready_immediately(broker: Broker) -> None:
    tracker = PublishTracker(broker, role=ROLE_MIRROR, pinned=())
    await tracker.reset()
    assert await tracker.is_ready() is True


@pytest.mark.asyncio
async def test_pin_live_sessions_registers_targets_without_stamping(
    broker: Broker,
) -> None:
    from mftik.exchange import PaperExchange
    from mftik_md.session import PaperPublicFactory, SessionManager

    async with PaperExchange(
        symbols={"BTCUSDT": Decimal("50000")},
        tick_interval=0.05,
        seed=1,
        volatility_bps=0,
    ) as paper:
        factory = PaperPublicFactory(broker, paper)
        tracker = PublishTracker(broker, role=ROLE_MIRROR)
        sessions = SessionManager(
            factory,
            broker,
            tracker=tracker,
            stamp_coverage=False,
            recorder=TapeRecorder(broker, topics=("trade",), maxlen=50, retention_s=60),
        )
        feed = Topics.md_feed("orderbook", UniversalTicker.parse("Paper_Spot_BTCUSDT"))
        pinned = await sessions.pin_live_sessions(
            [("sts-live", [feed])], grace_s=0
        )
        assert feed in pinned
        assert sessions.feed_refcount(feed) == 1
        coverage = await broker.tape_coverage(feed)
        assert coverage.get("recording") != "1"
        await sessions.close_all()
