"""MD attach / lease / feed refcount / dispatcher tests."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from datetime import UTC, datetime
from decimal import Decimal
from types import SimpleNamespace

import fakeredis.aioredis
import pytest
from mft.broker import Broker, BrokerConfig
from mft.exchange import PaperExchange
from mft.protocol import (
    MD_LEASE_ACK,
    MD_ORDERBOOK,
    MD_SESSION_ATTACH,
    STS_LEASE_HEARTBEAT,
    Envelope,
    LeaseHeartbeat,
    MdAttachRequest,
    MdAttachRequestEnvelope,
    MdAttachResult,
    MdLeaseAck,
    Topics,
)
from mft_md.rpc import dispatch
from mft_md.session import PaperPublicFactory, SessionManager
from mft_sts.strategy import Strategy


@dataclass
class FakeMdStore:
    rows: dict[tuple[str, str], SimpleNamespace] = field(default_factory=dict)

    async def persist_live(
        self,
        *,
        session_id: str,
        created_by: int,
        venues: list[str] | None = None,
    ) -> list[SimpleNamespace]:
        out: list[SimpleNamespace] = []
        for venue in venues or []:
            key = (venue, session_id)
            existing = self.rows.get(key)
            if existing is not None and existing.status == "live":
                out.append(existing)
                continue
            row = SimpleNamespace(
                venue=venue,
                session_id=session_id,
                created_by=created_by,
                created_at=datetime.now(UTC),
                finished_at=None,
                status="live",
            )
            self.rows[key] = row
            out.append(row)
        return out

    async def mark_done(self, *, session_id: str) -> list[SimpleNamespace]:
        done: list[SimpleNamespace] = []
        for key, row in self.rows.items():
            if row.session_id != session_id or row.status != "live":
                continue
            row.status = "done"
            row.finished_at = datetime.now(UTC)
            done.append(row)
        return done

    async def list_sessions(
        self,
        *,
        status: str | None = "live",
        created_by: int | None = None,
    ) -> list[SimpleNamespace]:
        out = []
        for row in self.rows.values():
            if status is not None and row.status != status:
                continue
            if created_by is not None and row.created_by != created_by:
                continue
            out.append(row)
        return out


async def _md_lease_publisher(
    broker: Broker, session_id: str, stop: asyncio.Event, *, interval: float = 0.1
) -> None:
    token = 0
    topic = Topics.sts_md_session(session_id)
    while not stop.is_set():
        token += 1
        await broker.publish(
            topic,
            Envelope[LeaseHeartbeat].wrap(
                LeaseHeartbeat(session_id=session_id, token=token),
                type=STS_LEASE_HEARTBEAT,
                source="sts",
                session_id=session_id,
            ),
        )
        try:
            await asyncio.wait_for(stop.wait(), timeout=interval)
        except TimeoutError:
            continue


@pytest.fixture
async def broker() -> Broker:
    redis = fakeredis.aioredis.FakeRedis(decode_responses=True)
    client = Broker(
        BrokerConfig(redis_url="redis://fake", key_prefix="test-md"),
        redis_client=redis,
    )
    await client.connect()
    yield client
    await client.close()
    await redis.aclose()


@pytest.fixture
async def paper() -> PaperExchange:
    async with PaperExchange(
        symbols={"BTCUSDT": Decimal("50000")},
        tick_interval=0.05,
        seed=1,
        volatility_bps=0,
    ) as ex:
        yield ex


@pytest.mark.asyncio
async def test_md_attach_lease_and_orderbook(
    broker: Broker, paper: PaperExchange
) -> None:
    store = FakeMdStore()
    factory = PaperPublicFactory(broker, paper)
    sessions = SessionManager(
        factory,
        broker,
        persist_live=store.persist_live,
        mark_done=store.mark_done,
        list_db_sessions=store.list_sessions,
        lease_grace=2.0,
    )
    session_id = "sts-md-1"
    feed = Topics.md_feed("paper", "orderbook", "BTCUSDT")
    stop = asyncio.Event()
    hb_task = asyncio.create_task(_md_lease_publisher(broker, session_id, stop))

    books: list[dict] = []
    acks: list[int] = []

    async def _collect() -> None:
        async for env in broker.subscribe(
            Topics.md_session(session_id), stop=stop
        ):
            if env.type == MD_LEASE_ACK:
                ack = MdLeaseAck.model_validate(env.payload)
                acks.append(ack.token)
            elif env.type == MD_ORDERBOOK:
                books.append(env.payload)

    collect_task = asyncio.create_task(_collect())
    await asyncio.sleep(0.05)

    result = await sessions.attach(
        MdAttachRequest(
            session_id=session_id,
            created_by=1,
            subscriptions=[feed],
            timeout=3.0,
        )
    )
    assert result.session_id == session_id
    assert feed in result.subscriptions
    assert result.refcounts[feed] == 1
    assert sessions.feed_refcount(feed) == 1

    await asyncio.wait_for(_wait_until(lambda: len(acks) >= 1), timeout=3.0)
    await asyncio.wait_for(_wait_until(lambda: len(books) >= 1), timeout=3.0)
    assert books[0]["symbol"] == "BTCUSDT"

    stop.set()
    await sessions.detach(session_id=session_id, reason="test")
    assert sessions.feed_refcount(feed) == 0
    await asyncio.gather(hb_task, collect_task, return_exceptions=True)
    await sessions.close_all()


@pytest.mark.asyncio
async def test_md_feed_refcount_shared(
    broker: Broker, paper: PaperExchange
) -> None:
    store = FakeMdStore()
    factory = PaperPublicFactory(broker, paper)
    sessions = SessionManager(
        factory,
        broker,
        persist_live=store.persist_live,
        mark_done=store.mark_done,
        lease_grace=2.0,
    )
    feed = Topics.md_feed("paper", "orderbook", "BTCUSDT")
    stop = asyncio.Event()
    s1, s2 = "sts-a", "sts-b"
    hb_tasks = [
        asyncio.create_task(_md_lease_publisher(broker, s1, stop)),
        asyncio.create_task(_md_lease_publisher(broker, s2, stop)),
    ]
    await asyncio.sleep(0.05)

    await sessions.attach(
        MdAttachRequest(
            session_id=s1, created_by=1, subscriptions=[feed], timeout=3.0
        )
    )
    await sessions.attach(
        MdAttachRequest(
            session_id=s2, created_by=1, subscriptions=[feed], timeout=3.0
        )
    )
    assert sessions.feed_refcount(feed) == 2
    assert sessions.dispatcher.refcount("paper", "orderbook", "BTCUSDT") == 2

    await sessions.detach(session_id=s1, reason="test")
    assert sessions.feed_refcount(feed) == 1

    await sessions.detach(session_id=s2, reason="test")
    assert sessions.feed_refcount(feed) == 0
    assert "paper" not in sessions._venues  # noqa: SLF001

    stop.set()
    await asyncio.gather(*hb_tasks, return_exceptions=True)
    await sessions.close_all()


@pytest.mark.asyncio
async def test_md_rpc_attach(broker: Broker, paper: PaperExchange) -> None:
    store = FakeMdStore()
    factory = PaperPublicFactory(broker, paper)
    sessions = SessionManager(
        factory,
        broker,
        persist_live=store.persist_live,
        mark_done=store.mark_done,
        lease_grace=2.0,
    )
    session_id = "sts-rpc"
    feed = "paper.orderbook.BTCUSDT"
    stop = asyncio.Event()
    hb_task = asyncio.create_task(_md_lease_publisher(broker, session_id, stop))
    rpc_stop = asyncio.Event()

    async def _rpc() -> None:
        async for req in broker.serve(Topics.MD, stop=rpc_stop):
            await dispatch(req, sessions=sessions)

    rpc_task = asyncio.create_task(_rpc())
    await asyncio.sleep(0.05)

    reply = await broker.request(
        Topics.MD,
        MdAttachRequestEnvelope.wrap(
            MdAttachRequest(
                session_id=session_id,
                created_by=7,
                subscriptions=[feed],
                timeout=3.0,
            ),
            type=MD_SESSION_ATTACH,
            source="api",
            session_id=session_id,
        ),
        timeout=5.0,
    )
    assert reply.type == MD_SESSION_ATTACH
    result = MdAttachResult.model_validate(reply.payload)
    assert result.subscriptions == [feed]
    assert result.refcounts[feed] == 1

    stop.set()
    rpc_stop.set()
    await sessions.close_all()
    await asyncio.gather(hb_task, rpc_task, return_exceptions=True)


@pytest.mark.asyncio
async def test_sts_on_order_book_from_md(
    broker: Broker, paper: PaperExchange
) -> None:
    """STS session pump delivers MD orderbook into strategy hook."""
    from mft_sts.session.session import StsSession

    class BookStrategy(Strategy):
        name = "book"
        id = 99

        def __init__(self) -> None:
            super().__init__()
            self.books: list = []

        async def on_order_book(self, book) -> None:  # noqa: ANN001
            self.books.append(book)

    store = FakeMdStore()
    factory = PaperPublicFactory(broker, paper)
    md = SessionManager(
        factory,
        broker,
        persist_live=store.persist_live,
        mark_done=store.mark_done,
        lease_grace=2.0,
    )
    feed = "paper.orderbook.BTCUSDT"
    session_id = "sts-book"
    strategy = BookStrategy()
    sts = StsSession(
        session_id=session_id,
        broker=broker,
        created_by=1,
        strategy=strategy,
        md_ids=[feed],
        heartbeat_interval=0.1,
    )
    await sts.start()
    await asyncio.sleep(0.05)
    await md.attach(
        MdAttachRequest(
            session_id=session_id,
            created_by=1,
            subscriptions=[feed],
            timeout=3.0,
        )
    )
    await asyncio.wait_for(
        _wait_until(lambda: len(strategy.books) >= 1), timeout=3.0
    )
    assert strategy.books[0].symbol == "BTCUSDT"

    await sts.stop()
    await md.close_all()


@pytest.mark.asyncio
async def test_sts_ticker_and_order_book_from_md(
    broker: Broker, paper: PaperExchange
) -> None:
    """Two feed topics on one session land in their own strategy hooks."""
    from mft_sts.session.session import StsSession

    class FeedStrategy(Strategy):
        name = "feeds"
        id = 97

        def __init__(self) -> None:
            super().__init__()
            self.books: list = []
            self.tickers: list = []

        async def on_order_book(self, book) -> None:  # noqa: ANN001
            self.books.append(book)

        async def on_ticker(self, ticker) -> None:  # noqa: ANN001
            self.tickers.append(ticker)

    store = FakeMdStore()
    factory = PaperPublicFactory(broker, paper)
    md = SessionManager(
        factory,
        broker,
        persist_live=store.persist_live,
        mark_done=store.mark_done,
        lease_grace=2.0,
    )
    feeds = ["paper.orderbook.BTCUSDT", "paper.ticker.BTCUSDT"]
    session_id = "sts-feeds"
    strategy = FeedStrategy()
    sts = StsSession(
        session_id=session_id,
        broker=broker,
        created_by=1,
        strategy=strategy,
        md_ids=feeds,
        heartbeat_interval=0.1,
    )
    await sts.start()
    await asyncio.sleep(0.05)
    result = await md.attach(
        MdAttachRequest(
            session_id=session_id,
            created_by=1,
            subscriptions=feeds,
            timeout=3.0,
        )
    )
    assert sorted(result.subscriptions) == sorted(feeds)

    await asyncio.wait_for(
        _wait_until(
            lambda: bool(strategy.books) and bool(strategy.tickers)
        ),
        timeout=3.0,
    )
    assert strategy.books[0].symbol == "BTCUSDT"
    assert strategy.tickers[0].symbol == "BTCUSDT"
    assert strategy.books[0].bids
    assert strategy.tickers[0].last is not None

    await sts.stop()
    await md.close_all()


async def _wait_until(pred, *, timeout: float = 3.0) -> None:  # noqa: ANN001
    deadline = asyncio.get_running_loop().time() + timeout
    while asyncio.get_running_loop().time() < deadline:
        if pred():
            return
        await asyncio.sleep(0.05)
    raise TimeoutError("condition not met")
