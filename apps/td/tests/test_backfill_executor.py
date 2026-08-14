"""The backfill walk, and the rule that makes its guarantee true.

Almost everything here is about the settlement line. A line that advances too
far is worse than one that does not advance at all: the first tells the
dashboard a window is settled when a row can still land in it, the second only
says "not yet". So the tests that matter are the ones about *not* confirming —
never past ``now - SAFETY_LAG``, never past what a page actually reached, and
never resuming by an id once caught up, because a venue's history can show a
trade after a walk has already read past its number.
"""

from __future__ import annotations

import time
from decimal import Decimal
from types import SimpleNamespace

import fakeredis.aioredis
import pytest
from db_harness import a_database
from mft.broker import Broker, BrokerConfig
from mft.exchange.models import Fill, Order, OrderStatus, OrderType, Side
from mft_db.models.history import Attribution, Source, Stream
from mft_db.repositories import (
    BackfillCursorRepository,
    FillRepository,
    OrderRepository,
)
from mft_td.backfill.executor import BackfillExecutor
from mft_td.backfill.reader import HistoryPage, NoHistoryReaderError
from mft_td.history import order_row

API_ID = 7
TICKER = "Binance_Spot_BTCUSDT"
LAG = 60.0


def ago(seconds: float) -> float:
    """A timestamp ``seconds`` before *now*, evaluated when the test runs.

    Not a module constant. The suite takes minutes end to end, and a timestamp
    fixed at import drifts out of the window the settlement line is being
    tested against — a "late arrival" placed 30s back is already older than the
    line by the time the test executes, and the walk correctly skips it.
    """
    return time.time() - seconds


@pytest.fixture
async def broker() -> Broker:
    redis = fakeredis.aioredis.FakeRedis(decode_responses=True)
    client = Broker(
        BrokerConfig(redis_url="redis://fake", key_prefix="test"),
        redis_client=redis,
    )
    await client.connect()
    yield client
    await client.close()
    await redis.aclose()


@pytest.fixture
async def scope(database_url):
    async with a_database(database_url) as database:
        yield database.scope


class FakeReader:
    """A venue whose history is a list, paged the way a real one pages it."""

    venue = "Binance"
    pages_newest_first = False
    max_page = 1000

    def __init__(
        self,
        *,
        trades: list[Fill] | None = None,
        orders: list[Order] | None = None,
        page: int = 100,
    ) -> None:
        self.trades = trades or []
        self.orders = orders or []
        self.page = page
        self.connected = False
        self.closed = False
        #: Every ``(stream, cursor, since_ts)`` this was asked for.
        self.calls: list[tuple[str, str | None, float | None]] = []

    async def connect(self) -> None:
        self.connected = True

    async def close(self) -> None:
        self.closed = True

    def _serve(self, rows, stream, cursor, since_ts, limit):
        self.calls.append((stream, cursor, since_ts))
        limit = min(limit, self.page)
        pool = sorted(rows, key=lambda r: int(_id_of(r)))
        if cursor is not None:
            pool = [r for r in pool if int(_id_of(r)) >= int(cursor)]
        elif since_ts is not None:
            pool = [r for r in pool if r.ts >= since_ts]
        served = pool[:limit]
        nxt = (
            str(max(int(_id_of(r)) for r in served) + 1)
            if len(served) >= limit and served
            else None
        )
        return HistoryPage(rows=served, next_cursor=nxt)

    async def fetch_my_trades(self, ticker, *, cursor=None, since_ts=None, limit=1000):
        return self._serve(self.trades, "trades", cursor, since_ts, limit)

    async def fetch_orders(self, ticker, *, cursor=None, since_ts=None, limit=1000):
        return self._serve(self.orders, "orders", cursor, since_ts, limit)


def _id_of(row):
    return row.fill_id if isinstance(row, Fill) else row.order_id


class FakeFactory:
    def __init__(self, reader=None, error: Exception | None = None) -> None:
        self.reader = reader
        self.error = error

    async def create(self, venue, row):
        if self.error is not None:
            raise self.error
        return self.reader


def a_trade(
    fill_id: str, *, ts: float, order_id: str = "500", cid: str | None = None
) -> Fill:
    """``cid`` is what Bybit and Gate echo back on the trade row itself."""
    return Fill(
        fill_id=fill_id,
        order_id=order_id,
        client_order_id=cid,
        universal_ticker=TICKER,
        side=Side.BUY,
        price=Decimal("100"),
        qty=Decimal("0.25"),
        fee=Decimal("0.001"),
        fee_asset="BNB",
        ts=ts,
    )


def an_order(order_id: str, *, ts: float, cid: str | None = None) -> Order:
    return Order(
        order_id=order_id,
        client_order_id=cid,
        universal_ticker=TICKER,
        side=Side.BUY,
        type=OrderType.LIMIT,
        status=OrderStatus.FILLED,
        qty=Decimal("0.25"),
        price=Decimal("100"),
        filled_qty=Decimal("0.25"),
        ts=ts,
    )


def executor(broker, scope, factory, **over) -> BackfillExecutor:
    kwargs = {
        "broker": broker,
        "factory": factory,
        "load_api": _api_row,
        "scope": scope,
        "safety_lag": LAG,
        "page_pause": 0.0,
    }
    kwargs.update(over)
    return BackfillExecutor(**kwargs)


async def _api_row(api_id: int):
    return SimpleNamespace(
        id=api_id, venue="Binance", api_key="k", api_secret="s", passphrase=None
    )


async def seed_order(
    scope,
    *,
    ts: float,
    cid: str = "cid-1",
    session="sess-1",
    key: str = "500",
    ticker: str = TICKER,
):
    """An order TD recorded live — what a walk bootstraps and attributes from."""
    order = an_order(key, ts=ts, cid=cid).model_copy(
        update={"universal_ticker": ticker}
    )
    async with scope() as db:
        await OrderRepository(db).bulk_upsert(
            [
                order_row(
                    order, api_id=API_ID, session_id=session, submitted_at=ts
                )
            ]
        )


async def cursor(scope, stream: str):
    async with scope() as db:
        return await BackfillCursorRepository(db).get(API_ID, stream, TICKER)


# --- the settlement line ---------------------------------------------------


async def test_a_drained_walk_confirms_up_to_the_safety_lag(broker, scope) -> None:
    """Not to now: a venue's history can lag its own stream."""
    await seed_order(scope, ts=ago(3600))
    reader = FakeReader(trades=[a_trade("1", ts=ago(1800))])
    before = time.time()

    outcome = await executor(broker, scope, FakeFactory(reader)).run(API_ID)

    assert outcome.ok
    row = await cursor(scope, Stream.TRADES)
    assert row.confirmed_through_ts <= time.time() - LAG
    assert row.confirmed_through_ts >= before - LAG, "and not short of it either"


async def test_a_caught_up_walk_stops_carrying_an_id(broker, scope) -> None:
    """So the tail window is re-read, and a late row cannot be skipped.

    A trade becomes visible on the venue *after* the walk has read past its
    number. Resuming by id would step over it forever; resuming by the line
    reads that window again.
    """
    await seed_order(scope, ts=ago(3600))
    reader = FakeReader(trades=[a_trade("1", ts=ago(1800))])
    ex = executor(broker, scope, FakeFactory(reader))
    await ex.run(API_ID)

    row = await cursor(scope, Stream.TRADES)
    assert row.last_id is None, "a caught-up walk resumes by time, not by id"

    # The late arrival lands behind the id already read, and inside the window
    # the line has not yet confirmed — which is the only window a late row can
    # be recovered from, and the whole reason SAFETY_LAG sizes one.
    reader.trades.append(a_trade("2", ts=ago(1)))
    await ex.run(API_ID)

    async with scope() as db:
        fills = await FillRepository(db).replay_for_session("sess-1")
    assert {f.fill_id for f in fills} == {"1", "2"}


async def test_a_walk_that_runs_out_of_pages_keeps_its_id(broker, scope) -> None:
    """Still catching up, so resume by id — there is no late-arrival hazard in
    history that is already old, and an id is what makes progress monotonic."""
    await seed_order(scope, ts=ago(7200))
    reader = FakeReader(
        trades=[a_trade(str(i), ts=ago(7000) + i) for i in range(1, 7)], page=2
    )

    await executor(broker, scope, FakeFactory(reader), max_pages=2).run(API_ID)

    row = await cursor(scope, Stream.TRADES)
    assert row.last_id is not None, "mid-catch-up walks resume by id"
    assert row.confirmed_through_ts < time.time() - LAG, (
        "and confirm only as far as the rows actually read"
    )


async def test_a_capped_walk_resumes_where_it_stopped(broker, scope) -> None:
    await seed_order(scope, ts=ago(7200))
    reader = FakeReader(
        trades=[a_trade(str(i), ts=ago(7000) + i) for i in range(1, 7)], page=2
    )
    ex = executor(broker, scope, FakeFactory(reader), max_pages=2)

    await ex.run(API_ID)
    await ex.run(API_ID)
    await ex.run(API_ID)

    async with scope() as db:
        fills = await FillRepository(db).replay_for_session("sess-1")
    assert len(fills) == 6, "successive runs finish the walk"


async def test_a_first_walk_starts_at_the_accounts_first_order(
    broker, scope
) -> None:
    """Not at the beginning of time: an instrument has no history worth
    reading before the first order this account placed on it."""
    first_order_ts = ago(3600)
    await seed_order(scope, ts=first_order_ts)
    reader = FakeReader(trades=[a_trade("1", ts=ago(1800))])

    await executor(broker, scope, FakeFactory(reader)).run(API_ID)

    trade_calls = [c for c in reader.calls if c[0] == "trades"]
    assert trade_calls[0][1] is None, "no id to resume from"
    assert trade_calls[0][2] == pytest.approx(first_order_ts)


# --- attribution -----------------------------------------------------------


async def test_a_backfilled_fill_is_attributed_through_its_order(
    broker, scope
) -> None:
    """A Binance trade row names an order id and no client order id."""
    await seed_order(scope, ts=ago(3600), cid="cid-1", session="sess-1")
    reader = FakeReader(trades=[a_trade("1", ts=ago(1800), order_id="500")])

    await executor(broker, scope, FakeFactory(reader)).run(API_ID)

    async with scope() as db:
        fills = await FillRepository(db).replay_for_session("sess-1")
    assert [f.fill_id for f in fills] == ["1"]
    assert fills[0].client_order_id == "cid-1"
    assert fills[0].source == Source.BACKFILL


async def test_a_fill_on_an_unknown_order_is_written_unattributed(
    broker, scope
) -> None:
    """A real state — placed elsewhere — not something to guess at."""
    await seed_order(scope, ts=ago(3600))
    reader = FakeReader(trades=[a_trade("9", ts=ago(1800), order_id="999")])

    await executor(broker, scope, FakeFactory(reader)).run(API_ID)

    async with scope() as db:
        rows = await FillRepository(db).list_for_session("sess-1")
    assert [r.fill_id for r in rows] == [], "not filed under somebody else"


async def test_our_own_id_on_the_trade_row_attributes_it(broker, scope) -> None:
    """Bybit and Gate echo the link id back, so the fill names its own owner.

    The order here has no venue id on file — a submit TD recorded before the
    ack came back — so the id join cannot see it at all. Ignoring what the
    venue handed us would file one of our own executions under nobody.
    """
    await seed_order(scope, ts=ago(3600), cid="cid-1", session="sess-1", key="")
    reader = FakeReader(
        trades=[a_trade("1", ts=ago(1800), order_id="500", cid="cid-1")]
    )

    await executor(broker, scope, FakeFactory(reader)).run(API_ID)

    async with scope() as db:
        fills = await FillRepository(db).replay_for_session("sess-1")
    assert [f.fill_id for f in fills] == ["1"]
    assert fills[0].client_order_id == "cid-1"


async def test_the_venues_own_id_wins_where_both_answer(broker, scope) -> None:
    """A slot wraps and is reused; a venue order id never is.

    So a trade carrying a client order id from far enough back can name an
    order a later session also used, and the id the venue itself assigned is
    the one to believe.
    """
    await seed_order(scope, ts=ago(3600), cid="cid-new", session="sess-new")
    await seed_order(
        scope, ts=ago(86400), cid="cid-old", session="sess-old", key="404"
    )
    reader = FakeReader(
        trades=[a_trade("1", ts=ago(1800), order_id="500", cid="cid-old")]
    )

    await executor(broker, scope, FakeFactory(reader)).run(API_ID)

    async with scope() as db:
        repo = FillRepository(db)
        assert [f.fill_id for f in await repo.replay_for_session("sess-new")] == ["1"]
        assert await repo.replay_for_session("sess-old") == []


async def test_a_link_id_we_never_placed_stays_unattributed(broker, scope) -> None:
    """``text`` is free-form on Gate: another tool may put anything there."""
    await seed_order(scope, ts=ago(3600))
    reader = FakeReader(
        trades=[a_trade("9", ts=ago(1800), order_id="999", cid="not-ours")]
    )

    await executor(broker, scope, FakeFactory(reader)).run(API_ID)

    async with scope() as db:
        (fill,) = await FillRepository(db).list_unattributed()
    assert fill.fill_id == "9"
    assert fill.client_order_id == "not-ours", "kept — it is what the venue said"


async def test_a_link_id_on_an_order_that_is_nobodys_stays_unattributed(
    broker, scope
) -> None:
    """An order on file with no session is an answer, not a missing one."""
    await seed_order(scope, ts=ago(3600), cid="by-hand", session=None, key="")
    reader = FakeReader(
        trades=[a_trade("9", ts=ago(1800), order_id="999", cid="by-hand")]
    )

    await executor(broker, scope, FakeFactory(reader)).run(API_ID)

    async with scope() as db:
        (fill,) = await FillRepository(db).list_unattributed()
    assert fill.fill_id == "9"


async def test_a_rediscovered_order_of_ours_keeps_its_session(
    broker, scope
) -> None:
    """``allOrders`` returns our orders knowing nothing about sessions."""
    await seed_order(scope, ts=ago(3600), cid="cid-1", session="sess-1")
    reader = FakeReader(orders=[an_order("500", ts=ago(1700), cid="cid-1")])

    await executor(broker, scope, FakeFactory(reader)).run(API_ID)

    async with scope() as db:
        row = await OrderRepository(db).get_by_key(API_ID, "cid-1")
    assert row.session_id == "sess-1"
    assert row.attribution == Attribution.DIRECT


async def test_an_order_placed_elsewhere_becomes_visible(broker, scope) -> None:
    """The account's manual and third-party activity, which nothing else sees."""
    await seed_order(scope, ts=ago(3600))
    reader = FakeReader(orders=[an_order("777", ts=ago(1700), cid="web_abc")])

    await executor(broker, scope, FakeFactory(reader)).run(API_ID)

    async with scope() as db:
        row = await OrderRepository(db).get_by_key(API_ID, "web_abc")
    assert row is not None
    assert row.session_id is None
    assert row.attribution == Attribution.EXTERNAL


async def test_orders_are_walked_before_trades(broker, scope) -> None:
    """Otherwise the fills walk has nothing to attribute against.

    The order here is discovered by the same run that discovers the fill, so
    only the ordering makes the join possible at all.
    """
    await seed_order(scope, ts=ago(3600))
    reader = FakeReader(
        trades=[a_trade("1", ts=ago(1800), order_id="777")],
        orders=[an_order("777", ts=ago(1700), cid="cid-9")],
    )

    await executor(broker, scope, FakeFactory(reader)).run(API_ID)

    assert [c[0] for c in reader.calls][0] == "orders"
    async with scope() as db:
        owners = await OrderRepository(db).by_venue_ids(API_ID, ["777"])
    assert owners["777"].client_order_id == "cid-9"


# --- refusals --------------------------------------------------------------


async def test_a_venue_with_no_reader_leaves_its_cursors_alone(
    broker, scope
) -> None:
    """The record goes on saying provisional, which is the truth."""
    await seed_order(scope, ts=ago(3600))
    factory = FakeFactory(error=NoHistoryReaderError("no reader for Paper"))

    outcome = await executor(broker, scope, factory).run(API_ID)

    assert outcome.ok, "not a failure — the venue simply cannot be re-read"
    assert outcome.tickers == []
    assert await cursor(scope, Stream.TRADES) is None


async def test_an_account_with_no_orders_has_nothing_to_walk(
    broker, scope
) -> None:
    reader = FakeReader(trades=[a_trade("1", ts=ago(0))])
    outcome = await executor(broker, scope, FakeFactory(reader)).run(API_ID)

    assert outcome.ok
    assert outcome.tickers == []
    assert reader.connected is False, "no venue was touched"


async def test_a_second_run_on_one_account_declines(broker, scope) -> None:
    """The rate-limit budget is shared with whatever is trading on the key."""
    await seed_order(scope, ts=ago(3600))
    ex = executor(broker, scope, FakeFactory(FakeReader()))
    await broker.redis.set(f"test:backfill:lock:{API_ID}", "someone-else")

    outcome = await ex.run(API_ID)

    assert outcome.ok
    assert outcome.skipped
    assert "another run" in outcome.reason


async def test_the_lock_is_released_for_the_next_run(broker, scope) -> None:
    await seed_order(scope, ts=ago(3600))
    ex = executor(broker, scope, FakeFactory(FakeReader()))

    await ex.run(API_ID)
    second = await ex.run(API_ID)

    assert not second.skipped


async def test_a_reader_that_throws_is_reported_not_raised(broker, scope) -> None:
    """A backfill is a bystander; it fails as data or not at all.

    Contained to the instrument it happened on, so the failure lands in
    ``failed`` rather than ending the run — a venue that refuses one symbol
    must not stop the account's others from settling, on this run or any
    later one.
    """
    await seed_order(scope, ts=ago(3600))

    class Broken(FakeReader):
        async def fetch_orders(self, *a, **kw):
            raise RuntimeError("venue said no")

    outcome = await executor(broker, scope, FakeFactory(Broken())).run(API_ID)

    assert outcome.ok, "one bad scope is not a failed run"
    assert outcome.tickers == []
    assert "venue said no" in outcome.failed[TICKER]


async def test_the_reader_is_closed_even_when_a_walk_fails(broker, scope) -> None:
    await seed_order(scope, ts=ago(3600))

    class Broken(FakeReader):
        async def fetch_orders(self, *a, **kw):
            raise RuntimeError("venue said no")

    reader = Broken()
    await executor(broker, scope, FakeFactory(reader)).run(API_ID)

    assert reader.closed, "a connection per run, and no run leaks one"


# --- venues that page backwards --------------------------------------------


class DescendingReader(FakeReader):
    """Bybit and Gate answer newest first. Verified against both.

    Pages the same list from the other end, so page one holds the newest rows
    and the unread pages hold the older ones — which is what makes a single
    ``confirmed_through_ts`` unable to describe the walk's real state.
    """

    venue = "Bybit"
    pages_newest_first = True

    def _serve(self, rows, stream, cursor, since_ts, limit):
        self.calls.append((stream, cursor, since_ts))
        limit = min(limit, self.page)
        pool = sorted(rows, key=lambda r: int(_id_of(r)), reverse=True)
        if cursor is not None:
            pool = [r for r in pool if int(_id_of(r)) <= int(cursor)]
        elif since_ts is not None:
            pool = [r for r in pool if r.ts >= since_ts]
        served = pool[:limit]
        nxt = (
            str(min(int(_id_of(r)) for r in served) - 1)
            if len(served) >= limit and served
            else None
        )
        return HistoryPage(rows=served, next_cursor=nxt)


async def test_a_backwards_walk_confirms_nothing_until_it_drains(
    broker, scope
) -> None:
    """The failure this whole design says it must not have.

    Page one is the *newest* rows, so its latest timestamp is roughly now.
    Advancing the line to it would mark the account's entire history settled
    while the pages holding most of that history had never been opened — every
    execution in them missing, and the board reporting the run as final.
    """
    await seed_order(scope, ts=ago(7200))
    reader = DescendingReader(
        trades=[a_trade(str(i), ts=ago(7000) + i) for i in range(1, 7)], page=2
    )

    await executor(broker, scope, FakeFactory(reader), max_pages=2).run(API_ID)

    row = await cursor(scope, Stream.TRADES)
    assert row.confirmed_through_ts == 0.0, "nothing may be claimed yet"
    assert row.last_id is not None, "but the walk must know where to carry on"


async def test_a_backwards_walk_confirms_once_it_reaches_the_end(
    broker, scope
) -> None:
    """Draining is what makes the interval whole, whichever way it was read."""
    await seed_order(scope, ts=ago(7200))
    reader = DescendingReader(
        trades=[a_trade(str(i), ts=ago(7000) + i) for i in range(1, 7)], page=2
    )
    ex = executor(broker, scope, FakeFactory(reader), max_pages=10)

    await ex.run(API_ID)

    row = await cursor(scope, Stream.TRADES)
    assert row.confirmed_through_ts > 0
    assert row.last_id is None
    async with scope() as db:
        assert len(await FillRepository(db).replay_for_session("sess-1")) == 6


async def test_a_backwards_walk_still_makes_progress_across_runs(
    broker, scope
) -> None:
    """It keeps its place even though it confirms nothing."""
    await seed_order(scope, ts=ago(7200))
    reader = DescendingReader(
        trades=[a_trade(str(i), ts=ago(7000) + i) for i in range(1, 7)], page=2
    )
    ex = executor(broker, scope, FakeFactory(reader), max_pages=1)

    for _ in range(6):
        await ex.run(API_ID)

    async with scope() as db:
        fills = await FillRepository(db).replay_for_session("sess-1")
    assert len(fills) == 6, "successive runs walk backwards to the beginning"


# --- one bad instrument ----------------------------------------------------


async def test_one_unwalkable_instrument_does_not_cost_the_others(
    broker, scope
) -> None:
    """These failures are deterministic, so an unguarded loop stalls forever.

    ``tickers_for`` returns them sorted, so the first bad scope would take
    every instrument after it in the alphabet with it — on this run and on
    every run after.
    """
    await seed_order(
        scope, ts=ago(3600), cid="cid-a", ticker="Binance_Spot_AAAUSDT"
    )
    await seed_order(scope, ts=ago(3600), cid="cid-z", ticker=TICKER)

    class Picky(FakeReader):
        async def fetch_orders(self, ticker, **kw):
            if "AAAUSDT" in str(ticker):
                raise RuntimeError("delisted")
            return await super().fetch_orders(ticker, **kw)

    reader = Picky(trades=[a_trade("1", ts=ago(1800))])
    outcome = await executor(broker, scope, FakeFactory(reader)).run(API_ID)

    assert outcome.ok, "a bad scope is not a failed run"
    assert outcome.tickers == [TICKER], "the good one was still walked"
    assert "Binance_Spot_AAAUSDT" in outcome.failed


# --- attribution -----------------------------------------------------------


async def test_a_venue_supplied_link_id_is_not_overwritten(broker, scope) -> None:
    """Bybit and Gate put our link id on the trade row itself.

    The matched order can carry a null one — discovered by an earlier backfill,
    say — and assigning unconditionally would throw the real id away on its way
    to a first insert, where the upsert's coalesce cannot save it.
    """
    async with scope() as db:
        await OrderRepository(db).bulk_upsert(
            [
                order_row(
                    an_order("500", ts=ago(3600), cid=None),
                    api_id=API_ID,
                    session_id="sess-1",
                )
            ]
        )
    reader = FakeReader(trades=[a_trade("1", ts=ago(1800), order_id="500")])
    reader.trades[0] = reader.trades[0].model_copy(
        update={"client_order_id": "from-the-venue"}
    )

    await executor(broker, scope, FakeFactory(reader)).run(API_ID)

    async with scope() as db:
        fills = await FillRepository(db).replay_for_session("sess-1")
    assert fills[0].client_order_id == "from-the-venue"
