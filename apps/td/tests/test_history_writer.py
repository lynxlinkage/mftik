"""TD's history writer — recording trading without being able to disturb it.

Two properties matter more than the writes themselves. Attribution has to
survive: the submit is the only event that knows which session an order belongs
to, and everything after it comes from a venue that has never heard of one. And
the writer has to stay out of the way: a database that is full, slow or broken
must cost the account nothing but history it can re-read later.
"""

from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from decimal import Decimal

import pytest
from db_harness import a_database
from mft.exchange.models import Fill, Order, OrderStatus, OrderType, Side
from mft_db.models.history import Attribution, Source
from mft_db.repositories import FillRepository, OrderRepository
from mft_td.history import HistoryWriter

API_ID = 7
TICKER = "Binance_Spot_BTCUSDT"
CID = "281474976710656001"


@pytest.fixture
async def scope():
    """A ``session_scope``-alike; the writer opens a fresh session per flush."""
    async with a_database() as database:
        yield database.scope


@pytest.fixture
async def read(scope):
    """Read committed rows back, out of band of the writer."""

    async def _read(fn):
        async with scope() as db:
            return await fn(db)

    return _read


def an_order(**over) -> Order:
    fields = {
        "client_order_id": CID,
        "universal_ticker": TICKER,
        "side": Side.BUY,
        "type": OrderType.LIMIT,
        "status": OrderStatus.PENDING_NEW,
        "qty": Decimal("1"),
        "price": Decimal("100"),
        "ts": 1000.0,
    }
    fields.update(over)
    return Order(**fields)


def a_fill(**over) -> Fill:
    fields = {
        "fill_id": "987654",
        "order_id": "4293153",
        "client_order_id": CID,
        "universal_ticker": TICKER,
        "side": Side.BUY,
        "price": Decimal("100"),
        "qty": Decimal("0.3"),
        "fee": Decimal("0.001"),
        "fee_asset": "BNB",
        "ts": 1001.0,
    }
    fields.update(over)
    return Fill(**fields)


# --- attribution -----------------------------------------------------------


async def test_a_submitted_order_records_its_session(scope, read) -> None:
    writer = HistoryWriter(scope=scope)
    writer.record_order(
        an_order(), api_id=API_ID, session_id="sess-1", submitted_at=1000.0
    )
    await writer.flush()

    row = await read(lambda db: OrderRepository(db).get_by_key(API_ID, CID))
    assert row.session_id == "sess-1"
    assert row.attribution == Attribution.DIRECT
    assert row.submitted_at == 1000.0
    assert row.source == Source.STREAM


async def test_a_venue_update_cannot_erase_the_session(scope, read) -> None:
    """Every event after the submit comes from a venue with no idea of one."""
    writer = HistoryWriter(scope=scope)
    writer.record_order(
        an_order(), api_id=API_ID, session_id="sess-1", submitted_at=1000.0
    )
    await writer.flush()

    writer.record_order(
        an_order(
            order_id="4293153",
            status=OrderStatus.FILLED,
            filled_qty=Decimal("1"),
            avg_price=Decimal("100"),
            ts=1002.0,
        ),
        api_id=API_ID,
    )
    await writer.flush()

    row = await read(lambda db: OrderRepository(db).get_by_key(API_ID, CID))
    assert row.session_id == "sess-1"
    assert row.attribution == Attribution.DIRECT
    assert row.status == "filled"
    assert row.venue_order_id == "4293153"


async def test_an_order_nobody_claimed_is_recorded_as_external(scope, read) -> None:
    """Recon turns these up; they belong in the record, not in a session."""
    writer = HistoryWriter(scope=scope)
    writer.record_order(
        an_order(client_order_id=None, order_id="99", status=OrderStatus.NEW),
        api_id=API_ID,
    )
    await writer.flush()

    row = await read(lambda db: OrderRepository(db).get_by_key(API_ID, "99"))
    assert row.session_id is None
    assert row.attribution == Attribution.EXTERNAL
    assert row.client_order_id is None


async def test_a_fill_is_attributed_through_its_order(scope, read) -> None:
    """A venue trade row names an order, never a session — this is the join."""
    writer = HistoryWriter(scope=scope)
    writer.record_order(an_order(), api_id=API_ID, session_id="sess-1")
    writer.record_fill(a_fill(), api_id=API_ID)
    await writer.flush()

    fills = await read(
        lambda db: FillRepository(db).replay_for_session("sess-1")
    )
    assert len(fills) == 1
    assert fills[0].fill_id == "987654"
    assert fills[0].session_id == "sess-1"


async def test_a_fill_is_attributed_across_a_restart(scope, read) -> None:
    """The order is on file from a previous run; no in-memory map would have it."""
    first = HistoryWriter(scope=scope)
    first.record_order(an_order(), api_id=API_ID, session_id="sess-1")
    await first.flush()

    restarted = HistoryWriter(scope=scope)
    restarted.record_fill(a_fill(), api_id=API_ID)
    await restarted.flush()

    fills = await read(
        lambda db: FillRepository(db).replay_for_session("sess-1")
    )
    assert [f.session_id for f in fills] == ["sess-1"]


async def test_a_fill_with_no_order_on_file_stays_unattributed(scope, read) -> None:
    """A real state — placed elsewhere — not something to guess at."""
    writer = HistoryWriter(scope=scope)
    writer.record_fill(a_fill(client_order_id=None), api_id=API_ID)
    await writer.flush()

    rows = await read(
        lambda db: FillRepository(db).list_for_session("sess-1")
    )
    assert rows == []


async def test_one_accounts_order_does_not_attribute_anothers_fill(
    scope, read
) -> None:
    """Client order ids are only unique within an account."""
    writer = HistoryWriter(scope=scope)
    writer.record_order(an_order(), api_id=API_ID, session_id="sess-1")
    await writer.flush()

    writer.record_fill(a_fill(), api_id=99)
    await writer.flush()

    theirs = await read(
        lambda db: FillRepository(db).list_for_session("sess-1")
    )
    assert theirs == [], "api_id 99's fill must not land in api_id 7's session"


# --- batching --------------------------------------------------------------


async def test_repeat_states_of_one_order_collapse_within_a_batch(
    scope, read
) -> None:
    """Postgres refuses an upsert whose own values collide on the key.

    PENDING_NEW, NEW and PARTIALLY_FILLED for one order inside a single flush
    is the ordinary case, not an edge one.
    """
    writer = HistoryWriter(scope=scope)
    writer.record_order(
        an_order(), api_id=API_ID, session_id="sess-1", submitted_at=1000.0
    )
    writer.record_order(an_order(status=OrderStatus.NEW, ts=1001.0), api_id=API_ID)
    writer.record_order(
        an_order(
            status=OrderStatus.PARTIALLY_FILLED,
            # Binary-exact on purpose. These tests run on sqlite, which has no
            # decimal type and round-trips NUMERIC through a float, so 0.3
            # would come back as 0.299999999999999989 and this test would be
            # about that rather than about batching. Postgres, which is what
            # production writes to, returns the figure it was given.
            filled_qty=Decimal("0.25"),
            ts=1002.0,
        ),
        api_id=API_ID,
    )
    await writer.flush()

    row = await read(lambda db: OrderRepository(db).get_by_key(API_ID, CID))
    assert row.status == "partially_filled"
    assert row.filled_qty == Decimal("0.25")
    # The submit is the earliest row in the batch and the only one that knows
    # the session; collapsing to "latest wins" alone would drop it.
    assert row.session_id == "sess-1"
    assert row.submitted_at == 1000.0


async def test_a_redelivered_fill_does_not_cost_the_whole_batch(
    scope, read
) -> None:
    """A reconnect can replay executions the stream already delivered.

    Postgres will not let one ``ON CONFLICT`` command touch a row twice, so an
    uncollapsed duplicate fails the statement — and with it every other fill
    and order queued alongside it, which is far worse than the duplicate.
    """
    writer = HistoryWriter(scope=scope)
    writer.record_order(an_order(), api_id=API_ID, session_id="sess-1")
    writer.record_fill(a_fill(), api_id=API_ID)
    writer.record_fill(a_fill(), api_id=API_ID)
    writer.record_fill(a_fill(fill_id="987655"), api_id=API_ID)
    await writer.flush()

    fills = await read(
        lambda db: FillRepository(db).replay_for_session("sess-1")
    )
    assert [f.fill_id for f in fills] == ["987654", "987655"]


async def test_a_backlog_drains_without_waiting_out_the_interval(
    scope, read
) -> None:
    """Otherwise a burst is throttled to one batch per tick and then dropped."""
    writer = HistoryWriter(scope=scope, batch_size=2, flush_interval=3600.0)
    writer.record_order(an_order(), api_id=API_ID, session_id="sess-1")
    for i in range(9):
        writer.record_fill(a_fill(fill_id=str(i)), api_id=API_ID)

    await writer.start()
    for _ in range(50):
        await asyncio.sleep(0.01)
        if writer.pending == 0:
            break
    await writer.stop()

    fills = await read(
        lambda db: FillRepository(db).replay_for_session("sess-1")
    )
    assert len(fills) == 9, "all ten rows written despite a batch size of two"


async def test_an_out_of_order_pair_in_one_batch_keeps_the_newer_state(
    scope, read
) -> None:
    writer = HistoryWriter(scope=scope)
    writer.record_order(an_order(status=OrderStatus.FILLED, ts=1002.0), api_id=API_ID)
    writer.record_order(an_order(status=OrderStatus.NEW, ts=1001.0), api_id=API_ID)
    await writer.flush()

    row = await read(lambda db: OrderRepository(db).get_by_key(API_ID, CID))
    assert row.status == "filled"


# --- staying out of the way ------------------------------------------------


async def test_a_full_queue_drops_rather_than_blocking(scope) -> None:
    """An unbounded queue turns a database outage into an OOM in a process
    holding live positions. A dropped row is what the backfill is for."""
    writer = HistoryWriter(scope=scope, max_queue=100)
    for i in range(150):
        writer.record_fill(a_fill(fill_id=str(i)), api_id=API_ID)

    assert writer.pending == 100
    assert writer.dropped == 50


async def test_recording_never_raises_at_the_call_site(scope) -> None:
    """Called from the socket pump, where an exception costs the whole stream."""
    writer = HistoryWriter(scope=scope, max_queue=1)
    writer.record_fill(a_fill(), api_id=API_ID)
    writer.record_fill(a_fill(fill_id="2"), api_id=API_ID)
    writer.record_order(an_order(), api_id=API_ID)


async def test_a_failed_flush_does_not_kill_the_writer(scope, read) -> None:
    """One hole must not become every hole after it."""

    @asynccontextmanager
    async def broken():
        raise RuntimeError("database is down")
        yield  # pragma: no cover

    writer = HistoryWriter(scope=broken)
    writer.record_order(an_order(), api_id=API_ID, session_id="sess-1")
    await writer.flush()
    assert writer.written == 0

    writer._scope = scope
    writer.record_order(an_order(), api_id=API_ID, session_id="sess-1")
    await writer.flush()

    row = await read(lambda db: OrderRepository(db).get_by_key(API_ID, CID))
    assert row is not None, "the writer recovered on the next batch"


async def test_stop_drains_what_is_queued(scope, read) -> None:
    """The queue is the only copy of anything not yet written."""
    writer = HistoryWriter(scope=scope, flush_interval=3600.0)
    await writer.start()
    writer.record_order(an_order(), api_id=API_ID, session_id="sess-1")
    writer.record_fill(a_fill(), api_id=API_ID)
    await writer.stop()

    fills = await read(
        lambda db: FillRepository(db).replay_for_session("sess-1")
    )
    assert len(fills) == 1


async def test_the_flush_loop_writes_on_its_own_interval(scope, read) -> None:
    writer = HistoryWriter(scope=scope, flush_interval=0.05)
    await writer.start()
    writer.record_order(an_order(), api_id=API_ID, session_id="sess-1")
    for _ in range(50):
        await asyncio.sleep(0.02)
        if writer.written:
            break
    await writer.stop()

    row = await read(lambda db: OrderRepository(db).get_by_key(API_ID, CID))
    assert row is not None
