"""Trading history persistence — mostly about what a second write may destroy.

The stream and the backfill describe the same executions and arrive in either
order, each knowing something the other does not. Almost every test here pins
one merge rule, because a wrong one does not fail loudly: it silently detaches
a fill from its session, or resurrects a stale fee, and the first sign of it is
a PnL figure nobody can reconcile.
"""

from __future__ import annotations

from decimal import Decimal

import pytest
from mft_db.models import Base
from mft_db.models.history import Attribution, Source, Stream
from mft_db.repositories import (
    BackfillCursorRepository,
    CashFlowRepository,
    FillRepository,
    OrderRepository,
)
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine


@pytest.fixture
async def db():
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    maker = async_sessionmaker(engine, expire_on_commit=False)
    async with maker() as session:
        yield session
    await engine.dispose()


def order_row(**over) -> dict:
    """An order as the submit path writes it: ours, attributed, no venue id."""
    row = {
        "api_id": 1,
        "order_key": "281474976710656001",
        "client_order_id": "281474976710656001",
        "venue_order_id": None,
        "session_id": "sess-1",
        "strategy": "twap",
        "cid_slot": 4,
        "attribution": Attribution.DIRECT,
        "universal_ticker": "Binance_Spot_BTCUSDT",
        "side": "buy",
        "order_type": "limit",
        "status": "new",
        "qty": Decimal("1"),
        "price": Decimal("100"),
        "filled_qty": Decimal("0"),
        "avg_price": None,
        "submitted_at": 1000.0,
        "ts": 1000.0,
        "source": Source.STREAM,
    }
    row.update(over)
    return row


def fill_row(**over) -> dict:
    row = {
        "api_id": 1,
        "fill_id": "987654",
        "universal_ticker": "Binance_Spot_BTCUSDT",
        "venue_order_id": "4293153",
        "client_order_id": "281474976710656001",
        "session_id": "sess-1",
        "side": "buy",
        "price": Decimal("100"),
        "qty": Decimal("0.3"),
        "fee": Decimal("0.001"),
        "fee_asset": "BNB",
        "realized_pnl": None,
        "is_maker": None,
        "ts": 1001.0,
        "source": Source.STREAM,
    }
    row.update(over)
    return row


# --- orders ----------------------------------------------------------------


async def test_an_order_is_one_row_however_many_updates_it_gets(db) -> None:
    repo = OrderRepository(db)
    await repo.bulk_upsert([order_row()])
    await repo.bulk_upsert(
        [
            order_row(
                venue_order_id="4293153",
                status="filled",
                filled_qty=Decimal("1"),
                avg_price=Decimal("100"),
                ts=1002.0,
            )
        ]
    )
    await db.commit()

    row = await repo.get_by_key(1, "281474976710656001")
    assert row is not None
    assert row.status == "filled"
    assert row.venue_order_id == "4293153"
    assert row.filled_qty == Decimal("1")


async def test_a_late_update_cannot_reopen_a_terminal_order(db) -> None:
    """Out-of-order arrival is normal; rolling the state back is not."""
    repo = OrderRepository(db)
    await repo.bulk_upsert([order_row(status="filled", ts=1002.0)])
    await repo.bulk_upsert([order_row(status="new", ts=1000.0)])
    await db.commit()

    row = await repo.get_by_key(1, "281474976710656001")
    assert row.status == "filled"
    assert row.ts == 1002.0


async def test_a_backfill_cannot_disown_an_order_we_placed(db) -> None:
    """``allOrders`` returns our orders too, knowing nothing about sessions.

    The rediscovered row carries no session and would file the order as
    somebody else's, which is the one way a live strategy's PnL could quietly
    empty out.
    """
    repo = OrderRepository(db)
    await repo.bulk_upsert([order_row()])
    await repo.bulk_upsert(
        [
            order_row(
                session_id=None,
                strategy=None,
                cid_slot=None,
                attribution=Attribution.EXTERNAL,
                submitted_at=None,
                venue_order_id="4293153",
                status="filled",
                ts=1002.0,
                source=Source.BACKFILL,
            )
        ]
    )
    await db.commit()

    row = await repo.get_by_key(1, "281474976710656001")
    assert row.session_id == "sess-1"
    assert row.strategy == "twap"
    assert row.cid_slot == 4
    assert row.attribution == Attribution.DIRECT
    assert row.submitted_at == 1000.0, "we were there; the history read was not"
    # The state it did know about still lands.
    assert row.status == "filled"
    assert row.source == Source.BACKFILL


async def test_an_order_discovered_by_backfill_first_is_still_attributable(db) -> None:
    """Backfill wrote it as external; the stream then proves it was ours."""
    repo = OrderRepository(db)
    await repo.bulk_upsert(
        [
            order_row(
                session_id=None,
                strategy=None,
                cid_slot=None,
                attribution=Attribution.EXTERNAL,
                submitted_at=None,
                source=Source.BACKFILL,
            )
        ]
    )
    await repo.bulk_upsert([order_row(ts=1003.0)])
    await db.commit()

    row = await repo.get_by_key(1, "281474976710656001")
    assert row.session_id == "sess-1"
    assert row.attribution == Attribution.DIRECT


async def test_an_order_with_no_client_order_id_keys_on_the_venues(db) -> None:
    """Some venues let an order be placed without one; it still needs a row."""
    repo = OrderRepository(db)
    await repo.bulk_upsert(
        [
            order_row(
                order_key="4293153",
                client_order_id=None,
                venue_order_id="4293153",
                session_id=None,
                strategy=None,
                cid_slot=None,
                attribution=Attribution.EXTERNAL,
            )
        ]
    )
    await db.commit()

    row = await repo.get_by_key(1, "4293153")
    assert row is not None
    assert row.client_order_id is None
    assert row.attribution == Attribution.EXTERNAL


async def test_a_walk_bootstraps_from_the_submit_not_the_last_update(db) -> None:
    """The window between them is where a first walk would lose executions.

    ``ts`` is pushed forward by every update, so an order placed at T and
    filled at T+5 carries T+5. Anchoring a first walk there would skip the
    fills in between — on the one run that establishes the settlement line,
    which nothing afterwards revisits.
    """
    repo = OrderRepository(db)
    await repo.bulk_upsert([order_row(submitted_at=1000.0, ts=1000.0)])
    await repo.bulk_upsert([order_row(status="filled", ts=1005.0)])
    await db.commit()

    assert await repo.earliest_ts(1, "Binance_Spot_BTCUSDT") == 1000.0


async def test_bootstrapping_falls_back_for_an_order_we_never_placed(db) -> None:
    """A backfilled order has no submit time of ours to anchor on."""
    repo = OrderRepository(db)
    await repo.bulk_upsert(
        [
            order_row(
                order_key="ext-1",
                client_order_id=None,
                session_id=None,
                attribution=Attribution.EXTERNAL,
                submitted_at=None,
                ts=900.0,
            )
        ]
    )
    await db.commit()

    assert await repo.earliest_ts(1, "Binance_Spot_BTCUSDT") == 900.0


async def test_an_instrument_never_traded_has_no_anchor(db) -> None:
    assert await OrderRepository(db).earliest_ts(1, "Binance_Spot_ETHUSDT") is None


async def test_two_accounts_may_mint_the_same_client_order_id(db) -> None:
    repo = OrderRepository(db)
    await repo.bulk_upsert([order_row(), order_row(api_id=2, session_id="sess-2")])
    await db.commit()

    assert (await repo.get_by_key(1, "281474976710656001")).session_id == "sess-1"
    assert (await repo.get_by_key(2, "281474976710656001")).session_id == "sess-2"


# --- fills -----------------------------------------------------------------


async def test_the_same_execution_from_both_sources_is_one_fill(db) -> None:
    """The invariant the two-tier record rests on, at the storage end."""
    repo = FillRepository(db)
    await repo.bulk_upsert([fill_row()])
    await repo.bulk_upsert(
        [
            fill_row(
                client_order_id=None,
                session_id=None,
                fee=Decimal("0.0008"),
                is_maker=True,
                source=Source.BACKFILL,
            )
        ]
    )
    await db.commit()

    rows = await repo.replay_for_session("sess-1")
    assert len(rows) == 1, "one execution, one row"
    assert rows[0].session_id == "sess-1", "backfill must not orphan it"
    assert rows[0].client_order_id == "281474976710656001"
    assert rows[0].fee == Decimal("0.0008"), "the settled fee wins"
    assert rows[0].is_maker is True
    assert rows[0].source == Source.BACKFILL


async def test_a_streamed_fee_never_overwrites_a_settled_one(db) -> None:
    """Arrival order must not decide which fee is right."""
    repo = FillRepository(db)
    await repo.bulk_upsert([fill_row(fee=Decimal("0.0008"), source=Source.BACKFILL)])
    await repo.bulk_upsert([fill_row(fee=Decimal("0.001"), source=Source.STREAM)])
    await db.commit()

    rows = await repo.replay_for_session("sess-1")
    assert rows[0].fee == Decimal("0.0008")
    assert rows[0].source == Source.BACKFILL


async def test_the_same_trade_id_on_two_books_is_two_fills(db) -> None:
    """Binance numbers trades per symbol; the key carries the instrument."""
    repo = FillRepository(db)
    await repo.bulk_upsert(
        [
            fill_row(),
            fill_row(universal_ticker="Binance_Spot_ETHUSDT"),
        ]
    )
    await db.commit()

    assert len(await repo.replay_for_session("sess-1")) == 2


async def test_a_fill_that_beat_its_order_is_attributed_afterwards(db) -> None:
    repo = FillRepository(db)
    await repo.bulk_upsert([fill_row(client_order_id=None, session_id=None)])
    await db.commit()

    touched = await repo.attribute(
        1, "4293153", session_id="sess-1", client_order_id="281474976710656001"
    )
    await db.commit()

    assert touched == 1
    rows = await repo.replay_for_session("sess-1")
    assert rows[0].client_order_id == "281474976710656001"


async def test_attributing_does_not_reassign_a_fill_that_already_has_a_session(
    db,
) -> None:
    """The submit-time attribution knows more than any later pass."""
    repo = FillRepository(db)
    await repo.bulk_upsert([fill_row()])
    await db.commit()

    touched = await repo.attribute(
        1, "4293153", session_id="sess-WRONG", client_order_id="999"
    )
    await db.commit()

    assert touched == 0
    assert (await repo.replay_for_session("sess-1"))[0].session_id == "sess-1"


async def test_a_replay_runs_oldest_first(db) -> None:
    """Cost matching starts at the first execution or it starts nowhere."""
    repo = FillRepository(db)
    await repo.bulk_upsert(
        [
            fill_row(fill_id="3", ts=1003.0),
            fill_row(fill_id="1", ts=1001.0),
            fill_row(fill_id="2", ts=1002.0),
        ]
    )
    await db.commit()

    assert [r.fill_id for r in await repo.replay_for_session("sess-1")] == [
        "1",
        "2",
        "3",
    ]


async def test_fills_list_newest_first_with_a_cursor(db) -> None:
    repo = FillRepository(db)
    await repo.bulk_upsert(
        [fill_row(fill_id=str(i), ts=1000.0 + i) for i in range(1, 5)]
    )
    await db.commit()

    newest = await repo.list_for_session("sess-1", limit=2)
    assert [r.fill_id for r in newest] == ["4", "3"]

    page = await repo.list_for_session(
        "sess-1", before_ts=newest[-1].ts, before_id=newest[-1].id, limit=2
    )
    assert [r.fill_id for r in page] == ["2", "1"]


# --- cash flows ------------------------------------------------------------


async def test_a_cash_movement_is_recorded_once(db) -> None:
    repo = CashFlowRepository(db)
    row = {
        "api_id": 1,
        "venue_id": "9689322392",
        "kind": "funding",
        "universal_ticker": "BinanceFuture_Perp_BTCUSDT",
        "asset": "USDT",
        "amount": Decimal("-0.375"),
        "ts": 1570636800.0,
        "source": Source.BACKFILL,
    }
    await repo.bulk_insert_ignore([row])
    await repo.bulk_insert_ignore([row])
    await db.commit()

    rows = await repo.list_for_api(1)
    assert len(rows) == 1
    assert rows[0].amount == Decimal("-0.375")


async def test_one_transaction_id_may_cover_two_kinds(db) -> None:
    """Which is why ``kind`` is part of the key and not just a label."""
    repo = CashFlowRepository(db)
    base = {
        "api_id": 1,
        "venue_id": "9689322392",
        "asset": "USDT",
        "ts": 1570636800.0,
        "source": Source.BACKFILL,
    }
    await repo.bulk_insert_ignore(
        [
            {**base, "kind": "transfer", "amount": Decimal("100")},
            {**base, "kind": "commission", "amount": Decimal("-0.05")},
        ]
    )
    await db.commit()

    assert len(await repo.list_for_api(1)) == 2
    assert len(await repo.list_for_api(1, kind="transfer")) == 1


# --- cursors ---------------------------------------------------------------


async def test_a_cursor_advances(db) -> None:
    repo = BackfillCursorRepository(db)
    await repo.advance(
        1, Stream.TRADES, scope="Binance_Spot_BTCUSDT",
        confirmed_through_ts=1000.0, last_id="500",
    )
    await repo.advance(
        1, Stream.TRADES, scope="Binance_Spot_BTCUSDT",
        confirmed_through_ts=2000.0, last_id="900",
    )
    await db.commit()

    row = await repo.get(1, Stream.TRADES, "Binance_Spot_BTCUSDT")
    assert row.confirmed_through_ts == 2000.0
    assert row.last_id == "900"


async def test_a_cursor_never_moves_backwards(db) -> None:
    """Two workers racing must not let the slower one un-settle a window."""
    repo = BackfillCursorRepository(db)
    await repo.advance(
        1, Stream.TRADES, scope="X", confirmed_through_ts=2000.0, last_id="900"
    )
    await repo.advance(
        1, Stream.TRADES, scope="X", confirmed_through_ts=1000.0, last_id="500"
    )
    await db.commit()

    row = await repo.get(1, Stream.TRADES, "X")
    assert row.confirmed_through_ts == 2000.0
    assert row.last_id == "900", "the stale page must not reset the resume point"


async def test_an_account_wide_walk_shares_the_table_with_per_symbol_ones(db) -> None:
    """Cash flows are not per-symbol; trades and orders are."""
    repo = BackfillCursorRepository(db)
    await repo.advance(1, Stream.TRADES, scope="X", confirmed_through_ts=1000.0)
    await repo.advance(1, Stream.CASH_FLOWS, confirmed_through_ts=3000.0)
    await db.commit()

    assert (await repo.get(1, Stream.CASH_FLOWS)).confirmed_through_ts == 3000.0
    assert (await repo.get(1, Stream.TRADES, "X")).confirmed_through_ts == 1000.0


async def test_the_settlement_line_is_the_weakest_of_the_walks_it_needs(db) -> None:
    repo = BackfillCursorRepository(db)
    await repo.advance(1, Stream.TRADES, scope="X", confirmed_through_ts=2000.0)
    await repo.advance(1, Stream.ORDERS, scope="X", confirmed_through_ts=1500.0)
    await db.commit()

    line = await repo.confirmed_through(
        1, [Stream.TRADES, Stream.ORDERS], scopes=["X"]
    )
    assert line == 1500.0


async def test_a_walk_that_has_never_run_settles_nothing(db) -> None:
    """A missing cursor is an unconfirmed window, not an absent constraint."""
    repo = BackfillCursorRepository(db)
    await repo.advance(1, Stream.TRADES, scope="X", confirmed_through_ts=2000.0)
    await db.commit()

    line = await repo.confirmed_through(
        1, [Stream.TRADES, Stream.ORDERS], scopes=["X"]
    )
    assert line == 0.0


async def test_session_pnl_can_settle_before_account_pnl(db) -> None:
    """Trading PnL excludes funding, so it does not wait on the cash walk."""
    repo = BackfillCursorRepository(db)
    await repo.advance(1, Stream.TRADES, scope="X", confirmed_through_ts=2000.0)
    await repo.advance(1, Stream.ORDERS, scope="X", confirmed_through_ts=2000.0)
    await repo.advance(1, Stream.CASH_FLOWS, confirmed_through_ts=500.0)
    await db.commit()

    session_line = await repo.confirmed_through(
        1, [Stream.TRADES, Stream.ORDERS], scopes=["X"]
    )
    assert session_line == 2000.0
    assert await repo.confirmed_through(1, [Stream.CASH_FLOWS]) == 500.0
