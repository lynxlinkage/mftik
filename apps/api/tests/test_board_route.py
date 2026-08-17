"""GET /board/sessions — what a run did, without claiming what it made.

The counts are the easy part. What is worth pinning is the distinction the page
rests on: `fills` is what has been *recorded*, and `settled` says whether the
venue has been re-read across the whole run and agreed. Collapsing the two
would turn "we have not checked yet" into "this is what happened", which is the
one thing a record like this must never do.
"""

from __future__ import annotations

import csv
import io
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest
from db_harness import a_database, an_owner
from fastapi import HTTPException
from mftik_api.routes import board as board_routes
from mftik_db.models import StsSessionRow
from mftik_db.models.history import Attribution, Source, Stream
from mftik_db.repositories import (
    BackfillCursorRepository,
    FillRepository,
    OrderRepository,
)

API_ID = 7
TICKER = "Binance_Spot_BTCUSDT"
START = datetime(2026, 8, 1, 12, 0, tzinfo=UTC)


@pytest.fixture
async def db(monkeypatch, database_url):
    async with a_database(database_url) as database:
        async with database.scope() as session:
            await an_owner(session)
        monkeypatch.setattr(board_routes, "session_scope", database.scope)
        yield database.scope


async def a_session(
    scope,
    session_id: str,
    *,
    status: str = "done",
    strategy: str = "twap",
    started: datetime = START,
    ran_for: timedelta | None = timedelta(minutes=10),
    api_ids: list[int] | None = None,
) -> None:
    async with scope() as session:
        session.add(
            StsSessionRow(
                session_id=session_id,
                created_by=1,
                created_at=started,
                finished_at=(started + ran_for) if ran_for else None,
                status=status,
                strategy=strategy,
                td_api_ids=api_ids if api_ids is not None else [API_ID],
                md_ids=[],
                st_paras={},
                st_facts={},
            )
        )


async def an_order(
    scope, session_id: str, *, key: str, ts: float, ticker: str = TICKER
) -> None:
    """A filled order. Only its instrument matters here — the page counts
    executions, and an order is on file to give the settlement line a scope."""
    async with scope() as session:
        await OrderRepository(session).bulk_upsert(
            [
                {
                    "api_id": API_ID,
                    "order_key": key,
                    "client_order_id": key,
                    "venue_order_id": None,
                    "session_id": session_id,
                    "strategy": None,
                    "cid_slot": None,
                    "attribution": Attribution.DIRECT,
                    "universal_ticker": ticker,
                    "side": "buy",
                    "order_type": "limit",
                    "status": "filled",
                    "qty": Decimal("1"),
                    "price": Decimal("100"),
                    "filled_qty": Decimal("1"),
                    "avg_price": Decimal("100"),
                    "submitted_at": ts,
                    "ts": ts,
                    "source": Source.STREAM,
                }
            ]
        )


async def a_fill(
    scope,
    session_id: str | None,
    *,
    fill_id: str,
    ts: float,
    price: str = "100",
    qty: str = "0.25",
    ticker: str = TICKER,
) -> None:
    """``session_id=None`` is an execution no run of ours placed."""
    async with scope() as session:
        await FillRepository(session).bulk_upsert(
            [
                {
                    "api_id": API_ID,
                    "fill_id": fill_id,
                    "universal_ticker": ticker,
                    "venue_order_id": "500",
                    "client_order_id": "cid-1",
                    "session_id": session_id,
                    "side": "buy",
                    "price": Decimal(price),
                    "qty": Decimal(qty),
                    "fee": Decimal("0"),
                    "fee_asset": "",
                    "realized_pnl": None,
                    "is_maker": None,
                    "ts": ts,
                    "source": Source.STREAM,
                }
            ]
        )


async def confirm(
    scope,
    through: float,
    *,
    ticker: str = TICKER,
    streams: tuple[str, ...] = (Stream.TRADES, Stream.ORDERS),
) -> None:
    async with scope() as session:
        repo = BackfillCursorRepository(session)
        for stream in streams:
            await repo.advance(
                API_ID, stream, scope=ticker, confirmed_through_ts=through
            )


async def rows(*, status: str | None = None, limit: int = 50):
    """Call the route the way FastAPI would, with its Query defaults resolved."""
    result = await board_routes.list_board_sessions(
        status=status, limit=limit
    )
    return result.sessions


async def fills_of(session_id: str, **kw):
    return await board_routes.list_board_fills(
        session_id,
        before_ts=kw.get("before_ts"),
        before_id=kw.get("before_id"),
        limit=kw.get("limit", 100),
    )


async def external_fills(**kw):
    return await board_routes.list_external_fills(
        before_ts=kw.get("before_ts"),
        before_id=kw.get("before_id"),
        limit=kw.get("limit", 100),
    )


# --- counts and times ------------------------------------------------------


async def test_a_run_reports_what_it_traded_and_how_long_it_ran(db) -> None:
    await a_session(db, "s1")
    await an_order(db, "s1", key="cid-1", ts=START.timestamp())
    await a_fill(db, "s1", fill_id="1", ts=START.timestamp())
    await a_fill(db, "s1", fill_id="2", ts=START.timestamp() + 1)

    (row,) = await rows()

    assert row.session_id == "s1"
    assert row.strategy == "twap"
    assert row.fills == 2
    assert row.duration_s == pytest.approx(600.0)
    assert row.running is False


async def test_a_live_run_is_measured_against_now(db) -> None:
    """It has no finish to measure to, and showing nothing would be worse."""
    started = datetime.now(UTC) - timedelta(seconds=30)
    await a_session(db, "s1", status="live", started=started, ran_for=None)
    await an_order(db, "s1", key="cid-1", ts=started.timestamp())

    (row,) = await rows()

    assert row.running is True
    assert row.finished_at is None
    assert row.duration_s >= 30


async def test_a_run_that_never_traded_reports_zero_not_nothing(db) -> None:
    """A strategy that placed no orders is a fact, not a missing row."""
    await a_session(db, "s1")

    (row,) = await rows()

    assert row.fills == 0


async def test_counts_do_not_leak_between_sessions(db) -> None:
    await a_session(db, "s1")
    await a_session(db, "s2")
    await an_order(db, "s1", key="cid-1", ts=START.timestamp())
    await an_order(db, "s2", key="cid-2", ts=START.timestamp())
    await an_order(db, "s2", key="cid-3", ts=START.timestamp())
    await a_fill(db, "s2", fill_id="1", ts=START.timestamp())

    by_id = {r.session_id: r for r in await rows()}

    assert by_id["s1"].fills == 0
    assert by_id["s2"].fills == 1


async def test_a_status_filter_narrows_the_list(db) -> None:
    await a_session(db, "s1", status="live", ran_for=None)
    await a_session(db, "s2", status="done")

    assert [r.session_id for r in await rows(status="live")] == ["s1"]
    assert [r.session_id for r in await rows(status="done")] == ["s2"]
    assert {r.session_id for r in await rows()} == {"s1", "s2"}


async def test_finished_is_done_and_ack(db) -> None:
    await a_session(db, "s-done", status="done")
    await a_session(db, "s-ack", status="ack")
    await a_session(db, "s-fail", status="failed")

    assert {r.session_id for r in await rows(status="done,ack")} == {
        "s-done",
        "s-ack",
    }


# --- settled vs provisional ------------------------------------------------


async def test_an_unwalked_run_is_provisional_and_says_so(db) -> None:
    """No cursor is not zero confirmed — it is the record declining to claim."""
    await a_session(db, "s1")
    await an_order(db, "s1", key="cid-1", ts=START.timestamp())
    await a_fill(db, "s1", fill_id="1", ts=START.timestamp())

    (row,) = await rows()

    assert row.confirmed_through_ts is None
    assert row.settled is False
    assert row.fills == 1, "the count is still real; only the guarantee is missing"


async def test_a_run_confirmed_past_its_end_is_settled(db) -> None:
    await a_session(db, "s1")
    await an_order(db, "s1", key="cid-1", ts=START.timestamp())
    await confirm(db, (START + timedelta(minutes=30)).timestamp())

    (row,) = await rows()

    assert row.settled is True


async def test_a_run_confirmed_only_partway_is_not_settled(db) -> None:
    """The last minutes of a run sit inside the safety lag for a while."""
    await a_session(db, "s1")
    await an_order(db, "s1", key="cid-1", ts=START.timestamp())
    await confirm(db, (START + timedelta(minutes=5)).timestamp())

    (row,) = await rows()

    assert row.confirmed_through_ts is not None
    assert row.settled is False


async def test_a_live_run_is_never_settled(db) -> None:
    """However far the line has come, it is still producing rows behind it."""
    await a_session(db, "s1", status="live", ran_for=None)
    await an_order(db, "s1", key="cid-1", ts=START.timestamp())
    await confirm(db, (START + timedelta(days=1)).timestamp())

    (row,) = await rows()

    assert row.running is True
    assert row.settled is False


async def test_one_unwalked_instrument_leaves_the_whole_run_provisional(
    db,
) -> None:
    """A result is only as settled as its least-confirmed input."""
    await a_session(db, "s1")
    await an_order(db, "s1", key="cid-1", ts=START.timestamp())
    await an_order(
        db, "s1", key="cid-2", ts=START.timestamp(), ticker="Binance_Spot_ETHUSDT"
    )
    await confirm(db, (START + timedelta(minutes=30)).timestamp())

    (row,) = await rows()

    assert row.settled is False, "ETHUSDT was never walked"


# --- one run's executions --------------------------------------------------


async def test_a_run_summary_is_readable_on_its_own(db) -> None:
    """The header a drill-in needs, without loading the whole board."""
    await a_session(db, "s1")
    await an_order(db, "s1", key="cid-1", ts=START.timestamp())
    await a_fill(db, "s1", fill_id="1", ts=START.timestamp())

    row = await board_routes.get_board_session("s1")

    assert row.session_id == "s1"
    assert row.fills == 1
    assert row.duration_s == pytest.approx(600.0)


async def test_asking_for_a_run_that_does_not_exist_is_a_404(db) -> None:
    with pytest.raises(HTTPException) as caught:
        await board_routes.get_board_session("nope")
    assert caught.value.status_code == 404


async def test_executions_come_back_newest_first(db) -> None:
    await a_session(db, "s1")
    for i in range(1, 4):
        await a_fill(db, "s1", fill_id=str(i), ts=START.timestamp() + i)

    page = await fills_of("s1")

    assert [f.fill_id for f in page.fills] == ["3", "2", "1"]
    assert page.has_more is False


async def test_executions_paginate_on_a_cursor(db) -> None:
    """An offset would shift under rows arriving while somebody reads."""
    await a_session(db, "s1")
    for i in range(1, 6):
        await a_fill(db, "s1", fill_id=str(i), ts=START.timestamp() + i)

    first = await fills_of("s1", limit=2)
    assert [f.fill_id for f in first.fills] == ["5", "4"]
    assert first.has_more is True

    oldest = first.fills[-1]
    second = await fills_of(
        "s1", before_ts=oldest.ts, before_id=oldest.id, limit=2
    )
    assert [f.fill_id for f in second.fills] == ["3", "2"]
    assert second.has_more is True


async def test_decimals_travel_as_strings(db) -> None:
    """A JSON number is a double, and these columns are NUMERIC(38,18).

    Rounding on the way out is the one place nothing downstream could notice.
    """
    await a_session(db, "s1")
    await a_fill(db, "s1", fill_id="1", ts=START.timestamp())

    (fill,) = (await fills_of("s1")).fills

    assert isinstance(fill.price, str)
    assert isinstance(fill.qty, str)
    assert Decimal(fill.price) == Decimal("100")
    assert Decimal(fill.qty) == Decimal("0.25")


async def test_amounts_arrive_without_the_columns_padding(db) -> None:
    """``NUMERIC(38,18)`` pads every value to its full scale.

    A price the venue reported as 63863.5 comes out of the column as
    ``63863.500000000000000000``, which is the same number and unreadable in a
    table of them. The padding is real here too: sqlite has no decimal type and
    reads the column back at its full scale, so this exercises the stripping
    even though the exact digits it pads are its own — hence a binary-exact
    price, which is the only kind sqlite returns unchanged.
    """
    await a_session(db, "s1")
    await a_fill(
        db, "s1", fill_id="1", ts=START.timestamp(), price="63863.5", qty="1"
    )

    (fill,) = (await fills_of("s1")).fills

    assert fill.price == "63863.5"
    assert fill.qty == "1"
    assert fill.fee == "0", "and a zero fee reads as a zero, not as padding"


async def test_each_execution_says_whether_it_is_settled(db) -> None:
    """Per row, because a page can straddle the line.

    The rows after it are the ones a reader should not treat as final, and a
    page-level flag would either hide that or overstate it.
    """
    await a_session(db, "s1")
    await an_order(db, "s1", key="cid-1", ts=START.timestamp())
    await a_fill(db, "s1", fill_id="early", ts=START.timestamp() + 60)
    await a_fill(db, "s1", fill_id="late", ts=START.timestamp() + 900)
    await confirm(db, START.timestamp() + 300)

    by_id = {f.fill_id: f for f in (await fills_of("s1")).fills}

    assert by_id["early"].settled is True
    assert by_id["late"].settled is False


async def test_a_run_with_no_cursor_has_no_settled_executions(db) -> None:
    await a_session(db, "s1")
    await a_fill(db, "s1", fill_id="1", ts=START.timestamp())

    (fill,) = (await fills_of("s1")).fills

    assert fill.settled is False


async def test_executions_do_not_leak_between_runs(db) -> None:
    await a_session(db, "s1")
    await a_session(db, "s2")
    await a_fill(db, "s1", fill_id="1", ts=START.timestamp())
    await a_fill(db, "s2", fill_id="2", ts=START.timestamp())

    assert [f.fill_id for f in (await fills_of("s1")).fills] == ["1"]
    assert [f.fill_id for f in (await fills_of("s2")).fills] == ["2"]


async def test_a_run_that_recorded_nothing_is_trivially_settled(db) -> None:
    """No order, no fill, nothing for a walk to cover.

    No cursor will ever mention this run, so "provisional" would report "not
    yet checked" about something with nothing to check.
    """
    await a_session(db, "s1")

    (row,) = await rows()

    assert row.settled is True
    assert row.confirmed_through_ts is None


async def test_fills_without_an_order_on_file_stay_provisional(db) -> None:
    """That is an anomaly, not an empty run, and a badge would hide it."""
    await a_session(db, "s1")
    await a_fill(db, "s1", fill_id="1", ts=START.timestamp())

    (row,) = await rows()

    assert row.fills == 1
    assert row.settled is False


async def test_a_live_run_that_recorded_nothing_is_still_not_settled(db) -> None:
    """It has not ended, so it can still record something."""
    await a_session(db, "s1", status="live", ran_for=None)

    (row,) = await rows()

    assert row.settled is False


# --- executions belonging to no run ----------------------------------------


async def test_executions_with_no_session_are_listed_on_their_own(db) -> None:
    """Every other listing is keyed by session, so these were in none of them."""
    await a_session(db, "s1")
    await a_fill(db, "s1", fill_id="ours", ts=START.timestamp())
    await a_fill(db, None, fill_id="theirs", ts=START.timestamp() + 1)

    page = await external_fills()

    assert [f.fill_id for f in page.fills] == ["theirs"]
    assert page.has_more is False


async def test_external_executions_come_back_newest_first(db) -> None:
    for i in range(1, 4):
        await a_fill(db, None, fill_id=str(i), ts=START.timestamp() + i)

    page = await external_fills()

    assert [f.fill_id for f in page.fills] == ["3", "2", "1"]


async def test_external_executions_paginate_on_a_cursor(db) -> None:
    for i in range(1, 6):
        await a_fill(db, None, fill_id=str(i), ts=START.timestamp() + i)

    first = await external_fills(limit=2)
    assert [f.fill_id for f in first.fills] == ["5", "4"]
    assert first.has_more is True

    oldest = first.fills[-1]
    second = await external_fills(
        before_ts=oldest.ts, before_id=oldest.id, limit=2
    )
    assert [f.fill_id for f in second.fills] == ["3", "2"]


async def test_an_external_execution_is_settled_once_the_walk_passed_it(db) -> None:
    await a_fill(db, None, fill_id="early", ts=START.timestamp() + 60)
    await a_fill(db, None, fill_id="late", ts=START.timestamp() + 900)
    await confirm(db, START.timestamp() + 300)

    by_id = {f.fill_id: f for f in (await external_fills()).fills}

    assert by_id["early"].settled is True
    assert by_id["late"].settled is False


async def test_an_unwalked_account_has_no_settled_external_executions(db) -> None:
    await a_fill(db, None, fill_id="1", ts=START.timestamp())

    (fill,) = (await external_fills()).fills

    assert fill.settled is False


async def test_an_unwalked_order_stream_leaves_it_provisional(db) -> None:
    """Here the *absence* of an attribution is the claim being made.

    The trades walk confirms the figures; only the orders walk can say nothing
    on file claims this fill. Calling it settled on trades alone would report
    "nobody's" about a fill whose order is still on its way.
    """
    await a_fill(db, None, fill_id="1", ts=START.timestamp())
    await confirm(db, START.timestamp() + 300, streams=(Stream.TRADES,))

    (fill,) = (await external_fills()).fills

    assert fill.settled is False


async def test_an_external_execution_on_another_instrument_reads_its_own_line(
    db,
) -> None:
    """One account, two books, and only one of them walked."""
    other = "Binance_Spot_ETHUSDT"
    await a_fill(db, None, fill_id="btc", ts=START.timestamp())
    await a_fill(db, None, fill_id="eth", ts=START.timestamp(), ticker=other)
    await confirm(db, START.timestamp() + 300)

    by_id = {f.fill_id: f for f in (await external_fills()).fills}

    assert by_id["btc"].settled is True
    assert by_id["eth"].settled is False


async def test_external_amounts_travel_as_strings_without_padding(db) -> None:
    await a_fill(
        db, None, fill_id="1", ts=START.timestamp(), price="63863.5", qty="1"
    )

    (fill,) = (await external_fills()).fills

    assert fill.price == "63863.5"
    assert fill.qty == "1"
    assert Decimal(fill.price) == Decimal("63863.5")


async def test_csv_export_is_oldest_first_and_named(db) -> None:
    await a_session(db, "s1")
    await an_order(db, "s1", key="cid-1", ts=START.timestamp())
    await a_fill(db, "s1", fill_id="1", ts=START.timestamp() + 1)
    await a_fill(db, "s1", fill_id="2", ts=START.timestamp() + 2)
    await confirm(db, (START + timedelta(minutes=30)).timestamp())

    response = await board_routes.export_board_fills_csv("s1")
    assert response.media_type is not None
    assert response.media_type.startswith("text/csv")
    assert 'filename="s1_historical_fills.csv"' in response.headers[
        "content-disposition"
    ]

    text = (
        response.body.decode()
        if isinstance(response.body, (bytes, bytearray))
        else str(response.body)
    )
    rows = list(csv.reader(io.StringIO(text)))
    assert rows[0] == [
        "ts",
        "fill_id",
        "universal_ticker",
        "side",
        "price",
        "qty",
        "fee",
        "fee_asset",
        "client_order_id",
        "venue_order_id",
        "api_id",
        "source",
        "settled",
    ]
    assert [row[1] for row in rows[1:]] == ["1", "2"]
    assert rows[1][12] == "true"


async def test_csv_export_of_a_missing_session_is_a_404(db) -> None:
    with pytest.raises(HTTPException) as caught:
        await board_routes.export_board_fills_csv("nope")
    assert caught.value.status_code == 404
