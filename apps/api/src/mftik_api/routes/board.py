"""What each strategy run did, as far as the record can state without arithmetic.

Executions and times. No PnL — deriving a result means matching executions into
positions and valuing whatever is left open when a run ends, and a number
produced before that machinery exists would be read as one that had been.

One count, on purpose. Orders were reported here too and are not: counting rows
described a strategy's working style rather than a run, since a chase re-prices
by cancelling and a TWAP slices; counting only the ones that traded said little
that the fills did not; and counting the ones still resting was the only figure
here that could go *up* when the record went wrong, because it reads a state
that has to be kept current rather than a fact that already happened. What is
left is cumulative and under-reports when rows are lost, which is the failure
direction a record should have.

"What is resting right now" is a real question and this is the wrong place for
it — the answer lives in TD's own book, and a session's page is where to ask.

One listing here is not keyed by a session at all: the executions that belong
to none. Everything else reads by ``session_id``, so a fill without one was in
no listing anywhere, and the row that most needs looking at — ours, with its
order missing — was the quietest thing in the record. It is a listing rather
than a count for the same reason: there is nothing to total, only rows to read.

Every summary carries its settlement line alongside its count, because the two
mean different things. ``fills`` is what has been recorded; ``settled`` says
whether the venue has been re-read across the whole run and agreed. A live
session is never settled, and neither is a finished one whose last minutes are
still inside the safety lag. The same line is carried down onto each execution,
so a reader looking at a single row can tell whether it is a figure or a note.
"""

from __future__ import annotations

import csv
import io
import time
from datetime import UTC, datetime
from typing import Annotated, Any

from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import Response
from mftik.protocol import attached_api_ids
from mftik_db.models.history import Stream
from mftik_db.models.session import SessionStatus
from mftik_db.repositories import (
    BackfillCursorRepository,
    FillRepository,
    OrderRepository,
    StsSessionRepository,
)
from mftik_db.session import session_scope

from mftik_api.decimals import wire_decimal
from mftik_api.paging import ListOffset
from mftik_api.schemas import (
    BoardFill,
    BoardFillListResponse,
    BoardResponse,
    BoardSession,
)

_CSV_COLUMNS = (
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
)

router = APIRouter(prefix="/board", tags=["board"])


def _epoch(value: datetime | None) -> float | None:
    """Seconds since the epoch, reading a naive timestamp as UTC.

    ``DateTime(timezone=True)`` comes back aware from Postgres and naive from
    anything without a timezone type, and ``.timestamp()`` on a naive value
    silently reads it as *local* time. Every other figure here is already a
    float epoch — the settlement line most of all — so a shift would not look
    wrong, it would just quietly disagree, and a run would report itself
    settled against a line it never reached.
    """
    if value is None:
        return None
    if value.tzinfo is None:
        value = value.replace(tzinfo=UTC)
    return value.timestamp()


async def _settlement_lines(
    db: Any, rows: list[Any]
) -> tuple[dict[str, float], dict[str, list[str]]]:
    """Lines by session, and the instruments each run placed an order on.

    A result is only as settled as its least-confirmed input, so one unwalked
    account or instrument leaves the whole run provisional. The instruments
    are the orders' tickers, not the strategy's subscriptions: a chase that
    never filled still names the book, and a subscribe-only run does not.

    A run that placed none has no scope for a walk to cover, so no cursor will
    ever mention it — which is not the same fact as an unwalked run. The
    caller tells them apart by whether the ticker list is empty.
    """
    session_ids = [row.session_id for row in rows]
    tickers = await OrderRepository(db).tickers_by_session(session_ids)
    every_api_id = [
        int(x) for row in rows for x in attached_api_ids(row)
    ]
    # One statement for the whole page. A query per session per account is the
    # shape the counts beside it were batched away from, and this list goes up
    # to five hundred.
    cursors = await BackfillCursorRepository(db).lines_for(
        every_api_id, [Stream.TRADES, Stream.ORDERS]
    )

    streams = (Stream.TRADES, Stream.ORDERS)
    lines: dict[str, float] = {}
    for row in rows:
        scopes = tickers.get(row.session_id, [])
        api_ids = attached_api_ids(row)
        if not api_ids:
            continue
        if not scopes:
            continue
        # A missing cursor is an unconfirmed window, not an absent constraint —
        # the same rule ``confirmed_through`` applies, kept here because the
        # batched read cannot tell absent from zero on the caller's behalf.
        lines[row.session_id] = min(
            cursors.get((api_id, stream, scope), 0.0)
            for api_id in api_ids
            for stream in streams
            for scope in scopes
        )
    return lines, tickers


def _summary(
    row: Any,
    *,
    fills: int,
    line: float | None,
    now: float,
    tickers: list[str] | None = None,
) -> BoardSession:
    created = _epoch(row.created_at) or 0.0
    finished = _epoch(row.finished_at)
    instruments = list(tickers or [])
    # A run that ended having recorded nothing at all has nothing to confirm,
    # and no cursor will ever mention it. Calling that provisional forever
    # would report "not yet checked" about a run where there is nothing to
    # check. Fills without an order on file is a different thing entirely — an
    # anomaly — and stays provisional so it does not get hidden behind a badge.
    nothing_to_settle = not instruments and fills == 0
    return BoardSession(
        session_id=row.session_id,
        strategy=row.strategy,
        status=row.status,
        reason=row.reason,
        created_at=created,
        finished_at=finished,
        duration_s=max(0.0, (finished if finished else now) - created),
        running=row.status == SessionStatus.LIVE.value,
        fills=fills,
        td_api_ids=attached_api_ids(row),
        confirmed_through_ts=line if line else None,
        # A run is settled only once it has ended *and* the line has passed its
        # end. A live one never is, however far the line has come — it is still
        # producing rows behind it.
        settled=bool(
            finished is not None
            and ((line and line >= finished) or nothing_to_settle)
        ),
        tickers=instruments,
    )


def _fill(row: Any, line: float | None) -> BoardFill:
    """One stored execution on the wire.

    ``settled`` is decided per row rather than per page: a page can straddle
    the line, and the rows after it are the ones a reader should not treat as
    final.
    """
    return BoardFill(
        id=row.id,
        fill_id=row.fill_id,
        universal_ticker=row.universal_ticker,
        side=row.side,
        price=wire_decimal(row.price) or "0",
        qty=wire_decimal(row.qty) or "0",
        fee=wire_decimal(row.fee) or "0",
        fee_asset=row.fee_asset,
        client_order_id=row.client_order_id,
        venue_order_id=row.venue_order_id,
        api_id=row.api_id,
        ts=row.ts,
        source=row.source,
        settled=bool(line and row.ts <= line),
    )


def _parse_statuses(status: str | None) -> str | list[str] | None:
    """``done,ack`` is the Finished tab; a single value is the rest."""
    if status is None or not status.strip():
        return None
    parts = [part.strip() for part in status.split(",") if part.strip()]
    if not parts:
        return None
    if len(parts) == 1:
        return parts[0]
    return parts


@router.get("/sessions", response_model=BoardResponse)
async def list_board_sessions(
    status: str | None = Query(default=None),
    offset: ListOffset = 0,
    limit: Annotated[int, Query(ge=1, le=500)] = 50,
) -> BoardResponse:
    """Recent runs, newest first. ``status`` omitted means every status.

    Comma-separated values select a union (``done,ack`` is Finished).
    ``offset`` / ``limit`` page a numbered browse. ``total`` is the match
    count before paging.
    """
    now = time.time()
    parsed = _parse_statuses(status)
    async with session_scope() as db:
        repo = StsSessionRepository(db)
        total = await repo.count_sessions(status=parsed)
        page = list(
            await repo.list_sessions(status=parsed, offset=offset, limit=limit)
        )
        fills = await FillRepository(db).count_by_session(
            [row.session_id for row in page]
        )
        lines, tickers = await _settlement_lines(db, page)

    return BoardResponse(
        sessions=[
            _summary(
                row,
                fills=fills.get(row.session_id, 0),
                line=lines.get(row.session_id),
                now=now,
                tickers=tickers.get(row.session_id, []),
            )
            for row in page
        ],
        total=total,
        has_more=offset + len(page) < total,
    )


@router.get("/sessions/{session_id}", response_model=BoardSession)
async def get_board_session(session_id: str) -> BoardSession:
    """One run's summary — the header a drill-in needs to keep its context."""
    now = time.time()
    async with session_scope() as db:
        row = await StsSessionRepository(db).get_by_session_id(session_id)
        if row is None:
            raise HTTPException(status_code=404, detail=f"no session {session_id}")
        fills = await FillRepository(db).count_by_session([session_id])
        lines, tickers = await _settlement_lines(db, [row])

    return _summary(
        row,
        fills=fills.get(session_id, 0),
        line=lines.get(session_id),
        now=now,
        tickers=tickers.get(session_id, []),
    )


@router.get("/sessions/{session_id}/fills", response_model=BoardFillListResponse)
async def list_board_fills(
    session_id: str,
    before_ts: float | None = Query(default=None),
    before_id: int | None = Query(default=None),
    limit: int = Query(default=100, ge=1, le=500),
) -> BoardFillListResponse:
    """This run's executions, newest first.

    Cursor-paginated on ``(ts, id)`` the way the log listing is, and for the
    same reason: a busy run has more of these than a page, and an offset would
    shift under rows arriving while somebody reads.
    """
    # One extra to answer has_more without a second COUNT.
    fetch = limit + 1
    async with session_scope() as db:
        rows = await FillRepository(db).list_for_session(
            session_id, before_ts=before_ts, before_id=before_id, limit=fetch
        )
        lines: dict[str, float] = {}
        summary_row = await StsSessionRepository(db).get_by_session_id(session_id)
        if summary_row is not None:
            lines, _tickers = await _settlement_lines(db, [summary_row])

    has_more = len(rows) > limit
    line = lines.get(session_id)
    return BoardFillListResponse(
        fills=[_fill(row, line) for row in rows[:limit]],
        has_more=has_more,
    )


@router.get("/sessions/{session_id}/fills.csv")
async def export_board_fills_csv(session_id: str) -> Response:
    """Every execution this run recorded, oldest first, as a CSV download."""
    async with session_scope() as db:
        summary_row = await StsSessionRepository(db).get_by_session_id(session_id)
        if summary_row is None:
            raise HTTPException(status_code=404, detail=f"no session {session_id}")
        fills = await FillRepository(db).replay_for_session(session_id)
        lines, _tickers = await _settlement_lines(db, [summary_row])

    line = lines.get(session_id)
    buf = io.StringIO()
    writer = csv.writer(buf)
    writer.writerow(_CSV_COLUMNS)
    for row in fills:
        item = _fill(row, line)
        writer.writerow(
            [
                item.ts,
                item.fill_id,
                item.universal_ticker,
                item.side,
                item.price,
                item.qty,
                item.fee,
                item.fee_asset,
                item.client_order_id or "",
                item.venue_order_id or "",
                item.api_id,
                item.source,
                "true" if item.settled else "false",
            ]
        )
    filename = f"{session_id}_historical_fills.csv"
    return Response(
        content=buf.getvalue(),
        media_type="text/csv; charset=utf-8",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


@router.get("/fills/external", response_model=BoardFillListResponse)
async def list_external_fills(
    before_ts: float | None = Query(default=None),
    before_id: int | None = Query(default=None),
    limit: int = Query(default=100, ge=1, le=500),
) -> BoardFillListResponse:
    """Executions on our accounts that no session of ours placed, newest first.

    These are recorded and, until now, invisible: every other listing here is
    keyed by session, and a fill with no session is in none of them. That hid
    two different things behind one silence — trading done by hand or by
    another tool, which is real and simply not a run's, and *our own* fills
    whose order never reached ``orders``, which is a hole in the record.

    They are not told apart here on purpose. The two are indistinguishable
    from the fill alone — the question is what the account was doing, and
    answering it needs a person. What this page can do is stop the second kind
    from being silent.

    Cursor-paginated on ``(ts, id)`` like the per-session listing, and for the
    same reason: rows keep arriving underneath a reader.
    """
    fetch = limit + 1
    async with session_scope() as db:
        rows = list(
            await FillRepository(db).list_unattributed(
                before_ts=before_ts, before_id=before_id, limit=fetch
            )
        )
        page = rows[:limit]
        cursors = await BackfillCursorRepository(db).lines_for(
            [row.api_id for row in page], [Stream.TRADES, Stream.ORDERS]
        )

    # Both streams, unlike a session's rows which need trades for the figures
    # and orders for the attribution: here the *absence* of an attribution is
    # the claim being made, and it only holds once the orders walk has passed
    # this instrument too. Until then the order that would claim this fill may
    # still be on its way.
    def line_for(row: Any) -> float:
        return min(
            cursors.get((row.api_id, stream, row.universal_ticker), 0.0)
            for stream in (Stream.TRADES, Stream.ORDERS)
        )

    return BoardFillListResponse(
        fills=[_fill(row, line_for(row)) for row in page],
        has_more=len(rows) > limit,
    )
