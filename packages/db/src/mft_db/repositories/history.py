"""Trading history persistence.

Every write here is idempotent, because every writer runs twice. The live
stream and the venue backfill describe the same executions, arrive in either
order, and each knows things the other does not — so the interesting part of
this module is not the inserts but **what an upsert is allowed to overwrite**.

Three rules, applied field by field rather than row by row:

* **Identity is filled in, never erased.** Neither Binance market puts a client
  order id on a trade row, so a backfilled Binance fill arrives without one;
  writing that over a row the stream had already attributed would silently
  orphan the fill from its session. Those fields coalesce.
* **Corrections flow one way.** Fees settle late — rebates, BNB discounts — so
  a backfilled fee replaces a streamed one, and a streamed fee never replaces a
  backfilled one, however it is ordered.
* **Facts are immutable.** Price, quantity, side and time are never updated. If
  the two sources disagree about those, something is wrong that overwriting
  would hide.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

from sqlalchemy import case, func, or_, select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.ext.asyncio import AsyncSession

from mft_db.models.history import (
    Attribution,
    BackfillCursorRow,
    CashFlowRow,
    FillRow,
    OrderRow,
    Source,
)
from mft_db.repositories.base import BaseRepository


def _insert(session: AsyncSession) -> Any:
    """The dialect's upsert constructor. Same switch the log repository makes."""
    bind = session.get_bind()
    dialect = bind.dialect.name if bind is not None else "postgresql"
    return sqlite_insert if dialect == "sqlite" else pg_insert


class OrderRepository(BaseRepository[OrderRow]):
    def __init__(self, session: AsyncSession) -> None:
        super().__init__(session, OrderRow)

    async def bulk_upsert(self, rows: Sequence[Mapping[str, Any]]) -> int:
        """Insert or merge orders on ``(api_id, order_key)``.

        State (status, fills, average) is taken from whichever row is *newer*
        by ``ts``, so an out-of-order arrival cannot roll a terminal order back
        to open. Attribution never degrades: once an order is ``direct`` — the
        only tier written from the submit itself — nothing downgrades it, which
        is what keeps a backfill that rediscovers our own order from filing it
        as somebody else's.
        """
        if not rows:
            return 0
        stmt = _insert(self.session)(OrderRow).values(list(rows))
        newer = stmt.excluded.ts >= OrderRow.ts
        stmt = stmt.on_conflict_do_update(
            index_elements=["api_id", "order_key"],
            set_={
                "status": case((newer, stmt.excluded.status), else_=OrderRow.status),
                "filled_qty": case(
                    (newer, stmt.excluded.filled_qty), else_=OrderRow.filled_qty
                ),
                "avg_price": case(
                    (newer, stmt.excluded.avg_price), else_=OrderRow.avg_price
                ),
                "ts": case((newer, stmt.excluded.ts), else_=OrderRow.ts),
                "venue_order_id": func.coalesce(
                    stmt.excluded.venue_order_id, OrderRow.venue_order_id
                ),
                "client_order_id": func.coalesce(
                    stmt.excluded.client_order_id, OrderRow.client_order_id
                ),
                "session_id": func.coalesce(
                    stmt.excluded.session_id, OrderRow.session_id
                ),
                "strategy": func.coalesce(
                    stmt.excluded.strategy, OrderRow.strategy
                ),
                "cid_slot": func.coalesce(
                    stmt.excluded.cid_slot, OrderRow.cid_slot
                ),
                "attribution": case(
                    (OrderRow.attribution == Attribution.DIRECT, Attribution.DIRECT),
                    else_=stmt.excluded.attribution,
                ),
                "reject_code": func.coalesce(
                    stmt.excluded.reject_code, OrderRow.reject_code
                ),
                "reject_reason": func.coalesce(
                    stmt.excluded.reject_reason, OrderRow.reject_reason
                ),
                # Ours first: a submit time we recorded beats one inferred from
                # a history row, and a backfilled order has none at all.
                "submitted_at": func.coalesce(
                    OrderRow.submitted_at, stmt.excluded.submitted_at
                ),
                "source": case(
                    (stmt.excluded.source == Source.BACKFILL, Source.BACKFILL),
                    else_=OrderRow.source,
                ),
            },
        )
        await self.session.execute(stmt)
        await self.session.flush()
        return len(rows)

    async def get_by_key(self, api_id: int, order_key: str) -> OrderRow | None:
        result = await self.session.execute(
            select(OrderRow).where(
                OrderRow.api_id == api_id, OrderRow.order_key == order_key
            )
        )
        return result.scalar_one_or_none()

    async def owners_for(
        self, keys: Sequence[tuple[int, str]]
    ) -> dict[tuple[int, str], str]:
        """``(api_id, order_key)`` → session, for those that have one.

        One statement for a whole batch: the caller is a flush loop attaching
        a page of fills to their orders, and a query per fill would put the
        database round trips back on the path the writer exists to keep them
        off.
        """
        if not keys:
            return {}
        conditions = [
            (OrderRow.api_id == api_id) & (OrderRow.order_key == key)
            for api_id, key in set(keys)
        ]
        result = await self.session.execute(
            select(OrderRow.api_id, OrderRow.order_key, OrderRow.session_id).where(
                or_(*conditions), OrderRow.session_id.is_not(None)
            )
        )
        return {(row.api_id, row.order_key): row.session_id for row in result.all()}

    async def list_for_session(
        self,
        session_id: str,
        *,
        before_ts: float | None = None,
        before_id: int | None = None,
        limit: int = 100,
    ) -> Sequence[OrderRow]:
        """Newest first, cursor-paginated the way the log listing is."""
        q = select(OrderRow).where(OrderRow.session_id == session_id)
        q = _before(q, OrderRow, before_ts, before_id)
        q = q.order_by(OrderRow.ts.desc(), OrderRow.id.desc()).limit(limit)
        return (await self.session.execute(q)).scalars().all()

    async def tickers_by_session(
        self, session_ids: Sequence[str]
    ) -> dict[str, list[str]]:
        """``session_id`` → the instruments it placed orders on."""
        if not session_ids:
            return {}
        result = await self.session.execute(
            select(OrderRow.session_id, OrderRow.universal_ticker)
            .where(OrderRow.session_id.in_(list(set(session_ids))))
            .distinct()
        )
        out: dict[str, list[str]] = {}
        for session_id, ticker in result.all():
            out.setdefault(session_id, []).append(ticker)
        return {key: sorted(value) for key, value in out.items()}

    async def by_venue_ids(
        self, api_id: int, venue_order_ids: Sequence[str]
    ) -> dict[str, OrderRow]:
        """``venue_order_id`` → order, for the ids asked about.

        The join a backfilled trade row cannot make for itself: the venue's
        trade history names an order id and no client order id, so this is
        where an execution re-read from a venue becomes a session's again.

        Scoped to the ids in hand rather than to a whole instrument — an
        account with a long history on one symbol would otherwise load every
        order it ever placed to attribute one page.
        """
        if not venue_order_ids:
            return {}
        result = await self.session.execute(
            select(OrderRow).where(
                OrderRow.api_id == api_id,
                OrderRow.venue_order_id.in_(list(set(venue_order_ids))),
            )
        )
        return {
            row.venue_order_id: row
            for row in result.scalars().all()
            if row.venue_order_id is not None
        }

    async def tickers_for(self, api_id: int) -> list[str]:
        """Every instrument this account has an order on file for.

        What a backfill walks, and it bootstraps itself: venues want a symbol
        on their history endpoints, and the orders TD recorded are the only
        record of which symbols an account has touched. Ordinarily sound — a
        submit is queued in-process before the order leaves, so an instrument
        with history has an order here even when every fill on it was lost.

        The exception is worth knowing, because nothing downstream can detect
        it: the writer's queue is bounded, so a long database outage drops
        rows. An instrument whose *only* order row was dropped is invisible
        here and will never be walked. ``HistoryWriter.dropped`` is the signal
        that this can have happened; a caller that knows better can name its
        instruments on the request instead.
        """
        result = await self.session.execute(
            select(OrderRow.universal_ticker)
            .where(OrderRow.api_id == api_id)
            .distinct()
        )
        return sorted(row[0] for row in result.all())

    async def api_ids_with_history(self) -> list[int]:
        """Every account that has an order on file.

        What a schedule sweeps. Derived from the record rather than from the
        ``apis`` table so a credential that has never traded is not asked
        about every tick — and so an account that has *stopped* trading still
        is, which is the one a per-account trigger would never reach.
        """
        result = await self.session.execute(select(OrderRow.api_id).distinct())
        return sorted(row[0] for row in result.all())

    async def earliest_ts(self, api_id: int, universal_ticker: str) -> float | None:
        """When this account first touched an instrument, or None.

        Where a walk that has never run starts from. An instrument has no
        history worth reading before the first order placed on it, and asking a
        venue for everything since the beginning of the account is a great deal
        of paging to discover the same thing.

        ``submitted_at`` rather than ``ts``, and the difference is a window of
        lost executions rather than a nicety. ``ts`` is the order's *last*
        state change and the upsert keeps pushing it forward, so an order
        placed at T and filled at T+5 carries ``ts = T+5`` — starting a walk
        there would skip every execution in between, on the one run that
        establishes the settlement line and can never be revisited afterwards.
        ``submitted_at`` is when we asked, and the upsert keeps the earliest.

        Coalesced because an order this account did not place has no submit
        time of ours; for the first walk there are none of those yet, but a
        later re-bootstrap could see them.
        """
        anchor = func.min(func.coalesce(OrderRow.submitted_at, OrderRow.ts))
        result = await self.session.execute(
            select(anchor).where(
                OrderRow.api_id == api_id,
                OrderRow.universal_ticker == universal_ticker,
            )
        )
        return result.scalar_one_or_none()


class FillRepository(BaseRepository[FillRow]):
    def __init__(self, session: AsyncSession) -> None:
        super().__init__(session, FillRow)

    async def bulk_upsert(self, rows: Sequence[Mapping[str, Any]]) -> int:
        """Insert or merge fills on ``(api_id, universal_ticker, fill_id)``.

        Price, quantity, side and time are deliberately absent from the update:
        the two sources describe one execution and must already agree on those.
        Letting a write settle a disagreement silently is how a double-counted
        or mispriced fill would reach a PnL figure with nothing to show for it.
        """
        if not rows:
            return 0
        stmt = _insert(self.session)(FillRow).values(list(rows))
        corrects = stmt.excluded.source == Source.BACKFILL
        stmt = stmt.on_conflict_do_update(
            index_elements=["api_id", "universal_ticker", "fill_id"],
            set_={
                "fee": case((corrects, stmt.excluded.fee), else_=FillRow.fee),
                "fee_asset": case(
                    (corrects, stmt.excluded.fee_asset), else_=FillRow.fee_asset
                ),
                "realized_pnl": func.coalesce(
                    stmt.excluded.realized_pnl, FillRow.realized_pnl
                ),
                "is_maker": func.coalesce(
                    stmt.excluded.is_maker, FillRow.is_maker
                ),
                "venue_order_id": func.coalesce(
                    stmt.excluded.venue_order_id, FillRow.venue_order_id
                ),
                "client_order_id": func.coalesce(
                    stmt.excluded.client_order_id, FillRow.client_order_id
                ),
                "session_id": func.coalesce(
                    stmt.excluded.session_id, FillRow.session_id
                ),
                "source": case(
                    (corrects, Source.BACKFILL), else_=FillRow.source
                ),
            },
        )
        await self.session.execute(stmt)
        await self.session.flush()
        return len(rows)

    async def attribute(
        self, api_id: int, venue_order_id: str, *, session_id: str, client_order_id: str
    ) -> int:
        """Attach a session to fills that arrived before their order did.

        Only fills still unattributed are touched — an attribution already
        recorded is the one written closest to the submit, and this pass knows
        strictly less than that one did.
        """
        result = await self.session.execute(
            FillRow.__table__.update()
            .where(
                FillRow.api_id == api_id,
                FillRow.venue_order_id == venue_order_id,
                FillRow.session_id.is_(None),
            )
            .values(session_id=session_id, client_order_id=client_order_id)
        )
        await self.session.flush()
        return int(result.rowcount or 0)

    async def list_for_session(
        self,
        session_id: str,
        *,
        before_ts: float | None = None,
        before_id: int | None = None,
        limit: int = 100,
    ) -> Sequence[FillRow]:
        q = select(FillRow).where(FillRow.session_id == session_id)
        q = _before(q, FillRow, before_ts, before_id)
        q = q.order_by(FillRow.ts.desc(), FillRow.id.desc()).limit(limit)
        return (await self.session.execute(q)).scalars().all()

    async def list_unattributed(
        self,
        *,
        before_ts: float | None = None,
        before_id: int | None = None,
        limit: int = 100,
    ) -> Sequence[FillRow]:
        """Executions on our accounts that no session of ours placed.

        Placed by hand, by another tool, or before the account was attached —
        and also, until the walk that finds them catches up, our own fills
        whose order is not on file yet. The two look identical here, which is
        the point of showing them at all: the second kind is a hole in the
        record and nothing else lists it.

        ``ix_fills_session_ts_id`` covers this: nulls are indexed, so this
        reads the head of one index rather than scanning every fill ever
        recorded to find the few that are nobody's.
        """
        q = select(FillRow).where(FillRow.session_id.is_(None))
        q = _before(q, FillRow, before_ts, before_id)
        q = q.order_by(FillRow.ts.desc(), FillRow.id.desc()).limit(limit)
        return (await self.session.execute(q)).scalars().all()

    async def count_by_session(
        self, session_ids: Sequence[str]
    ) -> dict[str, int]:
        """``session_id`` → executions recorded, for the sessions asked about.

        One statement for a page of sessions rather than a count per row: the
        dashboard lists them all at once, and a query each is the shape that
        looks fine with three sessions and falls over with three hundred.

        A session missing from the result has no fills, which is not the same
        as having none *yet* — the settlement line says which, and this does
        not pretend to.
        """
        if not session_ids:
            return {}
        result = await self.session.execute(
            select(FillRow.session_id, func.count())
            .where(FillRow.session_id.in_(list(set(session_ids))))
            .group_by(FillRow.session_id)
        )
        return {row[0]: row[1] for row in result.all()}

    async def replay_for_session(self, session_id: str) -> Sequence[FillRow]:
        """Every fill of a session, oldest first — the order PnL replays in.

        Not paginated: cost matching has to start at the first execution, so a
        page of the most recent ones cannot answer anything.
        """
        result = await self.session.execute(
            select(FillRow)
            .where(FillRow.session_id == session_id)
            .order_by(FillRow.ts.asc(), FillRow.id.asc())
        )
        return result.scalars().all()


class CashFlowRepository(BaseRepository[CashFlowRow]):
    def __init__(self, session: AsyncSession) -> None:
        super().__init__(session, CashFlowRow)

    async def bulk_insert_ignore(self, rows: Sequence[Mapping[str, Any]]) -> int:
        """Insert, skipping movements already recorded.

        Ignore rather than upsert: a settled cash movement has no later
        correction to apply, unlike a fee. If a venue ever restates one it
        should arrive as its own reversing row, which is what the venue's own
        ledger does.
        """
        if not rows:
            return 0
        stmt = (
            _insert(self.session)(CashFlowRow)
            .values(list(rows))
            .on_conflict_do_nothing(index_elements=["api_id", "venue_id", "kind"])
        )
        await self.session.execute(stmt)
        await self.session.flush()
        return len(rows)

    async def list_for_api(
        self,
        api_id: int,
        *,
        kind: str | None = None,
        since_ts: float | None = None,
        limit: int = 500,
    ) -> Sequence[CashFlowRow]:
        q = select(CashFlowRow).where(CashFlowRow.api_id == api_id)
        if kind is not None:
            q = q.where(CashFlowRow.kind == kind)
        if since_ts is not None:
            q = q.where(CashFlowRow.ts >= since_ts)
        q = q.order_by(CashFlowRow.ts.desc(), CashFlowRow.id.desc()).limit(limit)
        return (await self.session.execute(q)).scalars().all()


class BackfillCursorRepository(BaseRepository[BackfillCursorRow]):
    def __init__(self, session: AsyncSession) -> None:
        super().__init__(session, BackfillCursorRow)

    async def get(
        self, api_id: int, stream: str, scope: str = ""
    ) -> BackfillCursorRow | None:
        result = await self.session.execute(
            select(BackfillCursorRow).where(
                BackfillCursorRow.api_id == api_id,
                BackfillCursorRow.stream == stream,
                BackfillCursorRow.scope == scope,
            )
        )
        return result.scalar_one_or_none()

    async def advance(
        self,
        api_id: int,
        stream: str,
        *,
        scope: str = "",
        confirmed_through_ts: float | None,
        last_id: str | None = None,
    ) -> None:
        """Record where a walk got to. The line never moves backward.

        Monotonic in SQL rather than by reading first, so two workers racing on
        one account cannot have the slower one's older watermark land last and
        un-confirm a window that was already settled. That makes a duplicated
        backfill run wasteful and nothing worse, which is the property the
        whole design leans on.

        ``confirmed_through_ts=None`` saves the resume point and leaves the
        line alone. A walk against a venue that pages newest-first has that
        state after every page but its last: it knows where to carry on and has
        confirmed nothing, because what it has read is the *recent* end of a
        window whose older half it has not reached.
        """
        if confirmed_through_ts is None:
            await self._keep_line(api_id, stream, scope, last_id)
            return
        stmt = _insert(self.session)(BackfillCursorRow).values(
            api_id=api_id,
            stream=stream,
            scope=scope,
            last_id=last_id,
            confirmed_through_ts=confirmed_through_ts,
        )
        stmt = stmt.on_conflict_do_update(
            index_elements=["api_id", "stream", "scope"],
            set_={
                "last_id": stmt.excluded.last_id,
                "confirmed_through_ts": stmt.excluded.confirmed_through_ts,
                "updated_at": func.now(),
            },
            where=BackfillCursorRow.confirmed_through_ts
            <= stmt.excluded.confirmed_through_ts,
        )
        await self.session.execute(stmt)
        await self.session.flush()

    async def _keep_line(
        self, api_id: int, stream: str, scope: str, last_id: str | None
    ) -> None:
        """Save the resume point without touching the settlement line.

        A fresh row starts at zero rather than at the caller's guess: nothing
        has been confirmed, and zero is what ``confirmed_through`` already reads
        as "this walk has never run".
        """
        stmt = _insert(self.session)(BackfillCursorRow).values(
            api_id=api_id,
            stream=stream,
            scope=scope,
            last_id=last_id,
            confirmed_through_ts=0.0,
        )
        stmt = stmt.on_conflict_do_update(
            index_elements=["api_id", "stream", "scope"],
            set_={"last_id": stmt.excluded.last_id, "updated_at": func.now()},
        )
        await self.session.execute(stmt)
        await self.session.flush()

    async def lines_for(
        self, api_ids: Sequence[int], streams: Sequence[str]
    ) -> dict[tuple[int, str, str], float]:
        """``(api_id, stream, scope)`` → line, for a page of accounts at once.

        One statement where the caller would otherwise issue one per account
        per session — the same argument the fill and order counts are batched
        under, and the board lists up to five hundred runs.
        """
        if not api_ids or not streams:
            return {}
        result = await self.session.execute(
            select(
                BackfillCursorRow.api_id,
                BackfillCursorRow.stream,
                BackfillCursorRow.scope,
                BackfillCursorRow.confirmed_through_ts,
            ).where(
                BackfillCursorRow.api_id.in_(list(set(api_ids))),
                BackfillCursorRow.stream.in_(list(streams)),
            )
        )
        return {(r.api_id, r.stream, r.scope): r.confirmed_through_ts for r in result}

    async def confirmed_through(
        self, api_id: int, streams: Sequence[str], scopes: Sequence[str] = ()
    ) -> float:
        """The weakest settlement line across the walks a figure depends on.

        The minimum, and zero when any of them has never run: a record is only
        as settled as its least-confirmed input, and a missing cursor is not an
        absent constraint but an unconfirmed one. Callers pass the streams
        their number actually reads — session PnL needs trades and orders,
        account PnL needs cash flows too, and the first can settle earlier.
        """
        if not streams:
            return 0.0
        q = select(
            BackfillCursorRow.stream,
            BackfillCursorRow.scope,
            BackfillCursorRow.confirmed_through_ts,
        ).where(
            BackfillCursorRow.api_id == api_id,
            BackfillCursorRow.stream.in_(list(streams)),
        )
        if scopes:
            q = q.where(BackfillCursorRow.scope.in_(list(scopes)))
        rows = (await self.session.execute(q)).all()

        wanted = {
            (stream, scope)
            for stream in streams
            for scope in (scopes or ("",))
        }
        seen = {(row.stream, row.scope): row.confirmed_through_ts for row in rows}
        if not wanted <= set(seen):
            return 0.0
        return min(seen[key] for key in wanted)


def _before(query: Any, model: Any, before_ts: float | None, before_id: int | None):
    """Apply a ``(ts, id)`` cursor, the same shape the log listing uses."""
    if before_ts is None:
        return query
    if before_id is None:
        return query.where(model.ts < before_ts)
    return query.where(
        (model.ts < before_ts)
        | ((model.ts == before_ts) & (model.id < before_id))
    )


__all__ = [
    "BackfillCursorRepository",
    "CashFlowRepository",
    "FillRepository",
    "OrderRepository",
]
