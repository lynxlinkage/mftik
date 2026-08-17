"""One backfill run: walk a venue's history and move the settlement line.

The rule the whole two-tier record rests on is here, and it is one sentence:
**an id resumes a catch-up, a timestamp resumes a tail.**

While a walk is behind — a long history being read for the first time, a page
cap reached — it resumes from the venue's own id, which is monotonic and
skips nothing. Once it has caught up, it stops keeping an id and resumes from
the settlement line instead, re-reading the window between that line and now on
every run. That window is small, the writes are idempotent, and re-reading it
is what makes a late-appearing row impossible to miss: a venue's history
endpoint can lag its own stream, so a trade can become visible *after* a walk
has already read past its id. Resuming by id there would skip it forever.

For the same reason the line never advances to ``now``. It advances to
``now - SAFETY_LAG``, and everything after that stays provisional until a later
run confirms it.

Nothing here needs to be the only run. Cursor advance is monotonic in SQL and
every write is an idempotent upsert, so two runs over one account produce the
same result as one, only slower. The lock exists for the one thing that is
genuinely owned — the API key's rate-limit budget, which a live trading session
is spending at the same time — and not for correctness.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import os
import time
import uuid
from collections.abc import Sequence
from dataclasses import dataclass, field
from typing import Any

from mftik.broker import Broker
from mftik.exchange.models import Fill
from mftik.exchange.tickers import UniversalTicker
from mftik_db.models.history import Source, Stream
from mftik_db.repositories import (
    BackfillCursorRepository,
    FillRepository,
    OrderRepository,
)
from mftik_db.session import session_scope

from mftik_td.backfill.reader import (
    DEFAULT_PAGE,
    HistoryPage,
    HistoryReaderFactory,
    NoHistoryReaderError,
)
from mftik_td.history import Scope, fill_row, order_row

logger = logging.getLogger(__name__)

#: How far behind the present a run may confirm. A venue's history endpoint can
#: lag its own stream, so confirming right up to ``now`` would bless a window
#: that can still gain a row.
SAFETY_LAG_S = 60.0

#: Pages per instrument per stream in one run. A first backfill on an old
#: account is bounded work, not an unbounded one: what this does not reach, the
#: next run resumes from, and the cursor it leaves behind is real progress.
MAX_PAGES_PER_WALK = 20

#: How long one account's backfill may hold its lock before another run may
#: assume it died. Generous — the cost of waiting is a delayed run.
LOCK_TTL_S = 900

#: Between pages. Not a rate limiter, a pace: the same key is being spent by a
#: live trading session, and history that arrives ten minutes late costs
#: nothing while a limited-out submit costs a fill.
PAGE_PAUSE_S = 0.2


def _safety_lag() -> float:
    return max(0.0, float(os.getenv("BACKFILL_SAFETY_LAG", str(SAFETY_LAG_S))))


@dataclass
class BackfillOutcome:
    """What one run managed. Partial progress is a success, not a failure."""

    api_id: int
    ok: bool = True
    tickers: list[str] = field(default_factory=list)
    fills: int = 0
    orders: int = 0
    confirmed_through_ts: float = 0.0
    reason: str = ""
    #: Instruments this run could not walk, and why. Non-empty is not a failed
    #: run — the rest of the account still moved — but it is the only place a
    #: permanently unwalkable scope becomes visible.
    failed: dict[str, str] = field(default_factory=dict)

    @property
    def skipped(self) -> bool:
        return not self.tickers and self.ok


class BackfillExecutor:
    """Re-reads one account's history and advances its settlement line."""

    def __init__(
        self,
        *,
        broker: Broker,
        factory: HistoryReaderFactory,
        load_api: Any,
        scope: Scope | None = None,
        safety_lag: float | None = None,
        max_pages: int = MAX_PAGES_PER_WALK,
        page_size: int = DEFAULT_PAGE,
        page_pause: float = PAGE_PAUSE_S,
        lock_ttl: int = LOCK_TTL_S,
    ) -> None:
        self._broker = broker
        self._factory = factory
        self._load_api = load_api
        self._scope: Scope = scope or session_scope
        self._safety_lag = safety_lag if safety_lag is not None else _safety_lag()
        self._max_pages = max_pages
        self._page_size = page_size
        self._page_pause = page_pause
        self._lock_ttl = lock_ttl

    async def run(
        self,
        api_id: int,
        *,
        tickers: Sequence[str] = (),
        reason: str = "",
    ) -> BackfillOutcome:
        """Walk ``api_id``'s history. Never raises; refusals come back as data."""
        token = uuid.uuid4().hex
        if not await self._lock(api_id, token):
            logger.info("TD backfill skipped api_id=%s: already running", api_id)
            return BackfillOutcome(
                api_id=api_id, reason="another run holds this account"
            )
        try:
            return await self._run_locked(api_id, tickers, reason)
        except Exception as exc:
            logger.exception("TD backfill failed api_id=%s", api_id)
            return BackfillOutcome(api_id=api_id, ok=False, reason=str(exc))
        finally:
            await self._unlock(api_id, token)

    async def _run_locked(
        self, api_id: int, tickers: Sequence[str], reason: str
    ) -> BackfillOutcome:
        row = await self._load_api(api_id)
        if row is None:
            return BackfillOutcome(
                api_id=api_id, ok=False, reason=f"no api row for {api_id}"
            )
        try:
            reader = await self._factory.create(row.venue, row)
        except NoHistoryReaderError as exc:
            # Not a failure: the venue simply cannot be re-read, and the record
            # goes on saying so by leaving its cursors where they are.
            logger.info("TD backfill has no reader api_id=%s: %s", api_id, exc)
            return BackfillOutcome(api_id=api_id, reason=str(exc))

        scopes = list(tickers)
        if not scopes:
            async with self._scope() as db:
                scopes = await OrderRepository(db).tickers_for(api_id)
        if not scopes:
            logger.info(
                "TD backfill has nothing to walk api_id=%s (no orders on file)",
                api_id,
            )
            return BackfillOutcome(api_id=api_id, reason="no instruments on file")

        outcome = BackfillOutcome(api_id=api_id)
        await reader.connect()
        try:
            for scope in scopes:
                # Guarded per instrument, because these failures are
                # data-shaped and deterministic: a symbol the venue delisted,
                # one that no longer resolves. Left to propagate, the first
                # such instrument would end the run — and since the list comes
                # back sorted, every instrument after it in the alphabet would
                # be skipped on *every* run, forever, rather than retried on
                # the next tick. Partial progress is the property this design
                # already claims; one bad scope must not cost the rest.
                try:
                    ticker = UniversalTicker.resolve(scope)
                    # Orders first, always: a Binance trade row names an order
                    # and no session, so the fills walk can only attribute what
                    # the orders walk has already put on file.
                    outcome.orders += await self._walk(
                        api_id, reader, ticker, Stream.ORDERS
                    )
                    outcome.fills += await self._walk(
                        api_id, reader, ticker, Stream.TRADES
                    )
                except Exception as exc:
                    logger.exception(
                        "TD backfill failed on api_id=%s scope=%s", api_id, scope
                    )
                    outcome.failed[scope] = str(exc)
                    continue
                outcome.tickers.append(str(ticker))
        finally:
            with contextlib.suppress(Exception):
                await reader.close()

        async with self._scope() as db:
            outcome.confirmed_through_ts = await BackfillCursorRepository(
                db
            ).confirmed_through(
                api_id, [Stream.TRADES, Stream.ORDERS], scopes=outcome.tickers
            )
        if outcome.failed:
            logger.warning(
                "TD backfill could not walk %d scope(s) api_id=%s: %s",
                len(outcome.failed),
                api_id,
                ", ".join(sorted(outcome.failed)),
            )
        logger.info(
            "TD backfill done api_id=%s reason=%s tickers=%d orders=%d fills=%d "
            "confirmed_through=%.0f",
            api_id,
            reason or "unspecified",
            len(outcome.tickers),
            outcome.orders,
            outcome.fills,
            outcome.confirmed_through_ts,
        )
        return outcome

    # --- the walk ----------------------------------------------------------

    async def _walk(
        self,
        api_id: int,
        reader: Any,
        ticker: UniversalTicker,
        stream: str,
    ) -> int:
        """One instrument, one stream, until drained or out of pages."""
        fetch = getattr(
            reader,
            "fetch_orders" if stream == Stream.ORDERS else "fetch_my_trades",
            None,
        )
        if fetch is None:
            # A venue that serves one read and not the other still gets the one
            # it serves; the missing stream's line simply never moves.
            return 0

        cursor, since = await self._resume_from(api_id, ticker, stream)
        ceiling = time.time() - self._safety_lag
        written = 0
        last_ts = 0.0
        drained = False

        for page_no in range(self._max_pages):
            page: HistoryPage[Any] = await fetch(
                ticker, cursor=cursor, since_ts=since, limit=self._page_size
            )
            if page.rows:
                written += await self._persist(api_id, ticker, stream, page.rows)
                last_ts = max(row.ts for row in page.rows)
            cursor = page.next_cursor
            since = None
            if cursor is None:
                drained = True
                break
            if page_no + 1 < self._max_pages:
                await self._pause()

        # Caught up: stop carrying an id and resume from the line next time, so
        # a row that becomes visible behind us is read again rather than
        # skipped. True whichever way the venue pages — a drained walk has seen
        # everything up to the ceiling either way.
        if drained:
            await self._advance(api_id, ticker, stream, ceiling, None)
            return written

        # Still behind, and here the two directions part company. An ascending
        # walk's newest row is the frontier of what it has verified, so the line
        # moves to it. A descending walk's first page is the *newest* rows and
        # its unread pages are the older ones: what it has verified is
        # ``[oldest row read, ceiling]``, an interval whose lower bound is still
        # moving, and no single ``confirmed_through_ts`` can say that. Advancing
        # to the newest row there would mark the account's whole history settled
        # while the pages holding most of it had never been opened — a line that
        # has gone too far, which is the one failure this design must not have.
        # So it keeps its place and confirms nothing until it drains.
        if getattr(reader, "pages_newest_first", False):
            await self._advance(api_id, ticker, stream, None, cursor)
        elif last_ts:
            await self._advance(api_id, ticker, stream, min(last_ts, ceiling), cursor)
        return written

    async def _resume_from(
        self, api_id: int, ticker: UniversalTicker, stream: str
    ) -> tuple[str | None, float | None]:
        """Where this walk picks up: an id mid-catch-up, else a timestamp."""
        async with self._scope() as db:
            row = await BackfillCursorRepository(db).get(
                api_id, stream, str(ticker)
            )
            if row is not None and row.last_id:
                return row.last_id, None
            if row is not None and row.confirmed_through_ts:
                return None, row.confirmed_through_ts
            # Never walked, so there is no cursor row yet — this is where the
            # first one comes from. Start at the account's own first order on
            # the instrument: there is no history worth reading before it, and
            # asking a venue for everything since the beginning is a great deal
            # of paging to discover the same thing.
            earliest = await OrderRepository(db).earliest_ts(api_id, str(ticker))
        return None, earliest

    async def _advance(
        self,
        api_id: int,
        ticker: UniversalTicker,
        stream: str,
        confirmed_through_ts: float | None,
        last_id: str | None,
    ) -> None:
        """Record where the walk got to. ``None`` leaves the line where it is."""
        async with self._scope() as db:
            await BackfillCursorRepository(db).advance(
                api_id,
                stream,
                scope=str(ticker),
                confirmed_through_ts=confirmed_through_ts,
                last_id=last_id,
            )

    async def _persist(
        self,
        api_id: int,
        ticker: UniversalTicker,
        stream: str,
        rows: list[Any],
    ) -> int:
        async with self._scope() as db:
            if stream == Stream.ORDERS:
                return await OrderRepository(db).bulk_upsert(
                    [
                        order_row(order, api_id=api_id, source=Source.BACKFILL)
                        for order in rows
                    ]
                )
            return await self._persist_fills(db, api_id, rows)

    async def _persist_fills(
        self, db: Any, api_id: int, fills: list[Fill]
    ) -> int:
        """Write executions, attributed through the orders already on file.

        Two joins, because the venues do not agree on what a trade row says. A
        Binance trade names an order id and nothing else, so the only route to
        a session is the order that id belongs to. Bybit and Gate echo back our
        own link id on the trade itself — an execution that arrives already
        naming the session that placed it — and reaching that session still
        needs ``orders``, but by the key the fill itself carries.

        The venue's id is preferred where both answer. Ours is minted from a
        slot that wraps and is reused, so a client order id from far enough
        back can name an order a later session also used; the venue's is
        unambiguous for the life of the account.

        A fill neither join reaches is written unattributed rather than guessed
        at — that is a real state, and the next run has another chance once the
        orders walk has caught up.
        """
        rows = [fill_row(fill, api_id=api_id, source=Source.BACKFILL) for fill in fills]
        repo = OrderRepository(db)
        owners = await repo.by_venue_ids(
            api_id, [row["venue_order_id"] for row in rows if row["venue_order_id"]]
        )
        # Keyed the way ``orders`` keys itself — ``order_key`` is the client
        # order id whenever there is one. Empty for a venue that sends none, and
        # the read costs nothing then.
        by_cid = await repo.owners_for(
            [
                (api_id, row["client_order_id"])
                for row in rows
                if row["client_order_id"]
            ]
        )
        for row in rows:
            owner = owners.get(row["venue_order_id"] or "")
            if owner is not None:
                # Only when the venue gave us none. Bybit and Gate put the link
                # id on the trade row itself, and the matched order can carry a
                # null one — an order discovered by an earlier backfill, say —
                # so assigning unconditionally would throw away the real id on
                # its way to a first insert, where the upsert's coalesce cannot
                # save it.
                row["client_order_id"] = (
                    row["client_order_id"] or owner.client_order_id
                )
                if owner.session_id is not None:
                    row["session_id"] = owner.session_id
                    continue
            # The order is not on file, or is on file as nobody's. Our own id
            # on the trade row is the second chance, and the only one for a
            # fill whose order row exists but has no venue id yet — a submit TD
            # recorded before the ack came back, which is precisely the row the
            # first join cannot see.
            cid = row["client_order_id"]
            session_id = by_cid.get((api_id, cid)) if cid else None
            if session_id is not None:
                row["session_id"] = session_id
        return await FillRepository(db).bulk_upsert(rows)

    async def _pause(self) -> None:
        if self._page_pause > 0:
            await asyncio.sleep(self._page_pause)

    # --- the lock ----------------------------------------------------------

    def _lock_key(self, api_id: int) -> str:
        return f"{self._broker.config.key_prefix}:backfill:lock:{api_id}"

    async def _lock(self, api_id: int, token: str) -> bool:
        """Claim this account, or decline the run.

        Advisory. Nothing is wrong with two runs over one account except the
        rate-limit budget they share with whatever is trading on the same key,
        which is the whole reason this exists.
        """
        try:
            got = await self._broker.redis.set(
                self._lock_key(api_id), token, nx=True, ex=self._lock_ttl
            )
        except Exception:
            # A lock we cannot take is not a reason to skip work that is safe
            # to repeat.
            logger.warning("TD backfill lock unavailable api_id=%s", api_id)
            return True
        return bool(got)

    async def _unlock(self, api_id: int, token: str) -> None:
        key = self._lock_key(api_id)
        try:
            if await self._broker.redis.get(key) == token:
                await self._broker.redis.delete(key)
        except Exception:
            logger.debug("TD backfill unlock failed api_id=%s", api_id, exc_info=True)


__all__ = [
    "LOCK_TTL_S",
    "MAX_PAGES_PER_WALK",
    "PAGE_PAUSE_S",
    "SAFETY_LAG_S",
    "BackfillExecutor",
    "BackfillOutcome",
]
