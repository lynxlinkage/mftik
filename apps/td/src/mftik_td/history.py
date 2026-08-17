"""TD → Postgres history writer — the live tier of the trading record.

TD is the only process that sees every order and every fill for an account, so
it is the only one that can record them as they happen. It writes through a
queue rather than inline, for one reason: the fill path is a synchronous
callback driven by the venue's socket pump, and a database round trip taken
there would stall the stream that feeds the OMS, the ledger and every strategy
attached to the account. Recording history must never be able to slow trading
down, so it is decoupled from it by construction.

The queue is **bounded**, and a full queue drops rather than blocks. That is
the deliberate choice: a database that is down or slow would otherwise turn
into unbounded memory growth in a process holding live positions. A dropped row
is recoverable — the backfill re-reads the same window from the venue and puts
it back — while an out-of-memory TD is not. Drops are counted and logged so the
loss is visible rather than merely survived.

Nothing here is authoritative. Everything it writes carries ``source=stream``,
which is the record saying "timely, possibly incomplete". The backfill is what
turns a window into a settled one.
"""

from __future__ import annotations

import asyncio
import logging
import os
from collections.abc import Callable
from contextlib import AbstractAsyncContextManager
from typing import Any

from mftik.exchange.models import Fill, Order
from mftik_db.models.history import Attribution, Source
from mftik_db.repositories import FillRepository, OrderRepository
from mftik_db.session import session_scope

logger = logging.getLogger(__name__)

#: Opens a unit of work. ``mftik_db.session.session_scope`` in production; tests
#: pass one backed by sqlite so the writer can be exercised without a server.
Scope = Callable[[], AbstractAsyncContextManager[Any]]

ORDERS = "orders"
FILLS = "fills"

#: Rows held in memory before writes start being dropped. Sized to swallow a
#: database blip of a few seconds at a busy account's event rate, not an
#: outage — see the module docstring for why the ceiling exists at all.
DEFAULT_MAX_QUEUE = 10_000


def _batch_size() -> int:
    return max(1, int(os.getenv("HISTORY_BATCH_SIZE", "200")))


def _flush_interval() -> float:
    return max(0.1, float(os.getenv("HISTORY_FLUSH_INTERVAL", "1.0")))


def _max_queue() -> int:
    return max(100, int(os.getenv("HISTORY_MAX_QUEUE", str(DEFAULT_MAX_QUEUE))))


def order_row(
    order: Order,
    *,
    api_id: int,
    session_id: str | None = None,
    submitted_at: float | None = None,
    source: str = Source.STREAM,
) -> dict[str, Any]:
    """One order as the table holds it.

    ``session_id`` is only ever passed on the submit path, which is the one
    moment TD holds both it and the ``client_order_id``. Every later update —
    a venue push, a sweep, a resolve — leaves it None and lets the upsert keep
    what the submit recorded.

    ``attribution`` is ``external`` unless a session is named, because at any
    other point TD genuinely cannot tell one of ours from an order placed by
    hand. The upsert never downgrades a row already marked ``direct``, so the
    honest default here costs the real owner nothing.
    """
    key = order.client_order_id or order.order_id
    return {
        "api_id": api_id,
        "order_key": key,
        "client_order_id": order.client_order_id,
        "venue_order_id": order.order_id or None,
        "session_id": session_id,
        # Left to the backfill, which is the pass that needs it: at submit the
        # session is known outright, so decoding the slot would answer a
        # question nobody is asking.
        "cid_slot": None,
        "strategy": None,
        "attribution": (
            Attribution.DIRECT if session_id else Attribution.EXTERNAL
        ),
        "universal_ticker": order.universal_ticker,
        "side": order.side.value,
        "order_type": order.type.value,
        "status": order.status.value,
        "qty": order.qty,
        "price": order.price,
        "filled_qty": order.filled_qty,
        "avg_price": order.avg_price,
        "submitted_at": submitted_at,
        "ts": order.ts,
        "source": source,
    }


def fill_row(
    fill: Fill, *, api_id: int, source: str = Source.STREAM
) -> dict[str, Any]:
    """One execution as the table holds it, session still unresolved.

    The session is filled in at flush time from ``orders``, not here: this is
    called from the socket pump, and the answer lives in the database.
    """
    return {
        "api_id": api_id,
        "fill_id": fill.fill_id,
        "universal_ticker": fill.universal_ticker,
        "venue_order_id": fill.order_id or None,
        "client_order_id": fill.client_order_id,
        "session_id": None,
        "side": fill.side.value,
        "price": fill.price,
        "qty": fill.qty,
        "fee": fill.fee,
        "fee_asset": fill.fee_asset,
        "realized_pnl": None,
        "is_maker": None,
        "ts": fill.ts,
        "source": source,
    }


class HistoryWriter:
    """Batches order and fill rows onto Postgres, off the trading path."""

    def __init__(
        self,
        *,
        scope: Scope | None = None,
        batch_size: int | None = None,
        flush_interval: float | None = None,
        max_queue: int | None = None,
    ) -> None:
        self._scope: Scope = scope or session_scope
        self._batch_size = batch_size or _batch_size()
        self._flush_interval = (
            flush_interval if flush_interval is not None else _flush_interval()
        )
        self._queue: asyncio.Queue[tuple[str, dict[str, Any]]] = asyncio.Queue(
            maxsize=max_queue or _max_queue()
        )
        self._task: asyncio.Task[None] | None = None
        self._stop = asyncio.Event()
        self._dropped = 0
        self._written = 0

    @property
    def dropped(self) -> int:
        """Rows the queue refused. Non-zero means history has a hole in it."""
        return self._dropped

    @property
    def written(self) -> int:
        return self._written

    @property
    def pending(self) -> int:
        return self._queue.qsize()

    async def start(self) -> None:
        if self._task is not None:
            return
        self._stop.clear()
        self._task = asyncio.create_task(self._run(), name="td-history")
        logger.info(
            "TD history writer started (batch=%d interval=%.1fs queue=%d)",
            self._batch_size,
            self._flush_interval,
            self._queue.maxsize,
        )

    async def stop(self) -> None:
        """Drain what is queued, then stop.

        Called on the way down, where the queue is the only copy of anything
        not yet written. A best-effort drain here is cheap; what it misses the
        backfill re-reads.
        """
        if self._task is None:
            return
        self._stop.set()
        try:
            await self._task
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("TD history writer stopped badly")
        self._task = None
        await self.flush()
        if self._dropped:
            logger.warning(
                "TD history writer dropped %d row(s) this run", self._dropped
            )
        logger.info("TD history writer stopped (wrote %d row(s))", self._written)

    # --- recording ---------------------------------------------------------

    def record_order(
        self,
        order: Order,
        *,
        api_id: int,
        session_id: str | None = None,
        submitted_at: float | None = None,
    ) -> None:
        """Queue an order's current state. Never blocks, never raises."""
        self._offer(
            ORDERS,
            order_row(
                order,
                api_id=api_id,
                session_id=session_id,
                submitted_at=submitted_at,
            ),
        )

    def record_fill(self, fill: Fill, *, api_id: int) -> None:
        """Queue an execution. Never blocks, never raises."""
        self._offer(FILLS, fill_row(fill, api_id=api_id))

    def _offer(self, kind: str, row: dict[str, Any]) -> None:
        try:
            self._queue.put_nowait((kind, row))
        except asyncio.QueueFull:
            self._dropped += 1
            # One line per drop would itself become the problem under a long
            # outage; the count is the number that matters and stop() reports
            # it. The first drop is worth saying out loud.
            if self._dropped == 1:
                logger.error(
                    "TD history queue full — dropping rows; the backfill will "
                    "have to recover this window"
                )

    # --- draining ----------------------------------------------------------

    async def flush(self) -> None:
        """Write one batch now, rather than at the next interval.

        Public because two callers outside the loop need it: shutdown, where
        the queue is the only copy of what it holds, and tests, which have no
        interest in waiting out a timer.
        """
        await self._flush(self._drain())

    async def _run(self) -> None:
        while not self._stop.is_set():
            batch = self._drain()
            if batch:
                wrote = await self._flush(batch)
                # A backlog is drained at whatever rate the database will take,
                # not one batch per interval — waiting out the timer with rows
                # still queued is how a burst turns into dropped history. Only
                # on success, so a database that is down is retried on the
                # interval rather than in a tight loop.
                if wrote and not self._queue.empty():
                    continue
            try:
                await asyncio.wait_for(
                    self._stop.wait(), timeout=self._flush_interval
                )
            except TimeoutError:
                pass
            except asyncio.CancelledError:
                raise

    def _drain(self) -> list[tuple[str, dict[str, Any]]]:
        rows: list[tuple[str, dict[str, Any]]] = []
        while len(rows) < self._batch_size:
            try:
                rows.append(self._queue.get_nowait())
            except asyncio.QueueEmpty:
                break
        return rows

    async def _flush(self, batch: list[tuple[str, dict[str, Any]]]) -> bool:
        """Write one batch. Orders first, so fills can find their session."""
        if not batch:
            return True
        orders = _last_per_key(
            [row for kind, row in batch if kind == ORDERS],
            key=lambda row: (row["api_id"], row["order_key"]),
            merge=_keep_attribution,
        )
        fills = _last_per_key(
            [row for kind, row in batch if kind == FILLS],
            key=lambda row: (
                row["api_id"],
                row["universal_ticker"],
                row["fill_id"],
            ),
        )
        try:
            async with self._scope() as db:
                if orders:
                    await OrderRepository(db).bulk_upsert(orders)
                if fills:
                    await _resolve_sessions(db, fills)
                    await FillRepository(db).bulk_upsert(fills)
            self._written += len(batch)
            return True
        except Exception:
            # Swallowed on purpose: a failed write is a hole the backfill
            # fills, and raising here would take down the flush loop and turn
            # one hole into every hole after it.
            logger.exception(
                "TD history flush failed (%d order(s), %d fill(s))",
                len(orders),
                len(fills),
            )
            return False


def _keep_attribution(
    winner: dict[str, Any], loser: dict[str, Any]
) -> dict[str, Any]:
    """Carry a session across a collapse, whichever row was newer.

    Within one batch the submit is the only writer that knows the owner, and it
    is also the earliest — so "latest wins" applied on its own would drop the
    one field nothing downstream can reconstruct.
    """
    if winner.get("session_id") is None and loser.get("session_id"):
        winner = {
            **winner,
            "session_id": loser["session_id"],
            "attribution": loser["attribution"],
        }
    if winner.get("submitted_at") is None and loser.get("submitted_at"):
        winner = {**winner, "submitted_at": loser["submitted_at"]}
    return winner


def _last_per_key(
    rows: list[dict[str, Any]],
    *,
    key: Callable[[dict[str, Any]], tuple[Any, ...]],
    merge: Callable[[dict[str, Any], dict[str, Any]], dict[str, Any]] | None = None,
) -> list[dict[str, Any]]:
    """Collapse rows that share a uniqueness key, keeping the newest by ``ts``.

    Postgres refuses an ``ON CONFLICT`` statement whose own values collide on
    the key — it will not affect one row twice in a single command — so a batch
    holding two states of one order, or a fill the venue redelivered after a
    reconnect, would fail as a whole rather than in part. Collapsing here is
    what keeps one duplicate from costing the entire batch.
    """
    merged: dict[tuple[Any, ...], dict[str, Any]] = {}
    for row in rows:
        at = key(row)
        held = merged.get(at)
        if held is None:
            merged[at] = row
            continue
        winner = row if row["ts"] >= held["ts"] else held
        loser = held if winner is row else row
        merged[at] = merge(winner, loser) if merge is not None else winner
    return list(merged.values())


async def _resolve_sessions(db: Any, fills: list[dict[str, Any]]) -> None:
    """Attach each fill to the session that placed its order.

    Read back from ``orders`` rather than carried in memory, because the answer
    outlives the process: a fill on an order placed before the last restart is
    still attributable, and a map rebuilt at startup would not know it. Orders
    in the same batch are already written by the time this runs.

    A fill whose order is not on file is left unattributed rather than guessed
    at. That is a real state — an order placed elsewhere, or one from before
    this table existed — and the backfill has a second chance at it.
    """
    keys = [
        (row["api_id"], row["client_order_id"])
        for row in fills
        if row.get("client_order_id")
    ]
    if not keys:
        return
    owners = await OrderRepository(db).owners_for(keys)
    for row in fills:
        cid = row.get("client_order_id")
        if not cid:
            continue
        owner = owners.get((row["api_id"], cid))
        if owner is not None:
            row["session_id"] = owner


__all__ = ["FILLS", "ORDERS", "HistoryWriter", "fill_row", "order_row"]
