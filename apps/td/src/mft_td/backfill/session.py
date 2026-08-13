"""Serves ``td.backfill`` for as long as the process lives.

Deliberately unlike the trading sessions next to it. Those hold a fencing lease
because an account is leased to a strategy and two of them disagreeing about
who owns it matters. A history read is owned by nobody: any TD can load the
credential and ask, the answer is the same whoever asked, and the writes it
produces are idempotent — so this session needs no attach, no heartbeat and no
expiry. It is up whenever the process is, and it will answer for an ``api_id``
this process has never traded.

That last part is the point of the shape. A keyed subject would park a request
in a list until the account's owner picked it up, which for an account nobody
is trading any more is forever — and an account nobody is trading is exactly
one whose record nothing else is going to repair.

Requests are acked and then run out of band. A walk is minutes of venue round
trips and this loop is a single consumer; awaiting one inside it would stall
every other account behind it.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any

from mft.broker import Broker
from mft.broker.request import IncomingRequest
from mft.protocol import (
    TD_BACKFILL_RESULT,
    Envelope,
    TdBackfill,
    TdBackfillResult,
    Topics,
)

from mft_td.backfill.executor import BackfillExecutor, BackfillOutcome

logger = logging.getLogger(__name__)

#: Accounts backfilling at once in one process. A ceiling on concurrency, not a
#: rate limiter — each account paces itself and holds its own lock; this is the
#: cruder guard behind that, so a burst of requests cannot open a venue
#: connection per account all at once.
MAX_RUNS_IN_FLIGHT = 4

#: How long a stop may wait on walks already in flight. A run is up to
#: ``MAX_PAGES_PER_WALK`` pages per stream per instrument of venue round trips,
#: which is minutes — and a teardown that outlives the container's stop timeout
#: is SIGKILLed, taking with it the history drain ``app.py`` sequences after
#: this. What an abandoned walk loses is a cursor advance, which the next run
#: redoes.
STOP_GRACE_S = 5.0


class BackfillSession:
    """Takes backfill requests off ``td.backfill`` and runs them."""

    def __init__(
        self,
        broker: Broker,
        executor: BackfillExecutor,
        *,
        max_in_flight: int = MAX_RUNS_IN_FLIGHT,
        stop_grace: float = STOP_GRACE_S,
    ) -> None:
        self._broker = broker
        self._executor = executor
        self._max_in_flight = max_in_flight
        self._stop_grace = stop_grace
        self._runs: set[asyncio.Task[Any]] = set()
        self._stop = asyncio.Event()
        self._task: asyncio.Task[Any] | None = None

    @property
    def in_flight(self) -> int:
        return len(self._runs)

    async def start(self) -> None:
        if self._task is not None:
            return
        self._stop.clear()
        self._task = asyncio.create_task(self._serve(), name="td-backfill")
        logger.info(
            "TD backfill session listening subject=%s", Topics.td_backfill()
        )

    async def stop(self) -> None:
        self._stop.set()
        # Neither the serve loop nor a running walk is cancelled: both end by
        # touching a pooled Redis connection, and cancelling one mid-command
        # hands the connection back with its reply unread, which breaks
        # whatever borrows it next. ``serve`` rechecks the stop event between
        # polls, so this is bounded by a poll plus the venue's own timeout.
        pending = [t for t in (self._task, *self._runs) if t is not None]
        if pending:
            done, waiting = await asyncio.wait(pending, timeout=self._stop_grace)
            if waiting:
                # Abandoned rather than awaited. A walk in flight is minutes of
                # venue round trips and holding the stop path open for it costs
                # more than what it would have finished: the cursor it did not
                # advance is redone by the next run, where a SIGKILLed teardown
                # also skips the history drain that follows this.
                logger.warning(
                    "TD backfill left %d run(s) unfinished at stop", len(waiting)
                )
        self._task = None
        self._runs.clear()
        logger.info("TD backfill session stopped")

    async def _serve(self) -> None:
        try:
            async for req in self._broker.serve(
                Topics.td_backfill(), stop=self._stop
            ):
                await self._handle(req)
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("TD backfill serve loop failed")

    async def _handle(self, req: IncomingRequest) -> None:
        try:
            payload = TdBackfill.model_validate(req.envelope.payload or {})
        except Exception as exc:
            await self._reply(
                req, BackfillOutcome(api_id=0, ok=False, reason=f"invalid: {exc}")
            )
            return

        if len(self._runs) >= self._max_in_flight:
            # Refused rather than queued: the sender is a schedule or a detach
            # that will ask again, and a request held here is one nothing can
            # see the state of.
            await self._reply(
                req,
                BackfillOutcome(
                    api_id=payload.api_id,
                    reason=f"{len(self._runs)} runs already in flight",
                ),
            )
            return

        task = asyncio.create_task(
            self._run(req, payload), name=f"td-backfill-{payload.api_id}"
        )
        self._runs.add(task)
        task.add_done_callback(self._runs.discard)

    async def _run(self, req: IncomingRequest, payload: TdBackfill) -> None:
        outcome = await self._executor.run(
            payload.api_id, tickers=payload.tickers, reason=payload.reason
        )
        await self._reply(req, outcome)

    async def _reply(self, req: IncomingRequest, outcome: BackfillOutcome) -> None:
        """Answer the caller, whatever happened.

        Answered even when nothing was done. A caller waiting on this has no
        other way to learn the answer is never coming, and a run that was
        skipped is a different fact from one that failed.

        Unless nobody asked: the triggers post rather than request, because a
        schedule and a shutdown have no use for a result they would have to
        wait minutes for. A missing ``reply_to`` is that, not an error.
        """
        if not req.envelope.reply_to:
            return
        try:
            await req.reply(
                Envelope[TdBackfillResult].wrap(
                    TdBackfillResult(
                        api_id=outcome.api_id,
                        ok=outcome.ok,
                        tickers=list(outcome.tickers),
                        fills=outcome.fills,
                        orders=outcome.orders,
                        confirmed_through_ts=outcome.confirmed_through_ts,
                        reason=outcome.reason,
                    ),
                    type=TD_BACKFILL_RESULT,
                    source="td",
                )
            )
        except Exception:
            logger.exception("TD backfill reply failed api_id=%s", outcome.api_id)


__all__ = ["MAX_RUNS_IN_FLIGHT", "BackfillSession"]
