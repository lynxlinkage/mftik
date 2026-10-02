"""Serves ``td.backfill`` for as long as the process lives.

The walk belongs to the account worker (F35). Each request is forwarded
to ``td.account.{api_id}`` and the worker's reply is what the caller
gets. When nobody is subscribed there — a non-paper account, until a
worker is spawned for it — this process runs the walk itself and logs
that. The fallback goes away once every bound account has a worker.

Deliberately unlike the trading sessions next to it. Those are held by one
process at a time, because an account is traded by one strategy and two of them
disagreeing about who owns it matters. A history read is owned by nobody: any
TD can load the credential and ask, the answer is the same whoever asked, and
the writes it produces are idempotent — so this session needs no owner and no
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

from mftik.broker import Broker, NoRespondersError, RequestTimeoutError
from mftik.broker.request import IncomingRequest
from mftik.protocol import (
    TD_BACKFILL,
    TD_BACKFILL_RESULT,
    Envelope,
    TdBackfill,
    TdBackfillResult,
    Topics,
)

from mftik_td.backfill.executor import BackfillExecutor, BackfillOutcome
from mftik_td.backfill.trigger import REQUEST_TIMEOUT_S

logger = logging.getLogger(__name__)

#: Accounts backfilling at once in this process. Only the fallback path
#: counts: a request the account worker accepted is not one of these.
#: A ceiling on concurrency, not a rate limiter — each account paces
#: itself and holds its own lock; this is the cruder guard behind that,
#: so a burst of requests cannot open a venue connection per account
#: all at once.
MAX_RUNS_IN_FLIGHT = 4

#: How long to wait for the account worker's accept. The same budget a
#: detach already spends asking (:data:`REQUEST_TIMEOUT_S`). The walk
#: is not what this waits for: the worker acks, then runs out of band.
#: No responders give up sooner, on the broker's own re-ask ceiling.
FORWARD_TIMEOUT_S = REQUEST_TIMEOUT_S


def in_flight_reason(running: int) -> str:
    """What a saturated backfill answers with, instead of queueing.

    The account worker uses the same sentence when that account already
    has a run. A new refusal would be a second way to say the same thing.
    """
    return f"{running} runs already in flight"

#: How long the serve loop waits before rebuilding itself after an exception it
#: did not expect. ``Broker.serve`` already survives what it knows how to
#: survive, so this only paces the failures nothing has a name for yet.
SERVE_RESTART_DELAY_S = 1.0

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
        instance: str = "td",
        max_in_flight: int = MAX_RUNS_IN_FLIGHT,
        stop_grace: float = STOP_GRACE_S,
        forward_timeout: float = FORWARD_TIMEOUT_S,
    ) -> None:
        self._broker = broker
        self._executor = executor
        #: Whose queue this serves. Backfill loads the credential and opens a
        #: venue connection with it, so which host runs it is the compliance
        #: question — see :meth:`Topics.td_backfill`.
        self._instance = instance
        self._max_in_flight = max_in_flight
        self._stop_grace = stop_grace
        self._forward_timeout = forward_timeout
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
            "TD backfill session listening subject=%s",
            Topics.td_backfill(self._instance),
        )

    async def stop(self) -> None:
        self._stop.set()
        # Neither the serve loop nor a running walk is cancelled: both end by
        # touching the broker, and on a pooled transport cancelling one
        # mid-command hands the connection back with its reply unread, which
        # breaks whatever borrows it next. ``serve`` rechecks the stop event between
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
        while not self._stop.is_set():
            try:
                async for req in self._broker.serve(
                    Topics.td_backfill(self._instance), stop=self._stop
                ):
                    await self._handle(req)
            except asyncio.CancelledError:
                raise
            except Exception:
                # Rebuilt rather than abandoned. A backfill nobody serves is
                # a hole in the record that no caller is waiting on and no
                # log line repeats, which is the kind of gap that is found
                # months later by the report that needed the rows.
                logger.exception("TD backfill serve loop failed — restarting")
                try:
                    await asyncio.wait_for(
                        self._stop.wait(), timeout=SERVE_RESTART_DELAY_S
                    )
                except TimeoutError:
                    continue

    async def _handle(self, req: IncomingRequest) -> None:
        try:
            payload = TdBackfill.model_validate(req.envelope.payload or {})
        except Exception as exc:
            await self._reply(
                req, BackfillOutcome(api_id=0, ok=False, reason=f"invalid: {exc}")
            )
            return

        forwarded = await self._forward(payload)
        if forwarded is not None:
            await self._reply_forwarded(req, forwarded)
            return

        if len(self._runs) >= self._max_in_flight:
            # Refused rather than queued: the sender is a schedule or a detach
            # that will ask again, and a request held here is one nothing can
            # see the state of. The account worker is not in this count.
            await self._reply(
                req,
                BackfillOutcome(
                    api_id=payload.api_id,
                    ok=False,
                    reason=in_flight_reason(len(self._runs)),
                ),
            )
            return

        await self._reply(
            req,
            BackfillOutcome(api_id=payload.api_id, ok=True, reason="accepted"),
        )
        task = asyncio.create_task(
            self._run(payload), name=f"td-backfill-{payload.api_id}"
        )
        self._runs.add(task)
        task.add_done_callback(self._runs.discard)

    async def _forward(self, payload: TdBackfill) -> TdBackfillResult | None:
        """The account worker's reply, or ``None`` to run the walk here.

        ``None`` is only "nobody is subscribed". A worker that accepted
        the request and then went quiet is not a reason to open a second
        connection from this process.
        """
        envelope = Envelope[TdBackfill].wrap(
            payload, type=TD_BACKFILL, source="td"
        )
        try:
            reply = await self._broker.request(
                Topics.td_account(payload.api_id),
                envelope,
                timeout=self._forward_timeout,
            )
        except NoRespondersError:
            logger.info(
                "TD backfill no account worker api_id=%s; "
                "running in this process",
                payload.api_id,
            )
            return None
        except RequestTimeoutError:
            logger.warning(
                "TD backfill account worker timed out api_id=%s",
                payload.api_id,
            )
            return TdBackfillResult(
                api_id=payload.api_id,
                ok=False,
                reason="account worker did not answer",
            )
        except Exception:
            logger.exception(
                "TD backfill forward failed api_id=%s", payload.api_id
            )
            return TdBackfillResult(
                api_id=payload.api_id,
                ok=False,
                reason="account worker request failed",
            )
        try:
            return TdBackfillResult.model_validate(reply.payload)
        except Exception as exc:
            logger.warning(
                "TD backfill worker reply unreadable api_id=%s",
                payload.api_id,
                exc_info=True,
            )
            return TdBackfillResult(
                api_id=payload.api_id,
                ok=False,
                reason=f"invalid: {exc}",
            )

    async def _run(self, payload: TdBackfill) -> None:
        await self._executor.run(
            payload.api_id, tickers=payload.tickers, reason=payload.reason
        )

    async def _reply_forwarded(
        self, req: IncomingRequest, result: TdBackfillResult
    ) -> None:
        await self._reply(
            req,
            BackfillOutcome(
                api_id=result.api_id,
                ok=result.ok,
                tickers=list(result.tickers),
                fills=result.fills,
                orders=result.orders,
                confirmed_through_ts=result.confirmed_through_ts,
                reason=result.reason,
            ),
        )

    async def _reply(self, req: IncomingRequest, outcome: BackfillOutcome) -> None:
        """Ack that the walk was accepted (or refused), not that it finished.

        A walk is minutes of venue round trips. The caller is a schedule or a
        detach measured in seconds, and the cursor is the record of progress.
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


__all__ = [
    "FORWARD_TIMEOUT_S",
    "MAX_RUNS_IN_FLIGHT",
    "BackfillSession",
    "in_flight_reason",
]
