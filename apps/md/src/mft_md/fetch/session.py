"""MD's fetch session — one per process, answering queries for everyone."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any

from mft.broker import Broker
from mft.broker.request import IncomingRequest
from mft.protocol import (
    MD_BESTQUOTE_RESULT,
    MD_FETCH_BESTQUOTE,
    MD_FETCH_KLINES,
    MD_FETCH_ORDERBOOK,
    MD_KLINES_RESULT,
    MD_ORDERBOOK_RESULT,
    MD_QUERY_ACK,
    Envelope,
    MdBestQuoteResult,
    MdFetchBestQuote,
    MdFetchKlines,
    MdFetchOrderBook,
    MdFetchRequest,
    MdKlinesResult,
    MdOrderBookResult,
    MdQueryAck,
    QueryCode,
    Topics,
)
from mft.protocol.query_codes import describe

from mft_md.errors import normalize as normalize_query_error
from mft_md.fetch.readers import NoReaderError, ReaderFactory, VenueReader

logger = logging.getLogger(__name__)

#: Queries running at once, across every caller. A ceiling on concurrency, not
#: a rate limiter — the venue's own quota governs pacing, and a caller that
#: trips it gets ``VENUE_RATE_LIMITED`` back to act on. This is the cruder
#: guard behind that: refusing at the ack is the only refusal that reaches a
#: caller before an unbounded pile of tasks does.
MAX_QUERIES_IN_FLIGHT = 32


@dataclass(frozen=True)
class _Kind:
    """One kind of query, start to finish.

    A table rather than a branch per read: everything around a query — acking,
    the in-flight cap, dispatching out of band, publishing whether or not it
    worked — is identical for all of them, and only three things differ. Adding
    a read is an entry here, not another copy of the machinery.
    """

    #: Request payload, and the reader method that answers it.
    model: type[MdFetchRequest]
    read: str
    #: Wire type of the result, and how to build it from the answer.
    result_type: str
    call: Callable[[Any, Any], Awaitable[Any]]
    result: Callable[[Any, bool, Any, str, int | str], Any]


_KINDS: dict[str, _Kind] = {
    MD_FETCH_KLINES: _Kind(
        model=MdFetchKlines,
        read="fetch_klines",
        result_type=MD_KLINES_RESULT,
        call=lambda read, req: read(req.symbol, req.interval, limit=req.limit),
        result=lambda req, ok, answer, reason, code: MdKlinesResult(
            query_id=req.query_id,
            venue=req.venue,
            symbol=req.symbol,
            interval=req.interval,
            ok=ok,
            klines=list(answer or ()),
            reason=reason,
            error_code=code,
        ),
    ),
    MD_FETCH_ORDERBOOK: _Kind(
        model=MdFetchOrderBook,
        read="fetch_order_book",
        result_type=MD_ORDERBOOK_RESULT,
        call=lambda read, req: read(req.symbol, depth=req.depth),
        result=lambda req, ok, answer, reason, code: MdOrderBookResult(
            query_id=req.query_id,
            venue=req.venue,
            symbol=req.symbol,
            ok=ok,
            book=answer,
            reason=reason,
            error_code=code,
        ),
    ),
    MD_FETCH_BESTQUOTE: _Kind(
        model=MdFetchBestQuote,
        read="fetch_best_quote",
        result_type=MD_BESTQUOTE_RESULT,
        call=lambda read, req: read(req.symbol),
        result=lambda req, ok, answer, reason, code: MdBestQuoteResult(
            query_id=req.query_id,
            venue=req.venue,
            symbol=req.symbol,
            ok=ok,
            quote=answer,
            reason=reason,
            error_code=code,
        ),
    ),
}


class FetchSession:
    """Serves ``md.fetch`` for as long as the process lives.

    Deliberately unlike the market-data sessions next to it. Those hold a
    fencing lease because a feed is leased to a strategy and two of them
    disagreeing about who owns it matters; a read is owned by nobody. Two MD
    processes answering the same query produce the same candles, so there is
    nothing to fence, and the session needs no attach, no heartbeat and no
    expiry — it is up whenever the process is.

    That is what decouples reads from feeds. A venue's reader is built the
    first time it is asked for and kept, so a query never waits on a
    subscription, and a venue nothing streams is queryable all the same.

    Answers go where the request says (``reply_channel``); this session holds
    no idea who its callers are.
    """

    def __init__(
        self,
        broker: Broker,
        factory: ReaderFactory,
        *,
        max_in_flight: int = MAX_QUERIES_IN_FLIGHT,
    ) -> None:
        self._broker = broker
        self._factory = factory
        self._max_in_flight = max_in_flight
        self._readers: dict[str, VenueReader] = {}
        self._locks: dict[str, asyncio.Lock] = {}
        self._queries: set[asyncio.Task[Any]] = set()
        self._stop = asyncio.Event()
        self._task: asyncio.Task[Any] | None = None

    @property
    def in_flight(self) -> int:
        return len(self._queries)

    @property
    def venues(self) -> list[str]:
        """Venues with a reader built and connected."""
        return sorted(self._readers)

    async def start(self) -> None:
        if self._task is not None:
            return
        self._stop.clear()
        self._task = asyncio.create_task(self._serve(), name="md-fetch")
        logger.info("MD fetch session listening subject=%s", Topics.md_fetch())

    async def stop(self) -> None:
        self._stop.set()
        # Neither the serve loop nor a running query is cancelled: both end by
        # touching a pooled Redis connection, and cancelling one mid-command
        # hands the connection back with its reply unread, which breaks
        # whatever borrows it next. ``serve`` rechecks the stop event between
        # polls, so this is bounded by a poll plus the venue's own timeout.
        pending = [t for t in (self._task, *self._queries) if t is not None]
        if pending:
            await asyncio.gather(*pending, return_exceptions=True)
        self._task = None
        self._queries.clear()
        for venue, reader in list(self._readers.items()):
            try:
                await reader.close()
            except Exception:
                logger.exception("MD fetch reader close failed venue=%s", venue)
        self._readers.clear()

    async def _serve(self) -> None:
        try:
            async for req in self._broker.serve(
                Topics.md_fetch(), stop=self._stop
            ):
                await self._handle(req)
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("MD fetch serve loop failed")

    async def _handle(self, req: IncomingRequest) -> None:
        """Ack a query, then run it out of band.

        The reply goes out before the venue is touched: this loop is a single
        consumer, and a REST round trip awaited inside it would stall every
        query behind it. ``accepted`` says the query was taken, nothing about
        what the venue will answer.
        """
        env = req.envelope
        kind = _KINDS.get(env.type)
        if kind is None:
            await self._ack(
                req,
                "",
                False,
                f"unsupported request {env.type!r}",
                QueryCode.MD_UNSUPPORTED_REQUEST,
            )
            return

        try:
            payload = kind.model.model_validate(env.payload or {})
        except Exception as exc:
            await self._ack(
                req, "", False, f"invalid payload: {exc}", QueryCode.MD_INVALID_REQUEST
            )
            return

        if not payload.reply_channel:
            # Nowhere to send the answer, so taking the query would be a lie.
            await self._ack(
                req,
                payload.query_id,
                False,
                "no reply_channel on the request",
                QueryCode.MD_INVALID_REQUEST,
            )
            return

        if len(self._queries) >= self._max_in_flight:
            await self._ack(
                req,
                payload.query_id,
                False,
                f"{len(self._queries)} queries already in flight",
                QueryCode.MD_TOO_MANY_IN_FLIGHT,
            )
            return

        await self._ack(req, payload.query_id, True, "", QueryCode.NONE)
        task = asyncio.create_task(
            self._run(kind, payload), name=f"md-fetch-{payload.query_id}"
        )
        self._queries.add(task)
        task.add_done_callback(self._queries.discard)

    async def _ack(
        self,
        req: IncomingRequest,
        query_id: str,
        accepted: bool,
        reason: str,
        error_code: int | str,
    ) -> None:
        if not accepted:
            logger.warning(
                "MD fetch refused query_id=%s code=%s: %s",
                query_id,
                describe(error_code),
                reason,
            )
        try:
            await req.reply(
                Envelope[MdQueryAck].wrap(
                    MdQueryAck(
                        query_id=query_id,
                        accepted=accepted,
                        reason=reason,
                        error_code=error_code,
                    ),
                    type=MD_QUERY_ACK,
                    source="md",
                )
            )
        except Exception:
            logger.exception("MD fetch ack failed query_id=%s", query_id)

    async def _reader(self, venue: str) -> VenueReader:
        """The venue's reader, built and connected once and then kept.

        Under a per-venue lock: two queries arriving together on a venue that
        has not been asked for yet would otherwise each build a client, and one
        of them would be dropped still holding an open connection.
        """
        existing = self._readers.get(venue)
        if existing is not None:
            return existing
        lock = self._locks.setdefault(venue, asyncio.Lock())
        async with lock:
            existing = self._readers.get(venue)
            if existing is not None:
                return existing
            reader = await self._factory.create(venue)
            await reader.connect()
            self._readers[venue] = reader
            logger.info("MD fetch reader connected venue=%s", venue)
            return reader

    async def _run(self, kind: _Kind, req: MdFetchRequest) -> None:
        """Answer one query, and publish the result however it turns out."""
        try:
            reader = await self._reader(req.venue)
            read = getattr(reader, kind.read, None)
            if read is None:
                # Same rule as the feeds: a venue that cannot serve a read has
                # no method for it, and is refused by name rather than by
                # calling something that raises.
                raise NoReaderError(
                    f"venue {req.venue!r} does not serve {kind.read}"
                )
            answer = await kind.call(read, req)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            error_code = normalize_query_error(exc, venue=req.venue)
            logger.warning(
                "MD fetch failed query_id=%s venue=%s %s %s: %s",
                req.query_id,
                req.venue,
                req.symbol,
                kind.read,
                describe(error_code),
            )
            await self._publish(
                kind,
                req,
                ok=False,
                # The venue's own words, which the code deliberately drops.
                reason=str(exc),
                error_code=error_code,
            )
            return

        await self._publish(kind, req, ok=True, answer=answer)

    async def _publish(
        self,
        kind: _Kind,
        req: MdFetchRequest,
        *,
        ok: bool,
        answer: Any = None,
        reason: str = "",
        error_code: int | str = QueryCode.NONE,
    ) -> None:
        """Send the answer to the caller's channel, success or not.

        Failures are published too. A caller waiting on ``query_id`` has no
        other way to learn the answer is never coming — a result that only
        arrives on success is indistinguishable from one still in flight.
        """
        try:
            await self._broker.publish(
                req.reply_channel,
                Envelope[Any].wrap(
                    kind.result(req, ok, answer, reason, error_code),
                    type=kind.result_type,
                    source="md",
                ),
            )
        except Exception:
            logger.exception(
                "MD fetch result publish failed query_id=%s channel=%s",
                req.query_id,
                req.reply_channel,
            )


__all__ = ["MAX_QUERIES_IN_FLIGHT", "FetchSession"]
