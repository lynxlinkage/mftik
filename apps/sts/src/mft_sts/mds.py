"""Strategy-side market-data queries — history the feeds do not carry."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

from mft.broker.errors import RequestTimeoutError
from mft.exchange.intervals import InvalidIntervalError, normalize_interval
from mft.exchange.tickers import UniversalTicker
from mft.protocol import (
    MD_FETCH_BESTQUOTE,
    MD_FETCH_KLINES,
    MD_FETCH_ORDERBOOK,
    Envelope,
    MdFetchBestQuote,
    MdFetchKlines,
    MdFetchOrderBook,
    MdFetchRequest,
    MdQueryAck,
    QueryCode,
    Topics,
)
from mft.protocol.query_codes import describe

if TYPE_CHECKING:
    from mft_sts.strategy import Strategy

logger = logging.getLogger(__name__)

#: MD answers before it touches the venue, so an ack is about one Redis
#: round-trip away. This is *not* how long the query itself may take — that
#: bound is the venue's, and its answer arrives on the reply channel however
#: long it needs.
QUERY_ACK_TIMEOUT_S = 2.0


class StrategyMds:
    """On-demand reads from MD, for what the feeds cannot answer.

    Three reads: :meth:`fetch_klines` for history the feeds never carry, and
    :meth:`fetch_order_book` / :meth:`fetch_best_quote` for a snapshot *now*
    rather than a subscription to every change. The second pair overlaps feeds
    that exist, and is the right tool when a strategy needs the book once —
    subscribing to a stream to read its first message and drop the rest costs a
    feed for the life of the session and gets a staler answer.

    Independent of the feeds in both directions, which is the point. The
    request goes to one subject MD serves for everyone rather than to anything
    this session holds, and the answer comes back on a channel of this
    session's own — so a strategy that subscribes to no market data at all can
    still ask, and a venue nothing streams is queryable all the same.

    Order entry's two-step, for the same reason: the ack is cheap and the venue
    round trip is not, and holding the reply open for the second would stall
    every query behind it. So a ``query_id`` back from any of these means MD
    accepted the query, and says nothing about whether the venue will answer
    it. Correlate on that id — which is why they return one rather than a bool
    — and read the answer in the matching ``on_fetch_*`` hook.
    """

    def __init__(self, *, ack_timeout: float = QUERY_ACK_TIMEOUT_S) -> None:
        self._strategy: Strategy | None = None
        self._ack_timeout = ack_timeout
        self._seq = 0
        self._last_reason: str = ""
        self._last_code: int | str = QueryCode.NONE

    def bind(self, strategy: Strategy) -> None:
        self._strategy = strategy

    @property
    def last_reject_reason(self) -> str:
        """Why the most recent query was refused, or ``""`` if it was taken.

        Only ever set by a refusal at the ack. Failures after one land on the
        result instead, which carries the same pair of fields.
        """
        return self._last_reason

    @property
    def last_reject_code(self) -> int | str:
        """:attr:`last_reject_reason` as a code — see
        :mod:`mft.protocol.query_codes`.

        ``QueryCode.NONE`` when the last query was taken. A refusal at this
        stage is always in the ``1xx`` band, so
        :func:`~mft.protocol.query_codes.is_retryable` is the useful test
        rather than the band.
        """
        return self._last_code

    def _next_query_id(self) -> str:
        """``{session_id}:{seq}`` — unique per session, and readable in a log.

        No packing to do here, unlike ``client_order_id``: nothing downstream
        parses this and it never has to survive a restart. Its whole job is to
        tell this session's own queries apart from each other, since one reply
        channel carries every answer this session asked for.
        """
        session = self._require_session()
        self._seq += 1
        return f"{session.session_id}:{self._seq}"

    async def fetch_klines(
        self,
        ticker: UniversalTicker,
        interval: str,
        *,
        limit: int = 100,
    ) -> str | None:
        """Ask MD for the most recent ``limit`` candles. Returns the query id.

        ``None`` means the query never reached the venue — MD refused it, or
        no MD is running — and :attr:`last_reject_reason` says which.

        A ``query_id`` means MD took it. The candles do **not** come back here:
        they arrive at ``on_fetch_klines`` as an
        :class:`~mft.protocol.MdKlinesResult` carrying this same id. That hook
        fires whether the query worked or not, so a strategy holding an id
        always learns its fate.

        ``interval`` is canonical spelling — ``1m``, ``4h``, ``1mo``; see
        :mod:`mft.exchange.intervals`. It is normalized here, so what the
        result carries back is what the venue was actually asked for. Which
        intervals a venue serves is the venue's business, and asking for one it
        does not have comes back as ``MD_INTERVAL_NOT_SUPPORTED`` on the
        result.
        """
        try:
            canonical = normalize_interval(interval)
        except InvalidIntervalError as exc:
            # Refused here rather than sent: the wire contract is canonical
            # spelling, and MD would have no way to answer a request that
            # breaks it beyond repeating this same complaint a round trip later.
            self._refuse(str(exc), QueryCode.MD_INVALID_REQUEST)
            return None

        return await self._send(
            MD_FETCH_KLINES,
            MdFetchKlines,
            ticker=str(ticker),
            interval=canonical,
            limit=limit,
        )

    async def fetch_order_book(
        self, ticker: UniversalTicker, *, depth: int = 10
    ) -> str | None:
        """Ask MD for a book snapshot. Returns the query id, or ``None``.

        A snapshot, not a subscription: the answer arrives once, at
        ``on_fetch_orderbook``, and nothing follows it. A strategy that wants
        the book as it moves subscribes to the ``orderbook`` feed instead.
        """
        return await self._send(
            MD_FETCH_ORDERBOOK,
            MdFetchOrderBook,
            ticker=str(ticker),
            depth=depth,
        )

    async def fetch_best_quote(self, ticker: UniversalTicker) -> str | None:
        """Ask MD for the touch with its resting sizes. Returns the query id.

        The answer reaches ``on_fetch_bestquote``. Its ``quote`` is None when
        a side of the book was empty — not an error, but nothing to price
        against either, so a caller checking whether its own order can rest
        should ask again rather than treat it as a quote of zero.
        """
        return await self._send(
            MD_FETCH_BESTQUOTE, MdFetchBestQuote, ticker=str(ticker)
        )

    async def _send(
        self, msg_type: str, model: type[MdFetchRequest], **fields: Any
    ) -> str | None:
        """Mint an id, ask, and hand the id back if MD took the query."""
        session = self._require_session()
        query_id = self._next_query_id()
        accepted = await self._request_ack(
            query_id,
            Envelope[model].wrap(
                model(
                    reply_channel=Topics.md_fetch_reply(session.session_id),
                    query_id=query_id,
                    **fields,
                ),
                type=msg_type,
                source=f"strategy.{session.strategy.name}",
                session_id=session.session_id,
            ),
        )
        return query_id if accepted else None

    async def _request_ack(self, query_id: str, envelope: Envelope[Any]) -> bool:
        """Round-trip a query to MD and report whether it was taken."""
        session = self._require_session()
        log = session.event_log
        log.record(
            "query",
            envelope.type,
            dir="out",
            query_id=query_id,
            payload=envelope.payload,
        )
        # Cleared up front so a stale reason cannot outlive the refusal it
        # described and be read against a later, accepted query.
        self._last_reason = ""
        self._last_code = QueryCode.NONE
        try:
            reply = await session.broker.request(
                Topics.md_fetch(), envelope, timeout=self._ack_timeout
            )
        except RequestTimeoutError:
            # No MD is running, or every one of them is wedged. Either way the
            # query did not land: say so rather than letting the strategy wait
            # on a result that will never arrive.
            logger.warning(
                "MD fetch ack timed out query_id=%s type=%s",
                query_id,
                envelope.type,
            )
            self._refuse("no ack from MD", QueryCode.MD_NO_ACK)
            log.record(
                "query",
                "query_ack",
                query_id=query_id,
                accepted=False,
                code=self._last_code,
                reason=self._last_reason,
            )
            return False

        try:
            ack = MdQueryAck.model_validate(reply.payload)
        except Exception:
            logger.warning(
                "MD fetch ack unreadable query_id=%s reply=%s",
                query_id,
                reply.type,
            )
            self._refuse("unreadable ack from MD", QueryCode.MD_UNREADABLE_ACK)
            log.record(
                "query",
                "query_ack",
                query_id=query_id,
                accepted=False,
                code=self._last_code,
                reason=self._last_reason,
                sent_ts=reply.ts,
            )
            return False

        if not ack.accepted:
            logger.warning(
                "MD refused query query_id=%s code=%s: %s",
                query_id,
                describe(ack.error_code),
                ack.reason,
            )
            self._last_reason = ack.reason
            self._last_code = ack.error_code
        log.record(
            "query",
            "query_ack",
            query_id=query_id,
            accepted=ack.accepted,
            code=ack.error_code,
            reason=ack.reason or None,
            sent_ts=reply.ts,
        )
        return ack.accepted

    def _refuse(self, reason: str, code: int | str) -> None:
        """Record a refusal for :attr:`last_reject_reason` and its code."""
        self._last_reason = reason
        self._last_code = code

    def _require_session(self):
        if self._strategy is None or self._strategy.session is None:
            raise RuntimeError("strategy MDS is not bound to a session")
        return self._strategy.session
