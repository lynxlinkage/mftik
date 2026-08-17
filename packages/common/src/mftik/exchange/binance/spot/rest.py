"""Binance spot REST — the account-history reads, and nothing else.

The spot trading path has no REST half on purpose: order entry, cancels and
order queries all go over ``ws-api``, one authenticated connection with no
per-call signature, and a second transport managing the same live order state
would need reconciling against the first. That reasoning still holds — see
:mod:`mftik.exchange.binance.spot.private`, which composes exactly one transport.

This client is outside it. It reads history, holds no order state and takes
part in no lifecycle, so there is nothing for it to fall out of sync with. It
exists because the reads it serves are a **batch job on a schedule**, and:

* Building, authenticating and tearing down a WebSocket API session to ask
  three questions costs more and fails in more ways than three HTTP GETs.
* A second authenticated socket on the account, alongside the live trading
  one, is connection budget spent for nothing.
* Every other venue's history reader is REST. Making spot the exception means
  the plane that drives them all has to reason about socket lifecycle for one
  venue.

Both endpoints here are **per-symbol** — Binance requires it — which is why the
backfill cursor is keyed by instrument rather than by account.

Paginate by id (``fromId`` / ``orderId``), not by time window. Ids are strictly
monotonic per symbol and have no window-length limit, whereas a time range can
be capped by the venue and cannot separate two trades that share a millisecond.
Time is for the first page only, when there is no id to resume from.
"""

from __future__ import annotations

import logging

from mftik.exchange.binance.rest import BinanceRestError, BinanceSignedRest
from mftik.exchange.binance.spot.models import (
    BinanceSpotHistoricalOrder,
    BinanceSpotMyTrade,
)
from mftik.exchange.binance.spot.protocol import BINANCE_SPOT_REST_URL

logger = logging.getLogger(__name__)

API_PREFIX = "/api/v3"

#: Most rows either endpoint returns in one call. Asking for more is a 400,
#: not a truncated answer.
MAX_ROWS = 1000


class BinanceSpotRestError(BinanceRestError):
    """A non-2xx answer from Binance's spot REST API."""


class BinanceSpotRest(BinanceSignedRest):
    """Signed spot history reads: the account's own trades and orders.

    Deliberately narrow, and deliberately not wired into
    :class:`~mftik.exchange.binance.spot.private.BinanceSpotPrivateClient` — the
    backfill plane builds one of these, uses it and closes it, and nothing on
    the trading path knows it exists.
    """

    default_base_url = BINANCE_SPOT_REST_URL
    error_type = BinanceSpotRestError

    async def fetch_my_trades(
        self,
        symbol: str,
        *,
        from_id: int | None = None,
        start_time: int | None = None,
        end_time: int | None = None,
        limit: int = MAX_ROWS,
    ) -> list[BinanceSpotMyTrade]:
        """``GET /api/v3/myTrades`` — this account's executions, oldest first.

        ``from_id`` resumes from a known trade id (inclusive) and is how a
        backfill walks forward. ``start_time`` / ``end_time`` are for the first
        page, before any id is known; Binance caps how wide that range may be,
        so it is a way to *start* a walk rather than to do one.

        Passing both is refused rather than sent: Binance ignores the time
        range when an id is present, so the call would silently answer a
        different question than the one asked.
        """
        if from_id is not None and (start_time is not None or end_time is not None):
            raise ValueError(
                "pass from_id or a time range, not both: Binance ignores the "
                "range when fromId is set"
            )
        rows = await self._signed_get(
            f"{API_PREFIX}/myTrades",
            {
                "symbol": symbol,
                "fromId": from_id,
                "startTime": start_time,
                "endTime": end_time,
                "limit": min(limit, MAX_ROWS),
            },
        )
        return [BinanceSpotMyTrade.model_validate(row) for row in rows or []]

    async def fetch_orders(
        self,
        symbol: str,
        *,
        from_order_id: int | None = None,
        start_time: int | None = None,
        end_time: int | None = None,
        limit: int = MAX_ROWS,
    ) -> list[BinanceSpotHistoricalOrder]:
        """``GET /api/v3/allOrders`` — every order on ``symbol``, open or not.

        The counterpart to :meth:`fetch_my_trades`, and not optional alongside
        it: a trade row carries no ``clientOrderId``, so this is the only way
        to learn which order — and therefore whose — an execution belonged to.

        Same pagination rule, on ``orderId``. Returns orders this platform
        never placed as well; that is the point, not a nuisance.
        """
        if from_order_id is not None and (
            start_time is not None or end_time is not None
        ):
            raise ValueError(
                "pass from_order_id or a time range, not both: Binance ignores "
                "the range when orderId is set"
            )
        rows = await self._signed_get(
            f"{API_PREFIX}/allOrders",
            {
                "symbol": symbol,
                "orderId": from_order_id,
                "startTime": start_time,
                "endTime": end_time,
                "limit": min(limit, MAX_ROWS),
            },
        )
        return [
            BinanceSpotHistoricalOrder.model_validate(row) for row in rows or []
        ]


__all__ = [
    "API_PREFIX",
    "MAX_ROWS",
    "BinanceSpotRest",
    "BinanceSpotRestError",
]
