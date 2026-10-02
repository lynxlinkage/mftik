"""The resident layer: a warm HTTP pool, its keepalive, and backfill (F35).

**State authority (§3.3).** This object holds the HTTP connection pool
for one ``api_id``. It does not hold the OMS or the ledger; those are
the trading layer's. Order history, fills and cash flows are written by
this worker — live writes from the trading layer, catch-up from
backfill here — into Postgres. The exchange remains the authority for
what is actually resting.

**Invariants.**

* **R1.** An enabled account on this instance has a resident layer
  whether or not any session holds an intent. ``start`` does not
  require a trading connector.
* **R2.** :meth:`~mftik_td.account.trading.TradingLayer.activate` and
  :meth:`~mftik_td.account.trading.TradingLayer.deactivate` do not
  rebuild, close or replace the pool, and do not drop the keepalive
  hook. The pool object returned by :attr:`pool` is stable from
  :meth:`start` until :meth:`close`.
* **R3.** At most one backfill runs for this account at a time, and it
  does not fill the pool. Other requests keep using it. The one-at-a-time
  run is B6-05; this layer only leaves more than one connection in the
  pool. :meth:`handle_backfill` still raises.
* **R4.** Recon, leverage lookups, backfill and HTTP order entry share
  this pool. A venue REST client takes the client from
  :meth:`RestPool.client_for` as its ``client=``.
* **R5.** The keepalive is the adapter's lightweight public read
  (:mod:`mftik.exchange.keepalive`). The interval is the adapter's
  constant. It is not chosen here.

Paper has no HTTP pool. :meth:`start` and :meth:`close` for ``Paper``
are the connector's ``connect`` and ``close``. :attr:`pool` stays
``None``. :meth:`keepalive_once` still raises for paper.
:meth:`handle_backfill` raises for every venue (B6-05).

A keepalive failure is logged and retried on the next tick. It does
not stop the worker and it does not touch the trading layer.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Mapping, Sequence
from typing import TYPE_CHECKING, Protocol

import httpx
from mftik.broker.handler import Reply
from mftik.clock import Clock, SystemClock
from mftik.exchange.keepalive import RestKeepalive, for_venue
from mftik.protocol import UntypedEnvelope

from mftik_td.account._ticket import TICKET

if TYPE_CHECKING:
    from mftik_td.account.session import TradingConnector

logger = logging.getLogger(__name__)

#: Request timeout on a warm client. The same bound the venue REST
#: clients use when they build their own. Not a new setting.
_CLIENT_TIMEOUT_S = 10.0


class Keepalive(Protocol):
    """The adapter's lightweight request, run on the warm pool (F35).

    One venue, one callable. B6-01 names the request and the interval
    on the adapter. Calling it is :meth:`ResidentLayer.keepalive_once`.
    When the caller does not pass one, that method sends the adapter's
    public read on every client in the pool.
    """

    async def __call__(self) -> None:
        """One cheap public read. No order, no cancel."""


class RestPool:
    """Warm HTTP clients for one account, one per REST host (F35).

    Identity is stable from :meth:`ResidentLayer.start` until
    :meth:`ResidentLayer.close`. B6-02 and B6-05 pass
    :meth:`client_for` into a venue REST client's ``client=`` and do
    not close it; this layer does, on :meth:`ResidentLayer.close`.
    """

    def __init__(self, clients: Mapping[str, httpx.AsyncClient]) -> None:
        self._clients = {host.rstrip("/"): client for host, client in clients.items()}

    def client_for(self, base_url: str) -> httpx.AsyncClient:
        """The warm client for ``base_url``.

        The URL is matched with its trailing slash stripped, the same
        way the venue REST clients store ``base_url``.
        """
        key = base_url.rstrip("/")
        try:
            return self._clients[key]
        except KeyError:
            raise KeyError(f"no warm client for {base_url!r}") from None

    def hosts(self) -> tuple[str, ...]:
        """Base URLs this pool holds, in insertion order."""
        return tuple(self._clients)

    def clients(self) -> tuple[httpx.AsyncClient, ...]:
        """The clients, in the same order as :meth:`hosts`."""
        return tuple(self._clients.values())


class ResidentLayer:
    """The half of an account worker that does not follow intent (F35).

    Constructing it does not open a connection. :meth:`start` does.
    """

    def __init__(
        self,
        api_id: int,
        *,
        venue: str,
        keepalive: Keepalive | None = None,
        connector: TradingConnector | None = None,
        clock: Clock | None = None,
    ) -> None:
        self.api_id = api_id
        self.venue = venue
        self.keepalive = keepalive
        #: The paper connector, when this account is paper. Not an HTTP
        #: pool. Paper's :attr:`pool` stays ``None``.
        self._connector = connector
        self._clock: Clock = clock if clock is not None else SystemClock()
        self._started = False
        self._pool: RestPool | None = None
        self._task: asyncio.Task[None] | None = None

    @property
    def started(self) -> bool:
        """Whether :meth:`start` has connected this account.

        A trading-layer toggle does not change this (R2).
        """
        return self._started

    @property
    def pool(self) -> RestPool | None:
        """The warm HTTP clients, or ``None`` when this layer has none.

        Paper has no pool. For every other venue the object is built in
        :meth:`start` and stays the same object until :meth:`close`
        (R2). Callers compare with ``is``.
        """
        return self._pool

    async def start(
        self,
        *,
        base_urls: Sequence[str] | None = None,
        interval_s: float | None = None,
        keepalive_expiry_s: float | None = None,
        transport: httpx.AsyncBaseTransport | None = None,
    ) -> None:
        """Open the warm pool, or connect the paper connector.

        Does not start the trading layer. An account with no session
        still starts (R1). A second call is a no-op once this layer is up.

        ``base_urls``, ``interval_s``, ``keepalive_expiry_s`` and
        ``transport`` default to the adapter. Tests scale the interval
        and the expiry, and point the clients at a loopback server or a
        :class:`httpx.MockTransport`. They are not configuration.

        The first keepalive is best-effort. A failure is logged; the
        loop retries it. ``start`` still returns, and the trading layer
        is not touched.
        """
        if self._started:
            return
        if self.venue == "Paper":
            if self._connector is None:
                raise NotImplementedError(TICKET)
            await self._connector.connect()
            self._started = True
            return
        spec = for_venue(self.venue)
        interval = spec.interval_s if interval_s is None else interval_s
        expiry = spec.expiry_s if keepalive_expiry_s is None else keepalive_expiry_s
        if interval <= 0:
            raise ValueError("keepalive interval must be positive")
        if expiry <= interval:
            raise ValueError("keepalive_expiry must be longer than the interval")
        hosts = tuple(spec.hosts if base_urls is None else base_urls)
        if not hosts:
            raise ValueError("a resident pool needs at least one host")
        limits = _limits(spec, expiry)
        clients: dict[str, httpx.AsyncClient] = {}
        try:
            for host in hosts:
                key = host.rstrip("/")
                clients[key] = httpx.AsyncClient(
                    base_url=key,
                    timeout=_CLIENT_TIMEOUT_S,
                    limits=limits,
                    transport=transport,
                )
        except Exception:
            for client in clients.values():
                await client.aclose()
            raise
        self._pool = RestPool(clients)
        self._started = True
        try:
            await self.keepalive_once()
        except Exception:
            logger.exception(
                "resident keepalive failed api_id=%s venue=%s",
                self.api_id,
                self.venue,
            )
        self._task = asyncio.create_task(
            self._keepalive_loop(interval),
            name=f"td-keepalive-{self.api_id}",
        )

    async def close(self) -> None:
        """Stop the keepalive loop and close the clients.

        Not a substitute for :meth:`TradingLayer.deactivate`: closing
        the resident layer is the account worker exiting, not an intent
        going away. Paper closes its connector when that connector is
        still up. A layer that was never started still raises.
        """
        if not self._started:
            raise NotImplementedError(TICKET)
        task = self._task
        self._task = None
        if task is not None:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        pool = self._pool
        self._pool = None
        if pool is not None:
            for client in pool.clients():
                await client.aclose()
        connector = self._connector
        if connector is not None and getattr(connector, "connected", False):
            await connector.close()
        self._started = False

    async def keepalive_once(self) -> None:
        """Send the keepalive once, on every client in the pool.

        An injected :attr:`keepalive` is that request. Otherwise this
        sends the adapter's public read. Paper has no pool and raises.
        A failure propagates to the caller; the loop logs it and
        continues.
        """
        if self.venue == "Paper" or self._pool is None:
            raise NotImplementedError(TICKET)
        hook = self.keepalive
        if hook is not None:
            await hook()
            return
        spec = for_venue(self.venue)
        first: BaseException | None = None
        for client in self._pool.clients():
            try:
                await spec.send(client)
            except Exception as exc:
                if first is None:
                    first = exc
        if first is not None:
            raise first

    async def _keepalive_loop(self, interval: float) -> None:
        """Sleep on the injected clock, then send. Failures stay here."""
        while True:
            await self._clock.sleep(interval)
            try:
                await self.keepalive_once()
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception(
                    "resident keepalive failed api_id=%s venue=%s",
                    self.api_id,
                    self.venue,
                )

    async def handle_backfill(self, message: UntypedEnvelope) -> Reply | None:
        """Serve one ``TdBackfill`` on this pool (F35, R3). B6-05.

        A :class:`~mftik.broker.handler.Handler`. The account does not
        need a session. At most one run at a time, and it must leave
        room in the pool for orders. The reply is a ``TdBackfillResult``.

        This does not call :mod:`mftik_td.backfill`. That package still
        serves ``td.backfill.{instance}`` on the TD process; B6-05 moves
        the work onto this method. The TD process does not mount this
        handler.
        """
        raise NotImplementedError(TICKET)


def _limits(spec: RestKeepalive, expiry_s: float) -> httpx.Limits:
    """The adapter's pool limits, with the expiry the caller asked for."""
    return httpx.Limits(
        max_connections=spec.limits.max_connections,
        max_keepalive_connections=spec.limits.max_keepalive_connections,
        keepalive_expiry=expiry_s,
    )
