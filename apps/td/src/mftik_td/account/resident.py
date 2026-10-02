"""The resident layer: a warm HTTP pool, its keepalive, and backfill (F35).

**State authority (§3.3).** This object holds the HTTP connection pool
for one ``api_id``. It does not hold the OMS or the ledger; those are
the trading layer's. Order history, fills and cash flows are written by
this worker — live writes from the trading layer, catch-up from
backfill here — into Postgres. The exchange remains the authority for
what is actually resting.

**Invariants.**

* **R1.** An enabled account on this instance has a resident layer
  whether or not any session holds an intent.
* **R2.** :meth:`~mftik_td.account.trading.TradingLayer.activate` and
  :meth:`~mftik_td.account.trading.TradingLayer.deactivate` do not
  rebuild, close or replace the pool, and do not drop the keepalive
  hook.
* **R3.** At most one backfill runs for this account at a time, and it
  does not fill the pool. Other requests keep using it.
* **R4.** Recon, leverage lookups, backfill and HTTP order entry share
  this pool.
* **R5.** The keepalive is the adapter's lightweight request (server
  time, or whatever that venue can answer cheaply). The interval is
  B6-01's. It is not chosen here.

Paper has no HTTP pool to warm (B4-05). :meth:`start` and
:meth:`close` for ``Paper`` are the connector's ``connect`` and
``close``. :attr:`pool` stays ``None``: the warm HTTP client is
B6-01. :meth:`keepalive_once` and :meth:`handle_backfill` still raise
``NotImplementedError("IF-11")``. Any other venue does too, including
:meth:`start`.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Protocol

from mftik.broker.handler import Reply
from mftik.protocol import UntypedEnvelope

from mftik_td.account._ticket import TICKET

if TYPE_CHECKING:
    from mftik_td.account.session import TradingConnector


class Keepalive(Protocol):
    """The adapter's lightweight request, run on the warm pool (F35).

    One venue, one callable. B6-01 names the request and the interval
    on the adapter. Calling it is :meth:`ResidentLayer.keepalive_once`.
    """

    async def __call__(self) -> None:
        """One cheap authenticated-or-public read. No order, no cancel."""


class ResidentLayer:
    """The half of an account worker that does not follow intent (F35).

    Constructing it does not open a connection. :meth:`start` does, for
    paper. Other venues still raise until B6-01.
    """

    def __init__(
        self,
        api_id: int,
        *,
        venue: str,
        keepalive: Keepalive | None = None,
        connector: TradingConnector | None = None,
    ) -> None:
        self.api_id = api_id
        self.venue = venue
        self.keepalive = keepalive
        #: The paper connector, when this account is paper. Not an HTTP
        #: pool. B6-01 owns the pool; this stays ``None`` there.
        self._connector = connector
        self._started = False

    @property
    def started(self) -> bool:
        """Whether :meth:`start` has connected this account.

        False until paper :meth:`start` returns, and until B6-01 for
        every other venue. A trading-layer toggle does not change this
        (R2).
        """
        return self._started

    @property
    def pool(self) -> object | None:
        """The warm HTTP client, or ``None`` until B6-01 builds it.

        Paper has no pool. Identity, once B6-01 sets it, is stable
        across trading-layer ``activate`` and ``deactivate`` (R2).
        Callers compare with ``is``.
        """
        return None

    async def start(self) -> None:
        """Connect the paper connector. Other venues are B6-01.

        Does not start the trading layer, and does not open an HTTP
        pool. An account with no session still starts (R1). A second
        call is a no-op once this layer is up.
        """
        if self.venue != "Paper" or self._connector is None:
            raise NotImplementedError(TICKET)
        if self._started:
            return
        await self._connector.connect()
        self._started = True

    async def close(self) -> None:
        """Close the paper connector. Other venues are B6-01.

        Not a substitute for :meth:`TradingLayer.deactivate`: closing
        the resident layer is the account worker exiting, not an intent
        going away. If the trading layer already closed the connector,
        this only marks the layer down.
        """
        if not self._started:
            raise NotImplementedError(TICKET)
        connector = self._connector
        if connector is not None and getattr(connector, "connected", False):
            await connector.close()
        self._started = False

    async def keepalive_once(self) -> None:
        """Send :attr:`keepalive` on the pool, once. B6-01."""
        raise NotImplementedError(TICKET)

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
