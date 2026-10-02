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

Null until B6-01 and B6-05. :attr:`pool` is ``None``, :attr:`started`
is false, and every action raises ``NotImplementedError("IF-11")``.
"""

from __future__ import annotations

from typing import Protocol

from mftik.broker.handler import Reply
from mftik.protocol import UntypedEnvelope

from mftik_td.account._ticket import TICKET


class Keepalive(Protocol):
    """The adapter's lightweight request, run on the warm pool (F35).

    One venue, one callable. B6-01 names the request and the interval
    on the adapter. Calling it is :meth:`ResidentLayer.keepalive_once`.
    """

    async def __call__(self) -> None:
        """One cheap authenticated-or-public read. No order, no cancel."""


class ResidentLayer:
    """The half of an account worker that does not follow intent (F35).

    Constructing it does not open a connection. :meth:`start` is what
    will, and it raises until B6-01.
    """

    def __init__(
        self,
        api_id: int,
        *,
        venue: str,
        keepalive: Keepalive | None = None,
    ) -> None:
        self.api_id = api_id
        self.venue = venue
        self.keepalive = keepalive

    @property
    def started(self) -> bool:
        """Whether :meth:`start` has brought the pool up.

        False until B6-01. A trading-layer toggle does not change this
        (R2).
        """
        return False

    @property
    def pool(self) -> object | None:
        """The warm HTTP client, or ``None`` until B6-01 builds it.

        Identity is stable across trading-layer ``activate`` and
        ``deactivate`` (R2). Callers compare with ``is``.
        """
        return None

    async def start(self) -> None:
        """Open the pool and start the keepalive. B6-01.

        Does not start the trading layer. An account with no session
        still starts (R1).
        """
        raise NotImplementedError(TICKET)

    async def close(self) -> None:
        """Close the pool. The trading layer is already down by then.

        Not a substitute for :meth:`TradingLayer.deactivate`: closing
        the resident layer is the account worker exiting, not an intent
        going away.
        """
        raise NotImplementedError(TICKET)

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
