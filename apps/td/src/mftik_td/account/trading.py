"""The trading layer: private connections, OMS, ledger, recon (F35).

On while some session holds a TdIntent for this account, off the moment
the last one is gone. No linger. The resident layer is not part of that
switch (T1, R2).

**State authority (§3.3).**

* The OMS and the ledger (pre-locks, available) live here, in memory.
  The exchange is the authority for resting orders, positions and
  balances. After a restart this layer rebuilds them with ``reconcile``
  and the worker publishes ``td.account.reset`` (F13).
* Observed open or closed is this object's. Desired open or closed is
  the TD controller's, level-triggered from intent (IF-12). When the
  controller is absent this layer keeps the last desired: silence is
  not a :meth:`deactivate` (P5). This class does not watch the
  controller.

**Invariants.**

* **T1.** :meth:`activate` and :meth:`deactivate` do not touch the
  resident layer's pool or its keepalive hook.
* **T2.** The last intent disappearing deactivates immediately. There
  is no grace period.
* **T3.** The private websocket, the OMS, the ledger, recon, the
  leverage cache and the subscription on ``td.order.{api_id}`` exist
  only while :attr:`active`. ``TdReady`` stays false until recon
  finishes after :meth:`activate`.
* **T4.** This object does not spawn a second incarnation. At-most-one
  is the supervisor's pid fence (F36, IF-12). A trading layer assumes
  it is the only one for its ``api_id``.

F11's ``restarting`` window does not drop the intent (R4 of that
section), so a strategy restart does not flap this layer. That rule
belongs to the STS controller; this class only sees the intent count
the TD controller pushes.

Null until B6-02. :attr:`active` is false, and both switches raise
``NotImplementedError("IF-11")``.
"""

from __future__ import annotations

from decimal import Decimal
from typing import TYPE_CHECKING

from mftik_td.account._ticket import TICKET
from mftik_td.account.resident import ResidentLayer
from mftik_td.oms import Ledger, Oms

if TYPE_CHECKING:
    from mftik_td.session.session import TradingConnector


class TradingLayer:
    """The half of an account worker that follows intent (F35).

    ``oms`` and ``ledger`` start empty. That is the null book, not a
    reconcile. ``private`` is the venue connector B6-02 will drive;
    ``None`` until the caller passes one. The connector protocol is the
    one :class:`~mftik_td.session.session.Session` already consumes, not
    a second trading interface.
    """

    def __init__(
        self,
        resident: ResidentLayer,
        *,
        oms: Oms | None = None,
        ledger: Ledger | None = None,
        private: TradingConnector | None = None,
    ) -> None:
        self.resident = resident
        self.oms = oms if oms is not None else Oms()
        self.ledger = ledger if ledger is not None else Ledger()
        self.private = private
        #: Leverage figures, keyed by universal ticker. Empty until a
        #: lookup fills them (B6-02). The cache is this layer's, so it
        #: goes away on :meth:`deactivate` and is not part of the
        #: resident pool.
        self.leverage: dict[str, Decimal] = {}

    @property
    def active(self) -> bool:
        """Whether the private book is up. False until B6-02."""
        return False

    async def activate(self) -> None:
        """Open the private book (T3). Does not touch the resident layer (T1).

        Connects the private websocket, runs recon, brings the OMS and
        the ledger online, and subscribes ``td.order.{api_id}``.
        ``TdReady`` is false until that recon finishes. B6-02.
        """
        raise NotImplementedError(TICKET)

    async def deactivate(self) -> None:
        """Close the private book now (T2). The resident layer stays (T1).

        No linger. In-flight backfill on the resident pool is not
        cancelled by this. B6-02.
        """
        raise NotImplementedError(TICKET)
