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

Paper (B4-05) activates for the worker's whole life: there is no wire
type yet for the controller's ``PUSH_TRADING``, and switching the layer
by intent is B6-02. A trading layer constructed with a
:class:`~mftik_td.account.session.Session` starts that book in
:meth:`activate` and closes it in :meth:`deactivate`. Without one, both
switches still raise ``NotImplementedError("IF-11")``. The T1–T4
toggle, and every real venue, stay B6-02.
"""

from __future__ import annotations

from decimal import Decimal
from typing import TYPE_CHECKING

from mftik_td.account._ticket import TICKET
from mftik_td.account.resident import ResidentLayer
from mftik_td.oms import Ledger, Oms

if TYPE_CHECKING:
    from mftik_td.account.session import Session, TradingConnector


class TradingLayer:
    """The half of an account worker that follows intent (F35).

    ``oms`` and ``ledger`` start empty, or as the session's book when
    the caller passes one. That empty book is not a reconcile.
    ``private`` is the venue connector. The connector protocol is the
    one :class:`~mftik_td.account.session.Session` already consumes, not
    a second trading interface.

    Paper passes ``session``. :meth:`activate` starts it and
    :meth:`deactivate` destroys it, and neither call touches the
    resident layer (T1). B6-02 is what switches this from intent. Until
    then a paper worker activates once, at startup, and leaves the
    layer up.
    """

    def __init__(
        self,
        resident: ResidentLayer,
        *,
        oms: Oms | None = None,
        ledger: Ledger | None = None,
        private: TradingConnector | None = None,
        session: Session | None = None,
    ) -> None:
        self.resident = resident
        self._session = session
        if session is not None:
            self.oms = session.oms
            self.ledger = session.ledger
            self.private = session.private
        else:
            self.oms = oms if oms is not None else Oms()
            self.ledger = ledger if ledger is not None else Ledger()
            self.private = private
        #: Leverage figures, keyed by universal ticker. Empty until a
        #: lookup fills them (B6-02). The cache is this layer's, so it
        #: goes away on :meth:`deactivate` and is not part of the
        #: resident pool.
        self.leverage: dict[str, Decimal] = {}
        self._active = False

    @property
    def session(self) -> Session | None:
        """The paper book, or ``None`` until the caller binds one."""
        return self._session

    @property
    def active(self) -> bool:
        """Whether the private book is up.

        False until :meth:`activate` on a layer that has a session.
        """
        return self._active

    async def activate(self) -> None:
        """Open the private book. Does not touch the resident layer (T1).

        With a session, this is :meth:`Session.start`: connect, recon,
        the OMS and the ledger. The order subject is served by the
        worker process, not by this method. Without a session the
        switch is still B6-02.
        """
        if self._session is None:
            raise NotImplementedError(TICKET)
        if self._active:
            return
        await self._session.start()
        self.oms = self._session.oms
        self.ledger = self._session.ledger
        self.private = self._session.private
        self._active = True

    async def deactivate(self) -> None:
        """Close the private book. The resident layer stays (T1).

        With a session, this is :meth:`Session.destroy`. Without one the
        switch is still B6-02. A layer that was never activated still
        raises: there is nothing to close, and the null surface stays
        distinguishable from a book that came down.
        """
        if self._session is None or not self._active:
            raise NotImplementedError(TICKET)
        await self._session.destroy()
        self.leverage = {}
        self._active = False
