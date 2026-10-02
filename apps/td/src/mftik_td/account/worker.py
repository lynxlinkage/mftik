"""One account worker: resident layer plus trading layer (F34).

One process, one ``api_id``, every private connection that account has.
Splitting the two websockets a venue opens (Bybit's trade socket and
its private stream, Binance's WS API and its user stream) would put
the OMS and the ledger in two processes. They stay here.

The TD process does not import this package. It spawns
``python -m mftik_td.account``, and that entry constructs one worker.
Importing the module starts nothing.

Code identity (F39, F40) is not this layer's. There is no
``strategy_digest``, ``env_generation`` or ``code_ref`` here, and this
module does not import strategy code.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from mftik.clock import Clock
from mftik.exchange.venues import require

from mftik_td.account.broadcast import StateBroadcast
from mftik_td.account.deadman import DeadMansSwitch, deadman_for
from mftik_td.account.handlers import LedgerHandler, OmsHandler, OrderHandler
from mftik_td.account.resident import Keepalive, ResidentLayer
from mftik_td.account.trading import TradingLayer

if TYPE_CHECKING:
    from mftik_td.account.session import Session, TradingConnector
    from mftik_td.backfill.executor import BackfillExecutor
    from mftik_td.oms import Ledger, Oms


def _positive_id(value: object, name: str) -> int:
    if type(value) is not int or value <= 0:
        raise ValueError(f"{name} must be a positive int, got {value!r}")
    return value


def _incarnation(value: object) -> int:
    if type(value) is not int or value < 0:
        raise ValueError(f"incarnation must be an int >= 0, got {value!r}")
    return value


class AccountWorker:
    """The two layers, the handlers, the broadcast and the dead-man slot.

    ``cancel_on_disconnect`` defaults to false (F37). It is a copy of
    the account setting the caller already read. This object does not
    read ``apis``, and it does not turn the flag on by itself.

    ``worker_id`` is ``td/account/{api_id}``, the id under the shim in
    the §3 diagram.
    """

    def __init__(
        self,
        api_id: int,
        *,
        venue: str,
        incarnation: int = 0,
        cancel_on_disconnect: bool = False,
        keepalive: Keepalive | None = None,
        clock: Clock | None = None,
        oms: Oms | None = None,
        ledger: Ledger | None = None,
        private: TradingConnector | None = None,
        session: Session | None = None,
        backfill: BackfillExecutor | None = None,
    ) -> None:
        self.api_id = _positive_id(api_id, "api_id")
        self.incarnation = _incarnation(incarnation)
        if not isinstance(cancel_on_disconnect, bool):
            raise TypeError("cancel_on_disconnect must be a bool")
        self.venue = require(venue).name
        self.cancel_on_disconnect = cancel_on_disconnect
        connector = session.private if session is not None else private
        self.resident = ResidentLayer(
            self.api_id,
            venue=self.venue,
            keepalive=keepalive,
            connector=connector,
            clock=clock,
            backfill=backfill,
        )
        self.trading = TradingLayer(
            self.resident,
            oms=oms,
            ledger=ledger,
            private=private,
            session=session,
        )
        self.deadman: DeadMansSwitch = deadman_for(self.venue)
        self.broadcast = StateBroadcast(self.api_id, self.incarnation)
        self.orders = OrderHandler(self, clock=clock)
        self.oms = OmsHandler(self)
        self.ledger = LedgerHandler(self)

    @property
    def worker_id(self) -> str:
        """``td/account/{api_id}``."""
        return f"td/account/{self.api_id}"
