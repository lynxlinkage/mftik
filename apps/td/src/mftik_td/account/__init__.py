"""TD account worker — one ``api_id``, two layers (F34, F35, F37).

This is the layer §3.4 names ``mftik_td.account``. It replaces the lease
and refcount in the old session manager, and the lifecycle half of
``session/session.py``. The connector, the OMS and the ledger stay
where they are; this package is how an account worker holds them.
Paper order entry is B4-05. ``cancel_session`` is B6-03. The TD
process spawns ``python -m mftik_td.account`` and does not import
this package. The warm HTTP pool and its keepalive are the resident
layer (B6-01). Settled ``oms.view``, ``oms.order``, backfill,
the broadcast and the dead-man's switch stay later B6 tickets.

**State authority (§3.3).** One writer each.

* OMS and ledger (pre-locks, available): the trading layer, in memory.
  The exchange is the authority for resting orders, positions and
  balances. A new incarnation rebuilds from the exchange and publishes
  ``td.account.reset``.
* Trading layer open or closed. Desired: the TD controller, from
  intent, level-triggered (IF-12). Observed: this worker. Controller
  silence does not close it (P5).
* Account availability ``ready`` / ``degraded`` / ``unavailable``: this
  worker, on ``td.account.state.{api_id}``.
* Order history, fills, cash flows: this worker. Live writes from the
  trading layer, catch-up from the resident layer's backfill, both
  into Postgres.
* The HTTP pool: the resident layer. Not a second copy of venue state.
* Cancel-on-disconnect enabled or not: the user, via the API, on
  ``apis`` (IF-14 adds the column). This worker holds the flag it was
  given and defaults it to off. It is not the authority.
* Code identity (``code_ref``, ``strategy_digest``, ``env_generation``):
  not this layer (F39, F40). This package does not carry those fields.
  ``cancel_session`` decodes a cid with
  :func:`mftik.strategy.client_order_id.session_id_of` and does not
  import a strategy.

**Invariants.** The letters are what the contract tests and B6 cite.

* **R1–R5** resident layer (F35). Up for every enabled account, with or
  without an intent. The trading-layer switch does not rebuild the
  pool or drop the keepalive. One backfill at a time, off the same
  pool as recon, leverage, HTTP orders. See :class:`ResidentLayer`.
* **T1–T4** trading layer (F35). The switch does not touch the resident
  layer. The last intent closes it immediately, no linger. Private
  socket, OMS, ledger, recon, leverage cache and the order
  subscription exist only while it is active. It does not spawn a
  second incarnation (F36 is the supervisor's pid fence). See
  :class:`TradingLayer`.
* **C1–C4** ``cancel_session`` (F10). Scope is the session field of
  ``client_order_id``. ``PENDING_NEW`` and ``UNKNOWN`` wait for chase,
  then get the same treatment. ``ok`` only when every one of them is
  confirmed; a timeout lists the rest. Positions are left alone. See
  :class:`OrderHandler`.
* **V1–V3** ``oms.view`` (F13). Unsettled is a memory read, ``UNKNOWN``
  included. Settled on a clean book is also a memory read. Settled
  with ``UNKNOWN`` waits for chase or the timeout, then answers with
  the book as it stands. See :class:`OmsHandler`.
* **B1–B5** availability broadcast (F14). The worker publishes, on
  change and every two seconds, version increasing either way. Silence
  notifies and reclaims nothing. A new incarnation publishes
  ``td.account.reset`` on ``td.{api_id}.global``. See
  :class:`StateBroadcast`.
* **D1–D6** dead-man's switch (F37). Off unless the account says
  otherwise. Countdown only, refreshed while the trading layer is up
  and orders rest. Not Deribit COD, not Bybit DCP. Drain-replace
  lengthens the countdown first. One slot per registered venue. See
  :class:`DeadMansSwitch`.

Subjects the handlers serve are ``td.order.{api_id}`` and
``td.account.{api_id}``. ``td.oms.{api_id}`` and ``td.ledger.{api_id}``
remain fan-out. See :mod:`mftik_td.account.handlers`.

Null data: reads that have no value yet return ``None`` or an empty
collection, and ``active`` and ``started`` stay false until paper
:meth:`ResidentLayer.start` and :meth:`TradingLayer.activate`.
Actions this ticket does not implement still raise
``NotImplementedError("IF-11")``.
"""

from mftik_td.account._ticket import TICKET
from mftik_td.account.broadcast import (
    INTERVAL_S,
    AccountAvailability,
    StateBroadcast,
)
from mftik_td.account.deadman import (
    SLOTS,
    BinanceCmDeadMan,
    BinanceDeadMan,
    BinanceUmDeadMan,
    BitgetDeadMan,
    BybitDeadMan,
    DeadMansSwitch,
    DeribitDeadMan,
    GateDeadMan,
    GateFuturesDeadMan,
    OkxDeadMan,
    PaperDeadMan,
    deadman_for,
)
from mftik_td.account.handlers import (
    WAIT_TIMEOUT_S,
    LedgerHandler,
    OmsHandler,
    OrderHandler,
)
from mftik_td.account.resident import Keepalive, ResidentLayer, RestPool
from mftik_td.account.trading import TradingLayer
from mftik_td.account.worker import AccountWorker

__all__ = [
    "INTERVAL_S",
    "SLOTS",
    "TICKET",
    "WAIT_TIMEOUT_S",
    "AccountAvailability",
    "AccountWorker",
    "BinanceCmDeadMan",
    "BinanceDeadMan",
    "BinanceUmDeadMan",
    "BitgetDeadMan",
    "BybitDeadMan",
    "DeadMansSwitch",
    "DeribitDeadMan",
    "GateDeadMan",
    "GateFuturesDeadMan",
    "Keepalive",
    "LedgerHandler",
    "OkxDeadMan",
    "OmsHandler",
    "OrderHandler",
    "PaperDeadMan",
    "ResidentLayer",
    "RestPool",
    "StateBroadcast",
    "TradingLayer",
    "deadman_for",
]
