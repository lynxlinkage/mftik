"""Strategy-side reads of account *availability*, not of account contents.

One question: can this account trade right now (F14, §5.6). What the account
holds is :mod:`mftik.strategy.oms` and :mod:`mftik.strategy.ledger`; what
happened to an order is ``on_order_update`` and ``on_fill``. This accessor only
says whether the thing those read from is answering.

Three states, and the middle one is the one worth reading carefully:

``ready``
    The account worker's trading layer is up and its private venue
    connection is healthy.
``degraded``
    Orders can probably still be sent, but fill reports will be late — the
    private stream dropped while the HTTP side is fine. Submits are **not**
    refused here: a strategy that must flatten is better served by a slow
    confirmation than by a refusal.
``unavailable``
    The worker is restarting, has changed incarnation, or has gone silent for
    ten seconds. Submits and cancels are refused locally with
    :attr:`~mftik.protocol.reject_codes.RejectCode.TD_UNAVAILABLE` and never
    reach the wire, which keeps the existing meaning of ``False`` from
    :meth:`~mftik.strategy.oms.StrategyOms.submit_order` intact — it did not
    reach the venue.

**State authority (§3.3):** the account state belongs to the TD account worker,
which broadcasts it on ``td.account.state.{api_id}``; the session's ingress
follows that broadcast and notifies through ``on_td_update``. Ten seconds of
silence reads as ``unavailable``: a notification only, never a reclaim of
anything. The OMS and ledger behind it are the same worker's memory, so a
strategy that sees ``unavailable`` should expect the books it reads next to
have been rebuilt from the venue — that arrives as ``on_resync`` with
``cause="account_reset"`` before the account returns to ``ready``.

**Invariants:**

* Losing an account never fails the session by itself. The platform notifies;
  whether a strategy can survive a gap is the strategy's judgement, and there
  is no "fail after N seconds" setting to configure instead.
* An ``unavailable`` account is still attached. The intent is not released and
  the session keeps its accounts — this is a worker that went away, not a
  deployment that changed.

:meth:`StrategyTd.state` is filled by the session worker from
``td.account.state.{api_id}``. ``None`` until the first broadcast means
unknown, not ``unavailable``, and order entry is not refused locally while
it is unknown. The local refusal of an ``unavailable`` account lives on
:meth:`~mftik.strategy.oms.StrategyOms.submit_order` and
:meth:`~mftik.strategy.oms.StrategyOms.cancel_order`.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Literal

if TYPE_CHECKING:
    from mftik.strategy.base import Strategy

#: What an account can be, worst last. See the module docstring for what each
#: one means for order entry.
AccountState = Literal["ready", "degraded", "unavailable"]

#: Why the platform reconciled an account behind the strategy's back (F13).
#: ``reconnect`` is the session's own broker connection having dropped, so
#: events published while it was away are gone; ``account_reset`` is the
#: account worker having rebuilt its book from the venue. Both are platform
#: judgements — a strategy never asks for a resync, and TD's own reconcile
#: after a *venue* reconnect is not one of these, because its findings arrive
#: as ordinary order updates.
ResyncCause = Literal["reconnect", "account_reset"]


class StrategyTd:
    """Account availability, as a strategy reads it.

    Read-only. The push side of the same fact is ``on_td_update``; this is for
    asking at a moment of the strategy's choosing.
    """

    def __init__(self) -> None:
        self._strategy: Strategy | None = None

    def bind(self, strategy: Strategy) -> None:
        self._strategy = strategy

    def state(self, api_id: int) -> AccountState | None:
        """``"ready"``, ``"degraded"`` or ``"unavailable"``, or None.

        None means either this session is not using ``api_id``, or the
        account has not broadcast yet. Both are different from
        ``"unavailable"``: unknown does not refuse orders locally. TD's own
        ``TD_VENUE_NOT_CONNECTED`` is still the gate until a broadcast arms
        the silence timer.
        """
        strategy = self._strategy
        session = getattr(strategy, "session", None) if strategy is not None else None
        reader = (
            getattr(session, "account_state", None) if session is not None else None
        )
        if reader is None:
            return None
        return reader(api_id)
