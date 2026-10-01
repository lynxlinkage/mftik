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

IF-06 defines the surface and returns null data. The ingress that fills it in,
and the local refusal that reads it, land in B5-05.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Literal

if TYPE_CHECKING:
    from mftik.strategy.base import Strategy

#: What an account can be, worst last. See the module docstring for what each
#: one means for order entry.
AccountState = Literal["ready", "degraded", "unavailable"]


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

        None means this session is not using ``api_id`` at all, which is a
        different answer from ``"unavailable"``: one is a configuration
        mistake and the other is an account having a bad minute.
        """
        return None
