"""Account availability, published by the worker (F14, §5.6).

**State authority (§3.3).** The account worker is the authority for
``ready`` / ``degraded`` / ``unavailable``. The publication is
:class:`~mftik.protocol.TdAccountState` on
``td.account.state.{api_id}``. The controller does not publish it, and
a controller rolling does not stop it (P1).

**Invariants.**

* **B1.** The worker publishes. The controller does not.
* **B2.** The subject is :meth:`Topics.td_account_state`. The payload
  is ``TdAccountState``: state, incarnation, and a version.
* **B3.** A change is published at once. Otherwise the same state is
  published every :data:`INTERVAL_S` seconds (2). ``version`` increases
  on every publication, including the steady one.
* **B4.** Ten seconds of silence is the session ingress's notification
  and nothing more (P7). This layer does not reclaim an intent, an
  order or the resident pool because a broadcast stopped.
* **B5.** After a new incarnation has rebuilt the ledger it publishes
  :class:`~mftik.protocol.TdAccountReset` on ``td.{api_id}.global``.
  Ingress treats that as ``on_resync(cause="account_reset")``.

``degraded`` is a private venue connection that is down while orders
may still go out over HTTP. ``unavailable`` is the worker itself not
answering. ``ready`` is the trading layer up and recon settled.

Null until B6-06. :meth:`current` is ``None``, and the publishes raise
``NotImplementedError("IF-11")``.
"""

from __future__ import annotations

from typing import Literal

from mftik.protocol import TdAccountReset, TdAccountState, Topics

from mftik_td.account._ticket import TICKET

#: Steady cadence (§5.6). A change does not wait for it.
INTERVAL_S = 2.0

AccountAvailability = Literal["ready", "degraded", "unavailable"]

_STATES = frozenset({"ready", "degraded", "unavailable"})


class StateBroadcast:
    """The publications for one account worker. Not started here."""

    def __init__(self, api_id: int, incarnation: int) -> None:
        self.api_id = api_id
        self.incarnation = incarnation

    @property
    def subject(self) -> str:
        """``td.account.state.{api_id}``."""
        return Topics.td_account_state(self.api_id)

    @property
    def reset_subject(self) -> str:
        """``td.{api_id}.global``, where ``td.account.reset`` is published."""
        return Topics.td_global(self.api_id)

    def current(self) -> TdAccountState | None:
        """The last publication, or ``None`` while this is a stub.

        ``None`` is not ``unavailable``. Silence is the ingress's to
        notice (B4); this method does not invent a state.
        """
        return None

    async def publish(
        self, state: AccountAvailability, *, reason: str = ""
    ) -> TdAccountState:
        """Publish a transition immediately (B3). B6-06."""
        if state not in _STATES:
            raise ValueError(
                f"account state must be ready, degraded or unavailable, got {state!r}"
            )
        if not isinstance(reason, str):
            raise TypeError("reason must be a str")
        raise NotImplementedError(TICKET)

    async def publish_steady(self) -> TdAccountState:
        """The two-second publication of the current state (B3). B6-06.

        ``version`` increases even when the state did not change.
        """
        raise NotImplementedError(TICKET)

    async def publish_reset(self) -> TdAccountReset:
        """``td.account.reset`` after this incarnation rebuilt the book (B5).

        B6-06. Not sent for an ordinary reconnect that did not replace
        the process.
        """
        raise NotImplementedError(TICKET)
