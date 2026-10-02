"""``TdOrchestrator`` — desired accounts, the trading bit, drain-replace.

The running TD process does not construct this. B4-07 wires reconcile
and the intent handler. B6-04 fills in drain-replace. B3-03 is the pid
fence inside :meth:`mftik.procman.Supervisor.spawn`.
"""

from __future__ import annotations

from collections.abc import Sequence

from mftik.procman import (
    RestartDecision,
    RestartIntensity,
    Supervisor,
    WorkerPhase,
)
from mftik.protocol import TdIntentPut

from mftik_td.controller._ticket import unimplemented
from mftik_td.controller.decisions import plan_account_restart
from mftik_td.controller.types import (
    AccountView,
    BoundAccount,
    OrchestratorAction,
)


def _views(views: object) -> Sequence[AccountView]:
    if isinstance(views, str) or not isinstance(views, Sequence):
        raise TypeError("views must be a sequence of AccountView")
    for view in views:
        if not isinstance(view, AccountView):
            raise TypeError("views must be a sequence of AccountView")
    return views  # type: ignore[return-value]


class TdOrchestrator:
    """Account workers for this instance, and the trading-layer bit (§7.2).

    **State authority (§3.3).**

    * The desired account set is this controller's, in memory, recomputed
      from the bindings the caller read. The user, through the API, is
      the authority for ``api_id`` → instance on ``apis``. This layer
      does not write that row.
    * The desired trading-layer bit is this controller's, from intents,
      level-triggered (F35). The intent rows are the API's and the STS
      controller's, in Postgres ``td_intents`` (IF-13, IF-14). This layer
      reads them. The observed bit is the account worker's. When this
      controller is not publishing, the worker keeps the last bit (P5).
    * Whether the process exists, and the exit code and signal, are the
      shim's. ``pid_gone`` on :class:`AccountView` is the supervisor's
      reading of that. This layer does not open ``/proc``.
    * ``code_ref`` is the platform release this controller was built
      with (§4.5). It is copied onto the worker spec. This layer does
      not carry ``strategy_digest`` or ``env_generation`` (F39, IF-16)
      and does not import strategy code.
    * The OMS, the ledger, the resident pool and the dead-man's-switch
      countdown belong to the account worker (IF-11). This layer names
      a drain and an extend; it does not run them.
    * A missing ``procman.report`` reclaims nothing (F32). This layer
      has no function that drops intents because a report stopped.
      B4-07 owns that rule.

    **How an account is driven through procman (P6).**

    Procman knows processes. The account set, the trading bit and
    drain-replace stay here.

    1. Build the spec with :func:`mftik_td.controller.account_worker_spec`.
       ``restart`` is ``on_failure``. ``labels`` is empty. ``code_ref``
       is the one this orchestrator was given.
    2. Ask :func:`mftik_td.controller.td_reattach` for the §4.4 cell.
       That function is :func:`mftik.procman.reattach_action` with
       ``plane="td"``. Not a second table.
    3. ``ADOPT`` names no spawn and no stop. The running worker is the
       incarnation.
    4. ``APPLY_RESTART``, and the first spawn of an account the
       supervisor does not hold, name ``SPAWN`` only when
       :func:`mftik_td.controller.spawn_allowed` is true (F36). The
       action is applied with :meth:`~mftik.procman.Supervisor.spawn`,
       which is the fence that waits until the previous pid is gone.
       The incarnation is :data:`~mftik_td.controller.FIRST_INCARNATION`
       when the supervisor holds none, otherwise the held one plus one.
    5. ``STOP_AND_RELEASE`` names ``STOP``, applied with
       :meth:`~mftik.procman.Supervisor.stop`. An account that is no
       longer desired, while its worker is still running, is this cell.
    6. A failure the supervisor has classified is
       :meth:`account_restart`: :func:`mftik.procman.plan_restart` with
       ``restart="on_failure"`` and this orchestrator's intensity.
       There is no crash class (P6). The intensity is the caller's
       (issue #286).
    7. After the recompute, trading-layer pushes come from
       :func:`mftik_td.controller.trading_pushes` with ``publish`` true,
       one per desired account. :func:`mftik_td.controller.close_actions`
       is empty for both close modes, so a controller that is leaving
       does not push the bit off (P5).
    8. :meth:`drain_replace` is the operator entry for one account
       (F27). A release rolling forward does not call it.
    """

    def __init__(
        self,
        supervisor: Supervisor,
        *,
        intensity: RestartIntensity,
        code_ref: str,
    ) -> None:
        if supervisor.plane != "td":
            raise ValueError(
                f"TD orchestrator requires a td supervisor, got {supervisor.plane!r}"
            )
        if not isinstance(intensity, RestartIntensity):
            raise TypeError(
                "intensity must be a RestartIntensity; "
                "this layer does not choose the numbers"
            )
        if not isinstance(code_ref, str) or code_ref == "":
            raise ValueError("code_ref must be a non-empty string")
        self.supervisor = supervisor
        self.intensity = intensity
        self.code_ref = code_ref

    def reconcile(
        self,
        accounts: Sequence[BoundAccount],
        intents: Sequence[TdIntentPut],
        views: Sequence[AccountView],
    ) -> tuple[OrchestratorAction, ...]:
        """Name worker actions and trading pushes for one pass (§7.2).

        ``accounts`` is the bindings the caller read, including other
        instances; the desired set is
        :func:`mftik_td.controller.desired_accounts` for this
        supervisor's instance. ``intents`` are the held puts.
        ``views`` are the supervisor's observations, including a worker
        that is running and no longer desired.

        The actions come back with spawns only where
        :func:`mftik_td.controller.spawn_allowed` is true (F36), then
        the trading pushes for the desired accounts. Empty is not an
        answer this method returns today; it raises, so a caller cannot
        mistake "not implemented" for "nothing to do".

        Not implemented (IF-12). B4-07 fills this in.
        """
        if isinstance(accounts, str) or not isinstance(accounts, Sequence):
            raise TypeError("accounts must be a sequence of BoundAccount")
        for account in accounts:
            if not isinstance(account, BoundAccount):
                raise TypeError("accounts must be a sequence of BoundAccount")
        if isinstance(intents, str) or not isinstance(intents, Sequence):
            raise TypeError("intents must be a sequence of TdIntentPut")
        for intent in intents:
            if not isinstance(intent, TdIntentPut):
                raise TypeError("intents must be a sequence of TdIntentPut")
        _views(views)
        unimplemented()

    def drain_replace(
        self,
        account: BoundAccount,
        view: AccountView,
    ) -> tuple[OrchestratorAction, ...]:
        """Operator entry: replace one account's worker (F27, §4.6, D1).

        One account, the one ``account`` names. A release upgrade does
        not call this. The platform does not drain every account.

        While the supervisor still sees the pid, the actions are
        ``EXTEND_DEADMAN``, then ``DRAIN``, then ``STOP``, and not
        ``SPAWN``. Extend happens before the process is stopped (F37).
        Drain is the worker refusing new orders with a retryable
        ``td_draining`` and collecting in-flight acks. The numeric
        reject code is B6-04's. This method does not assign one.

        Once ``view.pid_gone`` is true, the action is ``SPAWN`` of
        ``incarnation + 1`` (or the first incarnation when the
        supervisor holds none), through the same gate as reconcile
        (F36). Other accounts are not in the list. The trading bit is
        not cleared: a drain is not a deactivate (F35).

        ``TdReady`` is false between the stop and the new incarnation's
        recon, and true again after. That transition is the account
        worker's broadcast (B6-04, B6-06).

        Not implemented (IF-12). B6-04 fills this in.
        """
        if not isinstance(account, BoundAccount):
            raise TypeError("account must be a BoundAccount")
        if not isinstance(view, AccountView):
            raise TypeError("view must be an AccountView")
        if account.instance != self.supervisor.instance:
            raise ValueError(
                f"account {account.api_id} is bound to {account.instance!r}, "
                f"not {self.supervisor.instance!r}"
            )
        if account.api_id != view.api_id:
            raise ValueError(
                f"view api_id {view.api_id} does not match account {account.api_id}"
            )
        unimplemented()

    def account_restart(
        self,
        *,
        phase: WorkerPhase,
        restarts_in_window: int,
        attempt: int,
    ) -> RestartDecision:
        """Pass this orchestrator's intensity to :func:`plan_account_restart`.

        The intensity is the one the caller supplied at construction.
        There is no other.
        """
        return plan_account_restart(
            phase=WorkerPhase(phase),
            restarts_in_window=restarts_in_window,
            intensity=self.intensity,
            attempt=attempt,
        )
