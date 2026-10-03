"""Decisions the TD orchestrator makes. Drain-replace is not one of them.

Procman owns the process table and the restart arithmetic (P6). This
module does not restate either.

* :func:`td_reattach` is :func:`mftik.procman.reattach_action` with
  ``plane="td"``. B3-03 implements that table. B4-07 makes this function
  call it.
* :func:`spawn_allowed` is the gate in front of
  :meth:`mftik.procman.Supervisor.spawn`. ``pid_gone`` is
  :func:`mftik.procman.previous_worker_gone` (F36). This function does
  not read ``/proc`` and does not change ``spawn``. Reconcile names a
  spawn only when the gate is open.
* :func:`release_named` is the ``release_slot`` rule.
  ``MARK_FAILED`` names a release. ``NONE`` names one only while the
  shim is still waiting (S3).
* :func:`plan_account_restart` is :func:`mftik.procman.plan_restart` with
  ``restart="on_failure"`` and the intensity the caller supplied.
  The defaults the TD process passes are Appendix D. F42 changes that
  curve; B3-08 (#365) owns the change. This module does not construct a
  :class:`~mftik.procman.RestartIntensity`.

Account membership and the trading-layer bit are this layer's (F35).
They are not procman vocabulary.
"""

from __future__ import annotations

from collections.abc import Sequence

from mftik.instance import validate_instance_name
from mftik.procman import (
    CloseMode,
    DesiredSlot,
    ObservedWorker,
    ReattachAction,
    ReattachObservation,
    RestartDecision,
    RestartIntensity,
    WorkerPhase,
    plan_restart,
    previous_worker_gone,
    reattach_action,
)
from mftik.protocol import TdIntentDelete, TdIntentPut

from mftik_td.controller.types import (
    AccountView,
    BoundAccount,
    OrchestratorAction,
    TradingDesired,
    account_worker_id,
    positive_api_id,
)


def _sequence(value: object, name: str) -> Sequence[object]:
    if isinstance(value, str) or not isinstance(value, Sequence):
        raise TypeError(f"{name} must be a sequence")
    return value


def _api_ids(ids: object) -> None:
    for api_id in _sequence(ids, "api_ids"):
        positive_api_id(api_id)


def _puts(held: object) -> Sequence[TdIntentPut]:
    items = _sequence(held, "intents")
    for item in items:
        if not isinstance(item, TdIntentPut):
            raise TypeError("intents must be a sequence of TdIntentPut")
        _api_ids(item.api_ids)
    return items  # type: ignore[return-value]


def _accounts(bound: object) -> Sequence[BoundAccount]:
    items = _sequence(bound, "accounts")
    for item in items:
        if not isinstance(item, BoundAccount):
            raise TypeError("accounts must be a sequence of BoundAccount")
    return items  # type: ignore[return-value]


def _bool(value: object, name: str) -> None:
    if not isinstance(value, bool):
        raise ValueError(f"{name} must be a bool")


def desired_accounts(
    bound: Sequence[BoundAccount],
    *,
    instance: str,
) -> tuple[BoundAccount, ...]:
    """Accounts this instance runs a worker for (F35, W1, W2).

    Every ``bound`` row whose ``instance`` is this one, in that order.
    An intent is not an argument: it does not add an account and it does
    not remove one. An account bound to another instance is absent (F36).

    B4-07. An empty result means this instance has no bound accounts.
    """
    validate_instance_name(instance)
    accounts = _accounts(bound)
    return tuple(account for account in accounts if account.instance == instance)


def apply_put(
    held: Sequence[TdIntentPut],
    put: TdIntentPut,
) -> tuple[TdIntentPut, ...]:
    """Replace the row with this owner (P-1, L1).

    The owner is ``(owner.sts_instance, owner.session_id)``. Other owners
    stay, in their previous order. A new owner is appended. A second put
    of the same owner occupies the same slot and replaces ``api_ids``
    wholesale, including when the new set is empty. It does not add a
    count.

    B4-07.
    """
    rows = _puts(held)
    if not isinstance(put, TdIntentPut):
        raise TypeError("put must be a TdIntentPut")
    _api_ids(put.api_ids)
    replaced = False
    kept: list[TdIntentPut] = []
    for row in rows:
        if row.owner == put.owner:
            kept.append(put)
            replaced = True
        else:
            kept.append(row)
    if not replaced:
        kept.append(put)
    return tuple(kept)


def apply_delete(
    held: Sequence[TdIntentPut],
    delete: TdIntentDelete,
) -> tuple[TdIntentPut, ...]:
    """Release accounts for one owner (§8.1, L2).

    ``api_ids`` empty drops that owner's row. A non-empty list drops
    those accounts and leaves the rest, in the order the row already
    had. An owner that is already gone, or an account the owner does
    not hold, leaves ``held`` unchanged. Idempotent.

    B4-07. An empty remainder stays as a row with no accounts: a put may
    name an empty set, and this is that set. The owner is dropped only
    when ``api_ids`` on the delete is empty.
    """
    rows = _puts(held)
    if not isinstance(delete, TdIntentDelete):
        raise TypeError("delete must be a TdIntentDelete")
    _api_ids(delete.api_ids)
    if not delete.api_ids:
        kept = tuple(row for row in rows if row.owner != delete.owner)
        if len(kept) == len(rows):
            return tuple(rows)
        return kept
    drop = set(delete.api_ids)
    changed = False
    out: list[TdIntentPut] = []
    for row in rows:
        if row.owner != delete.owner:
            out.append(row)
            continue
        remaining = [api_id for api_id in row.api_ids if api_id not in drop]
        if remaining == list(row.api_ids):
            out.append(row)
            continue
        changed = True
        out.append(row.model_copy(update={"api_ids": remaining}))
    if not changed:
        return tuple(rows)
    return tuple(out)


def trading_active(api_id: int, intents: Sequence[TdIntentPut]) -> bool:
    """Whether the trading layer should be on (F35, L1).

    True when any held intent's ``api_ids`` contains ``api_id``. Two
    owners, or the same id twice in one put, are still just on. No
    membership is off. There is no linger in this predicate: the last
    intent clearing makes it false (L2). The worker is what closes the
    private book.

    B4-07.
    """
    positive_api_id(api_id)
    rows = _puts(intents)
    return any(api_id in row.api_ids for row in rows)


def trading_pushes(
    *,
    publish: bool,
    accounts: Sequence[BoundAccount],
    intents: Sequence[TdIntentPut],
) -> tuple[TradingDesired, ...]:
    """The trading-layer pushes for one reconcile (P2, P5, L3).

    ``publish`` false: nothing. The controller is gone, or it has not
    finished recomputing. The worker keeps the last desired it was
    given. Silence is not a deactivate.

    ``publish`` true: one :class:`TradingDesired` per account, in
    ``accounts`` order. ``active`` is :func:`trading_active`. The same
    inputs produce the same pushes. ``accounts`` is already the desired
    set; this function does not filter by instance.

    B4-07.
    """
    _bool(publish, "publish")
    bound = _accounts(accounts)
    rows = _puts(intents)
    if not publish:
        return ()
    return tuple(
        TradingDesired(
            api_id=account.api_id,
            active=any(account.api_id in row.api_ids for row in rows),
        )
        for account in bound
    )


def td_reattach(
    *,
    desired: DesiredSlot,
    observed: ObservedWorker,
) -> ReattachAction:
    """The TD cell of §4.4 (N2).

    The body is
    ``reattach_action(plane="td", desired=desired, observed=observed)``
    and nothing else. Not a second table. B3-03 is what makes
    :func:`mftik.procman.reattach_action` answer.
    """
    return reattach_action(
        plane="td",
        desired=DesiredSlot(desired),
        observed=ObservedWorker(observed),
    )


def account_pid_gone(
    *,
    recorded_start_ticks: int | None,
    live_start_ticks: int | None,
) -> bool:
    """``AccountView.pid_gone`` (F36).

    The body is :func:`mftik.procman.previous_worker_gone` and nothing
    else. A live pid with no recorded start time stays "not gone",
    because reuse cannot be ruled out. This does not read ``/proc``.
    """
    return previous_worker_gone(
        recorded_start_ticks=recorded_start_ticks,
        live_start_ticks=live_start_ticks,
    )


def _shim_waiting(observation: ReattachObservation) -> bool:
    """True when the socket answered and the worker has already exited (S3)."""
    status = observation.status
    if status is None:
        return False
    return status.exit_code is not None or status.signal is not None


def observation_view(
    api_id: int,
    observation: ReattachObservation,
    *,
    recorded_start_ticks: int | None,
    live_start_ticks: int | None,
) -> AccountView:
    """One row of :meth:`mftik.procman.Supervisor.start` as an account view.

    ``start`` returns the observations and opens the report itself.
    This function does not call it, does not call ``allow_reports``,
    and does not read ``/proc``. ``pid_gone`` is :func:`account_pid_gone`.
    ``shim_waiting`` is the socket that is still up after the worker
    exited. An exit file alone is not that: the socket did not answer.
    """
    if not isinstance(observation, ReattachObservation):
        raise TypeError("observation must be a ReattachObservation")
    worker_id = account_worker_id(api_id)
    if observation.id != worker_id:
        raise ValueError(f"observation {observation.id} is not {worker_id}")
    return AccountView(
        api_id=api_id,
        observed=observation.observed,
        pid_gone=account_pid_gone(
            recorded_start_ticks=recorded_start_ticks,
            live_start_ticks=live_start_ticks,
        ),
        incarnation=observation.incarnation,
        shim_waiting=_shim_waiting(observation),
    )


def release_named(cell: ReattachAction, *, shim_waiting: bool) -> bool:
    """Whether reconcile names ``RELEASE`` for this §4.4 cell.

    Applied with :meth:`mftik.procman.Supervisor.release_slot`, not with
    ``spawn``. ``MARK_FAILED`` always does: the exit is already on the
    observation and nothing is spawned. ``NONE`` does when
    ``shim_waiting`` is true. The table's ``NONE`` does not spawn or
    stop, and a shim that has reaped its worker still waits for
    ``release`` (S3). ``ADOPT``, ``APPLY_RESTART`` and
    ``STOP_AND_RELEASE`` do not: spawn replaces a held slot, and stop
    releases the shim when the worker reaches ``STOPPED``.

    B4-07 names the action. It does not call ``release_slot``.
    """
    if not isinstance(cell, ReattachAction):
        raise TypeError("cell must be a ReattachAction")
    _bool(shim_waiting, "shim_waiting")
    if cell is ReattachAction.MARK_FAILED:
        return True
    if cell is ReattachAction.NONE:
        return shim_waiting
    return False


def spawn_allowed(*, previous: bool, pid_gone: bool) -> bool:
    """Whether reconcile may name a new incarnation (F36, N1).

    ``previous`` is false when the supervisor holds no incarnation for
    this account. The first spawn does not wait. ``previous`` true
    requires ``pid_gone``, which is :func:`account_pid_gone`: the
    supervisor has seen the old pid leave. A live pid means this
    returns false, and reconcile does not name ``SPAWN``.

    This is the gate in front of :meth:`mftik.procman.Supervisor.spawn`.
    That call is the fence. This function does not open ``/proc``, does
    not start a process, and does not change ``spawn``. ``ADOPT`` does
    not consult it: a running worker is not a new incarnation.

    B4-07.
    """
    _bool(previous, "previous")
    _bool(pid_gone, "pid_gone")
    if not previous:
        return True
    return pid_gone


def plan_account_restart(
    *,
    phase: WorkerPhase,
    restarts_in_window: int,
    intensity: RestartIntensity,
    attempt: int,
) -> RestartDecision:
    """Where an account-worker failure goes (K1, §4.3, §7.2).

    The body is :func:`mftik.procman.plan_restart` with
    ``restart="on_failure"`` and this ``intensity``. ``attempt`` starts
    at 1 and feeds the backoff curve only. Crash class is not an
    argument: procman does not know why a process died, and an account
    worker has no A/B/C (P6).

    ``intensity`` is the caller's. This function does not build one and
    does not substitute a default (Appendix D).

    B4-07. The numbers stay the caller's.
    """
    named = WorkerPhase(phase)
    if not isinstance(intensity, RestartIntensity):
        raise TypeError(
            "intensity must be a RestartIntensity; "
            "this layer does not choose the numbers"
        )
    if type(restarts_in_window) is not int or restarts_in_window < 0:
        raise ValueError("restarts_in_window must be an int >= 0")
    if type(attempt) is not int or attempt < 1:
        raise ValueError("attempt starts at 1")
    return plan_restart(
        phase=named,
        restart="on_failure",
        restarts_in_window=restarts_in_window,
        intensity=intensity,
        attempt=attempt,
    )


def close_actions(mode: CloseMode) -> tuple[OrchestratorAction, ...]:
    """TD actions added on top of :meth:`mftik.procman.Supervisor.close`.

    Both modes add nothing. ``DETACH`` leaves workers running and does
    not push a trading bit, so a controller that is leaving does not
    deactivate the trading layer (P5, L3). ``STOP`` is the host going
    down: the supervisor signals every worker. That signal is
    ``Supervisor.close``, not a trading-layer push and not a row of
    this tuple.

    B4-07. Both modes are empty on purpose.
    """
    CloseMode(mode)
    return ()
