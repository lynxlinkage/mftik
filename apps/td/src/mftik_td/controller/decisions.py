"""Decisions the TD orchestrator makes. Drain-replace is not one of them.

Procman owns the process table and the restart arithmetic (P6). This
module does not restate either.

* :func:`td_reattach` is :func:`mftik.procman.reattach_action` with
  ``plane="td"``. B3-03 implements that table. B4-07 makes this function
  call it.
* :func:`spawn_allowed` is the gate in front of
  :meth:`mftik.procman.Supervisor.spawn`. The supervisor's spawn is the
  fence that waits for the pid (F36, B3-03). This function does not read
  ``/proc``. Reconcile names a spawn only when the gate is open, so a
  caller cannot start an incarnation early.
* :func:`plan_account_restart` is :func:`mftik.procman.plan_restart` with
  ``restart="on_failure"`` and the intensity the caller supplied.
  Restart-intensity numbers for MD and TD are not decided (issue #286).
  This module does not construct a
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
    RestartDecision,
    RestartIntensity,
    WorkerPhase,
    plan_restart,
    reattach_action,
)
from mftik.protocol import TdIntentDelete, TdIntentPut

from mftik_td.controller.types import (
    BoundAccount,
    OrchestratorAction,
    TradingDesired,
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


def spawn_allowed(*, previous: bool, pid_gone: bool) -> bool:
    """Whether reconcile may name a new incarnation (F36, N1).

    ``previous`` is false when the supervisor holds no incarnation for
    this account. The first spawn does not wait. ``previous`` true
    requires ``pid_gone``: the supervisor has seen the old pid leave.
    A live pid means this returns false, and reconcile does not name
    ``SPAWN``.

    This is the gate in front of :meth:`mftik.procman.Supervisor.spawn`.
    That call is the fence that waits (B3-03). This function does not
    open ``/proc`` and it does not start a process. ``ADOPT`` does not
    consult it: a running worker is not a new incarnation.

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
    does not substitute a default (issue #286).

    B4-07. The numbers stay the caller's (issue #286).
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
