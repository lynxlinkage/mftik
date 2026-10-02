"""Apply what :class:`~mftik_td.controller.TdOrchestrator` names.

The controller package does not import this module, and this module
does not import :mod:`mftik_td.account`. The worker is a process. Its
argv is a string. Spawn, stop and release are
:class:`~mftik.procman.Supervisor` calls. ``PUSH_TRADING`` is
``td.account.trading`` on ``td.account.{api_id}``, one request per
desired account per pass. A push that times out is logged and retried
next pass. It does not stop the pass.

``live_start_ticks`` is always ``None``. Procman has no public accessor
for the live start time that is not a ``/proc`` read, and this plane
does not add one. ``recorded_start_ticks`` comes from
:func:`mftik.procman.load_supervisor_state`. A held slot's phase then
replaces that boot view, because ``previous_worker_gone`` treats a
missing live time as gone. :meth:`~mftik.procman.Supervisor.spawn`'s
pid fence stays the authority when a ``LOST`` pid is still alive.
"""

from __future__ import annotations

import logging
import os
import sys
from collections.abc import Mapping, Sequence

from mftik.exchange.venues import UnknownVenueError
from mftik.procman import (
    CapacityExceeded,
    ObservedWorker,
    ProcmanError,
    RestartIntensity,
    WorkerPhase,
    WorkerStatus,
    load_supervisor_state,
)
from mftik.protocol import (
    TD_ACCOUNT_TRADING,
    TD_ERROR,
    Envelope,
    TdAccountTrading,
    Topics,
)

from mftik_td.controller.decisions import observation_view
from mftik_td.controller.defaults import (
    ACCOUNT_HB_TIMEOUT_S,
    ACCOUNT_MAX_RESTARTS,
    ACCOUNT_MIN_BACKOFF_S,
    ACCOUNT_RECONCILE_PERIOD_S,
    ACCOUNT_RESTART_WINDOW_S,
    ACCOUNT_START_TIMEOUT_S,
    ACCOUNT_STOP_GRACE_S,
)
from mftik_td.controller.types import (
    AccountView,
    ActionKind,
    BoundAccount,
    OrchestratorAction,
    account_worker_id,
)
from mftik_td.controller.worker import account_worker_spec
from mftik_td.db import bindings_for_instance

logger = logging.getLogger(__name__)

#: How long one ``td.account.trading`` request waits.
#:
#: The plan does not name it. One reconcile period is the bound: the
#: next pass sends the same desired value, and the worker's handler is
#: sequential, so a push still in flight is not a second switch.
#: Provisional until that duration is confirmed.
TRADING_PUSH_TIMEOUT_S = ACCOUNT_RECONCILE_PERIOD_S

_ALIVE = frozenset(
    {WorkerPhase.STARTING, WorkerPhase.RUNNING, WorkerPhase.STOPPING}
)


def account_restart_intensity() -> RestartIntensity:
    """The provisional bound passed to :class:`~mftik_td.controller.TdOrchestrator`.

    The numbers live in :mod:`mftik_td.controller.defaults`. The call is
    here, not in that package: the controller surface test treats a
    ``RestartIntensity`` call there as the package choosing the numbers
    (issue #286).
    """
    return RestartIntensity(
        max_restarts=ACCOUNT_MAX_RESTARTS,
        window_s=ACCOUNT_RESTART_WINDOW_S,
        min_backoff_s=ACCOUNT_MIN_BACKOFF_S,
    )


def account_worker_argv(
    api_id: int,
    incarnation: int,
    cancel_on_disconnect: bool,
) -> tuple[str, ...]:
    """``python -m mftik_td.account`` for one account. Not an import."""
    flag = "true" if cancel_on_disconnect else "false"
    return (
        sys.executable,
        "-m",
        "mftik_td.account",
        "--api-id",
        str(api_id),
        "--incarnation",
        str(incarnation),
        "--cancel-on-disconnect",
        flag,
    )


async def load_accounts(
    instance: str,
) -> tuple[tuple[BoundAccount, ...], dict[int, bool]]:
    """Bindings for ``instance``, and each row's ``cancel_on_disconnect``.

    A database failure is an empty set: the controller stays up and the
    next pass tries again (P5). A venue this process does not know is
    skipped and logged. The flag stays beside the account.
    :class:`~mftik_td.controller.BoundAccount` does not carry it.
    """
    try:
        rows = await bindings_for_instance(instance)
    except Exception:
        logger.exception("TD could not read account bindings for %s", instance)
        return (), {}
    accounts: list[BoundAccount] = []
    flags: dict[int, bool] = {}
    for row in rows:
        try:
            account = BoundAccount(
                api_id=row.api_id, venue=row.venue, instance=instance
            )
        except (UnknownVenueError, ValueError):
            logger.warning(
                "TD skipping api_id=%s venue=%r; not a bound account",
                row.api_id,
                row.venue,
            )
            continue
        accounts.append(account)
        flags[account.api_id] = row.cancel_on_disconnect
    return tuple(accounts), flags


async def account_views(supervisor, observations, accounts) -> tuple[AccountView, ...]:
    """Boot observations, then the supervisor's current slots.

    Each observation from :meth:`~mftik.procman.Supervisor.start` is
    passed to :func:`~mftik_td.controller.observation_view` with
    ``recorded_start_ticks`` from ``supervisor.json`` and
    ``live_start_ticks=None``. A slot :meth:`~mftik.procman.Supervisor.status`
    still holds replaces that view: a live phase is not gone, and a
    slot that has been released is absent. Passing the boot tuple on a
    later pass does not resurrect a worker ``status`` no longer holds.
    """
    recorded = {
        record.id: record.worker_start_ticks
        for record in load_supervisor_state(supervisor.work_dir)
    }
    views: dict[int, AccountView] = {}
    for observation in observations:
        api_id = _api_id_of(observation.id)
        if api_id is None:
            continue
        views[api_id] = observation_view(
            api_id,
            observation,
            recorded_start_ticks=recorded.get(observation.id),
            live_start_ticks=None,
        )
    wanted = {account.api_id for account in accounts}
    for api_id in set(views) | wanted:
        status = await supervisor.status(account_worker_id(api_id))
        if status is None:
            views.pop(api_id, None)
            continue
        views[api_id] = _status_view(api_id, status)
    return tuple(views.values())


async def apply_reconcile(
    supervisor,
    actions: Sequence[OrchestratorAction],
    accounts: Sequence[BoundAccount],
    *,
    code_ref: str,
    cancel_on_disconnect: Mapping[int, bool],
    broker=None,
) -> None:
    """Spawn, stop, release and push the trading bit.

    Every bound venue is spawned. ``SPAWN`` that raises
    :class:`~mftik.procman.CapacityExceeded` logs the account and
    ``exc.code`` and leaves it unspawned. A plain
    :class:`~mftik.procman.ProcmanError` is logged the same way and is
    not ``capacity_exceeded``. ``PUSH_TRADING`` is one
    ``td.account.trading`` request. A timeout, a missing broker, or an
    ack that is not the desired bit is logged. None of those stop the
    pass. The next pass sends the bit again.
    """
    by_id = {account.api_id: account for account in accounts}
    for action in actions:
        if action.kind is ActionKind.PUSH_TRADING:
            await _push_trading(broker, action)
            continue
        try:
            if action.kind is ActionKind.SPAWN:
                await _spawn(
                    supervisor,
                    action,
                    by_id.get(action.api_id),
                    code_ref=code_ref,
                    cancel_on_disconnect=cancel_on_disconnect,
                )
            elif action.kind is ActionKind.STOP:
                await supervisor.stop(account_worker_id(action.api_id))
            elif action.kind is ActionKind.RELEASE:
                await supervisor.release_slot(account_worker_id(action.api_id))
            else:
                logger.info(
                    "TD not applying %s for api_id=%s",
                    action.kind.value,
                    action.api_id,
                )
        except CapacityExceeded as exc:
            logger.warning(
                "TD leaving api_id=%s unspawned code=%s",
                action.api_id,
                exc.code,
            )
        except ProcmanError as exc:
            logger.warning(
                "TD %s failed api_id=%s: %s",
                action.kind.value,
                action.api_id,
                exc,
            )


async def _spawn(
    supervisor,
    action: OrchestratorAction,
    account: BoundAccount | None,
    *,
    code_ref: str,
    cancel_on_disconnect: Mapping[int, bool],
) -> None:
    if account is None:
        logger.warning("TD spawn named api_id=%s with no binding", action.api_id)
        return
    logger.info(
        "TD spawning api_id=%s venue=%s",
        account.api_id,
        account.venue,
    )
    incarnation = action.incarnation
    if incarnation is None:
        raise ProcmanError(f"spawn api_id={account.api_id} has no incarnation")
    flag = cancel_on_disconnect.get(account.api_id, False)
    spec = account_worker_spec(
        account,
        incarnation=incarnation,
        argv=account_worker_argv(account.api_id, incarnation, flag),
        code_ref=code_ref,
        start_timeout_s=ACCOUNT_START_TIMEOUT_S,
        hb_timeout_s=ACCOUNT_HB_TIMEOUT_S,
        stop_grace_s=ACCOUNT_STOP_GRACE_S,
        env=dict(os.environ),
    )
    await supervisor.spawn(spec)


async def _push_trading(broker, action: OrchestratorAction) -> None:
    """Deliver one trading bit. A failure is the next pass's retry."""
    if action.active is None:
        logger.warning(
            "TD trading push api_id=%s has no active bit", action.api_id
        )
        return
    if broker is None:
        logger.warning(
            "TD trading push api_id=%s active=%s not delivered; no broker",
            action.api_id,
            action.active,
        )
        return
    subject = Topics.td_account(action.api_id)
    try:
        reply = await broker.request(
            subject,
            Envelope[TdAccountTrading].wrap(
                TdAccountTrading(api_id=action.api_id, active=action.active),
                type=TD_ACCOUNT_TRADING,
                source="td",
            ),
            timeout=TRADING_PUSH_TIMEOUT_S,
        )
    except Exception:
        logger.warning(
            "TD trading push api_id=%s active=%s failed; retried next pass",
            action.api_id,
            action.active,
            exc_info=True,
        )
        return
    observed = _observed_active(reply)
    if getattr(reply, "type", None) == TD_ERROR or observed is not action.active:
        logger.warning(
            "TD trading push api_id=%s wanted %s observed %s",
            action.api_id,
            action.active,
            observed,
        )


def _observed_active(reply) -> bool | None:
    payload = getattr(reply, "payload", None)
    if payload is None:
        return None
    if isinstance(payload, dict):
        value = payload.get("active")
    else:
        value = getattr(payload, "active", None)
    if type(value) is bool:
        return value
    return None


def _status_view(api_id: int, status: WorkerStatus) -> AccountView:
    phase = status.phase
    if phase in _ALIVE:
        observed = ObservedWorker.RUNNING
        pid_gone = False
        shim_waiting = False
    elif phase is WorkerPhase.LOST:
        observed = ObservedWorker.LOST
        pid_gone = True
        shim_waiting = False
    else:
        observed = ObservedWorker.EXITED
        pid_gone = True
        shim_waiting = status.exit_code is not None or status.signal is not None
    return AccountView(
        api_id=api_id,
        observed=observed,
        pid_gone=pid_gone,
        incarnation=status.spec.incarnation,
        shim_waiting=shim_waiting,
    )


def _api_id_of(worker_id: str) -> int | None:
    prefix = "td/account/"
    if not worker_id.startswith(prefix):
        return None
    rest = worker_id[len(prefix) :]
    if not rest.isdigit():
        return None
    api_id = int(rest)
    return api_id if api_id > 0 else None
