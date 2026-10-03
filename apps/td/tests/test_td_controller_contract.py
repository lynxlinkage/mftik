"""What the TD controller does (IF-12, B4-07).

The account set, the trading level, the spawn gate, restart planning,
reconcile and drain-replace answer. Reconcile uses procman's §4.4
table (B3-03). Drain-replace here is the pure plan (B6-04, F27).
Waiting, stopping and the replacement spawn are the supervisor's.

Nothing here starts a process or opens ``/proc``. The pid fence is the
supervisor's observation on :class:`~mftik_td.controller.AccountView`,
and the spawn it gates is :meth:`mftik.procman.Supervisor.spawn`. The
§4.4 cell is :func:`mftik.procman.reattach_action` for ``td``.
"""

from __future__ import annotations

from pathlib import Path

import pytest
from mftik.procman import (
    CloseMode,
    DesiredSlot,
    ExitRecord,
    ObservedWorker,
    ReattachAction,
    ReattachObservation,
    RestartIntensity,
    ShimStatus,
    Supervisor,
    WorkerPhase,
    plan_restart,
    previous_worker_gone,
    reattach_action,
)
from mftik.protocol import IntentOwner, TdIntentDelete, TdIntentPut
from mftik_td.controller import (
    FIRST_INCARNATION,
    AccountView,
    ActionKind,
    BoundAccount,
    OrchestratorAction,
    TdOrchestrator,
    TradingDesired,
    account_pid_gone,
    apply_delete,
    apply_put,
    close_actions,
    desired_accounts,
    observation_view,
    release_named,
    spawn_allowed,
    td_reattach,
    trading_active,
    trading_pushes,
)


def _intensity() -> RestartIntensity:
    return RestartIntensity(max_restarts=2, window_s=30, min_backoff_s=0.5)


def _orch(tmp_path: Path, intensity: RestartIntensity | None = None) -> TdOrchestrator:
    return TdOrchestrator(
        Supervisor(tmp_path, plane="td", instance="td"),
        intensity=intensity or _intensity(),
        code_ref="v1",
    )


def _account(api_id: int = 7, instance: str = "td") -> BoundAccount:
    return BoundAccount(api_id=api_id, venue="Paper", instance=instance)


def _owner(session_id: str, sts_instance: str = "sts") -> IntentOwner:
    return IntentOwner(sts_instance=sts_instance, session_id=session_id)


def _put(session_id: str, *api_ids: int, sts_instance: str = "sts") -> TdIntentPut:
    return TdIntentPut(
        session_id=session_id,
        owner=_owner(session_id, sts_instance),
        api_ids=list(api_ids),
    )


def _delete(session_id: str, *api_ids: int) -> TdIntentDelete:
    return TdIntentDelete(
        session_id=session_id,
        owner=_owner(session_id),
        api_ids=list(api_ids),
    )


def _view(
    api_id: int = 7,
    observed: ObservedWorker = ObservedWorker.EXITED,
    *,
    pid_gone: bool = False,
    incarnation: int | None = 1,
    shim_waiting: bool = False,
) -> AccountView:
    return AccountView(
        api_id=api_id,
        observed=observed,
        pid_gone=pid_gone,
        incarnation=incarnation,
        shim_waiting=shim_waiting,
    )


# --- desired accounts (F35, F36) -------------------------------------------


def test_desired_accounts_are_this_instances_bindings_in_input_order() -> None:
    """No intent is required. Another instance's account is absent (W1, W2)."""
    other = _account(8, instance="td-jp")
    later = _account(9)
    mine = _account(7)
    assert desired_accounts([other, later, mine], instance="td") == (later, mine)


# --- intent → trading level (F35) ------------------------------------------


def test_a_second_put_replaces_that_owners_accounts() -> None:
    """The put is the whole set (L1). It does not add a count."""
    held = apply_put((), _put("abc", 7, 8))
    held = apply_put(held, _put("abc", 8))
    assert trading_active(7, held) is False
    assert trading_active(8, held) is True


def test_one_owner_leaving_leaves_an_account_another_still_holds() -> None:
    held = apply_put((), _put("abc", 7))
    held = apply_put(held, _put("def", 7))
    held = apply_delete(held, _delete("abc"))
    assert trading_active(7, held) is True


def test_delete_without_api_ids_releases_that_owner() -> None:
    held = apply_put((), _put("abc", 7, 8))
    held = apply_delete(held, _delete("abc"))
    assert held == ()
    assert trading_active(7, held) is False
    assert trading_active(8, held) is False


def test_delete_of_one_account_leaves_the_rest() -> None:
    held = apply_put((), _put("abc", 7, 8))
    held = apply_delete(held, _delete("abc", 7))
    assert trading_active(7, held) is False
    assert trading_active(8, held) is True


def test_delete_of_an_absent_owner_keeps_the_held_set() -> None:
    held = apply_put((), _put("abc", 7))
    assert apply_delete(held, _delete("def")) == held
    assert apply_delete(held, _delete("abc", 9)) == held


def test_duplicate_api_ids_are_still_just_on() -> None:
    """Membership, not a refcount. Dropping the id once turns the bit off."""
    held = apply_put((), _put("abc", 7, 7))
    assert trading_active(7, held) is True
    held = apply_delete(held, _delete("abc", 7))
    assert trading_active(7, held) is False


def test_a_published_level_names_every_desired_account() -> None:
    """One bit per account, in the account order, from membership (L1, L2)."""
    pushes = trading_pushes(
        publish=True,
        accounts=(_account(8), _account(7)),
        intents=(_put("abc", 7),),
    )
    assert pushes == (
        TradingDesired(api_id=8, active=False),
        TradingDesired(api_id=7, active=True),
    )
    assert trading_pushes(
        publish=True,
        accounts=(_account(8), _account(7)),
        intents=(_put("abc", 7),),
    ) == pushes


# --- P5 --------------------------------------------------------------------


def test_an_absent_controller_pushes_nothing_and_the_last_desired_stands() -> None:
    """Silence is not a deactivate. The worker keeps the bit it already has."""
    previous = TradingDesired(api_id=7, active=True)
    pushes = trading_pushes(publish=False, accounts=(_account(),), intents=())
    assert pushes == ()
    assert previous.active is True
    assert TradingDesired(api_id=7, active=False) not in pushes


def test_reconcile_publishes_nothing_until_the_book_is_seeded(tmp_path: Path) -> None:
    """An empty book is not a deactivate. ``publish`` false names no push (P5).

    ``publish`` true with that same empty book is a seed that found no
    unreleased intent, and the bit is false.
    """
    view = _view(observed=ObservedWorker.RUNNING, pid_gone=False, incarnation=1)
    held = _orch(tmp_path).reconcile((_account(),), (), (view,), publish=False)
    assert all(action.kind is not ActionKind.PUSH_TRADING for action in held)
    seeded = _orch(tmp_path).reconcile((_account(),), (), (view,), publish=True)
    assert [
        action for action in seeded if action.kind is ActionKind.PUSH_TRADING
    ] == [
        OrchestratorAction(kind=ActionKind.PUSH_TRADING, api_id=7, active=False),
    ]


def test_close_does_not_push_a_trading_change() -> None:
    """DETACH leaves workers running. STOP signals them through the
    supervisor, not through a trading bit. Neither mode deactivates."""
    assert close_actions(CloseMode.DETACH) == ()
    assert close_actions(CloseMode.STOP) == ()


# --- F36 and reattach ------------------------------------------------------


def test_spawn_allowed_requires_the_old_pid_to_be_gone() -> None:
    """The first incarnation does not wait. A later one waits for the pid."""
    assert spawn_allowed(previous=True, pid_gone=False) is False
    assert spawn_allowed(previous=True, pid_gone=True) is True
    assert spawn_allowed(previous=False, pid_gone=False) is True
    assert spawn_allowed(previous=False, pid_gone=True) is True


def test_account_pid_gone_is_previous_worker_gone() -> None:
    """``AccountView.pid_gone`` is the F36 rule. This layer does not read ``/proc``."""
    cases = (
        (10, None),
        (None, None),
        (10, 10),
        (10, 11),
        (None, 11),
    )
    for recorded, live in cases:
        assert account_pid_gone(
            recorded_start_ticks=recorded, live_start_ticks=live
        ) is previous_worker_gone(
            recorded_start_ticks=recorded, live_start_ticks=live
        )


def test_release_is_mark_failed_or_a_shim_still_waiting() -> None:
    """``release_slot``. Spawn and stop are the other cells."""
    assert release_named(ReattachAction.MARK_FAILED, shim_waiting=False) is True
    assert release_named(ReattachAction.MARK_FAILED, shim_waiting=True) is True
    assert release_named(ReattachAction.NONE, shim_waiting=True) is True
    assert release_named(ReattachAction.NONE, shim_waiting=False) is False
    for cell in (
        ReattachAction.ADOPT,
        ReattachAction.APPLY_RESTART,
        ReattachAction.STOP_AND_RELEASE,
    ):
        assert release_named(cell, shim_waiting=True) is False
        assert release_named(cell, shim_waiting=False) is False


def _observation(
    observed: ObservedWorker,
    *,
    status: ShimStatus | None = None,
    exit_record: ExitRecord | None = None,
) -> ReattachObservation:
    return ReattachObservation(
        id="td/account/7",
        observed=observed,
        spec=None,
        incarnation=2,
        status=status,
        exit_record=exit_record,
    )


def test_observation_view_stores_the_pid_rule_and_a_waiting_shim() -> None:
    waiting = ShimStatus(
        id="td/account/7",
        incarnation=2,
        pid=40,
        ready=True,
        exit_code=1,
    )
    view = observation_view(
        7,
        _observation(ObservedWorker.EXITED, status=waiting),
        recorded_start_ticks=10,
        live_start_ticks=None,
    )
    assert view.pid_gone is True
    assert view.shim_waiting is True
    assert view.observed is ObservedWorker.EXITED
    assert view.incarnation == 2
    alive = ShimStatus(id="td/account/7", incarnation=2, pid=40, ready=True)
    running = observation_view(
        7,
        _observation(ObservedWorker.RUNNING, status=alive),
        recorded_start_ticks=10,
        live_start_ticks=10,
    )
    assert running.pid_gone is False
    assert running.shim_waiting is False
    filed = ExitRecord(
        id="td/account/7",
        incarnation=2,
        pid=40,
        exit_code=1,
        signal=None,
        ready=True,
    )
    gone = observation_view(
        7,
        _observation(ObservedWorker.EXITED, exit_record=filed),
        recorded_start_ticks=None,
        live_start_ticks=11,
    )
    assert gone.shim_waiting is False
    assert gone.pid_gone is False


@pytest.mark.parametrize(
    ("desired", "observed"),
    [
        (DesiredSlot.PRESENT, ObservedWorker.RUNNING),
        (DesiredSlot.PRESENT, ObservedWorker.ABSENT),
        (DesiredSlot.PRESENT, ObservedWorker.EXITED),
        (DesiredSlot.PRESENT, ObservedWorker.LOST),
        (DesiredSlot.ABSENT, ObservedWorker.RUNNING),
        (DesiredSlot.ABSENT, ObservedWorker.ABSENT),
        (DesiredSlot.ABSENT, ObservedWorker.EXITED),
        (DesiredSlot.ABSENT, ObservedWorker.LOST),
    ],
)
def test_td_reattach_is_the_procman_table(
    desired: DesiredSlot, observed: ObservedWorker
) -> None:
    assert td_reattach(desired=desired, observed=observed) is reattach_action(
        plane="td", desired=desired, observed=observed
    )


def test_the_first_incarnation_does_not_wait_for_a_pid(tmp_path: Path) -> None:
    actions = _orch(tmp_path).reconcile((_account(),), (), ())
    spawns = [action for action in actions if action.kind is ActionKind.SPAWN]
    assert spawns == [
        OrchestratorAction(
            kind=ActionKind.SPAWN, api_id=7, incarnation=FIRST_INCARNATION
        )
    ]


def test_a_new_incarnation_is_not_named_while_the_old_pid_is_alive(
    tmp_path: Path,
) -> None:
    view = _view(observed=ObservedWorker.EXITED, pid_gone=False, incarnation=1)
    actions = _orch(tmp_path).reconcile((_account(),), (), (view,))
    assert all(action.kind is not ActionKind.SPAWN for action in actions)


def test_a_new_incarnation_is_named_once_the_old_pid_is_gone(tmp_path: Path) -> None:
    view = _view(observed=ObservedWorker.EXITED, pid_gone=True, incarnation=1)
    actions = _orch(tmp_path).reconcile((_account(),), (), (view,))
    spawns = [action for action in actions if action.kind is ActionKind.SPAWN]
    assert spawns == [
        OrchestratorAction(kind=ActionKind.SPAWN, api_id=7, incarnation=2)
    ]


def test_a_lost_worker_is_not_replaced_while_its_pid_is_alive(tmp_path: Path) -> None:
    view = _view(observed=ObservedWorker.LOST, pid_gone=False, incarnation=2)
    actions = _orch(tmp_path).reconcile((_account(),), (), (view,))
    assert all(action.kind is not ActionKind.SPAWN for action in actions)


def test_a_running_account_is_not_spawned_beside_itself(tmp_path: Path) -> None:
    """ADOPT. The live pid is the incarnation (P4, F36)."""
    view = _view(observed=ObservedWorker.RUNNING, pid_gone=False, incarnation=4)
    actions = _orch(tmp_path).reconcile((_account(),), (), (view,))
    assert all(action.kind is not ActionKind.SPAWN for action in actions)
    assert all(action.kind is not ActionKind.STOP for action in actions)


def test_a_waiting_shim_on_an_undesired_account_is_released(tmp_path: Path) -> None:
    """``NONE`` does not stop or spawn. The shim still has to be released (S3)."""
    view = _view(observed=ObservedWorker.EXITED, shim_waiting=True, incarnation=2)
    actions = _orch(tmp_path).reconcile((), (), (view,))
    assert actions == (OrchestratorAction(kind=ActionKind.RELEASE, api_id=7),)


def test_an_exit_file_with_no_shim_names_nothing(tmp_path: Path) -> None:
    """``NONE`` without a waiting shim is the blank cell."""
    view = _view(observed=ObservedWorker.EXITED, shim_waiting=False, incarnation=2)
    assert _orch(tmp_path).reconcile((), (), (view,)) == ()


def test_a_desired_restart_does_not_release_the_slot_itself(tmp_path: Path) -> None:
    """``APPLY_RESTART`` names ``SPAWN``. Spawn replaces the held slot."""
    view = _view(
        observed=ObservedWorker.EXITED,
        pid_gone=True,
        incarnation=1,
        shim_waiting=True,
    )
    actions = _orch(tmp_path).reconcile((_account(),), (), (view,))
    assert OrchestratorAction(kind=ActionKind.RELEASE, api_id=7) not in actions
    spawns = [action for action in actions if action.kind is ActionKind.SPAWN]
    assert spawns == [
        OrchestratorAction(kind=ActionKind.SPAWN, api_id=7, incarnation=2)
    ]


def test_an_account_that_is_no_longer_desired_is_stopped(tmp_path: Path) -> None:
    view = _view(observed=ObservedWorker.RUNNING, pid_gone=False, incarnation=4)
    actions = _orch(tmp_path).reconcile((), (), (view,))
    assert any(
        action.kind is ActionKind.STOP and action.api_id == 7 for action in actions
    )
    assert all(action.kind is not ActionKind.SPAWN for action in actions)


def test_reconcile_does_not_spawn_another_instances_account(tmp_path: Path) -> None:
    actions = _orch(tmp_path).reconcile((_account(8, instance="td-jp"),), (), ())
    assert all(action.kind is not ActionKind.SPAWN for action in actions)
    assert all(action.api_id != 8 for action in actions)


def test_reconcile_pushes_the_trading_level_for_desired_accounts(
    tmp_path: Path,
) -> None:
    """A running worker is not respawned. The level is still pushed."""
    actions = _orch(tmp_path).reconcile(
        (_account(7), _account(8)),
        (_put("abc", 7),),
        (
            _view(7, ObservedWorker.RUNNING, pid_gone=False, incarnation=1),
            _view(8, ObservedWorker.RUNNING, pid_gone=False, incarnation=1),
        ),
    )
    pushes = [action for action in actions if action.kind is ActionKind.PUSH_TRADING]
    assert pushes == [
        OrchestratorAction(kind=ActionKind.PUSH_TRADING, api_id=7, active=True),
        OrchestratorAction(kind=ActionKind.PUSH_TRADING, api_id=8, active=False),
    ]
    assert all(action.kind is not ActionKind.SPAWN for action in actions)


# --- restart intensity (Appendix D, F42) -----------------------------------


def test_account_restart_uses_the_callers_intensity(tmp_path: Path) -> None:
    """``on_failure``, and the intensity this orchestrator was given. No other."""
    intensity = RestartIntensity(max_restarts=2, window_s=30, min_backoff_s=0.5)
    orch = _orch(tmp_path, intensity)
    for phase in (WorkerPhase.CRASHED, WorkerPhase.FAILED):
        decision = orch.account_restart(
            phase=phase, restarts_in_window=0, attempt=1
        )
        assert decision == plan_restart(
            phase=phase,
            restart="on_failure",
            restarts_in_window=0,
            intensity=intensity,
            attempt=1,
        )


# --- drain-replace (F27) ---------------------------------------------------


def test_drain_does_not_spawn_while_the_pid_is_alive(tmp_path: Path) -> None:
    """Extend, then drain, then stop. One account. The bit stays (D1, F36)."""
    view = _view(observed=ObservedWorker.RUNNING, pid_gone=False, incarnation=2)
    actions = _orch(tmp_path).drain_replace(_account(), view)
    assert {action.api_id for action in actions} == {7}
    kinds = [action.kind for action in actions]
    assert kinds.index(ActionKind.EXTEND_DEADMAN) < kinds.index(ActionKind.STOP)
    assert kinds.index(ActionKind.DRAIN) < kinds.index(ActionKind.STOP)
    assert ActionKind.SPAWN not in kinds
    assert ActionKind.PUSH_TRADING not in kinds


def test_drain_spawns_the_next_incarnation_once_the_pid_is_gone(
    tmp_path: Path,
) -> None:
    view = _view(observed=ObservedWorker.EXITED, pid_gone=True, incarnation=2)
    actions = _orch(tmp_path).drain_replace(_account(), view)
    spawns = [action for action in actions if action.kind is ActionKind.SPAWN]
    assert spawns == [
        OrchestratorAction(kind=ActionKind.SPAWN, api_id=7, incarnation=3)
    ]
    assert {action.api_id for action in actions} == {7}
    assert all(action.kind is not ActionKind.PUSH_TRADING for action in actions)
