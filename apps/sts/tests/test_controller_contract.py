"""What B4-02 and B5-06 have to make true of the STS controller.

Start, end, list, and the reconcile that creates or stops a worker are
B4-02. Crash class, F11, and R1–R4 are B5-06. R4's report membership is
what keeps MD/TD from reclaiming intents during ``restarting``; the
reclaim itself is B4-07.
"""

from __future__ import annotations

from pathlib import Path

import pytest
from mftik.procman import Supervisor
from mftik.protocol import (
    STS_REASON_OPERATOR_STOP,
    STS_SESSION_END,
    STS_SESSION_LIST,
    STS_SESSION_START,
    Envelope,
    ListSessionsRequest,
    ListSessionsResult,
    StsCreateSessionRequest,
    StsCreateSessionResult,
    StsSessionEndRequest,
    StsSessionEndResult,
)
from mftik_sts.controller import (
    FIRST_INCARNATION,
    REASON_CLEANUP_PENDING,
    REASON_CLEANUP_UNCONFIRMED,
    REASON_CRASH_CLASS_B,
    REASON_CRASH_CLASS_C,
    REASON_INIT_FAILURE,
    REASON_ON_FAILURE,
    REASON_RESTART_INTENSITY,
    REASON_RESTART_NEVER,
    REASON_WAITING_FOR_EXIT,
    STS_MIN_BACKOFF_S,
    ActionKind,
    Cleanup,
    CrashCause,
    CrashClass,
    DesiredPhase,
    ReportSlot,
    RestartVerdict,
    SessionPhase,
    SessionSpec,
    SessionStatus,
    StsOrchestrator,
    backoff_s,
    classify_crash,
    decide_restart,
    end_handler,
    list_handler,
    reported_session_ids,
    retains_intents,
    spawn_allowed,
    start_handler,
)


def _orch(tmp_path: Path) -> StsOrchestrator:
    return StsOrchestrator(Supervisor(tmp_path, plane="sts", instance="sts"))


def _spec(**overrides: object) -> SessionSpec:
    raw: dict[str, object] = {
        "session_id": "abc123",
        "instance": "sts",
        "strategy": "noop",
    }
    raw.update(overrides)
    return SessionSpec(**raw)  # type: ignore[arg-type]


def _rehang(**overrides: object):
    """An A-class crash that F11 would hang up again, unless overridden."""
    raw: dict[str, object] = {
        "restart": "on_failure",
        "crash_class": CrashClass.A,
        "ready": True,
        "cleanup": Cleanup.CONFIRMED,
        "exit_recorded": True,
        "pid_gone": True,
        "restarts_in_window": 0,
        "incarnation": 2,
        "attempt": 1,
    }
    raw.update(overrides)
    return decide_restart(**raw)  # type: ignore[arg-type]


def _message(payload: object, type_: str) -> Envelope[dict]:
    body = payload.model_dump() if hasattr(payload, "model_dump") else payload
    return Envelope[dict].wrap(body, type=type_, source="api")  # type: ignore[arg-type]


def _model(reply_payload: object, model: type):
    if hasattr(reply_payload, "model_dump"):
        reply_payload = reply_payload.model_dump()
    return model.model_validate(reply_payload)


# --- crash class -----------------------------------------------------------


@pytest.mark.parametrize(
    ("cause", "crash_class"),
    [
        (CrashCause.STRATEGY_EXCEPTION, CrashClass.A),
        (CrashCause.HOOK_BLOCKED, CrashClass.B),
        (CrashCause.STOP_STUCK, CrashClass.B),
        (CrashCause.PROCESS_DEATH, CrashClass.C),
    ],
)
def test_crash_cause_maps_to_class_a_b_or_c(
    cause: CrashCause, crash_class: CrashClass
) -> None:
    """A is a strategy exception. B is a stuck loop. C is the process gone.
    Procman's FAILED/CRASHED split is not this (P6)."""
    assert classify_crash(cause) is crash_class


# --- F11 -------------------------------------------------------------------


def test_f11_restart_never_fails() -> None:
    """The default. An A-class crash after ``on_ready`` is still not hung
    up again."""
    decision = _rehang(restart="never")
    assert decision.verdict is RestartVerdict.FAILED
    assert decision.reason == REASON_RESTART_NEVER
    assert decision.alert is False
    assert decision.error_log is True
    assert decision.delay_s is None
    assert decision.next_incarnation is None


@pytest.mark.parametrize(
    ("crash_class", "reason"),
    [
        (CrashClass.B, REASON_CRASH_CLASS_B),
        (CrashClass.C, REASON_CRASH_CLASS_C),
    ],
)
def test_f11_only_class_a_may_rehang(crash_class: CrashClass, reason: str) -> None:
    """B and C are failed and alerted after cleanup, even on ``on_failure``."""
    decision = _rehang(crash_class=crash_class)
    assert decision.verdict is RestartVerdict.FAILED
    assert decision.reason == reason
    assert decision.alert is True
    assert decision.next_incarnation is None


@pytest.mark.parametrize("crash_class", [CrashClass.B, CrashClass.C])
def test_f11_class_b_and_c_alert_even_when_restart_is_never(
    crash_class: CrashClass,
) -> None:
    """``never`` is the first fail rule, so it supplies the reason. B and C
    still alert: the plan says they always do."""
    decision = _rehang(restart="never", crash_class=crash_class)
    assert decision.verdict is RestartVerdict.FAILED
    assert decision.reason == REASON_RESTART_NEVER
    assert decision.alert is True


def test_f11_crash_before_on_ready_is_not_restarted() -> None:
    """Still class A. Init failure is the restart rule, not the class."""
    assert classify_crash(CrashCause.STRATEGY_EXCEPTION) is CrashClass.A
    decision = _rehang(ready=False)
    assert decision.verdict is RestartVerdict.FAILED
    assert decision.reason == REASON_INIT_FAILURE
    assert decision.alert is False
    assert decision.next_incarnation is None


def test_f11_init_timeout_is_not_restarted() -> None:
    """``start_timeout_s``, TD missing ``ready_timeout_s``, and ``on_ready``
    over its wall clock are init failures. They are not class B."""
    decision = _rehang(crash_class=None, ready=False)
    assert decision.verdict is RestartVerdict.FAILED
    assert decision.reason == REASON_INIT_FAILURE
    assert decision.alert is False
    assert decision.next_incarnation is None


def test_f11_class_b_before_ready_is_still_class_b() -> None:
    """The class rule is listed before the init-failure rule."""
    decision = _rehang(crash_class=CrashClass.B, ready=False)
    assert decision.verdict is RestartVerdict.FAILED
    assert decision.reason == REASON_CRASH_CLASS_B
    assert decision.alert is True


def test_f11_four_restarts_in_the_window_still_rehang() -> None:
    """``max_restarts=5`` allows a fifth. Four already started, so this one
    still rehanges. ``attempt`` does not decide the cap."""
    decision = _rehang(restarts_in_window=4, attempt=5)
    assert decision.verdict is RestartVerdict.REHANG
    assert decision.reason == REASON_ON_FAILURE
    assert decision.alert is False
    assert decision.error_log is True
    assert decision.next_incarnation == 3
    assert decision.delay_s == backoff_s(5)
    assert decision.delay_s is not None
    assert decision.delay_s >= STS_MIN_BACKOFF_S
    assert decision.cancels_positions is False


def test_f11_the_restart_past_max_restarts_fails_and_alerts() -> None:
    """Five already started fills the default window. The next one fails."""
    decision = _rehang(restarts_in_window=5, attempt=1)
    assert decision.verdict is RestartVerdict.FAILED
    assert decision.reason == REASON_RESTART_INTENSITY
    assert decision.alert is True
    assert decision.delay_s is None
    assert decision.next_incarnation is None


def test_f11_unconfirmed_cleanup_fails_and_alerts() -> None:
    decision = _rehang(cleanup=Cleanup.UNCONFIRMED)
    assert decision.verdict is RestartVerdict.FAILED
    assert decision.reason == REASON_CLEANUP_UNCONFIRMED
    assert decision.alert is True
    assert decision.next_incarnation is None


def test_f11_unconfirmed_cleanup_alerts_even_when_restart_is_never() -> None:
    decision = _rehang(restart="never", cleanup=Cleanup.UNCONFIRMED)
    assert decision.verdict is RestartVerdict.FAILED
    assert decision.reason == REASON_RESTART_NEVER
    assert decision.alert is True


def test_f11_intensity_alerts_even_when_restart_is_never() -> None:
    decision = _rehang(restart="never", restarts_in_window=5)
    assert decision.verdict is RestartVerdict.FAILED
    assert decision.reason == REASON_RESTART_NEVER
    assert decision.alert is True


def test_f11_class_a_on_failure_rehanges_from_on_start() -> None:
    decision = _rehang()
    assert decision.verdict is RestartVerdict.REHANG
    assert decision.reason == REASON_ON_FAILURE
    assert decision.next_incarnation == 3
    assert decision.delay_s is not None
    assert decision.delay_s >= STS_MIN_BACKOFF_S
    assert decision.cancels_positions is False
    assert decision.alert is False


# --- R1 --------------------------------------------------------------------


def test_r1_waits_until_the_exit_record_exists() -> None:
    decision = _rehang(exit_recorded=False)
    assert decision.verdict is RestartVerdict.WAIT
    assert decision.reason == REASON_WAITING_FOR_EXIT
    assert decision.alert is False
    assert decision.error_log is False
    assert decision.next_incarnation is None
    assert decision.delay_s is None


def test_r1_waits_while_the_old_pid_is_alive() -> None:
    decision = _rehang(pid_gone=False)
    assert decision.verdict is RestartVerdict.WAIT
    assert decision.next_incarnation is None


def test_r1_asks_cleanup_before_any_restart_choice() -> None:
    """Cleanup runs before F11, including for a B-class crash and for
    ``restart: never``. The choice waits until cancel has been asked."""
    for overrides in (
        {},
        {"crash_class": CrashClass.B},
        {"restart": "never"},
    ):
        decision = _rehang(cleanup=Cleanup.NOT_RUN, **overrides)
        assert decision.verdict is RestartVerdict.CLEANUP
        assert decision.reason == REASON_CLEANUP_PENDING
        assert decision.alert is False
        assert decision.error_log is False
        assert decision.next_incarnation is None


def test_r1_spawn_allowed_only_after_exit_and_confirmed_cleanup() -> None:
    assert (
        spawn_allowed(
            exit_recorded=True, pid_gone=True, cleanup=Cleanup.CONFIRMED
        )
        is True
    )
    assert (
        spawn_allowed(
            exit_recorded=False, pid_gone=True, cleanup=Cleanup.CONFIRMED
        )
        is False
    )
    assert (
        spawn_allowed(
            exit_recorded=True, pid_gone=False, cleanup=Cleanup.CONFIRMED
        )
        is False
    )
    assert (
        spawn_allowed(
            exit_recorded=True, pid_gone=True, cleanup=Cleanup.NOT_RUN
        )
        is False
    )
    assert (
        spawn_allowed(
            exit_recorded=True, pid_gone=True, cleanup=Cleanup.UNCONFIRMED
        )
        is False
    )


def test_r1_reconcile_does_not_spawn_before_cleanup_confirms(tmp_path: Path) -> None:
    orch = _orch(tmp_path)
    status = SessionStatus(
        phase=SessionPhase.RUNNING,
        worker_incarnation=1,
        ready=True,
        exit_recorded=True,
        pid_gone=True,
        crash_class=CrashClass.A,
        cleanup=Cleanup.NOT_RUN,
    )
    actions = orch.reconcile(_spec(restart="on_failure"), status)
    assert any(action.kind is ActionKind.CLEANUP for action in actions)
    assert all(action.kind is not ActionKind.SPAWN for action in actions)


# --- R2 --------------------------------------------------------------------


def test_r2_backoff_is_at_least_one_second_and_grows() -> None:
    """One second keeps the previous incarnation's last client_order_id and
    the next incarnation's first in different seconds. Seq restarts at 0."""
    delays = [backoff_s(attempt) for attempt in range(1, 5)]
    assert delays[0] >= STS_MIN_BACKOFF_S
    assert delays == sorted(delays)
    assert delays[-1] > delays[0]
    decision = _rehang(attempt=3)
    assert decision.delay_s == delays[2]


# --- R3 --------------------------------------------------------------------


def test_r3_rehang_keeps_positions() -> None:
    """Resting orders are gone because cleanup confirmed. The position is
    not cancelled, and the rehang does not grow a flatten step."""
    decision = _rehang()
    assert decision.verdict is RestartVerdict.REHANG
    assert decision.cancels_positions is False


# --- R4 --------------------------------------------------------------------


@pytest.mark.parametrize(
    "phase",
    [
        SessionPhase.PENDING,
        SessionPhase.STARTING,
        SessionPhase.RUNNING,
        SessionPhase.RESTARTING,
    ],
)
def test_r4_desired_running_phases_retain_intents(phase: SessionPhase) -> None:
    assert retains_intents(phase) is True


@pytest.mark.parametrize("phase", [SessionPhase.DONE, SessionPhase.FAILED])
def test_r4_terminal_phases_do_not_retain_intents(phase: SessionPhase) -> None:
    assert retains_intents(phase) is False


def test_r4_restarting_retains_intents_with_no_pid() -> None:
    """The report lists desired-running sessions, not live pids. A session
    between incarnations has no process and still owns its intents."""
    ids = reported_session_ids(
        (
            ReportSlot("aa0001", SessionPhase.RUNNING, pid=10),
            ReportSlot("aa0002", SessionPhase.RESTARTING, pid=None),
            ReportSlot("aa0003", SessionPhase.FAILED, pid=None),
            ReportSlot("aa0004", SessionPhase.DONE, pid=None),
        )
    )
    assert ids == {"aa0001", "aa0002"}


# --- reconcile: create / stop (B4-02) --------------------------------------


def test_reconcile_spawns_when_nothing_is_running(tmp_path: Path) -> None:
    """The first spawn is not a rehang. There is no exit record to wait for."""
    actions = _orch(tmp_path).reconcile(_spec(), SessionStatus())
    spawns = [action for action in actions if action.kind is ActionKind.SPAWN]
    assert len(spawns) == 1
    assert spawns[0].session_id == "abc123"
    assert spawns[0].incarnation == FIRST_INCARNATION


def test_reconcile_is_idle_while_desired_matches_the_worker(tmp_path: Path) -> None:
    status = SessionStatus(
        phase=SessionPhase.RUNNING,
        worker_incarnation=1,
        ready=True,
        pid=5,
        pid_gone=False,
    )
    assert _orch(tmp_path).reconcile(_spec(), status) == ()


def test_reconcile_stops_when_desired_is_stopped(tmp_path: Path) -> None:
    status = SessionStatus(
        phase=SessionPhase.RUNNING,
        worker_incarnation=1,
        ready=True,
        pid=5,
        pid_gone=False,
    )
    actions = _orch(tmp_path).reconcile(
        _spec(desired=DesiredPhase.STOPPED), status
    )
    assert any(action.kind is ActionKind.STOP for action in actions)
    assert all(action.kind is not ActionKind.SPAWN for action in actions)


def test_reconcile_marks_done_when_the_stop_has_exited(tmp_path: Path) -> None:
    status = SessionStatus(
        phase=SessionPhase.STOPPING,
        worker_incarnation=1,
        exit_recorded=True,
        pid_gone=True,
    )
    actions = _orch(tmp_path).reconcile(
        _spec(desired=DesiredPhase.STOPPED), status
    )
    terminals = [
        action for action in actions if action.kind is ActionKind.MARK_TERMINAL
    ]
    assert len(terminals) == 1
    assert terminals[0].phase is SessionPhase.DONE


def test_reconcile_marks_restarting_before_the_next_spawn(tmp_path: Path) -> None:
    """Step 1 writes ``restarting`` and an error log, then the rehang spawns.
    The spawn carries the backoff. No strategy state rides along."""
    status = SessionStatus(
        phase=SessionPhase.RUNNING,
        worker_incarnation=2,
        ready=True,
        exit_recorded=True,
        pid_gone=True,
        cleanup=Cleanup.CONFIRMED,
        crash_class=CrashClass.A,
        restarts_in_window=0,
    )
    actions = _orch(tmp_path).reconcile(_spec(restart="on_failure"), status)
    kinds = [action.kind for action in actions]
    assert ActionKind.MARK_RESTARTING in kinds
    assert ActionKind.ALERT in kinds
    assert ActionKind.SPAWN in kinds
    assert kinds.index(ActionKind.MARK_RESTARTING) < kinds.index(ActionKind.SPAWN)
    spawn = next(action for action in actions if action.kind is ActionKind.SPAWN)
    assert spawn.incarnation == 3
    assert spawn.delay_s is not None
    assert spawn.delay_s >= STS_MIN_BACKOFF_S


# --- start / end / list ----------------------------------------------------


async def test_start_accepts_with_status_starting(tmp_path: Path) -> None:
    """F12: the reply is an accept. ``on_start`` has not run."""
    request = StsCreateSessionRequest(
        session_id="abc123", created_by=1, strategy="noop"
    )
    reply = await start_handler(_orch(tmp_path))(
        _message(request, STS_SESSION_START)
    )
    assert reply is not None
    result = _model(reply.payload, StsCreateSessionResult)
    assert result.session_id == "abc123"
    assert result.status == "starting"


async def test_end_of_a_started_session_is_terminal(tmp_path: Path) -> None:
    """End waits until the session is terminal. The controller does not run
    ``on_stop`` itself; the reply is still the terminal status (§8.1)."""
    orch = _orch(tmp_path)
    request = StsCreateSessionRequest(
        session_id="abc123", created_by=1, strategy="noop"
    )
    await start_handler(orch)(_message(request, STS_SESSION_START))
    end = StsSessionEndRequest(
        session_id="abc123", reason=STS_REASON_OPERATOR_STOP
    )
    reply = await end_handler(orch)(_message(end, STS_SESSION_END))
    assert reply is not None
    result = _model(reply.payload, StsSessionEndResult)
    assert result.session_id == "abc123"
    assert result.status in {"done", "failed"}


async def test_list_includes_a_started_session(tmp_path: Path) -> None:
    orch = _orch(tmp_path)
    request = StsCreateSessionRequest(
        session_id="abc123", created_by=1, strategy="noop"
    )
    await start_handler(orch)(_message(request, STS_SESSION_START))
    reply = await list_handler(orch)(
        _message(ListSessionsRequest(domain="sts"), STS_SESSION_LIST)
    )
    assert reply is not None
    result = _model(reply.payload, ListSessionsResult)
    assert any(row.session_id == "abc123" for row in result.sessions)
