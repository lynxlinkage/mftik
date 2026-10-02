"""Crash class and the restart decision. Both raise until B5-06.

Procman classifies a process as ``FAILED`` or ``CRASHED`` from a ready bit
and a death, a start timeout, or a heartbeat timeout
(:func:`mftik.procman.classify_failure`). It then restarts only when the
worker spec says ``on_failure`` and the intensity window still has room
(:func:`mftik.procman.plan_restart`). Neither function takes a crash class,
and neither waits for ``td.order.cancel_session``. STS does not use them
to decide. The rules below are the whole of F11 for a session.

**Class (A, B, C).** :func:`classify_crash` maps the worker's own report:

* A — :attr:`~mftik_sts.controller.CrashCause.STRATEGY_EXCEPTION`. Ingress
  queues ``on_stop`` (bounded by ``ON_STOP_TIMEOUT_S``) and the process
  then exits. Cleanup still runs, as a backstop.
* B — :attr:`~mftik_sts.controller.CrashCause.HOOK_BLOCKED` or
  :attr:`~mftik_sts.controller.CrashCause.STOP_STUCK`. The strategy loop
  cannot run ``on_stop``. The shim kills. Cleanup is the only shutdown.
* C — :attr:`~mftik_sts.controller.CrashCause.PROCESS_DEATH`. The process
  is already gone. Cleanup is the only shutdown.

**Order.** Death is confirmed before cleanup is asked, and cleanup is
asked before F11 chooses. The choice itself is first match:

1. ``restart == "never"`` (the default) → ``failed``.
2. Class B or C → ``failed``, and alert. Only an A-class crash may rehang.
   B and C alert even when the deploy said ``never``: they are failed
   anyway, and the plan says they always alert.
3. ``on_ready`` has not returned, or there is no crash class because this
   was a start timeout, a TD ready timeout, or ``on_ready`` itself over
   its wall clock → ``failed``. Init failure. No restart.
4. ``restarts_in_window >= max_restarts`` → ``failed``, and alert. The
   count is restarts already started in the window. With the default 5,
   four prior restarts still rehang and five prior restarts fail. This is
   the comparison :class:`mftik.procman.RestartIntensity` documents, which
   is how F11's "exceeds ``max_restarts``" is read. This module does not
   define a second comparison.
5. Cleanup ended unconfirmed → ``failed``, and alert. The session does
   not start again beside orders nobody has accounted for.
6. Otherwise (``on_failure``, class A, past ``on_ready``, inside the
   window, cleanup confirmed) → rehang from ``on_start``, incarnation + 1,
   after a backoff of at least one second.

``reason`` on a failure or a rehang is rule 1–6, first match. Waiting
for the exit record and not having asked cleanup yet are earlier, and
they use their own reasons. ``alert`` is independent of the reason:
it is true when rule 2, 4, or 5 also matches, even if an earlier rule
already chose ``failed``. A class-B crash on a ``never`` deploy is
``reason="restart_never"`` and ``alert=True``.

``error_log`` is §5.2 step 1: once the choice is reached, the supervisor
writes ``restarting`` and publishes one error-level line on
``log.sts.{session_id}``. The existing alert pipeline matches that line.
Waiting on the exit record, and not having asked cleanup yet, do not
publish it.

The backoff curve has no fixed ratio here, same as procman. It is at
least :data:`~mftik_sts.controller.STS_MIN_BACKOFF_S` and it is strictly
increasing in ``attempt`` (attempt starts at 1).
"""

from __future__ import annotations

from collections.abc import Sequence

from mftik.procman import RESTART_MODES

from mftik_sts.controller._ticket import unimplemented
from mftik_sts.controller.defaults import STS_MAX_RESTARTS
from mftik_sts.controller.types import (
    Cleanup,
    CrashCause,
    CrashClass,
    ReportSlot,
    RestartDecision,
    SessionPhase,
)


def classify_crash(cause: CrashCause) -> CrashClass:
    """A, B or C from the worker's report. Not from procman.

    ``STRATEGY_EXCEPTION`` is A. ``HOOK_BLOCKED`` and ``STOP_STUCK`` are B.
    ``PROCESS_DEATH`` is C. The ready flag is not an input: a class-A crash
    before ``on_ready`` is still class A, and :func:`decide_restart` is what
    refuses to rehang it.
    """
    CrashCause(cause)
    unimplemented()


def decide_restart(
    *,
    restart: str,
    crash_class: CrashClass | None,
    ready: bool,
    cleanup: Cleanup,
    exit_recorded: bool,
    pid_gone: bool,
    restarts_in_window: int,
    incarnation: int,
    attempt: int,
    max_restarts: int = STS_MAX_RESTARTS,
) -> RestartDecision:
    """Fail, rehang, or not yet. See the module docstring for the order.

    Call this only for a crash or an init failure. ``ready`` with no crash
    class is neither: that is a session that came up, and ending it is
    :func:`mftik_sts.controller.end_handler`, not a restart decision.

    ``restarts_in_window`` is the caller's count. Produce it with
    :func:`mftik.procman.count_restarts_in_window` and the STS window
    (:data:`~mftik_sts.controller.STS_RESTART_WINDOW_S`), not with
    :func:`mftik.procman.plan_restart`.

    A rehang's ``delay_s`` is :func:`backoff_s` of ``attempt``, so it is at
    least :data:`~mftik_sts.controller.STS_MIN_BACKOFF_S` (R2).
    ``next_incarnation`` is ``incarnation + 1``. ``cancels_positions`` is
    false (R3): the new incarnation's recon sees the position and does not
    see the resting orders, because cleanup has confirmed.
    """
    if restart not in RESTART_MODES:
        raise ValueError(
            f"restart {restart!r} is not one of {', '.join(RESTART_MODES)}"
        )
    if crash_class is not None:
        CrashClass(crash_class)
    Cleanup(cleanup)
    for name, value in (
        ("ready", ready),
        ("exit_recorded", exit_recorded),
        ("pid_gone", pid_gone),
    ):
        if not isinstance(value, bool):
            raise ValueError(f"{name} must be a bool")
    if type(restarts_in_window) is not int or restarts_in_window < 0:
        raise ValueError("restarts_in_window must be an int >= 0")
    if type(incarnation) is not int or incarnation < 0:
        raise ValueError("incarnation must be an int >= 0")
    if type(attempt) is not int or attempt < 1:
        raise ValueError("attempt starts at 1")
    if type(max_restarts) is not int or max_restarts < 0:
        raise ValueError("max_restarts must be an int >= 0")
    if crash_class is None and ready:
        raise ValueError(
            "decide_restart is for a crash or an init failure; "
            "ready with no crash class is neither"
        )
    unimplemented()


def backoff_s(attempt: int) -> float:
    """Seconds to wait before incarnation ``attempt``.

    At least :data:`~mftik_sts.controller.STS_MIN_BACKOFF_S`, and strictly
    increasing in ``attempt``. ``attempt`` starts at 1. The ratio is not
    fixed. R2: the previous incarnation's last order and this one's first
    fall in different seconds, so a seq that restarts at 0 does not collide.
    """
    if type(attempt) is not int or attempt < 1:
        raise ValueError("attempt starts at 1")
    unimplemented()


def spawn_allowed(
    *,
    exit_recorded: bool,
    pid_gone: bool,
    cleanup: Cleanup,
) -> bool:
    """Whether R1 allows the next incarnation to be spawned.

    True only when the shim has written the exit record, the pid is gone,
    and cleanup is :attr:`~mftik_sts.controller.Cleanup.CONFIRMED`. The two
    incarnations do not coexist, and the new one does not start beside
    unconfirmed orders. The first spawn has no previous incarnation, so
    reconcile does not consult this gate for it.

    This is the gate in front of :meth:`mftik.procman.Supervisor.spawn`.
    It is not the F11 choice: a ``never`` deploy can pass this gate and
    still be failed by :func:`decide_restart`.
    """
    for name, value in (("exit_recorded", exit_recorded), ("pid_gone", pid_gone)):
        if not isinstance(value, bool):
            raise ValueError(f"{name} must be a bool")
    Cleanup(cleanup)
    unimplemented()


def retains_intents(phase: SessionPhase) -> bool:
    """Whether this phase stays on the liveness report (R4, §8.2 rule 3).

    MD and TD drop an owner's intents after the owner is missing from two
    consecutive ``procman.report.sts`` publications. The report lists
    sessions whose desired state is running, including ``restarting``, not
    only sessions whose process is alive. So:

    * ``pending``, ``starting``, ``running``, ``restarting`` retain.
    * ``done`` and ``failed`` do not. A failed session leaves the report
      and the backstop reclaims it. End's own ``intent.delete`` is the
      API's path (§8.1); this layer does not delete intents on a crash.
    * ``stopping`` retains. B3-04 DECIDED 2 (Yi Te, #298):
      :meth:`mftik.procman.Supervisor.report` lists ``STOPPING`` slots, so
      a session whose worker is still in ``on_stop`` stays on
      ``procman.report.sts.{instance}`` until the process exits. §8.1's
      ``intent.delete`` after ``on_stop`` is what releases the intents.

    The body still raises. B5-06 fills it in. Publishing the report does
    not filter phases, so a stopping worker stays listed either way.
    """
    SessionPhase(phase)
    unimplemented()


def reported_session_ids(slots: Sequence[ReportSlot]) -> frozenset[str]:
    """Session ids the STS report must carry so intent GC leaves them.

    Membership is :func:`retains_intents`. A ``restarting`` slot is
    included when its pid is ``None``: the process is gone and the intents
    stay (R4). ``done`` and ``failed`` are absent. The supervisor's own
    worker list is not this set. A session between incarnations may have
    no worker, and the orchestrator is what puts it on the report anyway.
    """
    if isinstance(slots, str) or not isinstance(slots, Sequence):
        raise ValueError("slots must be a sequence of ReportSlot")
    for slot in slots:
        if not isinstance(slot, ReportSlot):
            raise ValueError("slots must be a sequence of ReportSlot")
    unimplemented()
