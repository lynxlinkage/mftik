"""``StsOrchestrator`` — one reconcile per session (§5.1).

The running STS process does not construct this. B4-02 wires it. B5-06
fills in crash and rehang. Registry, extras, artifacts and event-log
reads are IF-16 (F40). This module does not import strategy code (F39).
"""

from __future__ import annotations

from mftik.procman import Supervisor

from mftik_sts.controller._ticket import unimplemented
from mftik_sts.controller.types import (
    OrchestratorAction,
    SessionSpec,
    SessionStatus,
)


class StsOrchestrator:
    """Session manager plus the restart decision (§5.1, §5.2).

    **State authority (§3.3).**

    * Writes session status: phase, conditions, worker incarnation,
      ``restart_count``, failure reason. The row is Postgres
      ``sts_sessions``. The live snapshot is ``sts.status.{session_id}``.
      The writer is this controller's Supervisor, from the worker's report
      and the shim's exit record (F10). The strategy does not write it and
      does not touch Postgres.
    * Reads :class:`SessionSpec`. The API is the authority for the spec,
      including the deploy's ``restart`` mode.
    * Reads the shim's exit record. The shim is the authority for whether
      the process exists and for the exit code and signal.
    * Does not own MD or TD intents. It re-puts them when it is healing
      (§8.2 rule 5). It keeps a ``restarting`` session on the liveness
      report so they are not reclaimed (R4). It does not delete them on a
      crash.
    * Does not hold strategy state. A rehang starts at ``on_start`` with
      none of it (F10). There is no rebuild.
    * Does not import strategy code, and does not serve the operator's
      host-disk paths (F39, F40, IF-16).

    **How a restart is driven through procman.**

    Procman restarts a worker on its own when the spec says ``on_failure``.
    That path does not know about crash class or about
    ``td.order.cancel_session``. R1 forbids a new incarnation until the
    old one is confirmed dead and cleanup has finished, so an STS session
    worker is never put on that path.

    1. Spawn with :func:`mftik_sts.controller.session_worker_spec`. Its
       ``restart`` is ``never``, whatever the deploy says. Procman records
       a death and does not spawn. On reattach, STS is ``mark_failed``,
       which also does not spawn.
    2. A rehang waits until the shim's exit record is present and the pid
       is gone. Until then :func:`mftik_sts.controller.spawn_allowed` is
       false, and reconcile does not name a spawn (R1). The two
       incarnations do not coexist. The first spawn has no previous
       incarnation, so this gate does not apply to it.
    3. Call ``td.order.cancel_session`` for each ``api_id`` on the spec and
       wait until every order is confirmed or the wait ends with a list of
       the ones that are not (§7.1). Positions are left as they are (R3).
    4. Classify with :func:`mftik_sts.controller.classify_crash`, from the
       worker's report. Do not call :func:`mftik.procman.classify_failure`.
    5. Count the window with :func:`mftik.procman.count_restarts_in_window`
       and :data:`~mftik_sts.controller.STS_RESTART_WINDOW_S`, then
       :func:`mftik_sts.controller.decide_restart`. The defaults are 5
       restarts, 600 seconds, backoff at least 1 second
       (:func:`~mftik_sts.controller.sts_restart_intensity`). Do not call
       :func:`mftik.procman.plan_restart`.
    6. On a rehang, wait the backoff and ``Supervisor.spawn`` incarnation
       + 1, again with ``restart="never"``. The new run starts at phase 0
       (§5.3), from ``on_start``, same ``session_id``.
    7. Keep the session id on ``procman.report.sts.{instance}`` for every
       phase :func:`mftik_sts.controller.retains_intents` accepts, including
       ``restarting`` with no pid (R4).
    """

    def __init__(self, supervisor: Supervisor) -> None:
        if supervisor.plane != "sts":
            raise ValueError(
                f"STS orchestrator requires an sts supervisor, got {supervisor.plane!r}"
            )
        self.supervisor = supervisor

    def reconcile(
        self, spec: SessionSpec, status: SessionStatus
    ) -> tuple[OrchestratorAction, ...]:
        """Compare desired phase with the worker and name the actions.

        Create, stop, or mark terminal (§5.1). A crash adds cleanup, the
        ``restarting`` write, and either a terminal ``failed`` or a delayed
        spawn. The actions come back in the order they have to run: no
        spawn before cleanup has confirmed (R1). Empty is not an answer
        this method returns today; it raises, so a caller cannot mistake
        "not implemented" for "nothing to do".

        ``spec.instance`` has to be this supervisor's instance. A session
        addressed to another STS is not reconciled here.
        """
        if not isinstance(spec, SessionSpec):
            raise TypeError("spec must be a SessionSpec")
        if not isinstance(status, SessionStatus):
            raise TypeError("status must be a SessionStatus")
        if spec.instance != self.supervisor.instance:
            raise ValueError(
                f"spec instance {spec.instance!r} does not match "
                f"orchestrator instance {self.supervisor.instance!r}"
            )
        unimplemented()
