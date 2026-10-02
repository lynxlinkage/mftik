"""STS controller: reconcile, crash class, restart, and the instance RPCs.

This is the layer §3.4 names ``mftik_sts.controller``. It replaces
``session/manager.py``. Start, end, list, and the reconcile that creates
or stops a worker are real (B4-02) and wired into the STS process.
Classifying a crash and choosing a rehang still raise
``NotImplementedError("IF-04")`` until B5-06. The code-identity pins and
the registry / env handler signatures are IF-16; those handlers raise
``NotImplementedError("IF-16")`` until B5-10. This package does not
import strategy code (F39). Artifact and event-log reads stay with
B5-11 (F40).

**State authority (§3.3).**

* Session status — phase, conditions, worker incarnation, ``restart_count``,
  failure reason — is written by this controller's Supervisor. The row is
  ``sts_sessions``. The live snapshot is ``sts.status.{session_id}``. The
  strategy does not write it (F10).
* The session spec is the API's. This layer reads it, including ``restart``.
* Whether the process exists, and the exit code and signal, are the shim's.
  This layer reads the exit record.
* MD and TD intents are not owned here. A ``restarting`` session stays on
  the liveness report so they are not reclaimed (R4). A crash does not
  delete them.
* Strategy state is the session worker's, and it is not persisted. A rehang
  starts at ``on_start`` with none (F10). There is no rebuild.

**Invariants.**

* **R1** The next incarnation is spawned only after the shim's exit record
  exists, the pid is gone, and ``td.order.cancel_session`` has confirmed.
  The two incarnations do not coexist. Procman does not spawn it: the
  worker spec's ``restart`` is ``never``, and reattach for STS does not
  spawn either. The numbered steps are on :class:`StsOrchestrator`.
* **R2** Backoff is at least one second, so the previous incarnation's last
  ``client_order_id`` and the next one's first fall in different seconds.
* **R3** Cleanup cancels resting orders and leaves positions. The new
  incarnation's recon sees the position. ``on_start`` / ``on_ready`` have
  to accept that; the SDK already says so.
* **R4** ``restarting`` stays on ``procman.report.sts.{instance}`` even
  when no process is alive, so MD/TD do not reclaim the intents (§8.2).

Crash class A/B/C and the F11 choice live here, not in procman (P6).
The defaults are 5 restarts inside 600 seconds, backoff at least 1 second.
MD and TD intensities are not defined in this package.
"""

from mftik_sts.controller._ticket import TICKET
from mftik_sts.controller.decisions import (
    backoff_s,
    classify_crash,
    decide_restart,
    reported_session_ids,
    retains_intents,
    spawn_allowed,
)
from mftik_sts.controller.defaults import (
    FIRST_INCARNATION,
    SESSION_HB_TIMEOUT_S,
    SESSION_START_TIMEOUT_S,
    SESSION_STOP_GRACE_S,
    STS_MAX_RESTARTS,
    STS_MIN_BACKOFF_S,
    STS_RESTART_WINDOW_S,
    sts_restart_intensity,
)
from mftik_sts.controller.handlers import (
    catch_up_registry,
    control_subject,
    end_handler,
    env_sync_handler,
    list_handler,
    registry_reload_handler,
    registry_sync_handler,
    start_handler,
)
from mftik_sts.controller.orchestrator import StsOrchestrator
from mftik_sts.controller.types import (
    REASON_CLEANUP_PENDING,
    REASON_CLEANUP_UNCONFIRMED,
    REASON_CRASH_CLASS_B,
    REASON_CRASH_CLASS_C,
    REASON_INIT_FAILURE,
    REASON_ON_FAILURE,
    REASON_RESTART_INTENSITY,
    REASON_RESTART_NEVER,
    REASON_WAITING_FOR_EXIT,
    SESSION_KIND,
    ActionKind,
    Cleanup,
    CrashCause,
    CrashClass,
    DesiredPhase,
    OrchestratorAction,
    ReportSlot,
    RestartDecision,
    RestartVerdict,
    SessionPhase,
    SessionSpec,
    SessionStatus,
    session_worker_id,
)
from mftik_sts.controller.worker import (
    LABEL_ENV_GENERATION,
    LABEL_STRATEGY_DIGEST,
    session_worker_spec,
)

__all__ = [
    "FIRST_INCARNATION",
    "REASON_CLEANUP_PENDING",
    "REASON_CLEANUP_UNCONFIRMED",
    "REASON_CRASH_CLASS_B",
    "REASON_CRASH_CLASS_C",
    "REASON_INIT_FAILURE",
    "REASON_ON_FAILURE",
    "REASON_RESTART_INTENSITY",
    "REASON_RESTART_NEVER",
    "REASON_WAITING_FOR_EXIT",
    "LABEL_ENV_GENERATION",
    "LABEL_STRATEGY_DIGEST",
    "SESSION_HB_TIMEOUT_S",
    "SESSION_KIND",
    "SESSION_START_TIMEOUT_S",
    "SESSION_STOP_GRACE_S",
    "STS_MAX_RESTARTS",
    "STS_MIN_BACKOFF_S",
    "STS_RESTART_WINDOW_S",
    "TICKET",
    "ActionKind",
    "Cleanup",
    "CrashCause",
    "CrashClass",
    "DesiredPhase",
    "OrchestratorAction",
    "ReportSlot",
    "RestartDecision",
    "RestartVerdict",
    "SessionPhase",
    "SessionSpec",
    "SessionStatus",
    "StsOrchestrator",
    "backoff_s",
    "catch_up_registry",
    "classify_crash",
    "control_subject",
    "decide_restart",
    "end_handler",
    "env_sync_handler",
    "list_handler",
    "registry_reload_handler",
    "registry_sync_handler",
    "reported_session_ids",
    "retains_intents",
    "session_worker_id",
    "session_worker_spec",
    "spawn_allowed",
    "start_handler",
    "sts_restart_intensity",
]
