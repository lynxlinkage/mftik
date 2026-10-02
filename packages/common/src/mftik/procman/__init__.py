"""Process supervision: the spec, the shim protocol, and the state machine.

One library, embedded in each plane's controller (§4.1). The planes differ
by :class:`WorkerSpec`. The restart policy is the orchestrator's: it calls
:func:`plan_restart` with its own intensity, waits, and calls
:meth:`Supervisor.spawn` for the next incarnation. The supervisor classifies
the failure and holds the slot. It does not restart on its own and it does
not take a restart intensity (§4.3). The shim is the worker's parent. The
supervisor talks to it over a unix-socket NDJSON protocol and is not that
parent (F6).

**State authority (§3.3).**

* The shim is the authority for whether its worker process exists and for
  the exit code and signal. It writes ``<id>.exit.json`` after it reaps the
  worker and answers ``status`` on ``${WORK_DIR}/run/<id>.sock``. The
  supervisor reads both.
* The supervisor is the authority for the set of workers it still holds on
  this instance. It publishes that set on
  ``procman.report.{plane}.{instance}`` and does not persist the report.
  While publication is stopped, the absence is not an observation:
  consumers reclaim nothing (F32, P7).
* ``code_ref`` is the release version of the controller that spawned the
  worker (§4.5).

**Invariants.**

* **S1–S7.** The shim is the worker's only parent and, on Linux, a child
  subreaper, so it also reaps the worker's descendants. The shim itself is
  adopted by host init. When the shim disappears the worker stops
  gracefully, by ``EPIPE`` on the status pipe or by ``PDEATHSIG=SIGTERM``,
  whichever arrives first. The exit record is written (temp file, then
  rename) before the shim waits for ``release``, so a controller that is
  offline does not lose the exit code or the signal. The shim holds the
  worker's stdio and the status pipe and rotates the logs, so a missing
  supervisor cannot stall the worker on ``SIGPIPE`` or a full buffer. The
  socket speaks ``status``, ``signal`` (``killpg`` on the worker's process
  group), ``watch`` and ``release``, and the supervisor finds the worker
  by that path rather than by pid. A status-pipe message is at most
  ``PIPE_BUF`` (4096) bytes, one full snapshot; a write that would block
  because the pipe is full is dropped, and the next heartbeat carries the
  whole snapshot again. ``SIGTERM`` delivered to the shim is forwarded to
  the worker, and the shim stays up until ``release``. The shim does not
  restart workers and does not know which plane it serves (S7, P6).
* **State machine.** See :data:`TRANSITIONS`. Death or start timeout before
  ready is ``FAILED`` and is not restarted. Death or heartbeat timeout
  after ready is ``CRASHED``. ``restart="on_failure"`` then takes
  ``BACKOFF`` unless the intensity window is already full, in which case
  the phase is ``FATAL``. ``restart="never"`` leaves the worker on
  ``CRASHED``. A shim that disappears while the worker was alive is
  ``LOST``.
* **Reattach (§4.4).** ``close(detach)`` signals nothing. ``close(stop)``
  stops every worker; it is the only close that does. On start, a desired
  worker that is running is adopted. A desired STS worker that is gone,
  exited or ``LOST`` is recorded from the exit file and is not spawned. A
  desired MD or TD worker in that state follows the restart policy. A
  running worker that is no longer desired is stopped and then released.
* **Spawn.** The intermediate process is ``subprocess.Popen``. It is not
  ``asyncio.create_subprocess_exec`` (§4.1): that transport kills the child
  when it is closed or collected. The shim applies ``oom_score_adj`` and
  the optional ``RLIMIT_DATA`` (§4.7, F7). Admission control is B3-05.

Framing, path names, the transition table and :class:`WorkerSpec`
validation are real. The shim — spawn, the socket, the exit record — is
real as of B3-01. Restart decisions and the live state machine (spawn,
stop, status, heartbeat timeout) are real as of B3-02. Reattach and
publishing a report still raise ``NotImplementedError("IF-03")``.
"""

from mftik.procman._ticket import TICKET
from mftik.procman.decisions import (
    BACKOFF_RATIO,
    DesiredSlot,
    FailureCause,
    ObservedWorker,
    ReattachAction,
    RestartDecision,
    RestartIntensity,
    classify_failure,
    count_restarts_in_window,
    observe_heartbeat,
    plan_restart,
    reattach_action,
)
from mftik.procman.errors import (
    InvalidTransition,
    InvalidWorkerId,
    InvalidWorkerSpec,
    MessageError,
    ProcmanError,
)
from mftik.procman.messages import (
    PIPE_BUF,
    STATUS_FD_ENV,
    ExitRecord,
    ReleaseCommand,
    ShimCommand,
    ShimStatus,
    SignalCommand,
    StatusQuery,
    WatchCommand,
    WorkerHeartbeat,
    decode_command,
    decode_exit,
    decode_heartbeat,
    decode_status,
    dump_frame,
    encode_command,
    encode_exit,
    encode_heartbeat,
    encode_status,
    exit_record_path,
    exit_record_tmp_path,
    load_frame,
    log_path,
    run_dir,
    socket_path,
    supervisor_state_path,
)
from mftik.procman.report import (
    ProcmanReport,
    ReportedWorker,
    decode_report,
    encode_report,
    report_subject,
)
from mftik.procman.shim import ShimClient, SpawnedShim, spawn_shim
from mftik.procman.shim import main as shim_main
from mftik.procman.spec import (
    CONTROLLER_OOM_SCORE_ADJ,
    OOM_SCORE_ADJ,
    PLANES,
    RESTART_MODES,
    SHIM_OOM_SCORE_ADJ,
    Plane,
    RestartMode,
    WorkerSpec,
    validate_worker_id,
)
from mftik.procman.state import (
    ALIVE_PHASES,
    TRANSITIONS,
    Trigger,
    WorkerPhase,
    transition,
)
from mftik.procman.supervisor import CloseMode, Supervisor, WorkerStatus

__all__ = [
    "ALIVE_PHASES",
    "BACKOFF_RATIO",
    "CONTROLLER_OOM_SCORE_ADJ",
    "OOM_SCORE_ADJ",
    "PIPE_BUF",
    "PLANES",
    "RESTART_MODES",
    "SHIM_OOM_SCORE_ADJ",
    "STATUS_FD_ENV",
    "TICKET",
    "TRANSITIONS",
    "CloseMode",
    "DesiredSlot",
    "ExitRecord",
    "FailureCause",
    "InvalidTransition",
    "InvalidWorkerId",
    "InvalidWorkerSpec",
    "MessageError",
    "ObservedWorker",
    "Plane",
    "ProcmanError",
    "ProcmanReport",
    "ReattachAction",
    "ReleaseCommand",
    "ReportedWorker",
    "RestartDecision",
    "RestartIntensity",
    "RestartMode",
    "ShimClient",
    "ShimCommand",
    "ShimStatus",
    "SignalCommand",
    "SpawnedShim",
    "StatusQuery",
    "Supervisor",
    "Trigger",
    "WatchCommand",
    "WorkerHeartbeat",
    "WorkerPhase",
    "WorkerSpec",
    "WorkerStatus",
    "classify_failure",
    "count_restarts_in_window",
    "decode_command",
    "decode_exit",
    "decode_heartbeat",
    "decode_report",
    "decode_status",
    "dump_frame",
    "encode_command",
    "encode_exit",
    "encode_heartbeat",
    "encode_report",
    "encode_status",
    "exit_record_path",
    "exit_record_tmp_path",
    "load_frame",
    "log_path",
    "observe_heartbeat",
    "plan_restart",
    "reattach_action",
    "report_subject",
    "run_dir",
    "shim_main",
    "socket_path",
    "spawn_shim",
    "supervisor_state_path",
    "transition",
    "validate_worker_id",
]
