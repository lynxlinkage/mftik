"""Spawn one OS process per STS session.

The parent stays the instance. A worker is not an instance: it does not
serve health or the instance subject, and it is started with
``start_new_session`` so a terminal SIGINT reaches only the parent.
"""

from __future__ import annotations

import asyncio
import json
import os
import signal
import sys
from dataclasses import dataclass, field
from typing import Any, Protocol

#: Why a worker that never reported a result is ``failed``. A crash after
#: that report is ``interrupted`` and may be rebuilt; this one must not be,
#: or a deploy the API already rejected comes back on its own.
START_FAIL_REASON = "worker exited during start"

#: Passed to the worker so it can tell STS from PID 1. ``tini`` is PID 1
#: in the image; this process is the worker's parent. After it dies,
#: ``getppid()`` is 1 and no longer matches.
PARENT_PID_ENV = "MFTIK_STS_PARENT_PID"

#: Write end of the result pipe. One JSON line, then the worker closes it.
RESULT_FD_ENV = "MFTIK_STS_RESULT_FD"

#: Read end of the lifeline pipe. The parent holds the write end and never
#: writes. EOF means the parent is gone. Darwin has no PDEATHSIG, and
#: ``start_new_session`` keeps a terminal signal from reaching the worker,
#: so this is how a dead parent is noticed on both platforms — including
#: the window between fork and ``prctl``.
LIFELINE_FD_ENV = "MFTIK_STS_LIFELINE_FD"

#: Write end of the beat pipe. The worker writes one byte per lease
#: interval. The parent holds the read end. A loop blocked in a sync
#: call stops writing, which is how a conditional kill tells a wedged
#: worker from one that is still running.
BEAT_FD_ENV = "MFTIK_STS_BEAT_FD"

#: How long ``close_all`` waits after SIGTERM before SIGKILL. Shorter than
#: ``ON_STOP_TIMEOUT_S`` and shorter than Docker's default grace. A strategy
#: whose ``on_stop`` uses the whole ten seconds is still cut off; the wait
#: is how long a fast detach gets, not a promise that cleanup finished.
WORKER_STOP_WAIT_S = 8.0


@dataclass
class WorkerSlot:
    """One worker in the parent's process table.

    ``started`` is set only after a success line. Until then a crash is a
    failed start. ``strategy_name`` and ``type`` are the names ``list`` and
    the event-log ``live`` bit read off an in-process session, so a slot
    can stand in for one there.
    """

    session_id: str
    role: str
    started: bool = False
    abandoned: bool = False
    strategy_name: str | None = None
    type: str | None = None
    created_by: int | None = None
    process: Any = None
    #: Set before the escalation kills. The exit watcher writes
    #: ``failed`` instead of ``interrupted`` and does not rebuild.
    stop_escalated: bool = False
    #: Create budget for this slot. Zero means the module default, which
    #: is what a rebuild slot and an old request both use.
    start_budget_s: float = 0.0
    #: When the parent read the worker's ``on_start`` mark, on this loop's
    #: clock. None means the mark never arrived.
    on_start_at: float | None = None
    #: The object ``read_result`` is filling. The deadline kill reads
    #: ``on_start_at`` off it while that read is still in progress.
    result_reader: Any = None
    #: Row reason for this kill. Unset means a stop that went unanswered.
    #: A create-timeout kill sets the start-deadline sentence so the row
    #: does not say the operator's stop timed out.
    kill_reason: str | None = None
    #: The in-flight ``escalate_stop`` result. A second force-stop awaits
    #: this instead of signalling again. A refusal (not stuck, or past
    #: the deadline) clears it, so a later stop can try again. Kept after
    #: the slot leaves ``_workers`` only for as long as the manager also
    #: holds it.
    escalation: asyncio.Future[Any] | None = None
    #: Parent's write end. Closed when the slot is dropped. The worker
    #: blocks in a read of the other end and treats EOF as parent death.
    lifeline_fd: int | None = None
    #: Parent's read end of the beat pipe, until the reader task owns it.
    beat_fd: int | None = None
    beat_task: asyncio.Task[None] | None = None
    #: ``loop.time()`` of the last beat byte, and of ``started`` becoming
    #: true. Both monotonic. A conditional kill needs the gap between them.
    last_beat: float | None = None
    started_at: float | None = None
    watcher: asyncio.Task[None] | None = None
    #: Bumped when this object is the one a settle timer is watching.
    #: Identity of the slot itself is the comparison; this exists so a
    #: replaced slot under the same id is a different object.
    _token: int = field(default=0, repr=False)


class SpawnedWorker(Protocol):
    """A worker the parent can read one result line from."""

    process: Any

    async def read_result(self) -> str | None:
        """The result line, or None on EOF before one arrives."""


class SessionSpawner(Protocol):
    async def spawn(
        self,
        *,
        session_id: str,
        role: str,
        request_json: bytes | None,
    ) -> SpawnedWorker:
        """Start a worker. ``request_json`` is the create body, or None."""


def note_on_start() -> None:
    """Tell the parent that ``on_start`` is beginning.

    One JSON line on the result pipe, which stays open for the result
    that follows. The parent times ``on_start`` from when it reads this.
    A no-op outside a worker: the fd is set only in that process.
    """
    raw = os.environ.get(RESULT_FD_ENV, "").strip()
    if not raw:
        return
    try:
        os.write(int(raw), b'{"phase":"on_start"}\n')
    except OSError:
        return


def kill_worker(process: Any) -> None:
    """SIGKILL the worker and the children it started.

    ``start_new_session`` makes the worker a session leader, so its pid
    is the process group. ``Process.kill`` signals only that pid. A
    child started in ``on_start`` would survive, be reparented to pid 1,
    and stay a zombie in a container whose main process does not reap.
    The image runs under ``tini``, which reaps what this already killed.
    """
    pid = getattr(process, "pid", None)
    if isinstance(pid, int) and pid > 0:
        try:
            # Only a session leader. ``killpg`` of some other pid signals
            # that pid's group, which may be this process.
            if os.getpgid(pid) == pid:
                os.killpg(pid, signal.SIGKILL)
        except (ProcessLookupError, PermissionError, OSError):
            pass
    try:
        process.kill()
    except ProcessLookupError:
        pass


def write_parent_beat() -> None:
    """One byte on the beat pipe. A blocked loop does not reach this.

    No-op outside a worker: the fd is set only in that process. The
    write is non-blocking so a parent that is not reading cannot stall
    the heartbeat loop.
    """
    raw = os.environ.get(BEAT_FD_ENV, "").strip()
    if not raw:
        return
    try:
        fd = int(raw)
        os.set_blocking(fd, False)
        os.write(fd, b"\n")
    except (BlockingIOError, OSError):
        return


def parse_worker_result(line: str | None) -> dict[str, Any] | None:
    """The JSON object on the result line, or None if there wasn't one.

    ``ok`` is the caller's to check. On ``ok: false`` the parent marks
    ``failed`` only when the row is still ``live``: ``start()`` has
    already written a terminal row, and validation or ``__init__`` has
    not. EOF, an empty read, and a line that is not JSON are not a
    result line; the parent marks ``failed`` there too.
    """
    if line is None:
        return None
    text = line.strip()
    if not text:
        return None
    try:
        payload = json.loads(text)
    except json.JSONDecodeError:
        return None
    if not isinstance(payload, dict):
        return None
    return payload


class _PipeWorker:
    def __init__(
        self,
        process: asyncio.subprocess.Process,
        read_fd: int,
        lifeline: int,
        beat: int,
    ) -> None:
        self.process = process
        self._read_fd = read_fd
        #: Parent's write end of the lifeline. Left open for the life of
        #: the worker. Closing it is how the worker learns the parent died.
        self.lifeline = lifeline
        #: Parent's read end of the beat pipe.
        self.beat = beat
        #: When the ``on_start`` mark was read. None until then.
        self.on_start_at: float | None = None

    async def read_result(self) -> str | None:
        # On the loop, not in a thread: a worker stuck in on_start would hold
        # a default-executor thread for as long as it stays stuck, and a
        # cancelled create could not let go of it until the pipe closed.
        # The first line may be the ``on_start`` mark. The result is the
        # line after it; the mark stays on this object so a deadline kill
        # can say how long the hook ran.
        loop = asyncio.get_running_loop()
        reader = asyncio.StreamReader()
        transport, _ = await loop.connect_read_pipe(
            lambda: asyncio.StreamReaderProtocol(reader),
            os.fdopen(self._read_fd, "rb", buffering=0),
        )
        try:
            while True:
                line = await reader.readline()
                if not line:
                    return None
                text = line.decode()
                parsed = parse_worker_result(text)
                if parsed is not None and parsed.get("phase") == "on_start":
                    self.on_start_at = loop.time()
                    continue
                return text
        finally:
            transport.close()


class SubprocessSpawner:
    """``exec`` ``python -m mftik_sts.worker`` on the parent's loop.

    stdout and stderr are inherited so Docker's log is the worker's log.
    The result is a separate pipe: a log line must not be parsed as one.
    """

    async def spawn(
        self,
        *,
        session_id: str,
        role: str,
        request_json: bytes | None,
    ) -> SpawnedWorker:
        read_fd, write_fd = os.pipe()
        life_read, life_write = os.pipe()
        beat_read, beat_write = os.pipe()
        os.set_inheritable(write_fd, True)
        os.set_inheritable(life_read, True)
        os.set_inheritable(beat_write, True)
        env = os.environ.copy()
        env[PARENT_PID_ENV] = str(os.getpid())
        env[RESULT_FD_ENV] = str(write_fd)
        env[LIFELINE_FD_ENV] = str(life_read)
        env[BEAT_FD_ENV] = str(beat_write)
        env["MFTIK_DB_POOL_SIZE"] = "1"
        try:
            process = await asyncio.create_subprocess_exec(
                sys.executable,
                "-m",
                "mftik_sts.worker",
                session_id,
                role,
                stdin=asyncio.subprocess.PIPE,
                stdout=None,
                stderr=None,
                pass_fds=(write_fd, life_read, beat_write),
                env=env,
                start_new_session=True,
            )
        except Exception:
            for fd in (read_fd, write_fd, life_read, life_write, beat_read, beat_write):
                os.close(fd)
            raise
        os.close(write_fd)
        os.close(life_read)
        os.close(beat_write)
        stdin = process.stdin
        if stdin is not None:
            try:
                if request_json:
                    stdin.write(request_json)
                    await stdin.drain()
            except (BrokenPipeError, ConnectionResetError):
                pass
            stdin.close()
        return _PipeWorker(process, read_fd, life_write, beat_read)
