"""One STS session, in its own process.

The parent is the instance. This process is not: it does not take the
health subject and it does not answer the instance subject. It serves
stop and fail on the session's control subject, and it exits only after
that session is gone, its control loop has finished, and the broker has
closed. A stop reply is sent from inside the control task; leaving
before that reply is flushed drops it.
"""

from __future__ import annotations

import asyncio
import ctypes
import json
import logging
import os
import signal
import sys
from typing import Any

import uvloop
from mftik import configure_logging, instance_name
from mftik.broker import Broker
from mftik.protocol import (
    STS_SESSION_FAIL,
    STS_SESSION_STOP,
    StsCreateSessionRequest,
    StsCreateSessionResult,
)

from mftik_sts import db as sts_db
from mftik_sts.runtime_env import refresh
from mftik_sts.session import SessionManager
from mftik_sts.spawn import PARENT_PID_ENV, RESULT_FD_ENV

logger = logging.getLogger(__name__)

#: ``prctl`` option. Linux only; the call is skipped everywhere else.
_PR_SET_PDEATHSIG = 1


def set_pdeathsig(signum: int) -> None:
    """Ask Linux to signal this process when its parent dies.

    A no-op on every other platform. The signal is not a substitute for
    the parent-pid check: if the parent is already gone, there is nobody
    for the kernel to deliver it to. In a container the parent is PID 1,
    and PID 1 dying SIGKILLs the whole namespace, so this is not delivered
    there either — the same as STS itself dying today.
    """
    if sys.platform != "linux":
        return
    libc = ctypes.CDLL(None, use_errno=True)
    libc.prctl.argtypes = [
        ctypes.c_int,
        ctypes.c_ulong,
        ctypes.c_ulong,
        ctypes.c_ulong,
        ctypes.c_ulong,
    ]
    libc.prctl.restype = ctypes.c_int
    if libc.prctl(_PR_SET_PDEATHSIG, signum, 0, 0, 0) != 0:
        err = ctypes.get_errno()
        raise OSError(err, os.strerror(err))


def arm_parent_death() -> None:
    """Exit unless this process was spawned by the pid in the environment.

    Compared to that pid, never to 1. Under compose the parent *is* PID 1,
    and ``getppid() == 1`` is the ordinary case. Treating it as "orphaned"
    would exit every worker at start.
    """
    raw = os.environ.get(PARENT_PID_ENV, "").strip()
    if not raw:
        raise SystemExit(1)
    try:
        expected = int(raw)
    except ValueError:
        raise SystemExit(1)
    # Armed first. A parent that dies between the two calls still
    # delivers the signal; a parent that died before the first is caught
    # by the pid check, because the signal was never going to arrive.
    set_pdeathsig(signal.SIGTERM)
    if os.getppid() != expected:
        raise SystemExit(1)


def write_result(payload: dict[str, Any]) -> None:
    """One JSON line on the result pipe, then close it.

    The parent is blocked in a read of that pipe. A log line on stdout
    must not be the thing it parses, so this is a separate fd.
    """
    raw = os.environ.get(RESULT_FD_ENV, "").strip()
    if not raw:
        raise RuntimeError("worker result fd is not set")
    fd = int(raw)
    data = (json.dumps(payload) + "\n").encode()
    try:
        view = memoryview(data)
        while view:
            written = os.write(fd, view)
            view = view[written:]
    finally:
        os.close(fd)


def _report(payload: dict[str, Any]) -> None:
    try:
        write_result(payload)
    except Exception:
        logger.exception("STS worker could not report its result")


async def _start(
    sessions: SessionManager, session_id: str, role: str
) -> StsCreateSessionResult:
    if role == "create":
        raw = await asyncio.to_thread(sys.stdin.buffer.read)
        request = StsCreateSessionRequest.model_validate_json(raw)
        if request.session_id != session_id:
            raise RuntimeError(
                f"worker session id {session_id} does not match the request"
            )
        return await sessions.create_session(request)
    if role == "rebuild":
        return await sessions.adopt_interrupted(session_id)
    raise RuntimeError(f"unknown worker role {role}")


async def hold_until_quiet(sessions: SessionManager, stop: asyncio.Event) -> None:
    """Stay up until the session is gone, or until a signal says to stop.

    On the signal, tear the session down as interrupted and wait again.
    The broker is still open here; the caller closes it after this
    returns, which is what flushes the last reply.
    """
    quiet = asyncio.create_task(sessions.wait_until_quiet(), name="sts-worker-quiet")
    stopped = asyncio.create_task(stop.wait(), name="sts-worker-signal")
    done, pending = await asyncio.wait(
        {quiet, stopped}, return_when=asyncio.FIRST_COMPLETED
    )
    for task in pending:
        task.cancel()
    await asyncio.gather(*pending, return_exceptions=True)
    if quiet in done:
        quiet.result()
        return
    await sessions.close_all()
    await sessions.wait_until_quiet()


async def amain(session_id: str, role: str) -> bool:
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, stop.set)
        except NotImplementedError:
            pass

    async with Broker() as broker:
        sessions: SessionManager | None = None
        try:
            refresh()
            sessions = SessionManager(
                broker,
                persist_live=sts_db.persist_live_session,
                mark_done=sts_db.mark_session_finished,
                list_db_sessions=sts_db.list_sessions,
                remember_fact=sts_db.remember_fact,
                mark_live=sts_db.mark_session_live,
                bump_rebuild_count=sts_db.bump_rebuild_count,
                reset_rebuild_count=sts_db.reset_rebuild_count,
                td_instance=sts_db.td_instance,
                derive_sts=sts_db.derived_sts,
                instance=instance_name("sts"),
                control_types=frozenset({STS_SESSION_STOP, STS_SESSION_FAIL}),
            )
            result = await _start(sessions, session_id, role)
            write_result(
                {
                    "ok": True,
                    "strategy": result.strategy,
                    "status": result.status,
                    "reason": result.reason,
                }
            )
        except Exception as exc:
            logger.exception(
                "STS worker failed to start session=%s role=%s", session_id, role
            )
            _report({"ok": False, "error": str(exc)})
            if sessions is not None:
                await sessions.close_all()
            return False
        assert sessions is not None
        await hold_until_quiet(sessions, stop)
    return True


def main(argv: list[str] | None = None) -> None:
    configure_logging("sts")
    args = list(sys.argv[1:] if argv is None else argv)
    if len(args) != 2 or args[1] not in {"create", "rebuild"}:
        raise SystemExit(2)
    arm_parent_death()
    if not uvloop.run(amain(args[0], args[1])):
        raise SystemExit(1)


if __name__ == "__main__":
    main()
