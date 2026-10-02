"""One fetch worker per MD instance, supervised by this process.

The worker id is :data:`FETCH_WORKER_ID` (``md/fetch``). Identity in §3.1
is the instance: the supervisor's work directory is already per instance,
so the id does not repeat it. There is always exactly one desired fetch
worker. It is not an intent row.

``restart="on_failure"``. A death after ready is
:func:`~mftik.procman.plan_restart`, then
:meth:`~mftik.procman.Supervisor.record_restart`, then
:meth:`~mftik.procman.Supervisor.spawn` at incarnation + 1 once the
backoff has elapsed. A death before ready stays ``FAILED`` and is not
spawned (§4.3). Reattach uses the same path for ``APPLY_RESTART``.

A running worker is adopted. This module does not stop workers it does
not own: connection workers are B4-06 and B8, and their observations are
left untouched.

``LOST`` is not a phase :func:`~mftik.procman.plan_restart` accepts. It
is logged and left. See the PR's 「需要決定」.
"""

from __future__ import annotations

import asyncio
import logging
import os
import sys
from collections.abc import Mapping, Sequence

from mftik.clock import Clock
from mftik.procman import (
    OOM_SCORE_ADJ,
    CapacityExceeded,
    CloseMode,
    DesiredSlot,
    ObservedWorker,
    ProcmanError,
    ReattachAction,
    ReattachObservation,
    RestartIntensity,
    Supervisor,
    WorkerPhase,
    WorkerSpec,
    WorkerStatus,
    count_restarts_in_window,
    plan_restart,
    reattach_action,
)

from mftik_md.defaults import (
    FETCH_HB_TIMEOUT_S,
    FETCH_MIN_BACKOFF_S,
    FETCH_RECONCILE_PERIOD_S,
    FETCH_RESTART_MAX,
    FETCH_RESTART_WINDOW_S,
    FETCH_START_TIMEOUT_S,
    FETCH_STOP_GRACE_S,
)

logger = logging.getLogger("md.fetch")

#: Fixed id, one per supervisor. The instance is the work directory, not
#: another path segment.
FETCH_WORKER_ID = "md/fetch"

#: Bus settings the worker needs and the controller already has. Not new
#: variables: absent ones are omitted. Secrets and database URLs are not
#: copied into ``supervisor.json``.
_FORWARDED_ENV = (
    "BROKER_KEY_PREFIX",
    "BROKER_REQUEST_TIMEOUT",
    "LOG_LEVEL",
    "NATS_URL",
    "PYTHONPATH",
    "SSL_CERT_DIR",
    "SSL_CERT_FILE",
)

#: provisional, pending Yi Te (#286) — assembled from the named constants.
FETCH_RESTART_INTENSITY = RestartIntensity(
    max_restarts=FETCH_RESTART_MAX,
    window_s=FETCH_RESTART_WINDOW_S,
    min_backoff_s=FETCH_MIN_BACKOFF_S,
)


def fetch_worker_argv(factory: str | None = None) -> tuple[str, ...]:
    """``python -m mftik_md.fetch``, plus an optional factory ref for tests."""
    argv = (sys.executable, "-m", "mftik_md.fetch")
    if factory:
        return (*argv, factory)
    return argv


def forwarded_env() -> dict[str, str]:
    """The subset of this process's environment the fetch worker runs with."""
    return {key: os.environ[key] for key in _FORWARDED_ENV if os.environ.get(key)}


def fetch_worker_spec(
    *,
    incarnation: int,
    code_ref: str,
    env: Mapping[str, str] | None = None,
    argv: tuple[str, ...] | None = None,
) -> WorkerSpec:
    """The fetch worker's spec. ``restart`` is ``on_failure`` (§4.3)."""
    return WorkerSpec(
        id=FETCH_WORKER_ID,
        plane="md",
        kind="fetch",
        incarnation=incarnation,
        argv=argv if argv is not None else fetch_worker_argv(),
        env=dict(env if env is not None else {}),
        code_ref=code_ref,
        restart="on_failure",
        start_timeout_s=FETCH_START_TIMEOUT_S,
        hb_timeout_s=FETCH_HB_TIMEOUT_S,
        oom_score_adj=OOM_SCORE_ADJ[("md", "fetch")],
        rlimit_data_bytes=None,
        stop_grace_s=FETCH_STOP_GRACE_S,
        labels={},
    )


class FetchController:
    """Adopt, spawn, and restart the one fetch worker on a supervisor.

    Restart times live in this object. They are not written down: a new
    controller process starts a new window. The supervisor holds the slot
    and does not restart on its own.
    """

    def __init__(
        self,
        supervisor: Supervisor,
        *,
        clock: Clock,
        code_ref: str,
        env: Mapping[str, str] | None = None,
        argv: tuple[str, ...] | None = None,
    ) -> None:
        if supervisor.plane != "md":
            raise ValueError("the fetch worker is an MD worker")
        self._supervisor = supervisor
        self._clock = clock
        self._code_ref = code_ref
        self._env = dict(forwarded_env() if env is None else env)
        self._argv = argv
        self._restarted_at: list[float] = []
        self._attempt = 1
        # Incarnation waiting out BACKOFF, spawned on a later pass.
        self._pending: int | None = None
        self._pending_at: float = 0.0
        # Spawn that raised, retried on the next pass. Includes a future
        # ``capacity_exceeded`` refusal (B3-05); no budget is configured.
        self._retry: int | None = None
        # ``(incarnation, phase)`` already recorded and not to be spawned.
        self._settled: tuple[int, WorkerPhase] | None = None
        # A LOST slot is left as it was found. The watch must not spawn
        # over it on a later pass when the slot is no longer held.
        self._lost = False

    def spec(self, incarnation: int) -> WorkerSpec:
        return fetch_worker_spec(
            incarnation=incarnation,
            code_ref=self._code_ref,
            env=self._env,
            argv=self._argv,
        )

    async def reconcile(self, observations: Sequence[ReattachObservation]) -> None:
        """Apply §4.4 to the fetch worker. Other ids are left alone."""
        found = next(
            (item for item in observations if item.id == FETCH_WORKER_ID),
            None,
        )
        if found is None:
            await self._spawn(1)
            return
        action = reattach_action(
            plane="md",
            desired=DesiredSlot.PRESENT,
            observed=found.observed,
        )
        if action is ReattachAction.ADOPT:
            return
        if action is not ReattachAction.APPLY_RESTART:
            return
        status = await self._supervisor.status(FETCH_WORKER_ID)
        lost = found.observed is ObservedWorker.LOST or (
            status is not None and status.phase is WorkerPhase.LOST
        )
        if lost:
            # plan_restart rejects LOST. Not mapped onto a spawn.
            self._lost = True
            logger.warning(
                "MD fetch worker is LOST id=%s; not spawning",
                FETCH_WORKER_ID,
            )
            return
        if status is None:
            await self._spawn((found.incarnation or 0) + 1)
            return
        await self.apply_failure(status)

    async def watch(self, stop: asyncio.Event) -> None:
        """Notice a dead fetch worker and run the restart path."""
        while not stop.is_set():
            try:
                await self.pass_once()
            except ProcmanError as exc:
                logger.error(
                    "MD fetch pass failed code=%s: %s", _failure_code(exc), exc
                )
            if stop.is_set():
                return
            await self._clock.sleep(FETCH_RECONCILE_PERIOD_S)

    async def pass_once(self) -> None:
        """One look at the slot. Safe to call from a test with a fake clock."""
        if self._lost:
            return
        if self._retry is not None:
            await self._spawn(self._retry)
            return
        if self._pending is not None:
            if self._clock.monotonic() < self._pending_at:
                return
            incarnation = self._pending
            if await self._spawn(incarnation):
                self._pending = None
                self._note_restart()
            return
        status = await self._supervisor.status(FETCH_WORKER_ID)
        if status is None:
            await self._spawn(1)
            return
        if status.phase in (
            WorkerPhase.STARTING,
            WorkerPhase.RUNNING,
            WorkerPhase.STOPPING,
            WorkerPhase.BACKOFF,
        ):
            return
        if status.phase is WorkerPhase.LOST:
            self._lost = True
            logger.warning(
                "MD fetch worker is LOST id=%s pid=%s; not spawning",
                status.spec.id,
                status.pid,
            )
            return
        if status.phase in (WorkerPhase.CRASHED, WorkerPhase.FAILED):
            await self.apply_failure(status)

    async def apply_failure(self, status: WorkerStatus) -> None:
        """``plan_restart``, then ``record_restart``. Spawn waits out backoff."""
        if self._settled == (status.spec.incarnation, status.phase):
            return
        decision = plan_restart(
            phase=status.phase,
            restart="on_failure",
            restarts_in_window=count_restarts_in_window(
                self._restarted_at,
                now_s=self._clock.monotonic(),
                window_s=FETCH_RESTART_WINDOW_S,
            ),
            intensity=FETCH_RESTART_INTENSITY,
            attempt=self._attempt,
        )
        await self._supervisor.record_restart(status.spec.id, decision)
        if decision.phase is not WorkerPhase.BACKOFF or decision.delay_s is None:
            self._settled = (status.spec.incarnation, decision.phase)
            return
        self._pending = status.spec.incarnation + 1
        self._pending_at = self._clock.monotonic() + decision.delay_s

    async def _spawn(self, incarnation: int) -> bool:
        try:
            await self._supervisor.spawn(self.spec(incarnation))
        except ProcmanError as exc:
            self._retry = incarnation
            logger.error(
                "MD fetch spawn failed code=%s: %s", _failure_code(exc), exc
            )
            return False
        self._retry = None
        self._settled = None
        return True

    def _note_restart(self) -> None:
        self._restarted_at.append(self._clock.monotonic())
        self._attempt += 1


def _failure_code(exc: ProcmanError) -> str:
    """``capacity_exceeded`` is B3-05's refusal. Anything else is a spawn fault.

    No memory budget is configured here. A refusal is logged and retried
    on the next pass.
    """
    if isinstance(exc, CapacityExceeded) or "capacity_exceeded" in str(exc):
        return "capacity_exceeded"
    return "spawn_failed"


def fetch_close_mode() -> CloseMode:
    """SIGTERM of the MD process is a roll: detach, do not stop the worker."""
    return CloseMode.DETACH


__all__ = [
    "FETCH_RESTART_INTENSITY",
    "FETCH_WORKER_ID",
    "FetchController",
    "fetch_close_mode",
    "fetch_worker_argv",
    "fetch_worker_spec",
    "forwarded_env",
]
