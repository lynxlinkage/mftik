"""The ``WorkerSpec`` procman is allowed to see for an STS session.

Building it is real. It does not spawn.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence

from mftik.procman import OOM_SCORE_ADJ, WorkerSpec

from mftik_sts.controller.types import SESSION_KIND, SessionSpec, session_worker_id


def session_worker_spec(
    spec: SessionSpec,
    *,
    incarnation: int,
    argv: Sequence[str],
    code_ref: str,
    start_timeout_s: float,
    hb_timeout_s: float | None,
    stop_grace_s: float,
    env: Mapping[str, str] | None = None,
    rlimit_data_bytes: int | None = None,
) -> WorkerSpec:
    """The spec the orchestrator hands :meth:`mftik.procman.Supervisor.spawn`.

    ``restart`` is ``never`` on every call, including when
    ``spec.restart`` is ``on_failure``. Procman must not restart an STS
    session worker on its own.

    :func:`mftik.procman.plan_restart` only sees ``WorkerSpec.restart`` and
    a caller-supplied intensity. It does not know the crash class, whether
    ``td.order.cancel_session`` has confirmed, or whether ``on_ready`` has
    returned (P6). If this spec said ``on_failure``, procman would spawn
    the next incarnation as soon as the process died — before cleanup —
    and R1 would be false. ``reattach_action`` for STS is already
    ``mark_failed`` (no spawn). This function is the other half: the spec
    procman holds also says never.

    The deploy's mode stays on :class:`SessionSpec` and is read by
    :func:`mftik_sts.controller.decide_restart`. After the shim's exit
    record exists and cleanup has confirmed, a rehang calls
    :meth:`~mftik.procman.Supervisor.spawn` with ``incarnation + 1`` and
    another spec from this function. ``restart`` is ``never`` again.

    ``labels`` is empty. ``strategy_digest`` and ``env_generation`` are
    IF-16. ``code_ref`` is the platform release the caller passes (§4.5),
    not a digest.

    ``start_timeout_s``, ``hb_timeout_s`` and ``stop_grace_s`` are the
    caller's. This function does not copy ``spec.start_timeout_s`` into
    procman's ready timer: that budget counts ``on_start`` only and the
    orchestrator enforces it (F12). The plan does not give STS a heartbeat
    number; MD and TD numbers are not defined here either.
    """
    return WorkerSpec(
        id=session_worker_id(spec.session_id),
        plane="sts",
        kind=SESSION_KIND,
        incarnation=incarnation,
        argv=tuple(argv),
        env={} if env is None else env,
        code_ref=code_ref,
        restart="never",
        start_timeout_s=start_timeout_s,
        hb_timeout_s=hb_timeout_s,
        oom_score_adj=OOM_SCORE_ADJ[("sts", "session")],
        rlimit_data_bytes=rlimit_data_bytes,
        stop_grace_s=stop_grace_s,
        labels={},
    )
