"""The ``WorkerSpec`` procman is allowed to see for a TD account.

Building it is real. It does not spawn.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence

from mftik.procman import OOM_SCORE_ADJ, WorkerSpec

from mftik_td.controller.types import (
    ACCOUNT_KIND,
    FIRST_INCARNATION,
    BoundAccount,
    account_worker_id,
)


def account_worker_spec(
    account: BoundAccount,
    *,
    incarnation: int,
    argv: Sequence[str],
    code_ref: str,
    start_timeout_s: float,
    hb_timeout_s: float,
    stop_grace_s: float,
    env: Mapping[str, str] | None = None,
    rlimit_data_bytes: int | None = None,
) -> WorkerSpec:
    """The spec the orchestrator hands :meth:`mftik.procman.Supervisor.spawn`.

    ``restart`` is ``on_failure`` on every call (§4.3). An account is
    infrastructure: a death after ready is restarted, with backoff, by
    procman. The intensity that bounds that restart is not a field of
    this spec. The orchestrator passes the caller's
    :class:`~mftik.procman.RestartIntensity` to
    :func:`mftik.procman.plan_restart` (issue #286). This function does
    not choose those numbers.

    ``labels`` is empty. ``strategy_digest`` and ``env_generation`` are
    IF-16. ``code_ref`` is the platform release the caller passes (§4.5),
    not a digest.

    ``start_timeout_s``, ``hb_timeout_s`` and ``stop_grace_s`` are the
    caller's. The plan gives an account worker a heartbeat timeout and
    does not give the number. ``None`` is refused: ``None`` means the
    supervisor does not arm a timer, and §4.3 says a stuck account loop
    is a dead worker.

    The incarnation is the controller's to assign (§4.3). The first one
    is :data:`~mftik_td.controller.FIRST_INCARNATION`. A replacement is
    the previous one plus one, and only after
    :func:`mftik_td.controller.spawn_allowed` (F36).
    """
    if not isinstance(account, BoundAccount):
        raise TypeError("account must be a BoundAccount")
    if type(incarnation) is not int or incarnation < FIRST_INCARNATION:
        raise ValueError("incarnation must be an int >= 1")
    if not isinstance(code_ref, str) or code_ref == "":
        raise ValueError("code_ref must be a non-empty string")
    if hb_timeout_s is None:
        raise ValueError(
            "an account worker arms a heartbeat timer (§4.3); "
            "hb_timeout_s must be a number > 0"
        )
    return WorkerSpec(
        id=account_worker_id(account.api_id),
        plane="td",
        kind=ACCOUNT_KIND,
        incarnation=incarnation,
        argv=tuple(argv),
        env={} if env is None else env,
        code_ref=code_ref,
        restart="on_failure",
        start_timeout_s=start_timeout_s,
        hb_timeout_s=hb_timeout_s,
        oom_score_adj=OOM_SCORE_ADJ[("td", "account")],
        rlimit_data_bytes=rlimit_data_bytes,
        stop_grace_s=stop_grace_s,
        labels={},
    )
