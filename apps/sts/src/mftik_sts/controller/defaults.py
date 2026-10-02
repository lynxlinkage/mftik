"""STS restart defaults (F11).

These numbers belong to the STS orchestrator. Procman takes a
:class:`~mftik.procman.RestartIntensity` from its caller and does not
keep a copy (IF-03). MD and TD are described as exponential backoff plus
an intensity (§4.3) and the plan does not give them numbers, so nothing
here invents any (issue #286).

A deploy may set a lower ``max_restarts`` or a different
``restart_window_s`` in ``strategy.yml``. It cannot lower the backoff
floor: that floor is R2, not a document field.
"""

from __future__ import annotations

from mftik.procman import RestartIntensity
from mftik.protocol import ON_STOP_TIMEOUT_S

#: Restarts allowed inside :data:`STS_RESTART_WINDOW_S` before the session
#: is failed instead of hung up again (F11). The same number as
#: ``strategy.yml``'s default. The comparison is
#: ``restarts_in_window >= max_restarts``, counting restarts already
#: started: four prior restarts still rehang, five prior restarts fail.
STS_MAX_RESTARTS = 5

#: The window those restarts are counted in, in seconds (F11).
STS_RESTART_WINDOW_S = 600

#: Shortest backoff before the next incarnation, in seconds (R2, F11).
#: Not a ``strategy.yml`` field. One second is what keeps the previous
#: incarnation's last ``client_order_id`` and the next incarnation's first
#: in different seconds: seq starts at 0 in each incarnation, and the id
#: packs ``session(24) | ts_sec(28) | seq(8)``.
STS_MIN_BACKOFF_S = 1.0

#: Incarnation of the first worker spawned for a session. A rehang uses
#: the previous incarnation plus one (§5.2). ``0`` on a status means no
#: worker has been spawned yet.
FIRST_INCARNATION = 1

#: Procman's ready timer for a session worker, in seconds.
#: Provisional, pending Yi Te (#286). Not F12's ``on_start`` budget.
#: Ten seconds covers a cold interpreter start; a stand-in is ready at once.
SESSION_START_TIMEOUT_S = 10.0

#: Heartbeat silence, in seconds, after which a running session worker
#: is no longer ``RUNNING``. Provisional, pending Yi Te (#286). The same
#: number the procman supervisor tests use for this timer.
SESSION_HB_TIMEOUT_S = 3.0

#: How long a stop waits before the worker is killed, in seconds.
#: Provisional, pending Yi Te (#286). The existing ``on_stop`` wall clock.
SESSION_STOP_GRACE_S = ON_STOP_TIMEOUT_S


def sts_restart_intensity(
    *,
    max_restarts: int = STS_MAX_RESTARTS,
    window_s: float = STS_RESTART_WINDOW_S,
    min_backoff_s: float = STS_MIN_BACKOFF_S,
) -> RestartIntensity:
    """The intensity the orchestrator passes when it counts a window.

    ``min_backoff_s`` stays at :data:`STS_MIN_BACKOFF_S` unless a caller
    is constructing a value to compare against. A deploy does not override
    it. The floor on :class:`~mftik.procman.RestartIntensity` is zero; the
    STS floor is one second, and :func:`mftik_sts.controller.backoff_s`
    is what applies it. This helper does not lower that floor.
    """
    return RestartIntensity(
        max_restarts=max_restarts,
        window_s=window_s,
        min_backoff_s=min_backoff_s,
    )
