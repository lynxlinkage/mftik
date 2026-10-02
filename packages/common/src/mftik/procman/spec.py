"""``WorkerSpec`` — one worker, as §4.3 names it.

Constructing a spec is real. Nothing here starts a process.
"""

from __future__ import annotations

import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from types import MappingProxyType
from typing import Literal

from mftik.procman.errors import InvalidWorkerId, InvalidWorkerSpec

#: Planes that embed a supervisor. SYM and Paper are not procman planes.
PLANES: tuple[Literal["sts", "md", "td"], ...] = ("sts", "md", "td")

Plane = Literal["sts", "md", "td"]
RestartMode = Literal["never", "on_failure"]

#: ``WorkerSpec.restart``. ``never`` records the crash and stops;
#: ``on_failure`` backs off unless the intensity window is already full.
RESTART_MODES: tuple[RestartMode, ...] = ("never", "on_failure")

#: Initial ``oom_score_adj`` by ``(plane, kind)`` (§4.7). Positive, so the
#: kernel prefers these processes when it has to kill something. Controller
#: and shim stay at :data:`CONTROLLER_OOM_SCORE_ADJ` / :data:`SHIM_OOM_SCORE_ADJ`
#: and are absent here. An STS offload child is +900 and is also absent: it
#: is a child of the session worker (§5.5), not a worker kind (§3.1). B4
#: replaces these numbers with measured RSS.
OOM_SCORE_ADJ: Mapping[tuple[str, str], int] = MappingProxyType(
    {
        ("sts", "session"): 800,
        ("md", "conn"): 300,
        ("md", "fetch"): 300,
        ("td", "account"): 100,
    }
)

#: Shim and controller are left at the kernel default. Lowering it would
#: need ``CAP_SYS_RESOURCE``, which the plane does not have (§4.7).
SHIM_OOM_SCORE_ADJ = 0
CONTROLLER_OOM_SCORE_ADJ = 0

#: One path segment of a worker id. Digits, so ``td/account/42`` and
#: ``md/conn/Deribit/public/0`` both fit. Dots and hyphens, so a session id
#: can. No leading dot, so ``.`` and ``..`` cannot be a segment.
_SEGMENT = r"[A-Za-z0-9][A-Za-z0-9._-]*"
_WORKER_ID = re.compile(rf"^{_SEGMENT}(/{_SEGMENT})*$")


def validate_worker_id(worker_id: str) -> str:
    """Return ``worker_id`` when it is safe as a relative path under ``run/``.

    The socket is ``${WORK_DIR}/run/<id>.sock`` and the supervisor finds the
    worker by that path, not by pid (§4.2 S5). An id with a ``..`` segment,
    an empty segment, or an absolute path would point the socket outside
    ``run/``. The id is otherwise opaque (P6): this check does not parse
    ``sts/session/…`` into a session.
    """
    if not isinstance(worker_id, str) or _WORKER_ID.fullmatch(worker_id) is None:
        raise InvalidWorkerId(
            f"worker id {worker_id!r} must be one or more path segments of "
            "letters, digits, '.', '_' and '-', so it can live under run/"
        )
    return worker_id


def _as_int(value: object, name: str) -> int:
    # ``bool`` is an ``int`` subclass; a flag here would be a spec bug.
    if type(value) is not int:
        raise InvalidWorkerSpec(f"{name} must be an int")
    return value


def _as_float(value: object, name: str) -> float:
    if type(value) is bool or not isinstance(value, int | float):
        raise InvalidWorkerSpec(f"{name} must be a number")
    return float(value)


def _str_mapping(value: object, name: str) -> Mapping[str, str]:
    if isinstance(value, str) or not isinstance(value, Mapping):
        raise InvalidWorkerSpec(f"{name} must map strings to strings")
    copied: dict[str, str] = {}
    for key, item in value.items():
        if not isinstance(key, str) or key == "" or not isinstance(item, str):
            raise InvalidWorkerSpec(f"{name} must map non-empty strings to strings")
        copied[key] = item
    return MappingProxyType(copied)


@dataclass(frozen=True)
class WorkerSpec:
    """What the supervisor needs to spawn one worker (§4.3).

    Frozen so a caller cannot change a spec the supervisor has already
    been handed. The plan's sketch writes ``list`` and ``dict``; both are
    stored as a tuple and a read-only mapping for that reason.

    ``code_ref`` is the release version of the controller that spawned the
    worker (§4.5).

    ``labels`` are opaque strings. Procman copies them and does not read
    them (P6).

    ``hb_timeout_s`` of ``None`` means the supervisor does not arm a
    heartbeat timer for this worker (§4.3).
    ``rlimit_data_bytes`` of ``None`` means the shim does not call
    ``setrlimit`` (§4.7). ``oom_score_adj`` is the value the shim writes
    to ``/proc/self/oom_score_adj`` after fork and before exec; the
    initial per-kind numbers are :data:`OOM_SCORE_ADJ`.
    """

    id: str
    plane: Plane
    kind: str
    incarnation: int
    argv: tuple[str, ...]
    env: Mapping[str, str] = field(hash=False)
    #: the release version of the controller that spawned the worker (§4.5)
    code_ref: str
    restart: RestartMode
    start_timeout_s: float
    hb_timeout_s: float | None
    oom_score_adj: int
    rlimit_data_bytes: int | None
    stop_grace_s: float
    labels: Mapping[str, str] = field(hash=False)

    def __post_init__(self) -> None:
        validate_worker_id(self.id)
        if self.plane not in PLANES:
            raise InvalidWorkerSpec(
                f"plane {self.plane!r} is not one of {', '.join(PLANES)}"
            )
        if not isinstance(self.kind, str) or self.kind == "":
            raise InvalidWorkerSpec("kind must be a non-empty string")
        incarnation = _as_int(self.incarnation, "incarnation")
        if incarnation < 0:
            raise InvalidWorkerSpec("incarnation must be >= 0")
        if isinstance(self.argv, str) or not isinstance(self.argv, Sequence):
            raise InvalidWorkerSpec("argv must be a sequence of strings")
        argv = tuple(self.argv)
        if not argv or not all(isinstance(arg, str) for arg in argv):
            raise InvalidWorkerSpec("argv must be a non-empty sequence of strings")
        if not isinstance(self.code_ref, str):
            raise InvalidWorkerSpec("code_ref must be a string")
        if self.restart not in RESTART_MODES:
            raise InvalidWorkerSpec(
                f"restart {self.restart!r} is not one of {', '.join(RESTART_MODES)}"
            )
        start_timeout_s = _as_float(self.start_timeout_s, "start_timeout_s")
        if start_timeout_s <= 0:
            raise InvalidWorkerSpec("start_timeout_s must be > 0")
        if self.hb_timeout_s is not None:
            hb_timeout_s = _as_float(self.hb_timeout_s, "hb_timeout_s")
            if hb_timeout_s <= 0:
                raise InvalidWorkerSpec("hb_timeout_s must be > 0 or None")
        else:
            hb_timeout_s = None
        oom = _as_int(self.oom_score_adj, "oom_score_adj")
        # The kernel accepts -1000..1000. Anything else fails the write
        # the shim does between fork and exec, so it is refused here.
        if not -1000 <= oom <= 1000:
            raise InvalidWorkerSpec("oom_score_adj must be in -1000..1000")
        if self.rlimit_data_bytes is not None:
            limit = _as_int(self.rlimit_data_bytes, "rlimit_data_bytes")
            if limit <= 0:
                raise InvalidWorkerSpec("rlimit_data_bytes must be > 0 or None")
        else:
            limit = None
        stop_grace_s = _as_float(self.stop_grace_s, "stop_grace_s")
        if stop_grace_s < 0:
            raise InvalidWorkerSpec("stop_grace_s must be >= 0")
        object.__setattr__(self, "argv", argv)
        object.__setattr__(self, "env", _str_mapping(self.env, "env"))
        object.__setattr__(self, "labels", _str_mapping(self.labels, "labels"))
        object.__setattr__(self, "incarnation", incarnation)
        object.__setattr__(self, "start_timeout_s", start_timeout_s)
        object.__setattr__(self, "hb_timeout_s", hb_timeout_s)
        object.__setattr__(self, "oom_score_adj", oom)
        object.__setattr__(self, "rlimit_data_bytes", limit)
        object.__setattr__(self, "stop_grace_s", stop_grace_s)
