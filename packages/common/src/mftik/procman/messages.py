"""Shim NDJSON, the status pipe, and the exit record.

The socket protocol is four commands — ``status``, ``signal``, ``watch``,
``release`` — each one JSON object and a newline (S5). The status pipe is
a different channel: the worker writes it, the shim reads it, and one
message is at most :data:`PIPE_BUF` bytes so the write is atomic (S6).
The exit file is what the shim leaves on disk after it reaps the worker,
before it waits for ``release`` (S3).

Framing is real. Opening a socket or writing the file is
:mod:`mftik.procman.shim` (B3-01). Types here use the standard library
only (F29).
"""

from __future__ import annotations

import json
import signal as _signal
from collections.abc import Mapping
from dataclasses import dataclass, field
from pathlib import Path
from types import MappingProxyType
from typing import Any, Literal

from mftik.procman.errors import MessageError
from mftik.procman.spec import validate_worker_id

#: POSIX atomic-write limit for a pipe. One status-pipe message stays at
#: or under this so the reader never sees a torn frame (S6).
PIPE_BUF = 4096

#: Write end of the status pipe, injected by the shim into the worker's
#: environment before exec. The worker writes :class:`WorkerHeartbeat`
#: frames there. The plan removes the old controller lifeline; this pipe's
#: other end is the shim (S2, S4, S6).
STATUS_FD_ENV = "MFTIK_STATUS_FD"

_RUN = "run"


def run_dir(work_dir: Path) -> Path:
    """``${WORK_DIR}/run``, where sockets, exit records and ``supervisor.json`` live."""
    return Path(work_dir) / _RUN


def socket_path(work_dir: Path, worker_id: str) -> Path:
    """``${WORK_DIR}/run/<worker_id>.sock`` (S5).

    ``sts/session/a1b2c3`` becomes ``run/sts/session/a1b2c3.sock``. The
    supervisor finds the worker by this path and does not store a pid.
    A unix-socket path is limited (108 bytes on Linux, including the
    trailing NUL); B3 keeps ``WORK_DIR`` short enough for the longest id.
    """
    return run_dir(work_dir) / f"{validate_worker_id(worker_id)}.sock"


_LOG_STREAMS = frozenset({"stdout", "stderr", "status"})


def log_path(work_dir: Path, worker_id: str, stream: str) -> Path:
    """``${WORK_DIR}/run/<worker_id>.<stream>.log`` (S4).

    ``stream`` is ``stdout``, ``stderr`` or ``status``. The shim holds
    the worker's stdio and the status pipe and writes them here, rotating
    when a file outgrows the shim's limit. The backup names are the
    shim's; this helper is the live file.
    """
    if stream not in _LOG_STREAMS:
        raise ValueError(
            f"log stream {stream!r} is not one of {', '.join(sorted(_LOG_STREAMS))}"
        )
    return run_dir(work_dir) / f"{validate_worker_id(worker_id)}.{stream}.log"


def exit_record_path(work_dir: Path, worker_id: str) -> Path:
    """``${WORK_DIR}/run/<worker_id>.exit.json`` (S3)."""
    return run_dir(work_dir) / f"{validate_worker_id(worker_id)}.exit.json"


def exit_record_tmp_path(work_dir: Path, worker_id: str) -> Path:
    """Sibling of the exit record. S3 writes this, then renames it into place."""
    final = exit_record_path(work_dir, worker_id)
    return final.with_name(final.name + ".tmp")


def supervisor_state_path(work_dir: Path) -> Path:
    """``${WORK_DIR}/run/supervisor.json``, loaded synchronously on start (§4.4).

    The bytes are B3-03's. The path is named here so both sides share it.
    """
    return run_dir(work_dir) / "supervisor.json"


def dump_frame(payload: Mapping[str, Any]) -> bytes:
    """One JSON object, keys sorted, ending in a single newline."""
    try:
        body = json.dumps(payload, separators=(",", ":"), sort_keys=True)
    except TypeError as exc:
        raise MessageError("frame is not JSON-serialisable") from exc
    return body.encode("utf-8") + b"\n"


def load_frame(line: bytes) -> dict[str, Any]:
    """Parse one :func:`dump_frame` line. Extra whitespace inside is accepted."""
    if not isinstance(line, bytes | bytearray):
        raise MessageError("a frame is bytes")
    if line.count(b"\n") != 1 or not line.endswith(b"\n"):
        raise MessageError("a frame is one JSON object and a newline")
    try:
        value = json.loads(line)
    except json.JSONDecodeError as exc:
        raise MessageError("frame is not JSON") from exc
    if not isinstance(value, dict):
        raise MessageError("frame must be a JSON object")
    return value


def _exact(payload: Mapping[str, Any], keys: set[str]) -> None:
    if set(payload) != keys:
        raise MessageError(f"frame keys {sorted(payload)} != {sorted(keys)}")


def _as_bool(value: object, name: str) -> bool:
    if type(value) is not bool:
        raise MessageError(f"{name} must be a boolean")
    return value


def _as_int(value: object, name: str) -> int:
    # ``signal.SIGTERM`` is an ``IntEnum``, which is an ``int`` but not
    # ``type is int``. ``bool`` is an ``int`` subclass and is not a count.
    if type(value) is bool or not isinstance(value, int):
        raise MessageError(f"{name} must be an int")
    return int(value)


def _signal_number(value: object, name: str) -> int:
    number = _as_int(value, name)
    if not 1 <= number < _signal.NSIG:
        raise MessageError(f"{name} must be a signal number in 1..{_signal.NSIG - 1}")
    return number


def _exit_code(value: object) -> int:
    code = _as_int(value, "exit_code")
    if not 0 <= code <= 255:
        raise MessageError("exit_code must be in 0..255")
    return code


def _pid(value: object, *, allow_none: bool) -> int | None:
    if value is None and allow_none:
        return None
    pid = _as_int(value, "pid")
    if pid <= 0:
        raise MessageError("pid must be > 0")
    return pid


def _rss(value: object) -> int | None:
    if value is None:
        return None
    rss = _as_int(value, "rss_bytes")
    if rss < 0:
        raise MessageError("rss_bytes must be >= 0")
    return rss


@dataclass(frozen=True)
class StatusQuery:
    """Ask the shim for one :class:`ShimStatus` (S5)."""

    op: Literal["status"] = "status"


@dataclass(frozen=True)
class SignalCommand:
    """``killpg`` this signal to the worker's process group (S5)."""

    signal: int
    op: Literal["signal"] = "signal"

    def __post_init__(self) -> None:
        object.__setattr__(self, "signal", _signal_number(self.signal, "signal"))


@dataclass(frozen=True)
class WatchCommand:
    """Stream :class:`ShimStatus` until the socket closes (S5).

    The first line is the shim's current view, not the next heartbeat.
    Later lines follow the status pipe and the worker's exit.
    """

    op: Literal["watch"] = "watch"


@dataclass(frozen=True)
class ReleaseCommand:
    """The supervisor has read the exit record; the shim may exit (S3)."""

    op: Literal["release"] = "release"


ShimCommand = StatusQuery | SignalCommand | WatchCommand | ReleaseCommand


@dataclass(frozen=True)
class ShimStatus:
    """What the shim reports. Facts it saw, not the supervisor's phase.

    ``exit_code`` and ``signal`` are both ``None`` while the worker is
    alive. After it has been reaped, exactly one of them is set: a normal
    exit carries the code, a signal death carries the signal. ``ready`` is
    the last status-pipe snapshot, or ``False`` when none has arrived.
    ``rss_bytes`` is the worker's process tree, not the shim (§4.7); ``None``
    when the shim has not read it yet.
    """

    id: str
    incarnation: int
    pid: int | None
    ready: bool
    exit_code: int | None = None
    signal: int | None = None
    rss_bytes: int | None = None
    op: Literal["status"] = "status"

    def __post_init__(self) -> None:
        object.__setattr__(self, "id", validate_worker_id(self.id))
        incarnation = _as_int(self.incarnation, "incarnation")
        if incarnation < 0:
            raise MessageError("incarnation must be >= 0")
        object.__setattr__(self, "incarnation", incarnation)
        object.__setattr__(self, "pid", _pid(self.pid, allow_none=True))
        object.__setattr__(self, "ready", _as_bool(self.ready, "ready"))
        object.__setattr__(self, "rss_bytes", _rss(self.rss_bytes))
        _check_exit_pair(self.exit_code, self.signal, allow_neither=True)
        if self.exit_code is not None:
            object.__setattr__(self, "exit_code", _exit_code(self.exit_code))
        if self.signal is not None:
            object.__setattr__(self, "signal", _signal_number(self.signal, "signal"))


def _check_exit_pair(
    exit_code: object, sig: object, *, allow_neither: bool
) -> None:
    has_code = exit_code is not None
    has_signal = sig is not None
    if has_code and has_signal:
        raise MessageError("exit carries both an exit code and a signal")
    if not allow_neither and not has_code and not has_signal:
        raise MessageError("exit carries neither an exit code nor a signal")


@dataclass(frozen=True)
class ExitRecord:
    """``<id>.exit.json``: the reap the shim witnessed (S3, §3.3).

    Exactly one of ``exit_code`` and ``signal`` is set. ``ready`` is whether
    a heartbeat had already reported ready, which is what reattach needs to
    tell ``FAILED`` from ``CRASHED``. ``pid`` is the process that was reaped.
    """

    id: str
    incarnation: int
    pid: int
    exit_code: int | None
    signal: int | None
    ready: bool

    def __post_init__(self) -> None:
        object.__setattr__(self, "id", validate_worker_id(self.id))
        incarnation = _as_int(self.incarnation, "incarnation")
        if incarnation < 0:
            raise MessageError("incarnation must be >= 0")
        object.__setattr__(self, "incarnation", incarnation)
        pid = _pid(self.pid, allow_none=False)
        if pid is None:
            raise MessageError("pid must be > 0")
        object.__setattr__(self, "pid", pid)
        object.__setattr__(self, "ready", _as_bool(self.ready, "ready"))
        _check_exit_pair(self.exit_code, self.signal, allow_neither=False)
        if self.exit_code is not None:
            object.__setattr__(self, "exit_code", _exit_code(self.exit_code))
        if self.signal is not None:
            object.__setattr__(self, "signal", _signal_number(self.signal, "signal"))


@dataclass(frozen=True)
class WorkerHeartbeat:
    """One status-pipe message: the worker's whole snapshot, not a delta (S6).

    Procman reads :attr:`ready` and nothing else (P6). Further keys ride in
    :attr:`extra` so a plane can put its own progress on the same atomic
    write; they must stay inside :data:`PIPE_BUF` together with ``ready``.
    """

    ready: bool
    extra: Mapping[str, Any] = field(default_factory=dict, hash=False)

    def __post_init__(self) -> None:
        object.__setattr__(self, "ready", _as_bool(self.ready, "ready"))
        if isinstance(self.extra, str) or not isinstance(self.extra, Mapping):
            raise MessageError("heartbeat extra must be a mapping")
        if "ready" in self.extra:
            raise MessageError("heartbeat extra must not override ready")
        object.__setattr__(self, "extra", MappingProxyType(dict(self.extra)))


def encode_command(command: ShimCommand) -> bytes:
    """One NDJSON line for a socket command."""
    if isinstance(command, StatusQuery):
        return dump_frame({"op": "status"})
    if isinstance(command, SignalCommand):
        return dump_frame({"op": "signal", "signal": command.signal})
    if isinstance(command, WatchCommand):
        return dump_frame({"op": "watch"})
    if isinstance(command, ReleaseCommand):
        return dump_frame({"op": "release"})
    raise MessageError(f"unknown command {command!r}")


def decode_command(line: bytes) -> ShimCommand:
    """The inverse of :func:`encode_command`. Unknown keys are refused."""
    payload = load_frame(line)
    op = payload.get("op")
    if op == "status":
        _exact(payload, {"op"})
        return StatusQuery()
    if op == "signal":
        _exact(payload, {"op", "signal"})
        return SignalCommand(payload["signal"])
    if op == "watch":
        _exact(payload, {"op"})
        return WatchCommand()
    if op == "release":
        _exact(payload, {"op"})
        return ReleaseCommand()
    raise MessageError(f"unknown shim op {op!r}")


_STATUS_KEYS = {
    "op",
    "id",
    "incarnation",
    "pid",
    "ready",
    "exit_code",
    "signal",
    "rss_bytes",
}


def encode_status(status: ShimStatus) -> bytes:
    """One NDJSON line for a ``status`` reply or a ``watch`` event."""
    if not isinstance(status, ShimStatus):
        raise MessageError("status frame must be a ShimStatus")
    return dump_frame(
        {
            "op": "status",
            "id": status.id,
            "incarnation": status.incarnation,
            "pid": status.pid,
            "ready": status.ready,
            "exit_code": status.exit_code,
            "signal": status.signal,
            "rss_bytes": status.rss_bytes,
        }
    )


def decode_status(line: bytes) -> ShimStatus:
    """The inverse of :func:`encode_status`."""
    payload = load_frame(line)
    _exact(payload, _STATUS_KEYS)
    if payload["op"] != "status":
        raise MessageError(f"status frame op is {payload['op']!r}")
    return ShimStatus(
        id=payload["id"],
        incarnation=payload["incarnation"],
        pid=payload["pid"],
        ready=payload["ready"],
        exit_code=payload["exit_code"],
        signal=payload["signal"],
        rss_bytes=payload["rss_bytes"],
    )


_EXIT_KEYS = {"id", "incarnation", "pid", "exit_code", "signal", "ready"}


def encode_exit(record: ExitRecord) -> bytes:
    """The body of ``<id>.exit.json``, one JSON object and a newline."""
    if not isinstance(record, ExitRecord):
        raise MessageError("exit frame must be an ExitRecord")
    return dump_frame(
        {
            "id": record.id,
            "incarnation": record.incarnation,
            "pid": record.pid,
            "exit_code": record.exit_code,
            "signal": record.signal,
            "ready": record.ready,
        }
    )


def decode_exit(line: bytes) -> ExitRecord:
    """The inverse of :func:`encode_exit`."""
    payload = load_frame(line)
    _exact(payload, _EXIT_KEYS)
    return ExitRecord(
        id=payload["id"],
        incarnation=payload["incarnation"],
        pid=payload["pid"],
        exit_code=payload["exit_code"],
        signal=payload["signal"],
        ready=payload["ready"],
    )


def encode_heartbeat(beat: WorkerHeartbeat) -> bytes:
    """One status-pipe frame. Longer than :data:`PIPE_BUF` is refused (S6)."""
    if not isinstance(beat, WorkerHeartbeat):
        raise MessageError("heartbeat must be a WorkerHeartbeat")
    payload: dict[str, Any] = {"ready": beat.ready, **dict(beat.extra)}
    frame = dump_frame(payload)
    if len(frame) > PIPE_BUF:
        raise MessageError(
            f"status pipe message is {len(frame)} bytes; S6 limits it to {PIPE_BUF}"
        )
    return frame


def decode_heartbeat(line: bytes) -> WorkerHeartbeat:
    """The inverse of :func:`encode_heartbeat`. Keys other than ``ready`` are extra."""
    if len(line) > PIPE_BUF:
        raise MessageError(
            f"status pipe message is {len(line)} bytes; S6 limits it to {PIPE_BUF}"
        )
    payload = load_frame(line)
    if "ready" not in payload:
        raise MessageError("heartbeat requires ready")
    extra = {key: value for key, value in payload.items() if key != "ready"}
    return WorkerHeartbeat(ready=payload["ready"], extra=extra)
