"""The shim process (B3-01, F29, §4.1, §4.2).

The supervisor starts a short-lived intermediate with ``subprocess.Popen``
(never ``asyncio.create_subprocess_exec``). The intermediate forks a
subreaper, ``setsid``s it, and exits, so the subreaper is adopted by host
init and is not in the supervisor's process group. That subreaper forks
the shim. The shim is the worker's parent: it sets
``PR_SET_CHILD_SUBREAPER``, applies ``oom_score_adj`` and ``RLIMIT_DATA``
between fork and exec, holds the worker's stdio and the status pipe, and
speaks the NDJSON socket in :mod:`mftik.procman.messages`.

Importing :mod:`mftik` runs ``mftik/__init__.py``, which pulls the broker.
The intermediate is launched from a bootstrap that loads this module
without executing that package init, so the shim process stays on the
standard library (F29).

``ShimStatus.rss_bytes`` stays ``None``. The worker's process-tree RSS is
B3-04.

S2's contract kills the shim with ``SIGKILL`` and still expects
``<id>.exit.json``. A dead shim cannot reap the worker or write that file
(S3). The subreaper parent stays up for that case: if the shim dies first
it adopts the worker, reaps it, and writes the record. The shim's parent
is that subreaper for the shim's whole life, not init. The numbers for
log rotation are named below; the plan does not give them.
"""

from __future__ import annotations

import ctypes
import ctypes.util
import json
import os
import resource
import select
import signal
import socket
import struct
import subprocess
import sys
import tempfile
import time
import traceback
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path

from mftik.procman.errors import MessageError, ProcmanError
from mftik.procman.messages import (
    PIPE_BUF,
    STATUS_FD_ENV,
    ExitRecord,
    ReleaseCommand,
    ShimStatus,
    SignalCommand,
    StatusQuery,
    WatchCommand,
    decode_command,
    decode_heartbeat,
    decode_status,
    encode_command,
    encode_exit,
    encode_status,
    exit_record_path,
    exit_record_tmp_path,
    log_path,
    run_dir,
    socket_path,
)
from mftik.procman.spec import SHIM_OOM_SCORE_ADJ, WorkerSpec

#: S4. The plan names neither a size nor how many old files to keep.
#: 256 KiB and one backup bound a stuck writer; a multi-megabyte stdio
#: burst still finishes because the shim is the reader. Confirm these.
LOG_MAX_BYTES = 256 * 1024
LOG_BACKUP_COUNT = 1

#: ``sockaddr_un.sun_path`` is 108 bytes including the trailing NUL.
_UNIX_PATH_MAX = 107

#: Write end of the status pipe, placed at a fixed fd so the worker's
#: ``MFTIK_STATUS_FD`` does not depend on which other fds were open.
_STATUS_FD = 3

_PR_SET_PDEATHSIG = 1
_PR_SET_CHILD_SUBREAPER = 36

_libc = ctypes.CDLL(ctypes.util.find_library("c"), use_errno=True)
_libc.prctl.argtypes = (
    ctypes.c_int,
    ctypes.c_ulong,
    ctypes.c_ulong,
    ctypes.c_ulong,
    ctypes.c_ulong,
)
_libc.prctl.restype = ctypes.c_int

# How long a client waits for one reply. A watch that sits idle is the
# supervisor's (B3-02); this only bounds a shim that never answers.
_CLIENT_TIMEOUT_S = 10.0

_LAUNCH = """
import importlib
import sys
import types
from pathlib import Path

root = Path(sys.argv[1])

def seed(name, path):
    mod = types.ModuleType(name)
    mod.__path__ = [str(path)]
    mod.__package__ = name
    mod.__file__ = str(path / "__init__.py")
    sys.modules[name] = mod

seed("mftik", root)
seed("mftik.procman", root / "procman")
importlib.import_module("mftik.procman.shim").main(sys.argv[2:])
"""


@dataclass(frozen=True)
class SpawnedShim:
    """Handle on a shim that has opened its socket.

    ``pid`` is the shim, discovered from the socket rather than from the
    intermediate's ``Popen``: that intermediate has already exited, and the
    shim has been adopted by host init or the nearest subreaper (S1).
    """

    worker_id: str
    socket: Path
    pid: int


class ShimClient:
    """One connection's worth of the NDJSON socket (S5).

    ``status`` reads one :class:`ShimStatus`. ``watch`` yields the current
    status first, then further snapshots, until the socket closes.
    ``signal`` delivers ``killpg`` to the worker's process group. ``release``
    lets the shim exit after the exit record is on disk (S3).
    """

    def __init__(self, socket: Path) -> None:
        self.socket = Path(socket)

    def status(self) -> ShimStatus:
        with self._connected() as sock:
            sock.sendall(encode_command(StatusQuery()))
            return decode_status(_read_frame(sock))

    def signal(self, sig: int) -> None:
        with self._connected() as sock:
            sock.sendall(encode_command(SignalCommand(sig)))
            # The reply is the status after ``killpg``. Reading it is what
            # makes the call wait until the shim has delivered the signal.
            decode_status(_read_frame(sock))

    def watch(self) -> Iterator[ShimStatus]:
        sock = self._open()
        try:
            sock.sendall(encode_command(WatchCommand()))
            buf = bytearray()
            while True:
                try:
                    chunk = sock.recv(4096)
                except TimeoutError:
                    continue
                if not chunk:
                    return
                buf += chunk
                while True:
                    nl = buf.find(b"\n")
                    if nl < 0:
                        break
                    line = bytes(buf[: nl + 1])
                    del buf[: nl + 1]
                    yield decode_status(line)
        finally:
            sock.close()

    def release(self) -> None:
        with self._connected() as sock:
            sock.sendall(encode_command(ReleaseCommand()))
            while sock.recv(4096):
                pass

    def _connected(self) -> socket.socket:
        return _Closing(self._open())

    def _open(self) -> socket.socket:
        sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        try:
            sock.settimeout(_CLIENT_TIMEOUT_S)
            sock.connect(os.fspath(self.socket))
        except OSError:
            sock.close()
            raise
        return sock


class _Closing:
    """``with`` wrapper so a client socket closes on the way out."""

    def __init__(self, sock: socket.socket) -> None:
        self.sock = sock

    def __enter__(self) -> socket.socket:
        return self.sock

    def __exit__(self, *exc: object) -> None:
        self.sock.close()


def spawn_shim(spec: WorkerSpec, *, work_dir: Path) -> SpawnedShim:
    """Double-fork a shim for ``spec`` and return once ``status`` answers.

    The worker's argv is ``spec.argv``. ``spec.env`` is the worker's
    environment; the shim adds :data:`~mftik.procman.messages.STATUS_FD_ENV`
    and does not drop the rest. ``spec.oom_score_adj`` and
    ``spec.rlimit_data_bytes`` are applied in the child before exec.
    """
    work_dir = Path(work_dir)
    run_dir(work_dir).mkdir(parents=True, exist_ok=True)
    spec_path = _write_spec(work_dir, spec)
    package = Path(__file__).resolve().parent.parent
    proc = subprocess.Popen(
        [sys.executable, "-c", _LAUNCH, str(package), str(spec_path)],
        stdin=subprocess.DEVNULL,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        close_fds=True,
    )
    witness: int | None = None
    try:
        deadline = time.monotonic() + spec.start_timeout_s
        witness = _read_witness_pid(proc, deadline)
        peer = _wait_for_peer(socket_path(work_dir, spec.id), deadline)
        rc = proc.wait(timeout=5)
        if rc != 0:
            raise ProcmanError(f"shim intermediate exited {rc}: {_stderr_text(proc)}")
        return SpawnedShim(
            worker_id=spec.id,
            socket=socket_path(work_dir, spec.id),
            pid=peer,
        )
    except Exception:
        _abort(proc, witness, work_dir, spec.id)
        raise
    finally:
        _close_captured(proc)
        spec_path.unlink(missing_ok=True)


def main(argv: list[str] | None = None) -> None:
    """Process entry for one shim (F29).

    The supervisor's ``Popen`` is this process. It forks the subreaper and
    exits; the subreaper forks the shim. Call it only from the bootstrap
    that has not imported :mod:`mftik`.
    """
    args = list(sys.argv[1:] if argv is None else argv)
    if len(args) != 1:
        sys.stderr.write("mftik-shim: expected one spec path\n")
        os._exit(2)
    spec_path = Path(args[0])
    try:
        work_dir, spec = _read_spec(spec_path)
    except (OSError, ValueError, KeyError, TypeError) as exc:
        sys.stderr.write(f"mftik-shim: {exc}\n")
        os._exit(2)
    spec_path.unlink(missing_ok=True)
    witness = os.fork()
    if witness > 0:
        os.write(1, f"{witness}\n".encode())
        os._exit(0)
    _detach_stdio()
    os.setsid()
    _prctl(_PR_SET_CHILD_SUBREAPER, 1)
    shim = os.fork()
    if shim == 0:
        try:
            _serve(work_dir, spec)
        except Exception:
            os._exit(1)
        os._exit(0)
    _witness(work_dir, spec, shim)
    os._exit(0)


def _prctl(option: int, arg: int) -> None:
    if _libc.prctl(option, arg, 0, 0, 0) != 0:
        err = ctypes.get_errno()
        raise OSError(err, os.strerror(err))


def _detach_stdio() -> None:
    fd = os.open(os.devnull, os.O_RDWR)
    try:
        os.dup2(fd, 0)
        os.dup2(fd, 1)
        os.dup2(fd, 2)
    finally:
        if fd > 2:
            os.close(fd)


def _write_spec(work_dir: Path, spec: WorkerSpec) -> Path:
    payload = {
        "work_dir": str(work_dir),
        "spec": {
            "id": spec.id,
            "plane": spec.plane,
            "kind": spec.kind,
            "incarnation": spec.incarnation,
            "argv": list(spec.argv),
            "env": dict(spec.env),
            "code_ref": spec.code_ref,
            "restart": spec.restart,
            "start_timeout_s": spec.start_timeout_s,
            "hb_timeout_s": spec.hb_timeout_s,
            "oom_score_adj": spec.oom_score_adj,
            "rlimit_data_bytes": spec.rlimit_data_bytes,
            "stop_grace_s": spec.stop_grace_s,
            "labels": dict(spec.labels),
        },
    }
    fd, name = tempfile.mkstemp(prefix=".spec-", dir=run_dir(work_dir))
    os.close(fd)
    path = Path(name)
    path.write_text(json.dumps(payload), encoding="utf-8")
    return path


def _read_spec(path: Path) -> tuple[Path, WorkerSpec]:
    raw = json.loads(path.read_text(encoding="utf-8"))
    body = raw["spec"]
    spec = WorkerSpec(
        id=body["id"],
        plane=body["plane"],
        kind=body["kind"],
        incarnation=body["incarnation"],
        argv=body["argv"],
        env=body["env"],
        code_ref=body["code_ref"],
        restart=body["restart"],
        start_timeout_s=body["start_timeout_s"],
        hb_timeout_s=body["hb_timeout_s"],
        oom_score_adj=body["oom_score_adj"],
        rlimit_data_bytes=body["rlimit_data_bytes"],
        stop_grace_s=body["stop_grace_s"],
        labels=body["labels"],
    )
    return Path(raw["work_dir"]), spec


def _read_frame(sock: socket.socket) -> bytes:
    buf = bytearray()
    while True:
        chunk = sock.recv(4096)
        if not chunk:
            break
        buf += chunk
        nl = buf.find(b"\n")
        if nl >= 0:
            return bytes(buf[: nl + 1])
    raise ProcmanError("shim closed the socket before a full frame")


def _peer_pid(path: Path) -> int:
    """Pid of the process that accepted on ``path`` (``SO_PEERCRED``)."""
    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as sock:
        sock.settimeout(_CLIENT_TIMEOUT_S)
        sock.connect(os.fspath(path))
        size = struct.calcsize("iii")
        pid, _uid, _gid = struct.unpack(
            "iii",
            sock.getsockopt(socket.SOL_SOCKET, socket.SO_PEERCRED, size),
        )
    if pid <= 0:
        raise ProcmanError(f"socket peer pid is {pid}")
    return pid


def _read_witness_pid(proc: subprocess.Popen[bytes], deadline: float) -> int:
    stdout = proc.stdout
    if stdout is None:
        raise ProcmanError("shim intermediate has no stdout")
    remaining = max(0.0, deadline - time.monotonic())
    ready, _, _ = select.select([stdout], [], [], remaining)
    if not ready:
        raise ProcmanError(f"shim did not report its pid ({_stderr_text(proc)})")
    line = stdout.readline()
    if not line:
        raise ProcmanError(f"shim exited before forking ({_stderr_text(proc)})")
    try:
        pid = int(line)
    except ValueError as exc:
        raise ProcmanError(f"shim pid line {line!r}") from exc
    if pid <= 1:
        raise ProcmanError(f"shim pid {pid}")
    return pid


def _wait_for_peer(path: Path, deadline: float) -> int:
    last = "socket was not created"
    while time.monotonic() < deadline:
        if path.exists() or path.is_symlink():
            try:
                ShimClient(path).status()
                return _peer_pid(path)
            except (OSError, ProcmanError, MessageError) as exc:
                last = str(exc)
        time.sleep(0.01)
    raise ProcmanError(f"shim did not answer status ({last})")


def _stderr_text(proc: subprocess.Popen[bytes]) -> str:
    stderr = proc.stderr
    if stderr is None:
        return ""
    os.set_blocking(stderr.fileno(), False)
    try:
        data = stderr.read() or b""
    except BlockingIOError:
        data = b""
    return data.decode("utf-8", errors="replace").strip()


def _close_captured(proc: subprocess.Popen[bytes]) -> None:
    if proc.stdout is not None:
        proc.stdout.close()
    if proc.stderr is not None:
        proc.stderr.close()


def _abort(
    proc: subprocess.Popen[bytes],
    witness: int | None,
    work_dir: Path,
    worker_id: str,
) -> None:
    live = _read_live(_live_path(work_dir, worker_id))
    if live is not None:
        _kill(live[0])
    if witness is not None:
        _kill_group(witness)
    if proc.poll() is None:
        proc.kill()
    try:
        proc.wait(timeout=2)
    except subprocess.TimeoutExpired:
        pass


def _kill(pid: int) -> None:
    if pid <= 1:
        return
    try:
        os.kill(pid, signal.SIGKILL)
    except OSError:
        pass


def _kill_group(pid: int) -> None:
    if pid <= 1:
        return
    try:
        os.killpg(pid, signal.SIGKILL)
    except OSError:
        _kill(pid)


def _live_path(work_dir: Path, worker_id: str) -> Path:
    """Pid and ``ready`` the subreaper reads if the shim cannot."""
    final = exit_record_path(work_dir, worker_id)
    return final.with_name(final.name.removesuffix(".exit.json") + ".live.json")


def _write_live(path: Path, pid: int, ready: bool) -> None:
    payload = json.dumps({"pid": pid, "ready": ready}, sort_keys=True).encode()
    tmp = path.with_name(path.name + ".tmp")
    tmp.write_bytes(payload + b"\n")
    os.replace(tmp, path)


def _read_live(path: Path) -> tuple[int, bool] | None:
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
        pid = raw["pid"]
        ready = raw["ready"]
    except (OSError, json.JSONDecodeError, KeyError, TypeError):
        return None
    if type(pid) is bool or not isinstance(pid, int) or pid <= 0:
        return None
    if type(ready) is not bool:
        return None
    return pid, ready


def _publish_exit(
    work_dir: Path,
    spec: WorkerSpec,
    *,
    pid: int,
    exit_code: int | None,
    sig: int | None,
    ready: bool,
) -> None:
    record = ExitRecord(
        id=spec.id,
        incarnation=spec.incarnation,
        pid=pid,
        exit_code=exit_code,
        signal=sig,
        ready=ready,
    )
    tmp = exit_record_tmp_path(work_dir, spec.id)
    final = exit_record_path(work_dir, spec.id)
    with tmp.open("wb") as handle:
        handle.write(encode_exit(record))
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(tmp, final)
    _live_path(work_dir, spec.id).unlink(missing_ok=True)


def _unlink_socket(work_dir: Path, worker_id: str) -> None:
    path = socket_path(work_dir, worker_id)
    try:
        if path.is_symlink():
            target = Path(os.readlink(path))
            path.unlink(missing_ok=True)
            target.unlink(missing_ok=True)
            try:
                target.parent.rmdir()
            except OSError:
                pass
            return
        path.unlink(missing_ok=True)
    except OSError:
        pass


def _decode_wait(status: int) -> tuple[int | None, int | None]:
    if os.WIFEXITED(status):
        return os.WEXITSTATUS(status), None
    if os.WIFSIGNALED(status):
        return None, os.WTERMSIG(status)
    return None, None


def _witness(work_dir: Path, spec: WorkerSpec, shim_pid: int) -> None:
    """Adopt the worker if the shim is killed before it can write the record.

    S3 says the shim writes ``<id>.exit.json`` after it reaps the worker.
    S2 kills that shim with ``SIGKILL`` and still requires the file. This
    process is the shim's parent and a child subreaper, so the worker is
    reparented here and the exit status is still observable.
    """
    reaped: dict[int, tuple[int | None, int | None]] = {}
    shim_dead = False
    while True:
        try:
            wpid, status = os.waitpid(-1, 0)
        except InterruptedError:
            continue
        except ChildProcessError:
            break
        if wpid == shim_pid:
            shim_dead = True
        else:
            reaped[wpid] = _decode_wait(status)
        if not shim_dead:
            continue
        if exit_record_path(work_dir, spec.id).exists():
            break
        live = _read_live(_live_path(work_dir, spec.id))
        if live is None:
            break
        worker_pid, ready = live
        if worker_pid not in reaped:
            continue
        code, sig = reaped[worker_pid]
        if code is None and sig is None:
            continue
        try:
            _publish_exit(
                work_dir,
                spec,
                pid=worker_pid,
                exit_code=code,
                sig=sig,
                ready=ready,
            )
        except (OSError, MessageError):
            pass
        break
    _live_path(work_dir, spec.id).unlink(missing_ok=True)
    _unlink_socket(work_dir, spec.id)


class _Rotating:
    """Append-only log with a fixed size and a fixed number of backups (S4)."""

    def __init__(self, path: Path) -> None:
        self.path = path
        self._size = 0
        self._fh = self._open()

    def write(self, data: bytes) -> None:
        if not data:
            return
        if self._size > 0 and self._size + len(data) > LOG_MAX_BYTES:
            self._rotate()
        self._fh.write(data)
        self._size += len(data)

    def _open(self) -> object:
        handle = self.path.open("ab", buffering=0)
        self._size = self.path.stat().st_size
        return handle

    def _rotate(self) -> None:
        self._fh.close()
        oldest = _backup(self.path, LOG_BACKUP_COUNT)
        oldest.unlink(missing_ok=True)
        for index in range(LOG_BACKUP_COUNT - 1, 0, -1):
            src = _backup(self.path, index)
            if src.exists():
                os.replace(src, _backup(self.path, index + 1))
        os.replace(self.path, _backup(self.path, 1))
        self._fh = self._open()


def _backup(path: Path, index: int) -> Path:
    return path.with_name(path.name + f".{index}")


def _foreign_loaded() -> bool:
    names = sys.modules
    return any(
        name == banned or name.startswith(banned + ".")
        for name in names
        for banned in ("pydantic", "nats")
    )


def _boot_line() -> bytes:
    def flag(banned: str) -> str:
        loaded = banned in sys.modules or any(
            name.startswith(banned + ".") for name in sys.modules
        )
        return "1" if loaded else "0"

    return f"boot pydantic={flag('pydantic')} nats={flag('nats')}\n".encode()


@dataclass
class _WorkerState:
    pid: int | None = None
    ready: bool = False
    exit_code: int | None = None
    signal: int | None = None
    released: bool = False
    record_written: bool = False

    @property
    def alive(self) -> bool:
        return (
            self.pid is not None
            and self.exit_code is None
            and self.signal is None
        )


class _Flags:
    term = False


def _on_signal(signum: int, _frame: object) -> None:
    if signum == signal.SIGTERM:
        _Flags.term = True


@dataclass
class _Client:
    sock: socket.socket
    buf: bytearray
    watch: bool = False


class _Server:
    def __init__(
        self,
        work_dir: Path,
        spec: WorkerSpec,
        stderr: _Rotating,
    ) -> None:
        self.work_dir = work_dir
        self.spec = spec
        self.stderr = stderr
        self.stdout = _Rotating(log_path(work_dir, spec.id, "stdout"))
        self.status_log = _Rotating(log_path(work_dir, spec.id, "status"))
        self.state = _WorkerState()
        self.status_buf = bytearray()
        self.out_r, out_w = _pipe()
        self.err_r, err_w = _pipe()
        self.st_r, st_w = _pipe()
        self._pipe_fds = {self.out_r, self.err_r, self.st_r}
        self.server = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self.server.set_inheritable(False)
        _bind_socket(self.server, socket_path(work_dir, spec.id))
        self.server.listen(16)
        self.server.setblocking(False)
        self.clients: dict[int, _Client] = {}
        self.watchers: list[socket.socket] = []
        self.poller = select.poll()
        worker = os.fork()
        if worker == 0:
            _exec_worker(spec, out_w, err_w, st_w)
            os._exit(127)
        os.close(out_w)
        os.close(err_w)
        os.close(st_w)
        try:
            os.setpgid(worker, worker)
        except OSError:
            pass
        self.state.pid = worker
        _write_live(_live_path(work_dir, spec.id), worker, False)
        wake_r, wake_w = os.pipe()
        os.set_blocking(wake_r, False)
        os.set_blocking(wake_w, False)
        os.set_inheritable(wake_r, False)
        os.set_inheritable(wake_w, False)
        signal.set_wakeup_fd(wake_w)
        signal.signal(signal.SIGTERM, _on_signal)
        signal.signal(signal.SIGCHLD, _on_signal)
        signal.signal(signal.SIGPIPE, signal.SIG_IGN)
        self.wake_r = wake_r
        self.poller.register(wake_r, select.POLLIN)
        self.poller.register(self.out_r, select.POLLIN)
        self.poller.register(self.err_r, select.POLLIN)
        self.poller.register(self.st_r, select.POLLIN)
        self.poller.register(self.server, select.POLLIN)

    def serve(self) -> None:
        try:
            while not (self.state.released and self.state.record_written):
                self.poller.poll(1000)
                self._reap()
                if _Flags.term:
                    _Flags.term = False
                    self._forward(signal.SIGTERM)
                self._drain_wake()
                self._drain_pipes()
                self._accept()
                self._read_clients()
                self._reap()
        finally:
            self._close()

    def _current(self) -> ShimStatus:
        # B3-04 fills rss_bytes from the worker's process tree. Until then
        # the field stays None, including after the worker has been reaped.
        return ShimStatus(
            id=self.spec.id,
            incarnation=self.spec.incarnation,
            pid=self.state.pid,
            ready=self.state.ready,
            exit_code=self.state.exit_code,
            signal=self.state.signal,
            rss_bytes=None,
        )

    def _reap(self) -> None:
        while True:
            try:
                wpid, status = os.waitpid(-1, os.WNOHANG)
            except ChildProcessError:
                return
            if wpid == 0:
                return
            if wpid != self.state.pid:
                continue
            code, sig = _decode_wait(status)
            if code is None and sig is None:
                continue
            self.state.exit_code = code
            self.state.signal = sig
            if not self.state.record_written:
                _publish_exit(
                    self.work_dir,
                    self.spec,
                    pid=wpid,
                    exit_code=code,
                    sig=sig,
                    ready=self.state.ready,
                )
                self.state.record_written = True
            self._fanout()

    def _forward(self, sig: int) -> None:
        # S7: the shim forwards the signal and does not exit. Restart is
        # the supervisor's decision, and this process does not make it.
        if not self.state.alive or self.state.pid is None:
            return
        try:
            os.killpg(self.state.pid, sig)
        except OSError:
            pass

    def _note_ready(self, ready: bool) -> None:
        if ready == self.state.ready or not self.state.alive:
            return
        self.state.ready = ready
        if self.state.pid is not None:
            _write_live(
                _live_path(self.work_dir, self.spec.id),
                self.state.pid,
                ready,
            )
        self._fanout()

    def _drain_wake(self) -> None:
        while True:
            try:
                chunk = os.read(self.wake_r, 256)
            except BlockingIOError:
                return
            if not chunk:
                return

    def _drain_pipes(self) -> None:
        streams = (
            (self.out_r, self.stdout),
            (self.err_r, self.stderr),
        )
        for fd, sink in streams:
            if fd in self._pipe_fds and not self._drain_stream(fd, sink):
                self._drop_fd(fd)
        if self.st_r in self._pipe_fds and not self._drain_status():
            self._drop_fd(self.st_r)

    def _drain_stream(self, fd: int, sink: _Rotating) -> bool:
        while True:
            try:
                chunk = os.read(fd, 65536)
            except BlockingIOError:
                return True
            if not chunk:
                return False
            sink.write(chunk)

    def _drain_status(self) -> bool:
        while True:
            try:
                chunk = os.read(self.st_r, 65536)
            except BlockingIOError:
                break
            if not chunk:
                return False
            self.status_buf += chunk
            while True:
                nl = self.status_buf.find(b"\n")
                if nl < 0:
                    break
                line = bytes(self.status_buf[: nl + 1])
                del self.status_buf[: nl + 1]
                self.status_log.write(line)
                self._apply_heartbeat(line)
        if len(self.status_buf) > PIPE_BUF:
            del self.status_buf[:]
        return True

    def _apply_heartbeat(self, line: bytes) -> None:
        # A torn or over-long frame is dropped. The next heartbeat is a
        # full snapshot, so the previous ``ready`` stands (S6).
        if len(line) > PIPE_BUF:
            return
        try:
            beat = decode_heartbeat(line)
        except MessageError:
            return
        self._note_ready(beat.ready)

    def _accept(self) -> None:
        while True:
            try:
                conn, _addr = self.server.accept()
            except BlockingIOError:
                return
            conn.setblocking(False)
            self.poller.register(conn, select.POLLIN)
            self.clients[conn.fileno()] = _Client(sock=conn, buf=bytearray())

    def _read_clients(self) -> None:
        for fd, client in list(self.clients.items()):
            if client.watch:
                # A closed watcher is POLLHUP. Leaving it registered makes
                # poll return immediately and the loop spins.
                self._probe_watch(fd, client)
                continue
            try:
                chunk = client.sock.recv(4096)
            except BlockingIOError:
                continue
            except OSError:
                self._drop_client(fd)
                continue
            if not chunk:
                self._drop_client(fd)
                continue
            client.buf += chunk
            nl = client.buf.find(b"\n")
            if nl < 0:
                if len(client.buf) > PIPE_BUF:
                    self._drop_client(fd)
                continue
            line = bytes(client.buf[: nl + 1])
            try:
                command = decode_command(line)
            except MessageError:
                self._drop_client(fd)
                continue
            self._dispatch(fd, client, command)

    def _dispatch(self, fd: int, client: _Client, command: object) -> None:
        # A heartbeat can land in the pipe before this command is read.
        # Drain first so ``status`` and the first ``watch`` line see it.
        self._drain_pipes()
        self._reap()
        if isinstance(command, StatusQuery):
            self._reply(fd, client, encode_status(self._current()))
            return
        if isinstance(command, SignalCommand):
            self._forward(command.signal)
            self._reply(fd, client, encode_status(self._current()))
            return
        if isinstance(command, ReleaseCommand):
            self.state.released = True
            self._drop_client(fd)
            return
        if isinstance(command, WatchCommand):
            client.watch = True
            if not self._send(client.sock, encode_status(self._current())):
                self._drop_client(fd)
                return
            self.watchers.append(client.sock)
            return
        self._drop_client(fd)

    def _reply(self, fd: int, client: _Client, frame: bytes) -> None:
        self._send(client.sock, frame)
        self._drop_client(fd)

    def _send(self, sock: socket.socket, frame: bytes) -> bool:
        view = memoryview(frame)
        while view:
            try:
                sent = sock.send(view)
            except (BlockingIOError, OSError):
                return False
            view = view[sent:]
        return True

    def _probe_watch(self, fd: int, client: _Client) -> None:
        try:
            chunk = client.sock.recv(1, socket.MSG_PEEK)
        except BlockingIOError:
            return
        except OSError:
            self._drop_client(fd)
            return
        if not chunk:
            self._drop_client(fd)

    def _fanout(self) -> None:
        if not self.watchers:
            return
        frame = encode_status(self._current())
        dead: list[socket.socket] = []
        for sock in self.watchers:
            if not self._send(sock, frame):
                dead.append(sock)
        for sock in dead:
            self._drop_sock(sock)

    def _drop_client(self, fd: int) -> None:
        client = self.clients.pop(fd, None)
        if client is None:
            return
        self._close_sock(client.sock)

    def _drop_sock(self, sock: socket.socket) -> None:
        for fd, client in list(self.clients.items()):
            if client.sock is sock:
                self.clients.pop(fd, None)
                break
        self._close_sock(sock)

    def _close_sock(self, sock: socket.socket) -> None:
        if sock in self.watchers:
            self.watchers.remove(sock)
        try:
            self.poller.unregister(sock)
        except (KeyError, OSError, ValueError):
            pass
        try:
            sock.close()
        except OSError:
            pass

    def _drop_fd(self, fd: int) -> None:
        self._pipe_fds.discard(fd)
        try:
            self.poller.unregister(fd)
        except (KeyError, OSError, ValueError):
            pass
        try:
            os.close(fd)
        except OSError:
            pass

    def _close(self) -> None:
        for fd in list(self.clients):
            self._drop_client(fd)
        try:
            self.server.close()
        except OSError:
            pass
        _unlink_socket(self.work_dir, self.spec.id)
        for fd in (self.out_r, self.err_r, self.st_r, self.wake_r):
            try:
                os.close(fd)
            except OSError:
                pass


def _serve(work_dir: Path, spec: WorkerSpec) -> None:
    _prctl(_PR_SET_CHILD_SUBREAPER, 1)
    _write_oom_soft(SHIM_OOM_SCORE_ADJ)
    run_dir(work_dir).mkdir(parents=True, exist_ok=True)
    log_path(work_dir, spec.id, "stderr").parent.mkdir(parents=True, exist_ok=True)
    stderr = _Rotating(log_path(work_dir, spec.id, "stderr"))
    stderr.write(_boot_line())
    if _foreign_loaded():
        stderr.write(b"refusing to run with pydantic or nats loaded\n")
        os._exit(1)
    try:
        _Server(work_dir, spec, stderr).serve()
    except Exception:
        stderr.write(traceback.format_exc().encode())
        os._exit(1)
    os._exit(0)


def _write_oom_soft(value: int) -> None:
    path = Path("/proc/self/oom_score_adj")
    try:
        current = int(path.read_text().strip())
    except OSError:
        return
    if current == value:
        return
    try:
        path.write_text(f"{value}\n")
    except OSError:
        return


def _pipe() -> tuple[int, int]:
    read, write = os.pipe()
    os.set_blocking(read, False)
    os.set_inheritable(read, False)
    os.set_inheritable(write, False)
    return read, write


def _bind_socket(sock: socket.socket, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.is_symlink() or path.exists():
        path.unlink()
    if len(os.fsencode(path)) <= _UNIX_PATH_MAX:
        sock.bind(os.fspath(path))
        os.chmod(path, 0o600)
        return
    # The canonical path does not fit in sockaddr_un. Bind a short name
    # and point the canonical path at it. ``connect`` follows the link,
    # and ``Path.is_socket`` does too.
    short_dir = Path(tempfile.mkdtemp(prefix="mftik-shim-"))
    short = short_dir / "s"
    sock.bind(os.fspath(short))
    os.chmod(short, 0o600)
    path.symlink_to(short)


def _exec_worker(spec: WorkerSpec, out_w: int, err_w: int, st_w: int) -> None:
    try:
        os.setpgid(0, 0)
        parent = os.getppid()
        _prctl(_PR_SET_PDEATHSIG, signal.SIGTERM)
        if os.getppid() != parent:
            os.kill(os.getpid(), signal.SIGTERM)
            os._exit(1)
        Path("/proc/self/oom_score_adj").write_text(f"{spec.oom_score_adj}\n")
        if spec.rlimit_data_bytes is not None:
            limit = spec.rlimit_data_bytes
            resource.setrlimit(resource.RLIMIT_DATA, (limit, limit))
        null = os.open(os.devnull, os.O_RDONLY)
        os.dup2(null, 0)
        os.dup2(out_w, 1)
        os.dup2(err_w, 2)
        os.dup2(st_w, _STATUS_FD)
        for fd in (0, 1, 2, _STATUS_FD):
            os.set_inheritable(fd, True)
        if null > _STATUS_FD:
            os.close(null)
        _close_other_fds({0, 1, 2, _STATUS_FD})
        env = dict(spec.env)
        env[STATUS_FD_ENV] = str(_STATUS_FD)
        os.execvpe(spec.argv[0], list(spec.argv), env)
    except OSError:
        os._exit(127)


def _close_other_fds(keep: set[int]) -> None:
    for name in os.listdir("/proc/self/fd"):
        fd = int(name)
        if fd in keep:
            continue
        try:
            os.close(fd)
        except OSError:
            pass
