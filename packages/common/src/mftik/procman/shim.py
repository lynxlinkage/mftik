"""The shim process, as an interface.

B3-01 fills this in with a double-fork and ``setsid``: a short-lived
intermediate started by ``subprocess.Popen`` (never
``asyncio.create_subprocess_exec``, §4.1), and a shim that is not a child
of the supervisor. The shim uses the standard library only (F29). It
applies ``oom_score_adj`` and ``RLIMIT_DATA`` between fork and exec (§4.7),
holds the worker's stdio, writes ``<id>.exit.json``, and speaks the NDJSON
socket in :mod:`mftik.procman.messages`.

Importing :mod:`mftik` runs ``mftik/__init__.py``, which pulls the broker.
The shim process must not pay that cost. B3-01 launches this entry without
going through that import; the message types it needs are stdlib-only.
"""

from __future__ import annotations

from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path

from mftik.procman._ticket import TICKET
from mftik.procman.messages import ShimStatus
from mftik.procman.spec import WorkerSpec


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
        raise NotImplementedError(TICKET)

    def signal(self, sig: int) -> None:
        del sig
        raise NotImplementedError(TICKET)

    def watch(self) -> Iterator[ShimStatus]:
        raise NotImplementedError(TICKET)

    def release(self) -> None:
        raise NotImplementedError(TICKET)


def spawn_shim(spec: WorkerSpec, *, work_dir: Path) -> SpawnedShim:
    """Double-fork a shim for ``spec`` and return once ``status`` answers.

    The worker's argv is ``spec.argv``. ``spec.env`` is the worker's
    environment; the shim adds :data:`~mftik.procman.messages.STATUS_FD_ENV`
    and does not drop the rest. ``spec.oom_score_adj`` and
    ``spec.rlimit_data_bytes`` are applied in the child before exec.
    """
    del spec, work_dir
    raise NotImplementedError(TICKET)


def main(argv: list[str] | None = None) -> None:
    """Process entry for one shim. B3-01 replaces this body (F29)."""
    del argv
    raise NotImplementedError(TICKET)
