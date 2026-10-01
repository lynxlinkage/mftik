"""Payload of ``procman.report.{plane}.{instance}``.

The supervisor is the authority for the set of workers it still holds on
this instance (§3.3). The report is that set, published and not persisted.
``generation`` increases by one each time this supervisor publishes, so a
consumer can tell two successive reports apart (§8.2). It is not durable:
a new process starts again.

The list is every worker the supervisor has not released, including one in
``BACKOFF``. A restarting worker is therefore still present. Workers that
were never spawned are absent; putting those on the report belongs to the
plane's orchestrator.

While publication is stopped the absence is not an observation. Consumers
reclaim nothing (F32, P7).

``code_ref`` on each worker is the release version of the controller that
spawned the worker (§4.5), copied from the spec. ``rss_bytes`` is that
worker's process tree, not the shim (§4.7), and ``None`` when there is no
process to measure (``BACKOFF`` before the next spawn).

The NATS subject helper and the envelope's ``pv`` are the protocol's
(IF-01). This module is the body.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass

from mftik.instance import validate_instance_name
from mftik.procman.errors import MessageError
from mftik.procman.messages import dump_frame, load_frame
from mftik.procman.spec import PLANES, Plane, validate_worker_id
from mftik.procman.state import WorkerPhase


def report_subject(plane: str, instance: str) -> str:
    """``procman.report.{plane}.{instance}`` (§3.3, §8.2)."""
    if plane not in PLANES:
        raise MessageError(f"plane {plane!r} is not one of {', '.join(PLANES)}")
    _instance(instance)
    return f"procman.report.{plane}.{instance}"


def _instance(instance: str) -> str:
    try:
        canonical = validate_instance_name(instance)
    except ValueError as exc:
        raise MessageError(str(exc)) from exc
    if canonical != instance:
        raise MessageError("instance must already be the canonical name")
    return canonical


@dataclass(frozen=True)
class ReportedWorker:
    """One worker in a :class:`ProcmanReport`.

    ``code_ref`` is copied from :attr:`WorkerSpec.code_ref`.
    """

    id: str
    incarnation: int
    phase: WorkerPhase
    code_ref: str
    rss_bytes: int | None
    ready: bool

    def __post_init__(self) -> None:
        object.__setattr__(self, "id", validate_worker_id(self.id))
        if type(self.incarnation) is not int or self.incarnation < 0:
            raise MessageError("incarnation must be an int >= 0")
        try:
            phase = WorkerPhase(self.phase)
        except ValueError as exc:
            raise MessageError(f"unknown phase {self.phase!r}") from exc
        object.__setattr__(self, "phase", phase)
        if not isinstance(self.code_ref, str):
            raise MessageError("code_ref must be a string")
        if self.rss_bytes is not None and (
            type(self.rss_bytes) is not int or self.rss_bytes < 0
        ):
            raise MessageError("rss_bytes must be an int >= 0 or None")
        if type(self.ready) is not bool:
            raise MessageError("ready must be a boolean")


@dataclass(frozen=True)
class ProcmanReport:
    """One publication of the supervisor's worker set."""

    plane: Plane
    instance: str
    generation: int
    workers: tuple[ReportedWorker, ...] = ()

    def __post_init__(self) -> None:
        if self.plane not in PLANES:
            raise MessageError(
                f"plane {self.plane!r} is not one of {', '.join(PLANES)}"
            )
        object.__setattr__(self, "instance", _instance(self.instance))
        if type(self.generation) is not int or self.generation < 0:
            raise MessageError("generation must be an int >= 0")
        if isinstance(self.workers, str) or not isinstance(self.workers, Sequence):
            raise MessageError("workers must be a sequence of ReportedWorker")
        workers = tuple(self.workers)
        if not all(isinstance(worker, ReportedWorker) for worker in workers):
            raise MessageError("workers must be a sequence of ReportedWorker")
        ids = [worker.id for worker in workers]
        if len(ids) != len(set(ids)):
            raise MessageError("report lists a worker id twice")
        object.__setattr__(self, "workers", workers)


def encode_report(report: ProcmanReport) -> bytes:
    """The payload as one JSON object and a newline."""
    if not isinstance(report, ProcmanReport):
        raise MessageError("report must be a ProcmanReport")
    return dump_frame(
        {
            "plane": report.plane,
            "instance": report.instance,
            "generation": report.generation,
            "workers": [
                {
                    "id": worker.id,
                    "incarnation": worker.incarnation,
                    "phase": worker.phase.value,
                    "code_ref": worker.code_ref,
                    "rss_bytes": worker.rss_bytes,
                    "ready": worker.ready,
                }
                for worker in report.workers
            ],
        }
    )


def decode_report(line: bytes) -> ProcmanReport:
    """The inverse of :func:`encode_report`."""
    payload = load_frame(line)
    expected = {"plane", "instance", "generation", "workers"}
    if set(payload) != expected:
        raise MessageError(f"report keys {sorted(payload)} != {sorted(expected)}")
    raw_workers = payload["workers"]
    if not isinstance(raw_workers, list):
        raise MessageError("workers must be a list")
    workers: list[ReportedWorker] = []
    for item in raw_workers:
        if not isinstance(item, dict):
            raise MessageError("each worker must be an object")
        keys = {"id", "incarnation", "phase", "code_ref", "rss_bytes", "ready"}
        if set(item) != keys:
            raise MessageError(f"worker keys {sorted(item)} != {sorted(keys)}")
        try:
            phase = WorkerPhase(item["phase"])
        except ValueError as exc:
            raise MessageError(f"unknown phase {item['phase']!r}") from exc
        workers.append(
            ReportedWorker(
                id=item["id"],
                incarnation=item["incarnation"],
                phase=phase,
                code_ref=item["code_ref"],
                rss_bytes=item["rss_bytes"],
                ready=item["ready"],
            )
        )
    return ProcmanReport(
        plane=payload["plane"],
        instance=payload["instance"],
        generation=payload["generation"],
        workers=tuple(workers),
    )
