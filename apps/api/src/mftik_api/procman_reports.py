"""Latest ``procman.report`` from every plane, for ``GET /workers``.

The API subscribes to :meth:`mftik.protocol.Topics.procman_report_pattern`
and keeps the newest :class:`~mftik.protocol.ProcmanReport` per
``(plane, instance)``, plus the monotonic time it arrived. The route
reads :meth:`ProcmanReportStore.rows`. Nothing here is written to
Postgres. The report is not durable (§3.3).

A plane that stops publishing stays in the store. Pausing is the absence
of a report, not an empty one, and a reader that treats absence as "no
workers" would reclaim intents (F32). The row's ``age_s`` is how long
ago that report arrived. An empty ``workers`` list is different: the
plane did publish, and it said nothing is live. That replaces the
previous list. The instance is still held.

``generation`` is not how "latest" is chosen. A new supervisor process
starts the counter again at 1, so a generation of 1 can be newer than a
generation of 100. The message that arrived last is the latest.

Code identity on this surface is ``code_ref`` only. Comparing a session's
strategy digest to the current tree is F39 and belongs to B5-10.
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass

from mftik.broker import Broker
from mftik.clock import Clock, SystemClock
from mftik.protocol import (
    PROCMAN_REPORT,
    PROTOCOL_VERSION,
    ProcmanReport,
    Topics,
    UntypedEnvelope,
)
from pydantic import ValidationError

logger = logging.getLogger("mftik_api.procman_reports")

_SUBJECT_PREFIX = "procman.report."


def parse_report_subject(subject: str) -> tuple[str, str] | None:
    """``procman.report.{plane}.{instance}``, or ``None``.

    Both tokens are one subject segment: non-empty and free of ``.``,
    the same rule as :meth:`Topics.procman_report`.
    """
    if not subject.startswith(_SUBJECT_PREFIX):
        return None
    rest = subject[len(_SUBJECT_PREFIX) :]
    plane, dot, instance = rest.partition(".")
    if not dot or not plane or not instance:
        return None
    if "." in plane or "." in instance:
        return None
    return plane, instance


@dataclass(frozen=True, slots=True)
class WorkerRow:
    """One worker on ``GET /workers``.

    ``age_s`` is the age of the report this worker was copied from, not
    the age of the process. Every worker in one report shares it.
    """

    plane: str
    instance: str
    id: str
    incarnation: int
    phase: str
    ready: bool
    code_ref: str
    rss_bytes: int | None
    age_s: float


@dataclass(frozen=True, slots=True)
class _Held:
    report: ProcmanReport
    arrived_s: float


class ProcmanReportStore:
    """The latest report per ``(plane, instance)``.

    :meth:`note` takes a decoded envelope. Tests call it directly and do
    not open a NATS connection (F31). :func:`run_procman_reports` is the
    subscriber that feeds this store in the API process.
    """

    def __init__(self, clock: Clock | None = None) -> None:
        self._clock: Clock = clock if clock is not None else SystemClock()
        self._reports: dict[tuple[str, str], _Held] = {}

    def note(self, subject: str, envelope: UntypedEnvelope) -> bool:
        """Store ``envelope`` when it is a report for ``subject``.

        A subject that is not a procman report, a ``pv`` that is not
        :data:`~mftik.protocol.PROTOCOL_VERSION`, a different envelope
        type, or a payload that is not a :class:`ProcmanReport` is
        ignored. The previous report for that instance stays. Returns
        whether this call replaced one.
        """
        parsed = parse_report_subject(subject)
        if parsed is None:
            return False
        # The broker has already decoded the frame. ``pv`` is still
        # checked before the payload is read (F26). A missing ``pv``
        # cannot be told apart here: ``from_json`` fills the default.
        if envelope.pv != PROTOCOL_VERSION:
            logger.debug("ignoring %s: pv %s", subject, envelope.pv)
            return False
        if envelope.type != PROCMAN_REPORT:
            logger.debug("ignoring %s: type %s", subject, envelope.type)
            return False
        payload = envelope.payload
        if not isinstance(payload, dict):
            logger.debug("ignoring %s: payload is not an object", subject)
            return False
        try:
            report = ProcmanReport.model_validate(payload)
        except ValidationError:
            logger.debug("ignoring %s: payload is not a procman report", subject)
            return False
        self._reports[parsed] = _Held(
            report=report, arrived_s=self._clock.monotonic()
        )
        return True

    def held_instances(self) -> tuple[tuple[str, str], ...]:
        """``(plane, instance)`` pairs still stored, including an empty report.

        A later :meth:`note` replaces the report. Nothing deletes a pair
        because time passed. Silence leaves the pair where it is (F32).
        """
        return tuple(sorted(self._reports))

    def rows(self) -> list[WorkerRow]:
        """One row per worker in the latest report of each instance.

        Ordered by plane, then instance, then worker id. An instance
        whose latest report lists no workers contributes nothing. The
        instance is still in :meth:`held_instances`. ``age_s`` is never
        negative.
        """
        now = self._clock.monotonic()
        found: list[WorkerRow] = []
        for (plane, instance), held in self._reports.items():
            age = now - held.arrived_s
            if age < 0:
                age = 0.0
            for worker in held.report.workers:
                found.append(
                    WorkerRow(
                        plane=plane,
                        instance=instance,
                        id=worker.id,
                        incarnation=worker.incarnation,
                        phase=worker.phase,
                        ready=worker.ready,
                        code_ref=worker.code_ref,
                        rss_bytes=worker.rss_bytes,
                        age_s=age,
                    )
                )
        found.sort(key=lambda row: (row.plane, row.instance, row.id))
        return found


_store = ProcmanReportStore()


def report_store() -> ProcmanReportStore:
    """The store the route and the subscriber share."""
    return _store


async def run_procman_reports(stop: asyncio.Event) -> None:
    """Subscribe to every ``procman.report.*.*`` until ``stop``.

    A private broker connection, same as the log persist worker. The
    request path does not publish, and a stall in that connection must
    not be what stops this subscription. Messages are applied with
    :meth:`ProcmanReportStore.note`. One bad frame does not end the
    loop: :meth:`note` ignores it.
    """
    store = report_store()
    broker = Broker()
    await broker.connect()
    logger.info("procman report subscriber started")
    try:
        async for topic, envelope in broker.psubscribe(
            Topics.procman_report_pattern(), stop=stop
        ):
            store.note(topic, envelope)
    finally:
        await broker.close()
        logger.info("procman report subscriber stopped")
