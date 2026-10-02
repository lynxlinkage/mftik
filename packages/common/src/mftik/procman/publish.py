"""Publish ``procman.report.{plane}.{instance}`` for one supervisor.

The Supervisor has no NATS client and does not grow one (B3-04). The
plane's orchestrator runs :func:`publish_reports` and passes the
callable that puts an envelope on the broker. Wiring that loop into the
STS, MD and TD controllers is B4 / B5.

``generation`` increases by one per publication inside
:meth:`~mftik.procman.Supervisor.report` and is not durable. While
publication is paused the loop sends nothing. An empty ``workers`` list
is only sent when reports are open: it means no live slot, not "paused"
(P5, F32).
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Sequence

from mftik.clock import Clock
from mftik.procman.errors import ProcmanError
from mftik.procman.supervisor import Supervisor
from mftik.protocol.messages import PROCMAN_REPORT
from mftik.protocol.topics import Topics
from mftik.protocol.v2 import (
    ProcmanReport,
    ProcmanReportEnvelope,
    ProcmanWorker,
)

#: How often an orchestrator publishes ``procman.report`` (§8.2).
#:
#: :func:`publish_reports` sleeps this long between publications. The
#: first publication goes out as soon as reports are open, then one per
#: period. A new process starts the generation count again.
REPORT_PERIOD_S = 5.0

#: ``publish(subject, envelope)``. The orchestrator owns the broker.
Publish = Callable[[str, ProcmanReportEnvelope], Awaitable[None]]

#: Extra workers the orchestrator adds to one publication. STS uses this
#: for a session in ``restarting`` that has no live process (R4). The
#: callable is synchronous: the ids are already in memory. Entries must
#: not repeat an id the supervisor already listed.
ExtraWorkers = Callable[[], Sequence[ProcmanWorker]]


def _with_extras(
    report: ProcmanReport, extras: Sequence[ProcmanWorker]
) -> ProcmanReport:
    if not extras:
        return report
    return ProcmanReport(
        generation=report.generation,
        workers=[*report.workers, *extras],
    )


async def publish_reports(
    supervisor: Supervisor,
    *,
    plane: str,
    instance: str,
    publish: Publish,
    clock: Clock,
    extra_workers: ExtraWorkers | None = None,
) -> None:
    """Publish :meth:`Supervisor.report` every :data:`REPORT_PERIOD_S`.

    ``plane`` and ``instance`` are the subject
    (:meth:`~mftik.protocol.topics.Topics.procman_report`). They have to
    be the supervisor's own: the report describes that process.

    The loop sends nothing until :meth:`Supervisor.allow_reports` (B3-03
    calls that when ``start`` has finished reconciling) and returns when
    :meth:`Supervisor.pause_reports` runs (``close``) or when this task
    is cancelled. A pause is the absence of a message. It is never an
    empty report.

    ``extra_workers``, when given, is called once per publication and
    its entries are appended. That is how an orchestrator keeps a
    restarting session on the subject after the process is gone (R4).
    This function does not know about sessions.
    """
    if plane != supervisor.plane or instance != supervisor.instance:
        raise ProcmanError(
            f"report subject {plane}.{instance} does not match "
            f"supervisor {supervisor.plane}.{supervisor.instance}"
        )
    subject = Topics.procman_report(plane, instance)
    while True:
        if supervisor.reports_closed():
            return
        if supervisor.reports_open():
            try:
                report = await supervisor.report()
            except ProcmanError:
                if not supervisor.reports_open():
                    return
                raise
            if not supervisor.reports_open():
                return
            if extra_workers is not None:
                report = _with_extras(report, extra_workers())
            envelope = ProcmanReportEnvelope.wrap(
                report, type=PROCMAN_REPORT, source=plane
            )
            if not supervisor.reports_open():
                return
            await publish(subject, envelope)
        await clock.sleep(REPORT_PERIOD_S)
