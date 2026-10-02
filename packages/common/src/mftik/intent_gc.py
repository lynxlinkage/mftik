"""Release intent owners from STS liveness reports (§8.2 rule 3).

MD and TD share this rule. Neither plane imports the other, so the
pure function lives here and both call it. :func:`mftik_md.controller.gc_owners`
stays importable: it delegates here and turns :class:`IntentGcError`
into that module's :class:`~mftik_md.controller.ControllerError`.

**State authority (§3.3).** This module holds none. The caller holds
the intents, in memory, and the per-instance cursor
(:class:`InstanceGcState`). Intent rows in Postgres are the API's and
the STS controller's. A release here drops an owner from the caller's
held set. It does not write ``released_at``.

**How a report becomes a sample.**

* One call of :func:`on_sts_report` is one STS instance. The subject
  ``procman.report.sts.{instance}`` names the instance. Worker ids
  ``sts/session/<session_id>`` become
  :class:`~mftik.protocol.IntentOwner` values; any phase counts as
  present, and every other id is ignored. Restarting sessions stay
  because the report lists them (R4), not because this module reads
  the word ``restarting``.
* An owner missing from two consecutive samples of their own instance
  is released. The first miss only records them in ``absent``.
* An equal ``generation`` is a replay. It is not a second sample.
* ``generation`` is per publishing process and is not durable (B3-04).
  A value lower than the previous one from that instance is a new
  publisher. :func:`on_sts_report` resets that instance's cursor — no
  previous generation, empty ``absent`` — and then samples, so a
  release needs two consecutive reports from the new process.
  :func:`gc_owners` itself still refuses a backwards generation. The
  reset happens before that call.
* ``report is None``, and a report that never arrives, release nothing
  (F32). The subscription simply does not sample when no message
  comes. :func:`gc_owners` with ``report=None`` is the same rule at
  the function: nobody is released, and the absence streak is left
  where it was. The contract does not say whether a gap should clear
  the streak; leaving it is what "not a sample" already means.
* A report from an instance that holds none of our owners changes
  nothing: no release, and that instance's cursor is not created or
  rewritten. Other instances are untouched.

Standing owners never come through here (C1, C8). The caller does not
pass them.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Callable, Collection, Iterable
from dataclasses import dataclass
from typing import TYPE_CHECKING

from mftik.broker.handler import RESTART_DELAY_SECONDS
from mftik.protocol import (
    PROCMAN_REPORT,
    IntentOwner,
    ProcmanReport,
    ProcmanWorker,
    Topics,
    UntypedEnvelope,
)

if TYPE_CHECKING:
    from mftik.broker import Broker

logger = logging.getLogger(__name__)

#: ``sts/session/<session_id>``. The same spelling
#: :func:`mftik_sts.controller.session_worker_id` produces. This module
#: does not import STS.
_SESSION_WORKER_PREFIX = "sts/session/"

_STS_SUBJECT_PREFIX = "procman.report.sts."


class IntentGcError(Exception):
    """A report this rule will not treat as a sample.

    A generation that went backwards, a report with no generation, a
    stopped report that brought one. The MD wrapper re-raises this as
    :class:`~mftik_md.controller.ControllerError`. A ``bool`` where an
    int belongs is a :class:`TypeError`, not this.
    """


def _generation(name: str, value: object) -> int:
    """A report generation: a non-negative int, and not ``True``."""
    if isinstance(value, bool) or not isinstance(value, int):
        raise TypeError(f"{name} must be an int, got {type(value).__name__}")
    if value < 0:
        raise IntentGcError(f"{name} must be >= 0, got {value}")
    return value


def _owners(value: object, name: str) -> frozenset[IntentOwner]:
    if isinstance(value, str) or not isinstance(value, Collection):
        raise TypeError(f"{name} must be a collection of IntentOwner")
    found: set[IntentOwner] = set()
    for item in value:
        if not isinstance(item, IntentOwner):
            raise TypeError(
                f"{name} must be a collection of IntentOwner, "
                f"got {type(item).__name__}"
            )
        found.add(item)
    return frozenset(found)


@dataclass(frozen=True)
class OwnerGc:
    """What one procman report did to the session owners we hold.

    ``release`` is who to drop now. ``absent`` is who was missing from
    this report and is not yet released, handed back as
    ``previous_absent`` next time. ``generation`` is the report
    generation this result consumed, so a replay is recognisable. A
    stopped report consumed none: ``generation`` is the previous one
    and ``absent`` is unchanged.

    ``release`` is an output. Feeding a previous result's ``release``
    back in is not how the next call learns anything; ``absent`` and
    ``generation`` are.
    """

    release: frozenset[IntentOwner]
    absent: frozenset[IntentOwner]
    generation: int | None


@dataclass(frozen=True)
class InstanceGcState:
    """The cursor for one ``sts_instance``.

    ``previous_generation`` is the last sample from that publisher.
    ``None`` means the next report is the first sample. ``absent`` is
    who that sample missed and who a second miss will release.
    """

    previous_generation: int | None = None
    absent: frozenset[IntentOwner] = frozenset()


def session_id_from_worker_id(worker_id: str) -> str | None:
    """The session id inside ``sts/session/<session_id>``, or None.

    None means "not a session worker". The caller ignores it. A
    trailing extra path segment is not a session id:
    :func:`mftik_sts.controller.session_worker_id` never produces one.
    """
    if not isinstance(worker_id, str) or not worker_id.startswith(
        _SESSION_WORKER_PREFIX
    ):
        return None
    session_id = worker_id[len(_SESSION_WORKER_PREFIX) :]
    if session_id == "" or "/" in session_id:
        return None
    return session_id


def owners_from_worker_ids(
    sts_instance: str, worker_ids: Iterable[str]
) -> frozenset[IntentOwner]:
    """Session owners named by those worker ids. Other ids are skipped."""
    found: set[IntentOwner] = set()
    for worker_id in worker_ids:
        session_id = session_id_from_worker_id(worker_id)
        if session_id is None:
            continue
        found.add(IntentOwner(sts_instance=sts_instance, session_id=session_id))
    return frozenset(found)


def owners_in_report(
    sts_instance: str, workers: Iterable[ProcmanWorker]
) -> frozenset[IntentOwner]:
    """Every session worker the report lists.

    ``phase`` is not read. A session the supervisor included because it
    is ``restarting`` is simply a worker id on the report (R4).
    """
    return owners_from_worker_ids(
        sts_instance, (worker.id for worker in workers)
    )


def sts_report_pattern() -> str:
    """``procman.report.sts.*``, the STS liveness subjects (§8.2)."""
    return Topics.procman_report("sts", "*")


def sts_instance_from_report_subject(subject: str) -> str | None:
    """The instance token of ``procman.report.sts.{instance}``, or None."""
    if not isinstance(subject, str) or not subject.startswith(_STS_SUBJECT_PREFIX):
        return None
    instance = subject[len(_STS_SUBJECT_PREFIX) :]
    if instance == "" or "." in instance:
        return None
    return instance


def gc_owners(
    held: Collection[IntentOwner],
    previous_absent: Collection[IntentOwner],
    previous_generation: int | None,
    *,
    report: Collection[IntentOwner] | None,
    report_generation: int | None,
) -> OwnerGc:
    """Release session owners absent from two consecutive reports (§8.2).

    One call is one instance's report, already turned into
    :class:`~mftik.protocol.IntentOwner` values. ``held`` is the session
    owners we currently have. Standing owners are not passed in.

    * A report whose generation is not newer than ``previous_generation``
      is a replay. It is not a second sample: ``release`` is empty and
      the streak is unchanged.
    * The first report an owner is missing from does not release them.
      They come back in ``absent``.
    * The next newer report they are also missing from releases them.
    * An owner who is in the report is not released, and is not absent,
      even if the previous report missed them.
    * ``report is None`` means the publication stopped. Release nothing
      (F32). It is not a sample: ``absent`` and ``generation`` are the
      previous values, unchanged.

    A generation that goes backwards, or a report that arrives without
    one, is refused. A stopped report has no generation. Callers that
    subscribe reset a lower generation before calling
    (:func:`on_sts_report`), because a new process starts again at 1.
    """
    if report is None:
        if report_generation is not None:
            raise IntentGcError(
                "a stopped report has no generation; "
                "pass report_generation=None"
            )
    elif report_generation is None:
        raise IntentGcError("a report needs its generation")
    else:
        _generation("report_generation", report_generation)
    if previous_generation is not None:
        _generation("previous_generation", previous_generation)
        if (
            report_generation is not None
            and report_generation < previous_generation
        ):
            raise IntentGcError(
                "report generation went backwards; publications only increase"
            )

    held_owners = _owners(held, "held")
    absent_before = _owners(previous_absent, "previous_absent")
    if report is None:
        return OwnerGc(
            release=frozenset(),
            absent=absent_before,
            generation=previous_generation,
        )
    assert report_generation is not None
    if (
        previous_generation is not None
        and report_generation == previous_generation
    ):
        return OwnerGc(
            release=frozenset(),
            absent=absent_before,
            generation=report_generation,
        )

    present = _owners(report, "report")
    missing = held_owners - present
    release = frozenset(owner for owner in missing if owner in absent_before)
    still_absent = frozenset(owner for owner in missing if owner not in release)
    return OwnerGc(
        release=release,
        absent=still_absent,
        generation=report_generation,
    )


def on_sts_report(
    states: dict[str, InstanceGcState],
    *,
    sts_instance: str,
    held: Collection[IntentOwner],
    report: ProcmanReport | None,
) -> frozenset[IntentOwner]:
    """Apply one STS instance's report, or the news that it stopped.

    ``report is None`` releases nothing and does not touch ``states``
    (F32). A generation lower than this instance's cursor is a new
    publisher: the cursor is reset (no previous generation, empty
    ``absent``) and this report is then the first sample, so a release
    needs a second consecutive report from that process. An equal
    generation is a replay and is not a second sample.

    When ``held`` has no owner for ``sts_instance``, the call changes
    nothing. A report from some other STS instance is that case for
    every cursor we already have.

    The returned set is who to drop from ``held``. Dropping them is the
    caller's, and it is the same drop an ``*.intent.delete`` of that
    owner would do.
    """
    if not isinstance(sts_instance, str) or sts_instance == "":
        raise ValueError("sts_instance must be a non-empty string")
    owners = _owners(held, "held")
    if report is None:
        return frozenset()
    if not isinstance(report, ProcmanReport):
        raise TypeError("report must be a ProcmanReport or None")
    mine = frozenset(
        owner for owner in owners if owner.sts_instance == sts_instance
    )
    if not mine:
        # Including a cursor a previous put left behind. The next put of
        # this instance continues that streak; this report is not a sample.
        return frozenset()

    generation = _generation("generation", report.generation)
    state = states.get(sts_instance, InstanceGcState())
    if (
        state.previous_generation is not None
        and generation < state.previous_generation
    ):
        # New process. generation is not durable (B3-04). Two reports
        # from this publisher are required before anyone is released.
        state = InstanceGcState()
    present = owners_in_report(sts_instance, report.workers)
    result = gc_owners(
        mine,
        state.absent,
        state.previous_generation,
        report=present,
        report_generation=generation,
    )
    states[sts_instance] = InstanceGcState(
        previous_generation=result.generation,
        absent=result.absent,
    )
    return result.release


async def watch_sts_reports(
    broker: Broker,
    *,
    held: Callable[[], Collection[IntentOwner]],
    release: Callable[[Collection[IntentOwner]], None],
    stop: asyncio.Event | None = None,
    states: dict[str, InstanceGcState] | None = None,
) -> None:
    """Subscribe to ``procman.report.sts.*`` and release absent owners.

    The generation rule is :func:`on_sts_report`: a lower generation is
    a new publisher and resets that instance before the sample; an equal
    generation is a replay; a message that never arrives is not a
    sample, so nothing is released (F32). A report this process cannot
    parse is also not a sample. It is logged and left.

    ``release`` is given the owners to drop. It must drop them the way
    an intent delete of that owner would. ``states`` is the cursor. The
    caller owns it so a test can read it; omitted, this function keeps
    one for the life of the subscription.

    A failure of the subscription itself restarts after
    :data:`mftik.broker.handler.RESTART_DELAY_SECONDS`. The gap releases
    nothing, which is the same rule as a stopped report.
    """
    cursor = states if states is not None else {}
    pattern = sts_report_pattern()
    while stop is None or not stop.is_set():
        try:
            async for subject, envelope in broker.psubscribe(pattern, stop=stop):
                _sample(cursor, subject, envelope, held, release)
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception(
                "sts report subscription failed pattern=%s — restarting",
                pattern,
            )
            if stop is None:
                await asyncio.sleep(RESTART_DELAY_SECONDS)
                continue
            try:
                await asyncio.wait_for(stop.wait(), timeout=RESTART_DELAY_SECONDS)
            except TimeoutError:
                continue


def _sample(
    states: dict[str, InstanceGcState],
    subject: str,
    envelope: UntypedEnvelope,
    held: Callable[[], Collection[IntentOwner]],
    release: Callable[[Collection[IntentOwner]], None],
) -> None:
    instance = sts_instance_from_report_subject(subject)
    if instance is None:
        return
    if envelope.type != PROCMAN_REPORT:
        logger.warning(
            "ignoring %s on %s; procman reports are type %s",
            envelope.type,
            subject,
            PROCMAN_REPORT,
        )
        return
    try:
        report = ProcmanReport.model_validate(envelope.payload)
        released = on_sts_report(
            states,
            sts_instance=instance,
            held=held(),
            report=report,
        )
    except Exception:
        logger.exception("ignoring procman report subject=%s", subject)
        return
    if released:
        release(released)


__all__ = [
    "InstanceGcState",
    "IntentGcError",
    "OwnerGc",
    "gc_owners",
    "on_sts_report",
    "owners_from_worker_ids",
    "owners_in_report",
    "session_id_from_worker_id",
    "sts_instance_from_report_subject",
    "sts_report_pattern",
    "watch_sts_reports",
]
