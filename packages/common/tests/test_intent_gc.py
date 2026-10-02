"""Owner GC from STS liveness reports (§8.2 rule 3).

Direct calls. No bus. A lower generation is a new publisher and is
reset before :func:`gc_owners`, which itself still refuses one.
"""

from __future__ import annotations

import pytest
from mftik.intent_gc import (
    InstanceGcState,
    IntentGcError,
    gc_owners,
    on_sts_report,
    owners_in_report,
    session_id_from_worker_id,
    sts_instance_from_report_subject,
    sts_report_pattern,
)
from mftik.protocol import IntentOwner, ProcmanReport, ProcmanWorker


def _owner(session_id: str, sts_instance: str = "sts") -> IntentOwner:
    return IntentOwner(sts_instance=sts_instance, session_id=session_id)


def _worker(worker_id: str, phase: str = "running") -> ProcmanWorker:
    return ProcmanWorker(
        id=worker_id,
        code_ref="v1",
        rss_bytes=None,
        phase=phase,
        ready=phase == "running",
        incarnation=1,
    )


def _report(generation: int, *workers: ProcmanWorker) -> ProcmanReport:
    return ProcmanReport(generation=generation, workers=list(workers))


def test_a_session_worker_id_round_trips_and_other_ids_do_not() -> None:
    assert session_id_from_worker_id("sts/session/abc123") == "abc123"
    assert session_id_from_worker_id("sts/session/abc123/extra") is None
    assert session_id_from_worker_id("sts/session/") is None
    assert session_id_from_worker_id("td/account/7") is None
    assert session_id_from_worker_id("sts/session") is None


def test_the_subscription_is_every_sts_report() -> None:
    assert sts_report_pattern() == "procman.report.sts.*"
    assert sts_instance_from_report_subject("procman.report.sts.sts-jp") == "sts-jp"
    assert sts_instance_from_report_subject("procman.report.sts.") is None
    assert sts_instance_from_report_subject("procman.report.sts.a.b") is None
    assert sts_instance_from_report_subject("procman.report.md.md") is None


def test_gc_owners_needs_two_newer_misses_and_ignores_a_replay() -> None:
    session = _owner("s1")
    other = _owner("s2")
    first = gc_owners(
        frozenset({session, other}),
        frozenset(),
        None,
        report=frozenset({other}),
        report_generation=1,
    )
    assert first.release == frozenset()
    assert first.absent == frozenset({session})
    replay = gc_owners(
        frozenset({session, other}),
        first.absent,
        first.generation,
        report=frozenset({other}),
        report_generation=1,
    )
    assert replay.release == frozenset()
    assert replay.absent == frozenset({session})
    second = gc_owners(
        frozenset({session, other}),
        replay.absent,
        replay.generation,
        report=frozenset({other}),
        report_generation=2,
    )
    assert second.release == frozenset({session})
    assert second.absent == frozenset()


def test_gc_owners_refuses_a_bad_generation() -> None:
    session = _owner("s1")
    with pytest.raises(IntentGcError):
        gc_owners(
            frozenset({session}),
            frozenset(),
            5,
            report=frozenset({session}),
            report_generation=4,
        )
    with pytest.raises(TypeError):
        gc_owners(
            frozenset({session}),
            frozenset(),
            None,
            report=frozenset({session}),
            report_generation=True,  # type: ignore[arg-type]
        )


def test_a_stopped_report_is_not_a_sample() -> None:
    """F32. The absence streak is left where it was."""
    session = _owner("s1")
    result = gc_owners(
        frozenset({session}),
        frozenset({session}),
        1,
        report=None,
        report_generation=None,
    )
    assert result.release == frozenset()
    assert result.absent == frozenset({session})
    assert result.generation == 1


def test_two_misses_release_and_a_replay_does_not() -> None:
    session = _owner("s1")
    states: dict[str, InstanceGcState] = {}
    held = frozenset({session})
    first = on_sts_report(
        states, sts_instance="sts", held=held, report=_report(1)
    )
    assert first == frozenset()
    assert states["sts"].absent == frozenset({session})
    replay = on_sts_report(
        states, sts_instance="sts", held=held, report=_report(1)
    )
    assert replay == frozenset()
    assert states["sts"].absent == frozenset({session})
    assert states["sts"].previous_generation == 1
    second = on_sts_report(
        states, sts_instance="sts", held=held, report=_report(2)
    )
    assert second == frozenset({session})


def test_a_lower_generation_resets_before_it_samples() -> None:
    """A new publisher starts again at 1. One report from it releases nobody."""
    session = _owner("s1")
    states = {
        "sts": InstanceGcState(
            previous_generation=5, absent=frozenset({session})
        )
    }
    held = frozenset({session})
    first = on_sts_report(
        states, sts_instance="sts", held=held, report=_report(1)
    )
    assert first == frozenset()
    assert states["sts"].previous_generation == 1
    assert states["sts"].absent == frozenset({session})
    second = on_sts_report(
        states, sts_instance="sts", held=held, report=_report(2)
    )
    assert second == frozenset({session})


def test_a_listed_phase_keeps_the_owner() -> None:
    """R4. Any phase on the report counts. ``restarting`` is not a phase."""
    session = _owner("s1")
    states = {
        "sts": InstanceGcState(
            previous_generation=1, absent=frozenset({session})
        )
    }
    report = _report(
        2,
        _worker("sts/session/s1", "failed"),
        _worker("td/account/7", "stopping"),
    )
    assert owners_in_report("sts", report.workers) == frozenset({session})
    released = on_sts_report(
        states, sts_instance="sts", held=frozenset({session}), report=report
    )
    assert released == frozenset()
    assert states["sts"].absent == frozenset()


def test_a_stopped_report_leaves_the_cursor() -> None:
    session = _owner("s1")
    states: dict[str, InstanceGcState] = {}
    on_sts_report(
        states,
        sts_instance="sts",
        held=frozenset({session}),
        report=_report(1),
    )
    cursor = states["sts"]
    assert (
        on_sts_report(
            states, sts_instance="sts", held=frozenset({session}), report=None
        )
        == frozenset()
    )
    assert states["sts"] == cursor


def test_a_report_from_another_instance_changes_nothing() -> None:
    session = _owner("s1", "sts")
    states = {
        "sts": InstanceGcState(
            previous_generation=1, absent=frozenset({session})
        )
    }
    cursor = states["sts"]
    released = on_sts_report(
        states,
        sts_instance="sts-jp",
        held=frozenset({session}),
        report=_report(1),
    )
    assert released == frozenset()
    assert states == {"sts": cursor}
    assert "sts-jp" not in states
