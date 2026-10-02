"""``StrategyOut.phase`` is the v2 word the CLI polls (B4-08)."""

from __future__ import annotations

from mftik_api.routes.sts import session_phase


def test_a_phase_in_conditions_wins() -> None:
    assert session_phase("live", {"phase": "running"}) == "running"
    assert session_phase("live", {"phase": "stopping", "MdReady": "1/2"}) == (
        "stopping"
    )
    assert session_phase("failed", {"phase": "failed"}) == "failed"


def test_null_or_empty_conditions_fall_back_to_the_column() -> None:
    """A live row the controller has not reported yet reads as starting.

    The column stays ``live`` for every non-terminal phase. A terminal
    column is its own word. An empty object is what the row stores
    before the controller writes, and it is treated as null.
    """
    assert session_phase("live", None) == "starting"
    assert session_phase("live", {}) == "starting"
    assert session_phase("done", None) == "done"
    assert session_phase("failed", {}) == "failed"
    assert session_phase("ack", None) == "ack"
    assert session_phase("interrupted", None) == "interrupted"


def test_a_blank_phase_is_not_a_phase() -> None:
    assert session_phase("live", {"phase": ""}) == "starting"
    assert session_phase("done", {"phase": ""}) == "done"
