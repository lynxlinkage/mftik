"""The F30 gates fail a slow call and a slow ``just test`` step.

Nothing in this module is itself slow. The durations are handed to the
gate, which is the mechanism CI runs. A red test left in the suite would
be the gate firing on the suite, not a proof that the gate can fire.
"""

from __future__ import annotations

from types import SimpleNamespace

from tier_budget import (
    CALL_LIMIT_S,
    WALL_LIMIT_S,
    apply_timeouts,
    call_budget_failure,
    database_params,
    enforce_call_budget,
    tier_of,
    wall_budget_failure,
)


class _Item:
    def __init__(self, *markers: str) -> None:
        self._markers = set(markers)
        self.added: list[object] = []

    def get_closest_marker(self, name: str) -> object | None:
        if name in self._markers:
            return object()
        return None

    def add_marker(self, marker: object) -> None:
        self.added.append(marker)
        mark = getattr(marker, "mark", None)
        if mark is not None:
            self._markers.add(mark.name)


def _report(when: str, outcome: str, duration: float) -> SimpleNamespace:
    return SimpleNamespace(when=when, outcome=outcome, duration=duration, longrepr=None)


def test_an_unmarked_test_is_unit() -> None:
    assert tier_of(_Item()) == "unit"


def test_integration_outranks_component() -> None:
    assert tier_of(_Item("component", "integration")) == "integration"


def test_e2e_outranks_integration() -> None:
    assert tier_of(_Item("integration", "e2e")) == "e2e"


def test_a_deliberately_slow_unit_call_fails_the_gate() -> None:
    """A unit call over 50 ms is a failure. Exactly 50 ms is not."""
    limit = CALL_LIMIT_S["unit"]
    assert limit == 0.05
    assert call_budget_failure("unit", limit) is None
    message = call_budget_failure("unit", limit + 0.001)
    assert message is not None
    assert "unit" in message
    assert "50 ms" in message


def test_the_hook_fails_a_passed_unit_call_that_ran_long() -> None:
    item = _Item()
    report = _report("call", "passed", 0.08)
    enforce_call_budget(item, report)
    assert report.outcome == "failed"
    assert report.longrepr is not None
    assert "50 ms" in report.longrepr


def test_a_unit_call_inside_the_cap_stays_passed() -> None:
    report = _report("call", "passed", 0.01)
    enforce_call_budget(_Item(), report)
    assert report.outcome == "passed"
    assert report.longrepr is None


def test_setup_duration_is_not_the_call_phase() -> None:
    report = _report("setup", "passed", 1.0)
    enforce_call_budget(_Item(), report)
    assert report.outcome == "passed"


def test_an_already_failed_call_keeps_its_failure() -> None:
    report = _report("call", "failed", 1.0)
    report.longrepr = "original"
    enforce_call_budget(_Item(), report)
    assert report.outcome == "failed"
    assert report.longrepr == "original"


def test_xfail_is_left_to_the_xfail_plugin() -> None:
    report = _report("call", "passed", 1.0)
    enforce_call_budget(_Item("xfail"), report)
    assert report.outcome == "passed"


def test_component_and_integration_caps() -> None:
    assert call_budget_failure("component", 0.5) is None
    assert call_budget_failure("component", 0.5 + 0.001) is not None
    assert call_budget_failure("integration", 10.0) is None
    assert call_budget_failure("integration", 10.0 + 0.001) is not None


def test_e2e_has_no_call_phase_cap() -> None:
    assert CALL_LIMIT_S["e2e"] is None
    assert call_budget_failure("e2e", 3600.0) is None


def test_a_component_call_over_the_cap_fails_in_the_hook() -> None:
    report = _report("call", "passed", 0.8)
    enforce_call_budget(_Item("component"), report)
    assert report.outcome == "failed"
    assert "500 ms" in report.longrepr


def test_a_deliberately_slow_wall_clock_fails_the_gate() -> None:
    """``just test`` over 120 s fails. Exactly 120 s does not."""
    assert WALL_LIMIT_S == 120.0
    assert wall_budget_failure(WALL_LIMIT_S) is None
    message = wall_budget_failure(WALL_LIMIT_S + 0.1)
    assert message is not None
    assert "120" in message
    assert "F30" in message


def test_postgres_param_is_integration_and_sqlite_is_component() -> None:
    params = database_params(
        {"sqlite": "sqlite://", "postgres": "postgresql://x"}
    )
    by_id = {param.id: {mark.name for mark in param.marks} for param in params}
    assert by_id == {"sqlite": {"component"}, "postgres": {"integration"}}


def test_each_tier_receives_a_pytest_timeout() -> None:
    unit = _Item()
    component = _Item("component")
    integration = _Item("integration")
    e2e = _Item("e2e")
    preset = _Item()

    def _already_timed(name: str) -> object | None:
        if name == "timeout":
            return object()
        return None

    preset.get_closest_marker = _already_timed  # type: ignore[method-assign]
    apply_timeouts([unit, component, integration, e2e, preset])  # type: ignore[list-item]

    def timeout_of(item: _Item) -> tuple[object, ...]:
        mark = item.added[0]
        return (mark.mark.args, mark.mark.kwargs.get("method"))  # type: ignore[attr-defined]

    assert timeout_of(unit) == ((5.0,), "thread")
    assert timeout_of(component) == ((5.0,), "thread")
    assert timeout_of(integration) == ((30.0,), "thread")
    assert timeout_of(e2e) == ((0.0,), "thread")
    assert preset.added == []
