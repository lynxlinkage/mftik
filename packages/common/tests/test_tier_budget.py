"""The F30 gates fail a slow call and a slow ``just test`` step.

Nothing in this module is itself slow. The durations are handed to the
gate, which is the mechanism CI runs. A red test left in the suite would
be the gate firing on the suite, not a proof that the gate can fire.

These tests describe the local gate unless they set ``CI`` themselves.
The suite runs on CI, where unit and component overages only warn, so
the fixture clears that for the assertions that expect a failure.
"""

from __future__ import annotations

from types import SimpleNamespace

import pytest
from tier_budget import (
    ANNOTATION_CAP,
    CALL_LIMIT_S,
    WALL_LIMIT_S,
    annotation_lines,
    apply_timeouts,
    budget_summary_lines,
    budget_warnings,
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
    return SimpleNamespace(
        when=when,
        outcome=outcome,
        duration=duration,
        longrepr=None,
        user_properties=[],
    )


@pytest.fixture(autouse=True)
def _local_call_budget(monkeypatch: pytest.MonkeyPatch) -> None:
    """Default these tests to the strict local gate."""
    monkeypatch.delenv("CI", raising=False)
    monkeypatch.delenv("GITHUB_ACTIONS", raising=False)


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


def test_on_ci_an_over_budget_call_passes_with_a_warning(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """CI warns for unit and component. The same call fails when CI is unset.

    ``GITHUB_ACTIONS`` alone is enough. The caps stay 50 ms and 500 ms.
    Exactly on the cap still passes. Integration still fails on CI.
    """
    monkeypatch.setenv("CI", "true")
    monkeypatch.delenv("GITHUB_ACTIONS", raising=False)

    warned = _report("call", "passed", 0.08)
    enforce_call_budget(_Item(), warned)
    assert warned.outcome == "passed"
    assert warned.longrepr is None
    warnings = budget_warnings(warned)
    assert len(warnings) == 1
    assert "50 ms" in warnings[0]
    assert "unit" in warnings[0]

    exact = _report("call", "passed", 0.05)
    enforce_call_budget(_Item(), exact)
    assert exact.outcome == "passed"
    assert budget_warnings(exact) == []

    component = _report("call", "passed", 0.8)
    monkeypatch.delenv("CI", raising=False)
    monkeypatch.setenv("GITHUB_ACTIONS", "true")
    enforce_call_budget(_Item("component"), component)
    assert component.outcome == "passed"
    component_warnings = budget_warnings(component)
    assert len(component_warnings) == 1
    assert "500 ms" in component_warnings[0]

    integration = _report("call", "passed", 11.0)
    enforce_call_budget(_Item("integration"), integration)
    assert integration.outcome == "failed"
    assert "10 s" in integration.longrepr

    monkeypatch.delenv("CI", raising=False)
    monkeypatch.delenv("GITHUB_ACTIONS", raising=False)
    local = _report("call", "passed", 0.08)
    enforce_call_budget(_Item(), local)
    assert local.outcome == "failed"
    assert local.longrepr is not None
    assert "50 ms" in local.longrepr
    assert budget_warnings(local) == []


def test_github_annotations_are_capped_and_the_summary_is_not() -> None:
    offenders = [
        (f"pkg/test.py::test_{i}", f"unit call phase {i} ms exceeds 50 ms")
        for i in range(ANNOTATION_CAP + 5)
    ]
    lines = annotation_lines(offenders)
    assert len(lines) == ANNOTATION_CAP
    assert lines[0].startswith("::warning::")
    assert "test_0" in lines[0]
    assert "exceeds 50 ms" in lines[0]
    joined = "\n".join(lines)
    assert "test_20" not in joined
    summary = "\n".join(budget_summary_lines(offenders))
    assert "test_0" in summary
    assert f"test_{ANNOTATION_CAP + 4}" in summary
    assert "::warning::" not in summary
    assert f"cap {ANNOTATION_CAP}" in summary
    flat = annotation_lines([("node", "line one\nline two")])
    assert flat == ["::warning::line one line two (node)"]


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
    assert timeout_of(component) == ((30.0,), "thread")
    assert timeout_of(integration) == ((60.0,), "thread")
    assert timeout_of(e2e) == ((0.0,), "thread")
    assert preset.added == []
