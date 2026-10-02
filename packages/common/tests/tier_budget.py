"""Call-phase budgets and the per-tier hang backstop (B2-04, F30, §9.1).

Two different clocks:

* The **call-phase gate** reads the duration pytest records for ``call``.
  That is the number F30 names. A unit test whose call ran longer than
  50 ms fails locally. Component and integration use the caps in the
  §9.1 table (500 ms and 10 s). e2e has no per-test cap. On CI the unit
  and component caps warn instead of failing the job; the numbers do
  not change, and integration still fails.
* **pytest-timeout** is the hang backstop (§9.2 rule 8). A test that never
  returns has no call duration to judge, so each tier still gets a
  timeout. Every backstop sits above its call-phase cap: a thread
  timeout at the cap races the measurement and, under xdist, kills the
  worker (``Not properly terminated``) instead of failing the test.

The 120 s wall budget is the whole ``just test`` step on ``ubuntu-latest``,
not a single test. :func:`wall_budget_failure` is what CI applies to that
step's elapsed time, and it stays a hard failure there.
"""

from __future__ import annotations

import os
from typing import Any

import pytest

#: Call-phase caps from §9.1. ``None`` means the tier has no cap.
CALL_LIMIT_S: dict[str, float | None] = {
    "unit": 0.05,
    "component": 0.5,
    "integration": 10.0,
    "e2e": None,
}

#: pytest-timeout, seconds. ``0`` disables it (e2e, §9.1 has no cap).
#:
#: These sit above the call-phase caps. A thread timeout *at* the cap
#: aborts the worker under xdist (``Not properly terminated``) instead of
#: failing the test, and it races a call that finished just under the cap.
#: A test that returns is judged by :func:`call_budget_failure`. A test
#: that never returns dies here.
HANG_TIMEOUT_S: dict[str, float] = {
    "unit": 5.0,
    # Above the waits component tests already pass to ``asyncio.wait_for``.
    # A thread timeout that fires first kills the xdist worker.
    "component": 30.0,
    "integration": 60.0,
    "e2e": 0.0,
}

#: F30: ``just test`` (unit + component) on ubuntu-latest.
WALL_LIMIT_S = 120.0

#: Tiers whose call-phase cap warns on CI instead of failing the job.
#: Integration stays a hard failure: the flaky band is 50 ms / 500 ms.
WARN_ON_CI_TIERS = frozenset({"unit", "component"})

#: Carried on the report so the controller can list it after xdist.
BUDGET_PROPERTY = "tier_budget"

#: GitHub annotation cap. The terminal summary still lists every offender.
ANNOTATION_CAP = 20

_TIER_ORDER = ("e2e", "integration", "component")


def tier_of(item: Any) -> str:
    """The §9.1 tier of a collected test.

    An unmarked test is unit. A postgres parameter is marked
    ``integration`` and outranks a ``component`` mark on the same item,
    so the dialect follows the tier that runs it.
    """
    for name in _TIER_ORDER:
        if item.get_closest_marker(name) is not None:
            return name
    return "unit"


def call_budget_failure(tier: str, duration_s: float) -> str | None:
    """Why this call phase missed its cap, or ``None`` when it did not.

    The comparison is strict. A call of exactly 50 ms has not exceeded
    50 ms, which is the word F30 uses (超過).
    """
    limit = CALL_LIMIT_S[tier]
    if limit is None or duration_s <= limit:
        return None
    return (
        f"{tier} call phase {_format_seconds(duration_s)} exceeds "
        f"{_format_seconds(limit)} (§9.1)"
    )


def wall_budget_failure(elapsed_s: float) -> str | None:
    """Why a ``just test`` step missed the F30 wall budget, or ``None``."""
    if elapsed_s <= WALL_LIMIT_S:
        return None
    return (
        f"just test wall time {elapsed_s:.1f}s exceeds {WALL_LIMIT_S:.0f}s "
        f"(F30). The budget is the unit+component step on ubuntu-latest, "
        f"excluding uv sync and service startup."
    )


#: Values that must not select the warn-only gate. ``CI=false`` and
#: ``CI=0`` are how a local shell says "not CI"; treating any non-empty
#: string as CI turned that gate off.
_NOT_CI = frozenset({"", "0", "false", "no", "off"})


def _env_means_ci(name: str) -> bool:
    raw = os.environ.get(name)
    if raw is None:
        return False
    return raw.strip().lower() not in _NOT_CI


def on_ci() -> bool:
    """True when this process is a CI job.

    GitHub Actions sets both ``CI`` and ``GITHUB_ACTIONS`` to ``true`` on
    every step, including the stdlib-loop pass. Either one selects the
    warn-only call-phase gate. Unset, empty, and ``0`` / ``false`` /
    ``no`` / ``off`` (any case, surrounding space ignored) are not CI.
    """
    return _env_means_ci("CI") or _env_means_ci("GITHUB_ACTIONS")


def enforce_call_budget(item: Any, report: Any) -> None:
    """Fail a passed call whose duration missed its tier cap.

    Setup and teardown are not the call phase. A test that already
    failed keeps that failure. ``xfail`` is left to the xfail plugin:
    rewriting it here would turn an expected failure into a second one.

    On CI, a unit or component call over its cap stays passed. The
    warning is stored on the report for the terminal summary. The cap
    itself is unchanged. Integration still fails, and this function
    does not implement the 120 s wall gate.
    """
    if getattr(report, "when", None) != "call":
        return
    if getattr(report, "outcome", None) != "passed":
        return
    if item.get_closest_marker("xfail") is not None:
        return
    tier = tier_of(item)
    message = call_budget_failure(tier, float(report.duration))
    if message is None:
        return
    if on_ci() and tier in WARN_ON_CI_TIERS:
        _record_budget_warning(report, message)
        return
    report.outcome = "failed"
    report.longrepr = message


def budget_warnings(report: Any) -> list[str]:
    """Call-phase warnings recorded on a report, in order."""
    found: list[str] = []
    for item in getattr(report, "user_properties", None) or []:
        if not isinstance(item, tuple | list) or len(item) != 2:
            continue
        name, value = item
        if name == BUDGET_PROPERTY and isinstance(value, str):
            found.append(value)
    return found


def budget_summary_lines(offenders: list[tuple[str, str]]) -> list[str]:
    """Every over-budget test, for the terminal summary.

    Annotations are capped separately. A run with more offenders than
    :data:`ANNOTATION_CAP` still prints all of them here.
    """
    lines = [f"{nodeid}: {message}" for nodeid, message in offenders]
    hidden = len(offenders) - ANNOTATION_CAP
    if hidden > 0:
        lines.append(
            f"{hidden} of these were not emitted as GitHub annotations "
            f"(cap {ANNOTATION_CAP})."
        )
    return lines


def annotation_lines(
    offenders: list[tuple[str, str]], *, cap: int = ANNOTATION_CAP
) -> list[str]:
    """GitHub ``::warning::`` commands, at most ``cap`` of them."""
    lines: list[str] = []
    for nodeid, message in offenders[:cap]:
        text = f"{message} ({nodeid})".replace("\r", " ").replace("\n", " ")
        lines.append(f"::warning::{text}")
    return lines


def _record_budget_warning(report: Any, message: str) -> None:
    properties = getattr(report, "user_properties", None)
    if properties is None:
        properties = []
        report.user_properties = properties
    properties.append((BUDGET_PROPERTY, message))


def database_params(urls: dict[str, str]) -> list[pytest.ParameterSet]:
    """One parameter per dialect, on the tier §9.1 gives that dialect.

    sqlite ``:memory:`` is component and stays in ``just test``. Postgres
    is integration and runs under ``just test-int``. The mark is on the
    parameter, so the same test function can sit in both tiers.
    """
    params: list[pytest.ParameterSet] = []
    for name, url in urls.items():
        if name == "postgres":
            mark: pytest.MarkDecorator = pytest.mark.integration
        else:
            mark = pytest.mark.component
        params.append(pytest.param(url, id=name, marks=mark))
    return params


def apply_timeouts(items: list[pytest.Item]) -> None:
    """Give every item its tier's pytest-timeout, unless one is set."""
    for item in items:
        if item.get_closest_marker("timeout") is not None:
            continue
        seconds = HANG_TIMEOUT_S[tier_of(item)]
        item.add_marker(pytest.mark.timeout(seconds, method="thread"))


def _format_seconds(seconds: float) -> str:
    if seconds < 1:
        ms = seconds * 1000
        nearest = round(ms)
        # Keep a tenth when rounding would hide that the call went over.
        if abs(ms - nearest) >= 0.05:
            return f"{ms:.1f} ms"
        return f"{nearest:.0f} ms"
    if seconds == int(seconds):
        return f"{seconds:.0f} s"
    return f"{seconds:.1f} s"
