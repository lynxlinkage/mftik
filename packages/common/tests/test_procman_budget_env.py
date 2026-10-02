"""Plane processes read the admission budget from the environment (B4-09).

:func:`decide_admission` and :class:`~mftik.procman.Supervisor` still do
not. Unset is no budget. A bad value fails the read instead of turning
the limit off.
"""

from __future__ import annotations

import pytest
from mftik.procman import (
    KIND_RSS_ESTIMATE_MIB,
    OOM_SCORE_ADJ,
    AdmissionBudget,
    InvalidWorkerSpec,
    admission_budget_from_environ,
    estimates_mib,
)


def test_unset_and_blank_are_no_budget() -> None:
    assert admission_budget_from_environ("sts", {}) is None
    assert (
        admission_budget_from_environ(
            "td",
            {"PROCMAN_MAX_WORKERS": "", "PROCMAN_MEMORY_BUDGET_MB": "  "},
        )
        is None
    )


def test_one_limit_keeps_the_other_off_and_uses_that_planes_kinds() -> None:
    budget = admission_budget_from_environ(
        "md", {"PROCMAN_MEMORY_BUDGET_MB": "512"}
    )
    assert isinstance(budget, AdmissionBudget)
    assert budget.max_workers is None
    assert budget.memory_budget_mb == 512
    assert dict(budget.estimate_mb) == {"conn": 61, "fetch": 60}
    workers = admission_budget_from_environ(
        "sts", {"PROCMAN_MAX_WORKERS": "3"}
    )
    assert workers is not None
    assert workers.max_workers == 3
    assert workers.memory_budget_mb is None
    assert dict(workers.estimate_mb) == {"session": 69}


def test_both_limits_and_the_account_estimate() -> None:
    budget = admission_budget_from_environ(
        "td",
        {"PROCMAN_MAX_WORKERS": "2", "PROCMAN_MEMORY_BUDGET_MB": "256"},
    )
    assert budget is not None
    assert budget.max_workers == 2
    assert budget.memory_budget_mb == 256
    assert dict(budget.estimate_mb) == {"account": 79}


@pytest.mark.parametrize(
    "raw",
    ["0", "00", "01", "-1", "+2", "1.5", "true", " 2 workers"],
)
def test_a_non_positive_or_non_integer_value_raises(raw: str) -> None:
    with pytest.raises(InvalidWorkerSpec):
        admission_budget_from_environ("sts", {"PROCMAN_MAX_WORKERS": raw})
    with pytest.raises(InvalidWorkerSpec):
        admission_budget_from_environ("sts", {"PROCMAN_MEMORY_BUDGET_MB": raw})


def test_an_unknown_plane_raises() -> None:
    with pytest.raises(InvalidWorkerSpec):
        admission_budget_from_environ("sym", {"PROCMAN_MAX_WORKERS": "1"})


def test_estimates_match_the_measured_table_and_oom_keys() -> None:
    assert set(KIND_RSS_ESTIMATE_MIB) == set(OOM_SCORE_ADJ)
    assert estimates_mib("paper") == {}
    assert all(mib > 0 for mib in KIND_RSS_ESTIMATE_MIB.values())
