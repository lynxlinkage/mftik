"""Exit codes the worker and the controller share (B5-06).

The shim's exit file is the crash fact that survives a controller
restart, so the cause rides on the code. The worker chooses it. The
controller maps it. Neither imports the other.
"""

from __future__ import annotations

import signal
from pathlib import Path

from mftik_sts.controller.defaults import STS_CLEANUP_TIMEOUT_S
from mftik_sts.exit_codes import CLEAN, HOOK_BLOCKED, STRATEGY_EXCEPTION, cause_for_exit

#: The account worker's own bound. A literal on purpose: this test must
#: not import ``apps/td``, and a cleanup budget under it would abandon a
#: cancel that is still going to answer.
_TD_CANCEL_SESSION_WAIT_S = 30.0


def test_cleanup_budget_is_longer_than_the_account_workers() -> None:
    assert STS_CLEANUP_TIMEOUT_S == 45.0
    assert STS_CLEANUP_TIMEOUT_S > _TD_CANCEL_SESSION_WAIT_S


def test_named_codes() -> None:
    assert CLEAN == 0
    assert STRATEGY_EXCEPTION == 75
    assert HOOK_BLOCKED == 76
    assert cause_for_exit(
        exit_code=CLEAN, signal_no=None, stopped_by_controller=False
    ) is None
    assert (
        cause_for_exit(
            exit_code=STRATEGY_EXCEPTION,
            signal_no=None,
            stopped_by_controller=False,
        )
        == "strategy_exception"
    )
    assert (
        cause_for_exit(
            exit_code=HOOK_BLOCKED, signal_no=None, stopped_by_controller=False
        )
        == "hook_blocked"
    )


def test_a_stop_this_controller_issued_is_stop_stuck() -> None:
    assert (
        cause_for_exit(
            exit_code=None,
            signal_no=signal.SIGKILL,
            stopped_by_controller=True,
        )
        == "stop_stuck"
    )
    assert (
        cause_for_exit(
            exit_code=1, signal_no=None, stopped_by_controller=True
        )
        == "stop_stuck"
    )


def test_any_other_death_is_process_death() -> None:
    assert (
        cause_for_exit(
            exit_code=None,
            signal_no=signal.SIGKILL,
            stopped_by_controller=False,
        )
        == "process_death"
    )
    assert (
        cause_for_exit(exit_code=1, signal_no=None, stopped_by_controller=False)
        == "process_death"
    )
    assert (
        cause_for_exit(exit_code=None, signal_no=None, stopped_by_controller=False)
        == "process_death"
    )


def test_the_worker_emits_the_strategy_code_and_not_the_blocked_hook(
    tmp_path: Path,
) -> None:
    del tmp_path
    source = (
        Path(__file__).resolve().parents[1]
        / "src"
        / "mftik_sts"
        / "session_worker"
        / "process.py"
    ).read_text(encoding="utf-8")
    assert "STRATEGY_EXCEPTION" in source
    assert "HOOK_BLOCKED" not in source
