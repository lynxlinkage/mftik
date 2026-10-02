"""``run_wait_action`` and the flag precedence, as a table (B4-08)."""

from __future__ import annotations

import pytest
from mftik.cli.client import DEFAULT_TIMEOUT_S
from mftik.cli.run import (
    _WAIT_MAX_MISSES,
    _WAIT_PHASES,
    _WAIT_POLL_S,
    run_disposition,
    run_wait_action,
)


@pytest.mark.parametrize(
    ("status", "step"),
    [
        ("pending", "wait"),
        ("starting", "wait"),
        ("restarting", "wait"),
        ("stopping", "wait"),
        ("running", "tail"),
        ("failed", "tail"),
        ("done", "tail"),
        ("sideways", "wait"),
    ],
)
def test_wait_keeps_watching_until_the_phase_settles(status: str, step: str) -> None:
    assert run_wait_action(status, wait=True) == step


@pytest.mark.parametrize(
    "status",
    ["pending", "starting", "running", "failed", "done", "stopping", "sideways"],
)
def test_no_wait_returns_the_id_for_every_status(status: str) -> None:
    assert run_wait_action(status, wait=False) == "return_id"


@pytest.mark.parametrize(
    ("wait", "no_follow", "phase", "action"),
    [
        (False, False, "starting", "return_id"),
        (False, True, "running", "return_id"),
        (False, True, "failed", "return_id"),
        (False, False, "done", "return_id"),
        (True, False, "pending", "watch"),
        (True, False, "starting", "watch"),
        (True, False, "restarting", "watch"),
        (True, False, "stopping", "watch"),
        (True, False, "sideways", "watch"),
        (True, True, "starting", "watch"),
        (True, False, "running", "tail"),
        (True, False, "failed", "report"),
        (True, False, "done", "report"),
        (True, True, "running", "report"),
        (True, True, "failed", "report"),
        (True, True, "done", "report"),
    ],
)
def test_flag_precedence(
    wait: bool, no_follow: bool, phase: str, action: str
) -> None:
    """``--no-wait`` wins. ``--no-follow`` does not tail a settled session."""
    assert run_disposition(phase, wait=wait, no_follow=no_follow) == action


def test_the_watch_words_are_the_ones_the_default_names() -> None:
    assert _WAIT_PHASES == frozenset(
        {"pending", "starting", "restarting", "stopping"}
    )


def test_poll_misses_add_up_to_the_http_timeout() -> None:
    assert _WAIT_MAX_MISSES * _WAIT_POLL_S == DEFAULT_TIMEOUT_S
