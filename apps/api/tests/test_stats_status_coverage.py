"""Every session status has to be visible somewhere on the dashboard.

This is a regression guard, not a unit test. ``failed`` was added without a
count of its own, and those sessions silently disappeared from the home page:
not live, not history, nowhere. The same thing would have happened again with
``interrupted``. Anyone adding the next status will fail this test rather than
find out from a confused user.
"""

from __future__ import annotations

from mft_api.schemas import DomainStats
from mft_db.models.session import SessionStatus


def test_every_status_is_counted_by_domain_stats() -> None:
    counted = set(DomainStats.model_fields) - {"domain", "healthy"}
    missing = {s.value for s in SessionStatus} - counted
    assert not missing, (
        f"sessions in {sorted(missing)} would not appear on the dashboard; "
        f"add a count to DomainStats and to the /stats route"
    )


def test_terminal_covers_every_status_that_is_not_live() -> None:
    """``terminal()`` exists to stop callers spelling this as ``== done``."""
    assert SessionStatus.terminal() == {
        s.value for s in SessionStatus if s is not SessionStatus.LIVE
    }
