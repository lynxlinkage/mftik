"""Every session status has to be visible somewhere on the dashboard.

This is a regression guard, not a unit test. ``failed`` was added without a
count of its own, and those sessions silently disappeared from the home page:
not live, not history, nowhere. The same thing would have happened again with
``interrupted``. Anyone adding the next status will fail this test rather than
find out from a confused user.
"""

from __future__ import annotations

from mftik_api.schemas import DomainStats
from mftik_db.models.session import SessionStatus

#: Fields that describe *which* instance a row is rather than counting
#: sessions. Named rather than subtracted from, so a status added later cannot
#: pass this test by colliding with a descriptive field nobody counted.
_DESCRIPTIVE = {
    "domain",
    "instance",
    "region",
    "enabled",
    "state",
    "healthy",
    "version",
    "venues",
    "api_ids",
}


def test_every_status_is_counted_by_domain_stats() -> None:
    counted = set(DomainStats.model_fields) - _DESCRIPTIVE
    missing = {s.value for s in SessionStatus} - counted
    assert not missing, (
        f"sessions in {sorted(missing)} would not appear on the dashboard; "
        f"add a count to DomainStats and to the /stats route"
    )


def test_the_descriptive_fields_are_all_real() -> None:
    """Keeps :data:`_DESCRIPTIVE` from rotting into a list of removed names.

    A stale entry here would silently re-open the hole the test above exists to
    close: subtracting a field that no longer exists does nothing, and the next
    status added would be counted as covered when it is not.
    """
    assert _DESCRIPTIVE <= set(DomainStats.model_fields)


def test_a_row_names_the_instance_it_describes() -> None:
    """One row per declared instance, not one per plane."""
    assert "instance" in DomainStats.model_fields
    assert "state" in DomainStats.model_fields


def test_terminal_covers_every_status_that_is_not_live() -> None:
    """``terminal()`` exists to stop callers spelling this as ``== done``."""
    assert SessionStatus.terminal() == {
        s.value for s in SessionStatus if s is not SessionStatus.LIVE
    }
