"""``MFTIK_ROLE`` — how much of its plane one instance answers for.

One ordered role rather than two booleans, because ``standby`` has to gate the
*unicast* subject too: blue and green are both ``md-jp-1``, so a green that
gated only the shared pool would still take a named attach for feeds it does
not have.
"""

from __future__ import annotations

import pytest
from mftik import (
    INSTANCED_PLANES,
    ROLE_ENV,
    STANDBY_PLANES,
    Role,
    control_subjects,
    instance_role,
)
from mftik.protocol import Topics


@pytest.mark.parametrize("plane", sorted(INSTANCED_PLANES))
def test_an_unset_role_is_active(monkeypatch, plane: str) -> None:
    """So adding unicast subjects changes nothing a deployment can observe."""
    monkeypatch.delenv(ROLE_ENV, raising=False)
    assert instance_role(plane) is Role.ACTIVE


def test_a_role_that_is_not_one_is_refused_by_name(monkeypatch) -> None:
    monkeypatch.setenv(ROLE_ENV, "primary")
    with pytest.raises(ValueError, match="primary"):
        instance_role("md")


def test_md_may_stand_by(monkeypatch) -> None:
    """The one plane that is ever replaced while running."""
    monkeypatch.setenv(ROLE_ENV, "standby")
    assert instance_role("md") is Role.STANDBY


@pytest.mark.parametrize(
    "plane", sorted(INSTANCED_PLANES - STANDBY_PLANES)
)
def test_standby_is_refused_where_it_would_do_nothing(
    monkeypatch, plane: str
) -> None:
    """Not because it is dangerous — because it silently accomplishes nothing.

    Neither STS nor TD is ever blue/greened (two copies of a session is two
    copies deciding to trade), so standby has no use there. A process that
    comes up answering nothing is worse than one that refuses to come up.
    """
    monkeypatch.setenv(ROLE_ENV, "standby")
    with pytest.raises(ValueError, match=plane):
        instance_role(plane)


def test_a_role_is_case_and_space_insensitive(monkeypatch) -> None:
    monkeypatch.setenv(ROLE_ENV, "  NAMED ")
    assert instance_role("td") is Role.NAMED


def test_active_serves_its_own_subject_and_the_pool() -> None:
    assert control_subjects("md", "md-jp-1", Role.ACTIVE) == [
        Topics.md("md-jp-1"),
        Topics.MD,
    ]


def test_named_serves_only_its_own_subject() -> None:
    """Work nobody addressed goes to a peer, not here."""
    assert control_subjects("td", "td-jp-1", Role.NAMED) == [
        Topics.td("td-jp-1")
    ]


def test_standby_serves_nothing_at_all() -> None:
    """Including its own name — the point *The hard parts* turns on."""
    assert control_subjects("md", "md-jp-1", Role.STANDBY) == []


def test_standby_runs_no_reaper() -> None:
    """It has nothing to reap and a peer that does."""
    assert Role.STANDBY.runs_reaper is False
    assert Role.NAMED.runs_reaper is True
    assert Role.ACTIVE.runs_reaper is True


@pytest.mark.parametrize("plane", ["sym", "paper", "api"])
def test_a_plane_that_is_not_instanced_has_no_subjects(plane: str) -> None:
    with pytest.raises(ValueError, match=plane):
        control_subjects(plane, "whatever", Role.ACTIVE)
