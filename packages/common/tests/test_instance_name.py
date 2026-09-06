"""``MFTIK_INSTANCE`` — the one place a process learns which instance it is."""

from __future__ import annotations

import pytest
from mftik import INSTANCE_ENV, INSTANCED_PLANES, instance_name


@pytest.mark.parametrize("plane", sorted(INSTANCED_PLANES))
def test_an_unset_env_is_the_plane_name(monkeypatch, plane: str) -> None:
    """What every existing deployment gets, without configuring anything.

    Migration 0031 declares ``td`` / ``md`` / ``sts``, so a node that has never
    heard of instances comes up already matching a declared row.
    """
    monkeypatch.delenv(INSTANCE_ENV, raising=False)
    assert instance_name(plane) == plane


def test_a_set_env_names_the_instance(monkeypatch) -> None:
    monkeypatch.setenv(INSTANCE_ENV, "md-jp-1")
    assert instance_name("md") == "md-jp-1"


@pytest.mark.parametrize("raw", ["", "   ", "\t\n"])
def test_a_blank_env_reads_as_unset(monkeypatch, raw: str) -> None:
    """``MFTIK_INSTANCE=`` left in a compose file is not an instance name.

    Taken literally it would serve a subject ending in a dot and match no
    declared row — a process that is up, answering nothing, and shows as *down*
    on Home with no clue why.
    """
    monkeypatch.setenv(INSTANCE_ENV, raw)
    assert instance_name("td") == "td"


def test_surrounding_whitespace_is_not_part_of_the_name(monkeypatch) -> None:
    monkeypatch.setenv(INSTANCE_ENV, "  sts-tw  ")
    assert instance_name("sts") == "sts-tw"


def test_sym_and_paper_are_not_instanced() -> None:
    """Stated here because the set is what the API validates against."""
    assert INSTANCED_PLANES == {"td", "md", "sts"}
