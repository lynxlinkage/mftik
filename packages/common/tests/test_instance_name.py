"""``MFTIK_INSTANCE`` — the one place a process learns which instance it is."""

from __future__ import annotations

import pytest
from mftik import INSTANCE_ENV, INSTANCED_PLANES, instance_name, validate_instance_name


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


@pytest.mark.parametrize("raw", ["md.jp", "MD-JP-1", "md jp", "md_jp_1", "*"])
def test_an_illegal_env_refuses_to_start(monkeypatch, raw: str) -> None:
    """The same rule ``/instances`` applies, on the side that sets the name.

    Refusing at boot rather than defaulting: a process that quietly fell back
    to ``md`` would answer for an instance it was not deployed as, and one
    that kept the bad name would read as *down* forever — ``/instances`` will
    not declare that name and there is no rename.
    """
    monkeypatch.setenv(INSTANCE_ENV, raw)

    with pytest.raises(ValueError) as caught:
        instance_name("md")

    assert INSTANCE_ENV in str(caught.value), (
        "the message names the variable, because the traceback is all a "
        "deploy gets"
    )


def test_sym_and_paper_are_not_instanced() -> None:
    """Stated here because the set is what the API validates against."""
    assert INSTANCED_PLANES == {"td", "md", "sts"}


@pytest.mark.parametrize(
    "name",
    ["td", "md-jp-1", "sts-tw", "td-us"],
)
def test_a_legal_instance_name_is_returned_stripped(name: str) -> None:
    assert validate_instance_name(f"  {name}  ") == name


@pytest.mark.parametrize(
    "name",
    ["", "   ", "md.jp", "md jp", "MD-JP-1", "md_jp_1", "*"],
)
def test_an_illegal_instance_name_is_refused(name: str) -> None:
    """Dots split the health subject; anything else will not match compose."""
    with pytest.raises(ValueError):
        validate_instance_name(name)
