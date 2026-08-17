"""Qualified type names for registry trees."""

from mftik.registry.qualify import (
    PRIVATE_ORIGIN,
    PUBLIC_ORIGIN,
    qualify,
    split_qualified,
)


def test_qualify_own_origins() -> None:
    assert qualify(PUBLIC_ORIGIN, "HelloStrategy") == "public::HelloStrategy"
    assert qualify(PRIVATE_ORIGIN, "HelloStrategy") == "private::HelloStrategy"


def test_qualify_remote() -> None:
    assert qualify("node1", "HelloStrategy") == "node1::HelloStrategy"


def test_split_qualified() -> None:
    assert split_qualified("public::HelloStrategy") == ("public", "HelloStrategy")
    assert split_qualified("private::HelloStrategy") == ("private", "HelloStrategy")
    assert split_qualified("HelloStrategy") is None
    assert split_qualified("a::b::c") is None
