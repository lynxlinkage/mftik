"""Deleting a tree this node owns."""

from __future__ import annotations

from pathlib import Path

import pytest
from mftik.registry import RegistryStore
from mftik.registry.errors import RegistryError

_TINY = """\
from mftik.strategy import Strategy

class Tiny(Strategy):
    name = "tiny"
"""


def test_remove_deletes_the_tree_and_returns_it(tmp_path: Path) -> None:
    store = RegistryStore(tmp_path)
    added = store.add({"strategy.py": _TINY})

    removed = store.remove("Tiny")

    assert removed.name == "Tiny"
    assert removed.type == "Tiny"
    assert removed.digest == added.digest
    assert not Path(added.path).exists()
    assert store.list_private() == []


def test_the_two_own_origins_are_separate(tmp_path: Path) -> None:
    """Same name in public and private is legal, so a delete must pick one."""
    store = RegistryStore(tmp_path)
    store.add({"strategy.py": _TINY}, origin="private")
    store.add({"strategy.py": _TINY}, origin="public")

    store.remove("Tiny", origin="private")

    assert store.list_private() == []
    assert [r.name for r in store.list_public()] == ["Tiny"]


def test_removing_what_is_not_there_says_so(tmp_path: Path) -> None:
    store = RegistryStore(tmp_path)

    with pytest.raises(RegistryError, match="no private strategy named 'Tiny'"):
        store.remove("Tiny")


def test_a_pulled_copy_is_not_removable_one_tree_at_a_time(
    tmp_path: Path,
) -> None:
    """It mirrors what a peer publishes; a hole in it is not a state to be in.

    The next diff would report the strategy missing and the next connect would
    put it back, so a partial mirror is a thing that repairs itself into the
    state the operator was trying to leave.
    """
    store = RegistryStore(tmp_path)
    store.add({"strategy.py": _TINY}, origin="node1")

    with pytest.raises(RegistryError, match="disconnect the remote"):
        store.remove("Tiny", origin="node1")

    assert [r.name for r in store.list_pulled()] == ["Tiny"]


def test_a_removed_name_can_be_added_again(tmp_path: Path) -> None:
    """The conflict check reads the directory, so the cache must not outlive it."""
    store = RegistryStore(tmp_path)
    store.add({"strategy.py": _TINY})
    store.remove("Tiny")

    again = store.add({"strategy.py": _TINY})

    assert again.name == "Tiny"
    assert [r.name for r in store.list_private()] == ["Tiny"]


def test_a_removed_tree_is_gone_from_the_cached_listing(tmp_path: Path) -> None:
    """``list_*`` caches per tree, and a delete has to invalidate that."""
    store = RegistryStore(tmp_path)
    store.add({"strategy.py": _TINY})
    assert [r.name for r in store.list_all()] == ["Tiny"]

    store.remove("Tiny")

    assert store.list_all() == []
