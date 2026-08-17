"""Re-reading the registry under a running STS.

The registry is written by the API process and imported by this one. Between
boots it used to be able to say anything at all and STS would not notice; now
it is re-read on demand, and what that has to get right is both directions —
a tree that changed, and a tree that went away.
"""

from __future__ import annotations

import pytest
from mftik.registry import RegistryStore
from mftik_sts.impl import (
    _REGISTRY,
    load_local_registry,
    resolve_class,
)
from mftik_sts.impl.noop import NoopStrategy

_TINY = """\
from mftik.strategy import Strategy

class Tiny(Strategy):
    name = "tiny"
"""


def _tiny(marker: str) -> str:
    return (
        "from mftik.strategy import Strategy\n\n"
        "class Tiny(Strategy):\n"
        '    name = "tiny"\n'
        f'    marker = "{marker}"\n'
    )


@pytest.fixture(autouse=True)
def _clean_registry():
    """``_REGISTRY`` is module state, and these tests add and remove keys."""
    before = dict(_REGISTRY)
    yield
    _REGISTRY.clear()
    _REGISTRY.update(before)


def test_a_replaced_tree_resolves_to_its_new_code(tmp_path) -> None:
    """The whole point: edit, push, run, and get what you just wrote."""
    store = RegistryStore(tmp_path)
    store.add({"strategy.py": _tiny("before")})
    load_local_registry(store)
    assert resolve_class("private::Tiny").marker == "before"

    store.add({"strategy.py": _tiny("after")}, replace=True)
    load_local_registry(store)

    assert resolve_class("private::Tiny").marker == "after"


def test_a_removed_tree_stops_resolving(tmp_path) -> None:
    """Deleting the source has to stop the deploy, not just the listing.

    A key left behind builds a session from a class whose source is not on
    disk anywhere — the operator deleted it precisely so that could not
    happen.
    """
    store = RegistryStore(tmp_path)
    store.add({"strategy.py": _TINY})
    load_local_registry(store)
    assert resolve_class("private::Tiny").__name__ == "Tiny"

    store.remove("tiny")
    loaded = load_local_registry(store)

    assert loaded == []
    with pytest.raises(KeyError):
        resolve_class("private::Tiny")


def test_a_disconnected_remote_stops_resolving(tmp_path) -> None:
    store = RegistryStore(tmp_path)
    store.add({"strategy.py": _TINY}, origin="node1")
    store.put_remote("node1", "http://peer:8000")
    load_local_registry(store)
    assert resolve_class("node1::Tiny").__name__ == "Tiny"

    store.drop_remote("node1")
    load_local_registry(store)

    with pytest.raises(KeyError):
        resolve_class("node1::Tiny")


def test_a_tree_that_stops_importing_stops_resolving(tmp_path) -> None:
    """It was loadable and now is not.

    Going on answering to the key would deploy the last version that happened
    to parse, which is a version nobody asked to run.
    """
    store = RegistryStore(tmp_path)
    store.add({"strategy.py": _TINY})
    load_local_registry(store)
    assert resolve_class("private::Tiny").__name__ == "Tiny"

    store.add(
        {
            "strategy.py": (
                "from mftik.strategy import Strategy\n"
                "class Tiny(Strategy):\n"
                '    name = "tiny"\n'
                "\n"
                'raise RuntimeError("boom")\n'
            )
        },
        replace=True,
    )
    load_local_registry(store)

    with pytest.raises(KeyError):
        resolve_class("private::Tiny")


def test_reloading_never_unregisters_a_bundled_strategy(tmp_path) -> None:
    """They come from this package, not from the store.

    An empty registry directory is the normal state of a fresh node, and it
    must not be read as "the bundled strategies are gone".
    """
    store = RegistryStore(tmp_path)

    assert load_local_registry(store) == []

    assert resolve_class("noop") is NoopStrategy
    assert resolve_class("NoopStrategy") is NoopStrategy


def test_reloading_leaves_an_unchanged_tree_alone(tmp_path) -> None:
    store = RegistryStore(tmp_path)
    store.add({"strategy.py": _TINY})
    load_local_registry(store)
    first = resolve_class("private::Tiny")

    load_local_registry(store)

    assert resolve_class("private::Tiny") is first
