"""Local registry trees become resolvable strategy classes on STS boot."""

from __future__ import annotations

import pytest
from mftik.registry import RegistryStore
from mftik.registry.migrate import migrate_registry
from mftik_sts.impl import load_local_registry, resolve, resolve_class
from mftik_sts.impl.noop import NoopStrategy

_TINY = """\
from mftik.strategy import Strategy

class Tiny(Strategy):
    name = "tiny"
"""


def test_add_then_load_is_resolvable(tmp_path) -> None:
    store = RegistryStore(tmp_path)
    store.add({"strategy.py": _TINY})
    loaded = load_local_registry(store)
    assert "private::Tiny" in loaded
    assert resolve("private::Tiny").name == "tiny"
    assert resolve_class("private::Tiny").__name__ == "Tiny"


def test_a_leftover_name_attribute_does_not_claim_a_bundled_key(tmp_path) -> None:
    """``name = "noop"`` is not an identity. The class is ``Hijack``."""
    store = RegistryStore(tmp_path)
    store.add(
        {
            "strategy.py": (
                "from mftik.strategy import Strategy\n"
                "class Hijack(Strategy):\n"
                '    name = "noop"\n'
            )
        }
    )
    loaded = load_local_registry(store)
    assert loaded == ["private::Hijack"]
    assert resolve_class("NoopStrategy") is NoopStrategy
    assert resolve_class("private::Hijack").__name__ == "Hijack"


def test_a_bundled_class_name_is_not_overwritten(tmp_path) -> None:
    store = RegistryStore(tmp_path)
    store.add(
        {
            "strategy.py": (
                "from mftik.strategy import Strategy\n"
                "class NoopStrategy(Strategy):\n"
                "    pass\n"
            )
        }
    )
    loaded = load_local_registry(store)
    assert loaded == []
    assert resolve_class("NoopStrategy") is NoopStrategy


def test_a_broken_tree_does_not_block_the_others(tmp_path) -> None:
    store = RegistryStore(tmp_path)
    store.add({"strategy.py": _TINY})
    store.add(
        {
            "strategy.py": (
                "from mftik_sts.strategy import Strategy\n"
                "class Broken(Strategy):\n"
                '    name = "broken"\n'
                "\n"
                'raise RuntimeError("boom")\n'
            )
        }
    )
    loaded = load_local_registry(store)
    assert loaded == ["private::Tiny"]
    assert resolve("private::Tiny").name == "tiny"


def test_pulled_tree_is_qualified_with_the_remote_name(tmp_path) -> None:
    store = RegistryStore(tmp_path)
    store.add({"strategy.py": _TINY}, origin="node1")
    loaded = load_local_registry(store)
    assert loaded == ["node1::Tiny"]
    assert resolve("node1::Tiny").name == "tiny"


def test_public_and_private_both_load(tmp_path) -> None:
    store = RegistryStore(tmp_path)
    store.add({"strategy.py": _TINY})
    store.add({"strategy.py": _TINY}, origin="public")
    loaded = load_local_registry(store)
    assert loaded == ["public::Tiny", "private::Tiny"]
    assert resolve("public::Tiny").name == "tiny"
    assert resolve("private::Tiny").name == "tiny"


# loads and resolves a tree; over the 50 ms unit call cap
@pytest.mark.component
def test_a_pulled_copy_resolves_once_the_migration_has_renamed_it(
    tmp_path,
) -> None:
    """The state a node upgrades from, and the state it boots into.

    A copy pulled under the old short name is invisible: the scan drops a
    directory whose name is not the class. The boot scan is also the only
    thing that restores an interrupted session, so ``node1::Tiny`` has to
    resolve when STS starts — running ``connect`` again afterwards is too
    late for a session that was running it.
    """
    old = tmp_path / "registry" / "pulled" / "node1" / "tiny"
    old.mkdir(parents=True)
    (old / "strategy.py").write_text(_TINY)

    store = RegistryStore(tmp_path)
    assert load_local_registry(store) == []

    migrate_registry(tmp_path)

    assert load_local_registry(store) == ["node1::Tiny"]
    assert resolve("node1::Tiny").name == "tiny"
