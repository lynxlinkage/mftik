"""Loading a tree whose contents changed since the last load.

The registry is a directory a *different* process writes to. Nothing about a
path changes when the source behind it does, so a loader keyed on the path
alone hands back the module it built the first time — successfully, with no
error anywhere — and the strategy that runs is the one from before the edit.
"""

from __future__ import annotations

from pathlib import Path

import pytest
from mftik.registry.digest import digest_files
from mftik.registry.errors import RegistryError
from mftik.registry.files import normalize_files
from mftik.registry.load import load_class


def _write(dest: Path, marker: str) -> str:
    """Write a one-class tree carrying ``marker``, and return its digest."""
    dest.mkdir(parents=True, exist_ok=True)
    source = (
        "from mftik.strategy import Strategy\n\n"
        "class Tiny(Strategy):\n"
        '    name = "tiny"\n'
        f'    marker = "{marker}"\n'
    )
    (dest / "strategy.py").write_text(source)
    return digest_files(normalize_files({"strategy.py": source}))


def test_a_replaced_tree_loads_its_new_code(tmp_path: Path) -> None:
    dest = tmp_path / "tiny"
    first = _write(dest, "before")
    loaded = load_class(dest, type_name="Tiny", source="private", name="tiny",
                        digest=first)
    assert loaded.marker == "before"

    second = _write(dest, "after")
    assert second != first
    reloaded = load_class(dest, type_name="Tiny", source="private", name="tiny",
                          digest=second)

    assert reloaded.marker == "after"
    assert reloaded is not loaded


def test_the_same_tree_twice_is_the_same_class(tmp_path: Path) -> None:
    """Reloading an unchanged tree must not rebuild it.

    A new class object each time would make ``isinstance`` against a live
    session's strategy false, and re-execute module-level state for nothing.
    """
    dest = tmp_path / "tiny"
    digest = _write(dest, "same")

    first = load_class(dest, type_name="Tiny", source="private", name="tiny",
                       digest=digest)
    second = load_class(dest, type_name="Tiny", source="private", name="tiny",
                        digest=digest)

    assert first is second


def test_without_a_digest_a_replaced_tree_keeps_the_old_class(
    tmp_path: Path,
) -> None:
    """The behaviour the digest exists to fix, pinned so it cannot come back.

    Callers that reload must pass one. This asserts what omitting it costs,
    rather than leaving the parameter looking optional in the sense of
    "either way is fine".
    """
    dest = tmp_path / "stale"
    _write(dest, "before")
    first = load_class(dest, type_name="Tiny", source="private", name="stale")

    _write(dest, "after")
    second = load_class(dest, type_name="Tiny", source="private", name="stale")

    assert second is first
    assert second.marker == "before"


def test_two_trees_at_different_paths_stay_separate(tmp_path: Path) -> None:
    a_digest = _write(tmp_path / "a" / "tiny", "a")
    b_digest = _write(tmp_path / "b" / "tiny", "b")

    a = load_class(tmp_path / "a" / "tiny", type_name="Tiny", source="private",
                   name="tiny", digest=a_digest)
    b = load_class(tmp_path / "b" / "tiny", type_name="Tiny", source="private",
                   name="tiny", digest=b_digest)

    assert a.marker == "a"
    assert b.marker == "b"


def test_a_missing_tree_is_a_registry_error(tmp_path: Path) -> None:
    with pytest.raises(RegistryError, match="does not exist"):
        load_class(tmp_path / "gone", type_name="Tiny", source="private",
                   name="gone", digest="sha256:whatever")
