"""Adding a tree writes ``.py`` files; identity is their digest."""

from __future__ import annotations

import os
from pathlib import Path

import pytest
from mftik.registry.digest import digest_files
from mftik.registry.errors import RegistryConflict, RegistryError
from mftik.registry.files import TEMPLATE_NAME
from mftik.registry.store import RegistryStore

_TINY = """\
from mftik.strategy import Strategy

class Tiny(Strategy):
    name = "tiny"
"""

_YML = "td: {}\nmd: []\nsts:\n  qty: 1\n"


def _touch_newer(path: Path) -> None:
    """Force ``st_mtime_ns`` strictly past its current value.

    An in-place rewrite can land in the same mtime bucket the store's
    ``_tree_cache`` is keyed on. Tests that assert invalidation must not
    race that resolution.
    """
    stat = path.stat()
    old = stat.st_mtime_ns
    os.utime(path, ns=(stat.st_atime_ns, old + 1))
    if path.stat().st_mtime_ns > old:
        return
    os.utime(path, ns=(stat.st_atime_ns, old + 1_000_000_000))
    assert path.stat().st_mtime_ns > old


def test_add_writes_source(tmp_path) -> None:
    store = RegistryStore(tmp_path)
    added = store.add({"strategy.py": _TINY})

    assert added.name == "Tiny"
    assert added.type == "Tiny"
    assert added.digest.startswith("sha256:")
    assert added.files == ("strategy.py",)
    dest = tmp_path / "registry" / "private" / "Tiny"
    assert (dest / "strategy.py").read_text() == _TINY
    assert not (dest / "mftik-strategy.toml").exists()
    assert digest_files({"strategy.py": _TINY.encode()}) == added.digest


def test_leftover_toml_is_ignored(tmp_path) -> None:
    store = RegistryStore(tmp_path)
    added = store.add(
        {
            "strategy.py": _TINY,
            "mftik-strategy.toml": "name = \"other\"\n",
            "README.md": "ignore me\n",
        }
    )
    assert added.name == "Tiny"
    assert added.files == ("strategy.py",)
    dest = tmp_path / "registry" / "private" / "Tiny"
    assert not (dest / "mftik-strategy.toml").exists()
    assert not (dest / "README.md").exists()


def test_same_class_in_one_origin_conflicts(tmp_path) -> None:
    store = RegistryStore(tmp_path)
    store.add({"strategy.py": _TINY})
    with pytest.raises(RegistryConflict, match="already"):
        store.add({"strategy.py": _TINY})


def test_casefold_collision_is_refused(tmp_path) -> None:
    """``tiny`` and ``Tiny`` are one directory on a case-insensitive volume.

    The check reads the names ``listdir`` returns. ``Path.exists`` would
    say the target is already there and then write into the existing tree.
    """
    store = RegistryStore(tmp_path)
    root = store.private_dir
    root.mkdir(parents=True)
    planted = root / "tiny"
    planted.mkdir()
    (planted / "strategy.py").write_text(_TINY)
    other = """\
from mftik.strategy import Strategy

class Tiny(Strategy):
    pass
"""
    with pytest.raises(RegistryConflict, match="collides with directory 'tiny'"):
        store.add({"strategy.py": other})
    assert (planted / "strategy.py").read_text() == _TINY


def test_replace_overwrites(tmp_path) -> None:
    store = RegistryStore(tmp_path)
    store.add({"strategy.py": _TINY})
    edited = _TINY.replace("tiny", "tiny") + "\n# changed\n"
    added = store.add({"strategy.py": edited}, replace=True)
    dest = tmp_path / "registry" / "private" / "Tiny"
    assert "# changed" in (dest / "strategy.py").read_text()
    assert added.digest != digest_files({"strategy.py": _TINY.encode()})


def test_no_strategy_subclass_is_refused(tmp_path) -> None:
    store = RegistryStore(tmp_path)
    with pytest.raises(RegistryError, match="no Strategy subclass"):
        store.add({"strategy.py": "x = 1\n"})


def test_a_class_without_a_name_attribute_is_stored(tmp_path) -> None:
    """The class name is the identity. ``name = "..."`` is not required."""
    store = RegistryStore(tmp_path)
    added = store.add(
        {
            "strategy.py": (
                "from mftik.strategy import Strategy\n"
                "class Tiny(Strategy):\n"
                "    pass\n"
            )
        }
    )
    assert added.name == "Tiny"
    assert added.type == "Tiny"
    assert (tmp_path / "registry" / "private" / "Tiny" / "strategy.py").is_file()


def test_requires_mftik_comes_from_the_class(tmp_path) -> None:
    store = RegistryStore(tmp_path)
    added = store.add(
        {
            "strategy.py": (
                "from mftik_sts.strategy import Strategy\n"
                "class Tiny(Strategy):\n"
                '    name = "tiny"\n'
                '    requires_mftik = "0.2.0"\n'
            )
        }
    )
    assert added.requires_mftik == "0.2.0"
    assert added.requires == ()


def test_requires_comes_from_the_class(tmp_path) -> None:
    store = RegistryStore(tmp_path)
    added = store.add(
        {
            "strategy.py": (
                "from mftik.strategy import Strategy\n"
                "class Tiny(Strategy):\n"
                '    name = "tiny"\n'
                '    requires = ("numpy",)\n'
            )
        }
    )
    assert added.requires == ("numpy",)
    listed = store.list_private()
    assert listed[0].requires == ("numpy",)


def test_parent_path_is_refused(tmp_path) -> None:
    store = RegistryStore(tmp_path)
    with pytest.raises(RegistryError, match=r"\.\."):
        store.add({"../outside.py": _TINY})


def test_pycache_is_ignored_and_does_not_change_digest(tmp_path) -> None:
    store = RegistryStore(tmp_path)
    added = store.add(
        {
            "strategy.py": _TINY,
            "__pycache__/strategy.cpython-312.pyc": b"junk",
        }
    )
    dest = tmp_path / "registry" / "private" / "Tiny"
    assert not (dest / "__pycache__").exists()
    assert added.digest == digest_files({"strategy.py": _TINY.encode()})


def test_multiple_subclasses_are_refused(tmp_path) -> None:
    source = """\
from mftik.strategy import Strategy

class One(Strategy):
    name = "one"

class Two(Strategy):
    name = "two"
"""
    store = RegistryStore(tmp_path)
    with pytest.raises(RegistryError, match="one tree, one subclass"):
        store.add({"strategy.py": source})


def test_list_private_reads_back_what_add_wrote(tmp_path) -> None:
    store = RegistryStore(tmp_path)
    store.add({"strategy.py": _TINY})
    listed = store.list_private()
    assert len(listed) == 1
    assert listed[0].name == "Tiny"
    assert listed[0].type == "Tiny"
    assert listed[0].origin == "private"
    assert listed[0].digest.startswith("sha256:")
    assert listed[0].files == ("strategy.py",)


def test_list_private_skips_junk(tmp_path) -> None:
    store = RegistryStore(tmp_path)
    (store.private_dir / ".tmp-x").mkdir(parents=True)
    (store.private_dir / "empty").mkdir(parents=True)
    broken = store.private_dir / "broken"
    broken.mkdir(parents=True)
    (broken / "strategy.py").write_text("def (\n")
    leftover = store.private_dir / "onlytoml"
    leftover.mkdir(parents=True)
    (leftover / "mftik-strategy.toml").write_text("name = \"x\"\n")
    assert store.list_private() == []


def test_leftover_toml_on_disk_is_not_listed(tmp_path) -> None:
    store = RegistryStore(tmp_path)
    store.add({"strategy.py": _TINY})
    dest = tmp_path / "registry" / "private" / "Tiny"
    (dest / "mftik-strategy.toml").write_text("name = \"other\"\n")
    listed = store.list_private()
    assert listed[0].files == ("strategy.py",)
    assert listed[0].name == "Tiny"


def test_directory_that_is_not_the_class_is_junk(tmp_path) -> None:
    """A scan that kept this tree would hide a rename that has not happened.

    The directory is ``Tiny`` and the class is no longer ``Tiny``. Listing
    returns nothing, and neither spelling answers a get.
    """
    store = RegistryStore(tmp_path)
    store.add({"strategy.py": _TINY})
    dest = tmp_path / "registry" / "private" / "Tiny"
    py = dest / "strategy.py"
    py.write_text(_TINY.replace("class Tiny(", "class Bar("))
    _touch_newer(py)
    assert store.list_private() == []
    assert store.get_private("Tiny") is None
    assert store.get_private("Bar") is None


def test_read_tree_cache_hits_until_mtime_changes(tmp_path) -> None:
    store = RegistryStore(tmp_path)
    store.add({"strategy.py": _TINY})
    first = store.list_private()[0]
    second = store.list_private()[0]
    assert first is second
    dest = tmp_path / "registry" / "private" / "Tiny" / "strategy.py"
    dest.write_text(_TINY + "\n# edited\n")
    _touch_newer(dest)
    third = store.list_private()[0]
    assert third is not first
    assert third.digest != first.digest
    assert store.get_private("Tiny") is third


_TORCH = """\
from mftik.strategy import Strategy

class Tiny(Strategy):
    name = "tiny"
    requires = ("torch",)
"""


def test_own_add_refuses_missing_applied_extras(tmp_path) -> None:
    store = RegistryStore(tmp_path)
    with pytest.raises(RegistryError, match="torch"):
        store.add({"strategy.py": _TORCH}, applied_extras={})
    assert not (tmp_path / "registry" / "private" / "Tiny").exists()


def test_own_add_accepts_applied_extras(tmp_path) -> None:
    store = RegistryStore(tmp_path)
    added = store.add({"strategy.py": _TORCH}, applied_extras={"torch": "2.0"})
    assert added.requires == ("torch",)


def test_remote_add_skips_applied_extras_check(tmp_path) -> None:
    store = RegistryStore(tmp_path)
    store.put_remote("node1", "http://peer:8000")
    added = store.add(
        {"strategy.py": _TORCH},
        origin="node1",
        applied_extras={},
    )
    assert added.origin == "node1"
    dest = tmp_path / "registry" / "pulled" / "node1" / "Tiny"
    assert dest.is_dir()


def test_add_with_origin_writes_pulled(tmp_path) -> None:
    store = RegistryStore(tmp_path)
    added = store.add({"strategy.py": _TINY}, origin="node1")
    dest = tmp_path / "registry" / "pulled" / "node1" / "Tiny"
    assert dest.is_dir()
    assert added.origin == "node1"
    assert store.list_private() == []
    assert store.list_public() == []
    pulled = store.list_pulled()
    assert len(pulled) == 1
    assert pulled[0].origin == "node1"
    assert pulled[0].name == "Tiny"


def test_put_and_list_remotes(tmp_path) -> None:
    store = RegistryStore(tmp_path)
    store.put_remote("node1", "http://host.docker.internal:8000")
    remotes = store.list_remotes()
    assert len(remotes) == 1
    assert remotes[0].name == "node1"
    assert remotes[0].url == "http://host.docker.internal:8000"


def test_drop_remote_forgets_the_url_and_the_copy(tmp_path) -> None:
    store = RegistryStore(tmp_path)
    store.add({"strategy.py": _TINY}, origin="node1")
    store.put_remote("node1", "http://host.docker.internal:8000")
    dest = tmp_path / "registry" / "pulled" / "node1"
    assert dest.is_dir()
    dropped = store.drop_remote("node1")
    assert dropped.name == "node1"
    assert store.list_remotes() == []
    assert store.list_pulled() == []
    assert not dest.exists()


def test_drop_unknown_remote_is_refused(tmp_path) -> None:
    store = RegistryStore(tmp_path)
    with pytest.raises(RegistryError, match="unknown remote"):
        store.drop_remote("node1")


def test_add_public_is_not_private(tmp_path) -> None:
    store = RegistryStore(tmp_path)
    added = store.add({"strategy.py": _TINY}, origin="public")
    dest = tmp_path / "registry" / "public" / "Tiny"
    assert dest.is_dir()
    assert added.origin == "public"
    assert [r.name for r in store.list_public()] == ["Tiny"]
    assert store.list_private() == []


def test_public_and_private_can_share_a_name(tmp_path) -> None:
    store = RegistryStore(tmp_path)
    private = store.add({"strategy.py": _TINY})
    public = store.add({"strategy.py": _TINY}, origin="public")
    assert private.origin == "private"
    assert public.origin == "public"
    assert store.get_private("Tiny") is not None
    assert store.get_public("Tiny") is not None


def test_remote_names_this_node_uses_are_reserved(tmp_path) -> None:
    store = RegistryStore(tmp_path)
    for name in ("local", "public", "private"):
        with pytest.raises(RegistryError, match="reserved"):
            store.put_remote(name, "http://example")


def test_read_contents_is_the_python(tmp_path) -> None:
    store = RegistryStore(tmp_path)
    added = store.add({"strategy.py": _TINY})
    contents = store.read_contents(added)
    assert contents == {"strategy.py": _TINY}


def test_add_writes_strategy_yml_without_changing_digest(tmp_path) -> None:
    store = RegistryStore(tmp_path)
    added = store.add({"strategy.py": _TINY, TEMPLATE_NAME: _YML})
    dest = tmp_path / "registry" / "private" / "Tiny"
    assert (dest / TEMPLATE_NAME).read_text() == _YML
    assert added.files == ("strategy.py", TEMPLATE_NAME)
    assert added.digest == digest_files({"strategy.py": _TINY.encode()})
    assert store.read_contents(added)[TEMPLATE_NAME] == _YML
    assert store.read_template(added) == _YML


def test_replace_without_yml_drops_the_old_template(tmp_path) -> None:
    store = RegistryStore(tmp_path)
    store.add({"strategy.py": _TINY, TEMPLATE_NAME: _YML})
    added = store.add({"strategy.py": _TINY}, replace=True)
    dest = tmp_path / "registry" / "private" / "Tiny"
    assert not (dest / TEMPLATE_NAME).exists()
    assert added.files == ("strategy.py",)
    assert store.read_template(added) is None


def test_bad_strategy_yml_is_refused(tmp_path) -> None:
    store = RegistryStore(tmp_path)
    with pytest.raises(RegistryError, match="strategy.yml"):
        store.add({"strategy.py": _TINY, TEMPLATE_NAME: "td: [\n"})


def test_two_writers_leave_one_complete_tree(tmp_path) -> None:
    """Pid-1 containers used to share ``.tmp-{name}-1`` and delete each other."""
    import threading

    store = RegistryStore(tmp_path)
    first = _TINY + "\n# a\n"
    second = _TINY + "\n# b\n"
    store.add({"strategy.py": first})
    barrier = threading.Barrier(2)
    errors: list[BaseException] = []

    def write(body: str) -> None:
        try:
            barrier.wait(timeout=5)
            for _ in range(40):
                store.add({"strategy.py": body}, replace=True)
        except BaseException as exc:
            errors.append(exc)

    threads = [
        threading.Thread(target=write, args=(body,)) for body in (first, second)
    ]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=30)
        assert not thread.is_alive()
    assert errors == []
    text = (tmp_path / "registry" / "private" / "Tiny" / "strategy.py").read_text()
    assert text in {first, second}
    private = tmp_path / "registry" / "private"
    assert list(private.glob(".tmp-*")) == []
    assert list(private.glob(".old-*")) == []


def test_yml_mtime_invalidates_the_tree_cache(tmp_path) -> None:
    store = RegistryStore(tmp_path)
    store.add({"strategy.py": _TINY})
    first = store.list_private()[0]
    assert store.read_template(first) is None
    dest = tmp_path / "registry" / "private" / "Tiny" / TEMPLATE_NAME
    dest.write_text(_YML)
    _touch_newer(dest)
    second = store.list_private()[0]
    assert second is not first
    assert TEMPLATE_NAME in second.files
    assert store.read_template(second) == _YML
