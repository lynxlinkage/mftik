"""Adding a tree writes ``.py`` files; identity is their digest."""

from __future__ import annotations

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

_YML = "td: []\nmd: []\nsts:\n  qty: 1\n"


def test_add_writes_source(tmp_path) -> None:
    store = RegistryStore(tmp_path)
    added = store.add({"strategy.py": _TINY})

    assert added.name == "tiny"
    assert added.type == "Tiny"
    assert added.digest.startswith("sha256:")
    assert added.files == ("strategy.py",)
    dest = tmp_path / "registry" / "private" / "tiny"
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
    assert added.name == "tiny"
    assert added.files == ("strategy.py",)
    dest = tmp_path / "registry" / "private" / "tiny"
    assert not (dest / "mftik-strategy.toml").exists()
    assert not (dest / "README.md").exists()


def test_second_add_of_the_same_name_conflicts(tmp_path) -> None:
    store = RegistryStore(tmp_path)
    store.add({"strategy.py": _TINY})
    with pytest.raises(RegistryConflict, match="already"):
        store.add({"strategy.py": _TINY})


def test_replace_overwrites(tmp_path) -> None:
    store = RegistryStore(tmp_path)
    store.add({"strategy.py": _TINY})
    edited = _TINY.replace("tiny", "tiny") + "\n# changed\n"
    added = store.add({"strategy.py": edited}, replace=True)
    dest = tmp_path / "registry" / "private" / "tiny"
    assert "# changed" in (dest / "strategy.py").read_text()
    assert added.digest != digest_files({"strategy.py": _TINY.encode()})


def test_no_strategy_subclass_is_refused(tmp_path) -> None:
    store = RegistryStore(tmp_path)
    with pytest.raises(RegistryError, match="no Strategy subclass"):
        store.add({"strategy.py": "x = 1\n"})


def test_missing_name_is_refused(tmp_path) -> None:
    store = RegistryStore(tmp_path)
    with pytest.raises(RegistryError, match="has no name"):
        store.add(
            {
                "strategy.py": (
                    "from mftik_sts.strategy import Strategy\n"
                    "class Tiny(Strategy):\n"
                    "    pass\n"
                )
            }
        )


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
    dest = tmp_path / "registry" / "private" / "tiny"
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
    assert listed[0].name == "tiny"
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
    dest = tmp_path / "registry" / "private" / "tiny"
    (dest / "mftik-strategy.toml").write_text("name = \"other\"\n")
    listed = store.list_private()
    assert listed[0].files == ("strategy.py",)
    assert listed[0].name == "tiny"


def test_class_name_mismatch_is_junk(tmp_path) -> None:
    store = RegistryStore(tmp_path)
    store.add({"strategy.py": _TINY})
    dest = tmp_path / "registry" / "private" / "tiny"
    (dest / "strategy.py").write_text(_TINY.replace('name = "tiny"', 'name = "bar"'))
    assert store.list_private() == []
    assert store.get_private("tiny") is None
    assert store.get_private("bar") is None


def test_read_tree_cache_hits_until_mtime_changes(tmp_path) -> None:
    store = RegistryStore(tmp_path)
    store.add({"strategy.py": _TINY})
    first = store.list_private()[0]
    second = store.list_private()[0]
    assert first is second
    dest = tmp_path / "registry" / "private" / "tiny" / "strategy.py"
    dest.write_text(_TINY + "\n# edited\n")
    third = store.list_private()[0]
    assert third is not first
    assert third.digest != first.digest
    assert store.get_private("tiny") is third


def test_add_with_origin_writes_pulled(tmp_path) -> None:
    store = RegistryStore(tmp_path)
    added = store.add({"strategy.py": _TINY}, origin="node1")
    dest = tmp_path / "registry" / "pulled" / "node1" / "tiny"
    assert dest.is_dir()
    assert added.origin == "node1"
    assert store.list_private() == []
    assert store.list_public() == []
    pulled = store.list_pulled()
    assert len(pulled) == 1
    assert pulled[0].origin == "node1"
    assert pulled[0].name == "tiny"


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
    dest = tmp_path / "registry" / "public" / "tiny"
    assert dest.is_dir()
    assert added.origin == "public"
    assert [r.name for r in store.list_public()] == ["tiny"]
    assert store.list_private() == []


def test_public_and_private_can_share_a_name(tmp_path) -> None:
    store = RegistryStore(tmp_path)
    private = store.add({"strategy.py": _TINY})
    public = store.add({"strategy.py": _TINY}, origin="public")
    assert private.origin == "private"
    assert public.origin == "public"
    assert store.get_private("tiny") is not None
    assert store.get_public("tiny") is not None


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
    dest = tmp_path / "registry" / "private" / "tiny"
    assert (dest / TEMPLATE_NAME).read_text() == _YML
    assert added.files == ("strategy.py", TEMPLATE_NAME)
    assert added.digest == digest_files({"strategy.py": _TINY.encode()})
    assert store.read_contents(added)[TEMPLATE_NAME] == _YML
    assert store.read_template(added) == _YML


def test_replace_without_yml_drops_the_old_template(tmp_path) -> None:
    store = RegistryStore(tmp_path)
    store.add({"strategy.py": _TINY, TEMPLATE_NAME: _YML})
    added = store.add({"strategy.py": _TINY}, replace=True)
    dest = tmp_path / "registry" / "private" / "tiny"
    assert not (dest / TEMPLATE_NAME).exists()
    assert added.files == ("strategy.py",)
    assert store.read_template(added) is None


def test_bad_strategy_yml_is_refused(tmp_path) -> None:
    store = RegistryStore(tmp_path)
    with pytest.raises(RegistryError, match="strategy.yml"):
        store.add({"strategy.py": _TINY, TEMPLATE_NAME: "td: [\n"})


def test_yml_mtime_invalidates_the_tree_cache(tmp_path) -> None:
    store = RegistryStore(tmp_path)
    store.add({"strategy.py": _TINY})
    first = store.list_private()[0]
    assert store.read_template(first) is None
    dest = tmp_path / "registry" / "private" / "tiny" / TEMPLATE_NAME
    dest.write_text(_YML)
    second = store.list_private()[0]
    assert second is not first
    assert TEMPLATE_NAME in second.files
    assert store.read_template(second) == _YML
