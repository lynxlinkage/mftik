"""Copy a strategy tree onto an STS registry that is not the API's disk."""

from __future__ import annotations

from pathlib import Path

import pytest
from mftik.protocol import StsRegistrySyncRequest, StsRegistryTreeOp
from mftik.registry import RegistryStore
from mftik_sts.impl import _REGISTRY
from mftik_sts.rpc.registry import SKIP_COLLISION, SKIP_DIGEST, apply_sync
from mftik_sts.runtime_env import reset_for_tests

_TINY = """\
from mftik.strategy import Strategy

class Tiny(Strategy):
    name = "tiny"
"""

_BAD = """\
from mftik.strategy import Strategy

class Tiny(Strategy):
    name = "tiny"

raise RuntimeError("boom")
"""

_COLLIDE = """\
from mftik.strategy import Strategy

class NoopStrategy(Strategy):
    name = "noop"
"""

_OTHER = """\
from mftik.strategy import Strategy

class Other(Strategy):
    name = "other"
"""


@pytest.fixture(autouse=True)
def _clean_registry():
    before = dict(_REGISTRY)
    yield
    _REGISTRY.clear()
    _REGISTRY.update(before)
    reset_for_tests()


def _upsert(
    origin: str, name: str, source: str, digest: str = ""
) -> StsRegistryTreeOp:
    return StsRegistryTreeOp(
        op="upsert",
        origin=origin,
        name=name,
        digest=digest,
        files={"strategy.py": source},
    )


def test_upsert_lands_on_this_stores_disk_and_loads(tmp_path: Path) -> None:
    store = RegistryStore(tmp_path)
    result = apply_sync(
        store, StsRegistrySyncRequest(trees=[_upsert("private", "Tiny", _TINY)])
    )
    assert "private::Tiny" in result.loaded
    written = tmp_path / "registry" / "private" / "Tiny" / "strategy.py"
    assert written.read_text() == _TINY
    assert result.skipped == {}


def test_delete_removes_the_tree_from_this_disk(tmp_path: Path) -> None:
    store = RegistryStore(tmp_path)
    apply_sync(
        store, StsRegistrySyncRequest(trees=[_upsert("private", "Tiny", _TINY)])
    )
    result = apply_sync(
        store,
        StsRegistrySyncRequest(
            trees=[StsRegistryTreeOp(op="delete", origin="private", name="Tiny")]
        ),
    )
    assert "private::Tiny" not in result.loaded
    assert not (tmp_path / "registry" / "private" / "Tiny").exists()


def test_delete_of_a_pulled_copy_uses_discard(tmp_path: Path) -> None:
    store = RegistryStore(tmp_path)
    apply_sync(
        store, StsRegistrySyncRequest(trees=[_upsert("node1", "Tiny", _TINY)])
    )
    assert (tmp_path / "registry" / "pulled" / "node1" / "Tiny").is_dir()
    apply_sync(
        store,
        StsRegistrySyncRequest(
            trees=[StsRegistryTreeOp(op="delete", origin="node1", name="Tiny")]
        ),
    )
    assert not (tmp_path / "registry" / "pulled" / "node1" / "Tiny").exists()


def test_a_broken_import_is_skipped_with_the_error(tmp_path: Path) -> None:
    store = RegistryStore(tmp_path)
    result = apply_sync(
        store, StsRegistrySyncRequest(trees=[_upsert("private", "Tiny", _BAD)])
    )
    assert "private::Tiny" not in result.loaded
    assert result.skipped["private::Tiny"].startswith("import error:")
    assert "boom" in result.skipped["private::Tiny"]


def test_a_bundled_name_is_a_collision(tmp_path: Path) -> None:
    store = RegistryStore(tmp_path)
    result = apply_sync(
        store,
        StsRegistrySyncRequest(trees=[_upsert("private", "NoopStrategy", _COLLIDE)]),
    )
    assert "private::NoopStrategy" not in result.loaded
    assert result.skipped["private::NoopStrategy"] == SKIP_COLLISION


def test_a_digest_mismatch_is_not_left_on_disk(tmp_path: Path) -> None:
    store = RegistryStore(tmp_path)
    result = apply_sync(
        store,
        StsRegistrySyncRequest(
            trees=[_upsert("private", "Tiny", _TINY, digest="sha256:not-the-tree")]
        ),
    )
    assert result.skipped["private::Tiny"] == SKIP_DIGEST
    assert "private::Tiny" not in result.loaded
    assert not (tmp_path / "registry" / "private" / "Tiny").exists()


_V2 = """\
from mftik.strategy import Strategy

class Tiny(Strategy):
    name = "tiny-v2"
"""


def test_a_digest_mismatch_keeps_the_previous_tree(tmp_path: Path) -> None:
    store = RegistryStore(tmp_path)
    apply_sync(
        store, StsRegistrySyncRequest(trees=[_upsert("private", "Tiny", _TINY)])
    )
    result = apply_sync(
        store,
        StsRegistrySyncRequest(
            trees=[_upsert("private", "Tiny", _V2, digest="sha256:not-the-tree")]
        ),
    )
    assert result.skipped["private::Tiny"] == SKIP_DIGEST
    written = tmp_path / "registry" / "private" / "Tiny" / "strategy.py"
    assert written.read_text() == _TINY
    assert "private::Tiny" in result.loaded


def test_a_rescan_retries_when_the_tree_was_missing_for_the_scan(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A peer mid-publish used to make this rescan unregister the type."""
    from mftik_sts.rpc import registry as registry_rpc

    store = RegistryStore(tmp_path)
    added = store.add({"strategy.py": _TINY})
    dest = tmp_path / "registry" / "private" / "Tiny"
    aside = tmp_path / "registry" / "private" / ".hidden-Tiny"
    calls = {"n": 0}
    real = registry_rpc.refresh

    def hiding(store: RegistryStore | None = None, data_dir: Path | None = None):
        calls["n"] += 1
        if calls["n"] == 1:
            dest.rename(aside)
            try:
                return real(store, data_dir=data_dir)
            finally:
                aside.rename(dest)
        return real(store, data_dir=data_dir)

    monkeypatch.setattr(registry_rpc, "refresh", hiding)
    result = apply_sync(
        store,
        StsRegistrySyncRequest(
            trees=[_upsert("private", "Tiny", _TINY, digest=added.digest)],
            reload=True,
        ),
    )
    assert calls["n"] >= 2
    assert "private::Tiny" in result.loaded
    assert "private::Tiny" not in result.skipped
    assert dest.is_dir()


def test_a_matching_digest_is_not_rewritten(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    store = RegistryStore(tmp_path)
    apply_sync(
        store, StsRegistrySyncRequest(trees=[_upsert("private", "Tiny", _TINY)])
    )
    digest = store.get_private("Tiny").digest

    def _refuse(*_args: object, **_kwargs: object) -> None:
        raise AssertionError("rewritten")

    monkeypatch.setattr(store, "add", _refuse)
    result = apply_sync(
        store,
        StsRegistrySyncRequest(
            trees=[_upsert("private", "Tiny", _TINY, digest=digest)]
        ),
    )
    assert result.skipped == {}
    assert "private::Tiny" in result.loaded


def test_a_path_refusal_is_not_called_an_import_error(tmp_path: Path) -> None:
    store = RegistryStore(tmp_path)
    result = apply_sync(
        store,
        StsRegistrySyncRequest(
            trees=[
                StsRegistryTreeOp(
                    op="upsert",
                    origin="private",
                    name="Tiny",
                    files={"../evil.py": "x = 1\n"},
                )
            ]
        ),
    )
    reason = result.skipped["private::Tiny"]
    assert reason.startswith("refused:")
    assert ".." in reason
    assert not reason.startswith("import error:")


def test_a_malformed_delete_does_not_drop_the_rest_of_the_batch(
    tmp_path: Path,
) -> None:
    store = RegistryStore(tmp_path)
    result = apply_sync(
        store,
        StsRegistrySyncRequest(
            trees=[
                StsRegistryTreeOp(op="delete", origin="node1", name="not-a-type"),
                _upsert("private", "Tiny", _TINY),
            ]
        ),
    )
    assert "private::Tiny" in result.loaded
    assert result.skipped["node1::not-a-type"].startswith("refused:")


def test_retain_deletes_a_tree_the_manifest_omits(tmp_path: Path) -> None:
    store = RegistryStore(tmp_path)
    apply_sync(
        store,
        StsRegistrySyncRequest(
            trees=[
                _upsert("private", "Tiny", _TINY),
                _upsert("private", "Other", _OTHER),
            ]
        ),
    )
    result = apply_sync(
        store,
        StsRegistrySyncRequest(trees=[], retain=["private::Tiny"]),
    )
    assert "private::Tiny" in result.loaded
    assert "private::Other" not in result.loaded
    assert not (tmp_path / "registry" / "private" / "Other").exists()
    assert (tmp_path / "registry" / "private" / "Tiny").is_dir()


def test_sync_uses_the_process_data_dir(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The handler's store is ``MFTIK_DATA``, not whoever called the API."""
    monkeypatch.setenv("MFTIK_DATA", str(tmp_path))
    other = RegistryStore(tmp_path / "api-only")
    other.add({"strategy.py": _TINY})
    store = RegistryStore.from_env()
    result = apply_sync(
        store, StsRegistrySyncRequest(trees=[_upsert("private", "Tiny", _TINY)])
    )
    assert store.data_dir == tmp_path
    assert "private::Tiny" in result.loaded
    assert (tmp_path / "registry" / "private" / "Tiny" / "strategy.py").is_file()
    assert (other.data_dir / "registry" / "private" / "Tiny").is_dir()


def test_last_batch_explains_an_earlier_batchs_import_error(tmp_path: Path) -> None:
    """A tree that rode in a non-reload batch still gets its import error."""
    store = RegistryStore(tmp_path)
    first = apply_sync(
        store,
        StsRegistrySyncRequest(
            trees=[_upsert("private", "Tiny", _BAD)], reload=False
        ),
    )
    assert first.skipped == {}
    last = apply_sync(
        store, StsRegistrySyncRequest(trees=[], explain=["private::Tiny"])
    )
    assert last.skipped["private::Tiny"].startswith("import error:")
    assert "boom" in last.skipped["private::Tiny"]
