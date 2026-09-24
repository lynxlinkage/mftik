"""The artifact store: one key, one file, replaced whole."""

from __future__ import annotations

import asyncio
import hashlib
import os
import time
from pathlib import Path
from types import SimpleNamespace

import pytest
from mftik.strategy.artifacts import (
    DIR_ENV,
    ArtifactNotFound,
    ArtifactStore,
    ArtifactUploadError,
    BadArtifactKey,
    StrategyArtifacts,
    get_store,
    reset_store,
)
from mftik.strategy.base import Strategy


def _store(tmp_path: Path) -> ArtifactStore:
    return ArtifactStore(tmp_path)


def test_a_write_is_what_a_read_returns(tmp_path: Path) -> None:
    store = _store(tmp_path)
    meta = store.write("weights/model.pt", b"weights")

    opened = store.read("weights/model.pt")
    assert opened is not None
    assert opened.body == b"weights"
    assert opened.size == len(b"weights")
    assert opened.path == "weights/model.pt"
    assert opened.digest == "sha256:" + hashlib.sha256(b"weights").hexdigest()
    assert meta.digest == opened.digest

    stated = store.stat("weights/model.pt")
    assert stated is not None
    assert stated.digest == opened.digest
    assert stated.size == opened.size


def test_a_missing_key_is_none_and_a_bad_key_is_refused(tmp_path: Path) -> None:
    store = _store(tmp_path)
    assert store.read("weights/model.pt") is None
    assert store.stat("weights/model.pt") is None
    for key in ("/abs", "a/../b", "a//b", "a/./b", "a/\x00b", "..", ""):
        with pytest.raises(BadArtifactKey):
            store.read(key)


def test_replace_leaves_the_previous_object_until_it_finishes(tmp_path: Path) -> None:
    store = _store(tmp_path)
    store.write("weights/model.pt", b"first")
    store.write("weights/model.pt", b"second")
    opened = store.read("weights/model.pt")
    assert opened is not None
    assert opened.body == b"second"
    # The part file is not an object, and it does not survive the replace.
    assert list(tmp_path.rglob("*.part")) == []


def test_two_uploads_of_one_key_do_not_share_a_part_file(tmp_path: Path) -> None:
    store = _store(tmp_path)
    first = store.begin("weights/model.pt")
    second = store.begin("weights/model.pt")
    store.chunk(first, 0, b"aaaa")
    store.chunk(second, 0, b"bbbb")
    # A retried chunk overwrites the same offset instead of appending.
    store.chunk(first, 0, b"aaaa")
    landed = store.commit(first)
    assert landed.size == 4
    assert store.read("weights/model.pt").body == b"aaaa"  # type: ignore[union-attr]
    other = store.commit(second)
    assert other.digest != landed.digest
    assert store.read("weights/model.pt").body == b"bbbb"  # type: ignore[union-attr]


def test_an_aborted_upload_does_not_become_an_object(tmp_path: Path) -> None:
    store = _store(tmp_path)
    token = store.begin("weights/model.pt")
    store.chunk(token, 0, b"nope")
    store.abort(token)
    assert store.read("weights/model.pt") is None
    with pytest.raises(ArtifactUploadError):
        store.commit(token)


def test_catalog_hides_the_session_tree_and_operators_cannot_write_it(
    tmp_path: Path,
) -> None:
    store = _store(tmp_path)
    store.write("weights/model.pt", b"uploaded")
    store.write("sessions/abc/weights/model.pt", b"session")

    listed = [row.path for row in store.list_catalog()]
    assert listed == ["weights/model.pt"]
    session = [row.path for row in store.list_session("abc")]
    assert session == ["sessions/abc/weights/model.pt"]

    with pytest.raises(BadArtifactKey):
        store.begin("sessions/abc/weights/model.pt")
    with pytest.raises(BadArtifactKey):
        store.remove("sessions/abc/weights/model.pt")
    store.remove("weights/model.pt")
    assert store.read("weights/model.pt") is None
    assert store.read("sessions/abc/weights/model.pt") is not None


def test_remove_of_a_missing_key_is_not_found(tmp_path: Path) -> None:
    store = _store(tmp_path)
    with pytest.raises(ArtifactNotFound):
        store.remove("weights/model.pt")


def test_a_symlink_that_leaves_the_store_is_not_an_object(tmp_path: Path) -> None:
    store = _store(tmp_path)
    outside = tmp_path.parent / "outside-artifact"
    outside.mkdir()
    (outside / "secret").write_bytes(b"no")
    (tmp_path / "escape").symlink_to(outside)
    assert store.read("escape/secret") is None
    with pytest.raises(BadArtifactKey):
        store.write("escape/secret", b"no")


def test_idle_part_files_are_swept(tmp_path: Path) -> None:
    store = _store(tmp_path)
    token = store.begin("weights/model.pt")
    part = next(tmp_path.rglob("*.part"))
    old = time.time() - 7200
    os.utime(part, (old, old))
    assert store.sweep_parts(now=time.time()) == 1
    assert list(tmp_path.rglob("*.part")) == []
    with pytest.raises(ArtifactUploadError):
        store.chunk(token, 0, b"x")


def test_a_fresh_part_file_is_kept(tmp_path: Path) -> None:
    store = _store(tmp_path)
    store.begin("weights/model.pt")
    assert store.sweep_parts() == 0
    assert list(tmp_path.rglob("*.part"))


def test_read_at_returns_a_slice_and_says_when_it_ends(tmp_path: Path) -> None:
    store = _store(tmp_path)
    store.write("weights/model.pt", b"abcdefghij")
    data, eof = store.read_at("weights/model.pt", 0, 4)
    assert data == b"abcd"
    assert eof is False
    data, eof = store.read_at("weights/model.pt", 8, 4)
    assert data == b"ij"
    assert eof is True
    with pytest.raises(ArtifactNotFound):
        store.read_at("missing", 0, 4)


def test_the_digest_is_cached_on_the_file_identity(tmp_path: Path, monkeypatch) -> None:
    store = _store(tmp_path)
    store.write("weights/model.pt", b"weights")
    calls = {"n": 0}
    real = hashlib.sha256

    def counting() -> hashlib._Hash:
        calls["n"] += 1
        return real()

    monkeypatch.setattr(hashlib, "sha256", counting)
    assert store.stat("weights/model.pt") is not None
    assert store.stat("weights/model.pt") is not None
    assert calls["n"] == 0


@pytest.mark.asyncio
async def test_an_unbound_strategy_cannot_touch_the_store(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv(DIR_ENV, str(tmp_path))
    reset_store()
    artifacts = StrategyArtifacts()
    with pytest.raises(RuntimeError):
        await artifacts.read("weights/model.pt")
    with pytest.raises(RuntimeError):
        await artifacts.write("sessions/None/weights/model.pt", b"x")


@pytest.mark.asyncio
async def test_a_bound_strategy_reads_what_it_wrote(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv(DIR_ENV, str(tmp_path))
    reset_store()
    strategy = Strategy()
    strategy.session = SimpleNamespace(session_id="abc123")  # type: ignore[assignment]
    strategy.artifacts.bind(strategy)
    key = f"sessions/{strategy.session.session_id}/weights/model.pt"
    await strategy.artifacts.write(key, b"session")
    opened = await strategy.artifacts.read(key)
    assert opened is not None
    assert opened.body == b"session"
    # The upload under the same relative tail is a different key.
    assert await strategy.artifacts.read("weights/model.pt") is None
    assert get_store().read(key) is not None


@pytest.mark.asyncio
async def test_writing_replaces_the_key_when_the_block_finishes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv(DIR_ENV, str(tmp_path))
    reset_store()
    strategy = Strategy()
    strategy.session = SimpleNamespace(session_id="abc123")  # type: ignore[assignment]
    strategy.artifacts.bind(strategy)
    key = f"sessions/{strategy.session.session_id}/weights/model.pt"
    await strategy.artifacts.write(key, b"first")

    async with strategy.artifacts.writing(key) as out:
        await asyncio.to_thread(out.write, b"second")

    async with strategy.artifacts.reading(key) as inp:
        body = await asyncio.to_thread(inp.read)
    assert body == b"second"
    assert list(tmp_path.rglob("*.part")) == []
    stated = await strategy.artifacts.stat(key)
    assert stated is not None
    assert stated.digest == "sha256:" + hashlib.sha256(b"second").hexdigest()


@pytest.mark.asyncio
async def test_a_failed_writing_keeps_the_previous_object(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv(DIR_ENV, str(tmp_path))
    reset_store()
    strategy = Strategy()
    strategy.session = SimpleNamespace(session_id="abc123")  # type: ignore[assignment]
    strategy.artifacts.bind(strategy)
    key = "weights/model.pt"
    await strategy.artifacts.write(key, b"first")

    with pytest.raises(RuntimeError, match="boom"):
        async with strategy.artifacts.writing(key) as out:
            await asyncio.to_thread(out.write, b"partial")
            raise RuntimeError("boom")

    opened = await strategy.artifacts.read(key)
    assert opened is not None
    assert opened.body == b"first"
    assert list(tmp_path.rglob("*.part")) == []


@pytest.mark.asyncio
async def test_reading_a_missing_key_raises(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv(DIR_ENV, str(tmp_path))
    reset_store()
    strategy = Strategy()
    strategy.session = SimpleNamespace(session_id="abc123")  # type: ignore[assignment]
    strategy.artifacts.bind(strategy)
    with pytest.raises(ArtifactNotFound):
        async with strategy.artifacts.reading("weights/model.pt"):
            pass


def test_a_swapped_part_file_is_not_committed(tmp_path: Path) -> None:
    store = _store(tmp_path)
    store.write("weights/model.pt", b"first")
    opened = store.open_write("weights/model.pt")
    opened.handle.write(b"second")
    opened.handle.flush()
    opened.handle.close()
    opened.path.unlink()
    opened.path.symlink_to("/etc/passwd")
    with pytest.raises(BadArtifactKey):
        store.commit_stream(opened)
    body = store.read("weights/model.pt")
    assert body is not None
    assert body.body == b"first"
    assert not opened.path.exists()
    assert not opened.path.is_symlink()
