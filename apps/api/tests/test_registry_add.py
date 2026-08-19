"""POST /registry/v1/add — validate, hash, copy. No other node is involved."""

from __future__ import annotations

from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any

import pytest
from db_harness import a_database, an_owner
from fastapi import HTTPException
from mftik.protocol import (
    STS_REGISTRY_RELOAD,
    StsRegistryReloadResult,
    StsRegistryReloadResultEnvelope,
)
from mftik.registry import RegistryStore, qualify
from mftik_api.routes import registry as registry_routes
from mftik_api.routes.registry import (
    add_strategy,
    disconnect_remote,
    get_published,
    get_remote,
    list_private,
    list_published,
    remote_diff,
)
from mftik_api.routes.sts import list_strategy_types, strategy_type_template
from mftik_api.schemas import RegistryAddBody
from mftik_db.repositories import StsSessionRepository

_TINY = """\
from mftik.strategy import Strategy

class Tiny(Strategy):
    name = "tiny"
"""

_YML = "td: []\nmd: []\nsts:\n  qty: 1\n"


class ReloadingBroker:
    """Stands in for the STS that answers ``sts.registry.reload``.

    It reports back whatever qualified keys it is told to, so a test can say
    "STS loaded this" or "STS did not" without a strategy runtime. ``None``
    means STS answers with every key the store holds, which is what a healthy
    one does.
    """

    def __init__(self, loaded: list[str] | None = None) -> None:
        self._loaded = loaded
        self.calls = 0

    def with_store(self, store: RegistryStore) -> ReloadingBroker:
        self._store = store
        return self

    async def request(self, subject, envelope, *, timeout=None):  # noqa: ANN001
        assert envelope.type == STS_REGISTRY_RELOAD
        self.calls += 1
        loaded = self._loaded
        if loaded is None:
            loaded = [
                qualify(rec.origin, rec.type) for rec in self._store.list_all()
            ]
        return StsRegistryReloadResultEnvelope.wrap(
            StsRegistryReloadResult(loaded=loaded),
            type=STS_REGISTRY_RELOAD,
            source="sts",
        )


def _broker(store: RegistryStore, loaded: list[str] | None = None):  # noqa: ANN202
    return ReloadingBroker(loaded).with_store(store)


async def test_add_returns_the_record_and_writes_files(tmp_path: Path) -> None:
    store = RegistryStore(tmp_path)
    body = RegistryAddBody(files={"strategy.py": _TINY})

    out = await add_strategy(body, store=store, broker=_broker(store))

    assert out.name == "tiny"
    assert out.type == "Tiny"
    assert out.digest.startswith("sha256:")
    assert out.origin == "private"
    assert out.files == ["strategy.py"]
    assert out.requires == []
    written = tmp_path / "registry" / "private" / "tiny" / "strategy.py"
    assert written.read_text() == _TINY


async def test_add_returns_declared_requires(tmp_path: Path) -> None:
    store = RegistryStore(tmp_path)
    body = RegistryAddBody(
        files={
            "strategy.py": (
                "from mftik.strategy import Strategy\n"
                "class Tiny(Strategy):\n"
                '    name = "tiny"\n'
                '    requires = ("numpy",)\n'
            )
        }
    )

    out = await add_strategy(body, store=store, broker=_broker(store))

    assert out.requires == ["numpy"]


async def test_disallowed_import_is_400(tmp_path: Path) -> None:
    store = RegistryStore(tmp_path)
    body = RegistryAddBody(
        files={
            "strategy.py": (
                "import requests\n"
                "from mftik_sts.strategy import Strategy\n"
                "class Tiny(Strategy):\n"
                '    name = "tiny"\n'
            )
        }
    )
    with pytest.raises(HTTPException) as caught:
        await add_strategy(body, store=store, broker=_broker(store))
    assert caught.value.status_code == 400
    assert "requests" in str(caught.value.detail)


async def test_duplicate_name_is_409(tmp_path: Path) -> None:
    store = RegistryStore(tmp_path)
    body = RegistryAddBody(files={"strategy.py": _TINY})
    await add_strategy(body, store=store, broker=_broker(store))
    with pytest.raises(HTTPException) as caught:
        await add_strategy(body, store=store, broker=_broker(store))
    assert caught.value.status_code == 409


async def test_types_include_private_and_public(tmp_path: Path) -> None:
    store = RegistryStore(tmp_path)
    await add_strategy(
        RegistryAddBody(files={"strategy.py": _TINY}),
        store=store,
        broker=_broker(store),
    )
    await add_strategy(
        RegistryAddBody(files={"strategy.py": _TINY}, origin="public"),
        store=store,
        broker=_broker(store),
    )

    listed = await list_strategy_types(store=store)

    assert "private::Tiny" in listed.types
    assert "public::Tiny" in listed.types
    assert "Tiny" not in listed.types
    assert "NoopStrategy" in listed.types
    tiny = next(t for t in listed.templates if t.type == "private::Tiny")
    assert tiny.label == "private::Tiny"
    assert tiny.source == "registry"
    noop = next(t for t in listed.templates if t.type == "NoopStrategy")
    assert noop.source == "bundled"

    template = await strategy_type_template("public::Tiny", store=store)
    assert template.type == "public::Tiny"
    assert template.yaml == "sts: {}\n"


async def test_add_keeps_strategy_yml_as_the_template(tmp_path: Path) -> None:
    store = RegistryStore(tmp_path)
    out = await add_strategy(
        RegistryAddBody(files={"strategy.py": _TINY, "strategy.yml": _YML}),
        store=store,
        broker=_broker(store),
    )
    assert "strategy.yml" in out.files
    written = tmp_path / "registry" / "private" / "tiny" / "strategy.yml"
    assert written.read_text() == _YML

    listed = await list_strategy_types(store=store)
    tiny = next(t for t in listed.templates if t.type == "private::Tiny")
    assert tiny.yaml == _YML
    template = await strategy_type_template("private::Tiny", store=store)
    assert template.yaml == _YML


async def test_bad_strategy_yml_is_400(tmp_path: Path) -> None:
    store = RegistryStore(tmp_path)
    with pytest.raises(HTTPException) as caught:
        await add_strategy(
            RegistryAddBody(files={"strategy.py": _TINY, "strategy.yml": "td: [\n"}),
            store=store,
            broker=_broker(store),
        )
    assert caught.value.status_code == 400
    assert "strategy.yml" in str(caught.value.detail)


async def test_published_list_is_public_only(tmp_path: Path) -> None:
    store = RegistryStore(tmp_path)
    await add_strategy(
        RegistryAddBody(files={"strategy.py": _TINY}),
        store=store,
        broker=_broker(store),
    )
    await add_strategy(
        RegistryAddBody(files={"strategy.py": _TINY}, origin="public"),
        store=store,
        broker=_broker(store),
    )
    store.add({"strategy.py": _TINY}, origin="node1")

    published = await list_published(store=store)
    assert [s.name for s in published.strategies] == ["tiny"]
    assert published.strategies[0].origin == "public"

    hidden = await list_private(store=store)
    assert [s.name for s in hidden.strategies] == ["tiny"]
    assert hidden.strategies[0].origin == "private"

    detail = await get_published("tiny", store=store)
    assert detail.contents == {"strategy.py": _TINY}
    assert detail.origin == "public"

    listed = await list_strategy_types(store=store)
    assert "private::Tiny" in listed.types
    assert "public::Tiny" in listed.types
    assert "node1::Tiny" in listed.types


async def test_get_remote_returns_pulled_strategies(tmp_path: Path) -> None:
    store = RegistryStore(tmp_path)
    store.add({"strategy.py": _TINY}, origin="node1")
    store.put_remote("node1", "http://example:8000")

    detail = await get_remote("node1", store=store)
    assert detail.name == "node1"
    assert detail.url == "http://example:8000"
    assert [s.name for s in detail.strategies] == ["tiny"]
    assert detail.strategies[0].origin == "node1"


def _no_live_sessions(monkeypatch: pytest.MonkeyPatch) -> None:
    class FakeRepo:
        def __init__(self, _db: object) -> None:
            pass

        async def list_live_for_origin(self, _origin: str) -> list:
            return []

    @asynccontextmanager
    async def scope() -> Any:
        yield object()

    monkeypatch.setattr(registry_routes, "session_scope", scope)
    monkeypatch.setattr(registry_routes, "StsSessionRepository", FakeRepo)


async def test_disconnect_drops_the_remote(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _no_live_sessions(monkeypatch)
    store = RegistryStore(tmp_path)
    store.add({"strategy.py": _TINY}, origin="node1")
    store.put_remote("node1", "http://example:8000")
    out = await disconnect_remote("node1", store=store, broker=_broker(store))
    assert out.name == "node1"
    assert store.get_remote("node1") is None
    assert store.list_pulled() == []
    with pytest.raises(HTTPException) as caught:
        await disconnect_remote("node1", store=store, broker=_broker(store))
    assert caught.value.status_code == 404


@pytest.fixture
async def registry_db(monkeypatch, database_url):
    async with a_database(database_url) as database:
        async with database.scope() as session:
            await an_owner(session)
        monkeypatch.setattr(registry_routes, "session_scope", database.scope)
        yield database.scope


async def _live_strategy(scope, *, session_id: str, type: str) -> None:
    async with scope() as db:
        await StsSessionRepository(db).create_live(
            session_id=session_id, created_by=1, strategy=type, type=type
        )


async def test_disconnect_refuses_while_a_session_is_live(
    tmp_path: Path, registry_db
) -> None:
    await _live_strategy(registry_db, session_id="s-tiny", type="node1::Tiny")
    store = RegistryStore(tmp_path)
    store.add({"strategy.py": _TINY}, origin="node1")
    store.put_remote("node1", "http://example:8000")

    with pytest.raises(HTTPException) as caught:
        await disconnect_remote("node1", store=store, broker=_broker(store))
    assert caught.value.status_code == 409
    assert "s-tiny" in str(caught.value.detail)
    assert "Tiny" in str(caught.value.detail)
    assert store.get_remote("node1") is not None


async def test_disconnect_does_not_treat_node10_as_node1(
    tmp_path: Path, registry_db
) -> None:
    await _live_strategy(registry_db, session_id="s-ten", type="node10::Tiny")
    store = RegistryStore(tmp_path)
    store.add({"strategy.py": _TINY}, origin="node1")
    store.put_remote("node1", "http://example:8000")

    out = await disconnect_remote("node1", store=store, broker=_broker(store))
    assert out.name == "node1"
    assert store.get_remote("node1") is None


async def test_unknown_remote_is_404(tmp_path: Path) -> None:
    store = RegistryStore(tmp_path)
    with pytest.raises(HTTPException) as caught:
        await get_remote("missing", store=store)
    assert caught.value.status_code == 404
    with pytest.raises(HTTPException) as caught:
        await remote_diff("missing", store=store)
    assert caught.value.status_code == 404
