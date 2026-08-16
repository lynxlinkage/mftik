"""POST /registry/v1/add — validate, hash, copy. No other node is involved."""

from __future__ import annotations

from pathlib import Path

import pytest
from fastapi import HTTPException
from mft.registry import RegistryStore
from mft_api.routes.registry import (
    add_strategy,
    get_published,
    get_remote,
    list_private,
    list_published,
    remote_diff,
)
from mft_api.routes.sts import list_strategy_types, strategy_type_template
from mft_api.schemas import RegistryAddBody

_TINY = """\
from mft_sts.strategy import Strategy

class Tiny(Strategy):
    name = "tiny"
"""


async def test_add_returns_the_record_and_writes_files(tmp_path: Path) -> None:
    store = RegistryStore(tmp_path)
    body = RegistryAddBody(files={"strategy.py": _TINY})

    out = await add_strategy(body, store=store)

    assert out.name == "tiny"
    assert out.type == "Tiny"
    assert out.digest.startswith("sha256:")
    assert out.origin == "private"
    assert out.files == ["strategy.py"]
    written = tmp_path / "registry" / "private" / "tiny" / "strategy.py"
    assert written.read_text() == _TINY


async def test_disallowed_import_is_400(tmp_path: Path) -> None:
    store = RegistryStore(tmp_path)
    body = RegistryAddBody(
        files={
            "strategy.py": (
                "import requests\n"
                "from mft_sts.strategy import Strategy\n"
                "class Tiny(Strategy):\n"
                '    name = "tiny"\n'
            )
        }
    )
    with pytest.raises(HTTPException) as caught:
        await add_strategy(body, store=store)
    assert caught.value.status_code == 400
    assert "requests" in str(caught.value.detail)


async def test_duplicate_name_is_409(tmp_path: Path) -> None:
    store = RegistryStore(tmp_path)
    body = RegistryAddBody(files={"strategy.py": _TINY})
    await add_strategy(body, store=store)
    with pytest.raises(HTTPException) as caught:
        await add_strategy(body, store=store)
    assert caught.value.status_code == 409


async def test_types_include_private_and_public(tmp_path: Path) -> None:
    store = RegistryStore(tmp_path)
    await add_strategy(RegistryAddBody(files={"strategy.py": _TINY}), store=store)
    await add_strategy(
        RegistryAddBody(files={"strategy.py": _TINY}, origin="public"), store=store
    )

    listed = await list_strategy_types(store=store)

    assert "private::Tiny" in listed.types
    assert "public::Tiny" in listed.types
    assert "Tiny" not in listed.types
    assert "NoopStrategy" in listed.types
    tiny = next(t for t in listed.templates if t.type == "private::Tiny")
    assert tiny.label == "private::Tiny"

    template = await strategy_type_template("public::Tiny", store=store)
    assert template.type == "public::Tiny"
    assert "sts:" in template.yaml


async def test_published_list_is_public_only(tmp_path: Path) -> None:
    store = RegistryStore(tmp_path)
    await add_strategy(RegistryAddBody(files={"strategy.py": _TINY}), store=store)
    await add_strategy(
        RegistryAddBody(files={"strategy.py": _TINY}, origin="public"), store=store
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


async def test_unknown_remote_is_404(tmp_path: Path) -> None:
    store = RegistryStore(tmp_path)
    with pytest.raises(HTTPException) as caught:
        await get_remote("missing", store=store)
    assert caught.value.status_code == 404
    with pytest.raises(HTTPException) as caught:
        await remote_diff("missing", store=store)
    assert caught.value.status_code == 404
