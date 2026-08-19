"""Connect copies another node's published trees into pulled/{name}/."""

from __future__ import annotations

from urllib.parse import urlparse

import httpx
import pytest
from mftik.registry.digest import digest_files
from mftik.registry.errors import RegistryError
from mftik.registry.files import TEMPLATE_NAME
from mftik.registry.protocol import (
    PROTOCOL,
    PROTOCOL_MIN,
    PROTOCOL_VERSION,
    handshake_info,
)
from mftik.registry.store import RegistryStore
from mftik.registry.sync import connect_remote, diff_remote

_TINY = """\
from mftik.strategy import Strategy

class Tiny(Strategy):
    name = "tiny"
"""

_YML = "sts: {}\n"


def _peer(
    store: RegistryStore, extras: object | None = None
) -> httpx.MockTransport:
    def handler(request: httpx.Request) -> httpx.Response:
        path = urlparse(str(request.url)).path.rstrip("/") or "/"
        if path == "/registry/v1/info":
            info = handshake_info(data_dir=store.data_dir)
            if extras is not None:
                info = {**info, "extras": extras}
            return httpx.Response(200, json=info)
        if path == "/registry/v1/strategies":
            return httpx.Response(
                200,
                json={
                    "strategies": [
                        {
                            "name": rec.name,
                            "type": rec.type,
                            "digest": rec.digest,
                            "requires_mftik": rec.requires_mftik,
                            "requires": list(rec.requires),
                            "origin": rec.origin,
                            "files": list(rec.files),
                        }
                        for rec in store.list_public()
                    ]
                },
            )
        prefix = "/registry/v1/strategies/"
        if path.startswith(prefix):
            name = path[len(prefix) :]
            rec = store.get_public(name)
            if rec is None:
                return httpx.Response(404, json={"detail": name})
            return httpx.Response(
                200,
                json={
                    "name": rec.name,
                    "type": rec.type,
                    "digest": rec.digest,
                    "requires_mftik": rec.requires_mftik,
                    "requires": list(rec.requires),
                    "origin": rec.origin,
                    "files": list(rec.files),
                    "contents": store.read_contents(rec),
                },
            )
        return httpx.Response(404)

    return httpx.MockTransport(handler)


@pytest.mark.asyncio
async def test_connect_pulls_into_named_origin(tmp_path) -> None:
    peer = RegistryStore(tmp_path / "peer")
    peer.add({"strategy.py": _TINY}, origin="public")
    local = RegistryStore(tmp_path / "local")
    async with httpx.AsyncClient(
        transport=_peer(peer), base_url="http://node1"
    ) as client:
        result = await connect_remote(
            local, name="node1", url="http://node1", client=client
        )
    assert result.name == "node1"
    assert len(result.pulled) == 1
    assert result.pulled[0].origin == "node1"
    dest = tmp_path / "local" / "registry" / "pulled" / "node1" / "tiny"
    assert (dest / "strategy.py").read_text() == _TINY
    assert local.list_public() == []
    assert local.list_private() == []
    assert local.list_remotes()[0].name == "node1"


@pytest.mark.asyncio
async def test_connect_pulls_strategy_yml_without_changing_digest(tmp_path) -> None:
    peer = RegistryStore(tmp_path / "peer")
    added = peer.add(
        {"strategy.py": _TINY, TEMPLATE_NAME: _YML},
        origin="public",
    )
    assert added.digest == digest_files({"strategy.py": _TINY.encode()})
    local = RegistryStore(tmp_path / "local")
    async with httpx.AsyncClient(
        transport=_peer(peer), base_url="http://node1"
    ) as client:
        result = await connect_remote(
            local, name="node1", url="http://node1", client=client
        )
    dest = tmp_path / "local" / "registry" / "pulled" / "node1" / "tiny"
    assert (dest / TEMPLATE_NAME).read_text() == _YML
    assert result.pulled[0].digest == added.digest
    assert TEMPLATE_NAME in result.pulled[0].files


@pytest.mark.asyncio
async def test_connect_does_not_pull_private(tmp_path) -> None:
    peer = RegistryStore(tmp_path / "peer")
    peer.add({"strategy.py": _TINY})
    peer.add(
        {
            "strategy.py": (
                "from mftik_sts.strategy import Strategy\n"
                "class Extra(Strategy):\n"
                '    name = "extra"\n'
            )
        },
        origin="public",
    )
    local = RegistryStore(tmp_path / "local")
    async with httpx.AsyncClient(
        transport=_peer(peer), base_url="http://node1"
    ) as client:
        result = await connect_remote(
            local, name="node1", url="http://node1", client=client
        )
    assert [rec.name for rec in result.pulled] == ["extra"]


@pytest.mark.asyncio
async def test_diff_marks_synced_diverged_and_remote_only(tmp_path) -> None:
    peer = RegistryStore(tmp_path / "peer")
    peer.add({"strategy.py": _TINY}, origin="public")
    local = RegistryStore(tmp_path / "local")
    transport = _peer(peer)
    async with httpx.AsyncClient(
        transport=transport, base_url="http://node1"
    ) as client:
        await connect_remote(local, name="node1", url="http://node1", client=client)
        peer.add(
            {"strategy.py": _TINY + "# changed\n"},
            replace=True,
            origin="public",
        )
        peer.add(
            {
                "strategy.py": (
                    "from mftik_sts.strategy import Strategy\n"
                    "class Extra(Strategy):\n"
                    '    name = "extra"\n'
                )
            },
            origin="public",
        )
        result = await diff_remote(local, name="node1", client=client)
    by_name = {row.name: row for row in result.rows}
    assert result.reachable is True
    assert by_name["tiny"].status == "diverged"
    assert by_name["extra"].status == "remote_only"
    assert by_name["extra"].local_digest is None


@pytest.mark.asyncio
async def test_diff_unreachable_peer_keeps_the_local_copy(tmp_path) -> None:
    local = RegistryStore(tmp_path)
    local.add({"strategy.py": _TINY}, origin="node1")
    local.put_remote("node1", "http://node1")

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(503, text="down")

    async with httpx.AsyncClient(
        transport=httpx.MockTransport(handler), base_url="http://node1"
    ) as client:
        result = await diff_remote(local, name="node1", client=client)
    assert result.reachable is False
    assert result.rows[0].status == "unknown"
    assert result.rows[0].local_digest is not None


def _info_with_extras(extras: object) -> dict:
    return {
        "protocol": PROTOCOL,
        "protocol_version": PROTOCOL_VERSION,
        "protocol_min": PROTOCOL_MIN,
        "mftik_version": "0.1.0",
        "extras": extras,
        "env_generation": 0,
    }


def _plant_numpy(data_dir, version: str = "2.2.2") -> None:
    from mftik.envapply import ApplySpec, apply_packages
    from mftik.environment import NodeEnv

    def plant(dest, packages):  # noqa: ANN001
        for name in packages:
            pkg = dest / name
            pkg.mkdir()
            (pkg / "__init__.py").write_text("ok\n")

    apply_packages(
        NodeEnv(data_dir),
        {"numpy": ApplySpec(version=version, dist="numpy")},
        installer=plant,
    )


@pytest.mark.asyncio
async def test_connect_refuses_missing_extra_names(tmp_path) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json=_info_with_extras(
                {"numpy": {"version": "2.2.1", "dist": "numpy"}}
            ),
        )

    local = RegistryStore(tmp_path / "local")
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(handler), base_url="http://peer"
    ) as client:
        with pytest.raises(RegistryError, match="numpy"):
            await connect_remote(
                local, name="peer", url="http://peer", client=client
            )
    assert local.list_remotes() == []
    assert not (tmp_path / "local" / "registry" / "remotes.toml").exists()
    assert not (tmp_path / "local" / "registry" / "pulled").exists()


@pytest.mark.asyncio
async def test_connect_refuses_legacy_flat_extras_the_same_way(tmp_path) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json=_info_with_extras({"numpy": "2.2.1"}))

    local = RegistryStore(tmp_path / "local")
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(handler), base_url="http://peer"
    ) as client:
        with pytest.raises(RegistryError, match="numpy"):
            await connect_remote(
                local, name="peer", url="http://peer", client=client
            )
    assert local.list_remotes() == []


@pytest.mark.asyncio
async def test_connect_ignores_pin_differences(tmp_path) -> None:
    _plant_numpy(tmp_path / "local", "2.2.2")
    peer = RegistryStore(tmp_path / "peer")
    peer.add({"strategy.py": _TINY}, origin="public")
    local = RegistryStore(tmp_path / "local")
    extras = {"numpy": {"version": "2.2.1", "dist": "numpy"}}
    async with httpx.AsyncClient(
        transport=_peer(peer, extras=extras), base_url="http://node1"
    ) as client:
        result = await connect_remote(
            local, name="node1", url="http://node1", client=client
        )
        diff = await diff_remote(local, name="node1", client=client)
    assert [rec.name for rec in result.pulled] == ["tiny"]
    assert local.list_remotes()[0].name == "node1"
    assert any("2.2.1" in row and "2.2.2" in row for row in diff.extras_warnings)


@pytest.mark.asyncio
@pytest.mark.asyncio
async def test_reconnect_accepts_extras_the_first_connect_would_refuse(
    tmp_path,
) -> None:
    _plant_numpy(tmp_path / "local", "1.0")
    peer = RegistryStore(tmp_path / "peer")
    peer.add({"strategy.py": _TINY}, origin="public")
    local = RegistryStore(tmp_path / "local")
    async with httpx.AsyncClient(
        transport=_peer(peer, extras={"numpy": {"version": "1.0", "dist": "numpy"}}),
        base_url="http://node1",
    ) as client:
        await connect_remote(local, name="node1", url="http://node1", client=client)

    peer.add(
        {
            "strategy.py": (
                "from mftik.strategy import Strategy\n\n"
                "class UsesTorch(Strategy):\n"
                '    name = "uses_torch"\n'
                '    requires = ("torch",)\n'
            )
        },
        origin="public",
    )
    heavier = {
        "numpy": {"version": "1.0", "dist": "numpy"},
        "torch": {"version": "2.0", "dist": "torch"},
    }
    async with httpx.AsyncClient(
        transport=_peer(peer, extras=heavier), base_url="http://node1"
    ) as client:
        result = await connect_remote(
            local, name="node1", url="http://node1", client=client
        )
    names = {rec.name for rec in result.pulled}
    assert "tiny" in names
    assert "uses_torch" in names
    assert local.get_remote("node1") is not None


@pytest.mark.asyncio
async def test_connect_refuses_a_bad_handshake(tmp_path) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={
                "protocol": "mftik.registry",
                "protocol_version": 99,
                "protocol_min": 99,
            },
        )

    local = RegistryStore(tmp_path)
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(handler), base_url="http://old"
    ) as client:
        with pytest.raises(RegistryError, match="incompatible"):
            await connect_remote(
                local, name="old", url="http://old", client=client
            )
