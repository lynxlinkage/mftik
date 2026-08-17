"""Connect copies another node's published trees into pulled/{name}/."""

from __future__ import annotations

from urllib.parse import urlparse

import httpx
import pytest
from mftik.registry.errors import RegistryError
from mftik.registry.protocol import handshake_info
from mftik.registry.store import RegistryStore
from mftik.registry.sync import connect_remote, diff_remote

_TINY = """\
from mftik_sts.strategy import Strategy

class Tiny(Strategy):
    name = "tiny"
"""


def _peer(store: RegistryStore) -> httpx.MockTransport:
    def handler(request: httpx.Request) -> httpx.Response:
        path = urlparse(str(request.url)).path.rstrip("/") or "/"
        if path == "/registry/v1/info":
            return httpx.Response(200, json=handshake_info())
        if path == "/registry/v1/strategies":
            return httpx.Response(
                200,
                json={
                    "strategies": [
                        {
                            "name": rec.name,
                            "type": rec.type,
                            "digest": rec.digest,
                            "requires_mft": rec.requires_mft,
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
                    "requires_mft": rec.requires_mft,
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


@pytest.mark.asyncio
async def test_connect_refuses_a_bad_handshake(tmp_path) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={
                "protocol": "mft.registry",
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
