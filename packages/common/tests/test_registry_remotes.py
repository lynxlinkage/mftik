"""What we keep about a peer, and what we send it.

A remote used to be a URL. It is now a URL and, when the peer asks for one,
the registry key that peer issued us — which makes ``remotes.toml`` a file
holding somebody else's credentials, and changes both how it is written and
what an older one on disk has to keep meaning.
"""

from __future__ import annotations

import stat
from pathlib import Path
from urllib.parse import urlparse

import httpx
import pytest
from mftik.registry.store import RegistryStore
from mftik.registry.sync import connect_remote, diff_remote

_TINY = """\
from mftik_sts.strategy import Strategy

class Tiny(Strategy):
    name = "tiny"
"""


def a_store(tmp_path: Path) -> RegistryStore:
    return RegistryStore(tmp_path)


def test_a_remote_round_trips_with_its_token(tmp_path: Path) -> None:
    store = a_store(tmp_path)
    store.put_remote("node1", "https://one.example", "mftik_rk_secret")
    store.put_remote("node2", "https://two.example")

    one = store.get_remote("node1")
    two = store.get_remote("node2")

    assert one is not None and one.token == "mftik_rk_secret"
    assert two is not None and two.token is None, "a peer may publish openly"
    assert [r.name for r in store.list_remotes()] == ["node1", "node2"]


def test_the_file_that_holds_a_token_is_not_world_readable(
    tmp_path: Path,
) -> None:
    store = a_store(tmp_path)
    store.put_remote("node1", "https://one.example", "mftik_rk_secret")

    mode = stat.S_IMODE(store.remotes_path.stat().st_mode)

    assert mode == 0o600, f"remotes.toml is {oct(mode)}"


def test_a_second_write_does_not_widen_it_again(tmp_path: Path) -> None:
    """O_CREAT leaves an existing file's mode alone, so the chmod is not
    belt-and-braces — without it the first write's mode is all you ever get,
    and a file created before tokens existed stays as it was."""
    store = a_store(tmp_path)
    store.remotes_path.parent.mkdir(parents=True, exist_ok=True)
    store.remotes_path.write_text('[remotes]\nnode1 = "https://one.example"\n')
    store.remotes_path.chmod(0o644)

    store.put_remote("node1", "https://one.example", "mftik_rk_secret")

    assert stat.S_IMODE(store.remotes_path.stat().st_mode) == 0o600


def test_the_old_flat_file_still_loads(tmp_path: Path) -> None:
    """Nodes running since before registry keys have this on disk. Rewriting
    it on read would be a migration nobody asked for; it converts on write."""
    store = a_store(tmp_path)
    store.registry_dir.mkdir(parents=True, exist_ok=True)
    store.remotes_path.write_text(
        '[remotes]\nnode1 = "https://one.example"\nnode2 = "https://two.example"\n',
        encoding="utf-8",
    )

    remotes = {r.name: r for r in store.list_remotes()}

    assert remotes["node1"].url == "https://one.example"
    assert remotes["node1"].token is None
    assert remotes["node2"].url == "https://two.example"


def test_writing_converts_the_old_file_and_keeps_the_others(
    tmp_path: Path,
) -> None:
    store = a_store(tmp_path)
    store.registry_dir.mkdir(parents=True, exist_ok=True)
    store.remotes_path.write_text(
        '[remotes]\nnode1 = "https://one.example"\n', encoding="utf-8"
    )

    store.put_remote("node2", "https://two.example", "mftik_rk_secret")

    body = store.remotes_path.read_text()
    assert "[remotes.node1]" in body and "[remotes.node2]" in body
    assert store.get_remote("node1") is not None, "the peer we did not touch"
    assert store.get_remote("node2").token == "mftik_rk_secret"


def test_dropping_a_remote_takes_its_token_with_it(tmp_path: Path) -> None:
    store = a_store(tmp_path)
    store.put_remote("node1", "https://one.example", "mftik_rk_secret")
    store.put_remote("node2", "https://two.example", "mftik_rk_other")

    dropped = store.drop_remote("node1")

    assert dropped.token == "mftik_rk_secret"
    assert "mftik_rk_secret" not in store.remotes_path.read_text()
    assert store.get_remote("node2").token == "mftik_rk_other"


# --------------------------------------------------------------- the wire ---


def a_locked_peer(store: RegistryStore, token: str) -> tuple[httpx.MockTransport, list]:
    """A peer that publishes only to whoever presents ``token``.

    ``/info`` stays open, the way a real node's does: a peer has to be able to
    find out it is talking to the wrong protocol version before the question
    of a key comes up at all.
    """
    seen: list[tuple[str, str | None]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        path = urlparse(str(request.url)).path.rstrip("/") or "/"
        auth = request.headers.get("authorization")
        seen.append((path, auth))
        if path == "/registry/v1/info":
            from mftik.registry.protocol import handshake_info

            return httpx.Response(200, json=handshake_info())
        if auth != f"Bearer {token}":
            return httpx.Response(401, json={"detail": "authentication required"})
        if path == "/registry/v1/strategies":
            return httpx.Response(
                200,
                json={
                    "strategies": [
                        {"name": rec.name, "type": rec.type, "digest": rec.digest}
                        for rec in store.list_public()
                    ]
                },
            )
        prefix = "/registry/v1/strategies/"
        if path.startswith(prefix):
            rec = store.get_public(path[len(prefix) :])
            if rec is None:
                return httpx.Response(404, json={"detail": "no"})
            return httpx.Response(
                200,
                json={
                    "name": rec.name,
                    "type": rec.type,
                    "digest": rec.digest,
                    "contents": store.read_contents(rec),
                },
            )
        return httpx.Response(404, json={"detail": path})

    return httpx.MockTransport(handler), seen


async def test_connect_presents_the_key_and_pulls(tmp_path: Path) -> None:
    theirs = RegistryStore(tmp_path / "peer")
    theirs.add({"strategy.py": _TINY}, origin="public")
    mine = RegistryStore(tmp_path / "mine")
    transport, seen = a_locked_peer(theirs, "mftik_rk_secret")

    async with httpx.AsyncClient(transport=transport) as client:
        result = await connect_remote(
            mine,
            name="node1",
            url="https://one.example",
            token="mftik_rk_secret",
            client=client,
        )

    assert [rec.name for rec in result.pulled] == ["tiny"]
    assert mine.get_remote("node1").token == "mftik_rk_secret"
    # /info without, everything else with.
    assert ("/registry/v1/info", None) in seen
    assert all(
        auth == "Bearer mftik_rk_secret"
        for path, auth in seen
        if path != "/registry/v1/info"
    )


async def test_connecting_to_a_locked_peer_without_the_key_fails(
    tmp_path: Path,
) -> None:
    theirs = RegistryStore(tmp_path / "peer")
    theirs.add({"strategy.py": _TINY}, origin="public")
    mine = RegistryStore(tmp_path / "mine")
    transport, _ = a_locked_peer(theirs, "mftik_rk_secret")

    with pytest.raises(Exception) as caught:
        async with httpx.AsyncClient(transport=transport) as client:
            await connect_remote(
                mine, name="node1", url="https://one.example", client=client
            )

    assert "401" in str(caught.value)


async def test_a_later_diff_presents_the_stored_key(tmp_path: Path) -> None:
    """The key has to outlive connect — a pull is not a one-time act."""
    theirs = RegistryStore(tmp_path / "peer")
    theirs.add({"strategy.py": _TINY}, origin="public")
    mine = RegistryStore(tmp_path / "mine")
    transport, seen = a_locked_peer(theirs, "mftik_rk_secret")

    async with httpx.AsyncClient(transport=transport) as client:
        await connect_remote(
            mine,
            name="node1",
            url="https://one.example",
            token="mftik_rk_secret",
            client=client,
        )
        seen.clear()
        result = await diff_remote(mine, name="node1", client=client)

    assert result.reachable is True
    assert [row.status for row in result.rows] == ["synced"]
    assert seen and all(auth == "Bearer mftik_rk_secret" for _, auth in seen)
