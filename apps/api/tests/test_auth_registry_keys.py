"""A registry key reads what this node publishes, and reaches nothing else.

This is the credential that leaves the building — it is handed to somebody
else's node, run by somebody else. So the interesting assertions are all
negative: the places it must not reach. A key that could start a session or
mint another key would make peering a way in.
"""

from __future__ import annotations

from pathlib import Path

import pytest
from auth_harness import a_client, an_api, use_database
from db_harness import a_database
from mftik_api.auth import routes as auth_routes
from mftik_api.auth.middleware import required_scope
from mftik_api.auth.principal import SCOPE_API, SCOPE_REGISTRY_READ

GOOD = "correct-horse-battery"

_TINY = """\
from mftik.strategy import Strategy

class Tiny(Strategy):
    name = "tiny"
"""


@pytest.fixture(autouse=True)
def _no_throttle() -> None:
    auth_routes._failures.clear()


@pytest.fixture
async def db(monkeypatch, database_url, tmp_path: Path):
    async with a_database(database_url) as database:
        use_database(monkeypatch, database.scope)
        monkeypatch.setenv("MFTIK_AUTH_ENABLED", "1")
        monkeypatch.setenv("MFTIK_DATA", str(tmp_path))
        yield database.scope


async def a_published_strategy(client) -> None:
    added = await client.post(
        "/registry/v1/add", json={"files": {"strategy.py": _TINY}, "origin": "public"}
    )
    assert added.status_code == 200, added.text


async def an_owner_with_keys(client) -> tuple[str, str]:
    created = await client.post(
        "/auth/setup", json={"username": "yite", "password": GOOD}
    )
    assert created.status_code == 201
    await a_published_strategy(client)
    peer = await client.post("/auth/keys", json={"name": "node2", "kind": "registry"})
    script = await client.post("/auth/keys", json={"name": "ci", "kind": "api"})
    assert peer.status_code == 201 and script.status_code == 201
    return peer.json()["token"], script.json()["token"]


async def test_a_registry_key_reads_what_this_node_publishes(db) -> None:
    app = an_api(registry=True)
    async with a_client(app) as client:
        peer, _ = await an_owner_with_keys(client)

    auth = {"Authorization": f"Bearer {peer}"}
    async with a_client(app) as client:
        listed = await client.get("/registry/v1/strategies", headers=auth)
        detail = await client.get("/registry/v1/strategies/tiny", headers=auth)

    assert peer.startswith("mftik_rk_")
    assert [s["name"] for s in listed.json()["strategies"]] == ["tiny"]
    assert "strategy.py" in detail.json()["contents"]


async def test_a_registry_key_reads_handshake_extras_not_environment(
    db, tmp_path: Path
) -> None:
    from mftik.envapply import ApplySpec, apply_packages
    from mftik.environment import NodeEnv

    def plant(dest: Path, packages: dict) -> None:  # noqa: ANN001
        for name in packages:
            pkg = dest / name
            pkg.mkdir()
            (pkg / "__init__.py").write_text("ok\n")

    apply_packages(
        NodeEnv(tmp_path),
        {"numpy": ApplySpec(version="1.0", dist="numpy")},
        installer=plant,
    )
    app = an_api(registry=True)
    async with a_client(app) as client:
        peer, _ = await an_owner_with_keys(client)

    auth = {"Authorization": f"Bearer {peer}"}
    async with a_client(app) as client:
        info = await client.get("/registry/v1/info", headers=auth)
        anon = await client.get("/registry/v1/info")
        env = await client.get("/environment", headers=auth)
        posted = await client.post("/environment/packages", headers=auth, json={})

    assert info.status_code == 200
    extra = info.json()["extras"]["numpy"]
    assert extra["version"] == "1.0"
    assert extra["dist"] == "numpy"
    assert "source" not in extra
    assert anon.status_code == 200
    assert "numpy" in anon.json()["extras"]
    assert anon.json()["extras"]["numpy"] == {}
    assert env.status_code == 403
    assert posted.status_code == 403


@pytest.mark.parametrize(
    "method,path",
    [
        ("GET", "/sts/sessions"),
        ("GET", "/registry/v1/private"),
        ("POST", "/registry/v1/add"),
        ("GET", "/registry/v1/remotes"),
        ("POST", "/registry/v1/remotes"),
        ("GET", "/auth/keys"),
        ("POST", "/auth/keys"),
        ("GET", "/environment"),
        ("PUT", "/environment"),
        ("POST", "/environment/packages"),
        ("POST", "/environment/import"),
        ("GET", "/alerts"),
        ("POST", "/alerts"),
    ],
)
async def test_a_registry_key_reaches_nothing_else(db, method: str, path: str) -> None:
    app = an_api(registry=True)
    async with a_client(app) as client:
        peer, _ = await an_owner_with_keys(client)

    auth = {"Authorization": f"Bearer {peer}"}
    async with a_client(app) as client:
        answered = await client.request(method, path, headers=auth, json={})

    assert answered.status_code == 403, (
        f"{method} {path} answered {answered.status_code}"
    )
    # Not 401: this is a real credential in the wrong place, and the peer
    # presenting it has no login to be sent to.
    assert "x-mftik-auth" not in answered.headers


async def test_the_source_dump_is_no_longer_public(db) -> None:
    """The hole this key closes. Before it, anyone could read the source."""
    app = an_api(registry=True)
    async with a_client(app) as client:
        await an_owner_with_keys(client)

    async with a_client(app) as client:
        listed = await client.get("/registry/v1/strategies")
        detail = await client.get("/registry/v1/strategies/tiny")
        info = await client.get("/registry/v1/info")
        env = await client.get("/environment")

    assert listed.status_code == 401
    assert detail.status_code == 401
    assert env.status_code == 401
    # Still open, so a peer learns it speaks the wrong protocol before it
    # goes looking for a key it may not even need.
    assert info.status_code == 200


async def test_an_api_key_can_read_the_registry_too(db) -> None:
    """The Owner's own script is not a lesser citizen than a peer."""
    app = an_api(registry=True)
    async with a_client(app) as client:
        _, script = await an_owner_with_keys(client)

    async with a_client(app) as client:
        listed = await client.get(
            "/registry/v1/strategies", headers={"Authorization": f"Bearer {script}"}
        )
    assert listed.status_code == 200


async def test_a_session_still_administers_the_registry(db) -> None:
    app = an_api(registry=True)
    async with a_client(app) as client:
        await an_owner_with_keys(client)
        private = await client.get("/registry/v1/private")
        remotes = await client.get("/registry/v1/remotes")

    assert private.status_code == 200
    assert remotes.status_code == 200


def test_only_the_two_peer_reads_relax_the_default() -> None:
    """The policy names exceptions, so a route that forgets to ask is closed
    to peers rather than open to them."""
    assert required_scope("GET", "/registry/v1/strategies") == SCOPE_REGISTRY_READ
    assert (
        required_scope("GET", "/registry/v1/strategies/tiny") == SCOPE_REGISTRY_READ
    )
    for path in (
        "/registry/v1/strategiesX",
        "/registry/v1/private",
        "/registry/v1/remotes",
        "/registry/v1/add",
        "/sts/sessions",
        "/auth/keys",
        "/environment",
    ):
        assert required_scope("GET", path) == SCOPE_API, path
    assert required_scope("PUT", "/environment") == SCOPE_API
    assert required_scope("POST", "/environment/packages") == SCOPE_API


def test_a_registry_key_cannot_write_to_the_paths_it_can_read() -> None:
    """The scope is ``registry:read``, and the method is what makes it read.

    A path prefix says which resource a request is about and nothing about
    what it intends to do to it. Deciding on the prefix alone would hand
    every write ever added under ``/registry/v1/strategies`` to every peer
    this node has issued a key to.
    """
    for method in ("DELETE", "POST", "PUT", "PATCH"):
        assert (
            required_scope(method, "/registry/v1/strategies/tiny") == SCOPE_API
        ), method
