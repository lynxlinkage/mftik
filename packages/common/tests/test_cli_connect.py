"""``mftik connect`` — what it stores, and what it refuses to store.

The interesting assertions are about the credential. A key is minted through a
session and the session is given back; the password is never written anywhere;
and a node that has not been claimed is not claimed by accident.
"""

from __future__ import annotations

import argparse
from pathlib import Path

import httpx
import pytest
from mftik.cli import client as client_module
from mftik.cli import config, connect
from mftik.cli.client import CliError, Node, NodeUnreachable
from mftik.cli.config import Profile

MINTED = "mftik_ak_thisistheminted"

_REAL_HTTPX_CLIENT = httpx.Client


@pytest.fixture(autouse=True)
def config_file(tmp_path: Path, monkeypatch) -> Path:
    path = tmp_path / "config.toml"
    monkeypatch.setenv(config.CONFIG_ENV, str(path))
    monkeypatch.delenv(config.PROFILE_ENV, raising=False)
    return path


class Node_:
    """Records what a fake node was asked, and answers like the real one."""

    def __init__(
        self,
        *,
        enabled: bool = True,
        setup_required: bool = False,
        username: str | None = "yite",
        bad_password: bool = False,
    ) -> None:
        self.enabled = enabled
        self.setup_required = setup_required
        self.username = username
        self.bad_password = bad_password
        self.paths: list[str] = []
        self.bodies: dict[str, object] = {}
        self.bearer: str | None = None

    def __call__(self, request: httpx.Request) -> httpx.Response:
        path = request.url.path
        self.paths.append(path)
        auth = request.headers.get("authorization")
        if auth:
            self.bearer = auth.removeprefix("Bearer ")
        if request.content:
            import json

            self.bodies[path] = json.loads(request.content)

        if path.endswith("/health"):
            return httpx.Response(200, json={"status": "ok"})
        if path.endswith("/auth/status"):
            return httpx.Response(
                200,
                json={
                    "enabled": self.enabled,
                    "setup_required": self.setup_required,
                    "providers": ["password"],
                    "authenticated": False,
                    "username": self.username,
                    "min_password_length": 8,
                },
            )
        if path.endswith("/auth/login/password"):
            if self.bad_password:
                return httpx.Response(
                    401, json={"detail": "invalid username or password"}
                )
            return httpx.Response(200, json=self._me("password"))
        if path.endswith("/auth/setup"):
            self.setup_required = False
            return httpx.Response(201, json=self._me("password"))
        if path.endswith("/auth/keys"):
            return httpx.Response(
                201,
                json={
                    "id": 1,
                    "name": "mftik-cli@host",
                    "kind": "api",
                    "prefix": "thisisth",
                    "scopes": ["api", "registry:read"],
                    "created_at": 0.0,
                    "token": MINTED,
                },
            )
        if path.endswith("/auth/logout"):
            return httpx.Response(200, json={"status": "ok"})
        if path.endswith("/auth/me"):
            if self.enabled and self.bearer != MINTED and self.bearer is not None:
                return httpx.Response(401, json={"detail": "authentication required"})
            # The real API answers ``key:{name}``, not ``key`` — see
            # apps/api/tests/test_auth_cli_flow.py, which holds it to that.
            via = "key:mftik-cli@host" if self.bearer else "password"
            return httpx.Response(200, json=self._me(via))
        raise AssertionError(f"unexpected path: {path}")

    def _me(self, via: str) -> dict:
        return {
            "user_id": 1,
            "username": self.username,
            "display_name": "Owner",
            "email": None,
            "via": via,
        }


@pytest.fixture
def a_node(monkeypatch):
    def install(node: Node_) -> Node_:
        def build(*args, **kwargs):  # noqa: ANN002, ANN003, ANN202
            kwargs.pop("transport", None)
            return _REAL_HTTPX_CLIENT(
                *args, transport=httpx.MockTransport(node), **kwargs
            )

        monkeypatch.setattr(client_module.httpx, "Client", build)
        return node

    return install


def _args(url: str = "http://localhost:8000", **kwargs) -> argparse.Namespace:
    return argparse.Namespace(
        url=url,
        name=kwargs.pop("name", None),
        token=kwargs.pop("token", None),
        setup=kwargs.pop("setup", False),
        keep_default=kwargs.pop("keep_default", False),
        profile=kwargs.pop("profile", None),
        **kwargs,
    )


def _answers(monkeypatch, *, user: str = "yite", secrets: list[str]) -> None:
    monkeypatch.setattr("sys.stdin.isatty", lambda: True)
    monkeypatch.setattr("builtins.input", lambda _prompt="": user)
    queue = list(secrets)
    monkeypatch.setattr(
        connect.getpass, "getpass", lambda _prompt="": queue.pop(0)
    )


# --- the ordinary path -----------------------------------------------------


def test_signing_in_stores_a_key_and_not_the_password(
    a_node, monkeypatch, capsys
) -> None:
    node = a_node(Node_())
    _answers(monkeypatch, secrets=["correct-horse-battery"])

    assert connect.connect(_args()) == 0

    stored = config.load().profiles["default"]
    assert stored.token == MINTED
    assert stored.url == "http://localhost:8000"
    # The password reached the node and nothing else.
    assert node.bodies["/auth/login/password"]["password"] == "correct-horse-battery"
    raw = Path(config.config_path()).read_text()
    assert "correct-horse-battery" not in raw


def test_the_session_is_given_back_after_minting(a_node, monkeypatch) -> None:
    """A session left open is a live credential nothing here will ever revoke."""
    node = a_node(Node_())
    _answers(monkeypatch, secrets=["correct-horse-battery"])

    connect.connect(_args())

    assert node.paths.index("/auth/logout") > node.paths.index("/auth/keys")


def test_the_key_is_named_after_this_machine(a_node, monkeypatch) -> None:
    """So revoking the laptop that was lost is not revoking all of them."""
    node = a_node(Node_())
    _answers(monkeypatch, secrets=["correct-horse-battery"])

    connect.connect(_args())

    assert node.bodies["/auth/keys"]["name"].startswith("mftik-cli@")
    assert node.bodies["/auth/keys"]["kind"] == "api"


def test_a_wrong_password_is_not_told_to_run_connect(a_node, monkeypatch) -> None:
    """It is running connect. The node's own words are the useful part."""
    a_node(Node_(bad_password=True))
    _answers(monkeypatch, secrets=["wrong-horse-battery"])

    with pytest.raises(CliError) as caught:
        connect.connect(_args())

    assert "invalid username or password" in str(caught.value)
    assert "mftik connect" not in str(caught.value)
    assert config.load().profiles == {}


# --- a node with its gate off ----------------------------------------------


def test_a_node_with_no_gate_stores_no_key(a_node, capsys) -> None:
    a_node(Node_(enabled=False))

    assert connect.connect(_args()) == 0

    stored = config.load().profiles["default"]
    assert stored.token is None
    assert "gate off" in capsys.readouterr().out


# --- an unclaimed node -----------------------------------------------------


def test_an_unclaimed_node_is_not_claimed_by_accident(a_node) -> None:
    """Claiming decides who owns the instance. It takes saying so."""
    a_node(Node_(setup_required=True, username=None))

    with pytest.raises(CliError) as caught:
        connect.connect(_args())

    assert "--setup" in str(caught.value)
    assert config.load().profiles == {}


def test_setup_claims_the_node_then_mints(a_node, monkeypatch) -> None:
    node = a_node(Node_(setup_required=True, username=None))
    _answers(monkeypatch, secrets=["correct-horse-battery"] * 2)

    assert connect.connect(_args(setup=True)) == 0

    assert node.bodies["/auth/setup"]["username"] == "yite"
    assert config.load().profiles["default"].token == MINTED


def test_setup_refuses_a_password_that_does_not_confirm(
    a_node, monkeypatch
) -> None:
    a_node(Node_(setup_required=True, username=None))
    _answers(monkeypatch, secrets=["correct-horse-battery", "typo-horse-battery"])

    with pytest.raises(CliError, match="do not match"):
        connect.connect(_args(setup=True))

    assert config.load().profiles == {}


def test_setup_refuses_a_password_the_node_would_refuse(
    a_node, monkeypatch
) -> None:
    """Checked here as well as there, so the second prompt is not wasted."""
    a_node(Node_(setup_required=True, username=None))
    _answers(monkeypatch, secrets=["short"])

    with pytest.raises(CliError, match="at least 8"):
        connect.connect(_args(setup=True))


# --- bringing your own key -------------------------------------------------


def test_a_supplied_key_is_checked_before_it_is_stored(a_node) -> None:
    """Storing one that does not work makes every later command a mystery 401."""
    a_node(Node_())

    assert connect.connect(_args(token=MINTED)) == 0

    assert config.load().profiles["default"].token == MINTED


def test_a_supplied_key_that_does_not_work_is_not_stored(a_node) -> None:
    a_node(Node_())

    with pytest.raises(CliError) as caught:
        connect.connect(_args(token="mftik_ak_nonsense"))

    # "authentication required" alone does not explain a key you just pasted.
    assert "per node" in str(caught.value)
    assert config.load().profiles == {}


def test_a_supplied_key_needs_no_terminal(a_node, monkeypatch) -> None:
    """The CI path: no prompt is reached, so a pipe is fine."""
    a_node(Node_())
    monkeypatch.setattr("sys.stdin.isatty", lambda: False)

    assert connect.connect(_args(token=MINTED)) == 0


def test_without_a_key_and_without_a_terminal_it_says_what_to_do(
    a_node, monkeypatch
) -> None:
    a_node(Node_())
    monkeypatch.setattr("sys.stdin.isatty", lambda: False)

    with pytest.raises(CliError, match="--token"):
        connect.connect(_args())


# --- naming and defaults ---------------------------------------------------


def test_the_profile_can_be_named(a_node) -> None:
    a_node(Node_(enabled=False))

    connect.connect(_args(url="https://node.example.com", name="prod"))

    assert set(config.load().profiles) == {"prod"}


def test_connecting_takes_the_default_unless_told_not_to(a_node) -> None:
    a_node(Node_(enabled=False))
    config.put(Profile(name="first", url="http://first"))

    connect.connect(_args(name="second"))
    assert config.load().default == "second"

    connect.connect(_args(name="third", keep_default=True))
    assert config.load().default == "second"


def test_the_deployed_url_shape_is_what_gets_stored(a_node) -> None:
    """probe resolves /api once, here, so nothing later has to try both."""

    class UnderApi(Node_):
        def __call__(self, request: httpx.Request) -> httpx.Response:
            if request.url.path == "/health":
                return httpx.Response(404, text="not found")
            return super().__call__(request)

    a_node(UnderApi(enabled=False))

    connect.connect(_args(url="https://node.example.com", name="prod"))

    stored = config.load().profiles["prod"]
    assert stored.url == "https://node.example.com/api"
    assert Node(api_base=stored.url).ws_base == "wss://node.example.com/ws"


def test_a_node_that_does_not_answer_is_unreachable(a_node) -> None:
    class Dead(Node_):
        def __call__(self, request: httpx.Request) -> httpx.Response:
            raise httpx.ConnectError("refused", request=request)

    a_node(Dead())

    with pytest.raises(NodeUnreachable):
        connect.connect(_args())

    assert config.load().profiles == {}


# --- whoami ----------------------------------------------------------------


def test_whoami_reports_the_profile_and_the_proof(a_node, capsys) -> None:
    a_node(Node_())
    config.put(Profile(name="prod", url="http://localhost:8000", token=MINTED))

    assert connect.whoami(argparse.Namespace(profile=None)) == 0

    out = capsys.readouterr().out
    assert "prod" in out
    assert "yite" in out
    assert "gate" in out
    # Never the secret itself.
    assert MINTED not in out


def test_whoami_with_nothing_connected_says_what_to_run() -> None:
    from mftik.cli.config import ConfigError

    with pytest.raises(ConfigError, match="mftik connect"):
        connect.whoami(argparse.Namespace(profile=None))
