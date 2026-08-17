"""Finding the API, and reporting what it said.

The two URL shapes are the substance here. A local stack publishes the app
directly and answers at the root; a deployed one sits behind Traefik, which
routes ``/api/*`` to the same app *after stripping the prefix* — and routes
``/ws/*`` to it without stripping anything. A client that assumed either
shape would work against exactly one kind of node.
"""

from __future__ import annotations

import httpx
import pytest
from mftik.cli.client import (
    Client,
    CliError,
    Node,
    NodeUnreachable,
    normalize_url,
    probe,
)


def _transport(handler) -> httpx.MockTransport:  # noqa: ANN001
    return httpx.MockTransport(handler)


def _client(node: Node, handler, token: str | None = None) -> Client:  # noqa: ANN001
    client = Client(node, token)
    client._http = httpx.Client(
        transport=_transport(handler),
        headers={"Authorization": f"Bearer {token}"} if token else {},
    )
    return client


# --- normalising what the user typed ---------------------------------------


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("https://node.example.com", "https://node.example.com"),
        ("https://node.example.com/", "https://node.example.com"),
        ("  http://localhost:8000  ", "http://localhost:8000"),
        ("node.example.com", "https://node.example.com"),
    ],
)
def test_normalize_url(raw: str, expected: str) -> None:
    assert normalize_url(raw) == expected


def test_a_bare_host_is_assumed_https_not_http() -> None:
    """This URL carries a bearer token; guessing the cleartext scheme leaks it."""
    assert normalize_url("node.example.com").startswith("https://")


@pytest.mark.parametrize("raw", ["", "   ", "ftp://node.example.com", "https://"])
def test_unusable_urls_are_refused(raw: str) -> None:
    with pytest.raises(CliError):
        normalize_url(raw)


# --- finding the API -------------------------------------------------------


def test_probe_finds_a_local_stack_at_the_root(monkeypatch) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        assert str(request.url) == "http://localhost:8000/health"
        return httpx.Response(200, json={"status": "ok"})

    monkeypatch.setattr(httpx, "Client", _patched(handler))
    node = probe("http://localhost:8000")

    assert node.api_base == "http://localhost:8000"


def test_probe_falls_through_to_the_api_prefix(monkeypatch) -> None:
    """Traefik gives the root to the frontend, so /health there is not the API."""
    seen: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request.url.path)
        if request.url.path == "/api/health":
            return httpx.Response(200, json={"status": "ok"})
        # What the SPA's index.html looks like to a health probe.
        return httpx.Response(404, text="not found")

    monkeypatch.setattr(httpx, "Client", _patched(handler))
    node = probe("https://node.example.com")

    assert node.api_base == "https://node.example.com/api"
    assert seen == ["/health", "/api/health"]


def test_probe_reports_both_attempts_when_neither_answers(monkeypatch) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(502, text="bad gateway")

    monkeypatch.setattr(httpx, "Client", _patched(handler))
    with pytest.raises(NodeUnreachable) as exc:
        probe("https://node.example.com")

    message = str(exc.value)
    assert "/health: HTTP 502" in message
    assert "/api/health: HTTP 502" in message


def test_probe_survives_a_connection_error_on_the_first_shape(
    monkeypatch,
) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/health":
            raise httpx.ConnectError("refused", request=request)
        return httpx.Response(200, json={"status": "ok"})

    monkeypatch.setattr(httpx, "Client", _patched(handler))
    assert probe("https://n.example.com").api_base == "https://n.example.com/api"


#: Captured before any test patches the name, so the stand-in below builds a
#: real client instead of calling itself.
_REAL_HTTPX_CLIENT = httpx.Client


def _patched(handler):  # noqa: ANN001, ANN202
    """A stand-in for ``httpx.Client`` that serves ``handler``."""

    def build(*args, **kwargs):  # noqa: ANN002, ANN003, ANN202
        kwargs.pop("transport", None)
        return _REAL_HTTPX_CLIENT(*args, transport=_transport(handler), **kwargs)

    return build


# --- the WebSocket origin --------------------------------------------------


@pytest.mark.parametrize(
    ("api_base", "expected"),
    [
        # Deployed: /api is stripped before the app, /ws is not.
        ("https://node.example.com/api", "wss://node.example.com/ws"),
        # Local: the app is published directly, so both are on the same base.
        ("http://localhost:8000", "ws://localhost:8000/ws"),
    ],
)
def test_ws_base_comes_from_the_origin_not_the_api_base(
    api_base: str, expected: str
) -> None:
    assert Node(api_base=api_base).ws_base == expected


def test_url_joins_without_doubling_the_slash() -> None:
    node = Node(api_base="https://node.example.com/api")
    assert node.url("/sts/sessions") == "https://node.example.com/api/sts/sessions"
    assert node.url("sts/sessions") == "https://node.example.com/api/sts/sessions"


# --- what comes back -------------------------------------------------------


def test_the_token_rides_as_a_bearer_header() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.headers["authorization"] == "Bearer mftik_ak_abc"
        return httpx.Response(200, json={"ok": True})

    with _client(Node("http://n"), handler, token="mftik_ak_abc") as client:
        assert client.get("/health") == {"ok": True}


def test_an_empty_body_is_none_rather_than_a_decode_error() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(204)

    with _client(Node("http://n"), handler) as client:
        assert client.delete("/registry/v1/remotes/x") is None


def test_a_refusal_carries_the_api_s_own_detail() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(404, json={"detail": "unknown strategy type: nope"})

    with _client(Node("http://n"), handler) as client:
        with pytest.raises(CliError, match="unknown strategy type: nope"):
            client.get("/sts/types/nope/template")


def test_a_401_says_how_to_authenticate() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(401, json={"detail": "authentication required"})

    with _client(Node("http://n"), handler) as client:
        with pytest.raises(CliError, match="mftik connect"):
            client.get("/sts/sessions")


def test_a_403_does_not_send_you_back_to_connect() -> None:
    """The credential is real. Connecting again would mint the same refusal."""

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(403, json={"detail": "not allowed here"})

    with _client(Node("http://n"), handler) as client:
        with pytest.raises(CliError) as exc:
            client.post("/sts/deploy/NoopStrategy")

    assert "mftik connect" not in str(exc.value)
    assert "registry key" in str(exc.value)


def test_a_proxy_error_without_json_still_reports_something() -> None:
    """A 502 from Traefik is HTML, and the status is all there is to say."""

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(502, text="<html>Bad Gateway</html>")

    with _client(Node("http://n"), handler) as client:
        with pytest.raises(CliError, match="HTTP 502"):
            client.get("/health")


def test_an_unreachable_node_is_its_own_error() -> None:
    """So the caller can exit 2 for 'try again' and 1 for 'fix your input'."""

    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("refused", request=request)

    with _client(Node("http://n"), handler) as client:
        with pytest.raises(NodeUnreachable):
            client.get("/health")
