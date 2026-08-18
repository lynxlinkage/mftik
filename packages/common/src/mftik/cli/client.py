"""Talking to a node's API, and turning what comes back into something readable.

The API is reachable at two shapes of URL and the difference is not the
client's to guess. A local stack publishes the app directly, so the routers
are at the root; a deployed one sits behind Traefik, which routes ``/api/*``
to the same app after stripping the prefix. :func:`probe` asks which it is,
once, at connect time, and the answer is what gets stored — every later
command reads it rather than trying both.

WebSockets are the exception to that prefix: ``/ws/*`` is routed without a
strip, so those paths are literal on the origin either way. Hence
:attr:`Node.ws_base` being derived from the origin rather than from the API
base beside it.
"""

from __future__ import annotations

import json
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any
from urllib.parse import urlparse, urlunparse

import httpx
from websockets.exceptions import InvalidStatus, WebSocketException
from websockets.sync.client import connect as ws_connect

from mftik.cli import config
from mftik.cli.config import Profile

#: How long a plain request may take. Short enough that an unreachable
#: host fails rather than hangs. ``run`` sizes its own timeout from the
#: document — a deploy's attach walk is longer than this.
DEFAULT_TIMEOUT_S = 30.0

#: Prefixes to try when connecting, in order. Empty first: a stack that
#: answers at the root is the local one, and a node deployed behind Traefik
#: does not answer ``/health`` at its root at all.
_PROBE_PREFIXES = ("", "/api")


class CliError(Exception):
    """Something the user can fix — a bad argument, a refusal, a 404."""


class NodeUnreachable(CliError):
    """The node did not answer. Distinguished so it can exit differently."""


@dataclass(frozen=True, slots=True)
class Node:
    """A resolved API base, and the WebSocket origin that goes with it."""

    api_base: str

    @property
    def ws_base(self) -> str:
        """``wss://host/ws`` — the origin of the API base, not the base.

        Traefik strips ``/api`` before the app sees a request and does not
        strip anything from ``/ws``, so a node whose API is at
        ``https://host/api`` serves its sockets at ``wss://host/ws``.
        """
        parts = urlparse(self.api_base)
        scheme = "wss" if parts.scheme == "https" else "ws"
        return urlunparse((scheme, parts.netloc, "/ws", "", "", "")).rstrip("/")

    def url(self, path: str) -> str:
        return f"{self.api_base}/{path.lstrip('/')}"


def normalize_url(raw: str) -> str:
    """A URL the user typed, as something httpx will accept.

    A bare host is assumed to be https. Not http: this carries a bearer
    token, and guessing the unencrypted scheme for a host that supports both
    would leak it. Someone running a local stack types the scheme.
    """
    url = raw.strip()
    if not url:
        raise CliError("give the node's URL, e.g. https://node.example.com")
    if "://" not in url:
        url = f"https://{url}"
    parts = urlparse(url)
    if parts.scheme not in {"http", "https"}:
        raise CliError(f"unsupported scheme in {raw!r}: {parts.scheme}")
    if not parts.netloc:
        raise CliError(f"no host in {raw!r}")
    # Trailing slashes come off the path, not off the whole string: stripping
    # them from ``https://`` would leave ``https:``, which then looks like a
    # host that needs a scheme prepending. Query and fragment go too — this is
    # a base to build paths on, not a request.
    return urlunparse((parts.scheme, parts.netloc, parts.path.rstrip("/"), "", "", ""))


class Client:
    """One node, one credential, for the life of a command."""

    def __init__(
        self,
        node: Node,
        token: str | None = None,
        *,
        timeout: float = DEFAULT_TIMEOUT_S,
        login_hint: bool = True,
    ) -> None:
        self.node = node
        self.token = token
        #: Whether a 401 should suggest running ``mftik connect``. It should,
        #: everywhere except inside ``connect`` itself — where a 401 means the
        #: password was wrong and being told to run the command you are
        #: running is not advice.
        self.login_hint = login_hint
        headers = {"Authorization": f"Bearer {token}"} if token else {}
        # Cookies persist across requests on this client, which is what lets
        # ``connect`` log in, mint a key with the session it was given, and
        # log out again.
        self._http = httpx.Client(
            timeout=timeout, headers=headers, follow_redirects=True
        )

    def __enter__(self) -> Client:
        return self

    def __exit__(self, *_: object) -> None:
        self.close()

    def close(self) -> None:
        self._http.close()

    def request(
        self,
        method: str,
        path: str,
        *,
        json_body: Any | None = None,
        params: dict[str, Any] | None = None,
    ) -> Any:
        """Call the API and return the decoded body, or raise something legible."""
        url = self.node.url(path)
        try:
            response = self._http.request(
                method, url, json=json_body, params=params
            )
        except httpx.HTTPError as exc:
            raise NodeUnreachable(f"cannot reach {url}: {exc}") from exc
        if response.status_code >= 400:
            raise _refusal(response, login_hint=self.login_hint)
        if not response.content:
            return None
        try:
            return response.json()
        except json.JSONDecodeError as exc:
            raise CliError(f"{url} did not answer with JSON: {exc}") from exc

    def get(self, path: str, **kwargs: Any) -> Any:
        return self.request("GET", path, **kwargs)

    def post(self, path: str, **kwargs: Any) -> Any:
        return self.request("POST", path, **kwargs)

    def delete(self, path: str, **kwargs: Any) -> Any:
        return self.request("DELETE", path, **kwargs)

    def follow_sts_logs(
        self,
        session_id: str,
        write: Callable[[str], None] = print,
    ) -> None:
        """Print live STS log lines until the socket closes or is interrupted.

        The URL is ``ws_base/sts/{id}``, not the API base: Traefik routes
        ``/ws/*`` without stripping, so a node whose API is at ``/api`` still
        serves sockets at ``/ws``. Ctrl-C is left to the caller — this does
        not stop the session.
        """
        url = f"{self.node.ws_base}/sts/{session_id}"
        headers = (
            [("Authorization", f"Bearer {self.token}")] if self.token else []
        )
        try:
            with ws_connect(url, additional_headers=headers, close_timeout=1) as ws:
                for raw in ws:
                    write(_format_log_frame(raw))
        except KeyboardInterrupt:
            raise
        except InvalidStatus as exc:
            raise CliError(f"{url} refused the socket ({exc})") from exc
        except (OSError, WebSocketException) as exc:
            raise NodeUnreachable(f"cannot reach {url}: {exc}") from exc


def _format_log_frame(raw: str) -> str:
    """``level  message`` from a log envelope, or the frame itself."""
    try:
        env = json.loads(raw)
    except json.JSONDecodeError:
        return raw
    payload = env.get("payload") if isinstance(env, dict) else None
    if not isinstance(payload, dict):
        return raw
    level = str(payload.get("level") or "info")
    message = str(payload.get("message") or "")
    return f"{level}  {message}"


def for_profile(profile: Profile, **kwargs: Any) -> Client:
    """A client aimed at a connected node, carrying its key."""
    return Client(Node(api_base=profile.url), profile.token, **kwargs)


def connected(name: str | None = None, **kwargs: Any) -> tuple[Profile, Client]:
    """The profile a command should act on, and a client for it.

    Every command that talks to a node starts here, so "which node" is
    resolved the same way once rather than at each call site.
    """
    profile = config.load().resolve(name)
    return profile, for_profile(profile, **kwargs)


def probe(url: str, *, timeout: float = 10.0) -> Node:
    """Find where the API answers under ``url``.

    Tries the root and then ``/api``. The health route is public on both, so
    this needs no credential — which is what makes it usable as the first
    thing ``connect`` does, before there is one.
    """
    errors: list[str] = []
    with httpx.Client(timeout=timeout, follow_redirects=True) as http:
        for prefix in _PROBE_PREFIXES:
            base = f"{url}{prefix}"
            try:
                response = http.get(f"{base}/health")
            except httpx.HTTPError as exc:
                errors.append(f"{base}/health: {exc}")
                continue
            if response.status_code == 200:
                return Node(api_base=base)
            errors.append(f"{base}/health: HTTP {response.status_code}")
    detail = "\n  ".join(errors)
    raise NodeUnreachable(
        f"no MFTIK API answered at {url}\n  {detail}\n"
        "Check the URL, and that the node is up."
    )


def _called(response: httpx.Response) -> str:
    """``POST /sts/deploy/…`` — what was asked, not the host it was asked of."""
    try:
        request = response.request
    except RuntimeError:
        return ""
    return f"{request.method} {request.url.path}"


def _refusal(response: httpx.Response, *, login_hint: bool = True) -> CliError:
    """The API's own explanation, when it gave one.

    FastAPI puts it in ``detail``; a proxy answering instead of the app will
    not, and for those the status line is all there is to report. The
    method and path go in too: ``run`` is two POSTs, and a bare
    ``HTTP 500`` does not say which one.
    """
    detail = ""
    try:
        body = response.json()
    except (json.JSONDecodeError, ValueError):
        body = None
    if isinstance(body, dict):
        raw = body.get("detail")
        detail = raw if isinstance(raw, str) else json.dumps(raw) if raw else ""
    if not detail:
        detail = response.text.strip()[:200] or response.reason_phrase

    status = response.status_code
    if status == 401:
        if not login_hint:
            return CliError(detail)
        return CliError(
            f"{detail}\nThis node wants a credential — run: mftik connect <url>"
        )
    if status == 403:
        return CliError(
            f"{detail}\nThe key is real but not allowed here. A registry key "
            "can only read published strategies; deploying needs an API key."
        )
    called = _called(response)
    prefix = f"{called} {status}" if called else f"HTTP {status}"
    return CliError(f"{prefix}: {detail}")
