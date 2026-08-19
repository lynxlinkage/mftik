"""The gate. Default deny, with a short list of routes that are not.

A pure ASGI middleware rather than ``BaseHTTPMiddleware`` because Starlette's
HTTP middleware never runs for WebSockets, and this API serves five of them.
Once the Traefik chain comes off, nothing else is authenticating those
handshakes — a gate that silently skipped them would be worse than none,
because it would look complete.

Deny is the default and the allowlist is explicit, so a route added later is
gated by having been added. That is the opposite of hanging a dependency on
each router, where the failure mode is forgetting one and never finding out.
"""

from __future__ import annotations

import json
import logging
import os
from http.cookies import SimpleCookie

from mftik_db.session import session_scope
from starlette.types import ASGIApp, Message, Receive, Scope, Send

from mftik_api.auth import keys, sessions
from mftik_api.auth.principal import (
    ANONYMOUS,
    SCOPE_API,
    SCOPE_REGISTRY_READ,
    Principal,
)
from mftik_api.deps import DEFAULT_USER_ID

logger = logging.getLogger(__name__)

#: Reachable with no credential at all.
#:
#: ``/health`` is here so compose health checks and CI can probe the API
#: without one — which is also why session keepalive must not use it; it
#: cannot report an expired session because it never looks.
PUBLIC_PATHS = frozenset(
    {
        "/health",
        "/auth/status",
        "/auth/setup",
        "/auth/login/password",
        # Versions only. A peer should learn it speaks the wrong protocol
        # before it goes looking for a key it may not need yet.
        "/registry/v1/info",
    }
)

#: Public by prefix: the OAuth entry points and returns added in later steps.
PUBLIC_PREFIXES = ("/auth/login/", "/auth/callback/")

#: What a gated route needs unless something says otherwise. Sessions imply
#: it and API keys carry it; a credential issued for one narrow job does not,
#: which is how a registry key stays out of everything but the registry.
DEFAULT_SCOPE = SCOPE_API

#: The peer-facing reads, and the only paths a registry key can reach. Listed
#: as an exception to the default rather than as a permission on the routes:
#: a route that forgets to ask is then closed to peers, not open to them.
#:
#: ``/registry/v1/info`` is not here because it is public — a peer has to be
#: able to find out it speaks the wrong protocol before it needs a key at all.
#: ``/private``, ``/add`` and ``/remotes`` are not here either, which is what
#: keeps this node's own registry administration away from other nodes.
REGISTRY_READ_PATHS = ("/registry/v1/strategies",)


#: Methods a registry key may use on the paths below. The name of the scope
#: is ``registry:read`` and this is what makes that true: a peer holding one
#: reads what this node publishes and does nothing else. Without the method in
#: the decision, adding any write under ``/registry/v1/strategies`` would hand
#: it to every peer that has ever been issued a key — a path prefix says which
#: resource a request is about, never what it intends to do to it.
_READ_METHODS = frozenset({"GET", "HEAD", "OPTIONS"})


def required_scope(method: str, path: str) -> str:
    if method.upper() in _READ_METHODS:
        for prefix in REGISTRY_READ_PATHS:
            if path == prefix or path.startswith(f"{prefix}/"):
                return SCOPE_REGISTRY_READ
    return DEFAULT_SCOPE

#: Identity is something this middleware decides. Anything arriving under
#: these names is a client trying to decide it instead. The Traefik chain
#: strips them today; the app has to keep doing it once that is gone.
_FORGEABLE = (b"x-auth-",)


def auth_enabled() -> bool:
    """Whether the gate is live.

    Off by default, and deliberately so. Merging to ``main`` deploys, and
    production still sits behind ``discord-auth-chain``; a live gate under
    that chain locks everyone out with no way back in, because the chain
    answers ``POST /auth/login/password`` with 401 before FastAPI sees it.
    This flag is what lets the module land as small PRs instead of a
    long-lived branch. See docs/Auth.md.
    """
    return os.getenv("MFTIK_AUTH_ENABLED", "0").strip().lower() in {"1", "true", "yes"}


def is_public(path: str) -> bool:
    return path in PUBLIC_PATHS or path.startswith(PUBLIC_PREFIXES)


class AuthMiddleware:
    def __init__(self, app: ASGIApp) -> None:
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] not in {"http", "websocket"}:
            await self.app(scope, receive, send)
            return

        scope = dict(scope)
        scope["headers"] = [
            (name, value)
            for name, value in scope["headers"]
            if not name.lower().startswith(_FORGEABLE)
        ]

        if not auth_enabled():
            # Exactly today's behaviour: every request is the Owner.
            principal = Principal.owner(DEFAULT_USER_ID, via="disabled")
        else:
            principal = await _resolve(scope)
            if not is_public(scope["path"]):
                if not principal.authenticated:
                    await _refuse(scope, receive, send)
                    return
                # A WebSocket handshake has no method; treat it as a read so
                # the lookup below is well-defined. It cannot reach the
                # registry paths either way — none of them are sockets.
                method = scope.get("method", "GET")
                if not principal.allows(required_scope(method, scope["path"])):
                    await _forbid(scope, receive, send)
                    return

        scope.setdefault("state", {})["principal"] = principal
        await self.app(scope, receive, send)


async def _resolve(scope: Scope) -> Principal:
    """The principal a request carries, or anonymous.

    A database that cannot answer produces an anonymous request, not a 500.
    The gate below still refuses it, so an outage costs logins rather than
    turning every route into a stack trace — and ``/health`` goes on
    answering, which is how anything finds out what broke.
    """
    bearer = _bearer(scope)
    if bearer is not None:
        try:
            async with session_scope() as db:
                key = await keys.resolve(db, bearer)
        except Exception:
            logger.exception("key lookup failed")
            return ANONYMOUS
        if key is None:
            return ANONYMOUS
        return Principal.machine(
            key.user_id,
            name=key.name,
            kind=key.kind,
            scopes=frozenset(key.scopes),
            key_id=key.id,
        )

    token = _cookie(scope, sessions.COOKIE_NAME)
    if token is None:
        return ANONYMOUS
    try:
        async with session_scope() as db:
            row = await sessions.resolve(db, token)
    except Exception:
        logger.exception("session lookup failed")
        return ANONYMOUS
    if row is None:
        return ANONYMOUS
    return Principal.owner(row.user_id, via=row.via, session_id=row.id)


def _bearer(scope: Scope) -> str | None:
    """The token in ``Authorization: Bearer``, if there is one.

    Checked before the cookie, not after: a browser sends its cookie with
    every request, so a script driven from one — or any client that keeps
    both — would otherwise never be able to act as its key.
    """
    for header, value in scope["headers"]:
        if header.lower() != b"authorization":
            continue
        raw = value.decode("latin-1").strip()
        scheme, _, token = raw.partition(" ")
        if scheme.lower() == "bearer" and token.strip():
            return token.strip()
    return None


def _cookie(scope: Scope, name: str) -> str | None:
    for header, value in scope["headers"]:
        if header.lower() != b"cookie":
            continue
        jar = SimpleCookie()
        jar.load(value.decode("latin-1"))
        morsel = jar.get(name)
        if morsel is not None:
            return morsel.value
    return None


async def _refuse(scope: Scope, receive: Receive, send: Send) -> None:
    """401, whatever asked.

    No 302 for document navigations. Nothing in this app redirects to log in:
    the frontend is served by its own container and is not gated, so a browser
    reaches the SPA either way and the SPA routes itself to /login when a
    request comes back 401. The redirect the old ``auth.ts`` reload dance
    existed to reach lived outside the app, and no longer exists.
    """
    if scope["type"] == "websocket":
        # Closing before accepting is how a handshake is refused; the server
        # turns it into an HTTP rejection the browser can see. A socket cannot
        # report a 401 once accepted, which is what made the old chain's
        # WebSocket failures indistinguishable from the API restarting.
        await receive()
        await send({"type": "websocket.close", "code": 1008})
        return

    await _json(send, 401, "authentication required", login=True)


async def _forbid(scope: Scope, receive: Receive, send: Send) -> None:
    """403: the credential is real, and not enough.

    Deliberately not a 401 and deliberately without the login header. Sending
    someone to /login here would be a lie — they are signed in, and signing in
    again with the same credential would fail the same way. This is a key
    being used somewhere its scopes do not reach.
    """
    if scope["type"] == "websocket":
        await receive()
        await send({"type": "websocket.close", "code": 1008})
        return
    await _json(send, 403, "this credential is not allowed here", login=False)


async def _json(send: Send, status: int, detail: str, *, login: bool) -> None:
    body = json.dumps({"detail": detail}).encode()
    headers = [
        (b"content-type", b"application/json"),
        (b"content-length", str(len(body)).encode()),
    ]
    if login:
        # Says this answer came from the app, not from the Traefik chain in
        # front of it. The two want opposite things from the browser: the
        # chain needs a document navigation to reach its redirect, while this
        # one wants the SPA to route itself to /login. Both gates exist at
        # once until the cutover, so the answer has to say which one it is
        # rather than the frontend guessing from a build flag.
        headers.append((b"x-mftik-auth", b"login-required"))
    message: Message = {
        "type": "http.response.start",
        "status": status,
        "headers": headers,
    }
    await send(message)
    await send({"type": "http.response.body", "body": body})
