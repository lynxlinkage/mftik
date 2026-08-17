"""Authenticating this machine against a node, once.

What gets stored is an API key, not a login. ``POST /auth/keys`` is gated on a
browser session rather than on a key — a key cannot mint another key, which is
the point of issuing scoped credentials at all — so this does what a browser
would: sign in with the password, mint a key with the session that grants,
keep the key, and drop the session. The password is never written down and
the cookie never leaves the process.

A node can also be running with its gate off (``MFTIK_AUTH_ENABLED=0``), which
is a normal way to run a local stack. Then there is no key to mint and the
profile is just a URL.
"""

from __future__ import annotations

import argparse
import getpass
import socket
import sys

from mftik.cli import config
from mftik.cli.client import Client, CliError, connected, normalize_url, probe
from mftik.cli.config import Profile
from mftik.cli.output import table


def _key_name() -> str:
    """What the minted key is called on the node.

    Names the machine, so ``mftik key ls`` and the audit trail can tell one
    workstation's key from another's — and so revoking the laptop that was
    lost does not mean revoking all of them.
    """
    try:
        host = socket.gethostname().strip() or "unknown"
    except OSError:
        host = "unknown"
    return f"mftik-cli@{host}"[:64]


def connect(args: argparse.Namespace) -> int:
    url = normalize_url(args.url)
    node = probe(url)
    name = args.name or config.default_name(url)

    with Client(node, login_hint=False) as client:
        status = client.get("/auth/status")
        token = _credential(client, args, status)

    saved = config.put(
        Profile(name=name, url=node.api_base, token=token),
        make_default=not args.keep_default,
    )
    print(f"connected {name} -> {node.api_base}")
    if token is None:
        print(
            "This node has its gate off, so no key was issued and every "
            "request is the Owner."
        )
    print(f"profile saved to {saved}")
    return 0


def _credential(
    client: Client, args: argparse.Namespace, status: dict
) -> str | None:
    """The token to store for this node, or None if it wants none."""
    if args.token:
        # Given a key, check it rather than trusting it. Storing one that does
        # not work turns every later command into a 401 whose cause is three
        # commands back.
        checked = Client(client.node, args.token, login_hint=False)
        try:
            with checked:
                me = checked.get("/auth/me")
        except CliError as exc:
            raise CliError(
                f"this node did not accept that key ({exc}).\n"
                "Keys are per node — one minted somewhere else will not work "
                "here, and a revoked one will not work anywhere."
            ) from exc
        print(f"key accepted as {me.get('username') or me['user_id']}")
        return args.token

    if not status.get("enabled", False):
        return None

    if status.get("setup_required", False):
        if not args.setup:
            raise CliError(
                "this node has not been claimed yet — nobody owns it and it "
                "has no password.\nClaim it with: mftik connect <url> --setup\n"
                "(or open the node's web UI, which offers the same thing)"
            )
        return _claim(client, status)

    return _login_and_mint(client, status)


def _claim(client: Client, status: dict) -> str:
    """Take ownership of a node nobody has claimed, then mint a key.

    Deliberately not silent about what it is: this is the one call that
    decides who owns the instance, and running it against the wrong URL is
    not recoverable from this side.
    """
    minimum = int(status.get("min_password_length", 8))
    print("Claiming this node. Whoever does this becomes its Owner.")
    username = _ask("username: ")
    password = _ask_secret(f"password ({minimum}+ characters): ")
    if len(password) < minimum:
        raise CliError(f"password must be at least {minimum} characters")
    if password != _ask_secret("confirm password: "):
        raise CliError("the two passwords do not match")

    client.post("/auth/setup", json_body={"username": username, "password": password})
    return _mint(client)


def _login_and_mint(client: Client, status: dict) -> str:
    default_user = status.get("username") or ""
    prompt = f"username [{default_user}]: " if default_user else "username: "
    username = _ask(prompt) or default_user
    if not username:
        raise CliError("a username is required")
    password = _ask_secret("password: ")

    client.post(
        "/auth/login/password", json_body={"username": username, "password": password}
    )
    return _mint(client)


def _mint(client: Client) -> str:
    """Trade the session for a key, then give the session back.

    The logout is not tidiness. A session left open is a second live
    credential for this node that nothing on this machine is holding and
    nothing will ever revoke, because nothing here knows it exists.
    """
    try:
        created = client.post(
            "/auth/keys", json_body={"name": _key_name(), "kind": "api"}
        )
    finally:
        try:
            client.post("/auth/logout")
        except CliError:
            # The key, if one was minted, matters more than the tidy exit.
            pass
    return created["token"]


def _ask(prompt: str) -> str:
    if not sys.stdin.isatty():
        raise CliError(
            "nothing to read a username from — pass an existing key instead: "
            "mftik connect <url> --token mftik_ak_..."
        )
    return input(prompt).strip()


def _ask_secret(prompt: str) -> str:
    if not sys.stdin.isatty():
        raise CliError(
            "nothing to read a password from — pass an existing key instead: "
            "mftik connect <url> --token mftik_ak_..."
        )
    return getpass.getpass(prompt)


def whoami(args: argparse.Namespace) -> int:
    """Who this machine is, to the node it is pointed at."""
    profile, client = connected(args.profile)
    with client:
        me = client.get("/auth/me")
        status = client.get("/auth/status")

    rows = [
        ("profile", profile.name),
        ("node", profile.url),
        ("user", me.get("username") or "(unnamed)"),
        ("user_id", str(me["user_id"])),
        # How this request proved itself. ``disabled`` is the node saying it
        # never asked, which is worth showing next to a profile that holds a
        # key — the key is not what got you in.
        ("via", me.get("via", "?")),
        ("gate", "on" if status.get("enabled") else "off"),
    ]
    print(table(("", ""), rows))
    return 0
