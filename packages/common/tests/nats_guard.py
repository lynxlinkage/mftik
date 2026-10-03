"""Fail unit and component tests that open a private NATS connection.

§9.2 rule 2: connection tests and behaviour tests are separate, and there
is no broker fake. A unit or component test may use the one shared client
this xdist worker already holds (:func:`broker_harness.nats_connection`,
named ``mftik-pytest-<worker>``). Opening another socket — :func:`a_broker`,
or ``Broker().connect()`` — is a private connection. Behaviour that today
goes through that socket is integration until it calls the handler
directly (F31). Broker semantics stay on the shared client.

The root ``conftest.py`` installs :func:`install` for the whole session and
arms :func:`arm` around each test, the same way the sleep guard does.

Exempt, because those tiers may open their own socket (§9.1):

* ``@pytest.mark.integration``
* ``@pytest.mark.e2e``

Outside a test (session startup) nothing is armed, so a fixture that
connects before the first test is not this guard's problem. The shared
client is allowed in every tier: it is the worker's one socket, not a
private one.
"""

from __future__ import annotations

import traceback
from collections.abc import Callable
from contextvars import ContextVar, Token
from typing import Any

import nats
import pytest
from broker_harness import SHARED_CLIENT_PREFIX

_ORIGINAL: Callable[..., Any] | None = None

# None means "not inside a test". A test sets this, and a task the test
# creates copies the value with the context.
_POLICY: ContextVar[_Policy | None] = ContextVar("mftik_nats_policy", default=None)


class PrivateNatsForbidden(RuntimeError):
    """A unit or component test opened a NATS connection of its own.

    ``filename`` and ``lineno`` are the caller of ``nats.connect``, not
    this guard.
    """

    def __init__(self, nodeid: str, filename: str, lineno: int, func: str) -> None:
        self.nodeid = nodeid
        self.filename = filename
        self.lineno = lineno
        self.func = func
        super().__init__(
            f"{nodeid}: a private NATS connection is forbidden in a unit or "
            f"component test; nats.connect called from {filename}:{lineno} "
            f"in {func}. Connection tests use the shared broker fixture. "
            f"Behaviour is a direct handler call (F31). integration and e2e "
            f"may open their own socket."
        )


class _Policy:
    __slots__ = ("allow_private", "nodeid")

    def __init__(self, *, allow_private: bool, nodeid: str) -> None:
        self.allow_private = allow_private
        self.nodeid = nodeid


def install() -> None:
    """Replace ``nats.connect`` with the guard. Idempotent."""
    global _ORIGINAL
    if _ORIGINAL is not None:
        return
    _ORIGINAL = nats.connect
    nats.connect = _guarded_connect  # type: ignore[assignment]


def arm(item: pytest.Item) -> Token[_Policy | None]:
    """Forbid a private socket for this test, unless it is exempt."""
    return _POLICY.set(
        _Policy(allow_private=allows_private(item), nodeid=item.nodeid)
    )


def disarm(token: Token[_Policy | None]) -> None:
    _POLICY.reset(token)


def allows_private(item: pytest.Item) -> bool:
    """Whether this test may open a NATS connection that is not the shared one."""
    if item.get_closest_marker("integration") is not None:
        return True
    if item.get_closest_marker("e2e") is not None:
        return True
    return False


def is_shared_client_name(name: object) -> bool:
    """True for the per-worker client :func:`broker_harness.shared_client_name`."""
    return isinstance(name, str) and name.startswith(SHARED_CLIENT_PREFIX)


def _caller() -> tuple[str, int, str]:
    """The direct caller of ``nats.connect``."""
    for frame in reversed(traceback.extract_stack()):
        if frame.filename == __file__:
            continue
        return frame.filename, frame.lineno, frame.name
    return "<unknown>", 0, "<unknown>"


async def _guarded_connect(
    servers: str | list[str] = "nats://localhost:4222",
    **options: Any,
) -> Any:
    policy = _POLICY.get()
    if (
        policy is not None
        and not policy.allow_private
        and not is_shared_client_name(options.get("name"))
    ):
        filename, lineno, func = _caller()
        raise PrivateNatsForbidden(policy.nodeid, filename, lineno, func)
    assert _ORIGINAL is not None
    return await _ORIGINAL(servers, **options)
