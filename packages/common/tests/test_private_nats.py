"""The private-NATS guard fails a unit or component test that opens a socket.

The failure is the assertion. A red test left in the suite would be the
guard firing on a test that still borrows a connection, not a proof that
the guard can fire. integration and e2e are exempt; the shared worker
client is not a private socket.
"""

from __future__ import annotations

import inspect

import nats
import pytest
from broker_harness import shared_client_name
from nats_guard import PrivateNatsForbidden


async def _open_private() -> None:
    # A broken guard must not sit in reconnect. The working guard raises
    # before this dial.
    await nats.connect(
        "nats://127.0.0.1:1",
        allow_reconnect=False,
        max_reconnect_attempts=0,
    )


async def test_a_unit_test_that_opens_a_private_nats_connection_fails() -> None:
    """The failure names the caller, not the guard."""
    with pytest.raises(PrivateNatsForbidden) as caught:
        await _open_private()
    err = caught.value
    assert err.func == "_open_private"
    assert err.filename.endswith("test_private_nats.py")
    source_lines, start = inspect.getsourcelines(_open_private)
    await_at = next(
        i for i, line in enumerate(source_lines) if "await nats.connect" in line
    )
    assert err.lineno == start + await_at
    assert "test_private_nats.py" in str(err)
    assert "_open_private" in str(err)
    assert err.nodeid.endswith(
        "test_a_unit_test_that_opens_a_private_nats_connection_fails"
    )
    assert "shared broker fixture" in str(err)


@pytest.mark.component
async def test_component_tier_still_forbids_a_private_nats_connection() -> None:
    with pytest.raises(PrivateNatsForbidden) as caught:
        await _open_private()
    assert caught.value.func == "_open_private"


async def test_the_shared_worker_connection_is_not_a_private_socket(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The session fixture's client name is the worker's one socket (B2-03)."""
    seen: dict[str, object] = {}

    async def fake(*args: object, **kwargs: object) -> object:
        seen["name"] = kwargs.get("name")
        return object()

    monkeypatch.setattr("nats_guard._ORIGINAL", fake)
    await nats.connect("nats://127.0.0.1:1", name=shared_client_name())
    assert seen["name"] == shared_client_name()
    assert str(seen["name"]).startswith("mftik-pytest-")


@pytest.mark.integration
async def test_integration_tier_may_open_a_private_nats_connection(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    called: list[str] = []

    async def fake(servers: object = "", **kwargs: object) -> object:
        called.append(str(servers))
        return object()

    monkeypatch.setattr("nats_guard._ORIGINAL", fake)
    await nats.connect("nats://127.0.0.1:1")
    assert called == ["nats://127.0.0.1:1"]


@pytest.mark.e2e
async def test_e2e_tier_may_open_a_private_nats_connection(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    called: list[str] = []

    async def fake(servers: object = "", **kwargs: object) -> object:
        called.append(str(servers))
        return object()

    monkeypatch.setattr("nats_guard._ORIGINAL", fake)
    await nats.connect("nats://127.0.0.1:1")
    assert called == ["nats://127.0.0.1:1"]
