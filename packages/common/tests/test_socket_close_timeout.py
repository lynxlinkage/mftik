"""Closing a venue socket must not take ten seconds.

``websockets`` waits ``close_timeout`` — 10 seconds by default — for the peer
to answer a close frame, and venues routinely do not. Measured against Binance
futures: 6.2s on one endpoint and the full 10.0s on the other, paid serially by
whoever is tearing the venue down, on a control-plane loop that serves one
request at a time.

Nothing on these sockets is worth that. ``close`` cancels every pending request
before it reaches the handshake, and orders already at a venue do not depend on
how the connection ends.
"""

from __future__ import annotations

from typing import Any

import pytest
from mft.exchange.binance import socket as binance_socket
from mft.exchange.bybit import socket as bybit_socket
from mft.exchange.gate.spot import client as gate_client


class _Recorder:
    """Stands in for ``websockets.connect``, keeping what it was called with."""

    def __init__(self) -> None:
        self.kwargs: dict[str, Any] = {}

    async def __call__(self, url: str, **kwargs: Any) -> object:
        self.kwargs = kwargs
        return object()


@pytest.mark.parametrize(
    ("module", "build"),
    [
        (binance_socket, lambda: binance_socket.BinanceSocket("ws://x")),
        (bybit_socket, lambda: bybit_socket.BybitSocket("ws://x")),
        (gate_client, lambda: gate_client.GateSpotWebSocket(url="ws://x")),
    ],
    ids=["binance", "bybit", "gate"],
)
async def test_every_venue_socket_bounds_its_closing_handshake(
    module: Any, build: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    recorder = _Recorder()
    monkeypatch.setattr(module, "connect", recorder)

    socket = build()
    await socket._open()  # noqa: SLF001

    assert "close_timeout" in recorder.kwargs, (
        "socket dialled without a close_timeout — it will inherit the "
        "library's 10 seconds"
    )
    assert recorder.kwargs["close_timeout"] == 2.0


async def test_the_bound_is_configurable(monkeypatch: pytest.MonkeyPatch) -> None:
    """A stub server that answers promptly should not need the default."""
    recorder = _Recorder()
    monkeypatch.setattr(binance_socket, "connect", recorder)

    socket = binance_socket.BinanceSocket("ws://x", close_timeout=0.1)
    await socket._open()  # noqa: SLF001

    assert recorder.kwargs["close_timeout"] == 0.1
