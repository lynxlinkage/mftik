"""/sym HTTP facade — request shape out, typed payload back, errors mapped.

The handlers are driven directly against a stub broker: there is no plane and
no Redis here, only the translation between HTTP and the sym RPC contract.
"""

from __future__ import annotations

from typing import Any

import pytest
from fastapi import HTTPException
from mft.broker.errors import RequestTimeoutError
from mft.protocol import (
    SYM_ERROR,
    SYM_LIST,
    SYM_VENUES,
    Envelope,
    RpcError,
    RpcErrorEnvelope,
    SymbolInfo,
    SymListResult,
    SymListResultEnvelope,
    SymVenuesResult,
    SymVenuesResultEnvelope,
    Topics,
    UntypedEnvelope,
)
from mft_api.routes.sym import list_symbols, list_venues


class StubBroker:
    """Records the outbound request and replies with a canned envelope."""

    def __init__(self, reply: Envelope[Any] | Exception) -> None:
        self._reply = reply
        self.subject: str | None = None
        self.sent: UntypedEnvelope | None = None

    async def request(
        self, subject: str, envelope: Envelope[Any], *, timeout: float | None = None
    ) -> UntypedEnvelope:
        self.subject = subject
        # Round-trip through the wire form so the test sees what sym would.
        self.sent = UntypedEnvelope.from_json(envelope.to_json())
        if isinstance(self._reply, Exception):
            raise self._reply
        return UntypedEnvelope.from_json(self._reply.to_json())


def _venues_reply(**kwargs: Any) -> Envelope[Any]:
    return SymVenuesResultEnvelope.wrap(
        SymVenuesResult(**kwargs), type=SYM_VENUES, source="sym"
    )


def _symbol(**overrides: Any) -> SymbolInfo:
    payload: dict[str, Any] = {
        "venue": "gate_spot",
        "symbol": "BTCUSDT",
        "base": "BTC",
        "quote": "USDT",
        "exch_ticker": "BTC_USDT",
    }
    payload.update(overrides)
    return SymbolInfo.model_validate(payload)


async def test_venues_returns_plane_coverage() -> None:
    broker = StubBroker(
        _venues_reply(
            venues=["gate_spot", "paper"], counts={"gate_spot": 2, "paper": 1}
        )
    )

    result = await list_venues(broker)  # type: ignore[arg-type]

    assert broker.subject == Topics.SYM
    assert broker.sent is not None
    assert broker.sent.type == SYM_VENUES
    assert broker.sent.source == "api"
    assert result.venues == ["gate_spot", "paper"]
    assert result.counts == {"gate_spot": 2, "paper": 1}


async def test_venues_maps_plane_error_to_502() -> None:
    broker = StubBroker(
        RpcErrorEnvelope.wrap(
            RpcError(code="venues_failed", message="plane down"),
            type=SYM_ERROR,
            source="sym",
        )
    )

    with pytest.raises(HTTPException) as exc:
        await list_venues(broker)  # type: ignore[arg-type]

    assert exc.value.status_code == 502
    assert exc.value.detail == "plane down"


async def test_venues_maps_timeout_to_502() -> None:
    broker = StubBroker(RequestTimeoutError(Topics.SYM, "req-1", 5.0))

    with pytest.raises(HTTPException) as exc:
        await list_venues(broker)  # type: ignore[arg-type]

    assert exc.value.status_code == 502
    assert "timed out" in exc.value.detail


async def test_symbols_forwards_query_filters() -> None:
    broker = StubBroker(
        SymListResultEnvelope.wrap(
            SymListResult(symbols=[_symbol()]), type=SYM_LIST, source="sym"
        )
    )

    result = await list_symbols(
        broker,  # type: ignore[arg-type]
        venue="gate_spot",
        category="spot",
        symbol=None,
        active_only=False,
    )

    assert broker.sent is not None
    assert broker.sent.type == SYM_LIST
    assert broker.sent.payload == {
        "venue": "gate_spot",
        "category": "spot",
        "symbol": None,
        "active_only": False,
    }
    assert [s.exch_ticker for s in result.symbols] == ["BTC_USDT"]


async def test_symbols_defaults_to_every_venue() -> None:
    broker = StubBroker(
        SymListResultEnvelope.wrap(
            SymListResult(symbols=[]), type=SYM_LIST, source="sym"
        )
    )

    await list_symbols(broker)  # type: ignore[arg-type]

    assert broker.sent is not None
    # Omitted filters widen the result — sym must not receive a venue here.
    assert broker.sent.payload["venue"] is None
    assert broker.sent.payload["active_only"] is True
