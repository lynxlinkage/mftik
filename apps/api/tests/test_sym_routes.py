"""/sym HTTP facade — request shape out, typed payload back, errors mapped.

The handlers are driven directly against a stub broker: there is no plane and
no Redis here, only the translation between HTTP and the sym RPC contract.
"""

from __future__ import annotations

from typing import Any

import pytest
from fastapi import HTTPException
from mftik.broker.errors import RequestTimeoutError
from mftik.protocol import (
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
from mftik_api.routes.sym import list_symbols, list_venues


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
        "universal_ticker": "Gate_Spot_BTCUSDT",
        "base": "BTC",
        "quote": "USDT",
        "exch_ticker": "BTC_USDT",
    }
    payload.update(overrides)
    return SymbolInfo.model_validate(payload)


def _list_reply(*symbols: SymbolInfo, total: int | None = None) -> Envelope[Any]:
    items = list(symbols)
    return SymListResultEnvelope.wrap(
        SymListResult(symbols=items, total=total if total is not None else len(items)),
        type=SYM_LIST,
        source="sym",
    )


async def test_venues_returns_plane_coverage() -> None:
    broker = StubBroker(
        _venues_reply(
            venues=["Gate", "Paper"], counts={"Gate": 2, "Paper": 1}
        )
    )

    result = await list_venues(broker)  # type: ignore[arg-type]

    assert broker.subject == Topics.SYM
    assert broker.sent is not None
    assert broker.sent.type == SYM_VENUES
    assert broker.sent.source == "api"
    assert result.venues == ["Gate", "Paper"]
    assert result.counts == {"Gate": 2, "Paper": 1}


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
    broker = StubBroker(_list_reply(_symbol()))

    result = await list_symbols(
        broker,  # type: ignore[arg-type]
        venue="gate",
        category="spot",
        symbol="btc/usdt",
        active_only=False,
    )

    assert broker.sent is not None
    assert broker.sent.type == SYM_LIST
    # Every part is normalized at the boundary, so what the plane matches on
    # is the spelling it stores — not whatever the query string carried.
    assert broker.sent.payload == {
        "universal_ticker": None,
        "venue": "Gate",
        "category": "Spot",
        "symbol": "BTCUSDT",
        "active_only": False,
        "q": None,
        "limit": None,
        "offset": 0,
        "slim": False,
    }
    assert [s.exch_ticker for s in result.symbols] == ["BTC_USDT"]
    assert result.total == 1


async def test_a_dated_symbol_filter_is_normalized_for_its_book() -> None:
    """``symbol`` is an exact suffix match, so it has to be the stored form.

    Both spellings a person might type land on the one the plane stores.
    Folding this as a pair — which is what ``canonical`` does — strips the
    hyphen and matches no dated row at all, in either spelling.
    """
    for typed in ("BTCUSDT250926", "btcusdt-250926", "btc/usdt-250926"):
        broker = StubBroker(_list_reply())
        await list_symbols(  # type: ignore[arg-type]
            broker,
            venue="binanceum",
            category="future",
            symbol=typed,
        )
        assert broker.sent is not None
        assert broker.sent.payload["symbol"] == "BTCUSDT-250926", typed


async def test_a_pair_symbol_filter_still_folds_its_punctuation() -> None:
    """The dated grammar is per-book: spot and perp keep the old folding."""
    broker = StubBroker(_list_reply())

    await list_symbols(  # type: ignore[arg-type]
        broker, venue="binanceum", category="perp", symbol="btc-usdt"
    )

    assert broker.sent is not None
    assert broker.sent.payload["symbol"] == "BTCUSDT"


async def test_symbols_rejects_a_filter_it_cannot_normalize() -> None:
    """A bad venue or category is a 400 here, not a 502 from the plane."""
    broker = StubBroker(_list_reply())

    with pytest.raises(HTTPException) as exc:
        await list_symbols(broker, category="futures")  # type: ignore[arg-type]

    assert exc.value.status_code == 400
    assert broker.sent is None


async def test_symbols_defaults_to_every_venue() -> None:
    broker = StubBroker(_list_reply())

    await list_symbols(broker)  # type: ignore[arg-type]

    assert broker.sent is not None
    # Omitted filters widen the result — sym must not receive a venue here.
    assert broker.sent.payload["venue"] is None
    assert broker.sent.payload["active_only"] is True


async def test_symbols_forwards_browse_knobs() -> None:
    broker = StubBroker(_list_reply(_symbol(), total=40))

    result = await list_symbols(
        broker,  # type: ignore[arg-type]
        venue="Gate",
        q="btc",
        limit=10,
        offset=20,
        slim=True,
    )

    assert broker.sent is not None
    assert broker.sent.payload["q"] == "btc"
    assert broker.sent.payload["limit"] == 10
    assert broker.sent.payload["offset"] == 20
    assert broker.sent.payload["slim"] is True
    assert result.total == 40
