"""POST /apis venue validation.

Every check here runs before ``session_scope()``, so the handler can be driven
directly with no database.
"""

from __future__ import annotations

import pytest
from fastapi import HTTPException
from mftik.exchange import venues
from mftik_api.routes.apis import create_api, list_venues
from mftik_api.schemas import ApiCreateBody


def _body(**overrides: object) -> ApiCreateBody:
    payload: dict[str, object] = {
        "name": "gate spot",
        "venue": "Gate",
        "api_key": "key-1",
        "api_secret": "secret-1",
        "type": "HMAC",
    }
    payload.update(overrides)
    return ApiCreateBody.model_validate(payload)


async def test_list_venues_exposes_every_registered_venue() -> None:
    result = await list_venues()
    by_name = {v.name: v for v in result.venues}

    assert set(by_name) == {
        "Binance",
        "BinanceFuture",
        "Bybit",
        "Gate",
        "Paper",
    }
    gate = by_name["Gate"]
    assert gate.label == "Gate Spot"
    assert gate.api_types == ["HMAC"]
    assert gate.simulated is False
    assert gate.ticker_example == "Gate_Spot_BTCUSDT"
    assert gate.categories == ["Spot"]
    assert by_name["Paper"].simulated is True
    # The UI drives its credential form off this, and Binance is the venue
    # that makes the field matter: it takes an Ed25519 key, not an HMAC secret.
    assert by_name["Binance"].api_types == ["ED25519"]
    # Bybit is the venue that makes ``categories`` matter: one credential, two
    # books, so the UI cannot infer which market a ticker is on from the venue.
    assert by_name["Bybit"].categories == ["Perp", "Spot"]
    # Binance's USD-M plane is a venue of its own, on one category: separate
    # keys and a separate wallet, so it cannot be a category of "Binance".
    assert by_name["BinanceFuture"].categories == ["Perp"]
    assert by_name["BinanceFuture"].api_types == ["ED25519"]
    assert by_name["BinanceFuture"].ticker_example == "BinanceFuture_Perp_BTCUSDT"


async def test_unknown_venue_is_rejected_with_400() -> None:
    with pytest.raises(HTTPException) as exc:
        await create_api(_body(venue="gate-spot"))

    assert exc.value.status_code == 400
    assert "unknown venue" in exc.value.detail
    # The message should point at the right spelling.
    assert "Gate" in exc.value.detail


async def test_unsupported_api_type_for_venue_is_rejected() -> None:
    with pytest.raises(HTTPException) as exc:
        await create_api(_body(type=venues.ED25519))

    assert exc.value.status_code == 400
    assert "does not support type" in exc.value.detail


async def test_blank_venue_is_rejected_before_the_registry() -> None:
    with pytest.raises(HTTPException) as exc:
        await create_api(_body(venue="   "))

    assert exc.value.status_code == 400
    assert "required" in exc.value.detail


async def test_garbage_api_type_is_rejected() -> None:
    with pytest.raises(HTTPException) as exc:
        await create_api(_body(type="RSA"))

    assert exc.value.status_code == 400
    assert "type must be one of" in exc.value.detail
