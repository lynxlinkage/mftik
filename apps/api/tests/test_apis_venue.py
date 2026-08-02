"""POST /apis venue validation.

Every check here runs before ``session_scope()``, so the handler can be driven
directly with no database.
"""

from __future__ import annotations

import pytest
from fastapi import HTTPException
from mft.exchange import venues
from mft_api.routes.apis import create_api, list_venues
from mft_api.schemas import ApiCreateBody


def _body(**overrides: object) -> ApiCreateBody:
    payload: dict[str, object] = {
        "name": "gate spot",
        "venue": "gate_spot",
        "api_key": "key-1",
        "api_secret": "secret-1",
        "type": "HMAC",
    }
    payload.update(overrides)
    return ApiCreateBody.model_validate(payload)


async def test_list_venues_exposes_gate_spot() -> None:
    result = await list_venues()
    by_name = {v.name: v for v in result.venues}

    assert set(by_name) == {"gate_spot", "paper"}
    gate = by_name["gate_spot"]
    assert gate.label == "Gate Spot"
    assert gate.api_types == ["HMAC"]
    assert gate.simulated is False
    assert gate.symbol_example == "BTCUSDT"
    assert by_name["paper"].simulated is True


async def test_unknown_venue_is_rejected_with_400() -> None:
    with pytest.raises(HTTPException) as exc:
        await create_api(_body(venue="gate-spot"))

    assert exc.value.status_code == 400
    assert "unknown venue" in exc.value.detail
    # The message should point at the right spelling.
    assert "gate_spot" in exc.value.detail


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
