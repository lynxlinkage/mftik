"""The balance ledger — what a pre-lock holds and when it goes back.

The point of the ledger is the window between "order sent" and "venue knows
about it", so most of these are about what ``available`` reads as *inside*
that window.
"""

from __future__ import annotations

from decimal import Decimal

import pytest
from mft.exchange.models import (
    Balance,
    Instrument,
    OrderType,
    PlaceOrderRequest,
    Side,
)
from mft_td.oms import InsufficientAvailable, Ledger, reservation_for

BTCUSDT = Instrument(symbol="BTCUSDT", base="BTC", quote="USDT")


def _ledger(**assets: str) -> Ledger:
    ledger = Ledger()
    for asset, free in assets.items():
        ledger.apply_venue(Balance(asset=asset, free=Decimal(free)))
    return ledger


def _request(**overrides: object) -> PlaceOrderRequest:
    payload: dict[str, object] = {
        "symbol": "BTCUSDT",
        "side": Side.BUY,
        "type": OrderType.LIMIT,
        "qty": Decimal("0.01"),
        "price": Decimal("50000"),
        "client_order_id": "cid-1",
    }
    payload.update(overrides)
    return PlaceOrderRequest.model_validate(payload)


# --- what an order commits ------------------------------------------------


def test_a_buy_commits_quote_currency() -> None:
    assert reservation_for(_request(), BTCUSDT) == ("USDT", Decimal("500"))


def test_a_sell_commits_base_currency() -> None:
    held = reservation_for(_request(side=Side.SELL), BTCUSDT)
    assert held == ("BTC", Decimal("0.01"))


def test_a_market_buy_cannot_be_priced() -> None:
    """No price means no notional; the caller decides what to do about it."""
    assert (
        reservation_for(
            _request(type=OrderType.MARKET, price=None), BTCUSDT
        )
        is None
    )


def test_a_market_sell_still_commits_base() -> None:
    """Selling commits quantity, which a market order does know."""
    held = reservation_for(
        _request(side=Side.SELL, type=OrderType.MARKET, price=None), BTCUSDT
    )
    assert held == ("BTC", Decimal("0.01"))


# --- reserving and releasing ----------------------------------------------


def test_reserving_reduces_available_but_not_free() -> None:
    ledger = _ledger(USDT="1000")

    ledger.reserve("cid-1", "USDT", Decimal("400"))

    balance = ledger.balance("USDT")
    assert balance.free == Decimal("1000")  # the venue still says 1000
    assert balance.prelock == Decimal("400")
    assert balance.available == Decimal("600")


def test_a_second_order_only_sees_what_the_first_left() -> None:
    """The whole reason the ledger exists."""
    ledger = _ledger(USDT="1000")

    ledger.reserve("cid-1", "USDT", Decimal("600"))
    with pytest.raises(InsufficientAvailable) as exc:
        ledger.reserve("cid-2", "USDT", Decimal("600"))

    assert exc.value.asset == "USDT"
    assert exc.value.available == Decimal("400")
    # The refused reservation left nothing behind.
    assert ledger.balance("USDT").prelock == Decimal("600")
    assert not ledger.has_reservation("cid-2")


def test_releasing_returns_the_funds() -> None:
    ledger = _ledger(USDT="1000")
    ledger.reserve("cid-1", "USDT", Decimal("400"))

    assert ledger.release("cid-1") is True

    assert ledger.balance("USDT").prelock == Decimal("0")
    assert ledger.available("USDT") == Decimal("1000")


def test_releasing_twice_is_harmless() -> None:
    """Fill, reject and recon all release without coordinating."""
    ledger = _ledger(USDT="1000")
    ledger.reserve("cid-1", "USDT", Decimal("400"))

    assert ledger.release("cid-1") is True
    assert ledger.release("cid-1") is False
    assert ledger.available("USDT") == Decimal("1000")


def test_releasing_an_unknown_cid_is_a_noop() -> None:
    assert _ledger(USDT="1000").release("never-seen") is False


def test_a_cid_cannot_hold_two_reservations() -> None:
    ledger = _ledger(USDT="1000")
    ledger.reserve("cid-1", "USDT", Decimal("100"))

    with pytest.raises(ValueError, match="already holds"):
        ledger.reserve("cid-1", "USDT", Decimal("100"))

    assert ledger.balance("USDT").prelock == Decimal("100")


def test_zero_and_negative_amounts_reserve_nothing() -> None:
    ledger = _ledger(USDT="1000")

    ledger.reserve("cid-1", "USDT", Decimal("0"))

    assert not ledger.has_reservation("cid-1")
    assert ledger.available("USDT") == Decimal("1000")


# --- venue updates vs local reservations ----------------------------------


def test_a_venue_update_does_not_clear_a_reservation() -> None:
    """A balance push landing mid-flight must not free money we committed."""
    ledger = _ledger(USDT="1000")
    ledger.reserve("cid-1", "USDT", Decimal("400"))

    ledger.apply_venue(Balance(asset="USDT", free=Decimal("900")))

    balance = ledger.balance("USDT")
    assert balance.free == Decimal("900")
    assert balance.prelock == Decimal("400")
    assert balance.available == Decimal("500")


def test_available_never_goes_negative() -> None:
    """The venue can debit before we release; that is not negative money."""
    ledger = _ledger(USDT="1000")
    ledger.reserve("cid-1", "USDT", Decimal("400"))

    ledger.apply_venue(Balance(asset="USDT", free=Decimal("100")))

    assert ledger.balance("USDT").available == Decimal("0")


def test_reserving_an_asset_the_venue_never_reported_is_refused() -> None:
    ledger = Ledger()

    with pytest.raises(InsufficientAvailable):
        ledger.reserve("cid-1", "DOGE", Decimal("1"))


def test_a_venue_supplied_prelock_is_ignored() -> None:
    """prelock is ours; a venue payload carrying one means nothing."""
    ledger = Ledger()

    ledger.apply_venue(
        Balance(asset="USDT", free=Decimal("1000"), prelock=Decimal("999"))
    )

    assert ledger.balance("USDT").prelock == Decimal("0")
    assert ledger.available("USDT") == Decimal("1000")


def test_snapshot_covers_venue_and_reserved_assets() -> None:
    ledger = _ledger(USDT="1000", BTC="2")
    ledger.reserve("cid-1", "BTC", Decimal("1"))

    snapshot = ledger.snapshot()

    assert sorted(snapshot) == ["BTC", "USDT"]
    assert snapshot["BTC"].available == Decimal("1")
    assert snapshot["USDT"].available == Decimal("1000")
