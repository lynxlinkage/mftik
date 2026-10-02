"""Selectors: the shape that is real now, and the derivation B9 owes.

IF-09 defines this layer and returns null data. The tests split in two:

* What is real now — a spec built from the parsed document, a hash that
  ignores the name, and :func:`evaluate` refusing with the ticket number.
* What B9 will make true — ``xfail(strict=True)``, covering the cases the
  ticket names: debounce, ``min_tte``, a roll that keeps the old contract
  until it expires, and fail-static. ``strict`` is the point.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest
from mftik.exchange.tickers import UniversalTicker
from mftik.protocol import (
    ChainExpiries,
    ChainRecenter,
    ChainStrikes,
    OptionChainSelect,
    RollingFutureSelect,
)
from mftik_md.selector import (
    Center,
    Hold,
    HoldReason,
    Listed,
    Listing,
    OptionChainSpec,
    RollingFutureSpec,
    Selection,
    SelectorError,
    evaluate,
    spec_hash,
)

STRIKES = (98000, 99000, 100000, 101000, 102000)
#: A Friday, and the last Friday of September 2026 — a quarterly.
FRONT = datetime(2026, 9, 25, 8, 0, tzinfo=UTC)
#: Last Friday of December 2026 — the next quarterly.
NEXT = datetime(2026, 12, 25, 8, 0, tzinfo=UTC)
#: Last Friday of March 2027 — the quarterly after that.
MARCH = datetime(2027, 3, 26, 8, 0, tzinfo=UTC)
#: First Friday of October 2026 — a weekly, not a month-end.
WEEKLY = datetime(2026, 10, 2, 8, 0, tzinfo=UTC)
#: Last Friday of October 2026 — a monthly, not a quarter.
MONTHLY = datetime(2026, 10, 30, 8, 0, tzinfo=UTC)
#: A Thursday. Not a Friday, so not in any series.
THURSDAY = datetime(2026, 9, 24, 8, 0, tzinfo=UTC)
CENTER_AT = FRONT - timedelta(hours=2)


def chain_spec(**overrides: object) -> OptionChainSpec:
    fields: dict[str, object] = {
        "name": "btc_chain",
        "venue": "Deribit",
        "underlying": "BTC",
        "ref": "ticker.Deribit_Perp_BTCUSD",
        "nearest": 1,
        "min_tte_s": 0,
        "atm": 1,
        "sides": ("C", "P"),
        "topics": ("ticker", "greeks"),
        "recenter_strikes": 1,
        "min_dwell_s": 60,
    }
    fields.update(overrides)
    return OptionChainSpec(**fields)  # type: ignore[arg-type]


def roll_spec() -> RollingFutureSpec:
    return RollingFutureSpec(
        name="btc_q",
        venue="Deribit",
        underlying="BTC",
        tenor="quarterly",
        roll_before_s=3 * 86400,
        topics=("ticker", "trade"),
    )


def option(when: datetime, strike: int, side: str) -> UniversalTicker:
    return UniversalTicker.parse(
        f"Deribit_Option_BTCUSD-{when.strftime('%y%m%d')}-{strike}-{side}"
    )


def option_members(
    when: datetime,
    strikes: tuple[int, ...] = (99000, 100000, 101000),
    sides: tuple[str, ...] = ("C", "P"),
) -> frozenset[UniversalTicker]:
    return frozenset(
        option(when, strike, side) for strike in strikes for side in sides
    )


def chain_listing(
    expiries: tuple[datetime, ...],
    *,
    strikes: tuple[int, ...] = STRIKES,
    stale: bool = False,
) -> Listing:
    rows: list[Listed] = []
    for expiry in expiries:
        for strike in strikes:
            for side in ("C", "P"):
                rows.append(
                    Listed(
                        ticker=option(expiry, strike, side),
                        underlying="BTC",
                        expiry=expiry,
                        strike=Decimal(strike),
                        option_type=side,
                    )
                )
    return Listing(tuple(rows), stale=stale)


def centred(
    expiries: tuple[datetime, ...],
    *,
    epoch: int = 3,
    at: datetime = CENTER_AT,
    strike: int = 100000,
) -> Selection:
    members: set[UniversalTicker] = set()
    for expiry in expiries:
        members |= option_members(expiry)
    return Selection(
        frozenset(members),
        epoch,
        center=Center(Decimal(strike), at),
    )


def future(when: datetime) -> Listed:
    return Listed(
        ticker=UniversalTicker.parse(
            f"Deribit_Future_BTCUSD-{when.strftime('%y%m%d')}"
        ),
        underlying="BTC",
        expiry=when,
    )


def future_ticker(when: datetime) -> UniversalTicker:
    return future(when).ticker


def curve() -> Listing:
    """Quarterlies, plus a weekly, a monthly, a Thursday and a perpetual.

    Only the quarterlies may become members of a ``quarterly`` roll. The
    others are here so a derivation that grabs "the nearest future" fails.
    """
    rows = [future(when) for when in (THURSDAY, FRONT, WEEKLY, MONTHLY, NEXT, MARCH)]
    rows.append(
        Listed(
            ticker=UniversalTicker.parse("Deribit_Perp_BTCUSD"),
            underlying="BTC",
        )
    )
    return Listing(tuple(rows))


def _not_quarterly(members: frozenset[UniversalTicker]) -> None:
    noise = {
        future_ticker(THURSDAY),
        future_ticker(WEEKLY),
        future_ticker(MONTHLY),
        future_ticker(MARCH),
        UniversalTicker.parse("Deribit_Perp_BTCUSD"),
    }
    assert noise.isdisjoint(members)


# --- types that are already true ------------------------------------------


def test_an_option_chain_spec_is_the_parsed_document() -> None:
    select = OptionChainSelect(
        name="btc_chain",
        venue="Deribit",
        underlying="BTC",
        ref="ticker.Deribit_Perp_BTCUSD",
        expiries=ChainExpiries(nearest=2, min_tte_s=2 * 3600),
        strikes=ChainStrikes(atm=5),
        topics=("ticker", "greeks"),
        recenter=ChainRecenter(strikes=1, min_dwell_s=60),
    )
    spec = OptionChainSpec.from_select(select)
    assert spec.name == "btc_chain"
    assert (spec.nearest, spec.min_tte_s, spec.atm) == (2, 7200, 5)
    assert spec.sides == ("C", "P")
    assert (spec.recenter_strikes, spec.min_dwell_s) == (1, 60)
    assert spec.ref == select.ref


def test_a_rolling_future_spec_is_the_parsed_document() -> None:
    select = RollingFutureSelect(
        name="btc_q",
        venue="Deribit",
        underlying="BTC",
        tenor="quarterly",
        roll_before_s=3 * 86400,
        topics=("trade", "ticker"),
    )
    spec = RollingFutureSpec.from_select(select)
    assert (spec.tenor, spec.roll_before_s) == ("quarterly", 3 * 86400)
    assert spec.topics == ("trade", "ticker")


def test_spec_hash_ignores_the_name_and_the_order_of_sets() -> None:
    one = chain_spec(name="btc_chain", sides=("C", "P"), topics=("ticker", "greeks"))
    two = chain_spec(name="btc_opts", sides=("P", "C"), topics=("greeks", "ticker"))
    assert spec_hash(one) == spec_hash(two)
    assert len(spec_hash(one)) == 64

    renamed = roll_spec()
    other_name = RollingFutureSpec(
        name="front",
        venue=renamed.venue,
        underlying=renamed.underlying,
        tenor=renamed.tenor,
        roll_before_s=renamed.roll_before_s,
        topics=("trade", "ticker"),
    )
    assert spec_hash(renamed) == spec_hash(other_name)
    assert spec_hash(one) != spec_hash(renamed)


def test_spec_hash_changes_when_the_shape_changes() -> None:
    assert spec_hash(chain_spec(min_tte_s=0)) != spec_hash(chain_spec(min_tte_s=7200))
    assert spec_hash(chain_spec(atm=1)) != spec_hash(chain_spec(atm=5))
    assert spec_hash(roll_spec()) != spec_hash(
        RollingFutureSpec(
            name="btc_q",
            venue="Deribit",
            underlying="BTC",
            tenor="weekly",
            roll_before_s=3 * 86400,
            topics=("ticker", "trade"),
        )
    )


def test_a_bad_spec_is_refused() -> None:
    with pytest.raises(SelectorError):
        chain_spec(nearest=0)
    with pytest.raises(SelectorError):
        chain_spec(recenter_strikes=0)
    with pytest.raises(SelectorError):
        chain_spec(sides=())
    with pytest.raises(SelectorError):
        RollingFutureSpec(
            name="btc_q",
            venue="Deribit",
            underlying="BTC",
            tenor="biweekly",
            roll_before_s=0,
            topics=("ticker",),
        )
    with pytest.raises(TypeError):
        OptionChainSpec.from_select(roll_spec())  # type: ignore[arg-type]


def test_a_selection_starts_at_epoch_one_and_current_is_a_member() -> None:
    ticker = future_ticker(FRONT)
    with pytest.raises(SelectorError):
        Selection(frozenset({ticker}), 0, current=ticker)
    with pytest.raises(SelectorError):
        Selection(frozenset(), 1, current=ticker)
    with pytest.raises(SelectorError):
        Center(Decimal(-1), CENTER_AT)
    with pytest.raises(SelectorError):
        Listed(
            ticker=ticker,
            underlying="BTC",
            expiry=datetime(2026, 9, 25, 8, 0),
        )


def test_evaluate_refuses_a_call_that_is_not_well_formed() -> None:
    listing = chain_listing((FRONT,))
    spec = chain_spec()
    now = CENTER_AT
    with pytest.raises(SelectorError):
        evaluate(listing, Decimal(100000), datetime(2026, 9, 25, 8, 0), None, spec=spec)
    with pytest.raises(TypeError):
        evaluate(listing, 100000, now, None, spec=spec)  # type: ignore[arg-type]
    with pytest.raises(TypeError):
        evaluate(listing, None, now, None, spec=object())  # type: ignore[arg-type]
    with pytest.raises(NotImplementedError, match="IF-09"):
        evaluate(listing, Decimal(100000), now, None, spec=spec)


# --- contracts B9 has to make true ----------------------------------------


@pytest.mark.xfail(strict=True, reason="B9-01 recentres only past the strike band")
def test_the_chain_does_not_recenter_inside_the_strike_band() -> None:
    """One listed strike away is not past ``recenter.strikes`` of 1 (S4).

    The dwell has elapsed, so the only reason to hold is the band. The
    reference sits on a listed strike: 101000 is the neighbour of 100000.
    """
    prev = centred((FRONT,))
    result = evaluate(
        chain_listing((FRONT,)),
        Decimal(101000),
        CENTER_AT + timedelta(seconds=120),
        prev,
        spec=chain_spec(),
    )
    assert result == Hold(HoldReason.WITHIN_BAND)


@pytest.mark.xfail(strict=True, reason="B9-01 waits out min_dwell before recentring")
def test_the_chain_does_not_recenter_inside_min_dwell() -> None:
    """Two strikes away is past the band. Thirty seconds is not an hour's dwell.

    ``min_dwell`` here is 60s. Exactly the band was the previous test;
    this one is past it and early.
    """
    prev = centred((FRONT,))
    result = evaluate(
        chain_listing((FRONT,)),
        Decimal(102000),
        CENTER_AT + timedelta(seconds=30),
        prev,
        spec=chain_spec(),
    )
    assert result == Hold(HoldReason.MIN_DWELL)


@pytest.mark.xfail(
    strict=True,
    reason="B9-01 recentres once the band and the dwell are past",
)
def test_the_chain_recenters_when_the_band_and_the_dwell_are_both_past() -> None:
    """Exactly ``min_dwell`` may move (S4). The new edge has no strike above it.

    102000 ± 1 on a board that ends there is 101000 and 102000, not a
    strike the venue did not list. The epoch increments. The old centre
    is no longer a member.
    """
    prev = centred((FRONT,))
    now = CENTER_AT + timedelta(seconds=60)
    result = evaluate(
        chain_listing((FRONT,)),
        Decimal(102000),
        now,
        prev,
        spec=chain_spec(),
    )
    assert isinstance(result, Selection)
    assert result.members == option_members(FRONT, (101000, 102000))
    assert result.epoch == prev.epoch + 1
    assert result.current is None
    assert result.center == Center(Decimal(102000), now)
    assert option(FRONT, 100000, "C") not in result.members


@pytest.mark.xfail(
    strict=True, reason="B9-01 does not bump the epoch when nothing changed"
)
def test_an_unchanged_chain_does_not_bump_the_epoch() -> None:
    """The reference is still on the centre, so this is not a band refusal."""
    prev = centred((FRONT,))
    result = evaluate(
        chain_listing((FRONT,)),
        Decimal(100000),
        CENTER_AT + timedelta(seconds=120),
        prev,
        spec=chain_spec(),
    )
    assert result == Hold(HoldReason.UNCHANGED)


@pytest.mark.xfail(strict=True, reason="B9-01 skips an expiry closer than min_tte")
def test_min_tte_skips_an_expiry_closer_than_the_window() -> None:
    """The front expiry is an hour away and ``min_tte`` is two hours (S2).

    ``nearest`` is 2 and only one expiry qualifies, so the chain is that
    one expiry — nothing is invented to fill the count. First selection,
    so it centres now, on the reference.
    """
    now = FRONT - timedelta(hours=1)
    back = FRONT + timedelta(days=10)
    result = evaluate(
        chain_listing((FRONT, back)),
        Decimal(100000),
        now,
        None,
        spec=chain_spec(nearest=2, min_tte_s=2 * 3600),
    )
    assert isinstance(result, Selection)
    assert result.members == option_members(back)
    assert result.epoch == 1
    assert result.current is None
    assert result.center == Center(Decimal(100000), now)
    assert option_members(FRONT).isdisjoint(result.members)


@pytest.mark.xfail(strict=True, reason="B9-01 keeps an expiry at exactly min_tte")
def test_an_expiry_at_exactly_min_tte_is_kept() -> None:
    """不到 means closer than the window. Equal to it is still in (S2)."""
    now = FRONT - timedelta(hours=2)
    back = FRONT + timedelta(days=10)
    result = evaluate(
        chain_listing((FRONT, back)),
        Decimal(100000),
        now,
        None,
        spec=chain_spec(nearest=1, min_tte_s=2 * 3600),
    )
    assert isinstance(result, Selection)
    assert result.members == option_members(FRONT)
    assert option_members(back).isdisjoint(result.members)


@pytest.mark.xfail(strict=True, reason="B9-01 rotates an expiry even inside min_dwell")
def test_min_tte_moves_membership_even_inside_min_dwell() -> None:
    """The reference has not moved. The front expiry has entered the window.

    Dwell is about the centre, not about keeping a contract that ``min_tte``
    has already dropped (S5). The centre's strike and its timestamp stay,
    and the epoch still increments because the universe changed.
    """
    now = CENTER_AT + timedelta(seconds=10)
    front = now + timedelta(hours=1)
    back = now + timedelta(days=30)
    prev = centred((front, back), at=CENTER_AT)
    result = evaluate(
        chain_listing((front, back)),
        Decimal(100000),
        now,
        prev,
        spec=chain_spec(nearest=2, min_tte_s=2 * 3600),
    )
    assert isinstance(result, Selection)
    assert result.members == option_members(back)
    assert result.epoch == prev.epoch + 1
    assert result.center == prev.center
    assert result.current is None


@pytest.mark.xfail(
    strict=True,
    reason="B9-01 empties a chain whose board is all inside min_tte",
)
def test_a_chain_with_nothing_outside_min_tte_is_empty() -> None:
    """A board that was read, and every expiry on it is too close (S2).

    This is not the unanswered question of a listing with no rows. The
    centre is kept so a later expiry still debounces against it.
    """
    now = FRONT - timedelta(hours=1)
    prev = centred((FRONT,), epoch=2)
    result = evaluate(
        chain_listing((FRONT,)),
        Decimal(100000),
        now,
        prev,
        spec=chain_spec(min_tte_s=2 * 3600),
    )
    assert isinstance(result, Selection)
    assert result.members == frozenset()
    assert result.epoch == 3
    assert result.current is None
    assert result.center == prev.center


@pytest.mark.xfail(strict=True, reason="B9-02 keeps the front future until roll_before")
def test_the_front_quarterly_stays_current_until_roll_before() -> None:
    """A second before the window opens, current is still the front (S6).

    The weekly, the monthly, the Thursday and the perpetual are on the
    board and are not members. Neither is the quarterly after next.
    """
    now = FRONT - timedelta(days=3, seconds=1)
    result = evaluate(curve(), None, now, None, spec=roll_spec())
    assert isinstance(result, Selection)
    assert result.current == future_ticker(FRONT)
    assert result.members == frozenset({future_ticker(FRONT)})
    assert result.epoch == 1
    assert result.center is None
    _not_quarterly(result.members)


@pytest.mark.xfail(
    strict=True,
    reason="B9-02 rolls at exactly roll_before and keeps the old one",
)
def test_the_roll_keeps_the_old_future_until_it_expires() -> None:
    """At exactly ``roll_before``, current is the next quarterly (S6).

    The old one is still a member. A second before it settles it is still
    a member. At the settlement instant it is gone, and current is the
    contract the roll already moved to. No reference price is required.
    """
    spec = roll_spec()
    opened = evaluate(
        curve(), None, FRONT - timedelta(days=3), None, spec=spec
    )
    assert isinstance(opened, Selection)
    assert opened.current == future_ticker(NEXT)
    assert opened.members == frozenset(
        {future_ticker(FRONT), future_ticker(NEXT)}
    )
    _not_quarterly(opened.members)

    almost = evaluate(
        curve(), None, FRONT - timedelta(seconds=1), None, spec=spec
    )
    assert isinstance(almost, Selection)
    assert almost.current == future_ticker(NEXT)
    assert future_ticker(FRONT) in almost.members
    assert future_ticker(NEXT) in almost.members

    settled = evaluate(curve(), None, FRONT, None, spec=spec)
    assert isinstance(settled, Selection)
    assert settled.current == future_ticker(NEXT)
    assert settled.members == frozenset({future_ticker(NEXT)})
    assert future_ticker(FRONT) not in settled.members


@pytest.mark.xfail(strict=True, reason="B9-01 / B9-02 hold on a stale listing")
def test_a_stale_listing_holds_even_when_the_reference_is_down() -> None:
    """Fail-static. Stale wins over a reference that is also down (S7)."""
    prev = centred((FRONT,))
    stale = chain_listing((FRONT,), stale=True)
    result = evaluate(stale, None, CENTER_AT, prev, spec=chain_spec())
    assert result == Hold(HoldReason.LISTING_STALE)


@pytest.mark.xfail(
    strict=True,
    reason="B9-01 holds an option chain when the reference is down",
)
def test_a_down_reference_holds_the_chain() -> None:
    listing = chain_listing((FRONT,))
    prev = centred((FRONT,))
    now = CENTER_AT
    for ref in (None, Decimal(0), Decimal("-1")):
        result = evaluate(listing, ref, now, prev, spec=chain_spec())
        assert result == Hold(HoldReason.REF_DOWN)


@pytest.mark.xfail(strict=True, reason="B9-02 does not roll on a stale listing")
def test_a_stale_listing_does_not_roll() -> None:
    """The window is open. The listing is not trusted, so current stays."""
    prev = Selection(
        frozenset({future_ticker(FRONT)}),
        1,
        current=future_ticker(FRONT),
    )
    listing = Listing(curve().instruments, stale=True)
    result = evaluate(
        listing, None, FRONT - timedelta(days=3), prev, spec=roll_spec()
    )
    assert result == Hold(HoldReason.LISTING_STALE)
