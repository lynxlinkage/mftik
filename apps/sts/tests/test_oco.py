"""OneCancelOther — what it refuses to place, and how the pair resolves.

The strategy is driven directly: recon through ``on_recon_done``, quotes
through ``on_fetch_bestquote``, and venue answers through ``on_order_update`` /
``on_order_reject``, with a fake OMS standing in for TD. What is under test is
the legality check against that one reference quote, that only the first quote
is ever read, and that every ending leaves nothing resting behind.
"""

from __future__ import annotations

from decimal import Decimal

import pytest
from mft.exchange.models import (
    BestQuote,
    Fill,
    Order,
    OrderStatus,
    OrderType,
    Side,
    TimeInForce,
)
from mft.exchange.oms import OmsView
from mft.exchange.tickers import UniversalTicker
from mft.protocol import (
    CancelReject,
    MdBestQuoteResult,
    OrderReject,
    ReconDone,
    RejectCode,
    SymbolInfo,
)
from mft_sts.impl.oco import LEG_COUNT, OneCancelOther

PAPER_BTC = UniversalTicker.parse("Paper_Spot_BTCUSDT")

BTCUSDT = SymbolInfo(
    universal_ticker="Paper_Spot_BTCUSDT",
    base="BTC",
    quote="USDT",
    exch_ticker="BTCUSDT",
    filters=[
        {"name": "price_tick", "value": Decimal("0.01")},
        {"name": "qty_step", "value": Decimal("0.00001")},
        {"name": "min_qty", "value": Decimal("0.00001")},
        {"name": "min_notional", "value": Decimal("5")},
    ],
)

API_ID = 7


class FakeOms:
    """Stands in for TD. ``accepts`` answers submits in order."""

    def __init__(self, *, accepts: list[bool] | None = None) -> None:
        self.submitted: list[dict] = []
        self.cancelled: list[str] = []
        self.accepts = accepts
        self.accept_cancel = True
        self.reject_reason = "no funds"
        self.reject_code: int | str = RejectCode.TD_INSUFFICIENT_BALANCE
        self._n = 0
        self._last_cid: str | None = None
        self._accepted_last = True

    @property
    def last_client_order_id(self) -> str | None:
        return self._last_cid

    @property
    def last_reject_reason(self) -> str:
        return "" if self._accepted_last else self.reject_reason

    @property
    def last_reject_code(self) -> int | str:
        return RejectCode.NONE if self._accepted_last else self.reject_code

    async def submit_order(
        self, api_id, *, symbol, side, qty, type, price=None, tif=None
    ):
        self._n += 1
        self._last_cid = f"cid-{self._n}"
        accepted = True
        if self.accepts is not None and self._n <= len(self.accepts):
            accepted = self.accepts[self._n - 1]
        self._accepted_last = accepted
        self.submitted.append(
            {
                "cid": self._last_cid,
                "symbol": symbol,
                "side": side,
                "qty": qty,
                "type": type,
                "price": price,
                "tif": tif,
                "accepted": accepted,
            }
        )
        return accepted

    async def cancel_order(self, api_id, client_order_id):
        self.cancelled.append(str(client_order_id))
        self._accepted_last = self.accept_cancel
        return self.accept_cancel


class _FakeToken:
    """A timer token the tests fire by hand."""

    def __init__(self) -> None:
        self.registered: list[tuple] = []

    def register(self, first_ms, interval_ms, func):
        self.registered.append((first_ms, interval_ms, func))
        return self

    def cancel(self) -> None:
        self.registered.clear()

    async def fire(self) -> None:
        """Run the callback the strategy registered, as the timer would."""
        assert self.registered, "nothing registered on this token"
        await self.registered[-1][2]()


class FakeLedger:
    """TD's ledger. Enough of everything unless a test says otherwise."""

    def __init__(self) -> None:
        self.balances: dict[str, Decimal] = {}
        self.default = Decimal("1000000")

    async def available(self, asset: str, api_id=None) -> Decimal:
        return self.balances.get(asset, self.default)


class FakeSession:
    def __init__(self) -> None:
        self.td_api_ids = [API_ID]
        self.md_ids = ["bestquote.Paper_Spot_BTCUSDT"]
        self.exits: list[str] = []
        #: Reasons the strategy ended as ``failed`` rather than ``done``.
        self.failures: list[str] = []

    def request_exit(self, reason: str, *, failed: bool = False) -> None:
        self.exits.append(reason)
        if failed:
            self.failures.append(reason)


def _filled_leg(strat) -> str | None:
    """The leg named in an `oco_filled` exit, or None if it did not fill."""
    for reason in strat.session.exits:
        if reason.startswith("oco_filled:"):
            return reason.split(":", 1)[1].strip()
    return None


class FakeTimer:
    """A clock the test moves by hand, handing out one shared token."""

    def __init__(self) -> None:
        self._now = 1_000_000
        self.tokens: list[_FakeToken] = []

    def now_ms(self) -> int:
        return self._now

    def advance_s(self, seconds: float) -> None:
        self._now += int(seconds * 1000)

    def token(self) -> _FakeToken:
        tok = _FakeToken()
        self.tokens.append(tok)
        return tok


class FakeMds:
    """Records quote requests; the test delivers the answers by hand."""

    def __init__(self, *, accepted: bool = True) -> None:
        self.calls: list[UniversalTicker] = []
        self.accepted = accepted
        self.last_reject_reason = "" if accepted else "no MD running"
        self._seq = 0

    async def fetch_best_quote(self, ticker: UniversalTicker) -> str | None:
        self.calls.append(ticker)
        if not self.accepted:
            return None
        self._seq += 1
        return f"q{self._seq}"


def _paras(**overrides) -> dict:
    payload = {
        "orders": [
            {"side": "buy", "price": 49000, "qty": Decimal("0.001")},
            {"side": "sell", "price": 51000, "qty": Decimal("0.001")},
        ]
    }
    payload.update(overrides)
    return payload


def _strategy(
    *,
    accepts: list[bool] | None = None,
    mds_accepts: bool = True,
    **overrides,
) -> OneCancelOther:
    """A bound strategy with TD, the clock and the symbol plane stubbed out."""
    strat = OneCancelOther()
    strat.paras = OneCancelOther.on_initialized(_paras(**overrides))
    strat.session = FakeSession()  # type: ignore[assignment]
    strat.oms = FakeOms(accepts=accepts)  # type: ignore[assignment]
    strat.timer = FakeTimer()  # type: ignore[assignment]
    strat.ledger = FakeLedger()  # type: ignore[assignment]
    strat.mds = FakeMds(accepted=mds_accepts)  # type: ignore[assignment]
    strat._info = BTCUSDT
    strat._ticker = PAPER_BTC

    async def _noop_log(message: str, *, level: str = "info", **extra) -> None:
        return None

    strat.log = _noop_log  # type: ignore[method-assign]
    # Every cid the fake OMS mints belongs to this session.
    strat.owns = lambda cid: True  # type: ignore[method-assign]
    return strat


async def _armed(**kwargs) -> OneCancelOther:
    """A strategy past ``on_start`` with TD recon already landed."""
    strat = _strategy(**kwargs)
    await strat.on_start()
    await strat.on_recon_done(ReconDone(session_id="s", api_id=API_ID))
    return strat


def _quote(bid: str = "50000", ask: str = "50010") -> MdBestQuoteResult:
    """A best-quote query answer, as MD's fetch plane would publish it."""
    return _quote_result(
        BestQuote(
            symbol="BTCUSDT",
            bid=Decimal(bid),
            bid_qty=Decimal("1"),
            ask=Decimal(ask),
            ask_qty=Decimal("1"),
        )
    )


def _quote_result(
    quote: BestQuote | None, *, ok: bool = True, reason: str = ""
) -> MdBestQuoteResult:
    return MdBestQuoteResult(
        query_id="q1",
        ticker="Paper_Spot_BTCUSDT",
        ok=ok,
        quote=quote,
        reason=reason,
    )


def _update(
    cid: str,
    status: OrderStatus,
    *,
    filled: str = "0",
    qty: str = "0.001",
    side: Side = Side.BUY,
) -> Order:
    return Order(
        client_order_id=cid,
        symbol="BTCUSDT",
        side=side,
        type=OrderType.LIMIT,
        status=status,
        qty=Decimal(qty),
        filled_qty=Decimal(filled),
        price=Decimal("49000"),
    )


# --- parameters ------------------------------------------------------------


def test_orders_is_required() -> None:
    with pytest.raises(ValueError, match="orders is required"):
        OneCancelOther.on_initialized({})


@pytest.mark.parametrize("count", [1, 3])
def test_exactly_two_legs(count: int) -> None:
    leg = {"side": "buy", "price": 49000, "qty": 1}
    with pytest.raises(ValueError, match="exactly 2 legs"):
        OneCancelOther.on_initialized({"orders": [leg] * count})


def test_each_leg_names_itself_when_its_side_is_wrong() -> None:
    with pytest.raises(ValueError, match=r"orders\[1\].side must be"):
        OneCancelOther.on_initialized(
            {
                "orders": [
                    {"side": "buy", "price": 49000, "qty": 1},
                    {"side": "long", "price": 51000, "qty": 1},
                ]
            }
        )


@pytest.mark.parametrize("field", ["price", "qty"])
def test_price_and_qty_are_required_per_leg(field: str) -> None:
    leg = {"side": "buy", "price": 49000, "qty": 1}
    del leg[field]
    with pytest.raises(ValueError, match=rf"orders\[0\].{field} is required"):
        OneCancelOther.on_initialized(
            {"orders": [leg, {"side": "sell", "price": 51000, "qty": 1}]}
        )


@pytest.mark.parametrize("bad", [0, -1])
def test_price_and_qty_must_be_positive(bad: int) -> None:
    with pytest.raises(ValueError, match="must be positive"):
        OneCancelOther.on_initialized(
            {
                "orders": [
                    {"side": "buy", "price": bad, "qty": 1},
                    {"side": "sell", "price": 51000, "qty": 1},
                ]
            }
        )


def test_arm_timeout_defaults_and_must_be_positive() -> None:
    assert OneCancelOther.on_initialized(_paras())["arm_timeout_s"] > 0
    with pytest.raises(ValueError, match="arm_timeout_s must be positive"):
        OneCancelOther.on_initialized(_paras(arm_timeout_s=0))


# --- arming ----------------------------------------------------------------


async def test_a_quote_alone_places_nothing() -> None:
    """The ledger is not real until recon, and the funds check reads it.

    A stray answer cannot arm the pair either: nothing asked for it, because
    asking is what recon triggers.
    """
    strat = _strategy()
    await strat.on_start()
    await strat.on_fetch_bestquote(_quote())

    assert strat.oms.submitted == []
    assert strat.mds.calls == []


async def test_recon_alone_places_nothing() -> None:
    strat = _strategy()
    await strat.on_start()
    await strat.on_recon_done(ReconDone(session_id="s", api_id=API_ID))

    assert strat.oms.submitted == []


async def test_recon_asks_for_the_quote_and_the_answer_places_the_pair() -> None:
    """One order now, not a race: recon arms, arming asks, the answer places.

    The quote is fetched as late as it can be and still be before the orders
    go out, which is what the legality check wants it to be.
    """
    strat = await _armed()

    assert strat.mds.calls == [PAPER_BTC]
    assert strat.oms.submitted == []

    await strat.on_fetch_bestquote(_quote())
    assert len(strat.oms.submitted) == 2


async def test_a_quote_that_never_comes_back_ends_the_session() -> None:
    """The arm timeout still bounds it — asking is not the same as answered."""
    strat = await _armed()
    assert strat.mds.calls

    await strat.timer.tokens[0].fire()

    assert strat.oms.submitted == []
    assert strat.session.failures == ["oco_not_armed"]


async def test_a_query_that_never_leaves_ends_the_session_now() -> None:
    """No MD to ask means nothing will arrive; waiting out the arm timeout
    would only make that a slower failure."""
    strat = _strategy(mds_accepts=False)
    await strat.on_start()
    await strat.on_recon_done(ReconDone(session_id="s", api_id=API_ID))

    assert strat.oms.submitted == []
    assert strat.session.failures == ["oco_no_quote"]


async def test_nothing_arriving_at_all_ends_the_session() -> None:
    strat = _strategy()
    await strat.on_start()
    await strat.timer.tokens[0].fire()

    assert strat.oms.submitted == []
    assert strat.session.exits == ["oco_not_armed"]
    # Nothing was placed and nothing will be — that is a failure.
    assert strat.session.failures == ["oco_not_armed"]


async def test_a_recon_for_another_account_does_not_arm_it() -> None:
    strat = _strategy()
    await strat.on_start()
    await strat.on_recon_done(ReconDone(session_id="s", api_id=API_ID + 1))
    await strat.on_fetch_bestquote(_quote())

    assert strat.oms.submitted == []


# --- the one quote ---------------------------------------------------------


async def test_a_legal_pair_places_both_legs_post_only() -> None:
    strat = await _armed()
    await strat.on_fetch_bestquote(_quote(bid="50000", ask="50010"))

    assert [o["side"] for o in strat.oms.submitted] == [Side.BUY, Side.SELL]
    assert [o["price"] for o in strat.oms.submitted] == [
        Decimal("49000"),
        Decimal("51000"),
    ]
    # Post-only is what keeps the legality check true if the book moved
    # between the reference quote and the send.
    assert all(o["tif"] is TimeInForce.POST_ONLY for o in strat.oms.submitted)
    assert strat.session.exits == []


async def test_only_the_first_quote_is_read() -> None:
    """A later quote that would make the pair illegal changes nothing."""
    strat = await _armed()
    await strat.on_fetch_bestquote(_quote(bid="50000", ask="50010"))
    placed = len(strat.oms.submitted)

    # The market runs through both legs. The pair is already resting.
    await strat.on_fetch_bestquote(_quote(bid="48000", ask="48010"))
    await strat.on_fetch_bestquote(_quote(bid="52000", ask="52010"))

    assert len(strat.oms.submitted) == placed
    assert strat.session.exits == []
    assert strat.oms.cancelled == []


async def test_an_empty_book_is_not_taken_as_the_reference() -> None:
    """A one-sided quote says nothing about whether a leg can rest.

    With a feed the next message came on its own; a query has to be asked
    again, so the retry is what keeps the old behaviour.
    """
    strat = await _armed()
    asked = len(strat.mds.calls)

    await strat.on_fetch_bestquote(_quote_result(None))
    assert strat.oms.submitted == []

    # A retry was scheduled rather than the pair being judged on nothing.
    await strat.timer.tokens[-1].fire()
    assert len(strat.mds.calls) == asked + 1

    await strat.on_fetch_bestquote(_quote())
    assert len(strat.oms.submitted) == 2


async def test_a_failed_query_is_asked_again_too() -> None:
    """A failed read says nothing at all about the book."""
    strat = await _armed()
    asked = len(strat.mds.calls)

    await strat.on_fetch_bestquote(
        _quote_result(None, ok=False, reason="[429] TOO_MANY_REQUESTS")
    )
    assert strat.oms.submitted == []

    await strat.timer.tokens[-1].fire()
    assert len(strat.mds.calls) == asked + 1


# --- legality --------------------------------------------------------------


async def test_a_buy_at_or_above_the_ask_is_illegal() -> None:
    strat = await _armed(
        orders=[
            {"side": "buy", "price": 50010, "qty": Decimal("0.001")},
            {"side": "sell", "price": 51000, "qty": Decimal("0.001")},
        ]
    )
    await strat.on_fetch_bestquote(_quote(bid="50000", ask="50010"))

    assert strat.oms.submitted == []
    assert strat.session.exits == ["oco_illegal"]


async def test_a_sell_at_or_below_the_bid_is_illegal() -> None:
    strat = await _armed(
        orders=[
            {"side": "buy", "price": 49000, "qty": Decimal("0.001")},
            {"side": "sell", "price": 50000, "qty": Decimal("0.001")},
        ]
    )
    await strat.on_fetch_bestquote(_quote(bid="50000", ask="50010"))

    assert strat.oms.submitted == []
    assert strat.session.exits == ["oco_illegal"]


async def test_an_illegal_pair_places_neither_leg() -> None:
    """The check runs before anything is sent, not between the two sends."""
    strat = await _armed(
        orders=[
            # The first leg on its own would be perfectly placeable.
            {"side": "buy", "price": 49000, "qty": Decimal("0.001")},
            {"side": "sell", "price": 49500, "qty": Decimal("0.001")},
        ]
    )
    await strat.on_fetch_bestquote(_quote(bid="50000", ask="50010"))

    assert strat.oms.submitted == []
    assert strat.oms.cancelled == []


async def test_two_legs_on_the_same_side_are_legal() -> None:
    """Two entries, whichever the market reaches first. Still an OCO."""
    strat = await _armed(
        orders=[
            {"side": "buy", "price": 49000, "qty": Decimal("0.001")},
            {"side": "buy", "price": 48000, "qty": Decimal("0.001")},
        ]
    )
    await strat.on_fetch_bestquote(_quote(bid="50000", ask="50010"))

    assert [o["side"] for o in strat.oms.submitted] == [Side.BUY, Side.BUY]
    assert strat.session.exits == []


async def test_a_leg_below_the_venue_minimum_is_illegal() -> None:
    strat = await _armed(
        orders=[
            # 0.00001 @ 49000 is 0.49 of notional against a minimum of 5.
            {"side": "buy", "price": 49000, "qty": Decimal("0.00001")},
            {"side": "sell", "price": 51000, "qty": Decimal("0.001")},
        ]
    )
    await strat.on_fetch_bestquote(_quote())

    assert strat.oms.submitted == []
    assert strat.session.exits == ["oco_illegal"]


async def test_a_qty_that_rounds_away_is_illegal() -> None:
    strat = await _armed(
        orders=[
            # Below the 0.00001 step, so it floors to nothing.
            {"side": "buy", "price": 49000, "qty": Decimal("0.000001")},
            {"side": "sell", "price": 51000, "qty": Decimal("0.001")},
        ]
    )
    await strat.on_fetch_bestquote(_quote())

    assert strat.oms.submitted == []
    assert strat.session.exits == ["oco_illegal"]


async def test_prices_are_snapped_to_the_tick_before_they_are_judged() -> None:
    """Flooring moves a SELL toward the bid, so the check reads what is sent."""
    strat = await _armed(
        orders=[
            {"side": "buy", "price": Decimal("49000.007"), "qty": Decimal("0.001")},
            {"side": "sell", "price": Decimal("51000.007"), "qty": Decimal("0.001")},
        ]
    )
    await strat.on_fetch_bestquote(_quote())

    assert [o["price"] for o in strat.oms.submitted] == [
        Decimal("49000.00"),
        Decimal("51000.00"),
    ]


# --- funding ---------------------------------------------------------------


async def test_both_legs_must_be_affordable_together() -> None:
    """The venue holds margin for each resting order, so both are checked."""
    strat = await _armed()
    # 0.001 @ 49000 needs 49 USDT; the sell needs 0.001 BTC.
    strat.ledger.balances["USDT"] = Decimal("48")
    await strat.on_fetch_bestquote(_quote())

    assert strat.oms.submitted == []
    assert strat.session.exits == ["oco_insufficient_balance"]


async def test_same_side_legs_have_their_commitments_summed() -> None:
    """Two buys tie up the quote currency twice, not once."""
    strat = await _armed(
        orders=[
            {"side": "buy", "price": 49000, "qty": Decimal("0.001")},
            {"side": "buy", "price": 48000, "qty": Decimal("0.001")},
        ]
    )
    # Enough for either leg alone (49 or 48), not for the pair (97).
    strat.ledger.balances["USDT"] = Decimal("60")
    await strat.on_fetch_bestquote(_quote())

    assert strat.oms.submitted == []
    assert strat.session.exits == ["oco_insufficient_balance"]


# --- how it resolves -------------------------------------------------------


async def _placed(**kwargs) -> OneCancelOther:
    strat = await _armed(**kwargs)
    await strat.on_fetch_bestquote(_quote())
    assert len(strat.oms.submitted) == 2
    return strat


async def test_a_complete_fill_cancels_the_other_and_exits() -> None:
    strat = await _placed()
    await strat.on_order_update(
        API_ID, _update("cid-1", OrderStatus.FILLED, filled="0.001")
    )

    assert strat.oms.cancelled == ["cid-2"]
    assert _filled_leg(strat) is not None
    # A pair that resolved the way an OCO should is a natural end.
    assert strat.session.failures == []


async def test_the_second_leg_can_be_the_winner_too() -> None:
    strat = await _placed()
    await strat.on_order_update(
        API_ID,
        _update("cid-2", OrderStatus.FILLED, filled="0.001", side=Side.SELL),
    )

    assert strat.oms.cancelled == ["cid-1"]
    assert _filled_leg(strat) is not None


async def test_a_partial_fill_decides_nothing() -> None:
    strat = await _placed()
    await strat.on_order_update(
        API_ID, _update("cid-1", OrderStatus.PARTIALLY_FILLED, filled="0.0002")
    )
    await strat.on_fill(
        API_ID,
        Fill(
            order_id="o1",
            client_order_id="cid-1",
            symbol="BTCUSDT",
            side=Side.BUY,
            price=Decimal("49000"),
            qty=Decimal("0.0002"),
        ),
    )

    assert strat.oms.cancelled == []
    assert strat.session.exits == []


async def test_the_losers_cancel_coming_back_does_not_re_trigger() -> None:
    """The loser's own CANCELED update must not read as a lost leg."""
    strat = await _placed()
    await strat.on_order_update(
        API_ID, _update("cid-1", OrderStatus.FILLED, filled="0.001")
    )
    await strat.on_order_update(
        API_ID, _update("cid-2", OrderStatus.CANCELED, side=Side.SELL)
    )

    assert strat.oms.cancelled == ["cid-2"]
    assert _filled_leg(strat) is not None


async def test_a_leg_lost_without_filling_takes_the_other_with_it() -> None:
    """One leg resting alone is not a choice between two prices."""
    strat = await _placed()
    await strat.on_order_update(
        API_ID, _update("cid-1", OrderStatus.CANCELED)
    )

    assert strat.oms.cancelled == ["cid-2"]
    assert strat.session.exits == ["oco_leg_lost"]


async def test_a_venue_reject_takes_the_other_leg_with_it() -> None:
    """Post-only refused it, so the book moved past the reference quote."""
    strat = await _placed()
    await strat.on_order_reject(
        API_ID,
        OrderReject(
            api_id=API_ID,
            client_order_id="cid-1",
            reason="would cross",
            error_code=RejectCode.VENUE_POST_ONLY_WOULD_CROSS,
        ),
    )

    assert strat.oms.cancelled == ["cid-2"]
    assert strat.session.exits == ["oco_leg_rejected"]


async def test_a_second_leg_td_refuses_unwinds_the_first() -> None:
    strat = await _armed(accepts=[True, False])
    await strat.on_fetch_bestquote(_quote())

    assert len(strat.oms.submitted) == 2
    assert strat.oms.cancelled == ["cid-1"]
    assert strat.session.exits == ["oco_refused"]


async def test_a_first_leg_td_refuses_places_no_second() -> None:
    strat = await _armed(accepts=[False])
    await strat.on_fetch_bestquote(_quote())

    assert len(strat.oms.submitted) == 1
    assert strat.oms.cancelled == []
    assert strat.session.exits == ["oco_refused"]


async def test_a_refused_cancel_is_not_an_error() -> None:
    """The loser was already gone, which is what the cancel wanted."""
    strat = await _placed()
    await strat.on_order_update(
        API_ID, _update("cid-1", OrderStatus.FILLED, filled="0.001")
    )
    await strat.on_cancel_reject(
        API_ID,
        CancelReject(
            api_id=API_ID,
            client_order_id="cid-2",
            reason="not found",
            error_code=RejectCode.VENUE_ORDER_NOT_FOUND,
        ),
    )

    assert _filled_leg(strat) is not None


# --- pause and stop --------------------------------------------------------


async def test_a_paused_strategy_places_nothing_until_it_resumes() -> None:
    strat = await _armed()
    await strat.on_pause()
    await strat.on_fetch_bestquote(_quote())
    assert strat.oms.submitted == []

    await strat.on_resume()
    assert len(strat.oms.submitted) == 2


async def test_stopping_leaves_nothing_resting() -> None:
    strat = await _placed()
    await strat.on_stop()

    assert sorted(strat.oms.cancelled) == ["cid-1", "cid-2"]


async def test_stopping_after_a_fill_only_cancels_what_is_still_open() -> None:
    strat = await _placed()
    await strat.on_order_update(
        API_ID, _update("cid-1", OrderStatus.FILLED, filled="0.001")
    )
    await strat.on_order_update(
        API_ID, _update("cid-2", OrderStatus.CANCELED, side=Side.SELL)
    )
    await strat.on_stop()

    assert strat.oms.cancelled == ["cid-2"]


# --- events that are not ours ----------------------------------------------


async def test_another_sessions_order_is_ignored() -> None:
    strat = await _placed()
    strat.owns = lambda cid: False  # type: ignore[method-assign]
    await strat.on_order_update(
        API_ID, _update("someone-else", OrderStatus.FILLED, filled="1")
    )

    assert strat.oms.cancelled == []
    assert strat.session.exits == []


async def test_our_own_slot_but_not_our_order_is_ignored() -> None:
    """Same session, different strategy order — not part of this pair."""
    strat = await _placed()
    await strat.on_order_update(
        API_ID, _update("cid-99", OrderStatus.FILLED, filled="1")
    )

    assert strat.oms.cancelled == []
    assert strat.session.exits == []


# --- rebuild ---------------------------------------------------------------


def _restorable(**overrides) -> OneCancelOther:
    """A pair that has not been armed — as one is when it is rebuilt."""
    strat = _strategy(**overrides)
    strat.owns = lambda cid: str(cid).startswith("mine-")  # type: ignore[method-assign]
    return strat


def _leg_order(
    cid: str,
    status: OrderStatus,
    *,
    side: Side,
    price: str,
    filled: str = "0",
) -> Order:
    return Order(
        client_order_id=cid,
        symbol="BTCUSDT",
        side=side,
        type=OrderType.LIMIT,
        status=status,
        qty=Decimal("0.001"),
        filled_qty=Decimal(filled),
        price=Decimal(price),
    )


async def _restore(strat: OneCancelOther, *orders: Order) -> None:
    await strat.on_rebuild({})
    await strat.on_start()
    await strat.on_recon_done(
        ReconDone(
            session_id="s",
            api_id=API_ID,
            oms=OmsView(orders={str(o.client_order_id): o for o in orders}),
        )
    )


@pytest.mark.asyncio
async def test_a_restored_pair_takes_both_legs_back() -> None:
    """Adoption has to rebuild the cid→leg map: every order handler ignores a
    cid it has no leg for, so without it the pair would rest unwatched."""
    strat = _restorable()
    buy = _leg_order("mine-buy", OrderStatus.NEW, side=Side.BUY, price="49000")
    sell = _leg_order("mine-sell", OrderStatus.NEW, side=Side.SELL, price="51000")

    await _restore(strat, buy, sell)

    assert strat._open == {"mine-buy", "mine-sell"}
    assert set(strat._legs) == {"mine-buy", "mine-sell"}
    assert strat._placed is True
    assert strat.session.exits == []


@pytest.mark.asyncio
async def test_a_restored_pair_still_settles_on_a_fill() -> None:
    """The adopted legs behave like placed ones — the point of restoring the
    mapping rather than only counting them."""
    strat = _restorable()
    buy = _leg_order("mine-buy", OrderStatus.NEW, side=Side.BUY, price="49000")
    sell = _leg_order("mine-sell", OrderStatus.NEW, side=Side.SELL, price="51000")
    await _restore(strat, buy, sell)

    await strat.on_order_update(
        API_ID,
        _leg_order(
            "mine-buy", OrderStatus.FILLED, side=Side.BUY, price="49000",
            filled="0.001",
        ),
    )

    assert _filled_leg(strat) is not None
    assert strat.oms.cancelled == ["mine-sell"]


@pytest.mark.asyncio
async def test_a_leg_that_filled_while_away_decides_the_pair() -> None:
    strat = _restorable()
    buy = _leg_order(
        "mine-buy", OrderStatus.FILLED, side=Side.BUY, price="49000",
        filled="0.001",
    )
    sell = _leg_order("mine-sell", OrderStatus.NEW, side=Side.SELL, price="51000")

    await _restore(strat, buy, sell)

    assert _filled_leg(strat) is not None
    assert strat.oms.cancelled == ["mine-sell"]


@pytest.mark.asyncio
async def test_a_lone_survivor_is_not_an_oco() -> None:
    """Same rule as at runtime: one leg left offers no choice, so it goes."""
    strat = _restorable()
    buy = _leg_order("mine-buy", OrderStatus.NEW, side=Side.BUY, price="49000")
    sell = _leg_order(
        "mine-sell", OrderStatus.CANCELED, side=Side.SELL, price="51000"
    )

    await _restore(strat, buy, sell)

    assert strat.session.failures == ["oco_leg_lost"]
    assert strat.oms.cancelled == ["mine-buy"]


@pytest.mark.asyncio
async def test_both_legs_gone_while_away_ends_the_pair() -> None:
    strat = _restorable()
    buy = _leg_order("mine-buy", OrderStatus.CANCELED, side=Side.BUY, price="49000")
    sell = _leg_order(
        "mine-sell", OrderStatus.CANCELED, side=Side.SELL, price="51000"
    )

    await _restore(strat, buy, sell)

    assert strat.session.failures == ["oco_legs_lost_while_away"]


@pytest.mark.asyncio
async def test_nothing_resting_means_placing_again() -> None:
    """The ordinary case: `on_stop` cancels both legs, so a shutdown that got
    that far leaves nothing behind and the pair is simply placed again."""
    strat = _restorable()
    await _restore(strat)
    assert strat._placed is False

    await strat.on_fetch_bestquote(_quote())

    assert strat._placed is True
    assert len(strat.oms.submitted) == LEG_COUNT
    assert strat.session.exits == []


@pytest.mark.asyncio
async def test_another_sessions_orders_are_not_adopted() -> None:
    strat = _restorable()
    theirs = _leg_order("theirs-1", OrderStatus.NEW, side=Side.BUY, price="49000")

    await _restore(strat, theirs)

    assert strat._legs == {}
    assert strat._placed is False


@pytest.mark.asyncio
async def test_a_market_that_moved_through_a_leg_says_why() -> None:
    """The known cost of re-placing: legality is judged against the market as
    it is now, and a restart can land in one that moved through a leg. Posting
    it would be refused by the venue anyway — but the reason has to say that
    the restart is what exposed it, not the pair."""
    strat = _restorable()
    await _restore(strat)

    # The buy leg at 49000 is now at or above the ask.
    await strat.on_fetch_bestquote(_quote(bid="48000", ask="48500"))

    assert strat.session.failures == ["oco_illegal_on_restart"]
    assert strat.oms.submitted == []


@pytest.mark.asyncio
async def test_restoring_without_an_instrument_refuses_to_place() -> None:
    """Not knowing what is resting is not the same as nothing resting.

    Without the rounded leg prices there is no way to match an order back to
    a leg, so placing again could put a second pair beside one still live.
    """
    strat = _restorable()
    strat._info = None
    strat._venue = strat._symbol = None

    await _restore(strat)

    assert strat.session.failures == ["oco_restore_no_instrument"]
    assert strat.oms.submitted == []


def test_a_market_named_in_paras_needs_no_feed() -> None:
    """The whole point of the quote being a query: no md_ids anywhere."""
    strat = _strategy(ticker="Gate_Spot_ETHUSDT")
    strat._ticker = None
    strat.session.md_ids = []
    strat._resolve_market()

    assert strat._ticker == UniversalTicker.parse("Gate_Spot_ETHUSDT")


def test_a_named_market_is_normalized_at_deploy_time() -> None:
    """A typo must refuse the deployment, not the first quote request."""
    paras = OneCancelOther.on_initialized(_paras(ticker=" gate_spot_eth/usdt "))
    assert paras["ticker"] == "Gate_Spot_ETHUSDT"

    with pytest.raises(Exception, match="unknown venue"):
        OneCancelOther.on_initialized(_paras(ticker="Kraken_Spot_ETHUSDT"))
