"""OneCancelOther — what it refuses to place, and how the pair resolves.

The strategy is driven directly: recon through ``on_recon_done``, quotes
through ``on_best_quote``, and venue answers through ``on_order_update`` /
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
from mft.protocol import CancelReject, OrderReject, ReconDone, RejectCode, SymbolInfo
from mft_sts.impl.oco import OneCancelOther

BTCUSDT = SymbolInfo(
    venue="paper",
    symbol="BTCUSDT",
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
        self.md_ids = ["paper.bestquote.BTCUSDT"]
        self.exits: list[str] = []
        #: Reasons the strategy ended as ``failed`` rather than ``done``.
        self.failures: list[str] = []

    def request_exit(self, reason: str, *, failed: bool = False) -> None:
        self.exits.append(reason)
        if failed:
            self.failures.append(reason)


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


def _paras(**overrides) -> dict:
    payload = {
        "orders": [
            {"side": "buy", "price": 49000, "qty": Decimal("0.001")},
            {"side": "sell", "price": 51000, "qty": Decimal("0.001")},
        ]
    }
    payload.update(overrides)
    return payload


def _strategy(*, accepts: list[bool] | None = None, **overrides) -> OneCancelOther:
    """A bound strategy with TD, the clock and the symbol plane stubbed out."""
    strat = OneCancelOther()
    strat.paras = OneCancelOther.on_initialized(_paras(**overrides))
    strat.session = FakeSession()  # type: ignore[assignment]
    strat.oms = FakeOms(accepts=accepts)  # type: ignore[assignment]
    strat.timer = FakeTimer()  # type: ignore[assignment]
    strat.ledger = FakeLedger()  # type: ignore[assignment]
    strat._info = BTCUSDT
    strat._venue, strat._symbol = "paper", "BTCUSDT"

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


def _quote(bid: str = "50000", ask: str = "50010") -> BestQuote:
    return BestQuote(
        symbol="BTCUSDT",
        bid=Decimal(bid),
        bid_qty=Decimal("1"),
        ask=Decimal(ask),
        ask_qty=Decimal("1"),
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
    """The ledger is not real until recon, and the funds check reads it."""
    strat = _strategy()
    await strat.on_start()
    await strat.on_best_quote(_quote())

    assert strat.oms.submitted == []


async def test_recon_alone_places_nothing() -> None:
    strat = _strategy()
    await strat.on_start()
    await strat.on_recon_done(ReconDone(session_id="s", api_id=API_ID))

    assert strat.oms.submitted == []


async def test_whichever_arrives_second_places_the_pair() -> None:
    quote_first = _strategy()
    await quote_first.on_start()
    await quote_first.on_best_quote(_quote())
    await quote_first.on_recon_done(ReconDone(session_id="s", api_id=API_ID))

    recon_first = await _armed()
    await recon_first.on_best_quote(_quote())

    assert len(quote_first.oms.submitted) == 2
    assert len(recon_first.oms.submitted) == 2


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
    await strat.on_best_quote(_quote())

    assert strat.oms.submitted == []


# --- the one quote ---------------------------------------------------------


async def test_a_legal_pair_places_both_legs_post_only() -> None:
    strat = await _armed()
    await strat.on_best_quote(_quote(bid="50000", ask="50010"))

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
    await strat.on_best_quote(_quote(bid="50000", ask="50010"))
    placed = len(strat.oms.submitted)

    # The market runs through both legs. The pair is already resting.
    await strat.on_best_quote(_quote(bid="48000", ask="48010"))
    await strat.on_best_quote(_quote(bid="52000", ask="52010"))

    assert len(strat.oms.submitted) == placed
    assert strat.session.exits == []
    assert strat.oms.cancelled == []


async def test_an_empty_book_is_not_taken_as_the_reference() -> None:
    """A one-sided quote says nothing about whether a leg can rest."""
    strat = await _armed()
    await strat.on_best_quote(
        BestQuote(
            symbol="BTCUSDT",
            bid=Decimal("0"),
            bid_qty=Decimal("0"),
            ask=Decimal("50010"),
            ask_qty=Decimal("1"),
        )
    )
    assert strat.oms.submitted == []

    await strat.on_best_quote(_quote())
    assert len(strat.oms.submitted) == 2


# --- legality --------------------------------------------------------------


async def test_a_buy_at_or_above_the_ask_is_illegal() -> None:
    strat = await _armed(
        orders=[
            {"side": "buy", "price": 50010, "qty": Decimal("0.001")},
            {"side": "sell", "price": 51000, "qty": Decimal("0.001")},
        ]
    )
    await strat.on_best_quote(_quote(bid="50000", ask="50010"))

    assert strat.oms.submitted == []
    assert strat.session.exits == ["oco_illegal"]


async def test_a_sell_at_or_below_the_bid_is_illegal() -> None:
    strat = await _armed(
        orders=[
            {"side": "buy", "price": 49000, "qty": Decimal("0.001")},
            {"side": "sell", "price": 50000, "qty": Decimal("0.001")},
        ]
    )
    await strat.on_best_quote(_quote(bid="50000", ask="50010"))

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
    await strat.on_best_quote(_quote(bid="50000", ask="50010"))

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
    await strat.on_best_quote(_quote(bid="50000", ask="50010"))

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
    await strat.on_best_quote(_quote())

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
    await strat.on_best_quote(_quote())

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
    await strat.on_best_quote(_quote())

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
    await strat.on_best_quote(_quote())

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
    await strat.on_best_quote(_quote())

    assert strat.oms.submitted == []
    assert strat.session.exits == ["oco_insufficient_balance"]


# --- how it resolves -------------------------------------------------------


async def _placed(**kwargs) -> OneCancelOther:
    strat = await _armed(**kwargs)
    await strat.on_best_quote(_quote())
    assert len(strat.oms.submitted) == 2
    return strat


async def test_a_complete_fill_cancels_the_other_and_exits() -> None:
    strat = await _placed()
    await strat.on_order_update(
        API_ID, _update("cid-1", OrderStatus.FILLED, filled="0.001")
    )

    assert strat.oms.cancelled == ["cid-2"]
    assert strat.session.exits == ["oco_filled"]
    # A pair that resolved the way an OCO should is a natural end.
    assert strat.session.failures == []


async def test_the_second_leg_can_be_the_winner_too() -> None:
    strat = await _placed()
    await strat.on_order_update(
        API_ID,
        _update("cid-2", OrderStatus.FILLED, filled="0.001", side=Side.SELL),
    )

    assert strat.oms.cancelled == ["cid-1"]
    assert strat.session.exits == ["oco_filled"]


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
    assert strat.session.exits == ["oco_filled"]


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
    await strat.on_best_quote(_quote())

    assert len(strat.oms.submitted) == 2
    assert strat.oms.cancelled == ["cid-1"]
    assert strat.session.exits == ["oco_refused"]


async def test_a_first_leg_td_refuses_places_no_second() -> None:
    strat = await _armed(accepts=[False])
    await strat.on_best_quote(_quote())

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

    assert strat.session.exits == ["oco_filled"]


# --- pause and stop --------------------------------------------------------


async def test_a_paused_strategy_places_nothing_until_it_resumes() -> None:
    strat = await _armed()
    await strat.on_pause()
    await strat.on_best_quote(_quote())
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
