"""ChaseOrder — where it posts, when it reprices, and how it ends.

The strategy is driven directly: quotes go in through ``on_best_quote``, ticks
through ``_on_tick``, and venue answers through ``on_order_update`` /
``on_order_reject``, with a fake OMS standing in for TD. What is under test is
the price it chooses, the conditions that make it reprice, and — as much as
anything — that every ending settles the size the way the config asked.
"""

from __future__ import annotations

import asyncio
from decimal import Decimal

import pytest
from mft.exchange.models import (
    BestQuote,
    Order,
    OrderStatus,
    OrderType,
    Side,
    TimeInForce,
)
from mft.exchange.oms import OmsView
from mft.protocol import (
    CancelReject,
    OrderReject,
    ReconDone,
    RejectCode,
    SymbolInfo,
)
from mft_sts.impl.chase import IOC_MAX_SLICES, ChaseOrder

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


class FakeOms:
    """Stands in for TD, and answers IOC slices the way a venue would.

    The sweep waits for each slice to reach a terminal state before sizing the
    next one, so something has to play the venue or every test pays the full
    timeout. ``ioc_fill`` is how much of each slice the book can absorb;
    ``None`` means all of it.
    """

    def __init__(self, *, accept: bool = True, accept_cancel: bool = True) -> None:
        self.submitted: list[dict] = []
        self.reject_reason = ""
        self.reject_code: int | str = RejectCode.NONE
        self.cancelled: list[str] = []
        self.accept = accept
        self.accept_cancel = accept_cancel
        self.strategy: ChaseOrder | None = None
        self.ioc_fill: Decimal | None = None
        #: Confirm cancels the way a venue does. Turned off in the one test
        #: that inspects the window before the confirmation arrives.
        self.answer_cancels = True
        self._n = 0
        self._last_cid: str | None = None

    @property
    def last_client_order_id(self) -> str | None:
        return self._last_cid

    @property
    def last_reject_reason(self) -> str:
        return "" if self.accept else self.reject_reason

    @property
    def last_reject_code(self) -> int | str:
        return RejectCode.NONE if self.accept else self.reject_code

    @property
    def ioc_slices(self) -> list[dict]:
        return [o for o in self.submitted if o["tif"] is TimeInForce.IOC]

    async def submit_order(
        self, api_id, *, symbol, side, qty, type, price=None, tif=None
    ):
        self._n += 1
        self._last_cid = f"cid-{self._n}"
        self.submitted.append(
            {
                "cid": self._last_cid,
                "symbol": symbol,
                "side": side,
                "qty": qty,
                "type": type,
                "price": price,
                "tif": tif,
            }
        )
        if tif is TimeInForce.IOC and self.strategy is not None and self.accept:
            asyncio.create_task(self._answer_ioc(self._last_cid, qty))
        return self.accept

    async def _answer_ioc(self, cid: str, qty: Decimal) -> None:
        # Yield first: the strategy records the cid right after submit returns.
        await asyncio.sleep(0)
        filled = qty if self.ioc_fill is None else min(qty, self.ioc_fill)
        status = OrderStatus.FILLED if filled >= qty else OrderStatus.CANCELED
        assert self.strategy is not None
        await self.strategy.on_order_update(7, _update(cid, str(filled), status))

    async def cancel_order(self, api_id, client_order_id):
        self.cancelled.append(str(client_order_id))
        if self.answer_cancels and self.strategy is not None and self.accept_cancel:
            asyncio.create_task(self._answer_cancel(str(client_order_id)))
        return self.accept_cancel

    async def _answer_cancel(self, cid: str) -> None:
        await asyncio.sleep(0)
        filled = self._filled_of(cid)
        assert self.strategy is not None
        await self.strategy.on_order_update(
            7, _update(cid, str(filled), OrderStatus.CANCELED)
        )

    def _filled_of(self, cid: str) -> Decimal:
        """Whatever the strategy already booked for this order, unchanged."""
        assert self.strategy is not None
        return self.strategy._filled.get(cid, Decimal("0"))


class _FakeToken:
    """A timer token the tests can re-register without a running loop."""

    def __init__(self) -> None:
        self.registered: list[tuple] = []

    def register(self, first_ms, interval_ms, func):
        self.registered.append((first_ms, interval_ms, func))
        return self

    def cancel(self) -> None:
        self.registered.clear()


class FakeLedger:
    """TD's ledger. Enough of everything unless a test says otherwise."""

    def __init__(self) -> None:
        self.balances: dict[str, Decimal] = {}
        self.default = Decimal("1000000")

    async def available(self, asset: str, api_id=None) -> Decimal:
        return self.balances.get(asset, self.default)


class FakeSession:
    def __init__(self) -> None:
        self.td_api_ids = [7]
        self.md_ids = ["paper.bestquote.BTCUSDT"]
        self.exits: list[str] = []

    def request_exit(self, reason: str) -> None:
        self.exits.append(reason)


class FakeTimer:
    """A clock the test moves by hand, so expiry is not a real wait."""

    def __init__(self) -> None:
        self._now = 1_000_000

    def now_ms(self) -> int:
        return self._now

    def advance_s(self, seconds: float) -> None:
        self._now += int(seconds * 1000)

    def token(self):  # pragma: no cover - the strategy is ticked directly
        raise AssertionError("tests drive _on_tick themselves")


def _strategy(**paras) -> ChaseOrder:
    """A bound strategy with TD, the clock, and the symbol plane stubbed out."""
    payload = {
        "side": "buy",
        # 0.1 at ~50000 clears the fixture's 5 min_notional.
        "qty": Decimal("0.1"),
        "gap_bps": 10,
        "expiry_s": 30,
        "extreme_bps": 50,
        "refresh_interval_ms": 1000,
    }
    payload.update(paras)

    strat = ChaseOrder()
    strat.paras = ChaseOrder.on_initialized(payload)
    strat.session = FakeSession()  # type: ignore[assignment]
    strat.oms = FakeOms()  # type: ignore[assignment]
    strat.oms.strategy = strat
    strat.timer = FakeTimer()  # type: ignore[assignment]
    strat.ledger = FakeLedger()  # type: ignore[assignment]
    # Tests drive _on_tick directly, so stand in for the recon that arms it.
    strat._armed = True
    strat._info = BTCUSDT
    strat._venue, strat._symbol = "paper", "BTCUSDT"
    strat._started_ms = strat.timer.now_ms()

    async def _noop_log(message: str, *, level: str = "info", **extra) -> None:
        return None

    strat.log = _noop_log  # type: ignore[method-assign]
    # Every cid the fake OMS mints belongs to this session.
    strat.owns = lambda cid: True  # type: ignore[method-assign]
    return strat


def _quote(bid: str, ask: str) -> BestQuote:
    return BestQuote(
        symbol="BTCUSDT",
        bid=Decimal(bid),
        bid_qty=Decimal("1"),
        ask=Decimal(ask),
        ask_qty=Decimal("1"),
    )


def _update(cid: str, filled: str, status: OrderStatus) -> Order:
    return Order(
        client_order_id=cid,
        symbol="BTCUSDT",
        side=Side.BUY,
        type=OrderType.LIMIT,
        status=status,
        qty=Decimal("0.1"),
        filled_qty=Decimal(filled),
        price=Decimal("50000"),
    )


# --- parameters ------------------------------------------------------------


def test_side_must_be_buy_or_sell() -> None:
    with pytest.raises(ValueError, match="side must be"):
        ChaseOrder.on_initialized(
            {"side": "long", "qty": 1, "gap_bps": 10, "expiry_s": 30,
             "extreme_bps": 50}
        )


def test_one_of_qty_or_qty_quote_is_required() -> None:
    with pytest.raises(ValueError, match="one of qty or qty_quote"):
        ChaseOrder.on_initialized(
            {"side": "buy", "gap_bps": 10, "expiry_s": 30, "extreme_bps": 50}
        )


def test_qty_and_qty_quote_together_are_refused() -> None:
    with pytest.raises(ValueError, match="not both"):
        ChaseOrder.on_initialized(
            {"side": "buy", "qty": 1, "qty_quote": 100, "gap_bps": 10,
             "expiry_s": 30, "extreme_bps": 50}
        )


@pytest.mark.parametrize("bad", [0, -1])
def test_gap_bps_must_be_positive(bad: int) -> None:
    """At zero it posts at the touch, the one price post-only always refuses."""
    with pytest.raises(ValueError, match="gap_bps must be positive"):
        ChaseOrder.on_initialized(
            {"side": "buy", "qty": 1, "gap_bps": bad, "expiry_s": 30,
             "extreme_bps": 50}
        )


@pytest.mark.parametrize("field", ["expiry_s", "extreme_bps"])
def test_the_guards_are_required_and_positive(field: str) -> None:
    payload = {"side": "buy", "qty": 1, "gap_bps": 10, "expiry_s": 30,
               "extreme_bps": 50}
    del payload[field]
    with pytest.raises(ValueError):
        ChaseOrder.on_initialized(payload)


def test_must_exec_defaults_to_false() -> None:
    """Spending money at market has to be asked for, not inherited."""
    out = ChaseOrder.on_initialized(
        {"side": "buy", "qty": 1, "gap_bps": 10, "expiry_s": 30,
         "extreme_bps": 50}
    )
    assert out["must_exec"] is False


# --- where it posts --------------------------------------------------------


@pytest.mark.asyncio
async def test_a_buy_posts_below_the_ask() -> None:
    strat = _strategy(side="buy", gap_bps=10)
    await strat.on_best_quote(_quote("49999", "50000"))
    await strat._on_tick()

    order = strat.oms.submitted[0]
    # 50000 * (1 - 0.001)
    assert order["price"] == Decimal("49950")
    assert order["type"] is OrderType.LIMIT
    assert order["tif"] is TimeInForce.POST_ONLY


@pytest.mark.asyncio
async def test_a_sell_posts_above_the_bid() -> None:
    strat = _strategy(side="sell", gap_bps=10)
    await strat.on_best_quote(_quote("50000", "50001"))
    await strat._on_tick()

    order = strat.oms.submitted[0]
    # 50000 * (1 + 0.001)
    assert order["price"] == Decimal("50050")
    assert order["tif"] is TimeInForce.POST_ONLY


@pytest.mark.asyncio
async def test_each_side_reads_the_book_it_would_have_to_cross() -> None:
    """A buy prices off the ask, a sell off the bid — never its own side."""
    buy = _strategy(side="buy")
    sell = _strategy(side="sell")
    quote = _quote("100", "200")
    await buy.on_best_quote(quote)
    await sell.on_best_quote(quote)
    assert buy._ref == Decimal("200")
    assert sell._ref == Decimal("100")


@pytest.mark.asyncio
async def test_it_does_not_place_before_the_first_quote() -> None:
    strat = _strategy()
    await strat._on_tick()
    assert strat.oms.submitted == []
    assert strat.session.exits == []


# --- repricing -------------------------------------------------------------


@pytest.mark.asyncio
async def test_a_small_move_leaves_the_order_alone() -> None:
    strat = _strategy(gap_bps=10)
    await strat.on_best_quote(_quote("49999", "50000"))
    await strat._on_tick()
    await strat.on_best_quote(_quote("50000", "50001"))
    await strat._on_tick()

    assert len(strat.oms.submitted) == 1
    assert strat.oms.cancelled == []


@pytest.mark.asyncio
async def test_a_move_past_the_gap_cancels_the_resting_order() -> None:
    strat = _strategy(gap_bps=10)
    await strat.on_best_quote(_quote("49999", "50000"))
    await strat._on_tick()
    cid = strat.oms.submitted[0]["cid"]

    await strat.on_best_quote(_quote("50099", "50100"))
    await strat._on_tick()
    assert strat.oms.cancelled == [cid]


@pytest.mark.asyncio
async def test_the_replacement_waits_for_the_cancel_to_land() -> None:
    """Two live orders for one remaining size can both fill. Never overlap."""
    strat = _strategy(gap_bps=10)
    strat.oms.answer_cancels = False
    await strat.on_best_quote(_quote("49999", "50000"))
    await strat._on_tick()
    await strat.on_best_quote(_quote("50099", "50100"))
    await strat._on_tick()  # cancels
    await strat._on_tick()  # must not place while unconfirmed
    assert len(strat.oms.submitted) == 1

    cid = strat.oms.submitted[0]["cid"]
    await strat.on_order_update(7, _update(cid, "0", OrderStatus.CANCELED))
    await strat._on_tick()
    assert len(strat.oms.submitted) == 2
    assert strat.oms.submitted[1]["price"] == Decimal("50049.9")


@pytest.mark.asyncio
async def test_a_post_only_refusal_just_reprices() -> None:
    """Being refused is the normal cost of never crossing, not an error."""
    strat = _strategy()
    await strat.on_best_quote(_quote("49999", "50000"))
    await strat._on_tick()
    cid = strat.oms.submitted[0]["cid"]

    await strat.on_order_reject(
        7, OrderReject(api_id=7, client_order_id=cid, reason="would cross")
    )
    assert strat._open_cid is None

    await strat.on_best_quote(_quote("50000", "50001"))
    await strat._on_tick()
    assert len(strat.oms.submitted) == 2
    assert strat.session.exits == []


@pytest.mark.asyncio
async def test_a_cancel_reject_frees_the_slot() -> None:
    """The order was already gone; waiting on it forever would stall the chase."""
    strat = _strategy()
    await strat.on_best_quote(_quote("49999", "50000"))
    await strat._on_tick()
    cid = strat.oms.submitted[0]["cid"]
    await strat.on_cancel_reject(
        7, CancelReject(api_id=7, client_order_id=cid, reason="not found")
    )
    assert strat._open_cid is None
    assert strat._canceling is False


# --- endings ---------------------------------------------------------------


@pytest.mark.asyncio
async def test_a_full_fill_ends_the_session() -> None:
    strat = _strategy(qty=Decimal("0.1"))
    await strat.on_best_quote(_quote("49999", "50000"))
    await strat._on_tick()
    cid = strat.oms.submitted[0]["cid"]

    await strat.on_order_update(7, _update(cid, "0.1", OrderStatus.FILLED))
    assert strat.session.exits == ["chase_filled"]


@pytest.mark.asyncio
async def test_a_partial_fill_keeps_chasing_the_remainder() -> None:
    strat = _strategy(qty=Decimal("0.1"), gap_bps=10)
    await strat.on_best_quote(_quote("49999", "50000"))
    await strat._on_tick()
    cid = strat.oms.submitted[0]["cid"]

    await strat.on_order_update(
        7, _update(cid, "0.04", OrderStatus.CANCELED)
    )
    assert strat.session.exits == []

    await strat.on_best_quote(_quote("50099", "50100"))
    await strat._on_tick()
    # Only what is left, not the original size.
    assert strat.oms.submitted[1]["qty"] == Decimal("0.06")


@pytest.mark.asyncio
async def test_expiry_ends_the_session() -> None:
    strat = _strategy(expiry_s=30, must_exec=False)
    await strat.on_best_quote(_quote("49999", "50000"))
    await strat._on_tick()

    strat.timer.advance_s(31)
    await strat._on_tick()
    assert strat.session.exits == ["chase_expired"]
    # must_exec is false, so the remainder is left undone.
    assert all(o["type"] is OrderType.LIMIT for o in strat.oms.submitted)


@pytest.mark.asyncio
async def test_expiry_with_must_exec_sweeps_the_rest_with_ioc() -> None:
    strat = _strategy(qty=Decimal("0.1"), expiry_s=30, must_exec=True)
    await strat.on_best_quote(_quote("49999", "50000"))
    await strat._on_tick()
    cid = strat.oms.submitted[0]["cid"]
    await strat.on_order_update(
        7, _update(cid, "0.04", OrderStatus.CANCELED)
    )

    strat.timer.advance_s(31)
    await strat._on_tick()

    slices = strat.oms.ioc_slices
    assert len(slices) == 1
    # The remainder of the partial fill, not the whole size.
    assert slices[0]["qty"] == Decimal("0.06")
    # A limit priced at the far touch, which crosses — never a market order.
    assert slices[0]["type"] is OrderType.LIMIT
    assert slices[0]["price"] == Decimal("50000")
    assert not any(
        o["type"] is OrderType.MARKET for o in strat.oms.submitted
    )
    assert strat.session.exits == ["chase_expired"]


@pytest.mark.asyncio
async def test_the_sweep_takes_one_level_at_a_time() -> None:
    """Slice by slice off the touch, rather than one walk down the book."""
    strat = _strategy(qty=Decimal("0.1"), expiry_s=30, must_exec=True)
    strat.oms.ioc_fill = Decimal("0.03")
    await strat.on_best_quote(_quote("49999", "50000"))
    await strat._on_tick()

    strat.timer.advance_s(31)
    await strat._on_tick()

    slices = strat.oms.ioc_slices
    # 0.1 taken 0.03 at a time: 0.03, 0.03, 0.03, then the 0.01 tail.
    assert [s["qty"] for s in slices] == [
        Decimal("0.1"),
        Decimal("0.07"),
        Decimal("0.04"),
        Decimal("0.01"),
    ]
    assert strat._remaining() == Decimal("0")


@pytest.mark.asyncio
async def test_the_sweep_reprices_off_a_fresher_quote_each_slice() -> None:
    """Quotes keep arriving while the sweep pauses; each slice reads the last."""
    strat = _strategy(qty=Decimal("0.1"), expiry_s=30, must_exec=True)
    strat.oms.ioc_fill = Decimal("0.05")
    await strat.on_best_quote(_quote("49999", "50000"))
    await strat._on_tick()

    async def _move_the_book() -> None:
        await asyncio.sleep(0.2)
        await strat.on_best_quote(_quote("50009", "50010"))

    strat.timer.advance_s(31)
    mover = asyncio.create_task(_move_the_book())
    await strat._on_tick()
    await mover

    slices = strat.oms.ioc_slices
    assert slices[0]["price"] == Decimal("50000")
    assert slices[-1]["price"] == Decimal("50010")


@pytest.mark.asyncio
async def test_the_sweep_gives_up_rather_than_looping_forever() -> None:
    """must_exec is a promise the venue can still refuse to let us keep."""
    strat = _strategy(qty=Decimal("0.1"), expiry_s=30, must_exec=True)
    strat.oms.ioc_fill = Decimal("0")
    await strat.on_best_quote(_quote("49999", "50000"))
    await strat._on_tick()

    strat.timer.advance_s(31)
    await strat._on_tick()

    assert len(strat.oms.ioc_slices) == IOC_MAX_SLICES
    assert strat._remaining() == Decimal("0.1")
    assert strat.session.exits == ["chase_expired"]


@pytest.mark.asyncio
async def test_slippage_past_extreme_bps_ends_the_session() -> None:
    strat = _strategy(side="buy", extreme_bps=50, must_exec=False)
    await strat.on_best_quote(_quote("49999", "50000"))
    await strat._on_tick()

    # Ask up 60bps from where the chase armed.
    await strat.on_best_quote(_quote("50299", "50300"))
    await strat._on_tick()
    assert strat.session.exits == ["chase_slipped"]


@pytest.mark.asyncio
async def test_slippage_is_measured_in_the_direction_that_costs() -> None:
    """A buy is hurt by the ask rising; the ask falling is not slippage."""
    strat = _strategy(side="buy", extreme_bps=50)
    await strat.on_best_quote(_quote("49999", "50000"))
    await strat._on_tick()

    await strat.on_best_quote(_quote("49699", "49700"))
    await strat._on_tick()
    assert strat.session.exits == []


@pytest.mark.asyncio
async def test_a_sell_slips_when_the_bid_falls() -> None:
    strat = _strategy(side="sell", extreme_bps=50)
    await strat.on_best_quote(_quote("50000", "50001"))
    await strat._on_tick()

    await strat.on_best_quote(_quote("49700", "49701"))
    await strat._on_tick()
    assert strat.session.exits == ["chase_slipped"]


@pytest.mark.asyncio
async def test_a_complete_fill_at_expiry_sends_no_market_order() -> None:
    strat = _strategy(qty=Decimal("0.1"), expiry_s=30, must_exec=True)
    await strat.on_best_quote(_quote("49999", "50000"))
    await strat._on_tick()
    cid = strat.oms.submitted[0]["cid"]
    await strat.on_order_update(7, _update(cid, "0.1", OrderStatus.FILLED))

    before = len(strat.oms.submitted)
    strat.timer.advance_s(31)
    await strat._on_tick()
    assert len(strat.oms.submitted) == before


@pytest.mark.asyncio
async def test_it_ends_only_once() -> None:
    strat = _strategy(expiry_s=30, must_exec=True)
    await strat.on_best_quote(_quote("49999", "50000"))
    await strat._on_tick()

    strat.timer.advance_s(31)
    await strat._on_tick()
    await strat._on_tick()
    assert strat.session.exits == ["chase_expired"]
    # The sweep ran once; the second tick found the ending already done.
    assert len(strat.oms.ioc_slices) == 1


@pytest.mark.asyncio
async def test_no_td_account_exits() -> None:
    strat = _strategy()
    strat.session.td_api_ids = []
    await strat._on_tick()
    assert strat.session.exits == ["chase_no_td"]


@pytest.mark.asyncio
async def test_qty_quote_is_converted_once_at_the_first_target_price() -> None:
    strat = _strategy(qty=None, qty_quote=Decimal("1000"))
    await strat.on_best_quote(_quote("49999", "50000"))
    await strat._on_tick()
    # 1000 / 49950, rounded to the 0.00001 step.
    assert strat._target_qty == Decimal("0.02002")

    # The target does not drift when the book does.
    await strat.on_best_quote(_quote("50099", "50100"))
    await strat._on_tick()
    assert strat._target_qty == Decimal("0.02002")


# --- refusals --------------------------------------------------------------


@pytest.mark.asyncio
async def test_a_balance_refusal_ends_the_chase_at_once() -> None:
    """TD refuses the pre-lock; the next tick would be refused identically."""
    strat = _strategy()
    strat.oms.accept = False
    strat.oms.reject_code = RejectCode.TD_INSUFFICIENT_BALANCE
    strat.oms.reject_reason = "insufficient balance: need 0.000469 BTC, free=0"
    await strat.on_best_quote(_quote("63901", "63902"))
    await strat._on_tick()

    assert strat.session.exits == ["chase_insufficient_balance"]
    assert len(strat.oms.submitted) == 1

    # And it really stops: further ticks place nothing more.
    await strat._on_tick()
    await strat._on_tick()
    assert len(strat.oms.submitted) == 1


@pytest.mark.asyncio
async def test_the_balance_refusal_is_recognised_by_code_not_wording() -> None:
    """The code decides, so TD can reword the reason without breaking this."""
    strat = _strategy()
    strat.oms.accept = False
    strat.oms.reject_code = RejectCode.TD_INSUFFICIENT_BALANCE
    strat.oms.reject_reason = "pre-lock short by 0.000469 BTC"
    await strat.on_best_quote(_quote("63901", "63902"))
    await strat._on_tick()

    assert strat.session.exits == ["chase_insufficient_balance"]


@pytest.mark.asyncio
async def test_an_uncoded_balance_refusal_still_reads_as_one() -> None:
    """No code at all — an ack we could not read — falls back to the wording."""
    strat = _strategy()
    strat.oms.accept = False
    strat.oms.reject_code = RejectCode.NONE
    strat.oms.reject_reason = "insufficient balance: need 0.000469 BTC, free=0"
    await strat.on_best_quote(_quote("63901", "63902"))
    await strat._on_tick()

    assert strat.session.exits == ["chase_insufficient_balance"]


@pytest.mark.asyncio
async def test_any_other_td_refusal_also_stops() -> None:
    strat = _strategy()
    strat.oms.accept = False
    strat.oms.reject_code = RejectCode.TD_SESSION_NOT_ATTACHED
    strat.oms.reject_reason = "session not attached to api_id=3"
    await strat.on_best_quote(_quote("63901", "63902"))
    await strat._on_tick()

    assert strat.session.exits == ["chase_refused"]


@pytest.mark.asyncio
async def test_the_sweep_stops_on_a_refusal_instead_of_burning_its_budget() -> None:
    strat = _strategy(qty=Decimal("0.1"), expiry_s=30, must_exec=True)
    await strat.on_best_quote(_quote("49999", "50000"))
    await strat._on_tick()
    assert len(strat.oms.submitted) == 1

    # The account runs dry before the sweep starts.
    strat.oms.accept = False
    strat.oms.reject_code = RejectCode.TD_INSUFFICIENT_BALANCE
    strat.oms.reject_reason = "insufficient balance: need 0.1 BTC, free=0"
    strat.timer.advance_s(31)
    await strat._on_tick()

    assert len(strat.oms.ioc_slices) == 1
    assert strat.session.exits == ["chase_expired"]


# --- the ledger check ------------------------------------------------------


@pytest.mark.asyncio
async def test_a_buy_that_the_quote_balance_cannot_fund_exits() -> None:
    """Asked of the ledger, not discovered one refusal per tick."""
    strat = _strategy(side="buy", qty=Decimal("0.1"))
    strat.ledger.balances["USDT"] = Decimal("100")  # 0.1 BTC needs ~5000
    await strat.on_best_quote(_quote("49999", "50000"))
    await strat._on_tick()

    assert strat.oms.submitted == []
    assert strat.session.exits == ["chase_insufficient_balance"]


@pytest.mark.asyncio
async def test_a_sell_is_measured_in_base_not_quote() -> None:
    strat = _strategy(side="sell", qty=Decimal("0.1"))
    strat.ledger.balances["BTC"] = Decimal("0.05")
    strat.ledger.balances["USDT"] = Decimal("0")
    await strat.on_best_quote(_quote("50000", "50001"))
    await strat._on_tick()

    assert strat.oms.submitted == []
    assert strat.session.exits == ["chase_insufficient_balance"]


@pytest.mark.asyncio
async def test_exactly_enough_is_enough() -> None:
    """The boundary funds the order — TD reserves qty * price, no fee on top."""
    strat = _strategy(side="buy", qty=Decimal("0.1"))
    # The order posts at 49950 (10bps under the ask), so that is the commitment.
    strat.ledger.balances["USDT"] = Decimal("0.1") * Decimal("49950")
    await strat.on_best_quote(_quote("49999", "50000"))
    await strat._on_tick()

    assert len(strat.oms.submitted) == 1
    assert strat.session.exits == []


@pytest.mark.asyncio
async def test_the_sweep_checks_the_ledger_too() -> None:
    strat = _strategy(qty=Decimal("0.1"), expiry_s=30, must_exec=True)
    await strat.on_best_quote(_quote("49999", "50000"))
    await strat._on_tick()

    strat.ledger.balances["USDT"] = Decimal("1")
    strat.timer.advance_s(31)
    await strat._on_tick()

    assert strat.oms.ioc_slices == []
    assert strat.session.exits == ["chase_expired"]


# --- arming on recon -------------------------------------------------------


@pytest.mark.asyncio
async def test_nothing_is_placed_before_recon() -> None:
    """The balance check reads TD's ledger, which is not real until recon."""
    strat = _strategy()
    strat._armed = False
    await strat.on_best_quote(_quote("49999", "50000"))
    await strat._on_tick()
    assert strat.oms.submitted == []


@pytest.mark.asyncio
async def test_recon_arms_the_chase_and_starts_the_clock() -> None:
    strat = _strategy(expiry_s=30)
    strat._armed = False
    strat._started_ms = None
    strat._tick_token = _FakeToken()
    strat.timer.advance_s(120)  # a slow recon must not eat the budget

    await strat.on_recon_done(ReconDone(session_id="s1", api_id=7, oms=OmsView()))

    assert strat._armed is True
    assert strat._started_ms == strat.timer.now_ms()
    assert not strat._expired()


@pytest.mark.asyncio
async def test_a_second_recon_does_not_hand_out_a_fresh_budget() -> None:
    """Recon runs again after a venue reconnect; the chase is already going."""
    strat = _strategy(expiry_s=30)
    strat._tick_token = _FakeToken()
    started = strat._started_ms

    strat.timer.advance_s(20)
    await strat.on_recon_done(ReconDone(session_id="s1", api_id=7, oms=OmsView()))
    assert strat._started_ms == started

    strat.timer.advance_s(11)
    assert strat._expired()


@pytest.mark.asyncio
async def test_a_recon_that_never_comes_ends_the_session() -> None:
    """Otherwise it waits on an event that is not coming, forever."""
    strat = _strategy()
    strat._armed = False
    await strat._on_recon_timeout()
    assert strat.session.exits == ["chase_no_recon"]


@pytest.mark.asyncio
async def test_the_recon_deadline_is_a_no_op_once_armed() -> None:
    strat = _strategy()
    await strat._on_recon_timeout()
    assert strat.session.exits == []
