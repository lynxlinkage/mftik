"""MD controller: the types that are real now, and the decisions B8 owes.

IF-09 defines this layer and returns null data. The tests split in two:

* What is real now — a generation orders lexicographically, a placement
  cannot describe an atom on two connections, an owner set can hold a
  session and a standing subscription together, and every function that
  would decide something refuses with the ticket number.
* What B8 will make true — ``xfail(strict=True)``, one per case the
  ticket's acceptance names plus the owner and expiry rules the module
  states. ``strict`` is the point: the implementation cannot land
  without deleting the marker.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest
from mftik.exchange.atoms import Atom, Capacity, UnknownEndpointError
from mftik.exchange.tickers import UniversalTicker
from mftik.protocol import IntentOwner
from mftik_md.conn import ConnError, ConnId, Desired, Generation
from mftik_md.controller import (
    ConnAssignment,
    ConnDesired,
    ConnView,
    ControllerError,
    Demand,
    ExpiryNotice,
    FeedBinding,
    MdOrchestrator,
    Placement,
    StandingOwner,
    desired_atoms,
    expire,
    gc_owners,
    place,
)

DERIBIT = "Deribit"
PUBLIC = "public"


def cap(max_atoms: int) -> Capacity:
    return Capacity(
        max_atoms=max_atoms,
        max_messages_per_second=100,
        subscribe_batch=10,
        subscribe_per_second=5,
    )


def atom(channel: str, endpoint: str = PUBLIC, venue: str = DERIBIT) -> Atom:
    return Atom(venue, endpoint, channel)


def owner(session_id: str, instance: str = "sts-a") -> IntentOwner:
    return IntentOwner(sts_instance=instance, session_id=session_id)


# --- types that are already true ------------------------------------------


def test_a_generation_orders_lexicographically() -> None:
    """Epoch first, then seq. A new controller beats any push of the old one."""
    assert Generation(1, 2) > Generation(1, 1)
    assert not Generation(1, 1) > Generation(1, 1)
    assert Generation(2, 0) > Generation(1, 10**9)


def test_a_generation_refuses_a_negative_or_a_bool() -> None:
    """The pair is the connection module's. Both bad parts are its error."""
    with pytest.raises(ConnError):
        Generation(-1, 0)
    with pytest.raises(ConnError):
        Generation(True, 0)  # type: ignore[arg-type]


def test_the_orchestrator_mints_its_epoch_and_does_not_count() -> None:
    """``generation`` is the pair. The sequence is the caller's (C6)."""
    orchestrator = MdOrchestrator(4)
    assert orchestrator.controller_epoch == 4
    assert orchestrator.generation(0) == Generation(4, 0)
    assert orchestrator.generation(3) == Generation(4, 3)
    with pytest.raises(ControllerError):
        MdOrchestrator(-1)
    with pytest.raises(TypeError):
        MdOrchestrator(True)  # type: ignore[arg-type]


def test_session_and_standing_owners_share_one_set() -> None:
    session = owner("s1")
    standing = StandingOwner("btc-tape")
    assert frozenset({session, standing, session}) == frozenset({session, standing})


def test_a_placement_refuses_what_cannot_be_placed() -> None:
    here = ConnId(DERIBIT, PUBLIC, 0)
    a = atom("a")
    b = atom("b")
    elsewhere = atom("c", endpoint="market")

    with pytest.raises(ControllerError):
        ConnView(here, frozenset())
    with pytest.raises(ControllerError):
        ConnAssignment(here, frozenset({elsewhere}))
    with pytest.raises(ControllerError):
        Placement(
            (
                ConnAssignment(here, frozenset({a})),
                ConnAssignment(ConnId(DERIBIT, PUBLIC, 1), frozenset({a})),
            )
        )
    with pytest.raises(ControllerError):
        Placement(
            (
                ConnAssignment(here, frozenset({a, b})),
                ConnAssignment(here, frozenset({b})),
            )
        )
    with pytest.raises(ConnError):
        ConnId(DERIBIT, PUBLIC, -1)
    with pytest.raises(ConnError):
        ConnId("Binance.UM", PUBLIC, 0)
    with pytest.raises(ConnError):
        ConnId(DERIBIT, "public/main", 0)
    with pytest.raises(ConnError):
        ConnId(DERIBIT, "public:main", 0)


def test_placement_lookup_and_an_empty_result() -> None:
    a = atom("a")
    b = atom("b")
    low = ConnId(DERIBIT, PUBLIC, 0)
    high = ConnId(DERIBIT, PUBLIC, 1)
    # Constructed high-then-low, stored in id order.
    placed = Placement(
        (
            ConnAssignment(high, frozenset({b})),
            ConnAssignment(low, frozenset({a})),
        )
    )
    assert [conn.id for conn in placed.conns] == [low, high]
    assert placed.conn_of(a) == low
    assert placed.on(high) == frozenset({b})
    assert placed.conn_of(atom("missing")) is None
    assert placed.on(ConnId(DERIBIT, PUBLIC, 9)) == frozenset()

    empty = Placement()
    assert empty.conns == ()
    assert empty.views() == ()
    assert placed.views() == (
        ConnView(low, frozenset({a})),
        ConnView(high, frozenset({b})),
    )


def test_a_pushed_set_cannot_be_empty_or_on_the_wrong_connection() -> None:
    """An empty push is not a desired set; that worker has ended (C4)."""
    with pytest.raises(ControllerError):
        ConnDesired(
            ConnId(DERIBIT, PUBLIC, 0),
            Desired(Generation(1, 0), frozenset()),
        )
    with pytest.raises(ControllerError):
        ConnDesired(
            ConnId(DERIBIT, "market", 0),
            Desired(Generation(1, 0), frozenset({atom("a")})),
        )


def test_a_binding_needs_an_aware_expiry_and_a_notice_is_expired() -> None:
    ticker = UniversalTicker.parse("Deribit_Future_BTCUSD-260925")
    with pytest.raises(ControllerError):
        FeedBinding(
            owner("s1"),
            ticker,
            "ticker",
            frozenset({atom("a")}),
            datetime(2026, 9, 25, 8, 0),
        )
    notice = ExpiryNotice(
        owner("s1"),
        ticker,
        "ticker",
        datetime(2026, 9, 25, 8, 0, tzinfo=UTC),
    )
    assert (notice.state, notice.code) == ("expired", "expired")
    with pytest.raises(ControllerError):
        ExpiryNotice(
            owner("s1"),
            ticker,
            "ticker",
            datetime(2026, 9, 25, 8, 0, tzinfo=UTC),
            state="down",
        )


def test_the_stubs_refuse_with_the_ticket_number() -> None:
    """Null data, and it says which ticket owns the gap."""
    session = owner("s1")
    demands = (Demand(session, frozenset({atom("a")})),)
    now = datetime(2026, 9, 25, 8, 0, tzinfo=UTC)
    orchestrator = MdOrchestrator(1)
    calls = (
        lambda: desired_atoms(demands),
        lambda: place(frozenset({atom("a")}), (), {}),
        lambda: expire({}, (), now),
        lambda: gc_owners(
            frozenset({session}),
            frozenset(),
            None,
            report=None,
            report_generation=None,
        ),
        lambda: orchestrator.desired(demands),
        lambda: orchestrator.place(frozenset({atom("a")}), (), {}),
        lambda: orchestrator.expire({}, (), now),
        lambda: orchestrator.gc_owners(
            frozenset({session}),
            frozenset(),
            None,
            report=None,
            report_generation=None,
        ),
    )
    for call in calls:
        with pytest.raises(NotImplementedError, match="IF-09"):
            call()


def test_expiry_refuses_a_naive_clock() -> None:
    with pytest.raises(ControllerError):
        expire({}, (), datetime(2026, 9, 25, 8, 0))


def test_a_report_with_a_bad_generation_is_refused() -> None:
    session = owner("s1")
    with pytest.raises(ControllerError):
        gc_owners(
            frozenset({session}),
            frozenset(),
            5,
            report=frozenset({session}),
            report_generation=4,
        )
    with pytest.raises(ControllerError):
        gc_owners(
            frozenset({session}),
            frozenset(),
            None,
            report=frozenset({session}),
            report_generation=None,
        )
    with pytest.raises(ControllerError):
        gc_owners(
            frozenset({session}),
            frozenset(),
            None,
            report=None,
            report_generation=1,
        )
    with pytest.raises(TypeError):
        gc_owners(
            frozenset({session}),
            frozenset(),
            None,
            report=frozenset({session}),
            report_generation=True,  # type: ignore[arg-type]
        )


# --- contracts B8 has to make true ----------------------------------------


@pytest.mark.xfail(strict=True, reason="B8-02 places atoms and does not move them")
def test_placement_does_not_move_an_atom() -> None:
    """An atom stays on the connection it joined, hole or no hole (F22, C2).

    ``c`` cannot fit on connection 0, so it opens the next ``n``. Dropping
    ``b`` leaves a hole on 0; ``c`` does not come back to fill it, and a
    later ``d`` does, because ``d`` was never placed.
    """
    a, b, c, d = atom("a"), atom("b"), atom("c"), atom("d")
    limits = {(DERIBIT, PUBLIC): cap(2)}
    conn0 = ConnId(DERIBIT, PUBLIC, 0)

    placed = place(
        frozenset({a, b, c}),
        (ConnView(conn0, frozenset({a, b})),),
        limits,
    )
    assert placed.conn_of(a) == conn0
    assert placed.conn_of(b) == conn0
    c_conn = placed.conn_of(c)
    assert c_conn == ConnId(DERIBIT, PUBLIC, 1)

    again = place(frozenset({a, c}), placed.views(), limits)
    assert again.conn_of(a) == conn0
    assert again.conn_of(b) is None
    assert again.conn_of(c) == c_conn
    assert again.on(conn0) == frozenset({a})

    filled = place(frozenset({a, c, d}), again.views(), limits)
    assert filled.conn_of(d) == conn0
    assert filled.conn_of(a) == conn0
    assert filled.conn_of(c) == c_conn


@pytest.mark.xfail(strict=True, reason="B8-02 places atoms and does not move them")
def test_a_smaller_capacity_does_not_evict() -> None:
    """The ceiling shrank. The atoms that are already there stay (C2)."""
    a, b, c = atom("a"), atom("b"), atom("c")
    conn0 = ConnId(DERIBIT, PUBLIC, 0)
    placed = place(
        frozenset({a, b, c}),
        (ConnView(conn0, frozenset({a, b})),),
        {(DERIBIT, PUBLIC): cap(1)},
    )
    assert placed.on(conn0) == frozenset({a, b})
    assert placed.conn_of(c) == ConnId(DERIBIT, PUBLIC, 1)


@pytest.mark.xfail(strict=True, reason="B8-02 ends a worker whose connection is empty")
def test_an_empty_connection_is_dropped() -> None:
    a = atom("a")
    placed = place(
        frozenset(),
        (ConnView(ConnId(DERIBIT, PUBLIC, 0), frozenset({a})),),
        {(DERIBIT, PUBLIC): cap(10)},
    )
    assert placed.conns == ()
    assert placed.conn_of(a) is None


@pytest.mark.xfail(strict=True, reason="B8-02 prefers the lowest connection with room")
def test_a_new_atom_fills_the_lowest_connection_that_has_room() -> None:
    a, c, d = atom("a"), atom("c"), atom("d")
    placed = place(
        frozenset({a, c, d}),
        (
            ConnView(ConnId(DERIBIT, PUBLIC, 1), frozenset({c})),
            ConnView(ConnId(DERIBIT, PUBLIC, 0), frozenset({a})),
        ),
        {(DERIBIT, PUBLIC): cap(2)},
    )
    assert placed.conn_of(d) == ConnId(DERIBIT, PUBLIC, 0)
    assert placed.conn_of(c) == ConnId(DERIBIT, PUBLIC, 1)
    assert placed.conn_of(a) == ConnId(DERIBIT, PUBLIC, 0)


@pytest.mark.xfail(strict=True, reason="B8-02 keeps an atom on its own endpoint")
def test_atoms_of_different_endpoints_do_not_share_a_connection() -> None:
    book = Atom("BinanceUM", "public", "btcusdt@bookTicker")
    market = Atom("BinanceUM", "market", "btcusdt@ticker")
    placed = place(
        frozenset({book, market}),
        (),
        {
            ("BinanceUM", "public"): cap(10),
            ("BinanceUM", "market"): cap(10),
        },
    )
    assert placed.conn_of(book) == ConnId("BinanceUM", "public", 0)
    assert placed.conn_of(market) == ConnId("BinanceUM", "market", 0)


@pytest.mark.xfail(strict=True, reason="B8-02 refuses to invent a capacity")
def test_a_missing_capacity_is_refused() -> None:
    with pytest.raises(UnknownEndpointError):
        place(frozenset({atom("a")}), (), {})


@pytest.mark.xfail(strict=True, reason="B8-02 refuses a capacity that can never fill")
def test_a_non_positive_capacity_is_refused() -> None:
    with pytest.raises(ControllerError):
        place(frozenset({atom("a")}), (), {(DERIBIT, PUBLIC): cap(0)})


@pytest.mark.xfail(strict=True, reason="B8-01 unions demand into an owner set")
def test_an_atoms_owners_are_everyone_who_demands_it() -> None:
    """No refcount (C1). The same session twice is one owner.

    An owner who is not in the demands is not in the set: releasing a
    terminal session is omitting it, after :func:`gc_owners` says so.
    A standing subscription with nothing resolved does not appear.
    """
    s1, s2 = owner("s1"), owner("s2")
    tape = StandingOwner("btc-tape")
    shared = atom("ticker.BTC-PERPETUAL.100ms")
    book = atom("book.BTC-PERPETUAL.none.20.100ms")

    owned = desired_atoms(
        (
            Demand(s1, frozenset({shared})),
            Demand(s1, frozenset({shared, book})),
            Demand(s2, frozenset({shared})),
            Demand(tape, frozenset({shared})),
            Demand(StandingOwner("idle"), frozenset()),
        )
    )

    assert owned[shared] == frozenset({s1, s2, tape})
    assert owned[book] == frozenset({s1})
    assert set(owned) == {shared, book}


@pytest.mark.xfail(strict=True, reason="B8-01 releases an owner absent twice, not once")
def test_one_missed_report_does_not_release_an_owner() -> None:
    session = owner("s1")
    other = owner("s2")
    result = gc_owners(
        frozenset({session, other}),
        frozenset(),
        None,
        report=frozenset({other}),
        report_generation=1,
    )
    assert result.release == frozenset()
    assert result.absent == frozenset({session})
    assert result.generation == 1


@pytest.mark.xfail(strict=True, reason="B8-01 does not count a replayed report twice")
def test_a_replayed_report_is_not_a_second_sample() -> None:
    """The generation is how two publications are told apart (§8.2)."""
    session = owner("s1")
    other = owner("s2")
    result = gc_owners(
        frozenset({session, other}),
        frozenset({session}),
        1,
        report=frozenset({other}),
        report_generation=1,
    )
    assert result.release == frozenset()
    assert result.absent == frozenset({session})
    assert result.generation == 1


@pytest.mark.xfail(strict=True, reason="B8-01 releases an owner absent twice, not once")
def test_a_second_missed_report_releases_the_owner() -> None:
    session = owner("s1")
    other = owner("s2")
    result = gc_owners(
        frozenset({session, other}),
        frozenset({session}),
        1,
        report=frozenset({other}),
        report_generation=2,
    )
    assert result.release == frozenset({session})
    assert result.absent == frozenset()
    assert result.generation == 2


@pytest.mark.xfail(strict=True, reason="B8-01 keeps an owner who is in the report")
def test_an_owner_who_reappears_is_kept() -> None:
    """A session STS included because it is restarting is simply present (R4)."""
    session = owner("s1")
    result = gc_owners(
        frozenset({session}),
        frozenset({session}),
        1,
        report=frozenset({session}),
        report_generation=2,
    )
    assert result.release == frozenset()
    assert result.absent == frozenset()
    assert result.generation == 2


@pytest.mark.xfail(strict=True, reason="B8-01 releases nothing when the report stops")
def test_a_stopped_report_releases_nothing() -> None:
    """F32. The absence streak across the gap is deliberately not asserted."""
    session = owner("s1")
    result = gc_owners(
        frozenset({session}),
        frozenset({session}),
        1,
        report=None,
        report_generation=None,
    )
    assert result.release == frozenset()


@pytest.mark.xfail(strict=True, reason="B8-04 drops a settled instrument and notifies")
def test_expiry_removes_the_atom_and_notifies_each_owner_per_topic() -> None:
    """One shared atom, two topics, two owners: a notice per (owner, topic).

    The future that has not settled stays, with the owner it had. The
    moment of settlement is already expired.
    """
    holder = owner("s1")
    other = owner("s2")
    settled = UniversalTicker.parse("Deribit_Option_BTCUSD-260925-100000-C")
    live = UniversalTicker.parse("Deribit_Future_BTCUSD-261225")
    shared = atom("ticker.BTC-25SEP26-100000-C.100ms")
    still = atom("ticker.BTC-25DEC26.100ms")
    at = datetime(2026, 9, 25, 8, 0, tzinfo=UTC)
    later = datetime(2026, 12, 25, 8, 0, tzinfo=UTC)
    bindings = (
        FeedBinding(holder, settled, "ticker", frozenset({shared}), at),
        FeedBinding(holder, settled, "greeks", frozenset({shared}), at),
        FeedBinding(other, settled, "ticker", frozenset({shared}), at),
        FeedBinding(holder, live, "ticker", frozenset({still}), later),
    )

    result = expire(
        {shared: frozenset({holder, other}), still: frozenset({holder})},
        bindings,
        at,
    )

    assert shared not in result.desired
    assert result.desired[still] == frozenset({holder})
    assert {
        (notice.owner, notice.topic, notice.ticker, notice.state, notice.code)
        for notice in result.notices
    } == {
        (holder, "ticker", settled, "expired", "expired"),
        (holder, "greeks", settled, "expired", "expired"),
        (other, "ticker", settled, "expired", "expired"),
    }
    assert {notice.expiry for notice in result.notices} == {at}


@pytest.mark.xfail(strict=True, reason="B8-04 keeps an instrument until it settles")
def test_an_instrument_is_kept_until_the_expiry_instant() -> None:
    holder = owner("s1")
    ticker = UniversalTicker.parse("Deribit_Future_BTCUSD-260925")
    staying = atom("ticker.BTC-25SEP26.100ms")
    at = datetime(2026, 9, 25, 8, 0, tzinfo=UTC)
    desired = {staying: frozenset({holder})}
    bindings = (FeedBinding(holder, ticker, "ticker", frozenset({staying}), at),)

    result = expire(desired, bindings, at - timedelta(seconds=1))

    assert result.desired == desired
    assert result.notices == ()
