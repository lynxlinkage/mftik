"""A refusal the venue puts on the order stream, not on the call.

Most venues say no by failing the call, and ``mftik_td.errors.normalize`` turns
that exception into a code. Bybit does not, for the one refusal a passive
strategy meets constantly: a post-only order that would cross is accepted,
killed, and explained on the ``order`` topic as ``rejectReason``. That row
reaches TD as an order update, so it takes a path of its own —
``_store_then_announce_order`` — and this is what that path owes a strategy.

The regression it guards is narrow and was live in 0.4.1: the branch published
``VENUE_REJECTED`` and the literal string ``"rejected"`` no matter what the
venue said, which made a crossed post-only indistinguishable from a refusal for
short balance or a notional under the floor.

OKX reaches the same path from further back. It does not even call the refusal
a rejection — the row says ``canceled``, and only ``cancelSource`` separates it
from the strategy's own cancel — so the adapter is what puts it on this path at
all. The last test here is the end of that: an OKX row, the real converter, and
the reject a strategy was owed.
"""

from __future__ import annotations

import asyncio
from decimal import Decimal
from typing import Any

import pytest
from broker_harness import a_broker
from mftik.broker import Broker
from mftik.exchange import PaperExchange, Side
from mftik.exchange.models import Order, OrderStatus, OrderType
from mftik.exchange.okx.models import OkxOrderUpdate
from mftik.exchange.tickers import UniversalTicker
from mftik.protocol import (
    STS_LEASE_HEARTBEAT,
    TD_ORDER_REJECT,
    Envelope,
    LeaseHeartbeat,
    RejectCode,
    TdAttachRequest,
    Topics,
)
from mftik_td.session import PaperSessionFactory, SessionManager

API_ID = 42
SESSION = "sts-stream-reject"
TICKER = "Paper_Spot_BTCUSDT"


@pytest.fixture
async def broker() -> Broker:
    async with a_broker() as client:
        yield client


@pytest.fixture
async def paper() -> PaperExchange:
    async with PaperExchange(
        symbols={"BTCUSDT": Decimal("50000")}, tick_interval=0.05, seed=7
    ) as ex:
        yield ex


async def _lease_publisher(
    broker: Broker, session_id: str, stop: asyncio.Event
) -> None:
    token = 0
    topic = Topics.sts_td_session(session_id)
    while not stop.is_set():
        token += 1
        await broker.publish(
            topic,
            Envelope[LeaseHeartbeat].wrap(
                LeaseHeartbeat(session_id=session_id, token=token),
                type=STS_LEASE_HEARTBEAT,
                source="sts",
                session_id=session_id,
            ),
        )
        try:
            await asyncio.wait_for(stop.wait(), timeout=0.1)
        except TimeoutError:
            continue


@pytest.fixture
async def session(broker: Broker, paper: PaperExchange):
    """One attached session, renamed to Bybit — the venue this path is for.

    ``venue`` reads the private client's own name, so the rename is what makes
    the session look up the Bybit table. Nothing else about the paper connector
    is used here: the refusal is injected as the row a venue stream would have
    delivered.
    """
    manager = SessionManager(
        PaperSessionFactory(broker, paper), broker, lease_grace=2.0
    )
    stop = asyncio.Event()
    pub = asyncio.create_task(_lease_publisher(broker, SESSION, stop))
    await manager.attach(
        TdAttachRequest(
            session_id=SESSION, api_id=API_ID, timeout=2.0, created_by=1
        )
    )
    live = manager.get(API_ID)
    live.private.name = "Bybit"
    yield live
    stop.set()
    await asyncio.gather(pub, return_exceptions=True)
    await manager.close_all()


def _refused(cid: str, reason: str) -> Order:
    return Order(
        client_order_id=cid,
        universal_ticker=TICKER,
        side=Side.BUY,
        type=OrderType.LIMIT,
        status=OrderStatus.REJECTED,
        qty=Decimal("0.001"),
        price=Decimal("50001"),
        reject_reason=reason,
    )


async def _reject_for(broker: Broker, session, order: Order) -> dict[str, Any]:
    """Drive one refused row through the stream path and read what went out."""
    got: asyncio.Future[dict[str, Any]] = (
        asyncio.get_running_loop().create_future()
    )
    stop = asyncio.Event()

    async def _listen() -> None:
        async for env in broker.subscribe(Topics.td_global(API_ID), stop=stop):
            payload = env.payload
            if (
                env.type == TD_ORDER_REJECT
                and payload.get("client_order_id") == order.client_order_id
                and not got.done()
            ):
                got.set_result(payload)
                stop.set()
                return

    listener = asyncio.create_task(_listen())
    await asyncio.sleep(0.05)
    await session._store_then_announce_order(order)
    try:
        return await asyncio.wait_for(got, timeout=2.0)
    finally:
        stop.set()
        await asyncio.gather(listener, return_exceptions=True)


async def test_a_crossed_post_only_off_the_stream_is_202(
    broker: Broker, session
) -> None:
    """The code ``ChaseOrder`` branches on, from the one venue that never
    raised it. Before this, every crossed post-only on Bybit arrived as
    ``200 VENUE_REJECTED`` and was logged — and alerted — as a warning."""
    payload = await _reject_for(
        broker, session, _refused("cid-cross", "EC_PostOnlyWillTakeLiquidity")
    )

    assert payload["error_code"] == RejectCode.VENUE_POST_ONLY_WOULD_CROSS
    assert payload["reason"] == "EC_PostOnlyWillTakeLiquidity"
    assert payload["universal_ticker"] == TICKER


async def test_the_venues_own_words_reach_the_strategy(
    broker: Broker, session
) -> None:
    """An unmapped reason is still worth more than ``"rejected"``: it comes
    back as the code *and* the text, so the table can catch up later."""
    payload = await _reject_for(
        broker, session, _refused("cid-new", "EC_SomethingNew")
    )

    assert payload["error_code"] == "EC_SomethingNew"
    assert payload["reason"] == "EC_SomethingNew"


async def test_a_refusal_with_no_reason_is_still_a_plain_reject(
    broker: Broker, session
) -> None:
    """The venues that carry no such field must land exactly where they did."""
    payload = await _reject_for(broker, session, _refused("cid-bare", ""))

    assert payload["error_code"] == RejectCode.VENUE_REJECTED
    assert payload["reason"] == "rejected"


@pytest.mark.parametrize(
    ("reason", "level"),
    [
        # Not a fault — post-only exists to be refused when the price crossed,
        # and a node with an alert matcher on TD warnings would page someone
        # for every one of them.
        ("EC_PostOnlyWillTakeLiquidity", "info"),
        ("EC_LimitOrderInvalidPrice", "warn"),
        ("", "warn"),
    ],
)
async def test_only_a_crossed_post_only_drops_out_of_the_warnings(
    broker: Broker, session, reason: str, level: str
) -> None:
    seen: list[tuple[str, str]] = []

    async def _log(message: str, *, level: str = "info", **_: Any) -> None:
        seen.append((level, message))

    session._td_log = _log
    await session._store_then_announce_order(_refused("cid-level", reason))

    assert [lvl for lvl, msg in seen if "order rejected" in msg] == [level]


async def test_an_okx_row_that_says_canceled_still_reaches_on_order_reject(
    broker: Broker, session
) -> None:
    """#37, end to end. OKX pushed ``state=canceled`` for a post-only it had
    refused, so nothing on this path fired: no reject, no log line, and a
    strategy with no way to tell a refusal from its own cancel."""
    session.private.name = "Okx"
    order = OkxOrderUpdate.model_validate(
        {
            "instId": "BTC-USDT",
            "ordId": "ord-okx",
            "clOrdId": "cid-okx",
            "side": "buy",
            "ordType": "post_only",
            "state": "canceled",
            "cancelSource": "31",
            "px": "77518.1",
            "sz": "0.00001",
            "accFillSz": "0",
        }
    ).to_order(UniversalTicker.parse("Okx_Spot_BTCUSDT"))

    payload = await _reject_for(broker, session, order)

    assert payload["error_code"] == RejectCode.VENUE_POST_ONLY_WOULD_CROSS
    assert "post-only order will take liquidity" in payload["reason"]
