"""Order entry over ``td.order.{api_id}`` — what TD refuses before the venue.

RM-06 took the serving side with it: ``td.order.{api_id}`` was answered by the
session manager's own loop, started by attach. IF-11 defines the interface and
B6-02 / B6-08 reimplement the refusals. What is left here is the one case that
never needed a TD to be serving at all.
"""

from __future__ import annotations

from decimal import Decimal
from typing import Any

import pytest
from broker_harness import a_broker
from mftik.broker import Broker
from mftik.exchange import Side
from mftik.exchange.models import OrderType
from mftik.protocol import (
    STS_ORDER_SUBMIT,
    Envelope,
    OrderSubmit,
    Topics,
)

# B2-05: order refusals return with B6-02 (#220) and B6-08 (#226). What is
# left opens a private socket to time out an unserved ``td.order`` subject,
# so it stays integration until that rewrite.
pytestmark = pytest.mark.integration

API_ID = 42
SESSION = "sts-rpc"


@pytest.fixture
async def broker() -> Broker:
    async with a_broker() as client:
        yield client


def _submit_envelope(**overrides: Any) -> Envelope[Any]:
    payload: dict[str, Any] = {
        "session_id": SESSION,
        "api_id": API_ID,
        "universal_ticker": "Paper_Spot_BTCUSDT",
        "side": Side.BUY,
        "type": OrderType.LIMIT,
        "qty": Decimal("0.01"),
        "price": Decimal("1000"),
        "client_order_id": "cid-1",
    }
    payload.update(overrides)
    return Envelope[OrderSubmit].wrap(
        OrderSubmit.model_validate(payload),
        type=STS_ORDER_SUBMIT,
        source="sts",
        session_id=str(payload["session_id"]),
    )


@pytest.mark.real_sleep(
    reason="NATS no-responders grace is a real asyncio.sleep"
)
async def test_no_td_serving_times_out(broker: Broker) -> None:
    """Nothing is attached, so the request waits in the list and times out.

    This is the case the old pub/sub path lost silently: the message went
    nowhere and the strategy never learned it.
    """
    from mftik.broker.errors import RequestTimeoutError

    with pytest.raises(RequestTimeoutError):
        await broker.request(
            Topics.td_order(999), _submit_envelope(), timeout=0.3
        )
