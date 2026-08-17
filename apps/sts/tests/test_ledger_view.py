"""The strategy-side ledger — a read of TD's Redis state, not a local copy.

Every assertion here goes through the same path a strategy does: write the
hash the way TD would, then read it back through ``StrategyLedger``. If the
two ever disagree, one of them is keeping state it should not.
"""

from __future__ import annotations

from decimal import Decimal
from types import SimpleNamespace

import fakeredis.aioredis
import pytest
from mftik.broker import Broker, BrokerConfig
from mftik.exchange.oms import LedgerEntry
from mftik.protocol import Topics
from mftik.strategy.ledger import StrategyLedger


@pytest.fixture
async def broker() -> Broker:
    redis = fakeredis.aioredis.FakeRedis(decode_responses=True)
    client = Broker(
        BrokerConfig(redis_url="redis://fake", key_prefix="test"),
        redis_client=redis,
    )
    await client.connect()
    yield client
    await client.close()
    await redis.aclose()


def _ledger(broker: Broker, *api_ids: int) -> StrategyLedger:
    """A ledger bound to a strategy whose session attaches ``api_ids``."""
    ledger = StrategyLedger()
    session = SimpleNamespace(broker=broker, td_api_ids=list(api_ids))
    ledger.bind(SimpleNamespace(session=session))
    return ledger


async def _write(
    broker: Broker, api_id: int, asset: str, free: str, prelock: str = "0"
) -> None:
    """Write the row exactly as TD does."""
    await broker.state_put(
        Topics.td_ledger(api_id),
        asset,
        LedgerEntry(
            free=Decimal(free), prelock=Decimal(prelock), lock=Decimal("0")
        ),
    )


async def test_nothing_written_yet_reads_as_zero(broker: Broker) -> None:
    """A strategy sizing an order before TD has written must not crash."""
    ledger = _ledger(broker, 7)

    assert await ledger.available("USDT") == Decimal("0")
    assert await ledger.balances() == {}


async def test_available_subtracts_the_prelock(broker: Broker) -> None:
    ledger = _ledger(broker, 7)
    await _write(broker, 7, "USDT", "1000", "400")

    assert await ledger.free("USDT") == Decimal("1000")
    assert await ledger.prelock("USDT") == Decimal("400")
    assert await ledger.available("USDT") == Decimal("600")


async def test_a_single_account_needs_no_api_id(broker: Broker) -> None:
    ledger = _ledger(broker, 7)
    await _write(broker, 7, "USDT", "1000")

    assert await ledger.available("USDT") == Decimal("1000")


async def test_multiple_accounts_must_be_named(broker: Broker) -> None:
    """Guessing which account the strategy meant would be a money bug."""
    ledger = _ledger(broker, 7, 8)
    await _write(broker, 7, "USDT", "1000")
    await _write(broker, 8, "USDT", "50")

    assert await ledger.available("USDT") == Decimal("0")
    assert await ledger.available("USDT", 7) == Decimal("1000")
    assert await ledger.available("USDT", 8) == Decimal("50")


async def test_an_unknown_asset_reads_as_zero(broker: Broker) -> None:
    ledger = _ledger(broker, 7)
    await _write(broker, 7, "USDT", "1000")

    assert await ledger.available("DOGE") == Decimal("0")


async def test_a_later_write_is_what_the_strategy_sees(broker: Broker) -> None:
    """No caching: the second read reflects TD's second write."""
    ledger = _ledger(broker, 7)
    await _write(broker, 7, "USDT", "1000")
    assert await ledger.available("USDT") == Decimal("1000")

    await _write(broker, 7, "USDT", "900", "100")

    assert await ledger.available("USDT") == Decimal("800")


async def test_clearing_td_state_empties_the_view(broker: Broker) -> None:
    """When the TD session dies its state goes, and STS stops seeing it."""
    ledger = _ledger(broker, 7)
    await _write(broker, 7, "USDT", "1000")

    await broker.state_clear(Topics.td_ledger(7))

    assert await ledger.balances() == {}
    assert await ledger.available("USDT") == Decimal("0")


async def test_the_stored_shape_is_free_prelock_lock(broker: Broker) -> None:
    """The on-wire contract STS depends on."""
    await _write(broker, 7, "USDT", "1000", "400")

    row = await broker.state_get(Topics.td_ledger(7), "USDT")

    assert sorted(row) == ["free", "lock", "prelock"]
