"""PI-7 — an `api_id` is held by exactly one TD process, enforced.

`attach` used to decide this from `self._accounts`, which is process-local
memory: nothing stopped two processes each building a `TradingAccount` for one
credential and each serving `td.order.{api_id}`. That subject is a `BLPOP`, so
they become competing consumers — the account's order flow split between two
processes with their own OMS, their own ledger and their own reservations,
both publishing to `td.oms.{api_id}` while the strategy watches its balances
alternate between two half-pictures.

Latent rather than live before instances: nothing in the tree configured
replicas. This design is the first thing that makes several TDs ordinary,
which is why the invariant had to stop being an assertion.
"""

from __future__ import annotations

import asyncio

import pytest
from broker_harness import MIN_LEASE_TTL, a_broker
from mftik.broker import Broker
from mftik.liveness import claim_owner, hold_owner, owner_name, release_owner

DOMAIN = "td"
API = "42"


@pytest.fixture
async def broker() -> Broker:
    async with a_broker("td-own") as client:
        yield client


async def test_the_first_claim_wins_and_the_second_is_told_who_holds_it(
    broker: Broker,
) -> None:
    """A refusal has to name the holder, or nobody can act on it."""
    assert await claim_owner(broker, API, domain=DOMAIN, owner="a") is None
    assert await claim_owner(broker, API, domain=DOMAIN, owner="b") == "a"


async def test_a_claim_is_keyed_by_process_not_by_instance(
    broker: Broker,
) -> None:
    """Two processes sharing an `MFTIK_INSTANCE` is the case being guarded.

    A claim keyed by instance name would hand the account straight to the
    second one — the two would agree they were the same owner.
    """
    assert await claim_owner(broker, API, domain=DOMAIN, owner="pid-1") is None
    assert (
        await claim_owner(broker, API, domain=DOMAIN, owner="pid-2") == "pid-1"
    )


async def test_holding_refreshes_only_while_it_is_ours(broker: Broker) -> None:
    await claim_owner(broker, API, domain=DOMAIN, owner="a", ttl=30)

    assert await hold_owner(broker, API, domain=DOMAIN, owner="a") is True
    assert await hold_owner(broker, API, domain=DOMAIN, owner="b") is False


async def test_a_lapsed_claim_is_not_re_created_by_a_refresh(
    broker: Broker,
) -> None:
    """Losing a claim must send a process back through `claim_owner`.

    Refreshing a key that is gone would be the quiet way to end up with two
    owners: the process that let its claim expire would carry on believing it
    still held the account, with no moment at which a rival could say no.
    """
    await claim_owner(broker, API, domain=DOMAIN, owner="a")
    await broker.lease_drop(owner_name(API, domain=DOMAIN))

    assert await hold_owner(broker, API, domain=DOMAIN, owner="a") is False
    assert not await broker.lease_held(owner_name(API, domain=DOMAIN))


async def test_a_claim_taken_over_is_not_stolen_back_by_a_refresh(
    broker: Broker,
) -> None:
    """The race the read-then-`PEXPIRE` shape exists to make harmless.

    If A's claim lapses and B takes it, A's next refresh must not write its own
    token back over B's. The worst it may do is extend B's TTL once — B keeps
    the account, and A finds out on its next pass.
    """
    await claim_owner(broker, API, domain=DOMAIN, owner="a", ttl=30)
    await broker.lease_drop(owner_name(API, domain=DOMAIN))
    await claim_owner(broker, API, domain=DOMAIN, owner="b", ttl=30)

    assert await hold_owner(broker, API, domain=DOMAIN, owner="a") is False
    assert await broker.lease_owner(owner_name(API, domain=DOMAIN)) == "b"


async def test_releasing_lets_the_next_process_take_it(broker: Broker) -> None:
    """A redeploy that had to wait out a TTL would be an outage nobody caused."""
    await claim_owner(broker, API, domain=DOMAIN, owner="a")
    await release_owner(broker, API, domain=DOMAIN, owner="a")

    assert await claim_owner(broker, API, domain=DOMAIN, owner="b") is None


async def test_releasing_a_claim_that_moved_on_leaves_it_alone(
    broker: Broker,
) -> None:
    await claim_owner(broker, API, domain=DOMAIN, owner="a")
    await release_owner(broker, API, domain=DOMAIN, owner="b")

    assert await claim_owner(broker, API, domain=DOMAIN, owner="c") == "a"


async def test_a_claim_lapses_so_a_restarted_process_can_take_the_account(
    broker: Broker,
) -> None:
    """The cost of keying on the process: a restart waits out the TTL.

    Accepted deliberately. Keying on anything a restarted process could
    reproduce would also be reproducible by a rival, which is the whole point.
    """
    # Cut its remaining life to the shortest the transport can express rather
    # than waiting out a real thirty second claim.
    await claim_owner(broker, API, domain=DOMAIN, owner="old-pid", ttl=1)
    await broker.lease_hold(
        owner_name(API, domain=DOMAIN), owner="old-pid", ttl=MIN_LEASE_TTL
    )

    # Retried rather than slept past, which is also what a booting process
    # does. When the claim goes is the store's business — Redis drops a key on
    # the millisecond it expires, and NATS sweeps its expiries a little after
    # the second it floors them to — and a test that guessed a margin instead
    # would be asserting on that.
    deadline = asyncio.get_running_loop().time() + MIN_LEASE_TTL + 3.0
    while asyncio.get_running_loop().time() < deadline:
        if await claim_owner(broker, API, domain=DOMAIN, owner="new-pid") is None:
            return
        await asyncio.sleep(0.05)
    raise AssertionError("the old claim never lapsed, so the account is stuck")
