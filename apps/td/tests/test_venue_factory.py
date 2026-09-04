"""TD builds the private client from ``apis.venue``, not from a hardcoded one."""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal

import pytest
from broker_harness import a_broker
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from cryptography.hazmat.primitives.serialization import (
    Encoding,
    NoEncryption,
    PrivateFormat,
)
from mftik.broker import Broker
from mftik.exchange import PaperExchange
from mftik.exchange.binance.delivery.private import BinanceDeliveryPrivateClient
from mftik.exchange.binance.future.private import BinanceFuturePrivateClient
from mftik.exchange.binance.spot.private import BinanceSpotPrivateClient
from mftik.exchange.binance.spot.protocol import BinanceAuthError
from mftik.exchange.bybit.private import BybitPrivateClient
from mftik.exchange.errors import ExchangeError
from mftik.exchange.gate.future.private import GateFuturesPrivateClient
from mftik.exchange.gate.spot.private import GateSpotPrivateClient
from mftik.exchange.okx.private import OkxPrivateClient
from mftik.exchange.tickers import Category
from mftik_td.session import PaperSessionFactory, VenueSessionFactory

#: What a Binance credential's ``api_secret`` actually holds: an Ed25519
#: private key, not a shared secret.
ED25519_PEM = (
    Ed25519PrivateKey.generate()
    .private_bytes(
        encoding=Encoding.PEM,
        format=PrivateFormat.PKCS8,
        encryption_algorithm=NoEncryption(),
    )
    .decode("ascii")
)


@dataclass
class FakeApiRow:
    """Stands in for the ``apis`` row TD loads by api_id."""

    id: int
    venue: str
    api_key: str
    api_secret: str
    passphrase: str | None = None


@pytest.fixture
async def broker() -> Broker:
    async with a_broker() as client:
        yield client


@pytest.fixture
async def paper() -> PaperExchange:
    async with PaperExchange(
        symbols={"BTCUSDT": Decimal("50000")}, tick_interval=0.05, seed=3
    ) as ex:
        yield ex


def _factory(broker: Broker, rows: dict[int, FakeApiRow], **kwargs):
    async def load_api(api_id: int) -> FakeApiRow | None:
        return rows.get(api_id)

    return VenueSessionFactory(broker, load_api=load_api, **kwargs)


async def test_paper_venue_builds_a_paper_session(
    broker: Broker, paper: PaperExchange
) -> None:
    rows = {
        1: FakeApiRow(id=1, venue="Paper", api_key="k1", api_secret="s1"),
    }
    factory = _factory(broker, rows, paper=PaperSessionFactory(broker, paper))

    session = await factory.create(1)

    assert session.api_id == 1
    # Credentials come from the row, not synthesized from the api_id.
    assert session.private.api_key == "k1"


async def test_gate_spot_venue_builds_a_gate_client(broker: Broker) -> None:
    rows = {
        7: FakeApiRow(
            id=7, venue="Gate", api_key="gk", api_secret="gs"
        ),
    }
    factory = _factory(broker, rows)

    session = await factory.create(7)

    assert isinstance(session.private, GateSpotPrivateClient)
    assert session.private.name == "Gate"
    assert session.private.api_key == "gk"
    assert session.api_id == 7
    # Built but not connected — the manager starts it.
    assert not session.private.connected


async def test_gate_futures_venue_builds_a_perp_client(broker: Broker) -> None:
    rows = {
        17: FakeApiRow(
            id=17, venue="GateFutures", api_key="gfk", api_secret="gfs"
        ),
    }
    factory = _factory(broker, rows)

    session = await factory.create(17)

    assert isinstance(session.private, GateFuturesPrivateClient)
    assert session.private.name == "GateFutures"
    assert session.private.category is Category.PERP
    assert hasattr(session.private, "fetch_positions")
    assert hasattr(session.private, "fetch_leverage")
    assert not session.private.connected


async def test_binance_venue_builds_a_binance_client(broker: Broker) -> None:
    rows = {
        8: FakeApiRow(
            id=8, venue="Binance", api_key="bk", api_secret=ED25519_PEM
        ),
    }
    factory = _factory(broker, rows)

    session = await factory.create(8)

    assert isinstance(session.private, BinanceSpotPrivateClient)
    assert session.private.name == "Binance"
    assert session.private.api_key == "bk"
    assert session.api_id == 8
    # Built but not connected — the manager starts it.
    assert not session.private.connected


async def test_binance_future_venue_builds_a_futures_client(
    broker: Broker,
) -> None:
    """Same credential shape as spot, different account entirely.

    And a connector that reports positions, which no spot session does — TD
    picks those up by name off the client it was handed.
    """
    rows = {
        11: FakeApiRow(
            id=11, venue="BinanceUM", api_key="fk", api_secret=ED25519_PEM
        ),
    }
    factory = _factory(broker, rows)

    session = await factory.create(11)

    assert isinstance(session.private, BinanceFuturePrivateClient)
    assert session.private.name == "BinanceUM"
    assert session.private.category is Category.PERP
    assert hasattr(session.private, "fetch_positions")
    assert hasattr(session.private, "stream_positions")
    assert not session.private.connected


async def test_binance_delivery_venue_builds_an_inverse_client(
    broker: Broker,
) -> None:
    """Same Ed25519 PEM shape, different wallet; positions come off the listen key."""
    rows = {
        13: FakeApiRow(
            id=13, venue="BinanceCM", api_key="dk", api_secret=ED25519_PEM
        ),
    }
    factory = _factory(broker, rows)

    session = await factory.create(13)

    assert isinstance(session.private, BinanceDeliveryPrivateClient)
    assert session.private.name == "BinanceCM"
    assert session.private.category is Category.INVERSE
    assert hasattr(session.private, "fetch_positions")
    assert hasattr(session.private, "stream_positions")
    assert not session.private.connected


async def test_bybit_venue_builds_one_client_for_the_whole_account(
    broker: Broker,
) -> None:
    """One credential, one connector — and it reports every book.

    ``category`` picks the book orders go to; the account stream is unscoped,
    so a spot-ordering session still sees the perp positions that share the
    wallet.
    """
    rows = {
        9: FakeApiRow(id=9, venue="Bybit", api_key="yk", api_secret="ys"),
    }
    factory = _factory(broker, rows)

    session = await factory.create(9)

    assert isinstance(session.private, BybitPrivateClient)
    assert session.private.name == "Bybit"
    assert session.private.api_key == "yk"
    assert session.private.category is Category.SPOT
    # Unscoped: the private stream carries every category on the account.
    assert session.private.stream.product is None
    # The capability TD probes for before pumping a position feed.
    assert hasattr(session.private, "stream_positions")
    assert hasattr(session.private, "fetch_positions")
    # Built but not connected — the manager starts it.
    assert not session.private.connected


async def test_okx_venue_builds_one_client_for_the_whole_account(
    broker: Broker,
) -> None:
    """One credential, a passphrase, one connector — every book."""
    rows = {
        12: FakeApiRow(
            id=12,
            venue="Okx",
            api_key="ok",
            api_secret="os",
            passphrase="op",
        ),
    }
    factory = _factory(broker, rows)

    session = await factory.create(12)

    assert isinstance(session.private, OkxPrivateClient)
    assert session.private.name == "Okx"
    assert session.private.api_key == "ok"
    assert session.private.passphrase == "op"
    assert session.private.category is Category.SPOT
    assert hasattr(session.private, "stream_positions")
    assert hasattr(session.private, "fetch_positions")
    assert not session.private.connected


async def test_a_binance_credential_that_is_not_a_key_fails_the_attach(
    broker: Broker,
) -> None:
    """Better here than on the first order, hours later, as a signature error."""
    rows = {
        8: FakeApiRow(
            id=8, venue="Binance", api_key="bk", api_secret="not-a-key"
        ),
    }
    factory = _factory(broker, rows)

    with pytest.raises(BinanceAuthError):
        await factory.create(8)


async def test_venue_is_resolved_case_insensitively(broker: Broker) -> None:
    rows = {
        7: FakeApiRow(id=7, venue="  gate ", api_key="gk", api_secret="gs"),
    }
    factory = _factory(broker, rows)

    session = await factory.create(7)
    assert isinstance(session.private, GateSpotPrivateClient)


async def test_unknown_venue_fails_loudly(broker: Broker) -> None:
    """A bad venue must not quietly fall back to paper and trade elsewhere."""
    rows = {
        9: FakeApiRow(id=9, venue="gate-spot", api_key="k", api_secret="s"),
    }
    factory = _factory(broker, rows)

    with pytest.raises(ExchangeError, match="unknown venue"):
        await factory.create(9)


async def test_missing_credential_row_fails(broker: Broker) -> None:
    factory = _factory(broker, {})

    with pytest.raises(ExchangeError, match="no api credential"):
        await factory.create(404)


async def test_gate_client_gets_the_symbol_plane(broker: Broker) -> None:
    """Symbol translation must go through the plane, not string surgery."""
    from mftik.exchange.symbols import SymbolResolver

    rows = {7: FakeApiRow(id=7, venue="Gate", api_key="gk", api_secret="gs")}
    factory = _factory(broker, rows)

    session = await factory.create(7)

    resolver: SymbolResolver = session.private.symbols
    assert hasattr(resolver, "exch_ticker")
    assert hasattr(resolver, "symbol_for")


async def test_one_symbol_client_is_shared_across_sessions(
    broker: Broker,
) -> None:
    """Its cache is what keeps symbol lookups off the order path."""
    rows = {
        7: FakeApiRow(id=7, venue="Gate", api_key="a", api_secret="b"),
        8: FakeApiRow(id=8, venue="Gate", api_key="c", api_secret="d"),
    }
    factory = _factory(broker, rows)

    first = await factory.create(7)
    second = await factory.create(8)

    assert first.private.symbols is second.private.symbols
