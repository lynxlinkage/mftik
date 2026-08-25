"""Cross-key scenarios at the MD boundary — I1 and I6, no sockets.

Detach never reaches a venue ``unsubscribe()``. These tests assert what MD
can observe: which ``stream_*`` the connector was asked for, which sources
closed, and which pumps stay fed.
"""

from __future__ import annotations

import ast
from decimal import Decimal
from pathlib import Path

import pytest
from mftik.exchange.models import AggTrade, BestQuote, Ticker
from mftik.exchange.tickers import UniversalTicker
from mftik_md.session.dispatcher import Dispatcher
from mftik_md.session.venue import VenueSession
from test_md_venue_feeds import FAKE, FakePublic, _wait_until


def _ticker(*, last: str = "100.5") -> Ticker:
    return Ticker(
        universal_ticker=str(FAKE),
        bid=Decimal("100"),
        ask=Decimal("101"),
        last=Decimal(last),
    )


def _quote(*, bid: str = "100") -> BestQuote:
    return BestQuote(
        universal_ticker=str(FAKE),
        bid=Decimal(bid),
        bid_qty=Decimal("1"),
        ask=Decimal("101"),
        ask_qty=Decimal("2"),
    )


@pytest.mark.asyncio
async def test_detach_bestquote_leaves_the_ticker_pump_fed() -> None:
    """S2 — drop one product key; MD must not close the survivor."""
    public = FakePublic()
    seen: list[str] = []

    async def _on_update(topic, ticker, env) -> None:  # noqa: ANN001
        seen.append(f"{topic}:{env.payload.get('last', env.payload.get('bid'))}")

    disp = Dispatcher(broker=None)  # type: ignore[arg-type]
    sess = VenueSession(FAKE.venue, public, on_update=_on_update)
    await sess.start()
    disp.subscribe("s1", "ticker", FAKE)
    disp.subscribe("s1", "bestquote", FAKE)
    await sess.ensure_feed("ticker", FAKE)
    await sess.ensure_feed("bestquote", FAKE)
    await _wait_until(
        lambda: "ticker" in public.opened and "best_quote" in public.opened
    )

    emptied, rc = disp.unsubscribe("s1", "bestquote", FAKE)
    assert emptied and rc == 0
    await sess.stop_feed("bestquote", FAKE)
    await _wait_until(lambda: "best_quote" in public.closed)

    assert "ticker" not in public.closed
    assert disp.refcount("ticker", FAKE) == 1
    public.push("ticker", _ticker(last="200"))
    await _wait_until(lambda: any(row.startswith("ticker:200") for row in seen))
    await sess.stop()


@pytest.mark.asyncio
async def test_detach_ticker_leaves_the_bestquote_pump_fed() -> None:
    """S3 — the other way."""
    public = FakePublic()
    seen: list[object] = []

    async def _on_update(topic, ticker, env) -> None:  # noqa: ANN001
        seen.append(topic)

    disp = Dispatcher(broker=None)  # type: ignore[arg-type]
    sess = VenueSession(FAKE.venue, public, on_update=_on_update)
    await sess.start()
    disp.subscribe("s1", "ticker", FAKE)
    disp.subscribe("s1", "bestquote", FAKE)
    await sess.ensure_feed("ticker", FAKE)
    await sess.ensure_feed("bestquote", FAKE)
    await _wait_until(lambda: len(seen) >= 2)

    emptied, _ = disp.unsubscribe("s1", "ticker", FAKE)
    assert emptied
    await sess.stop_feed("ticker", FAKE)
    await _wait_until(lambda: "ticker" in public.closed)

    assert "best_quote" not in public.closed
    public.push("best_quote", _quote(bid="99"))
    await _wait_until(lambda: seen.count("bestquote") >= 2)
    await sess.stop()


@pytest.mark.asyncio
async def test_two_sessions_on_one_key_share_one_pump() -> None:
    """S4 — two STS links, one ``stream_ticker``. Dropping one leaves the pump."""
    public = FakePublic()
    disp = Dispatcher(broker=None)  # type: ignore[arg-type]

    async def _on_update(topic, ticker, env) -> None:  # noqa: ANN001
        return None

    sess = VenueSession(FAKE.venue, public, on_update=_on_update)
    await sess.start()
    first, _ = disp.subscribe("s1", "ticker", FAKE)
    await sess.ensure_feed("ticker", FAKE)
    second, rc = disp.subscribe("s2", "ticker", FAKE)
    await sess.ensure_feed("ticker", FAKE)
    assert first and not second
    assert rc == 2
    await _wait_until(lambda: public.opened == ["ticker"])

    emptied, rc = disp.unsubscribe("s1", "ticker", FAKE)
    assert not emptied and rc == 1
    assert "ticker" not in public.closed
    await sess.stop()


@pytest.mark.asyncio
async def test_dropping_one_of_two_product_topics_leaves_the_other() -> None:
    """S5 — two product feeds; MD's refcount does not close the survivor."""
    public = FakePublic()
    seen: list[str] = []

    async def _on_update(topic, ticker, env) -> None:  # noqa: ANN001
        seen.append(topic)

    disp = Dispatcher(broker=None)  # type: ignore[arg-type]
    sess = VenueSession(FAKE.venue, public, on_update=_on_update)
    await sess.start()
    disp.subscribe("s1", "trade", FAKE)
    disp.subscribe("s1", "aggtrade", FAKE)
    await sess.ensure_feed("trade", FAKE)
    await sess.ensure_feed("aggtrade", FAKE)
    await _wait_until(lambda: set(seen) == {"trade", "aggtrade"})

    emptied, _ = disp.unsubscribe("s1", "trade", FAKE)
    assert emptied
    await sess.stop_feed("trade", FAKE)
    await _wait_until(lambda: "trades" in public.closed)

    assert "agg_trades" not in public.closed
    public.push(
        "agg_trades",
        AggTrade(
            universal_ticker=str(FAKE),
            price=Decimal("1"),
            qty=Decimal("1"),
            side="buy",
            first_trade_id="20",
            last_trade_id="21",
        ),
    )
    await _wait_until(lambda: seen.count("aggtrade") >= 2)
    await sess.stop()


def test_md_imports_no_venue_channel_or_stream_module() -> None:
    """S6 — MD names no venue channel. Lint-shaped, starts green."""
    root = Path(__file__).resolve().parents[1] / "src"
    offenders: list[str] = []
    for path in root.rglob("*.py"):
        tree = ast.parse(path.read_text())
        for node in ast.walk(tree):
            module = getattr(node, "module", None)
            if not isinstance(module, str) or "mftik.exchange" not in module:
                continue
            if module.endswith(".channels") or module.endswith(".streams"):
                offenders.append(f"{path.relative_to(root)}:{node.lineno} {module}")
    assert offenders == []


def test_dispatcher_refcount_is_per_product_key() -> None:
    disp = Dispatcher(broker=None)  # type: ignore[arg-type]
    other = UniversalTicker.parse("Fake_Spot_ETHUSDT")
    disp.subscribe("s1", "ticker", FAKE)
    disp.subscribe("s1", "bestquote", FAKE)
    disp.subscribe("s2", "ticker", FAKE)
    assert disp.refcount("ticker", FAKE) == 2
    assert disp.refcount("bestquote", FAKE) == 1
    assert disp.refcount("ticker", other) == 0
