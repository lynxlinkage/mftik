"""COIN-M stream names — lowercase, and no endpoint routing table.

dapi still answers on one combined socket. A ``group_of`` would imply a
split that this market does not have, so the builders are names only.
"""

from __future__ import annotations

from mftik.exchange.binance.delivery import streams as st
from mftik.exchange.binance.delivery.protocol import BINANCE_DELIVERY_STREAM_URL


def test_stream_names_are_lowercase() -> None:
    """Binance rejects ``BTCUSD_PERP@aggTrade``; the symbol case is load-bearing."""
    assert st.agg_trade("BTCUSD_PERP") == "btcusd_perp@aggTrade"
    assert st.kline("BTCUSD_PERP", "1m") == "btcusd_perp@kline_1m"
    assert st.ticker("BTCUSD_PERP") == "btcusd_perp@ticker"
    assert st.book_ticker("BTCUSD_PERP") == "btcusd_perp@bookTicker"
    assert st.force_order("BTCUSD_PERP") == "btcusd_perp@forceOrder"
    assert st.mark_price("BTCUSD_PERP") == "btcusd_perp@markPrice@1s"
    assert st.depth("BTCUSD_PERP", levels=20) == "btcusd_perp@depth20@100ms"


def test_the_symbol_is_readable_back_off_a_name() -> None:
    assert st.symbol_of("btcusd_perp@depth20@100ms") == "BTCUSD_PERP"
    assert st.symbol_of("btcusd_perp@kline_1m") == "BTCUSD_PERP"


def test_there_is_no_group_routing() -> None:
    """A table would invent a split this host does not have."""
    assert not hasattr(st, "group_of")
    assert not hasattr(st, "GROUPS")


def test_the_live_feed_is_the_combined_dstream() -> None:
    assert BINANCE_DELIVERY_STREAM_URL == "wss://dstream.binance.com/stream"
