"""COIN-M stream names — lowercase, and no endpoint routing table.

dapi still answers on one combined socket. A ``group_of`` would imply a
split that this market does not have, so the builders are names only.
"""

from __future__ import annotations

import pytest
from mftik.exchange.binance.delivery import streams as st
from mftik.exchange.binance.delivery.protocol import (
    BINANCE_DELIVERY_PRIVATE_STREAM_URL,
    BINANCE_DELIVERY_STREAM_URL,
    user_stream_url,
)
from mftik.exchange.binance.protocol import BinanceAuthError


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


def test_the_user_stream_is_the_classic_ws_path() -> None:
    """dapi was not part of the 2026 ``/private`` split."""
    assert BINANCE_DELIVERY_PRIVATE_STREAM_URL == "wss://dstream.binance.com/ws"
    assert user_stream_url("k1") == "wss://dstream.binance.com/ws/k1"


def test_a_user_stream_without_a_key_is_refused() -> None:
    with pytest.raises(BinanceAuthError, match="listen key"):
        user_stream_url("")
