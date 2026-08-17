"""Futures stream names, endpoint routing, and the method auth classes.

The routing table is the thing worth testing hardest here. Since Binance split
``fstream`` into ``/public`` and ``/market``, a subscribe sent to the wrong
endpoint is *accepted and then silent* — no error, no push, a feed that looks
healthy and carries nothing. :func:`group_of` is what stands between the
adapter and that, so it is checked name by name.
"""

from __future__ import annotations

import pytest
from mftik.exchange.binance.future import methods as m
from mftik.exchange.binance.future import streams as st
from mftik.exchange.binance.future.protocol import (
    BINANCE_FUTURE_MARKET_STREAM_URL,
    BINANCE_FUTURE_PRIVATE_STREAM_URL,
    BINANCE_FUTURE_PUBLIC_STREAM_URL,
    BINANCE_FUTURE_WS_API_URL,
    BinanceAuthError,
    user_stream_url,
)

# --- names -----------------------------------------------------------------


def test_stream_names_are_lowercase() -> None:
    """Binance rejects ``BTCUSDT@aggTrade``; the symbol case is load-bearing."""
    assert st.agg_trade("BTCUSDT") == "btcusdt@aggTrade"
    assert st.kline("BTCUSDT", "1m") == "btcusdt@kline_1m"
    assert st.book_ticker("BTCUSDT") == "btcusdt@bookTicker"
    assert st.force_order("BTCUSDT") == "btcusdt@forceOrder"
    assert st.mark_price("BTCUSDT") == "btcusdt@markPrice@1s"
    assert st.depth("BTCUSDT", levels=20) == "btcusdt@depth20@100ms"
    assert st.depth_diff("BTCUSDT") == "btcusdt@depth@100ms"


def test_symbol_and_channel_are_readable_back_off_a_name() -> None:
    assert st.symbol_of("btcusdt@depth20@100ms") == "BTCUSDT"
    assert st.channel_of("btcusdt@depth20@100ms") == "depth20"
    assert st.channel_of("btcusdt@depth@100ms") == "depth"
    # The interval is dropped: every kline window lives on the same endpoint.
    assert st.channel_of("btcusdt@kline_1mo_is_not_a_thing") == "kline"
    assert st.channel_of("btcusdt@markPrice@1s") == "markPrice"


# --- endpoint routing ------------------------------------------------------


@pytest.mark.parametrize(
    ("name", "group"),
    [
        ("btcusdt@bookTicker", st.PUBLIC),
        ("btcusdt@depth20@100ms", st.PUBLIC),
        ("btcusdt@depth@100ms", st.PUBLIC),
        ("btcusdt@aggTrade", st.MARKET),
        ("btcusdt@markPrice@1s", st.MARKET),
        ("btcusdt@kline_1m", st.MARKET),
        ("btcusdt@ticker", st.MARKET),
        ("btcusdt@miniTicker", st.MARKET),
        ("btcusdt@forceOrder", st.MARKET),
    ],
)
def test_every_stream_knows_which_endpoint_answers_it(
    name: str, group: str
) -> None:
    assert st.group_of(name) == group


def test_the_book_and_the_tape_are_not_on_the_same_socket() -> None:
    """The whole reason the feed holds two connections."""
    assert st.group_of(st.depth("BTCUSDT")) != st.group_of(st.agg_trade("BTCUSDT"))


def test_an_unknown_stream_is_refused_rather_than_defaulted() -> None:
    """A default would be a coin flip, and the wrong side of it is silent."""
    with pytest.raises(st.UnknownStreamError, match="unknown"):
        st.group_of("btcusdt@compositeIndex")


# --- endpoints -------------------------------------------------------------


def test_the_urls_are_the_post_migration_ones() -> None:
    """The legacy ``fstream.binance.com/stream`` was retired 2026-04-23."""
    assert BINANCE_FUTURE_WS_API_URL == "wss://ws-fapi.binance.com/ws-fapi/v1"
    assert BINANCE_FUTURE_PUBLIC_STREAM_URL.endswith("/public/stream")
    assert BINANCE_FUTURE_MARKET_STREAM_URL.endswith("/market/stream")
    assert BINANCE_FUTURE_PRIVATE_STREAM_URL.endswith("/private/ws")


def test_the_user_socket_is_addressed_by_its_listen_key() -> None:
    assert user_stream_url("abc123") == (
        "wss://fstream.binance.com/private/ws/abc123"
    )


def test_a_user_socket_without_a_key_is_refused_before_dialling() -> None:
    """The key is the only thing authenticating that socket."""
    with pytest.raises(BinanceAuthError, match="listen key"):
        user_stream_url("")


# --- method auth classes ---------------------------------------------------


def test_trading_and_account_reads_are_signed() -> None:
    assert m.ORDER_PLACE in m.SIGNED
    assert m.ORDER_CANCEL in m.SIGNED
    assert m.ACCOUNT_BALANCE in m.SIGNED
    assert m.ACCOUNT_POSITION in m.SIGNED


def test_listen_key_methods_are_key_only_and_never_signed() -> None:
    """Binance's ``USER_STREAM`` class: an api key, no timestamp, no signature."""
    assert m.USER_DATA_STREAM_START in m.API_KEY_ONLY
    assert m.USER_DATA_STREAM_PING in m.API_KEY_ONLY
    assert not (m.API_KEY_ONLY & m.SIGNED)


def test_market_data_needs_nothing() -> None:
    for method in (m.DEPTH, m.TICKER_BOOK, m.TICKER_PRICE):
        assert method not in m.SIGNED
        assert method not in m.API_KEY_ONLY


def test_the_account_methods_are_the_v2_ones() -> None:
    """v1 answers a row per listed contract; v2 only what the account holds."""
    assert m.ACCOUNT_BALANCE.startswith("v2/")
    assert m.ACCOUNT_POSITION.startswith("v2/")
