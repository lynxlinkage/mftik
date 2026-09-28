"""A Binance ticker assembled from two sockets ends when either one does."""

from __future__ import annotations

import asyncio

import pytest
from mftik.exchange.binance.delivery.public import _merge as merge_delivery
from mftik.exchange.binance.future.public import _merge as merge_future
from mftik.exchange.stream import EventStream, SourceEnded


@pytest.mark.asyncio
@pytest.mark.parametrize("merge", [merge_future, merge_delivery])
async def test_one_socket_giving_up_ends_the_merged_feed(merge) -> None:  # noqa: ANN001
    quote: EventStream[dict[str, int]] = EventStream()
    stats: EventStream[dict[str, int]] = EventStream()
    quote.push({"bid": 1})
    merged = merge(quote=quote, stats=stats)
    assert await asyncio.wait_for(merged.__anext__(), 1) == ("quote", {"bid": 1})
    quote.close("binance.futures giving up after 11 reconnect attempts")
    with pytest.raises(SourceEnded, match="11 reconnect attempts"):
        await asyncio.wait_for(merged.__anext__(), 1)
