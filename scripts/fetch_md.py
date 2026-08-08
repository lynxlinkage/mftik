#!/usr/bin/env python3
"""Ask a running MD for market data, and print what comes back.

Intended for local / docker-compose testing::

    just fetch quote  Gate_Spot_BTCUSDT
    just fetch book   Gate_Spot_BTCUSDT 5
    just fetch klines Gate_Spot_BTCUSDT 1h 5

The ticker is resolved leniently, so ``gate_spot_btcusdt`` works too.

Deliberately not a strategy, and not attached to anything. It opens a reply
channel of its own, sends one request on ``md.fetch`` and prints the ack and
the answer — which is the plane's whole claim: anything can ask, without first
becoming a market-data session.

The ack and the answer are separate on purpose. ``accepted`` means MD took the
query; the venue can still refuse it afterwards, and the failure comes back on
the result with a code. Both are printed, because telling them apart is the
point of the two-step.
"""

from __future__ import annotations

import asyncio
import os
import sys
import uuid

from mft.broker import Broker, BrokerConfig
from mft.exchange.tickers import UniversalTicker
from mft.protocol import (
    MD_BESTQUOTE_RESULT,
    MD_FETCH_BESTQUOTE,
    MD_FETCH_KLINES,
    MD_FETCH_ORDERBOOK,
    MD_KLINES_RESULT,
    MD_ORDERBOOK_RESULT,
    Envelope,
    MdBestQuoteResult,
    MdFetchBestQuote,
    MdFetchKlines,
    MdFetchOrderBook,
    MdFetchRequest,
    MdKlinesResult,
    MdOrderBookResult,
    MdQueryAck,
    Topics,
)
from mft.protocol.query_codes import describe, is_retryable

#: How long to wait for the data itself. Generous: the ack is one Redis
#: round-trip, but the answer behind it is a venue call.
RESULT_TIMEOUT_S = 20.0

RESULTS = {
    MD_KLINES_RESULT: MdKlinesResult,
    MD_ORDERBOOK_RESULT: MdOrderBookResult,
    MD_BESTQUOTE_RESULT: MdBestQuoteResult,
}


def build(kind: str, args: list[str], reply_channel: str) -> tuple[str, MdFetchRequest]:
    if not args:
        raise SystemExit(f"usage: {kind} <universal_ticker> [...]")
    common = {
        "reply_channel": reply_channel,
        "query_id": f"cli-{uuid.uuid4().hex[:8]}",
        "ticker": str(UniversalTicker.resolve(args[0])),
    }
    rest = args[1:]
    if kind == "klines":
        interval = rest[0] if rest else "1h"
        limit = int(rest[1]) if len(rest) > 1 else 5
        return MD_FETCH_KLINES, MdFetchKlines(**common, interval=interval, limit=limit)
    if kind == "book":
        depth = int(rest[0]) if rest else 5
        return MD_FETCH_ORDERBOOK, MdFetchOrderBook(**common, depth=depth)
    if kind == "quote":
        return MD_FETCH_BESTQUOTE, MdFetchBestQuote(**common)
    raise SystemExit(f"unknown query {kind!r}; use one of: klines, book, quote")


def show(result: object) -> None:
    if not result.ok:  # type: ignore[attr-defined]
        code = result.error_code  # type: ignore[attr-defined]
        print(f"  failed: {describe(code)}  retryable={is_retryable(code)}")
        print(f"  reason: {result.reason}")  # type: ignore[attr-defined]
        return
    if isinstance(result, MdKlinesResult):
        print(f"  {len(result.klines)} candles at {result.interval}")
        for k in result.klines:
            print(
                f"    t={int(k.open_time)} O={k.open} H={k.high} "
                f"L={k.low} C={k.close} closed={k.closed}"
            )
    elif isinstance(result, MdOrderBookResult):
        book = result.book
        assert book is not None  # ok results always carry one
        print(f"  {len(book.bids)} bids / {len(book.asks)} asks")
        for level in book.asks[::-1]:
            print(f"    ask {level.price} x {level.qty}")
        for level in book.bids:
            print(f"    bid {level.price} x {level.qty}")
    elif isinstance(result, MdBestQuoteResult):
        if result.quote is None:
            # Not a failure: a side of the book was empty, so there is nothing
            # to price against — which is different from a quote of zero.
            print("  ok, but no quote: a side of the book was empty")
        else:
            q = result.quote
            print(f"  bid {q.bid} x {q.bid_qty} | ask {q.ask} x {q.ask_qty}")


async def run(kind: str, args: list[str]) -> int:
    caller = f"cli-{os.getpid()}"
    channel = Topics.md_fetch_reply(caller)
    msg_type, payload = build(kind, args, channel)

    async with Broker(BrokerConfig.from_env()) as broker:
        arrived = asyncio.Event()
        answers: list[object] = []

        async def listen() -> None:
            async for env in broker.subscribe(channel):
                model = RESULTS.get(env.type)
                if model is not None:
                    answers.append(model.model_validate(env.payload))
                    arrived.set()
                    return

        # Listening before asking: the answer is published, not queued, and a
        # subscriber that is not up yet simply misses it.
        listener = asyncio.create_task(listen())
        await asyncio.sleep(0.2)

        print(f"→ {Topics.md_fetch()}  {msg_type}  {payload.ticker}")
        reply = await broker.request(
            Topics.md_fetch(),
            Envelope[type(payload)].wrap(payload, type=msg_type, source=caller),
            timeout=5.0,
        )
        ack = MdQueryAck.model_validate(reply.payload)
        if not ack.accepted:
            print(f"← refused: {describe(ack.error_code)} — {ack.reason}")
            listener.cancel()
            return 1
        print("← accepted")

        try:
            await asyncio.wait_for(arrived.wait(), timeout=RESULT_TIMEOUT_S)
        except TimeoutError:
            print(f"← no answer within {RESULT_TIMEOUT_S:.0f}s")
            listener.cancel()
            return 1

        result = answers[0]
        print(f"← {type(result).__name__}")
        show(result)
        listener.cancel()
        return 0 if result.ok else 1  # type: ignore[attr-defined]


def main() -> None:
    if len(sys.argv) < 2:
        raise SystemExit(__doc__)
    try:
        code = asyncio.run(run(sys.argv[1], sys.argv[2:]))
    except Exception as exc:  # noqa: BLE001 — a CLI, not a service
        print(f"failed: {type(exc).__name__}: {exc}", file=sys.stderr)
        raise SystemExit(1) from exc
    raise SystemExit(code)


if __name__ == "__main__":
    main()
