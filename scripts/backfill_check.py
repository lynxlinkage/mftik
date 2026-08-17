"""Ask a venue for one page of history, with a real credential, and print it.

The history readers were written against documented endpoint shapes and tested
against a mock transport. That pins *this codebase's understanding* of an API,
which is the one thing a mock cannot check. This closes that gap: it signs a
real request with a stored credential and shows what came back beside what the
adapter made of it.

**Read-only by construction.** Every call here is a GET on a history endpoint —
there is no code path to an order — and nothing is written to any table. Run it
with a read-only API key and a bug in this repository still cannot place a
trade.

``limit`` defaults to 1 on purpose: two rows of history are enough to force a
``next_cursor`` out of a venue, which is the part most likely to be wrong and
the part a large page would hide.

    just backfill-check 3 Gate_Spot_ETHUSDT
    just backfill-check 5 Bybit_Perp_BTCUSDT --limit 2
"""

from __future__ import annotations

import argparse
import asyncio
import sys
from typing import Any

from mftik.broker import Broker
from mftik.symbols import SymbolClient
from mftik_db.repositories import ApiRepository, OrderRepository
from mftik_db.session import session_scope
from mftik_td.backfill.reader import HistoryReaderFactory, NoHistoryReaderError


def show(label: str, value: Any) -> None:
    print(f"  {label:<18} {value}")


async def known_tickers(api_id: int) -> list[str]:
    async with session_scope() as db:
        return await OrderRepository(db).tickers_for(api_id)


async def check(api_id: int, ticker: str | None, limit: int) -> int:
    async with session_scope() as db:
        row = await ApiRepository(db).get(api_id)
    if row is None:
        print(f"no api row for {api_id}", file=sys.stderr)
        return 2

    tickers = [ticker] if ticker else await known_tickers(api_id)
    if not tickers:
        print(
            f"api_id={api_id} ({row.venue}) has no orders on file; "
            "name an instrument to check one anyway",
            file=sys.stderr,
        )
        return 2

    async with Broker() as broker:
        factory = HistoryReaderFactory(SymbolClient(broker))
        try:
            reader = await factory.create(row.venue, row)
        except NoHistoryReaderError as exc:
            print(f"api_id={api_id} ({row.venue}): {exc}")
            return 0

        # The credential is never printed — only the fact that one was used.
        print(f"api_id={api_id} venue={row.venue} key={row.api_key[:6]}…")
        await reader.connect()
        try:
            for name in tickers:
                print(f"\n{name}")
                for call in ("fetch_my_trades", "fetch_orders"):
                    await _one(reader, call, name, limit)
        finally:
            await reader.close()
    return 0


async def _one(reader: Any, call: str, ticker: str, limit: int) -> None:
    from mftik.exchange.tickers import UniversalTicker

    fetch = getattr(reader, call, None)
    print(f"\n  {call}")
    if fetch is None:
        show("unsupported", "this venue serves no such read")
        return
    try:
        page = await fetch(UniversalTicker.resolve(ticker), limit=limit)
    except Exception as exc:
        # The venue's own words. A 401 means the signature is wrong, a 404 the
        # path is, and a 400 usually names the parameter it did not like —
        # which is the whole point of running this.
        show("FAILED", f"{type(exc).__name__}: {exc}")
        return
    show("rows", len(page.rows))
    show("next_cursor", page.next_cursor)
    for parsed in page.rows[:limit]:
        show("parsed", parsed.model_dump(mode="json"))


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("api_id", type=int)
    parser.add_argument(
        "ticker",
        nargs="?",
        help="universal ticker; defaults to every one this account has traded",
    )
    parser.add_argument("--limit", type=int, default=1)
    args = parser.parse_args()
    raise SystemExit(asyncio.run(check(args.api_id, args.ticker, args.limit)))


if __name__ == "__main__":
    main()
