"""Forget what the Gate backfill claimed to have confirmed, so it walks again.

Gate's ``from`` is in seconds and the reader sent milliseconds, so every Gate
history request asked for trades after a date fifty thousand years out. Gate
answers that with ``200`` and an empty page rather than an error, the walk read
the short page as a drained one, and the executor did what a drained walk earns:
moved the settlement line to ``now - SAFETY_LAG``.

So the cursor rows for Gate accounts assert that history was re-read and agreed
when nothing was ever read. Fixing the unit does not undo that on its own —
``BackfillExecutor._resume_from`` resumes from the line it finds, so the next
walk would start at roughly *now* and the history behind it would stay unread
forever. The lines have to go before the fix can do anything.

**Deleted, not zeroed.** A row kept at ``confirmed_through_ts=0`` still carries
its ``last_id``, and ``_resume_from`` prefers that id — which on Gate is a
millisecond window string, the very thing being undone. Deleting the row puts
the walk back in the state it should have been in all along: never walked, so
start at this account's first order on file.

**Only ``backfill_cursors``, only the named venue.** Orders and fills are not
touched; nothing that was recorded is lost. The worst this can cost is a
re-walk of history already on file, which is idempotent by design — every
history write is an upsert.

Dry run by default: it prints what it would delete and writes nothing.

    uv run --all-packages python scripts/reset_gate_cursors.py
    uv run --all-packages python scripts/reset_gate_cursors.py --api-id 3
    uv run --all-packages python scripts/reset_gate_cursors.py --apply
"""

from __future__ import annotations

import argparse
import asyncio
from datetime import UTC, datetime
from typing import Any

from mftik.exchange import venues
from mftik_db.models.history import BackfillCursorRow
from mftik_db.repositories import ApiRepository, OrderRepository
from mftik_db.session import session_scope
from sqlalchemy import delete, select


def when(ts: float | None) -> str:
    """A settlement line as a date. Zero and null both mean never walked."""
    if not ts:
        return "never"
    return datetime.fromtimestamp(ts, UTC).strftime("%Y-%m-%d %H:%M:%SZ")


async def accounts(venue: str, api_id: int | None) -> list[Any]:
    """The accounts in scope: one venue's, or the single one named."""
    async with session_scope() as db:
        rows = list(await ApiRepository(db).list_all())
    if api_id is not None:
        return [row for row in rows if row.id == api_id]
    return [row for row in rows if row.venue == venue]


async def cursors_of(api_ids: list[int]) -> list[BackfillCursorRow]:
    if not api_ids:
        return []
    async with session_scope() as db:
        result = await db.execute(
            select(BackfillCursorRow)
            .where(BackfillCursorRow.api_id.in_(api_ids))
            .order_by(
                BackfillCursorRow.api_id.asc(),
                BackfillCursorRow.scope.asc(),
                BackfillCursorRow.stream.asc(),
            )
        )
        return list(result.scalars().all())


async def restart_at(api_id: int) -> str:
    """Where the next walk will begin: this account's first order on file.

    The same answer ``_resume_from`` reaches with no cursor row, resolved here
    so the report says what the reset actually buys rather than only what it
    removes. Per instrument in the executor; the earliest across them is what
    is worth printing.
    """
    async with session_scope() as db:
        repo = OrderRepository(db)
        tickers = await repo.tickers_for(api_id)
        stamps = [await repo.earliest_ts(api_id, name) for name in tickers]
    real = [ts for ts in stamps if ts]
    if not tickers:
        return "nothing — no orders on file, so no instrument to walk"
    if not real:
        return f"{len(tickers)} instrument(s), no timestamp on file"
    return f"{when(min(real))} ({len(tickers)} instrument(s))"


async def reset(venue: str, api_id: int | None, apply: bool) -> int:
    rows = await accounts(venue, api_id)
    if not rows:
        target = f"api_id {api_id}" if api_id is not None else f"venue {venue!r}"
        print(f"no accounts match {target}")
        return 2

    by_id = {row.id: row for row in rows}
    cursors = await cursors_of(sorted(by_id))
    if not cursors:
        print("no cursor rows for those accounts — nothing to reset")
        return 0

    seen: set[int] = set()
    for cursor in cursors:
        if cursor.api_id not in seen:
            seen.add(cursor.api_id)
            account = by_id[cursor.api_id]
            print(f"\napi_id={account.id} venue={account.venue}")
        print(
            f"  {cursor.stream:<10} {cursor.scope or '(account)':<28} "
            f"confirmed_through={when(cursor.confirmed_through_ts):<22} "
            f"last_id={cursor.last_id}"
        )

    print()
    for account_id in sorted(seen):
        print(f"  api_id={account_id} would restart at {await restart_at(account_id)}")

    print(f"\n{len(cursors)} cursor row(s) across {len(seen)} account(s)")
    if not apply:
        print("dry run — nothing written. Re-run with --apply to delete them.")
        return 0

    async with session_scope() as db:
        result = await db.execute(
            delete(BackfillCursorRow).where(
                BackfillCursorRow.api_id.in_(sorted(seen))
            )
        )
    print(f"deleted {result.rowcount} cursor row(s)")
    print(
        "the next sweep will walk these accounts from their first order; "
        "until it finishes they read as provisional, which is now true"
    )
    return 0


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--venue",
        default=venues.GATE.name,
        help="reset every account on this venue (default: Gate)",
    )
    parser.add_argument(
        "--api-id",
        type=int,
        help="reset one account instead, whatever venue it is on",
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="actually delete. Without it this only reports.",
    )
    args = parser.parse_args()
    raise SystemExit(asyncio.run(reset(args.venue, args.api_id, args.apply)))


if __name__ == "__main__":
    main()
