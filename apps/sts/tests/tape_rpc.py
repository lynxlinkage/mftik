"""Serve ``md.tape.tail`` from a TapeStore — the STS-side test double."""

from __future__ import annotations

import asyncio

from mftik.broker import Broker
from mftik.protocol import MD_TAPE_TAIL, Topics
from mftik_md.rpc.tape import handle_tape_tail
from mftik_md.tape_store import TapeStore


async def serve_tape(
    broker: Broker,
    store: TapeStore,
    *,
    instance: str = "md",
    stop: asyncio.Event,
    chunk: int | None = None,
) -> None:
    """Answer tape reads on ``Topics.md(instance)`` until ``stop``."""
    kwargs = {} if chunk is None else {"chunk": chunk}
    async for req in broker.serve(Topics.md(instance), stop=stop):
        if req.envelope.type != MD_TAPE_TAIL:
            continue
        await handle_tape_tail(req, store=store, **kwargs)
