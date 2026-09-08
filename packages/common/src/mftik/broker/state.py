"""Pattern G — a local projection of shared mutable state.

TD writes the book and the ledger; STS used to rebuild the hash on every
``view()``. A watch feeds this map instead.
"""

from __future__ import annotations

import asyncio
import json
import logging
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from mftik.broker.client import Broker

logger = logging.getLogger(__name__)


class StateProjection:
    """Local copy of one state name, kept current by :meth:`Broker.state_watch`."""

    def __init__(self, broker: Broker, name: str) -> None:
        self._broker = broker
        self.name = name
        self._rows: dict[str, dict[str, Any]] = {}
        self._stop = asyncio.Event()
        self._ready = asyncio.Event()
        self._task: asyncio.Task[None] | None = None

    async def start(self) -> None:
        if self._task is not None:
            return
        self._stop.clear()
        self._rows = await self._broker.state_all(self.name)
        self._ready.set()
        self._task = asyncio.create_task(
            self._run(), name=f"state-watch-{self.name}"
        )

    async def close(self) -> None:
        self._stop.set()
        task = self._task
        self._task = None
        if task is not None:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        self._rows.clear()

    def get(self, field: str) -> dict[str, Any] | None:
        return self._rows.get(field)

    def all(self) -> dict[str, dict[str, Any]]:
        return dict(self._rows)

    async def _run(self) -> None:
        try:
            async for field, raw in self._broker.state_watch(
                self.name, stop=self._stop
            ):
                if raw is None:
                    self._rows.pop(field, None)
                else:
                    try:
                        value = json.loads(raw)
                    except ValueError:
                        logger.warning(
                            "state projection dropped unreadable field "
                            "name=%s field=%s",
                            self.name,
                            field,
                        )
                        continue
                    if isinstance(value, dict):
                        self._rows[field] = value
                if not self._ready.is_set():
                    self._ready.set()
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception(
                "state projection ended name=%s", self.name
            )
        finally:
            self._ready.set()
