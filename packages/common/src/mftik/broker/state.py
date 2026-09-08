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

#: How long a watch may run before this map is rebuilt from ``state_all``.
#: ``state_drop`` deletes a field and then purges the marker so the bucket
#: does not grow; a live watch can miss that pair. Reseeding is how the
#: projection learns the field is gone without the writer waiting.
_RESEED_S = 1.0


class StateProjection:
    """Local copy of one state name, kept current by :meth:`Broker.state_watch`.

    A watch that ends is reseeded from :meth:`Broker.state_all` and opened
    again. A hard failure clears :attr:`live` so readers fall back to a
    direct read rather than serving a frozen map.
    """

    def __init__(self, broker: Broker, name: str) -> None:
        self._broker = broker
        self.name = name
        self._rows: dict[str, dict[str, Any]] = {}
        self._stop = asyncio.Event()
        self._ready = asyncio.Event()
        self._task: asyncio.Task[None] | None = None
        self._failed = False

    @property
    def live(self) -> bool:
        """True while the watch task is running and has not failed."""
        return self._task is not None and not self._failed

    async def start(self) -> None:
        if self._task is not None:
            return
        self._stop.clear()
        self._failed = False
        self._rows = await self._broker.state_all(self.name)
        self._ready.set()
        self._task = asyncio.create_task(self._run(), name=f"state-watch-{self.name}")

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

    def _apply(self, field: str, raw: str | None) -> None:
        if raw is None:
            self._rows.pop(field, None)
            return
        try:
            value = json.loads(raw)
        except ValueError:
            logger.warning(
                "state projection dropped unreadable field name=%s field=%s",
                self.name,
                field,
            )
            return
        if isinstance(value, dict):
            self._rows[field] = value
            return
        # A non-object JSON value is not a row. Keeping the last dict
        # would freeze a field the writer has already replaced.
        self._rows.pop(field, None)

    async def _run(self) -> None:
        try:
            while not self._stop.is_set():
                self._rows = await self._broker.state_all(self.name)
                self._ready.set()
                try:
                    async with asyncio.timeout(_RESEED_S):
                        async for field, raw in self._broker.state_watch(
                            self.name, stop=self._stop
                        ):
                            self._apply(field, raw)
                            if not self._ready.is_set():
                                self._ready.set()
                except TimeoutError:
                    continue
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("state projection ended name=%s", self.name)
            self._failed = True
        finally:
            self._task = None
            self._ready.set()
