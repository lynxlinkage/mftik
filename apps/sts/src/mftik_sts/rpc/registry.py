"""Re-read the strategy registry without restarting STS.

Adding a strategy writes files into ``MFTIK_DATA`` from the API process. This
one imports them, and until it runs the running STS has never heard of the
tree — a deploy naming it answers ``unknown_strategy``, and a *replaced* tree
is worse, because the deploy succeeds and runs the code from before the edit.

Scanning is the whole job; ``load_local_registry`` already decides what to
skip and logs why. What comes back is the qualified keys this process now
answers to, which is what lets the caller distinguish "written to disk" from
"loadable".
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from mftik.broker import IncomingRequest
from mftik.protocol import (
    STS_ERROR,
    STS_REGISTRY_RELOAD,
    RpcError,
    RpcErrorEnvelope,
    StsRegistryReloadResult,
    StsRegistryReloadResultEnvelope,
)

from mftik_sts.impl import load_local_registry

if TYPE_CHECKING:
    from mftik_sts.session import SessionManager

logger = logging.getLogger(__name__)


async def handle_registry_reload(
    req: IncomingRequest,
    *,
    sessions: SessionManager | None = None,
) -> None:
    del sessions
    try:
        # Synchronous, and deliberately not moved to a thread. It imports
        # Python modules, which mutates ``sys.modules`` — running that
        # alongside a session's own imports is not something to arrange
        # casually, and the scan is a directory walk over a handful of trees.
        loaded = load_local_registry()
    except Exception as exc:
        # A scan that raises is a broken store, not a broken tree: individual
        # trees are already skipped one by one inside. Worth answering as an
        # error rather than as an empty list, which would read as "nothing is
        # loadable" and send the caller looking at their strategy.
        logger.exception("registry reload failed")
        await req.reply(
            RpcErrorEnvelope.wrap(
                RpcError(code="reload_failed", message=str(exc)),
                type=STS_ERROR,
                source="sts",
                session_id=req.envelope.session_id,
            )
        )
        return

    logger.info("registry reloaded: %d strategy(ies)", len(loaded))
    await req.reply(
        StsRegistryReloadResultEnvelope.wrap(
            StsRegistryReloadResult(loaded=loaded),
            type=STS_REGISTRY_RELOAD,
            source="sts",
            session_id=req.envelope.session_id,
        )
    )
