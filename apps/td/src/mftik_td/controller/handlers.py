"""``td.intent.put`` / ``td.intent.delete`` as one handler (IF-02).

Not registered on the running process. B4-07 connects this callable to
``td.{instance}``.

A handler's whole input is the decoded envelope and its whole output is
the reply (H1). It raises until B4-07.
"""

from __future__ import annotations

from mftik.broker.handler import Handler, Reply
from mftik.instance import validate_instance_name
from mftik.protocol import TD_INTENT_DELETE, TD_INTENT_PUT, Topics, UntypedEnvelope

from mftik_td.controller._ticket import unimplemented
from mftik_td.controller.orchestrator import TdOrchestrator

#: The two types this subject carries (§8.1, §8.3). A put replaces the
#: owner's whole account set. A delete releases accounts. Both feed the
#: trading-layer level. Neither creates an account worker (F35).
INTENT_TYPES = frozenset({TD_INTENT_PUT, TD_INTENT_DELETE})


def intent_handler(orchestrator: TdOrchestrator) -> Handler:
    """``td.intent.put`` and ``td.intent.delete`` for this controller.

    Served on :func:`control_subject` when B4-07 registers it. The
    subject is not bound here. The reply is
    :class:`~mftik.protocol.v2.TdIntentPutResult` or
    :class:`~mftik.protocol.v2.TdIntentDeleteResult`, echoing
    ``session_id``. Applying the put or delete is
    :func:`mftik_td.controller.apply_put` /
    :func:`mftik_td.controller.apply_delete`, and the trading bit is
    recomputed from what is still held.

    The order path does not come through here (P1). ``td.order.{api_id}``
    is the account worker's.
    """

    async def handle(message: UntypedEnvelope) -> Reply | None:
        # Held for B4-07. The stub does not read the orchestrator.
        _ = (orchestrator, message)
        unimplemented()

    return handle


def control_subject(instance: str) -> str:
    """``td.{instance}``, the subject the intent handler is served on."""
    return Topics.td(validate_instance_name(instance))
