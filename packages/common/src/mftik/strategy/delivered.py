"""What a strategy reads as ``event.seq`` and ``event.age`` (F25, §5.3).

The hook argument is still the platform model.
:class:`~mftik.exchange.delivery_stamp.HasDelivery` is how that model
answers ``seq``, ``recv_ts`` and ``age``. The session worker calls
:func:`bind_delivery` after it validates the payload and before it
calls the hook. A model that was built in a test, or handed over by
the older session shell, is unstamped: each answer is ``None``.

``seq`` is the MD publisher's per-atom sequence from the envelope. This
module does not mint one. A hole in it, on an ``all`` feed, is the
strategy's loss signal (F25, F23).
"""

from mftik.exchange.delivery_stamp import HasDelivery, bind_delivery

__all__ = ["HasDelivery", "bind_delivery"]
