"""Gate adapters — spot and USDT-perpetual, two venues that share HMAC.

``Gate`` is the spot plane. ``GateFutures`` is a separate venue: different
host, different credential, different wallet. Framing lives in
:mod:`mftik.exchange.gate.protocol`.
"""
