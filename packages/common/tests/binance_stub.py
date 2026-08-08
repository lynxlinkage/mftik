"""Local stand-ins for Binance's two spot sockets.

Speaks the real envelopes: ``{"id", "method", "params"}`` in, ``{"id",
"status", "result" | "error"}`` back, ``{"stream", "data"}`` for market pushes
and ``{"subscriptionId", "event"}`` for user data.

:class:`FakeBinanceApi` **verifies the Ed25519 signature** on ``session.logon``
rather than waving it through. That is the point of having it: signing is the
one part of this adapter with no way to be partly right, and a stub that
accepted anything would leave the sorted-params rule and the base64 encoding
untested until a real key hit a real venue.
"""

from __future__ import annotations

import base64
import json
import time
from typing import Any

from cryptography.exceptions import InvalidSignature
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from cryptography.hazmat.primitives.serialization import (
    Encoding,
    NoEncryption,
    PrivateFormat,
)
from mft.exchange.binance.spot import methods as m
from mft.exchange.binance.spot.protocol import payload_for

API_KEY = "test-api-key"


def keypair() -> tuple[Ed25519PrivateKey, str]:
    """A throwaway Ed25519 key and its PKCS#8 PEM, as a user would store it."""
    key = Ed25519PrivateKey.generate()
    pem = key.private_bytes(
        encoding=Encoding.PEM,
        format=PrivateFormat.PKCS8,
        encryption_algorithm=NoEncryption(),
    ).decode("ascii")
    return key, pem


class FakeBinanceApi:
    """Minimal Binance spot WebSocket API server."""

    def __init__(self, public_key: Any = None) -> None:
        self.public_key = public_key
        self.received: list[dict[str, Any]] = []
        self.clients: set[Any] = set()
        #: method -> the ``result`` to answer with.
        self.results: dict[str, Any] = {}
        #: method -> the ``error`` block to answer with instead.
        self.errors: dict[str, dict[str, Any]] = {}
        self.connections = 0
        self.drop_next = False
        self.logons = 0
        self.user_subscribes = 0
        #: Hold this many replies, then release them in reverse arrival order,
        #: so correlation cannot lean on the order things came back.
        self.hold_replies = 0
        self._held: list[tuple[Any, str]] = []

    async def handler(self, websocket: Any) -> None:
        self.connections += 1
        self.clients.add(websocket)
        try:
            async for raw in websocket:
                msg = json.loads(raw)
                self.received.append(msg)
                await self._answer(websocket, msg)
                if self.drop_next:
                    self.drop_next = False
                    await websocket.close()
                    return
        except Exception:
            pass
        finally:
            self.clients.discard(websocket)

    async def _answer(self, websocket: Any, msg: dict[str, Any]) -> None:
        method = msg.get("method", "")
        req_id = msg.get("id")

        # Binance requires ``timestamp`` on every SIGNED endpoint however the
        # connection was authenticated — a logged-on session replaces the key
        # and the signature, not the clock. Enforced here because a stub that
        # let it slide is exactly how a missing timestamp reached production.
        if method in m.SIGNED and "timestamp" not in (msg.get("params") or {}):
            await self._send(
                websocket,
                {
                    "id": req_id,
                    "status": 400,
                    "error": {
                        "code": -1102,
                        "msg": (
                            "Mandatory parameter 'timestamp' was not sent, "
                            "was empty/null, or malformed."
                        ),
                    },
                },
            )
            return

        if method in self.errors:
            await self._send(
                websocket,
                {"id": req_id, "status": 400, "error": self.errors[method]},
            )
            return

        if method == m.SESSION_LOGON:
            result = self._logon(msg.get("params") or {})
            if isinstance(result, dict) and "code" in result:
                await self._send(
                    websocket, {"id": req_id, "status": 401, "error": result}
                )
                return
            self.logons += 1
            await self._send(websocket, {"id": req_id, "status": 200, "result": result})
            return

        if method == m.USER_DATA_STREAM_SUBSCRIBE:
            self.user_subscribes += 1
            await self._send(
                websocket,
                {"id": req_id, "status": 200, "result": {"subscriptionId": 0}},
            )
            return

        reply = json.dumps(
            {
                "id": req_id,
                "status": 200,
                "result": self.results.get(method, {}),
                "rateLimits": [],
            }
        )
        if self.hold_replies > 0:
            self._held.append((websocket, reply))
            self.hold_replies -= 1
            if self.hold_replies == 0:
                for sock, held in reversed(self._held):
                    await sock.send(held)
                self._held.clear()
            return
        await websocket.send(reply)

    def _logon(self, params: dict[str, Any]) -> dict[str, Any]:
        """Verify the signature the way Binance does, or answer ``-1022``."""
        signature = params.get("signature", "")
        if self.public_key is not None:
            try:
                self.public_key.verify(
                    base64.b64decode(signature),
                    payload_for(params).encode("utf-8"),
                )
            except (InvalidSignature, ValueError):
                return {
                    "code": -1022,
                    "msg": "Signature for this request is not valid.",
                }
        return {
            "apiKey": params.get("apiKey", API_KEY),
            "authorizedSince": int(time.time() * 1000),
            "connectedSince": int(time.time() * 1000),
            "returnRateLimits": False,
            "serverTime": int(time.time() * 1000),
            "userDataStream": False,
        }

    async def _send(self, websocket: Any, frame: dict[str, Any]) -> None:
        await websocket.send(json.dumps(frame))

    async def push_event(self, event: dict[str, Any], *, subscription: int = 0) -> None:
        """Push one user data event to every connected client."""
        frame = json.dumps({"subscriptionId": subscription, "event": event})
        for client in list(self.clients):
            await client.send(frame)

    async def drop(self) -> None:
        """Close every live connection — what Binance does every 24 hours."""
        for client in list(self.clients):
            await client.close()

    def call(self, method: str) -> dict[str, Any]:
        for msg in self.received:
            if msg.get("method") == method:
                return msg
        raise AssertionError(f"no call for {method}")

    def calls(self, method: str) -> list[dict[str, Any]]:
        return [msg for msg in self.received if msg.get("method") == method]


class FakeBinanceStream:
    """Minimal Binance spot market-streams server (combined endpoint)."""

    def __init__(self) -> None:
        self.received: list[dict[str, Any]] = []
        self.clients: set[Any] = set()
        self.subscribed: set[str] = set()
        self.connections = 0
        self.drop_next = False
        #: method -> the ``error`` block to answer a subscribe with.
        self.errors: dict[str, dict[str, Any]] = {}

    async def handler(self, websocket: Any) -> None:
        self.connections += 1
        self.clients.add(websocket)
        try:
            async for raw in websocket:
                msg = json.loads(raw)
                self.received.append(msg)
                method = msg.get("method", "")
                params = msg.get("params") or []
                if method in self.errors:
                    await websocket.send(
                        json.dumps(
                            {
                                "id": msg.get("id"),
                                "error": self.errors[method],
                            }
                        )
                    )
                    continue
                if method == "SUBSCRIBE":
                    self.subscribed.update(params)
                    result: Any = None
                elif method == "UNSUBSCRIBE":
                    self.subscribed.difference_update(params)
                    result = None
                elif method == "LIST_SUBSCRIPTIONS":
                    result = sorted(self.subscribed)
                else:
                    result = None
                await websocket.send(
                    json.dumps({"id": msg.get("id"), "result": result})
                )
                if self.drop_next:
                    self.drop_next = False
                    await websocket.close()
                    return
        except Exception:
            pass
        finally:
            self.clients.discard(websocket)

    async def push(self, stream: str, data: Any) -> None:
        """Push one combined-endpoint message to every connected client."""
        frame = json.dumps({"stream": stream, "data": data})
        for client in list(self.clients):
            await client.send(frame)

    async def drop(self) -> None:
        """Close every live connection — what Binance does every 24 hours."""
        for client in list(self.clients):
            await client.close()

    def frames_for(self, method: str) -> list[dict[str, Any]]:
        return [msg for msg in self.received if msg.get("method") == method]
