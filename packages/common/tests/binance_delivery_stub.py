"""Local stand-ins for Binance's COIN-M WebSocket API and user data socket.

Speaks the real envelopes: ``{"id", "method", "params"}`` in, ``{"id",
"status", "result" | "error"}`` back on the API socket, and **bare events**
on the user data socket.

:class:`FakeBinanceDeliveryApi` **verifies the Ed25519 signature** on
``session.logon`` rather than waving it through, and enforces two rules
Binance enforces:

* a ``SIGNED`` method with no ``timestamp`` is ``-1102``;
* a listen-key method that carries one is ``-1101``.
"""

from __future__ import annotations

import base64
import json
import time
from typing import Any

from cryptography.exceptions import InvalidSignature
from mftik.exchange.binance.delivery import methods as m
from mftik.exchange.binance.delivery.protocol import payload_for

API_KEY = "test-delivery-key"
LISTEN_KEY = "listen-key-1"


class FakeBinanceDeliveryApi:
    """Minimal Binance COIN-M WebSocket API server."""

    def __init__(self, public_key: Any = None) -> None:
        self.public_key = public_key
        self.received: list[dict[str, Any]] = []
        self.clients: set[Any] = set()
        self.results: dict[str, Any] = {}
        self.errors: dict[str, dict[str, Any]] = {}
        self.connections = 0
        self.drop_next = False
        self.logons = 0
        self.listen_keys: list[str] = []
        self.pings = 0
        self.listen_key = LISTEN_KEY

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
        params = msg.get("params") or {}

        if method in m.SIGNED and "timestamp" not in params:
            await self._error(
                websocket,
                req_id,
                -1102,
                "Mandatory parameter 'timestamp' was not sent, was empty/null, "
                "or malformed.",
            )
            return
        if method in m.API_KEY_ONLY and "timestamp" in params:
            await self._error(
                websocket, req_id, -1101, "Too many parameters; expected 'apiKey'."
            )
            return

        if method in self.errors:
            await self._send(
                websocket,
                {"id": req_id, "status": 400, "error": self.errors[method]},
            )
            return

        if method == m.SESSION_LOGON:
            result = self._logon(params)
            if "code" in result:
                await self._send(
                    websocket, {"id": req_id, "status": 401, "error": result}
                )
                return
            self.logons += 1
            await self._send(
                websocket, {"id": req_id, "status": 200, "result": result}
            )
            return

        if method == m.USER_DATA_STREAM_START:
            self.listen_keys.append(self.listen_key)
            await self._send(
                websocket,
                {
                    "id": req_id,
                    "status": 200,
                    "result": {"listenKey": self.listen_key},
                },
            )
            return

        if method == m.USER_DATA_STREAM_PING:
            self.pings += 1
            await self._send(websocket, {"id": req_id, "status": 200, "result": {}})
            return

        await self._send(
            websocket,
            {
                "id": req_id,
                "status": 200,
                "result": self.results.get(method, {}),
                "rateLimits": [],
            },
        )

    def _logon(self, params: dict[str, Any]) -> dict[str, Any]:
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
        }

    async def _error(
        self, websocket: Any, req_id: Any, code: int, message: str
    ) -> None:
        await self._send(
            websocket,
            {"id": req_id, "status": 400, "error": {"code": code, "msg": message}},
        )

    async def _send(self, websocket: Any, frame: dict[str, Any]) -> None:
        await websocket.send(json.dumps(frame))

    async def drop(self) -> None:
        for client in list(self.clients):
            await client.close()

    def call(self, method: str) -> dict[str, Any]:
        for msg in self.received:
            if msg.get("method") == method:
                return msg
        raise AssertionError(f"no call for {method}")

    def calls(self, method: str) -> list[dict[str, Any]]:
        return [msg for msg in self.received if msg.get("method") == method]


class FakeBinanceDeliveryUser:
    """Minimal COIN-M user data socket — the listen-key connection.

    Sends nothing back and reads nothing: a real one never receives a frame.
    What it records is the **path** each connection was opened on.
    """

    def __init__(self) -> None:
        self.clients: set[Any] = set()
        self.paths: list[str] = []
        self.connections = 0

    async def handler(self, websocket: Any) -> None:
        self.connections += 1
        self.paths.append(getattr(websocket.request, "path", ""))
        self.clients.add(websocket)
        try:
            async for _raw in websocket:
                pass
        except Exception:
            pass
        finally:
            self.clients.discard(websocket)

    async def push(self, event: dict[str, Any]) -> None:
        frame = json.dumps(event)
        for client in list(self.clients):
            await client.send(frame)

    async def drop(self) -> None:
        for client in list(self.clients):
            await client.close()

    @property
    def listen_keys(self) -> list[str]:
        return [path.rsplit("/", 1)[-1] for path in self.paths]
