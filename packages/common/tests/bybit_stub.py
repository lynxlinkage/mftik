"""A local stand-in for Bybit's v5 sockets.

Speaks the real envelopes: ``{"op", "args", "req_id"}`` in, ``{"op",
"success", "ret_msg", "req_id"}`` back on the stream sockets, ``{"reqId",
"retCode", "retMsg", "data"}`` on the trade socket, and ``{"topic", "type",
"ts", "data"}`` for pushes.

:class:`FakeBybit` **verifies the HMAC-SHA256 auth signature** rather than
waving it through. That is the point of having it: signing is the one part of
this adapter with no way to be partly right, and a stub that accepted anything
would leave the ``GET/realtime<expires>`` payload untested until a real key hit
a real venue. It also enforces the expiry, because a deadline that is only
*computed* correctly is not the same as one the venue would accept.
"""

from __future__ import annotations

import hashlib
import hmac
import json
import time
from typing import Any

API_KEY = "test-api-key"
API_SECRET = "test-api-secret"


def sign(secret: str, expires: int) -> str:
    """What Bybit computes to check an ``op: auth``."""
    return hmac.new(
        secret.encode("utf-8"), f"GET/realtime{expires}".encode(), hashlib.sha256
    ).hexdigest()


class FakeBybit:
    """One Bybit v5 socket — private, trade or public, depending on use.

    All three speak the same envelope, so one server covers them; a test picks
    the behaviour it needs by which ops it sends and what it pushes.
    """

    def __init__(self, *, api_secret: str | None = API_SECRET) -> None:
        #: ``None`` disables signature checking, for a public socket where no
        #: auth should ever arrive.
        self.api_secret = api_secret
        self.url = ""
        self.received: list[dict[str, Any]] = []
        self.clients: set[Any] = set()
        self.subscribed: set[str] = set()
        self.connections = 0
        self.auths = 0
        self.pings = 0
        self.drop_next = False
        #: op → the ``data`` to answer a trade call with.
        self.results: dict[str, Any] = {}
        #: op → ``(retCode, retMsg)`` to refuse with instead.
        self.errors: dict[str, tuple[int, str]] = {}
        #: Answer pongs the way the private socket does — ``op: pong``, no
        #: ``req_id`` — rather than the public socket's echo.
        self.private_pong = False
        #: Hold this many trade replies, then release them in reverse arrival
        #: order, so correlation cannot lean on the order things came back.
        self.hold_replies = 0
        self._held: list[tuple[Any, str]] = []

    # --- server ------------------------------------------------------------

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
        op = msg.get("op", "")
        req_id = msg.get("req_id") or msg.get("reqId")

        if op == "auth":
            await self._send(websocket, self._auth(msg, req_id))
            return

        if op == "ping":
            self.pings += 1
            if self.private_pong:
                await self._send(
                    websocket,
                    {"op": "pong", "args": [str(int(time.time() * 1000))],
                     "conn_id": "conn-1", "success": True, "ret_msg": "pong"},
                )
            else:
                await self._send(
                    websocket,
                    {"op": "ping", "req_id": req_id, "success": True,
                     "ret_msg": "pong", "conn_id": "conn-1"},
                )
            return

        if op in ("subscribe", "unsubscribe"):
            args = [str(a) for a in msg.get("args") or []]
            failure = self.errors.get(op)
            if failure is not None:
                code, message = failure
                await self._send(
                    websocket,
                    {"op": op, "req_id": req_id, "success": False,
                     "ret_msg": message, "retCode": code, "conn_id": "conn-1"},
                )
                return
            if op == "subscribe":
                self.subscribed.update(args)
            else:
                self.subscribed.difference_update(args)
            await self._send(
                websocket,
                {"op": op, "req_id": req_id, "success": True, "ret_msg": "",
                 "conn_id": "conn-1"},
            )
            return

        # Anything else is a trade op, which answers in the REST vocabulary.
        failure = self.errors.get(op)
        if failure is not None:
            code, message = failure
            reply = {
                "reqId": req_id,
                "retCode": code,
                "retMsg": message,
                "op": op,
                "data": {},
                "connId": "conn-1",
            }
        else:
            reply = {
                "reqId": req_id,
                "retCode": 0,
                "retMsg": "OK",
                "op": op,
                "data": self.results.get(op, {"orderId": "ord-1"}),
                "header": {"X-Bapi-Limit": "20"},
                "connId": "conn-1",
            }
        encoded = json.dumps(reply)
        if self.hold_replies > 0:
            self._held.append((websocket, encoded))
            self.hold_replies -= 1
            if self.hold_replies == 0:
                for sock, held in reversed(self._held):
                    await sock.send(held)
                self._held.clear()
            return
        await websocket.send(encoded)

    def _auth(self, msg: dict[str, Any], req_id: Any) -> dict[str, Any]:
        """Check the signature the way Bybit does, or answer ``10004``."""
        args = msg.get("args") or []
        refused = {
            "op": "auth",
            "req_id": req_id,
            "success": False,
            "conn_id": "conn-1",
        }
        if self.api_secret is None or len(args) != 3:
            return {**refused, "ret_msg": "malformed auth"}
        key, expires, signature = args[0], int(args[1]), args[2]
        if expires <= int(time.time() * 1000):
            return {**refused, "ret_msg": "expires is in the past", "retCode": 10004}
        if signature != sign(self.api_secret, expires):
            return {**refused, "ret_msg": "error sign!", "retCode": 10004}
        self.auths += 1
        return {
            "op": "auth",
            "req_id": req_id,
            "success": True,
            "ret_msg": "",
            "conn_id": "conn-1",
            "args": [key],
        }

    async def _send(self, websocket: Any, frame: dict[str, Any]) -> None:
        await websocket.send(json.dumps(frame))

    # --- driving a test ----------------------------------------------------

    async def push(
        self, topic: str, data: Any, *, kind: str = "snapshot", ts: int | None = None
    ) -> None:
        """Push one topic frame to every connected client."""
        frame = json.dumps(
            {
                "topic": topic,
                "type": kind,
                "ts": int(time.time() * 1000) if ts is None else ts,
                "data": data,
            }
        )
        for client in list(self.clients):
            await client.send(frame)

    async def drop(self) -> None:
        """Close every live connection."""
        for client in list(self.clients):
            await client.close()

    def frames_for(self, op: str) -> list[dict[str, Any]]:
        return [msg for msg in self.received if msg.get("op") == op]

    def call(self, op: str) -> dict[str, Any]:
        for msg in self.received:
            if msg.get("op") == op:
                return msg
        raise AssertionError(f"no call for {op}")
