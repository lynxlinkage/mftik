"""A local stand-in for Bitget's UTA v3 sockets.

Speaks the real envelopes: ``{"id", "op", "args"}`` in, an id-less
``{"event", "arg", "connId"}`` subscribe ACK (the venue does not echo
``id``), ``{"event", "code", "msg"}`` for login, ``{"arg", "data"}`` for
pushes, and the literal strings ``ping`` / ``pong``. :class:`FakeBitget`
**verifies the login signature** the way OKX's stub verifies HMAC —
signing is the one part with no way to be partly right.
"""

from __future__ import annotations

import json
from typing import Any

from mftik.exchange.bitget.channels import arg_key
from mftik.exchange.bitget.protocol import sign_ws

API_KEY = "test-api-key"
API_SECRET = "test-api-secret"
PASSPHRASE = "test-passphrase"


class FakeBitget:
    """One Bitget v3 socket — public or private, depending on use."""

    def __init__(self, *, api_secret: str | None = API_SECRET) -> None:
        #: ``None`` disables login checking, for a public socket where no
        #: login should ever arrive.
        self.api_secret = api_secret
        self.url = ""
        self.received: list[dict[str, Any]] = []
        self.clients: set[Any] = set()
        self.subscribed: set[tuple[str, str, str, str]] = set()
        self.connections = 0
        self.logins = 0
        self.pings = 0

    async def handler(self, websocket: Any) -> None:
        self.connections += 1
        self.clients.add(websocket)
        try:
            async for raw in websocket:
                text = raw.decode() if isinstance(raw, bytes) else raw
                if text == "ping":
                    self.pings += 1
                    await websocket.send("pong")
                    continue
                msg = json.loads(text)
                self.received.append(msg)
                await self._answer(websocket, msg)
        except Exception:
            pass
        finally:
            self.clients.discard(websocket)

    async def _answer(self, websocket: Any, msg: dict[str, Any]) -> None:
        op = msg.get("op", "")

        if op == "login":
            await websocket.send(json.dumps(self._login(msg)))
            return

        if op in ("subscribe", "unsubscribe"):
            args = [a for a in (msg.get("args") or []) if isinstance(a, dict)]
            keys = [arg_key(arg) for arg in args]
            if op == "subscribe":
                self.subscribed.update(keys)
            else:
                self.subscribed.difference_update(keys)
            # Venue v3 omits ``id`` on subscribe / unsubscribe ACKs even
            # when the client sent one. Echoing it hid #64 from CI.
            for arg in args or [{}]:
                await websocket.send(
                    json.dumps(
                        {
                            "event": op,
                            "arg": arg,
                            "connId": "stub-conn",
                        }
                    )
                )
            return

    def _login(self, msg: dict[str, Any]) -> dict[str, Any]:
        refused = {"event": "login", "code": "40009", "msg": "error"}
        args = msg.get("args") or []
        if self.api_secret is None or not args:
            return {**refused, "msg": "malformed login"}
        row = args[0]
        signature = str(row.get("sign") or "")
        timestamp = str(row.get("timestamp") or "")
        if signature != sign_ws(self.api_secret, timestamp):
            return {**refused, "msg": "error sign"}
        # V1: seconds are ~10 digits; milliseconds are ~13. Refuse the
        # unit we do not send so a reconnect-loop is a test failure.
        if len(timestamp) > 11:
            return {**refused, "msg": "timestamp unit"}
        self.logins += 1
        return {"event": "login", "code": "0", "msg": ""}

    async def push(
        self,
        arg: dict[str, Any],
        data: Any,
        *,
        action: str | None = None,
    ) -> None:
        frame: dict[str, Any] = {"arg": arg, "data": data}
        if action is not None:
            frame["action"] = action
        encoded = json.dumps(frame)
        for client in list(self.clients):
            await client.send(encoded)

    async def drop(self) -> None:
        for client in list(self.clients):
            await client.close()

    def frames_for(self, op: str) -> list[dict[str, Any]]:
        return [msg for msg in self.received if msg.get("op") == op]
