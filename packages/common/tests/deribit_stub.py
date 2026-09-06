"""A local stand-in for Deribit's JSON-RPC 2.0 sockets.

Speaks the real envelopes: ``{"jsonrpc","id","method","params"}`` in,
an id-echoing ``{"id","result"}`` or ``{"id","error"}`` out, and
``{"method":"subscription","params":{"channel","data"}}`` for pushes.
:class:`FakeDeribit` verifies ``public/auth`` ``client_signature`` the
way Bitget's stub verifies login — signing is the one part with no way
to be partly right.
"""

from __future__ import annotations

import json
from typing import Any

from mftik.exchange.deribit.protocol import sign_ws

API_KEY = "test-client-id"
API_SECRET = "test-client-secret"


class FakeDeribit:
    """One Deribit JSON-RPC socket — public or private, depending on use."""

    def __init__(self, *, api_secret: str | None = API_SECRET) -> None:
        #: ``None`` disables auth checking, for a public socket where no
        #: ``public/auth`` should ever arrive.
        self.api_secret = api_secret
        self.url = ""
        self.received: list[dict[str, Any]] = []
        self.clients: set[Any] = set()
        self.subscribed: set[str] = set()
        self.connections = 0
        self.auths = 0
        self.heartbeats = 0
        self.rpc_results: dict[str, Any] = {}
        self.rpc_errors: dict[str, tuple[int, str]] = {}

    async def handler(self, websocket: Any) -> None:
        self.connections += 1
        self.clients.add(websocket)
        try:
            async for raw in websocket:
                text = raw.decode() if isinstance(raw, bytes) else raw
                msg = json.loads(text)
                self.received.append(msg)
                await self._answer(websocket, msg)
        except Exception:
            pass
        finally:
            self.clients.discard(websocket)

    async def _answer(self, websocket: Any, msg: dict[str, Any]) -> None:
        method = str(msg.get("method") or "")
        req_id = msg.get("id")
        params = msg.get("params") if isinstance(msg.get("params"), dict) else {}

        if method == "public/auth":
            await websocket.send(json.dumps(self._auth(msg, req_id)))
            return

        if method in ("public/subscribe", "private/subscribe"):
            channels = [
                c for c in (params.get("channels") or []) if isinstance(c, str)
            ]
            self.subscribed.update(channels)
            await websocket.send(
                json.dumps({"jsonrpc": "2.0", "id": req_id, "result": channels})
            )
            return

        if method in ("public/unsubscribe", "private/unsubscribe"):
            channels = [
                c for c in (params.get("channels") or []) if isinstance(c, str)
            ]
            self.subscribed.difference_update(channels)
            await websocket.send(
                json.dumps({"jsonrpc": "2.0", "id": req_id, "result": channels})
            )
            return

        if method == "public/set_heartbeat":
            self.heartbeats += 1
            await websocket.send(
                json.dumps({"jsonrpc": "2.0", "id": req_id, "result": "ok"})
            )
            return

        if method == "public/test":
            await websocket.send(
                json.dumps(
                    {"jsonrpc": "2.0", "id": req_id, "result": {"version": "stub"}}
                )
            )
            return

        if method in self.rpc_errors:
            code, message = self.rpc_errors[method]
            await websocket.send(
                json.dumps(
                    {
                        "jsonrpc": "2.0",
                        "id": req_id,
                        "error": {"code": code, "message": message},
                    }
                )
            )
            return

        result = self.rpc_results.get(method, {})
        if callable(result):
            result = result(params)
        await websocket.send(
            json.dumps({"jsonrpc": "2.0", "id": req_id, "result": result})
        )

    def _auth(self, msg: dict[str, Any], req_id: Any) -> dict[str, Any]:
        envelope = {"jsonrpc": "2.0", "id": req_id}
        if self.api_secret is None:
            return {
                **envelope,
                "error": {"code": 10000, "message": "no auth on public"},
            }
        params = msg.get("params") if isinstance(msg.get("params"), dict) else {}
        timestamp = params.get("timestamp")
        nonce = str(params.get("nonce") or "")
        data = str(params.get("data") or "")
        signature = str(params.get("signature") or "")
        if signature != sign_ws(self.api_secret, timestamp, nonce, data):
            return {
                **envelope,
                "error": {"code": 10000, "message": "invalid signature"},
            }
        if len(str(timestamp)) < 13:
            return {
                **envelope,
                "error": {"code": 10000, "message": "timestamp unit"},
            }
        self.auths += 1
        return {
            **envelope,
            "result": {
                "access_token": "tok",
                "refresh_token": "ref",
                "expires_in": 900,
            },
        }

    async def push(self, channel: str, data: Any) -> None:
        encoded = json.dumps(
            {
                "jsonrpc": "2.0",
                "method": "subscription",
                "params": {"channel": channel, "data": data},
            }
        )
        for client in list(self.clients):
            await client.send(encoded)

    async def heartbeat(self, kind: str = "test_request") -> None:
        encoded = json.dumps(
            {
                "jsonrpc": "2.0",
                "method": "heartbeat",
                "params": {"type": kind},
            }
        )
        for client in list(self.clients):
            await client.send(encoded)

    async def drop(self) -> None:
        for client in list(self.clients):
            await client.close()

    def frames_for(self, method: str) -> list[dict[str, Any]]:
        return [msg for msg in self.received if msg.get("method") == method]
