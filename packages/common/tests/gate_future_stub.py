"""Local stand-in for Gate's futures v4 WebSocket."""

from __future__ import annotations

import json
import time
from typing import Any

from mftik.exchange.gate.future import channels as ch

API_KEY = "test-key"
API_SECRET = "test-secret"
UID = "110284739"


class FakeGateFutures:
    """Minimal Gate futures v4 server.

    Same envelope as spot, plus a login result that carries ``uid``.
    """

    def __init__(self) -> None:
        self.received: list[dict[str, Any]] = []
        self.api_calls: list[dict[str, Any]] = []
        self.clients: set[Any] = set()
        self.errors: dict[str, dict[str, Any]] = {}
        self.api_data: dict[str, dict[str, Any]] = {
            ch.LOGIN: {"result": {"uid": UID, "api_key": API_KEY}},
        }
        self.connections = 0
        self.drop_next = False
        self.hold_api_replies = 0
        self._held: list[tuple[Any, str]] = []

    async def handler(self, websocket: Any) -> None:
        self.connections += 1
        self.clients.add(websocket)
        try:
            async for raw in websocket:
                msg = json.loads(raw)
                channel = msg.get("channel", "")

                if channel == ch.PING:
                    await websocket.send(
                        json.dumps({"time": int(time.time()), "channel": ch.PONG})
                    )
                    continue

                if msg.get("event") == ch.API:
                    await self._answer_api(websocket, msg)
                    continue

                self.received.append(msg)
                event = msg.get("event", "")
                ack: dict[str, Any] = {
                    "time": int(time.time()),
                    "channel": channel,
                    "event": event,
                }
                if channel in self.errors:
                    ack["error"] = self.errors[channel]
                else:
                    ack["result"] = {"status": "success"}
                await websocket.send(json.dumps(ack))

                if self.drop_next:
                    self.drop_next = False
                    await websocket.close()
                    return
        except Exception:
            pass
        finally:
            self.clients.discard(websocket)

    async def _answer_api(self, websocket: Any, msg: dict[str, Any]) -> None:
        self.api_calls.append(msg)
        channel = msg["channel"]
        req_id = msg["payload"]["req_id"]
        header = {
            "response_time": str(int(time.time() * 1000)),
            "status": "200",
            "channel": channel,
            "event": ch.API,
        }
        if channel != ch.LOGIN:
            await websocket.send(
                json.dumps(
                    {
                        "request_id": req_id,
                        "ack": True,
                        "header": header,
                        "data": {
                            "result": {
                                "req_id": req_id,
                                "req_param": msg["payload"].get("req_param"),
                            }
                        },
                    }
                )
            )
        reply = json.dumps(
            {
                "request_id": req_id,
                "header": header,
                "data": self.api_data.get(channel, {"result": {}}),
            }
        )
        if channel != ch.LOGIN and self.hold_api_replies > 0:
            self._held.append((websocket, reply))
            self.hold_api_replies -= 1
            if self.hold_api_replies == 0:
                for sock, held in reversed(self._held):
                    await sock.send(held)
                self._held.clear()
            return
        await websocket.send(reply)

    def api_call(self, channel: str) -> dict[str, Any]:
        for call in self.api_calls:
            if call["channel"] == channel:
                return call
        raise AssertionError(f"no api call for {channel}")

    async def push(self, channel: str, result: Any) -> None:
        frame = json.dumps(
            {
                "time": int(time.time()),
                "channel": channel,
                "event": ch.UPDATE,
                "result": result,
            }
        )
        for client in list(self.clients):
            await client.send(frame)

    def frames_for(self, channel: str, event: str = ch.SUBSCRIBE) -> list[dict]:
        return [
            m
            for m in self.received
            if m.get("channel") == channel and m.get("event") == event
        ]
