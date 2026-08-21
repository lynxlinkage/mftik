"""Discord webhook POST. The URL never appears in an audit ``result``."""

from __future__ import annotations

from typing import Any
from urllib.parse import urlparse

import httpx

_client: httpx.AsyncClient | None = None
_WEBHOOK_HOSTS = frozenset(
    {
        "discord.com",
        "discordapp.com",
        "ptb.discord.com",
        "canary.discord.com",
    }
)


class InvalidWebhookUrl(ValueError):
    """A webhook the API will not store or POST."""


def set_http_client(client: httpx.AsyncClient | None) -> None:
    """Inject the client tests drive. ``None`` restores the default."""
    global _client
    _client = client


def validate_webhook_url(url: str) -> str:
    cleaned = url.strip()
    parsed = urlparse(cleaned)
    host = (parsed.hostname or "").lower()
    if parsed.scheme != "https":
        raise InvalidWebhookUrl("webhook_url must be https")
    if host not in _WEBHOOK_HOSTS:
        raise InvalidWebhookUrl("webhook_url host is not Discord")
    if not parsed.path.startswith("/api/webhooks/"):
        raise InvalidWebhookUrl("webhook_url path is not a Discord webhook")
    return cleaned


def mask_webhook_url(url: str) -> str:
    parsed = urlparse(url)
    host = parsed.netloc or "discord.com"
    scheme = parsed.scheme or "https"
    return f"{scheme}://{host}/api/webhooks/…/***"


def test_fire_payload(name: str) -> dict[str, Any]:
    return {
        "embeds": [
            {
                "title": "test fire from this node",
                "description": name,
            }
        ]
    }


async def post_webhook(url: str, payload: dict[str, Any]) -> httpx.Response:
    if _client is not None:
        return await _client.post(url, json=payload)
    async with httpx.AsyncClient(timeout=10.0) as client:
        return await client.post(url, json=payload)
