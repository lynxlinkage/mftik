"""Load a pretrained model from a read-only session input. This strategy does not trade.

The model is a data artifact uploaded with ``mftiko-pack data``. The
session file names it under ``inputs`` as ``model``, and the platform
unpacks it before the process starts. ``OnInit`` reads ``weights.json``
from ``ctx.input_dir("model")`` and keeps it in memory. The input is
read-only, so the strategy never writes there. A missing input or file
ends the session ``INIT_FAILED``.

``params`` is not used. Each hook writes one JSON line to stdout.
``sha256`` is the digest of the bytes that were read.
"""

from __future__ import annotations

import hashlib
import json
import sys
from pathlib import Path
from typing import Any

import mftik
from mftik.strategy import Strategy

__all__ = ["Pretrained"]

_INPUT = "model"
_FILE = "weights.json"


def _log(fields: dict[str, Any]) -> None:
    sys.stdout.write(json.dumps(fields, separators=(",", ":"), ensure_ascii=True) + "\n")
    sys.stdout.flush()


class Pretrained(Strategy):
    """Holds the weights read from the ``model`` input."""

    def __init__(self) -> None:
        self._weights: dict[str, Any] = {}

    def on_init(self, ctx: mftik.Context) -> None:
        directory = ctx.input_dir(_INPUT)
        if not directory:
            raise RuntimeError("pretrained: the session has no model input")
        raw = (Path(directory) / _FILE).read_bytes()
        weights = json.loads(raw)
        if not isinstance(weights, dict):
            raise ValueError(f"pretrained: {_FILE} must be a JSON object")
        self._weights = weights
        _log(
            {
                "hook": "OnInit",
                "keys": sorted(weights),
                "sha256": hashlib.sha256(raw).hexdigest(),
            }
        )

    def on_start(self, _ctx: mftik.Context) -> None:
        _log({"hook": "OnStart", "keys": len(self._weights)})

    def on_stop(self, _ctx: mftik.Context) -> None:
        _log({"hook": "OnStop", "keys": len(self._weights)})
