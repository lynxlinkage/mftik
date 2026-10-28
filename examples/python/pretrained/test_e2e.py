"""End-to-end test for pretrained.

The harness writes the ``model`` input as a plain directory. The packed run
against a real data artifact is ``tests/python/data_input_e2e.py``.
"""

from __future__ import annotations

import hashlib
import json
import sys
import unittest
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from harness import by_hook, drive, logs  # noqa: E402

_HERE = Path(__file__).resolve().parent
_TICKER = "Ex_Spot_ABC"
_WEIGHTS = '{"bias":0.5,"scale":[1,2,3]}'


def _scenario(**extra: Any) -> dict[str, Any]:
    body: dict[str, Any] = {
        "spec": "pretrained:Pretrained",
        "pythonpath": str(_HERE),
        "params": {},
        "ticker": _TICKER,
        "uid": 7,
        "input_name": "model",
        "volume_name": "weights.json",
        "input_data": _WEIGHTS,
        "steps": ["stop"],
    }
    body.update(extra)
    return body


class PretrainedE2E(unittest.TestCase):
    def test_on_init_reads_the_model_input(self) -> None:
        report = drive(_scenario())
        self.assertEqual(report["code"], 0, report["stderr"])
        self.assertNotIn("Traceback", report["stderr"])
        rows = logs(report)
        init = by_hook(rows, "OnInit")
        self.assertEqual(len(init), 1, rows)
        self.assertEqual(init[0]["keys"], ",".join(sorted(json.loads(_WEIGHTS))))
        self.assertEqual(init[0]["sha256"], hashlib.sha256(_WEIGHTS.encode()).hexdigest())
        self.assertEqual(by_hook(rows, "OnStart")[0]["keys"], 2)
        self.assertEqual(by_hook(rows, "OnStop")[0]["keys"], 2)


if __name__ == "__main__":
    unittest.main()
