"""End-to-end test for stateful (E05-D031, E05-D034).

Three starts: a volume file (which wins over the input), only the input, and
neither. The volume run also checks the timer write, the OnStop write, and a
later incarnation that continues from the file the first one left.
"""

from __future__ import annotations

import json
import sys
import unittest
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from harness import by_hook, drive, logs  # noqa: E402

_HERE = Path(__file__).resolve().parent
_TICKER = "Ex_Spot_ABC"


def _scenario(**extra: Any) -> dict[str, Any]:
    body: dict[str, Any] = {
        "spec": "stateful:Stateful",
        "pythonpath": str(_HERE),
        "params": {"ticker": _TICKER, "api_id": 7, "every_s": 30},
        "ticker": _TICKER,
        "uid": 7,
        "api_id": 7,
        "algo_ids": [1],
        "steps": ["stop"],
    }
    body.update(extra)
    return body


def _fills(report: dict[str, Any]) -> int:
    text = report["volume"]
    if not isinstance(text, str):
        raise AssertionError(repr(report["volume"]))
    data = json.loads(text)
    if not isinstance(data, dict):
        raise AssertionError(text)
    return int(data["fills"])


class StatefulE2E(unittest.TestCase):
    def test_volume_wins_and_the_next_incarnation_continues(self) -> None:
        report = drive(
            _scenario(
                params={"ticker": _TICKER, "api_id": 7, "every_s": 0.2},
                volume_data='{"fills":4}',
                input_data='{"fills":9}',
                steps=["sleep:0.6", "fill", "sleep:0.4", "stop"],
            )
        )
        self.assertEqual(report["code"], 0, report["stderr"])
        self.assertNotIn("Traceback", report["stderr"])
        rows = logs(report)
        self.assertEqual(by_hook(rows, "OnInit")[0]["fills"], 4)
        self.assertEqual(by_hook(rows, "OnInit")[0]["ticker"], _TICKER)
        self.assertGreaterEqual(len(by_hook(rows, "OnTimer")), 1, rows)
        self.assertEqual(by_hook(rows, "OnFill")[0]["fills"], 5)
        self.assertEqual(by_hook(rows, "OnStop")[0]["fills"], 5)
        self.assertEqual(_fills(report), 5)

        continued = drive(_scenario(volume_data=report["volume"]))
        self.assertEqual(continued["code"], 0, continued["stderr"])
        rows = logs(continued)
        self.assertEqual(by_hook(rows, "OnInit")[0]["fills"], 5)
        self.assertEqual(by_hook(rows, "OnFill"), [])
        self.assertEqual(by_hook(rows, "OnStop")[0]["fills"], 5)
        self.assertEqual(_fills(continued), 5)

    def test_input_only_starts_from_that_file(self) -> None:
        report = drive(_scenario(input_data='{"fills":2}'))
        self.assertEqual(report["code"], 0, report["stderr"])
        rows = logs(report)
        self.assertEqual(by_hook(rows, "OnInit")[0]["fills"], 2)
        self.assertEqual(by_hook(rows, "OnStop")[0]["fills"], 2)
        self.assertEqual(_fills(report), 2)

    def test_neither_volume_nor_input_starts_at_zero(self) -> None:
        report = drive(_scenario())
        self.assertEqual(report["code"], 0, report["stderr"])
        rows = logs(report)
        self.assertEqual(by_hook(rows, "OnInit")[0]["fills"], 0)
        self.assertEqual(by_hook(rows, "OnStop")[0]["fills"], 0)
        self.assertEqual(_fills(report), 0)


if __name__ == "__main__":
    unittest.main()
