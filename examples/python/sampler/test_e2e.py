"""End-to-end test for sampler (E07-D015).

The peers are the mftik-testkit shim and nats-server. The timer period
comes from params. The strategy does not subscribe to a feed.
"""

from __future__ import annotations

import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from harness import by_hook, drive, logs  # noqa: E402

_HERE = Path(__file__).resolve().parent
_TICKER = "Ex_Spot_ABC"


def _stamp(row: dict[str, object]) -> None:
    now = row["now"]
    if not isinstance(now, int) or now <= 0:
        raise AssertionError(row)
    if row["again"] != now:
        raise AssertionError(row)
    if row["same"] is not True:
        raise AssertionError(row)
    if not isinstance(row["draw"], int):
        raise AssertionError(row)


class SamplerE2E(unittest.TestCase):
    def test_now_is_the_hooks_start_and_rand_is_one_object(self) -> None:
        report = drive(
            {
                "spec": "sampler:Sampler",
                "pythonpath": str(_HERE),
                "params": {"every_s": 0.2},
                "ticker": _TICKER,
                "uid": 7,
                "steps": ["sleep:0.5", "stop"],
            }
        )
        self.assertEqual(report["code"], 0, report["stderr"])
        self.assertEqual(report["orders"], [])
        self.assertNotIn("Traceback", report["stderr"])
        rows = logs(report)
        init = by_hook(rows, "OnInit")
        start = by_hook(rows, "OnStart")
        timer = by_hook(rows, "OnTimer")
        stop = by_hook(rows, "OnStop")
        self.assertEqual(len(init), 1, rows)
        self.assertEqual(len(start), 1, rows)
        self.assertGreaterEqual(len(timer), 1, rows)
        self.assertEqual(len(stop), 1, rows)
        for row in (*init, *start, *timer, *stop):
            _stamp(row)
        self.assertLessEqual(init[0]["now"], start[0]["now"])
        self.assertLessEqual(start[0]["now"], timer[0]["now"])
        self.assertLessEqual(timer[-1]["now"], stop[0]["now"])


if __name__ == "__main__":
    unittest.main()
