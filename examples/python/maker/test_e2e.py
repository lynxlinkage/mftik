"""End-to-end test for maker (E05-D031).

The peers are the mftik-testkit shim, nats-server, and fake SYM and TD
responders. Symbol, size, and price come from params.
"""

from __future__ import annotations

import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from harness import by_hook, drive, logs  # noqa: E402

_HERE = Path(__file__).resolve().parent
_TICKER = "Ex_Spot_ABC"


class MakerE2E(unittest.TestCase):
    def test_recon_batch_cancel_submit_cancel_and_fill(self) -> None:
        report = drive(
            {
                "spec": "maker:Maker",
                "pythonpath": str(_HERE),
                "params": {
                    "ticker": _TICKER,
                    "api_id": 7,
                    "algo_id": 1,
                    "qty": 1,
                    "price": 10,
                },
                "ticker": _TICKER,
                "uid": 7,
                "api_id": 7,
                "algo_ids": [1],
                "recon_open": True,
                "position_size": 2,
                "cancel_batch": 4,
                "steps": ["wait_submit", "accept", "wait_cancel", "fill", "sleep:0.4", "stop"],
            }
        )
        self.assertEqual(report["code"], 0, report["stderr"])
        self.assertNotIn("Traceback", report["stderr"])
        rows = logs(report)

        init = by_hook(rows, "OnInit")
        self.assertEqual(init[0]["ticker"], _TICKER)
        self.assertEqual(init[0]["uid"], 7)

        recon = by_hook(rows, "OnRecon")
        self.assertEqual(len(recon), 1, rows)
        self.assertEqual(recon[0]["position"], 2.0)
        self.assertEqual(len(recon[0]["open"]), 1)

        orders = report["orders"]
        self.assertEqual([row["op"] for row in orders], ["batch_cancel", "submit", "cancel"])
        batch, submit, cancel = orders
        self.assertEqual(len(batch["ids"]), 1)
        self.assertEqual(batch["ids"], recon[0]["open"])
        self.assertEqual(submit["uid"], 7)
        self.assertEqual(submit["qty"], 1.0)
        self.assertEqual(submit["price"], 10.0)
        self.assertEqual(submit["side"], 1)
        self.assertEqual(submit["order_type"], 1)
        self.assertEqual(submit["tif"], 1)
        self.assertEqual(cancel["cloid"], submit["cloid"])
        self.assertNotEqual(cancel["cloid"], batch["ids"][0])

        submitted = by_hook(rows, "submit")
        self.assertEqual(submitted[0]["cloid"], submit["cloid"])
        self.assertEqual(submitted[0]["qty"], 1)
        self.assertEqual(submitted[0]["price"], 10)
        accepted = by_hook(rows, "OnOrder")
        self.assertEqual(accepted[0]["cloid"], submit["cloid"])
        self.assertEqual(accepted[0]["status"], 3)
        self.assertEqual(by_hook(rows, "cancel")[0]["cloid"], submit["cloid"])
        fill = by_hook(rows, "OnFill")
        self.assertEqual(fill[0]["fill_id"], "f1")
        self.assertEqual(fill[0]["uid"], 7)
        self.assertEqual(fill[0]["cloid"], submit["cloid"])


if __name__ == "__main__":
    unittest.main()
