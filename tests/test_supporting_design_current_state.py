from __future__ import annotations

import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


class SupportingDesignCurrentStateTest(unittest.TestCase):
    def test_controlled_live_design_no_longer_describes_old_boundary_as_current(self) -> None:
        text = (
            ROOT / "docs" / "30-trading" / "Controlled_Live_Boundary_Design.md"
        ).read_text(encoding="utf-8")
        self.assertIn("attestation v2 的 nonce / expiry / consume-once 已经落地", text)
        self.assertIn("signer secret name 已不再接受调用方注入", text)
        self.assertIn("当时深度审计指出的 3 条边界薄弱点是", text)

    def test_calibration_design_no_longer_says_manual_materialization(self) -> None:
        text = (
            ROOT / "docs" / "40-weather" / "Forecast_Calibration_v2_Design.md"
        ).read_text(encoding="utf-8")
        self.assertIn("-> scheduled profile materialization", text)
        self.assertNotIn("-> manual profile materialization", text)


if __name__ == "__main__":
    unittest.main()
