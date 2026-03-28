from __future__ import annotations

import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


class P8CloseoutDocsTest(unittest.TestCase):
    def test_p8_remains_accepted_but_no_longer_latest(self) -> None:
        expectations = {
            ROOT / "docs" / "10-implementation" / "versions" / "v2.0" / "phase-plans" / "P8_Implementation_Plan.md": [
                "accepted closeout record",
                "Phase 0` 到 `Phase 8` 已 accepted",
                "closeout residuals也已经收口",
                "tests.test_calibration_gate_default_clear",
                "tests.test_calibration_gate_fallback_surfaces",
                "tests.test_market_chain_degraded_source_preserves_gate_fields",
            ],
            ROOT / "docs" / "10-implementation" / "versions" / "v2.0" / "phase-plans" / "V2_Implementation_Plan.md": [
                "Phase 8. Calibration Hard Gates and Scaling-Aware Capital Discipline",
                "accepted",
                "`P8` 已 accepted",
            ],
            ROOT / "README.md": ["P8_Implementation_Plan.md"],
            ROOT / "docs" / "10-implementation" / "versions" / "v2.0" / "checklists" / "P8_Closeout_Checklist.md": [
                "accepted closeout checklist",
                "tests.test_calibration_hard_gate_acceptance",
            ],
            ROOT / "docs" / "10-implementation" / "Implementation_Index.md": [
                "更早 accepted tranche record",
                "P8_Closeout_Checklist.md",
            ],
        }
        missing: list[str] = []
        for path, needles in expectations.items():
            text = path.read_text(encoding="utf-8")
            for needle in needles:
                if needle not in text:
                    missing.append(f"{path}:{needle}")
        self.assertEqual(missing, [])
        self.assertNotIn("最近 accepted tranche: `Phase 8`", (ROOT / "README.md").read_text(encoding="utf-8"))


if __name__ == "__main__":
    unittest.main()
