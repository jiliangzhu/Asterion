from __future__ import annotations

import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


class StartupTruthSourceTest(unittest.TestCase):
    def test_startup_and_sidebar_share_latest_accepted_tranche_copy(self) -> None:
        startup = (ROOT / "start_asterion.sh").read_text(encoding="utf-8")
        app = (ROOT / "ui" / "app.py").read_text(encoding="utf-8")
        shared = (ROOT / "asterion_core" / "ui" / "surface_truth_shared.py").read_text(encoding="utf-8")
        readme = (ROOT / "README.md").read_text(encoding="utf-8")

        self.assertIn("latest accepted tranche: Phase 11", startup)
        self.assertIn("no newer tranche-specific implementation plan currently open", startup)
        self.assertNotIn("remediation in progress", startup)

        self.assertIn("Asterion Ops Console", app)
        self.assertNotIn("Asterion v1.2", app)

        self.assertIn("P4 accepted; post-P4 remediation accepted; v2.0 implementation active", shared)
        self.assertIn("P9_Implementation_Plan.md", readme)
        self.assertIn("P10_Implementation_Plan.md", readme)
        self.assertIn("P11_Closeout_Checklist.md", readme)


if __name__ == "__main__":
    unittest.main()
