from __future__ import annotations

import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


class P9CloseoutDocsTest(unittest.TestCase):
    def test_p9_docs_mark_current_tranche_as_closeout_pending(self) -> None:
        p9_plan = (
            ROOT
            / "docs"
            / "10-implementation"
            / "versions"
            / "v2.0"
            / "phase-plans"
            / "P9_Implementation_Plan.md"
        ).read_text(encoding="utf-8")
        v2_plan = (
            ROOT
            / "docs"
            / "10-implementation"
            / "versions"
            / "v2.0"
            / "phase-plans"
            / "V2_Implementation_Plan.md"
        ).read_text(encoding="utf-8")
        readme = (ROOT / "README.md").read_text(encoding="utf-8")

        self.assertIn("current tranche implementation plan", p9_plan)
        self.assertIn("core implemented / closeout pending", p9_plan)
        self.assertIn("delivery contract closeout", p9_plan)
        self.assertIn("Phase 9 — Operator Surface Delivery and Throughput Scaling", v2_plan)
        self.assertIn("core implemented / closeout pending", v2_plan)
        self.assertIn("delivery contract closeout", v2_plan)
        self.assertIn("current tranche: `Phase 9` in progress", readme)
        self.assertIn("core implemented / closeout pending", readme)

    def test_p9_closeout_checklist_is_not_created_early(self) -> None:
        checklist = (
            ROOT
            / "docs"
            / "10-implementation"
            / "versions"
            / "v2.0"
            / "checklists"
            / "P9_Closeout_Checklist.md"
        )
        self.assertFalse(checklist.exists())


if __name__ == "__main__":
    unittest.main()
