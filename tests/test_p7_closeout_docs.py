from __future__ import annotations

import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


class P7CloseoutDocsTest(unittest.TestCase):
    def test_p7_and_v2_docs_reflect_closeout_pending_current_reality(self) -> None:
        p7_plan = (
            ROOT / "docs" / "10-implementation" / "versions" / "v2.0" / "phase-plans" / "P7_Implementation_Plan.md"
        ).read_text(encoding="utf-8")
        v2_plan = (
            ROOT / "docs" / "10-implementation" / "versions" / "v2.0" / "phase-plans" / "V2_Implementation_Plan.md"
        ).read_text(encoding="utf-8")
        checklist = (
            ROOT / "docs" / "10-implementation" / "versions" / "v2.0" / "checklists" / "P7_Closeout_Checklist.md"
        ).read_text(encoding="utf-8")

        self.assertIn("in progress / closeout pending", p7_plan)
        self.assertIn("tests.test_deployable_rerank_acceptance", p7_plan)
        self.assertIn("tests.test_allocator_rerank_surface_consistency", p7_plan)
        self.assertIn("tests.test_retrospective_uplift_integration", p7_plan)
        self.assertIn("current tranche in progress", v2_plan)
        self.assertIn("rerank 已进入主链", v2_plan)
        self.assertIn("hard gate deferred to P8", v2_plan)
        self.assertIn("P7` / `V2` current-state wording refreshed", checklist)


if __name__ == "__main__":
    unittest.main()
