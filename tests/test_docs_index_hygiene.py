from __future__ import annotations

import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


class DocsIndexHygieneTest(unittest.TestCase):
    def test_analysis_index_is_canonical_entry_for_analysis_directory(self) -> None:
        index_path = ROOT / "docs" / "analysis" / "Analysis_Index.md"
        text = index_path.read_text(encoding="utf-8")
        for needle in [
            "01_Current_Code_Reassessment.md",
            "02_Current_Deep_Audit_and_Improvement_Plan.md",
            "10_Claude_Asterion_Project_Assessment.md",
            "11_Project_Full_Assessment.md",
            "12_Remediation_Plan.md",
            "13_UI_Redesign_Assessment.md",
            "01-09",
            "10+",
        ]:
            self.assertIn(needle, text)

    def test_archive_checklist_is_not_presented_as_active_closeout_entry(self) -> None:
        redirect_path = (
            ROOT
            / "docs"
            / "10-implementation"
            / "checklists"
            / "P1_P2_AlphaDesk_Remaining_Migration_Checklist.md"
        )
        text = redirect_path.read_text(encoding="utf-8")
        for needle in [
            "archived redirect note",
            "Checklist_Index.md",
            "P2_Closeout_Checklist.md",
            "AlphaDesk_Migration_Ledger.md",
        ]:
            self.assertIn(needle, text)
        self.assertNotIn("## 4. P2 迁移清单", text)

    def test_readme_and_indices_point_to_index_first(self) -> None:
        paths = [
            ROOT / "README.md",
            ROOT / "docs" / "00-overview" / "Documentation_Index.md",
            ROOT / "docs" / "10-implementation" / "Implementation_Index.md",
        ]
        for path in paths:
            text = path.read_text(encoding="utf-8")
            self.assertIn("Checklist_Index.md", text)
        self.assertIn(
            "Version_Index.md",
            (ROOT / "README.md").read_text(encoding="utf-8"),
        )
        self.assertIn(
            "Analysis_Index.md",
            (ROOT / "README.md").read_text(encoding="utf-8"),
        )

    def test_version_tree_and_active_entry_are_consistent(self) -> None:
        docs = [
            ROOT / "README.md",
            ROOT / "AGENTS.md",
            ROOT / "docs" / "00-overview" / "Documentation_Index.md",
            ROOT / "docs" / "10-implementation" / "Implementation_Index.md",
        ]
        for path in docs:
            text = path.read_text(encoding="utf-8")
            self.assertIn("versions/v2.0/phase-plans/V2_Implementation_Plan.md", text)
            self.assertIn("V2_Implementation_Plan.md", text)
            self.assertIn("v2.0 implementation active", text)
            self.assertNotIn("v2.0 planning", text)
        version_index = (ROOT / "docs" / "00-overview" / "Version_Index.md").read_text(encoding="utf-8")
        for needle in ["v1.0", "v1.0-remediation", "v2.0", "当前 active version: `v2.0`"]:
            self.assertIn(needle, version_index)
        p4_docs = [
            ROOT / "docs" / "10-implementation" / "versions" / "v1.0" / "phase-plans" / "P4_Implementation_Plan.md",
            ROOT / "docs" / "10-implementation" / "versions" / "v1.0" / "checklists" / "P4_Closeout_Checklist.md",
            ROOT / "docs" / "10-implementation" / "versions" / "v1.0" / "runbooks" / "P4_Controlled_Rollout_Decision_Runbook.md",
            ROOT / "docs" / "10-implementation" / "versions" / "v1.0" / "runbooks" / "P4_Controlled_Live_Smoke_Runbook.md",
            ROOT / "docs" / "10-implementation" / "versions" / "v1.0" / "runbooks" / "P4_Real_Weather_Chain_Smoke_Runbook.md",
        ]
        for path in p4_docs:
            text = path.read_text(encoding="utf-8")
            self.assertIn("archived accepted historical", text)

    def test_current_tranche_plan_is_indexed(self) -> None:
        tranche_plan = (
            ROOT
            / "docs"
            / "10-implementation"
            / "versions"
            / "v2.0"
            / "phase-plans"
            / "P11_Implementation_Plan.md"
        )
        self.assertTrue(tranche_plan.exists())
        docs = [
            ROOT / "README.md",
            ROOT / "docs" / "00-overview" / "Documentation_Index.md",
            ROOT / "docs" / "10-implementation" / "Implementation_Index.md",
            ROOT / "docs" / "00-overview" / "versions" / "v2.0" / "Asterion_Project_Plan.md",
            ROOT / "docs" / "00-overview" / "versions" / "v2.0" / "DEVELOPMENT_ROADMAP.md",
            ROOT / "docs" / "10-implementation" / "versions" / "v2.0" / "phase-plans" / "V2_Implementation_Plan.md",
        ]
        for path in docs:
            text = path.read_text(encoding="utf-8")
            self.assertIn("P11_Implementation_Plan.md", text)

    def test_recent_and_historical_tranche_plans_are_indexed(self) -> None:
        follow_on_plans = [
            ROOT / "docs" / "10-implementation" / "versions" / "v2.0" / "phase-plans" / "P10_Implementation_Plan.md",
            ROOT / "docs" / "10-implementation" / "versions" / "v2.0" / "phase-plans" / "P9_Implementation_Plan.md",
        ]
        for path in follow_on_plans:
            self.assertTrue(path.exists())
        docs = [
            ROOT / "README.md",
            ROOT / "docs" / "00-overview" / "Documentation_Index.md",
            ROOT / "docs" / "10-implementation" / "Implementation_Index.md",
            ROOT / "docs" / "00-overview" / "versions" / "v2.0" / "Asterion_Project_Plan.md",
            ROOT / "docs" / "00-overview" / "versions" / "v2.0" / "DEVELOPMENT_ROADMAP.md",
            ROOT / "docs" / "10-implementation" / "versions" / "v2.0" / "phase-plans" / "V2_Implementation_Plan.md",
        ]
        for path in docs:
            text = path.read_text(encoding="utf-8")
            self.assertIn("P10_Implementation_Plan.md", text)
            self.assertIn("P11_Implementation_Plan.md", text)
            self.assertIn("P9_Implementation_Plan.md", text)


if __name__ == "__main__":
    unittest.main()
