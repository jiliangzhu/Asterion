from __future__ import annotations

import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


class DeepAuditPlanDocsTest(unittest.TestCase):
    def test_phase10_plus_supporting_docs_exist(self) -> None:
        expected = [
            ROOT / "docs" / "30-trading" / "Controlled_Live_Boundary_Design.md",
            ROOT / "docs" / "30-trading" / "Execution_Economics_Design.md",
            ROOT / "docs" / "40-weather" / "Forecast_Calibration_v2_Design.md",
            ROOT / "docs" / "50-operations" / "Operator_Console_Truth_Source_Design.md",
            ROOT / "docs" / "20-architecture" / "UI_Read_Model_Design.md",
            ROOT / "docs" / "10-implementation" / "checklists" / "Post_P4_P10_Closeout_Checklist.md",
            ROOT / "docs" / "10-implementation" / "checklists" / "Post_P4_P11_Closeout_Checklist.md",
            ROOT / "docs" / "10-implementation" / "checklists" / "Post_P4_P12_Closeout_Checklist.md",
            ROOT / "docs" / "10-implementation" / "checklists" / "Post_P4_P13_Closeout_Checklist.md",
            ROOT / "docs" / "10-implementation" / "checklists" / "Post_P4_P14_Closeout_Checklist.md",
            ROOT / "docs" / "10-implementation" / "checklists" / "Post_P4_P15_Closeout_Checklist.md",
        ]
        missing = [str(path) for path in expected if not path.exists()]
        self.assertEqual(missing, [])

    def test_navigation_docs_reference_deep_audit_materials(self) -> None:
        expected_refs = {
            ROOT / "README.md": [
                "historical accepted remediation record",
                "Controlled_Live_Boundary_Design.md",
                "Execution_Economics_Design.md",
                "Forecast_Calibration_v2_Design.md",
                "Operator_Console_Truth_Source_Design.md",
                "UI_Read_Model_Design.md",
            ],
            ROOT / "docs" / "10-implementation" / "Implementation_Index.md": [
                "Post-P4 Phase 10` 到 `Post-P4 Phase 15",
                "Post_P4_P10_Closeout_Checklist.md",
                "Post_P4_P15_Closeout_Checklist.md",
                "Controlled_Live_Boundary_Design.md",
            ],
            ROOT / "docs" / "00-overview" / "Documentation_Index.md": [
                "Post_P4_P10_Closeout_Checklist.md",
                "Controlled_Live_Boundary_Design.md",
                "Forecast_Calibration_v2_Design.md",
                "Operator_Console_Truth_Source_Design.md",
                "UI_Read_Model_Design.md",
            ],
            ROOT / "docs" / "00-overview" / "DEVELOPMENT_ROADMAP.md": [
                "Deep Audit Improvement Roadmap",
                "Post-P4 Phase 10",
                "Post-P4 Phase 15",
            ],
            ROOT / "docs" / "00-overview" / "Asterion_Project_Plan.md": [
                "Post-P4 Phase 10` 到 `Post-P4 Phase 15",
                "Controlled_Live_Boundary_Design.md",
                "UI_Read_Model_Design.md",
            ],
            ROOT / "docs" / "10-implementation" / "phase-plans" / "Post_P4_Remediation_Implementation_Plan.md": [
                "## 11. Deep Audit Improvement Roadmap",
                "### 11.3 Post-P4 Phase 10: Boundary Hardening v2",
                "### 11.8 Post-P4 Phase 15: UI Read-Model and Truth-Source Refactor",
            ],
        }
        missing_refs: list[str] = []
        for path, refs in expected_refs.items():
            text = path.read_text(encoding="utf-8")
            for ref in refs:
                if ref not in text:
                    missing_refs.append(f"{path}:{ref}")
        self.assertEqual(missing_refs, [])

    def test_only_one_active_planning_entry_is_described(self) -> None:
        docs = [
            ROOT / "README.md",
            ROOT / "docs" / "10-implementation" / "Implementation_Index.md",
            ROOT / "docs" / "00-overview" / "Documentation_Index.md",
            ROOT / "docs" / "00-overview" / "DEVELOPMENT_ROADMAP.md",
            ROOT / "docs" / "00-overview" / "Asterion_Project_Plan.md",
        ]
        missing: list[str] = []
        for path in docs:
            text = path.read_text(encoding="utf-8")
            if "V2_Implementation_Plan.md" not in text:
                missing.append(str(path))
        self.assertEqual(missing, [])


if __name__ == "__main__":
    unittest.main()
